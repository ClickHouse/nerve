"""Restarting the daemon, from either side of the door.

``nerve restart`` and the web setup wizard's last step have to do the same
thing, and the interesting part is that neither of them can do it directly: the
process that must go down is (usually) the very process being asked. So a
detached helper does it — started in its own session, so it survives its
parent's death, waits for the old daemon to exit and starts a new one.

This module is that mechanism, with no ``click`` in it. :mod:`nerve.cli` keeps
its own PID-file helpers and its own console output and calls in here for the
part that matters; ``POST /api/system/restart`` calls the same function, so a
restart asked for in a browser and one typed at a terminal are the same
restart.

Two environments cannot use the helper and are decided by :func:`restart_mode`:

* inside a container the daemon is PID 1 and the compose file says
  ``restart: unless-stopped``, so the process simply exits
  (:func:`exit_for_supervisor`) and Docker brings it back;
* under systemd the HTTP path refuses with the exact command, because the
  browser user cannot see whether the unit says ``Restart=``; the CLI, whose
  user is on the box, keeps its SIGTERM-and-let-the-unit-respawn behaviour.
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from nerve import paths

#: Overrides the environment detection in :func:`restart_mode`: ``exit`` for
#: a supervisor that restarts an exited process, ``refuse`` for one that must
#: be asked by an operator.
SUPERVISOR_ENV = "NERVE_SUPERVISOR"
#: With ``NERVE_SUPERVISOR=refuse`` outside systemd: the command the operator
#: runs to restart Nerve, returned as ``command`` by ``POST /api/system/restart``
#: so the page can show it. Never executed by Nerve.
RESTART_COMMAND_ENV = "NERVE_RESTART_COMMAND"


def pid_file_pid() -> int | None:
    """The PID recorded in the pid file, or ``None``. Reads; never writes.

    The PID does not tell if the daemon is running: a dead daemon can leave the
    file behind. :func:`nerve.pidfile.live_pid` tells that.
    """
    try:
        return int(paths.pid_file().read_text().strip())
    except (FileNotFoundError, ValueError, OSError):
        return None


def process_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # Process exists but we can't signal it


def is_systemd_managed() -> bool:
    """Whether this process was started by systemd (``Restart=always``)."""
    return os.environ.get("INVOCATION_ID") is not None


def in_container() -> bool:
    """Whether this process runs inside the generated Docker image."""
    return os.environ.get("NERVE_DOCKER") == "1"


_CGROUP_FILE = Path("/proc/self/cgroup")
_DEFAULT_UNIT = "nerve.service"
_USER_MANAGER_RE = re.compile(r"^user@\d+\.service$")


def _unit_from_cgroup(text: str) -> tuple[str, bool]:
    """``(unit, user_manager)`` from the text of ``/proc/self/cgroup``.

    Only the **leaf** of the cgroup path names this process's unit. A
    ``.service`` segment higher up is somebody else's: a process in a
    transient ``run-….scope`` under ``user@<uid>.service`` is not the user
    manager, and restarting that manager would tear down the operator's
    whole session — so anything but a ``.service`` leaf falls back to
    ``nerve.service``. A ``user@<uid>.service`` anywhere on the path means a
    *user* manager started the process, whose units are addressed with
    ``systemctl --user``.
    """
    unit = _DEFAULT_UNIT
    user_manager = False
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # v2: "0::/path"; v1: "hierarchy:controllers:/path". The path is the
        # part after the second colon, so a colon inside the path survives.
        path = line.split(":", 2)[-1]
        parts = [p for p in path.split("/") if p]
        if not parts:
            continue
        if any(_USER_MANAGER_RE.match(p) for p in parts):
            user_manager = True
        leaf = parts[-1]
        if leaf.endswith(".service") and unit == _DEFAULT_UNIT:
            unit = leaf
    return unit, user_manager


def _cgroup_text() -> str:
    try:
        return _CGROUP_FILE.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return ""


def systemd_unit_name() -> str:
    """The ``*.service`` unit this process runs in, else ``nerve.service``.

    Read from ``/proc/self/cgroup``, leaf segment only (see
    :func:`_unit_from_cgroup`). Never raises — the name only decorates a
    message and a command the operator is shown, never one Nerve runs.
    """
    return _unit_from_cgroup(_cgroup_text())[0]


def systemd_user_manager() -> bool:
    """Whether a systemd *user* manager (``user@<uid>.service``) runs this process."""
    return _unit_from_cgroup(_cgroup_text())[1]


def systemd_restart_command() -> str:
    """The ``systemctl`` command that restarts this process's unit —
    ``systemctl --user restart <unit>`` under a user manager."""
    unit, user_manager = _unit_from_cgroup(_cgroup_text())
    return f"systemctl {'--user ' if user_manager else ''}restart {unit}"


def restart_mode() -> Literal["helper", "exit", "refuse"]:
    """How a restart asked for *from inside the daemon* has to happen.

    ``NERVE_SUPERVISOR=exit|refuse`` overrides the detection; otherwise a
    container exits (compose restarts it), systemd refuses (the operator runs
    ``systemctl restart``), and anything else spawns the detached helper.
    """
    override = (os.environ.get(SUPERVISOR_ENV) or "").strip().lower()
    if override == "exit":
        return "exit"
    if override == "refuse":
        return "refuse"
    if in_container():
        return "exit"
    if is_systemd_managed():
        return "refuse"
    return "helper"


def start_command(config_dir: Path | str, *, verbose: bool = False) -> list[str]:
    """The command ``nerve start`` would use to launch the daemon.

    Always ``-m nerve`` so a restart works regardless of how *this* process was
    invoked (console script, ``python -m nerve``, a Docker entrypoint).
    """
    parts = [sys.executable, "-m", "nerve", "-c", str(config_dir)]
    if verbose:
        parts.append("-v")
    parts.extend(["start", "--foreground"])
    return parts


@dataclass(frozen=True)
class RestartOutcome:
    """What was arranged. ``message`` is written for a person to read."""

    method: str          # "systemd" | "helper" | "exit"
    message: str
    old_pid: int | None


def exit_for_supervisor(delay_seconds: float = 0.75) -> RestartOutcome:
    """Arrange for this process to receive SIGTERM shortly, and nothing else.

    For a daemon whose supervisor restarts an exited process (the generated
    compose file's ``restart: unless-stopped``). The delay lets the HTTP
    response that asked for it reach the client first. No helper subprocess:
    inside a dying container there is nothing for one to start.
    """
    pid = os.getpid()
    timer = threading.Timer(
        max(0.0, float(delay_seconds)), os.kill, [pid, signal.SIGTERM],
    )
    timer.daemon = True
    timer.start()
    return RestartOutcome(
        method="exit",
        message=(
            "The process exits and its supervisor starts it again."
        ),
        old_pid=pid,
    )


def restart_daemon(
    config_dir: Path | str,
    *,
    verbose: bool = False,
    old_pid: int | None = None,
    systemd: bool | None = None,
    delay_seconds: float = 0.0,
) -> RestartOutcome:
    """Arrange for the daemon to stop and a fresh one to take its place.

    ``old_pid`` is the process to replace, or ``None`` when nothing is running
    (then this only starts one). ``systemd`` overrides the environment check —
    the CLI passes what its own helper decided so that stays patchable in
    tests.

    ``delay_seconds`` holds the helper back before it signals anything, which
    is what a caller *inside* the daemon needs: an HTTP response has to reach
    the client before the process serving it is stopped. The command-line
    caller passes 0 and keeps its immediate behaviour. A non-zero delay also
    forces the systemd path through the same detached helper rather than
    signalling here, because sleeping in the caller would block the very loop
    that still has a response to flush.

    Raises ``OSError`` if the helper cannot be started: whether a restart was
    *begun* is knowable here, and a caller that reports "restarting" for a
    helper that never existed is lying to the person waiting for it.
    """
    if systemd is None:
        systemd = is_systemd_managed()

    if systemd and delay_seconds <= 0:
        if old_pid is not None:
            os.kill(old_pid, signal.SIGTERM)
            return RestartOutcome(
                method="systemd",
                message=f"Restarting Nerve (PID {old_pid})... systemd will respawn.",
                old_pid=old_pid,
            )
        return RestartOutcome(
            method="systemd",
            message="Nerve is not running — systemd will start it shortly.",
            old_pid=None,
        )

    # Spawn a detached helper that: waits for old PID to exit, then starts
    # a new daemon.  Written as an inline Python script so we don't need an
    # external shell script on disk.
    #
    # Under systemd it starts nothing — the unit does that — so `start_cmd` is
    # None there and the helper exists only to hold the delay and signal.
    start_cmd = None if systemd else start_command(config_dir, verbose=verbose)
    helper_script = (
        "import os, signal, subprocess, sys, time\n"
        f"old_pid = {old_pid if old_pid is not None else 'None'}\n"
        f"pid_file = {str(paths.pid_file())!r}\n"
        f"log_file = {str(paths.log_file())!r}\n"
        f"start_cmd = {start_cmd!r}\n"
        f"time.sleep({max(0.0, float(delay_seconds))!r})\n"
        "if old_pid is not None:\n"
        "    try:\n"
        "        os.kill(old_pid, signal.SIGTERM)\n"
        "    except ProcessLookupError:\n"
        "        pass\n"
        "    for _ in range(30):\n"
        "        time.sleep(0.5)\n"
        "        try:\n"
        "            os.kill(old_pid, 0)\n"
        "        except ProcessLookupError:\n"
        "            break\n"
        "    else:\n"
        "        try:\n"
        "            os.kill(old_pid, signal.SIGKILL)\n"
        "            time.sleep(0.5)\n"
        "        except ProcessLookupError:\n"
        "            pass\n"
        "    # Remove stale PID file\n"
        "    try:\n"
        "        os.unlink(pid_file)\n"
        "    except FileNotFoundError:\n"
        "        pass\n"
        "if start_cmd is None:\n"
        "    sys.exit(0)\n"
        "time.sleep(0.5)\n"
        "log_fd = open(log_file, 'a')\n"
        "proc = subprocess.Popen(\n"
        "    start_cmd,\n"
        "    stdout=log_fd,\n"
        "    stderr=log_fd,\n"
        "    stdin=subprocess.DEVNULL,\n"
        "    start_new_session=True,\n"
        ")\n"
        "log_fd.close()\n"
        "time.sleep(1)\n"
        "if proc.poll() is not None:\n"
        "    sys.exit(1)\n"
    )

    paths.ensure_nerve_home()
    log_fd = open(paths.log_file(), "a")
    try:
        subprocess.Popen(
            [sys.executable, "-c", helper_script],
            stdout=log_fd,
            stderr=log_fd,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    finally:
        log_fd.close()

    if systemd:
        message = f"Restarting Nerve (PID {old_pid})... systemd will respawn."
    elif old_pid is not None:
        message = (
            f"Restarting Nerve (PID {old_pid})... new instance will start shortly."
        )
    else:
        message = "Starting Nerve... new instance will start shortly."
    return RestartOutcome(
        method="systemd" if systemd else "helper", message=message, old_pid=old_pid,
    )
