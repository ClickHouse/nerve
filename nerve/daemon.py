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
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from nerve import paths


def pid_file_pid() -> int | None:
    """The PID recorded in the pid file, or ``None``. Reads; never writes.

    Deliberately without the stale-file cleanup :func:`nerve.cli._read_pid`'s
    caller does: this is called from inside the running daemon, where deleting
    a pid file is how you lose track of the process you are.
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

    method: str          # "systemd" | "helper"
    message: str
    old_pid: int | None


def restart_daemon(
    config_dir: Path | str,
    *,
    verbose: bool = False,
    old_pid: int | None = None,
    systemd: bool | None = None,
) -> RestartOutcome:
    """Arrange for the daemon to stop and a fresh one to take its place.

    ``old_pid`` is the process to replace, or ``None`` when nothing is running
    (then this only starts one). ``systemd`` overrides the environment check —
    the CLI passes what its own helper decided so that stays patchable in
    tests.

    Under systemd there is nothing to spawn: the unit is ``Restart=always``, so
    stopping the process *is* the restart. Otherwise a detached helper is
    started, and this function returns as soon as it exists — by design,
    because the caller is very often the process the helper is about to kill,
    and an HTTP response has to be on the wire before that happens.
    """
    if systemd is None:
        systemd = is_systemd_managed()

    if systemd:
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
    helper_script = (
        "import os, signal, subprocess, sys, time\n"
        f"old_pid = {old_pid if old_pid is not None else 'None'}\n"
        f"pid_file = {str(paths.pid_file())!r}\n"
        f"log_file = {str(paths.log_file())!r}\n"
        f"start_cmd = {start_command(config_dir, verbose=verbose)!r}\n"
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

    if old_pid is not None:
        message = (
            f"Restarting Nerve (PID {old_pid})... new instance will start shortly."
        )
    else:
        message = "Starting Nerve... new instance will start shortly."
    return RestartOutcome(method="helper", message=message, old_pid=old_pid)
