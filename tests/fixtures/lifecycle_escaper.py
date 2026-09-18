#!/usr/bin/env python3
"""Escaping-descendant fixture for Codex lifecycle tests (no model, no network).

Run as the main command of a systemd ``--scope``, it setsid+double-forks a
sleeper grandchild that outlives the scope's main process — the escape a
single ``killpg`` cannot reach. Every process carries a unique ``ESCAPER_TOKEN``
so cleanup only ever signals this fixture's own processes, by pidfd.

Commands: ``spawn-escapee <outdir>`` (plain), ``spawn-escapee-nested <outdir>``
(migrate the grandchild into a child cgroup so the scope root empties but
recursive ``cgroup.events populated`` stays 1), ``sleeper``, ``sweep <token>``.
"""

from __future__ import annotations

import os
import signal
import sys
import time

SELF = os.path.abspath(__file__)
_SLEEP = 300


# --------------------------- identity + cleanup --------------------------- #


def _environ_token(pid: int) -> str | None:
    try:
        with open(f"/proc/{pid}/environ", "rb") as fh:
            raw = fh.read()
    except OSError:
        return None
    for entry in raw.split(b"\0"):
        if entry.startswith(b"ESCAPER_TOKEN="):
            return entry[len(b"ESCAPER_TOKEN="):].decode("utf-8", "replace")
    return None


def _cmdline(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as fh:
            return fh.read().replace(b"\0", b" ").decode("utf-8", "replace")
    except OSError:
        return ""


def is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def owned_by_me(pid: int, token: str) -> bool:
    if pid <= 1 or pid == os.getpid():
        return False
    if _environ_token(pid) != token:
        return False
    return SELF in _cmdline(pid)


def identity_kill(pid: int, token: str) -> str:
    if not owned_by_me(pid, token):
        return "skipped(not-owned)"
    try:
        pidfd = os.pidfd_open(pid)
    except (ProcessLookupError, OSError) as e:
        return f"gone({e.__class__.__name__})"
    try:
        if not owned_by_me(pid, token):
            return "skipped(reuse)"
        try:
            signal.pidfd_send_signal(pidfd, signal.SIGKILL)
            return "killed"
        except ProcessLookupError:
            return "already-gone"
    finally:
        os.close(pidfd)


def sweep(token: str) -> list[tuple[int, str]]:
    out = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        pid = int(name)
        if owned_by_me(pid, token):
            out.append((pid, identity_kill(pid, token)))
    return out


# ------------------------------ the fork tree ----------------------------- #


def _become_sleeper(token: str, role: str, outdir: str) -> None:
    os.execv(sys.executable, [sys.executable, SELF, "sleeper", token, role, outdir])


def _write(outdir: str, name: str, value: str) -> None:
    with open(os.path.join(outdir, name), "w") as fh:
        fh.write(value)


def _own_cgroup() -> str | None:
    try:
        with open("/proc/self/cgroup") as fh:
            for line in fh:
                if line.startswith("0::"):
                    return line[3:].strip()
    except OSError:
        return None
    return None


def _double_fork_grandchild(token: str, outdir: str, variant: str) -> None:
    """In a child: setsid → NEW group, close stdio, double-fork the survivor."""
    os.setsid()
    devnull = os.open(os.devnull, os.O_RDWR)
    for fd in (0, 1, 2):
        os.dup2(devnull, fd)
    g = os.fork()
    if g != 0:
        os._exit(0)  # sandbox exits → grandchild is double-fork orphaned

    # grandchild (the survivor)
    if variant == "nested":
        # Migrate into a child cgroup of the delegated scope: the scope root's
        # cgroup.procs empties, but recursive cgroup.events populated stays 1.
        cg = _own_cgroup()
        if cg:
            leaf = "/sys/fs/cgroup" + cg + "/nested"
            try:
                os.mkdir(leaf)
                with open(leaf + "/cgroup.procs", "w") as fh:
                    fh.write(str(os.getpid()))
            except OSError:
                pass  # non-fatal: fall back to plain containment

    _write(outdir, "grandchild.pid", str(os.getpid()))
    _become_sleeper(token, "grandchild", outdir)


def _spawn(variant: str, argv: list[str]) -> int:
    token = os.environ["ESCAPER_TOKEN"]
    outdir = argv[0]
    child = os.fork()
    if child == 0:
        _double_fork_grandchild(token, outdir, variant)
        os._exit(0)  # unreachable
    # scope main: wait for the grandchild to materialize, reap the middle
    # child (so it is not a zombie), then exit — leaving the escapee behind.
    deadline = time.monotonic() + 5
    gc = os.path.join(outdir, "grandchild.pid")
    while time.monotonic() < deadline and not os.path.exists(gc):
        time.sleep(0.05)
    try:
        os.waitpid(child, 0)
    except ChildProcessError:
        pass
    return 0


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: lifecycle_escaper.py <cmd> ...", file=sys.stderr)
        return 2
    cmd = sys.argv[1]
    if cmd == "sleeper":
        time.sleep(_SLEEP)
        return 0
    if cmd == "spawn-escapee":
        return _spawn("plain", sys.argv[2:])
    if cmd == "spawn-escapee-nested":
        return _spawn("nested", sys.argv[2:])
    if cmd == "sweep":
        token = os.environ.get("ESCAPER_TOKEN") or (sys.argv[2] if len(sys.argv) > 2 else "")
        print(sweep(token))
        return 0
    print(f"unknown cmd {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
