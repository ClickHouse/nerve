"""Daemon liveness through a lock file.

The daemon holds an exclusive ``flock`` on the lock file for its whole life
and writes its PID to the PID file. The kernel releases the lock when the
process exits for any reason, including SIGKILL and a reboot. Thus the lock,
not the PID, tells if the daemon is running. A PID file that a dead daemon
left behind cannot block a start, and a PID that the system gave to a
different process cannot look like a live daemon.

Do not delete the lock file. Two processes could then lock two different
files at the same path.
"""

from __future__ import annotations

import os
from pathlib import Path

# The descriptor that holds the lock. It stays open until the process exits.
_lock_fd: int | None = None


def pid_exists(pid: int) -> bool:
    """Check if a process with the given PID is alive."""
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # Process exists but we can't signal it


def acquire(lock_path: Path, pid_path: Path) -> bool:
    """Lock ``lock_path`` and write the PID of this process to ``pid_path``.

    Returns False if the lock is held.
    """
    import fcntl

    global _lock_fd
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return False
    _lock_fd = fd
    pid_path.write_text(str(os.getpid()))
    return True


def live_pid(lock_path: Path, pid_path: Path) -> int | None:
    """Return the daemon PID if the daemon is running, else None."""
    import fcntl

    try:
        pid = int(pid_path.read_text().strip())
    except (OSError, ValueError):
        return None
    try:
        fd = os.open(lock_path, os.O_RDONLY)
    except FileNotFoundError:
        # A daemon from a Nerve version without the lock file holds no lock.
        # Until a daemon that takes the lock starts, the PID is the only data.
        return pid if pid_exists(pid) else None
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    except BlockingIOError:
        return pid
    finally:
        os.close(fd)
    return None
