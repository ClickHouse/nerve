"""The daemon lock, not the PID in the PID file, decides if the daemon runs."""

from __future__ import annotations

import fcntl
import os
import subprocess
import sys

import pytest

from nerve import cli, paths, pidfile


@pytest.fixture
def files(tmp_path):
    return tmp_path / "nerve.lock", tmp_path / "nerve.pid"


@pytest.fixture
def release_lock():
    yield
    if pidfile._lock_fd is not None:
        os.close(pidfile._lock_fd)
        pidfile._lock_fd = None


def _hold_lock(lock_path) -> int:
    """Lock through a new descriptor. flock treats it as a different holder."""
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT)
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd


def test_acquire_writes_own_pid(files, release_lock):
    lock, pid = files
    assert pidfile.acquire(lock, pid)
    assert pid.read_text() == str(os.getpid())
    assert pidfile.live_pid(lock, pid) == os.getpid()


def test_acquire_fails_while_the_lock_is_held(files):
    lock, pid = files
    pid.write_text("12345")
    fd = _hold_lock(lock)
    try:
        assert not pidfile.acquire(lock, pid)
        assert pid.read_text() == "12345"
        assert pidfile.live_pid(lock, pid) == 12345
    finally:
        os.close(fd)


def test_reused_pid_is_not_live(files):
    """After a reboot, the old daemon PID can belong to a different live process."""
    lock, pid = files
    lock.touch()
    pid.write_text("1")
    assert pidfile.pid_exists(1)
    assert pidfile.live_pid(lock, pid) is None


def test_killed_daemon_releases_the_lock(files):
    lock, pid = files
    child = subprocess.Popen(
        [
            sys.executable, "-c",
            "import sys, time\n"
            "from pathlib import Path\n"
            "from nerve import pidfile\n"
            "assert pidfile.acquire(Path(sys.argv[1]), Path(sys.argv[2]))\n"
            "print('locked', flush=True)\n"
            "time.sleep(60)\n",
            str(lock), str(pid),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "locked"
        assert pidfile.live_pid(lock, pid) == child.pid
    finally:
        child.kill()
        child.wait()
        child.stdout.close()

    assert pid.read_text() == str(child.pid)
    assert pidfile.live_pid(lock, pid) is None


def test_no_lock_file_falls_back_to_the_pid(files):
    """A daemon from a Nerve version without the lock file is still found."""
    lock, pid = files
    pid.write_text(str(os.getpid()))
    assert pidfile.live_pid(lock, pid) == os.getpid()

    exited = subprocess.Popen([sys.executable, "-c", "pass"])
    exited.wait()
    pid.write_text(str(exited.pid))
    assert pidfile.live_pid(lock, pid) is None


def test_missing_or_invalid_pid_file(files):
    lock, pid = files
    fd = _hold_lock(lock)
    try:
        assert pidfile.live_pid(lock, pid) is None
        pid.write_text("not-a-pid")
        assert pidfile.live_pid(lock, pid) is None
    finally:
        os.close(fd)


def test_daemon_status_ignores_a_reused_pid():
    paths.nerve_home().mkdir(parents=True)
    paths.lock_file().touch()
    paths.pid_file().write_text("1")
    assert cli._get_daemon_status() == (False, None)
