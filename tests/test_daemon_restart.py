"""One restart mechanism, reachable from the CLI and from the wizard.

``nerve restart`` and the setup wizard's last step have to do the same thing,
and the interesting part is that the process which must go down is the one
being asked: both hand the job to a detached helper. This pins that the helper
is what gets spawned, what it is told to start, and that the systemd path stops
the process instead (the unit brings it back).
"""

from __future__ import annotations

import os
import signal
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from nerve import daemon


@pytest.fixture
def spawned(tmp_path, monkeypatch):
    """Capture the detached helper instead of running it."""
    monkeypatch.setattr("nerve.paths.log_file", lambda: tmp_path / "nerve.log")
    popen = MagicMock()
    with patch("nerve.daemon.subprocess.Popen", popen):
        yield popen


class TestTheHelper:
    def test_it_is_detached_and_starts_the_daemon_again(self, spawned, tmp_path):
        outcome = daemon.restart_daemon(tmp_path / "config", old_pid=4242, systemd=False)

        assert outcome.method == "helper"
        assert "4242" in outcome.message
        assert spawned.call_count == 1
        argv, kwargs = spawned.call_args
        assert kwargs["start_new_session"] is True, (
            "the helper outlives the process it is about to kill"
        )
        script = argv[0][2]
        assert argv[0][:2] == [sys.executable, "-c"]
        assert "old_pid = 4242" in script
        assert "SIGTERM" in script and "SIGKILL" in script
        assert str(tmp_path / "config") in script
        assert "'start', '--foreground'" in script

    def test_nothing_is_killed_when_nothing_is_running(self, spawned, tmp_path):
        outcome = daemon.restart_daemon(tmp_path / "config", old_pid=None, systemd=False)
        assert "Starting Nerve" in outcome.message
        assert "old_pid = None" in spawned.call_args[0][0][2]

    def test_the_start_command_does_not_depend_on_how_we_were_invoked(self, tmp_path):
        command = daemon.start_command(tmp_path, verbose=True)
        assert command[:3] == [sys.executable, "-m", "nerve"]
        assert command[-2:] == ["start", "--foreground"]
        assert "-v" in command


class TestSystemd:
    def test_it_stops_the_process_and_lets_the_unit_do_the_rest(
        self, spawned, tmp_path,
    ):
        with patch("nerve.daemon.os.kill") as kill:
            outcome = daemon.restart_daemon(
                tmp_path / "config", old_pid=99, systemd=True,
            )
        kill.assert_called_once_with(99, signal.SIGTERM)
        assert outcome.method == "systemd"
        assert spawned.call_count == 0, "systemd needs no helper"

    def test_with_nothing_running_it_only_says_so(self, spawned, tmp_path):
        outcome = daemon.restart_daemon(tmp_path / "config", old_pid=None, systemd=True)
        assert outcome.old_pid is None
        assert spawned.call_count == 0

    def test_the_environment_decides_when_the_caller_does_not(self, monkeypatch):
        monkeypatch.delenv("INVOCATION_ID", raising=False)
        assert daemon.is_systemd_managed() is False
        monkeypatch.setenv("INVOCATION_ID", "0123456789abcdef")
        assert daemon.is_systemd_managed() is True


class TestReadingThePidFile:
    def test_a_missing_or_unreadable_file_is_not_a_pid(self, tmp_path, monkeypatch):
        monkeypatch.setattr("nerve.paths.pid_file", lambda: tmp_path / "nerve.pid")
        assert daemon.pid_file_pid() is None
        (tmp_path / "nerve.pid").write_text("not a number")
        assert daemon.pid_file_pid() is None

    def test_it_never_deletes_the_file_it_reads(self, tmp_path, monkeypatch):
        """Called from inside the daemon, where removing the pid file is how
        you lose track of the process you are."""
        pid_file = tmp_path / "nerve.pid"
        monkeypatch.setattr("nerve.paths.pid_file", lambda: pid_file)
        pid_file.write_text("4242")
        assert daemon.pid_file_pid() == 4242
        assert pid_file.exists()

    def test_this_process_is_alive(self):
        assert daemon.process_is_alive(os.getpid()) is True
        # A pid that cannot exist: the kernel's maximum is well under this.
        assert daemon.process_is_alive(2**30) is False
