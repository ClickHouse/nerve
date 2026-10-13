"""One restart mechanism, reachable from the CLI and from the wizard.

``nerve restart`` and the setup wizard's last step have to do the same thing,
and the interesting part is that the process which must go down is the one
being asked: both hand the job to a detached helper. This pins that the helper
is what gets spawned, what it is told to start, that the systemd path stops
the process instead (the unit brings it back), and how the HTTP path decides
between the helper, a plain exit (container) and a refusal (systemd).
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

    def test_a_delay_is_honoured_before_anything_is_signalled(self, spawned, tmp_path):
        daemon.restart_daemon(tmp_path / "config", old_pid=4242, systemd=False, delay_seconds=0.75)
        script = spawned.call_args[0][0][2]
        assert "time.sleep(0.75)" in script
        assert script.index("time.sleep(0.75)") < script.index("SIGTERM")

    def test_nothing_is_killed_when_nothing_is_running(self, spawned, tmp_path):
        outcome = daemon.restart_daemon(tmp_path / "config", old_pid=None, systemd=False)
        assert "Starting Nerve" in outcome.message
        assert "old_pid = None" in spawned.call_args[0][0][2]

    def test_the_start_command_does_not_depend_on_how_we_were_invoked(self, tmp_path):
        command = daemon.start_command(tmp_path, verbose=True)
        assert command[:3] == [sys.executable, "-m", "nerve"]
        assert command[-2:] == ["start", "--foreground"]
        assert "-v" in command

    def test_a_failed_popen_propagates(self, tmp_path, monkeypatch):
        """Whether a restart *began* is knowable; a caller must not say
        "restarting" for a helper that never existed."""
        monkeypatch.setattr("nerve.paths.log_file", lambda: tmp_path / "nerve.log")
        with patch("nerve.daemon.subprocess.Popen", side_effect=OSError(12, "cannot fork")):
            with pytest.raises(OSError):
                daemon.restart_daemon(tmp_path / "config", old_pid=1, systemd=False)


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


class TestRestartMode:
    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        monkeypatch.delenv("INVOCATION_ID", raising=False)
        monkeypatch.delenv("NERVE_DOCKER", raising=False)
        monkeypatch.delenv(daemon.SUPERVISOR_ENV, raising=False)

    def test_plain_host_uses_the_helper(self):
        assert daemon.restart_mode() == "helper"

    def test_restart_mode_docker_is_exit(self, monkeypatch):
        monkeypatch.setenv("NERVE_DOCKER", "1")
        assert daemon.in_container() is True
        assert daemon.restart_mode() == "exit"

    def test_restart_mode_systemd_is_refuse(self, monkeypatch):
        monkeypatch.setenv("INVOCATION_ID", "0123456789abcdef")
        assert daemon.restart_mode() == "refuse"

    @pytest.mark.parametrize(("value", "mode"), [("exit", "exit"), ("refuse", "refuse"), ("REFUSE", "refuse")])
    def test_env_override(self, monkeypatch, value, mode):
        monkeypatch.setenv("INVOCATION_ID", "0123456789abcdef")  # would be refuse
        monkeypatch.setenv(daemon.SUPERVISOR_ENV, value)
        assert daemon.restart_mode() == mode

    def test_unit_name_never_raises(self, monkeypatch):
        assert daemon.systemd_unit_name().endswith(".service")


class TestSystemdUnit:
    """The unit is the cgroup *leaf*; a user manager gets ``--user``."""

    def test_system_service_leaf(self):
        unit, user = daemon._unit_from_cgroup("0::/system.slice/nerve-gateway.service\n")
        assert (unit, user) == ("nerve-gateway.service", False)

    def test_scope_under_a_user_manager_falls_back_to_the_default_unit(self):
        """A transient scope under user@1000.service: the '.service' segment up
        the path is the user *manager* — restarting it would tear down the
        operator's whole session, so it is never the answer."""
        text = "0::/user.slice/user-1000.slice/user@1000.service/app.slice/run-u123.scope\n"
        unit, user = daemon._unit_from_cgroup(text)
        assert unit == "nerve.service" and user is True

    def test_user_service_leaf_is_addressed_with_user(self, monkeypatch):
        text = "0::/user.slice/user-1000.slice/user@1000.service/app.slice/nerve.service\n"
        assert daemon._unit_from_cgroup(text) == ("nerve.service", True)
        monkeypatch.setattr(daemon, "_cgroup_text", lambda: text)
        assert daemon.systemd_restart_command() == "systemctl --user restart nerve.service"
        assert daemon.systemd_unit_name() == "nerve.service"
        assert daemon.systemd_user_manager() is True

    def test_system_service_command(self, monkeypatch):
        monkeypatch.setattr(daemon, "_cgroup_text", lambda: "0::/system.slice/nerve-gateway.service\n")
        assert daemon.systemd_restart_command() == "systemctl restart nerve-gateway.service"
        assert daemon.systemd_user_manager() is False

    def test_v1_lines_and_junk_are_tolerated(self):
        text = (
            "12:pids:/system.slice/nerve.service\n"
            "11:memory:/system.slice/nerve.service\n"
            "1:name=systemd:/system.slice/nerve.service\n"
            "\n"
            "garbage without colons\n"
        )
        assert daemon._unit_from_cgroup(text) == ("nerve.service", False)
        assert daemon._unit_from_cgroup("") == ("nerve.service", False)
        assert daemon._unit_from_cgroup("0::/\n") == ("nerve.service", False)

    def test_unreadable_cgroup_file_means_the_default(self, monkeypatch):
        monkeypatch.setattr(daemon, "_CGROUP_FILE", Path("/nonexistent/cgroup"))
        assert daemon.systemd_unit_name() == "nerve.service"
        assert daemon.systemd_restart_command() == "systemctl restart nerve.service"


class TestExitForSupervisor:
    def test_exit_for_supervisor_signals_self(self, monkeypatch):
        calls = []
        monkeypatch.setattr(daemon.os, "kill", lambda pid, sig: calls.append((pid, sig)))

        started = {}

        class _Timer:
            def __init__(self, delay, fn, args):
                started["delay"] = delay
                self._fn, self._args = fn, args
                self.daemon = False

            def start(self):
                self._fn(*self._args)

        monkeypatch.setattr(daemon.threading, "Timer", _Timer)
        outcome = daemon.exit_for_supervisor(0.75)
        assert outcome.method == "exit"
        assert started["delay"] == 0.75
        assert calls == [(os.getpid(), signal.SIGTERM)]


class TestReadingThePidFile:
    def test_a_missing_or_unreadable_file_is_not_a_pid(self, tmp_path, monkeypatch):
        monkeypatch.setattr("nerve.paths.pid_file", lambda: tmp_path / "nerve.pid")
        assert daemon.pid_file_pid() is None
        (tmp_path / "nerve.pid").write_text("not a number")
        assert daemon.pid_file_pid() is None

    def test_it_never_deletes_the_file_it_reads(self, tmp_path, monkeypatch):
        pid_file = tmp_path / "nerve.pid"
        monkeypatch.setattr("nerve.paths.pid_file", lambda: pid_file)
        pid_file.write_text("4242")
        assert daemon.pid_file_pid() == 4242
        assert pid_file.exists()

    def test_this_process_is_alive(self):
        assert daemon.process_is_alive(os.getpid()) is True
        assert daemon.process_is_alive(2**30) is False


class TestCliDelegates:
    def test_cli_restart_delegates_to_daemon(self, tmp_path, monkeypatch):
        """``nerve restart`` on a plain host calls the shared helper with the
        running pid, keeping its own compose and systemd branches."""
        from click.testing import CliRunner

        from nerve import cli

        (tmp_path / "config.yaml").write_text(f"workspace: {tmp_path / 'ws'}\n", encoding="utf-8")
        monkeypatch.delenv("INVOCATION_ID", raising=False)
        monkeypatch.setattr(cli, "_get_daemon_status", lambda: (True, 4242))
        called = {}

        def _fake(config_dir, **kwargs):
            called["config_dir"] = config_dir
            called.update(kwargs)
            return daemon.RestartOutcome(method="helper", message="Restarting Nerve (PID 4242)...", old_pid=4242)

        monkeypatch.setattr(daemon, "restart_daemon", _fake)
        result = CliRunner().invoke(cli.main, ["-c", str(tmp_path), "restart"])
        assert result.exit_code == 0, result.output
        assert "Restarting Nerve (PID 4242)" in result.output
        assert called["old_pid"] == 4242
        assert called["systemd"] is False
        assert str(called["config_dir"]) == str(tmp_path)

    def test_cli_restart_under_systemd_still_signals(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        from nerve import cli

        (tmp_path / "config.yaml").write_text(f"workspace: {tmp_path / 'ws'}\n", encoding="utf-8")
        monkeypatch.setenv("INVOCATION_ID", "0123456789abcdef")
        monkeypatch.setattr(cli, "_get_daemon_status", lambda: (True, 4242))
        killed = []
        monkeypatch.setattr(cli.os, "kill", lambda pid, sig: killed.append((pid, sig)))
        monkeypatch.setattr(daemon, "restart_daemon", lambda *a, **k: pytest.fail("helper must not run"))
        result = CliRunner().invoke(cli.main, ["-c", str(tmp_path), "restart"])
        assert result.exit_code == 0, result.output
        assert killed == [(4242, signal.SIGTERM)]
        assert "systemd will respawn" in result.output
