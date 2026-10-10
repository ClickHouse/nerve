"""The slim ``nerve init``: host decisions at the terminal, the rest in the browser.

Driven through the click runner with the daemon spawn and the health wait
monkeypatched (a test must never start a gateway). What is pinned: the files
it writes, what it prints (URL and token once, never a URL carrying the
token, never the token as an argv element), the port probe, the bind
mapping, that ``--terminal`` and ``--non-interactive`` keep their old
behaviour and start nothing, and that a configured install prints status and
regenerates nothing.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from click.testing import CliRunner

from nerve import bootstrap, cli, paths
from nerve.config import load_config
from nerve.db import Database
from nerve.setup_token import SETUP_TOKEN_NAME

_TOKEN = "setup-token-REPLACE-0123456789abcdef"
_REAL_COPY = cli._copy_to_clipboard
_REAL_PORT_IS_FREE = bootstrap.port_is_free
_REAL_PROPOSE_PORT = bootstrap.propose_port


def _seed_token():
    """What the daemon does at its first boot: mint the setup token."""

    async def _run():
        db = Database(paths.db_path())
        await db.connect()
        try:
            await db._ensure_instance_secret(SETUP_TOKEN_NAME, _TOKEN)
        finally:
            await db.close()

    asyncio.run(_run())


@pytest.fixture
def no_daemon(monkeypatch):
    """The daemon never starts; /health "answers" and the token appears."""
    spawned: list[Path] = []

    def _spawn(config_dir, verbose, env=None):
        spawned.append(Path(config_dir))
        return SimpleNamespace(pid=4242, poll=lambda: None)

    def _health(url, timeout):
        _seed_token()
        # What `start --foreground` does first thing: record its own pid. The
        # slim init requires the pid file to name the process it spawned.
        paths.ensure_nerve_home()
        paths.pid_file().write_text("4242")
        return 1.2

    monkeypatch.setattr(cli, "_spawn_daemon", _spawn)
    monkeypatch.setattr(cli, "_wait_for_health", _health)
    monkeypatch.setattr(cli, "_get_daemon_status", lambda: (False, None))
    monkeypatch.setattr(bootstrap, "_ensure_web_ui", lambda config_dir: "present")
    monkeypatch.setattr(cli, "_copy_to_clipboard", lambda text: False)
    monkeypatch.setattr(cli.click, "launch", lambda url: pytest.fail("no browser in tests"))
    # The port probe is real only in the tests about it (``real_ports``): the
    # fixed 8977/8979 these tests pass must not fail because another process
    # on the developer's machine happens to listen there.
    monkeypatch.setattr(bootstrap, "port_is_free", lambda port, host="127.0.0.1": True)
    monkeypatch.setattr(bootstrap, "propose_port", lambda preferred, bind: (preferred, False))
    return spawned


@pytest.fixture
def real_ports(monkeypatch):
    """Undo ``no_daemon``'s port stubs for the tests that exercise the probe."""
    monkeypatch.setattr(bootstrap, "port_is_free", _REAL_PORT_IS_FREE)
    monkeypatch.setattr(bootstrap, "propose_port", _REAL_PROPOSE_PORT)


def _init(config_dir: Path, *args: str, input: str | None = None, env: dict | None = None):
    return CliRunner().invoke(
        cli.main, ["-c", str(config_dir), "init", *args], input=input, env=env,
        catch_exceptions=False,
    )


_FLAGS = ("--deployment", "server", "--bind", "localhost", "--no-browser", "--no-clipboard", "--yes")


class TestFlagsPath:
    def test_writes_the_slim_layout_and_prints_url_and_token(self, tmp_path, no_daemon):
        config_dir = tmp_path / "config"
        ws = tmp_path / "workspace"
        result = _init(config_dir, "--workspace", str(ws), "--port", "8977", *_FLAGS)
        assert result.exit_code == 0, result.output
        out = result.output
        assert "Setup page   http://127.0.0.1:8977/setup" in out
        assert f"Setup token  {_TOKEN}" in out
        assert "Nerve started (PID 4242)" in out
        assert no_daemon == [config_dir]
        machine = yaml.safe_load((config_dir / "config.yaml").read_text())
        assert machine["workspace"] == str(ws) and machine["deployment"] == "server"
        assert machine["telegram"] == {"enabled": False}
        settings = yaml.safe_load((ws / "config" / "settings.yaml").read_text())
        assert settings["gateway"] == {"host": "127.0.0.1", "port": 8977}
        assert settings["sync"]["github"]["enabled"] is False
        local = yaml.safe_load((config_dir / "config.local.yaml").read_text())
        assert set(local) == {"auth"} and local["auth"]["jwt_secret"]
        assert not (ws / "SOUL.md").exists(), "mode templates belong to the browser step"
        assert not (ws / "config" / "cron" / "system.yaml").exists()
        assert (ws / "skills").is_dir() and (ws / "config" / "cron" / "gates").is_dir()
        assert (paths.config_pointer_file()).read_text().strip() == str(config_dir)
        assert not bootstrap._init_state_file().exists(), "the checkpoint is cleared"
        # The owner account exists before the gateway serves.
        from nerve.db.accounts import inspect_bootstrap_state

        assert inspect_bootstrap_state(paths.db_path())[0] == ["none"]
        # And a re-run is a no-op that prints status.
        config = load_config(config_dir)
        assert config.gateway.port == 8977

    def test_token_never_in_url_or_argv(self, tmp_path, no_daemon, monkeypatch):
        config_dir = tmp_path / "config"
        seen: list[tuple[list[str], bytes]] = []

        def _run(argv, **kwargs):
            seen.append((list(argv), kwargs.get("input", b"")))
            return SimpleNamespace(returncode=0)

        monkeypatch.setattr(cli, "_copy_to_clipboard", _REAL_COPY)
        monkeypatch.setattr(cli.subprocess, "run", _run)
        monkeypatch.setattr(cli.shutil, "which", lambda name: "/usr/bin/pbcopy" if name == "pbcopy" else None)
        result = _init(config_dir, "--workspace", str(tmp_path / "ws"), "--port", "8977",
                       "--deployment", "server", "--bind", "localhost", "--no-browser", "--yes")
        assert result.exit_code == 0, result.output
        assert "(copied to your clipboard)" in result.output
        assert f"/setup?{_TOKEN}" not in result.output and f"setup_token={_TOKEN}" not in result.output
        assert len(seen) == 1
        argv, stdin = seen[0]
        assert argv == ["pbcopy"]
        assert _TOKEN not in " ".join(argv)
        assert stdin == _TOKEN.encode()
        assert result.output.count(_TOKEN) == 1

    def test_no_clipboard_skips_the_copy(self, tmp_path, no_daemon, monkeypatch):
        config_dir = tmp_path / "config"
        monkeypatch.setattr(cli, "_copy_to_clipboard", lambda text: pytest.fail("must not copy"))
        result = _init(config_dir, "--workspace", str(tmp_path / "ws"), "--port", "8977", *_FLAGS)
        assert result.exit_code == 0, result.output
        assert "copied to your clipboard" not in result.output

    def test_bind_lan_listens_everywhere(self, tmp_path, no_daemon):
        config_dir = tmp_path / "config"
        ws = tmp_path / "ws"
        result = _init(config_dir, "--workspace", str(ws), "--port", "8977", "--deployment", "server",
                       "--bind", "lan", "--no-browser", "--no-clipboard", "--yes")
        assert result.exit_code == 0, result.output
        settings = yaml.safe_load((ws / "config" / "settings.yaml").read_text())
        assert settings["gateway"]["host"] == "0.0.0.0"
        assert "Setup page   http://localhost:8977/setup" in result.output

    def test_no_start_writes_files_only(self, tmp_path, no_daemon):
        config_dir = tmp_path / "config"
        result = _init(config_dir, "--workspace", str(tmp_path / "ws"), "--port", "8977", "--no-start", *_FLAGS)
        assert result.exit_code == 0, result.output
        assert no_daemon == []
        assert "Not started (--no-start)" in result.output
        assert _TOKEN not in result.output
        assert (config_dir / "config.local.yaml").exists()

    def test_health_timeout_is_a_clear_failure(self, tmp_path, no_daemon, monkeypatch):
        monkeypatch.setattr(cli, "_wait_for_health", lambda url, timeout: None)
        result = CliRunner().invoke(
            cli.main, ["-c", str(tmp_path / "config"), "init", "--workspace", str(tmp_path / "ws"),
                       "--port", "8977", *_FLAGS],
        )
        assert result.exit_code != 0
        assert "did not answer within 60 s" in result.output
        assert "nerve logs" in result.output
        # The files exist by now, so a re-run would only print status: the
        # message points at `nerve start` + `nerve status`, not at a re-run.
        assert "nerve start" in result.output and "nerve status" in result.output
        assert "Re-run nerve init" not in result.output

    def test_a_busy_explicit_port_is_refused_before_anything_starts(self, tmp_path, no_daemon, real_ports):
        config_dir = tmp_path / "config"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as busy:
            busy.bind(("127.0.0.1", 0))
            port = busy.getsockname()[1]
            busy.listen(1)
            result = CliRunner().invoke(
                cli.main, ["-c", str(config_dir), "init", "--workspace", str(tmp_path / "ws"),
                           "--port", str(port), *_FLAGS],
            )
        assert result.exit_code != 0
        assert f"Port {port} is already in use by another program; pick another with --port" in result.output
        assert no_daemon == [], "nothing was spawned"
        assert _TOKEN not in result.output and "Nerve started" not in result.output
        assert not (config_dir / "config.local.yaml").exists(), "refused before the files were written"

    def test_health_from_another_process_is_not_a_success(self, tmp_path, no_daemon, monkeypatch):
        """/health green but the pid file names another process (the child
        died on a busy port): a clear failure, no token block."""
        def _health(url, timeout):
            _seed_token()
            paths.ensure_nerve_home()
            paths.pid_file().write_text("999999")
            return 0.1

        monkeypatch.setattr(cli, "_wait_for_health", _health)
        result = CliRunner().invoke(
            cli.main, ["-c", str(tmp_path / "config"), "init", "--workspace", str(tmp_path / "ws"),
                       "--port", "8977", *_FLAGS],
        )
        assert result.exit_code != 0
        assert "did not start cleanly" in result.output and "something else answers" in result.output
        assert _TOKEN not in result.output and "Nerve started" not in result.output

    def test_a_dead_child_is_not_a_success(self, tmp_path, no_daemon, monkeypatch):
        monkeypatch.setattr(
            cli, "_spawn_daemon",
            lambda config_dir, verbose, env=None: SimpleNamespace(pid=4242, poll=lambda: 1, returncode=1),
        )
        result = CliRunner().invoke(
            cli.main, ["-c", str(tmp_path / "config"), "init", "--workspace", str(tmp_path / "ws"),
                       "--port", "8977", *_FLAGS],
        )
        assert result.exit_code != 0
        assert "exited with code 1" in result.output and _TOKEN not in result.output

    def test_shell_credentials_are_not_passed_to_the_daemon(self, tmp_path, no_daemon, monkeypatch):
        seen: dict = {}

        def _spawn(config_dir, verbose, env=None):
            seen["env"] = env
            return SimpleNamespace(pid=4242, poll=lambda: None)

        monkeypatch.setattr(cli, "_spawn_daemon", _spawn)
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-REPLACE-shell")
        result = _init(tmp_path / "config", "--workspace", str(tmp_path / "ws"), "--port", "8977", *_FLAGS)
        assert result.exit_code == 0, result.output
        assert "ANTHROPIC_API_KEY is set in this shell; not passed to Nerve" in result.output
        assert seen["env"] is not None and "ANTHROPIC_API_KEY" not in seen["env"]
        assert "PATH" in seen["env"], "the rest of the environment is kept"
        assert "sk-ant-REPLACE-shell" not in result.output

    def test_keep_env_passes_the_shell_credential_through(self, tmp_path, no_daemon, monkeypatch):
        seen: dict = {}

        def _spawn(config_dir, verbose, env=None):
            seen["env"] = env
            return SimpleNamespace(pid=4242, poll=lambda: None)

        monkeypatch.setattr(cli, "_spawn_daemon", _spawn)
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-REPLACE-shell")
        result = _init(tmp_path / "config", "--workspace", str(tmp_path / "ws"), "--port", "8977", "--keep-env", *_FLAGS)
        assert result.exit_code == 0, result.output
        assert seen["env"] is None, "inherit the environment untouched"
        assert "not passed to Nerve" not in result.output

    def test_yes_prints_one_using_line_and_no_empty_headers(self, tmp_path, no_daemon):
        ws = tmp_path / "ws"
        result = _init(tmp_path / "config", "--workspace", str(ws), "--port", "8977", *_FLAGS)
        assert result.exit_code == 0, result.output
        assert f"Using: server · workspace {ws} · port 8977 · localhost" in result.output
        assert "Step 1 of 3" not in result.output
        assert "Step 2 of 3" not in result.output
        assert "Step 3 of 3" not in result.output

    def test_external_mode_shell_prints_no_token_block(self, tmp_path, no_daemon, monkeypatch):
        config_dir = tmp_path / "config"
        result = _init(config_dir, "--workspace", str(tmp_path / "ws"), "--port", "8979", *_FLAGS,
                       env={"NERVE_AUTH_MODE": "external"})
        assert result.exit_code == 0, result.output
        assert "Setup page   http://127.0.0.1:8979/setup" in result.output
        # Two clauses on two lines, never a sentence broken mid-clause.
        assert "Sign in through your gateway." in result.output
        assert "No setup token is used in external mode." in result.output
        assert _TOKEN not in result.output
        # There is no token in this mode, so the epilogue does not promise one.
        assert "Later: `nerve status` prints the page again." in result.output
        assert "and token" not in result.output


class TestStdinPath:
    def test_answers_are_read_from_stdin(self, tmp_path, no_daemon):
        config_dir = tmp_path / "config"
        ws = tmp_path / "ws"
        result = _init(
            config_dir, "--no-browser", "--no-clipboard", "--port", "8977",
            input=f"server\n{ws}\nlocalhost\n",
        )
        assert result.exit_code == 0, result.output
        assert "Step 1 of 3" in result.output and "Step 2 of 3" in result.output and "Step 3 of 3" in result.output
        assert yaml.safe_load((config_dir / "config.yaml").read_text())["workspace"] == str(ws)

    def test_choice_prompts_list_the_options_once(self, tmp_path, no_daemon):
        """``Choose [server/docker]`` names the options; click must not add
        ``(server, docker)`` after it."""
        result = _init(
            tmp_path / "config", "--no-browser", "--no-clipboard", "--port", "8977",
            input=f"server\n{tmp_path / 'ws'}\nlocalhost\n",
        )
        assert result.exit_code == 0, result.output
        assert "Choose [server/docker]" in result.output
        assert "(server, docker)" not in result.output
        assert "Reachable from" in result.output
        assert "(localhost, lan)" not in result.output

    def test_a_busy_port_proposes_the_next_free_one(self, tmp_path, no_daemon, real_ports):
        config_dir = tmp_path / "config"
        ws = tmp_path / "ws"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as busy:
            busy.bind(("127.0.0.1", 0))
            port = busy.getsockname()[1]
            busy.listen(1)
            proposed, was_busy = bootstrap.propose_port(port, "localhost")
            assert was_busy and proposed != port
            # Accept the proposal from the prompt's default.
            result = _init(
                config_dir, "--no-browser", "--no-clipboard", "--deployment", "server",
                "--workspace", str(ws), "--bind", "localhost",
                input="\n",
            )
        assert result.exit_code == 0, result.output
        if port == 8900:  # pragma: no cover - only on a box running the live gateway
            assert "Port 8900 is already in use" in result.output

    def test_ctrl_c_keeps_the_answers(self, tmp_path, no_daemon, monkeypatch):
        config_dir = tmp_path / "config"
        answers = iter(["server", str(tmp_path / "ws")])

        def _prompt(label, **kwargs):
            try:
                return next(answers)
            except StopIteration:
                raise KeyboardInterrupt

        monkeypatch.setattr(bootstrap.click, "prompt", _prompt)
        result = CliRunner().invoke(cli.main, ["-c", str(config_dir), "init", "--no-browser", "--no-clipboard"])
        assert result.exit_code == 130
        assert "answers are saved" in result.output
        saved = bootstrap._load_init_state()
        assert set(saved["completed"]) == {"slim:deployment", "slim:workspace"}
        assert saved["choices"]["workspace_path"] == str(tmp_path / "ws")
        assert not (config_dir / "config.local.yaml").exists()


class TestOtherPathsAreUntouched:
    def test_terminal_invokes_the_full_wizard_and_starts_nothing(self, tmp_path, no_daemon, monkeypatch):
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        ws = tmp_path / "ws"
        ran = []

        class StubWizard:
            def __init__(self, cfg_dir, inside_docker=False):
                self.config_dir = cfg_dir

            def run(self):
                ran.append(True)
                (self.config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
                (self.config_dir / "config.local.yaml").write_text("auth:\n  jwt_secret: x\n", encoding="utf-8")
                choices = bootstrap.SetupChoices()
                choices.user_name = "alice"
                return choices

            def checkpoint(self):
                return True

        monkeypatch.setattr("nerve.bootstrap.SetupWizard", StubWizard)
        result = _init(config_dir, "--terminal")
        assert result.exit_code == 0, result.output
        assert ran == [True]
        assert no_daemon == []
        assert "Setup page   " not in result.output

    def test_non_interactive_is_unchanged_and_starts_nothing(self, tmp_path, no_daemon, monkeypatch):
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        monkeypatch.setattr(bootstrap.SetupWizard, "_build_web_ui", lambda self: None)
        monkeypatch.setattr(bootstrap.shutil, "which", lambda name: None)
        result = _init(
            config_dir, "--non-interactive",
            env={"ANTHROPIC_API_KEY": "sk-ant-REPLACE", "NERVE_WORKSPACE": str(tmp_path / "ws"),
                 "NERVE_PASSWORD": "x", "NERVE_TIMEZONE": "UTC"},
        )
        assert result.exit_code == 0, result.output
        assert no_daemon == []
        assert (tmp_path / "ws" / "SOUL.md").exists()
        assert (tmp_path / "ws" / "config" / "cron" / "system.yaml").exists()
        assert "Setup complete." in result.output
        from nerve.db.accounts import read_setup_required

        assert read_setup_required(paths.db_path(), configured_password=True) is False

    def test_start_foreground_on_a_fresh_install_runs_setup_then_serves_here(self, tmp_path, no_daemon, monkeypatch):
        """`nerve start -f` on a fresh install: the slim init writes the files
        without starting anything, and *this* process becomes the server."""
        served: list = []
        monkeypatch.setattr("nerve.gateway.server.run_server", lambda config: served.append(config))
        monkeypatch.setattr(cli, "_get_daemon_status", lambda: (False, None))
        config_dir = tmp_path / "config"
        result = CliRunner().invoke(
            cli.main, ["-c", str(config_dir), "start", "-f"],
            input=f"server\n{tmp_path / 'ws'}\n8977\nlocalhost\n", catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        assert no_daemon == [], "no detached daemon from a foreground start"
        assert len(served) == 1 and served[0].gateway.port == 8977
        assert "Setup page: http://127.0.0.1:8977/setup" in result.output
        assert "nerve status" in result.output
        assert _TOKEN not in result.output, "the token exists only once the gateway booted"
        assert "already running" not in result.output
        assert (config_dir / "config.local.yaml").exists()

    def test_configured_install_prints_status_and_regenerates_nothing(self, tmp_path, no_daemon):
        config_dir = tmp_path / "config"
        first = _init(config_dir, "--workspace", str(tmp_path / "ws"), "--port", "8977", *_FLAGS)
        assert first.exit_code == 0, first.output
        local_before = (config_dir / "config.local.yaml").read_bytes()
        again = _init(config_dir, "--workspace", str(tmp_path / "other"), "--port", "9000", *_FLAGS)
        assert again.exit_code == 0, again.output
        assert "Nerve is already configured." in again.output
        assert "Setup page: http://127.0.0.1:8977/setup" in again.output
        assert f"Setup token: {_TOKEN}" in again.output
        assert "Reconfigure everything: nerve init --terminal" in again.output
        assert (config_dir / "config.local.yaml").read_bytes() == local_before
        assert yaml.safe_load((config_dir / "config.yaml").read_text())["workspace"] == str(tmp_path / "ws")
        assert len(no_daemon) == 1


def _claimed_db():
    """Claim the account the installer created: setup is not required any more."""
    from nerve.gateway.auth import hash_password

    async def _run():
        db = Database(paths.db_path())
        await db.connect()
        try:
            await db.claim_sole_account(
                username="alice", credential=hash_password("correct horse battery staple"),
                display_name=None, invalidate_secret_name=SETUP_TOKEN_NAME,
            )
        finally:
            await db.close()

    asyncio.run(_run())


class TestStatusAndDoctor:
    def test_status_prints_the_checklist_line_once_claimed(self, tmp_path, no_daemon, monkeypatch):
        config_dir = tmp_path / "config"
        assert _init(config_dir, "--workspace", str(tmp_path / "ws"), "--port", "8977", "--no-start", *_FLAGS).exit_code == 0
        _claimed_db()
        result = CliRunner().invoke(cli.main, ["-c", str(config_dir), "status"])
        assert result.exit_code == 0, result.output
        # Everything but the claim, including the steps that wait for Mode.
        assert "Setup checklist: 8 steps left — http://127.0.0.1:8977/setup" in result.output
        assert "Setup token" not in result.output

    def test_status_keeps_the_token_lines_while_unclaimed(self, tmp_path, no_daemon):
        config_dir = tmp_path / "config"
        assert _init(config_dir, "--workspace", str(tmp_path / "ws"), "--port", "8977", *_FLAGS).exit_code == 0
        result = CliRunner().invoke(cli.main, ["-c", str(config_dir), "status"])
        assert result.exit_code == 0, result.output
        assert "Setup page: http://127.0.0.1:8977/setup" in result.output
        assert f"Setup token: {_TOKEN}" in result.output
        assert "Setup checklist:" not in result.output

    def test_doctor_prints_checklist_incomplete_and_never_the_token(self, tmp_path, no_daemon):
        config_dir = tmp_path / "config"
        assert _init(config_dir, "--workspace", str(tmp_path / "ws"), "--port", "8977", *_FLAGS).exit_code == 0
        report = cli.doctor_report(load_config(config_dir))
        assert "[WARN] Setup checklist incomplete (steps: claim, mode, provider, memory, channels)" in report
        assert "finish at /setup" in report
        assert _TOKEN not in report
        assert "no mode chosen yet" in report

    def test_doctor_reports_complete_and_a_telegram_boot_error(self, tmp_path, no_daemon, monkeypatch):
        from nerve import setup_state
        from nerve.setup_writer import apply_mode, merge_private_paths, merge_settings_paths

        config_dir = tmp_path / "config"
        ws = tmp_path / "ws"
        assert _init(config_dir, "--workspace", str(ws), "--port", "8977", "--no-start", *_FLAGS).exit_code == 0
        _claimed_db()
        apply_mode(ws, "personal")
        merge_private_paths(config_dir / "config.local.yaml", {"anthropic_api_key": "sk-ant-REPLACE0000000000"})
        merge_settings_paths(ws, {"timezone": "UTC"})
        state = setup_state.SetupState(skipped={"memory", "channels", "sources", "agents"}, answered={"automation"})
        setup_state.save_state(state)
        report = cli.doctor_report(load_config(config_dir))
        assert "[OK] Setup checklist complete" in report
        setup_state.record_boot_error("telegram", "InvalidToken: The token <token> was rejected by the server.")
        report = cli.doctor_report(load_config(config_dir))
        assert "Telegram bot failed to start at the last boot: InvalidToken" in report
        assert "<token>" in report


class TestDockerSlim:
    @pytest.fixture
    def docker_host(self, monkeypatch, no_daemon):
        compose_calls: list[list[str]] = []
        monkeypatch.setattr(cli, "_docker_compose", lambda config_dir, args, **k: compose_calls.append(args) or 0)
        monkeypatch.setattr(bootstrap, "_resolve_claude_credential", lambda: ("", "none", []))
        monkeypatch.setattr(bootstrap, "_resolve_gh_token", lambda: ("", "none"))
        monkeypatch.delenv("NERVE_DOCKER", raising=False)
        return compose_calls

    def test_docker_init_writes_templates_with_port_and_bind_and_publishes_the_port(
        self, tmp_path, docker_host, monkeypatch,
    ):
        config_dir = tmp_path / "config"
        ws = tmp_path / "ws"
        result = _init(config_dir, "--deployment", "docker", "--workspace", str(ws), "--port", "8977",
                       "--bind", "localhost", "--no-browser", "--no-clipboard", "--yes")
        assert result.exit_code == 0, result.output
        assert docker_host == [["up", "-d", "--build"]]
        compose = (config_dir / "docker-compose.yml").read_text()
        assert '"127.0.0.1:8977:8977"' in compose
        dockerfile = (config_dir / "Dockerfile").read_text()
        assert "EXPOSE 8977" in dockerfile and "http://localhost:8977/health" in dockerfile
        machine = yaml.safe_load((config_dir / "config.yaml").read_text())
        assert machine["workspace"] == "/root/nerve-workspace"
        assert machine["gateway"] == {"host": "0.0.0.0", "port": 8977}
        assert machine["docker"] == {"extra_mounts": []}
        settings = yaml.safe_load((ws / "config" / "settings.yaml").read_text())
        assert settings["gateway"] == {"host": "0.0.0.0", "port": 8977}
        assert "restart: unless-stopped" in result.output
        assert f"Setup token  {_TOKEN}" in result.output

    def test_status_prints_the_published_port(self, tmp_path, docker_host, monkeypatch):
        """On the host the config loads with the container workspace path, so
        the port must come from config.yaml — never fall back to 8900."""
        config_dir = tmp_path / "config"
        result = _init(config_dir, "--deployment", "docker", "--workspace", str(tmp_path / "ws"), "--port", "8977",
                       "--bind", "localhost", "--no-browser", "--no-clipboard", "--yes")
        assert result.exit_code == 0, result.output
        status = CliRunner().invoke(cli.main, ["-c", str(config_dir), "status"])
        assert status.exit_code == 0, status.output
        assert ":8977/setup" in status.output
        assert "8900" not in status.output

    def test_status_derives_the_checklist_from_the_host_workspace(self, tmp_path, docker_host, monkeypatch):
        """config.yaml names the *container* workspace (/root/nerve-workspace),
        which does not exist on the host. The checklist is derived against the
        bind-mounted host path from docker-compose.yml instead — otherwise a
        finished Docker install would read "3 steps left" forever."""
        from nerve import setup_state
        from nerve.setup_writer import apply_mode, merge_private_paths, merge_settings_paths

        config_dir = tmp_path / "config"
        ws = tmp_path / "ws"
        assert _init(config_dir, "--deployment", "docker", "--workspace", str(ws), "--port", "8977",
                     "--bind", "localhost", "--no-browser", "--no-clipboard", "--yes").exit_code == 0
        assert yaml.safe_load((config_dir / "config.yaml").read_text())["workspace"] == "/root/nerve-workspace"
        _claimed_db()
        status = CliRunner().invoke(cli.main, ["-c", str(config_dir), "status"])
        assert status.exit_code == 0, status.output
        assert "Setup checklist: 8 steps left — http://localhost:8977/setup" in status.output
        # Finish the wizard on disk, at the host path.
        apply_mode(ws, "personal")
        merge_private_paths(config_dir / "config.local.yaml", {"anthropic_api_key": "sk-ant-REPLACE0000000000"})
        merge_settings_paths(ws, {"timezone": "UTC"})
        setup_state.save_state(setup_state.SetupState(
            skipped={"memory", "channels", "sources", "agents"}, answered={"automation"},
        ))
        status = CliRunner().invoke(cli.main, ["-c", str(config_dir), "status"])
        assert status.exit_code == 0, status.output
        assert "Setup checklist" not in status.output, status.output
        assert "steps left" not in status.output
        report = cli.doctor_report(load_config(config_dir))
        assert "[OK] Setup checklist complete" in report

    def test_status_without_a_readable_mount_prints_a_neutral_line(self, tmp_path, docker_host, monkeypatch):
        config_dir = tmp_path / "config"
        assert _init(config_dir, "--deployment", "docker", "--workspace", str(tmp_path / "ws"), "--port", "8977",
                     "--bind", "localhost", "--no-browser", "--no-clipboard", "--yes").exit_code == 0
        _claimed_db()
        (config_dir / "docker-compose.yml").unlink()
        status = CliRunner().invoke(cli.main, ["-c", str(config_dir), "status"])
        assert status.exit_code == 0, status.output
        assert "Setup checklist: open http://localhost:8977/setup" in status.output
        assert "steps left" not in status.output
        report = cli.doctor_report(load_config(config_dir))
        assert "[--] Setup checklist: not derived here" in report
        assert "steps:" not in report

    def test_gh_token_is_forwarded_only_after_confirmation(self, tmp_path, docker_host, monkeypatch):
        """The operator's personal GitHub token lands in the instance's secret
        file only after the same confirmation the Claude login gets; "n" keeps
        it out, and the value itself is never printed."""
        token = "ghp_REPLACE0000000000000000000000000000"
        monkeypatch.setattr(bootstrap, "_resolve_gh_token", lambda: (token, "gh CLI"))
        config_dir = tmp_path / "config"
        result = _init(config_dir, "--deployment", "docker", "--workspace", str(tmp_path / "ws"), "--port", "8977",
                       "--bind", "localhost", "--no-browser", "--no-clipboard", input="n\n")
        assert result.exit_code == 0, result.output
        assert "Found a GitHub login (gh CLI). Forward it to the container?" in result.output
        assert "github_token" not in (config_dir / "config.local.yaml").read_text()
        assert "Forwarding the GitHub token" not in result.output
        assert token not in result.output

    def test_yes_forwards_the_gh_token_and_says_so_without_the_value(self, tmp_path, docker_host, monkeypatch):
        token = "ghp_REPLACE0000000000000000000000000000"
        monkeypatch.setattr(bootstrap, "_resolve_gh_token", lambda: (token, "gh CLI"))
        config_dir = tmp_path / "config"
        result = _init(config_dir, "--deployment", "docker", "--workspace", str(tmp_path / "ws"), "--port", "8977",
                       "--bind", "localhost", "--no-browser", "--no-clipboard", "--yes")
        assert result.exit_code == 0, result.output
        assert yaml.safe_load((config_dir / "config.local.yaml").read_text())["github_token"] == token
        assert "✓ Forwarding the GitHub token (gh CLI) as github_token in config.local.yaml" in result.output
        assert token not in result.output

    def test_existing_compose_with_another_port_is_not_overwritten(self, tmp_path, docker_host):
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "docker-compose.yml").write_text('services:\n  nerve:\n    ports:\n      - "8900:8900"\n')
        result = _init(config_dir, "--deployment", "docker", "--workspace", str(tmp_path / "ws"), "--port", "8977",
                       "--bind", "lan", "--no-browser", "--no-clipboard", "--yes")
        assert result.exit_code == 0, result.output
        assert '"8900:8900"' in (config_dir / "docker-compose.yml").read_text()
        assert "does not publish port 8977" in result.output


class TestHelpers:
    def test_port_is_free_sees_through_time_wait(self):
        """A listener that just closed its connections leaves the port in
        TIME_WAIT for up to a minute: a plain bind says EADDRINUSE although
        nothing listens — exactly a re-run of `nerve init` after `nerve stop`.
        The probe must answer as the server's own SO_REUSEADDR bind would."""
        import errno

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        client = socket.create_connection(("127.0.0.1", port))
        accepted, _peer = listener.accept()
        accepted.close()  # the server side closes first: its socket enters TIME_WAIT
        client.close()
        listener.close()
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as plain:
                plain.bind(("127.0.0.1", port))
        except OSError as exc:
            assert exc.errno == errno.EADDRINUSE
        else:
            pytest.skip("this kernel lets a plain bind reuse a TIME_WAIT port; nothing to see through")
        assert bootstrap.port_is_free(port) is True
        assert bootstrap.propose_port(port, "localhost") == (port, False)

    def test_port_is_free_still_refuses_a_live_listener(self):
        """SO_REUSEADDR must not make a wildcard listener invisible to a
        loopback probe (BSD lets a specific bind over a wildcard one): the
        connect probe sees the listener on either address."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as busy:
            busy.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # what uvicorn sets
            busy.bind(("0.0.0.0", 0))
            busy.listen(1)
            port = busy.getsockname()[1]
            assert bootstrap.port_is_free(port, "127.0.0.1") is False
            assert bootstrap.port_is_free(port, "0.0.0.0") is False
            proposed, was_busy = bootstrap.propose_port(port, "lan")
            assert was_busy and proposed != port

    def test_load_config_reads_the_settings_at_an_explicit_workspace(self, tmp_path):
        """The host side of a Docker deployment: config.yaml names the container
        path; the tracked settings live at the bind-mounted host path."""
        from nerve.setup_writer import SetupChoices, merge_settings_paths, write_slim_install

        config_dir = tmp_path / "config"
        host_ws = tmp_path / "host-ws"
        write_slim_install(
            SetupChoices(deployment="docker", workspace_path=Path("/root/nerve-workspace"),
                         gateway_host="0.0.0.0", gateway_port=8977),
            config_dir, host_workspace=host_ws,
        )
        merge_settings_paths(host_ws, {"timezone": "UTC"})
        container_view = load_config(config_dir)
        assert str(container_view.workspace) == "/root/nerve-workspace"
        assert container_view.timezone != "UTC", "the container path has no settings file on the host"
        host_view = load_config(config_dir, workspace=host_ws)
        assert host_view.workspace == host_ws
        assert host_view.timezone == "UTC"
        assert host_view.gateway.port == 8977 and host_view.deployment == "docker"

    def test_wait_for_health_polls_until_200(self, monkeypatch):
        import urllib.request

        answers = iter([OSError("refused"), SimpleNamespace(status=200)])

        class _Ctx:
            def __init__(self, value):
                self.value = value

            def __enter__(self):
                if isinstance(self.value, Exception):
                    raise self.value
                return self.value

            def __exit__(self, *a):
                return False

        monkeypatch.setattr(urllib.request, "urlopen", lambda url, timeout: _Ctx(next(answers)))
        monkeypatch.setattr(cli.time, "sleep", lambda s: None)
        assert cli._wait_for_health("http://127.0.0.1:8977/health", timeout=5) is not None

    def test_build_templates_defaults_reproduce_todays_bytes(self):
        assert bootstrap._build_dockerfile() == bootstrap._DOCKERFILE_TEMPLATE
        assert '"8900:8900"' in bootstrap._build_docker_compose("~/ws")
        assert "127.0.0.1:" not in bootstrap._build_docker_compose("~/ws")
