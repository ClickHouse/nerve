"""Tests for multi-source config resolution.

workspace/config/settings.yaml (shareable) is merged underneath config.yaml
(machine base) and config.local.yaml (machine secrets/overrides).
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from nerve.config import (
    ConfigError,
    load_config,
    load_mcp_servers,
    workspace_config_dir,
    workspace_settings_file,
)
from nerve.workspace import install_config_scaffold


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _setup(tmp_path: Path):
    """Return (config_dir, workspace) with the workspace wired into config.yaml."""
    config_dir = tmp_path / "cfg"
    workspace = tmp_path / "ws"
    config_dir.mkdir(parents=True, exist_ok=True)
    workspace.mkdir(parents=True, exist_ok=True)
    return config_dir, workspace


class TestMultiSourceMerge:
    def test_workspace_settings_applied(self, tmp_path):
        config_dir, workspace = _setup(tmp_path)
        _write(config_dir / "config.yaml", f"workspace: {workspace}\n")
        _write(workspace_settings_file(workspace), "timezone: Europe/Amsterdam\n")

        cfg = load_config(config_dir)
        assert cfg.timezone == "Europe/Amsterdam"

    def test_config_yaml_overrides_workspace_settings(self, tmp_path):
        config_dir, workspace = _setup(tmp_path)
        _write(
            config_dir / "config.yaml",
            f"workspace: {workspace}\ntimezone: America/New_York\n",
        )
        _write(workspace_settings_file(workspace), "timezone: Europe/Amsterdam\n")

        cfg = load_config(config_dir)
        # Machine config.yaml wins over shared settings.
        assert cfg.timezone == "America/New_York"

    def test_local_overrides_everything(self, tmp_path):
        config_dir, workspace = _setup(tmp_path)
        _write(
            config_dir / "config.yaml",
            f"workspace: {workspace}\ntimezone: America/New_York\n",
        )
        _write(config_dir / "config.local.yaml", "timezone: UTC\n")
        _write(workspace_settings_file(workspace), "timezone: Europe/Amsterdam\n")

        cfg = load_config(config_dir)
        assert cfg.timezone == "UTC"

    def test_absent_settings_is_backwards_compatible(self, tmp_path):
        config_dir, workspace = _setup(tmp_path)
        _write(
            config_dir / "config.yaml",
            f"workspace: {workspace}\ntimezone: America/New_York\n",
        )
        # No workspace/config/settings.yaml at all.
        cfg = load_config(config_dir)
        assert cfg.timezone == "America/New_York"

    def test_workspace_key_in_settings_ignored(self, tmp_path):
        config_dir, workspace = _setup(tmp_path)
        bogus = tmp_path / "bogus_ws"
        _write(config_dir / "config.yaml", f"workspace: {workspace}\n")
        _write(
            workspace_settings_file(workspace),
            f"workspace: {bogus}\ntimezone: Europe/Amsterdam\n",
        )

        cfg = load_config(config_dir)
        # settings.yaml's workspace key is ignored (would be circular); the
        # real workspace stays the one from config.yaml, and its other keys apply.
        assert cfg.workspace == workspace
        assert cfg.timezone == "Europe/Amsterdam"

    def test_settings_can_reference_env(self, tmp_path, monkeypatch):
        config_dir, workspace = _setup(tmp_path)
        monkeypatch.setenv("TZ_FROM_ENV", "Asia/Tokyo")
        _write(config_dir / "config.yaml", f"workspace: {workspace}\n")
        _write(workspace_settings_file(workspace), "timezone: ${TZ_FROM_ENV}\n")

        cfg = load_config(config_dir)
        assert cfg.timezone == "Asia/Tokyo"

    def test_nested_deep_merge_across_layers(self, tmp_path):
        config_dir, workspace = _setup(tmp_path)
        _write(
            config_dir / "config.yaml",
            f"workspace: {workspace}\nagent:\n  effort: high\n",
        )
        _write(
            workspace_settings_file(workspace),
            "agent:\n  model: claude-opus-4-8\n",
        )
        cfg = load_config(config_dir)
        # Nested dicts deep-merge across layers rather than clobbering.
        assert cfg.agent.model == "claude-opus-4-8"  # from settings
        assert cfg.agent.effort == "high"            # from config.yaml

    def test_workspace_path_env_default_locates_settings(self, tmp_path, monkeypatch):
        config_dir = tmp_path / "cfg"
        workspace = tmp_path / "wsx"
        config_dir.mkdir(parents=True, exist_ok=True)
        workspace.mkdir(parents=True, exist_ok=True)
        monkeypatch.delenv("WS_DIR", raising=False)
        # workspace path itself uses a ${VAR:-default} ref — the default must be
        # honored when locating settings.yaml.
        _write(config_dir / "config.yaml", f'workspace: "${{WS_DIR:-{workspace}}}"\n')
        _write(workspace_settings_file(workspace), "timezone: Pacific/Auckland\n")
        cfg = load_config(config_dir)
        assert cfg.workspace == workspace
        assert cfg.timezone == "Pacific/Auckland"

    def test_non_mapping_settings_is_an_error(self, tmp_path):
        """A shared settings file of the wrong shape must not evaporate.

        Ignoring it means every key it was supposed to supply silently
        reverts to the machine-local layers, with nothing but a log line —
        and this is the layer that arrives from a remote repo, where nobody
        is reading the daemon's log. A truncated write or a merge conflict
        resolved into a sequence produces exactly this.
        """
        config_dir, workspace = _setup(tmp_path)
        _write(
            config_dir / "config.yaml",
            f"workspace: {workspace}\ntimezone: America/New_York\n",
        )
        _write(workspace_settings_file(workspace), "- just\n- a\n- list\n")
        with pytest.raises(ConfigError) as excinfo:
            load_config(config_dir)
        assert "must be a mapping" in str(excinfo.value)
        assert "list" in str(excinfo.value)

    def test_comments_only_settings_is_fine(self, tmp_path):
        """The shipped scaffold is 100% comments and parses to None.

        Treating "present but empty" as an error would break every fresh
        install, so it stays permissive — only a wrong *shape* is an error.
        """
        config_dir, workspace = _setup(tmp_path)
        _write(config_dir / "config.yaml", f"workspace: {workspace}\n")
        _write(
            workspace_settings_file(workspace),
            "# Shareable settings.\n# Uncomment what you need.\n",
        )
        assert load_config(config_dir).workspace == workspace

    def test_empty_settings_file_is_fine(self, tmp_path):
        config_dir, workspace = _setup(tmp_path)
        _write(config_dir / "config.yaml", f"workspace: {workspace}\n")
        _write(workspace_settings_file(workspace), "")
        assert load_config(config_dir).workspace == workspace

    def test_malformed_settings_yaml_is_a_config_error(self, tmp_path):
        """Not a raw yaml.ParserError — cli.py routes doctor/validate around
        ConfigError specifically so they can report instead of crashing."""
        config_dir, workspace = _setup(tmp_path)
        _write(config_dir / "config.yaml", f"workspace: {workspace}\n")
        _write(workspace_settings_file(workspace), "a: [1, 2\nb: {oops\n")
        with pytest.raises(ConfigError) as excinfo:
            load_config(config_dir)
        assert "Failed to parse" in str(excinfo.value)

    def test_malformed_machine_config_is_a_config_error(self, tmp_path):
        config_dir, _workspace = _setup(tmp_path)
        _write(config_dir / "config.yaml", "timezone: [unclosed\n")
        with pytest.raises(ConfigError) as excinfo:
            load_config(config_dir)
        assert "Failed to parse" in str(excinfo.value)

    def test_non_mapping_machine_config_stays_lenient(self, tmp_path):
        """config.yaml is local and hand-edited; the operator sees the warning
        immediately. Only the layer that arrives over the wire is strict."""
        config_dir, _workspace = _setup(tmp_path)
        _write(config_dir / "config.yaml", "- not\n- a\n- mapping\n")
        assert load_config(config_dir).timezone  # loads with defaults

    # geteuid is POSIX-only and this runs at collection time, so an unguarded
    # call is an error for the whole module rather than a skip for one test.
    # Skipping where it is absent is also the right answer on the merits: a
    # chmod(0o000) does not take read access away on Windows, so the test could
    # not pass there even if it were collected.
    @pytest.mark.skipif(
        not hasattr(os, "geteuid") or os.geteuid() == 0,
        reason="root (or a platform without POSIX uids) ignores file permissions",
    )
    def test_unreadable_settings_is_a_config_error(self, tmp_path):
        """A file we can see but cannot open is a config problem too.

        The layer exists, so the loader commits to reading it and gets an
        OSError — wrong mode after a careless chmod, owned by another user, or
        deleted between the exists() check and the open. Letting that escape
        gives a PermissionError traceback out of the group callback, which
        also takes down the commands you would use to fix it.
        """
        config_dir, workspace = _setup(tmp_path)
        _write(config_dir / "config.yaml", f"workspace: {workspace}\n")
        settings = workspace_settings_file(workspace)
        _write(settings, "timezone: Europe/Amsterdam\n")
        settings.chmod(0o000)
        try:
            with pytest.raises(ConfigError) as excinfo:
                load_config(config_dir)
        finally:
            settings.chmod(0o600)
        # The path and the reason both have to survive — "cannot read config"
        # with neither is not actionable.
        assert str(settings) in str(excinfo.value)
        assert "Permission denied" in str(excinfo.value)

    def test_non_utf8_settings_is_a_config_error(self, tmp_path):
        """Latin-1 bytes in a UTF-8 file: still a report, not a traceback."""
        config_dir, workspace = _setup(tmp_path)
        _write(config_dir / "config.yaml", f"workspace: {workspace}\n")
        settings = workspace_settings_file(workspace)
        settings.parent.mkdir(parents=True, exist_ok=True)
        settings.write_bytes("timezone: Europe/Zürich\n".encode("latin-1"))
        with pytest.raises(ConfigError) as excinfo:
            load_config(config_dir)
        assert str(settings) in str(excinfo.value)

    def test_non_ascii_settings_load_in_a_non_utf8_locale(self, tmp_path):
        """The locale must not decide whether a valid UTF-8 file parses.

        Runs a real load under ``LC_ALL=C`` with PEP 538 locale coercion
        disabled — the environment a daemon gets from a service manager on an
        image with no C.UTF-8 locale. There the interpreter's default encoding
        is ASCII, so an unpinned open() fails on the first accented byte: the
        same config file loads from an interactive shell and refuses to load
        under systemd.
        """
        config_dir, workspace = _setup(tmp_path)
        _write(config_dir / "config.yaml", f"workspace: {workspace}\n")
        _write(
            workspace_settings_file(workspace),
            "timezone: Europe/Amsterdam\nsync:\n  gmail:\n"
            "    prompt_hint: Grüße aus München — ça va?\n",
        )
        proc = subprocess.run(
            [
                sys.executable, "-c",
                "import sys; from pathlib import Path;"
                "from nerve.config import load_config;"
                "print(load_config(Path(sys.argv[1])).sync.gmail.prompt_hint)",
                str(config_dir),
            ],
            capture_output=True, text=True, encoding="utf-8",
            env={
                **os.environ,
                "LC_ALL": "C",
                "LANG": "C",
                "PYTHONCOERCECLOCALE": "0",  # defeat PEP 538 C.UTF-8 coercion
                "PYTHONUTF8": "0",           # ...and PEP 540 UTF-8 mode
                "PYTHONIOENCODING": "utf-8",  # so we can read the answer back
                # Beat the editable install: this checkout, not site-packages.
                "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
            },
        )
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip() == "Grüße aus München — ça va?"

    def test_init_writes_utf8_in_a_non_utf8_locale(self, tmp_path):
        """The write side of the previous test: the wizard, not the loader.

        Every file `nerve init` emits starts with a comment header containing an
        em-dash, so in this environment an unpinned open() raises on the first
        write, before any user value is involved. A user value cannot trigger it:
        safe_dump escapes non-ASCII to \\xNN, so the dumped body is always ASCII.

        The wizard also reads its own output back, under ``except Exception``.
        A decode failure there does not abort the run — _resolve_jwt_secret
        returns a different secret, signs the external-agent MCP token with it,
        and the daemon rejects every call that token makes. The assertions below
        therefore check the round-trip, not just that bytes were written.
        """
        config_dir = tmp_path / "cfg"
        config_dir.mkdir(parents=True, exist_ok=True)
        proc = subprocess.run(
            [
                sys.executable, "-c",
                "import sys, yaml; from pathlib import Path;"
                "from nerve.bootstrap import SetupWizard;"
                "from nerve.config import load_config;"
                "d = Path(sys.argv[1]);"
                "w = SetupWizard(d);"
                "w.choices.workspace_path = d / 'ws';"
                "w.choices.mode = 'personal';"
                "w.choices.timezone = 'Europe/Amsterdam';"
                "w.choices.anthropic_api_key = 'sk-ant-api03-test';"
                "w._write_config_yaml();"
                "w._write_workspace_settings();"
                "w._write_config_local_yaml();"
                "w._write_cron_jobs();"
                # The Dockerfile and entrypoint templates carry an em-dash too.
                "w._ensure_docker_files();"
                # Read-back paths: the secret recovered from disk must be the
                # one on disk, and the URL must reflect the config just written.
                "on_disk = yaml.safe_load("
                "    (d / 'config.local.yaml').read_text(encoding='utf-8'));"
                "print('JWT_MATCH',"
                "    w._resolve_jwt_secret()"
                "    == (on_disk.get('auth') or {}).get('jwt_secret'));"
                "print('MCP_URL', w._compute_nerve_mcp_url());"
                "print(load_config(d).timezone)",
                str(config_dir),
            ],
            capture_output=True, text=True, encoding="utf-8",
            env={
                **os.environ,
                "LC_ALL": "C",
                "LANG": "C",
                "PYTHONCOERCECLOCALE": "0",
                "PYTHONUTF8": "0",
                "PYTHONIOENCODING": "utf-8",
                # Keep the cron writer out of the real state dir.
                "NERVE_HOME": str(tmp_path / "state"),
                "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
            },
        )
        assert proc.returncode == 0, proc.stderr
        out = proc.stdout
        # Located rather than hardcoded: the cron directory moves from NERVE_HOME
        # into the workspace later in this stack, and this file is carried up
        # unchanged.
        cron_files = sorted(
            p
            for root in (tmp_path / "state", config_dir / "ws")
            if root.exists()
            for p in root.rglob("*.yaml")
            if p.name in {"system.yaml", "jobs.yaml"}
        )
        assert [p.name for p in cron_files] == ["jobs.yaml", "system.yaml"], cron_files
        # Written as UTF-8, not in the locale's encoding. The loader rejects a
        # settings.yaml it cannot decode, so getting this wrong would produce a
        # config the daemon refuses to load.
        for path in (
            config_dir / "config.yaml",
            config_dir / "config.local.yaml",
            workspace_settings_file(config_dir / "ws"),
            *cron_files,
        ):
            assert "—" in path.read_text(encoding="utf-8"), path
        assert "JWT_MATCH True" in out, out
        # host 0.0.0.0 is rewritten to localhost, so this is the configured
        # gateway rather than the hardcoded 127.0.0.1 fallback a failed read
        # would leave behind.
        assert "MCP_URL http://localhost:8900/mcp/v1/" in out, out
        # Last line, not the whole of stdout: the settings writer prints an
        # "added: ..." summary of the keys it emitted ahead of this.
        assert out.strip().splitlines()[-1] == "Europe/Amsterdam"


class TestMcpServersFromWorkspace:
    def test_mcp_server_defined_in_workspace_settings(self, tmp_path):
        config_dir, workspace = _setup(tmp_path)
        _write(config_dir / "config.yaml", f"workspace: {workspace}\n")
        _write(
            workspace_settings_file(workspace),
            "mcp_servers:\n"
            "  shared:\n"
            "    type: http\n"
            "    url: https://example.com/mcp\n",
        )
        servers = load_mcp_servers(config_dir)
        assert any(s.name == "shared" for s in servers)


class TestConfigScaffold:
    def test_scaffold_creates_settings(self, tmp_path):
        created = install_config_scaffold(tmp_path)
        assert (workspace_config_dir(tmp_path)).is_dir()
        assert workspace_settings_file(tmp_path).exists()
        assert "config/settings.yaml" in created

    def test_scaffold_is_idempotent_and_non_destructive(self, tmp_path):
        install_config_scaffold(tmp_path)
        # User edits their settings.
        workspace_settings_file(tmp_path).write_text("timezone: UTC\n", encoding="utf-8")
        created = install_config_scaffold(tmp_path)
        assert created == []  # nothing re-created
        assert workspace_settings_file(tmp_path).read_text() == "timezone: UTC\n"


class TestBrokenConfigIsRepairable:
    """A settings.yaml that won't load must not take out the tools you'd use
    to fix it. The group callback runs before every subcommand, so without an
    exemption a bad shared file bricks `doctor` and `init` too."""

    def _broken(self, tmp_path):
        config_dir, workspace = _setup(tmp_path)
        _write(config_dir / "config.yaml", f"workspace: {workspace}\n")
        _write(workspace_settings_file(workspace), "- not\n- a\n- mapping\n")
        return config_dir

    def test_doctor_reports_instead_of_crashing(self, tmp_path):
        from click.testing import CliRunner

        from nerve.cli import main

        result = CliRunner().invoke(
            main, ["-c", str(self._broken(tmp_path)), "doctor"]
        )
        assert result.exit_code == 1
        assert "must be a mapping" in result.output
        assert "Traceback" not in result.output

    def test_init_still_runs(self, tmp_path):
        """`nerve init` is the repair tool; it must not need a loadable config."""
        from click.testing import CliRunner

        from nerve.cli import main

        result = CliRunner().invoke(
            main, ["-c", str(self._broken(tmp_path)), "init", "--if-needed"]
        )
        assert "must be a mapping" not in result.output
        assert "Traceback" not in result.output

    def test_other_commands_get_a_clean_error(self, tmp_path):
        from click.testing import CliRunner

        from nerve.cli import main

        result = CliRunner().invoke(
            main, ["-c", str(self._broken(tmp_path)), "status"]
        )
        assert result.exit_code != 0
        assert "must be a mapping" in result.output
        assert "Traceback" not in result.output

    def test_a_bad_backend_value_is_a_clean_error_too(self, tmp_path):
        """The other half of "the config is broken": typing is what rejects an
        unknown agent.backend, and it raises a plain ValueError rather than
        ConfigError. Re-raising that gave a traceback for a one-word typo."""
        from click.testing import CliRunner

        from nerve.cli import main

        config_dir, workspace = _setup(tmp_path)
        _write(config_dir / "config.yaml", f"workspace: {workspace}\n")
        _write(workspace_settings_file(workspace), "agent:\n  backend: bogus\n")

        result = CliRunner().invoke(main, ["-c", str(config_dir), "status"])

        assert result.exit_code != 0
        assert "agent.backend" in result.output
        assert "Traceback" not in result.output
        assert not isinstance(result.exception, ValueError)

    def test_an_internal_error_keeps_its_traceback(self, tmp_path, monkeypatch):
        """The clean-error path is for the operator's config, not for nerve's
        own bugs: reporting a defect here as "Error: <config problem>" sends
        someone off to edit a file that was never at fault."""
        import nerve.cli as cli_mod
        from click.testing import CliRunner

        config_dir, workspace = _setup(tmp_path)
        _write(config_dir / "config.yaml", f"workspace: {workspace}\n")

        def _boom(_dir):
            raise RuntimeError("internal defect")

        monkeypatch.setattr(cli_mod, "load_config", _boom)

        result = CliRunner().invoke(cli_mod.main, ["-c", str(config_dir), "status"])

        assert isinstance(result.exception, RuntimeError)
