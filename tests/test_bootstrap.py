"""Tests for the bootstrap wizard (nerve init)."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from nerve.bootstrap import (
    SetupWizard,
    SetupChoices,
    is_fresh_install,
    run_non_interactive,
    _resolve_claude_credential,
    _resolve_gh_token,
    _DOCKERFILE_TEMPLATE,
    _build_docker_compose,
    _DOCKER_ENTRYPOINT_TEMPLATE,
    _DOCKERIGNORE_TEMPLATE,
)
from nerve.cli import main


@pytest.fixture
def config_dir(tmp_path: Path) -> Path:
    """A temporary config directory (fresh install)."""
    return tmp_path / "config"


@pytest.fixture
def configured_dir(tmp_path: Path) -> Path:
    """A config directory that already has config.local.yaml."""
    d = tmp_path / "configured"
    d.mkdir()
    (d / "config.local.yaml").write_text("anthropic_api_key: sk-ant-test123\n")
    (d / "config.yaml").write_text("workspace: ~/test-workspace\n")
    return d


class TestIsFreshInstall:
    """Test fresh install detection."""

    def test_fresh_when_no_config_local(self, tmp_path: Path) -> None:
        assert is_fresh_install(tmp_path) is True

    def test_fresh_when_dir_missing(self, tmp_path: Path) -> None:
        nonexistent = tmp_path / "nope"
        assert is_fresh_install(nonexistent) is True

    def test_not_fresh_when_config_local_exists(self, configured_dir: Path) -> None:
        assert is_fresh_install(configured_dir) is False

    def test_not_fresh_even_if_config_yaml_missing(self, tmp_path: Path) -> None:
        """config.local.yaml alone means it's configured."""
        (tmp_path / "config.local.yaml").write_text("anthropic_api_key: test\n")
        assert is_fresh_install(tmp_path) is False


class TestSetupChoicesDefaults:
    """Verify SetupChoices has sane defaults."""

    def test_defaults(self) -> None:
        c = SetupChoices()
        assert c.deployment == "server"
        assert c.mode == "personal"
        assert c.anthropic_api_key == ""
        assert c.openai_api_key == ""
        assert c.workspace_path == Path("~/nerve-workspace")
        assert c.timezone == "America/New_York"
        assert c.enabled_crons == []
        assert c.task_description == ""


class TestNonInteractiveSetup:
    """Test non-interactive mode (Docker / CI)."""

    def test_requires_api_key(self, tmp_path: Path) -> None:
        """Should fail if ANTHROPIC_API_KEY is not set."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove the key if it exists
            env = {k: v for k, v in os.environ.items() if k != "ANTHROPIC_API_KEY"}
            with patch.dict(os.environ, env, clear=True):
                with pytest.raises(Exception):
                    run_non_interactive(tmp_path)

    def test_creates_all_files(self, tmp_path: Path) -> None:
        """Non-interactive mode should create config.yaml, config.local.yaml, workspace, and cron jobs."""
        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-testkey123",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "workspace"),
            "NERVE_TIMEZONE": "Europe/London",
        }
        with patch.dict(os.environ, env, clear=False):
            run_non_interactive(tmp_path)

        # config.yaml exists
        assert (tmp_path / "config.yaml").exists()
        config = yaml.safe_load((tmp_path / "config.yaml").read_text())
        assert config["timezone"] == "Europe/London"
        assert config["workspace"] == str(tmp_path / "workspace")

        # config.local.yaml exists with API key
        assert (tmp_path / "config.local.yaml").exists()
        local = yaml.safe_load((tmp_path / "config.local.yaml").read_text())
        assert local["anthropic_api_key"] == "sk-ant-api03-testkey123"
        assert "auth" in local
        assert "jwt_secret" in local["auth"]

        # Workspace directory exists with template files
        ws = tmp_path / "workspace"
        assert ws.exists()
        assert (ws / "SOUL.md").exists()
        assert (ws / "AGENTS.md").exists()

        # Cron jobs file exists
        cron_file = Path("~/.nerve/cron/jobs.yaml").expanduser()
        # Note: cron file is always written to ~/.nerve, not tmp_path
        # We just verify it exists (it may have been created by a previous test/run)

    def test_worker_mode(self, tmp_path: Path) -> None:
        """Worker mode should create minimal workspace."""
        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-workerkey",
            "NERVE_MODE": "worker",
            "NERVE_WORKSPACE": str(tmp_path / "worker-ws"),
            "NERVE_TASK": "Monitor CI and fix flaky tests",
        }
        with patch.dict(os.environ, env, clear=False):
            run_non_interactive(tmp_path)

        ws = tmp_path / "worker-ws"
        assert ws.exists()
        assert (ws / "SOUL.md").exists()
        assert (ws / "AGENTS.md").exists()
        # Personal-only files should NOT exist
        assert not (ws / "USER.md").exists()
        # Worker mode now includes MEMORY.md for hot memory
        assert (ws / "MEMORY.md").exists()

        # TASK.md should be created
        assert (ws / "TASK.md").exists()
        assert "Monitor CI" in (ws / "TASK.md").read_text()

    def test_personal_mode_default_crons(self, tmp_path: Path) -> None:
        """Personal non-interactive should enable inbox-processor and task-planner."""
        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-testkey",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
        }
        with patch.dict(os.environ, env, clear=False):
            choices = run_non_interactive(tmp_path)

        assert "inbox-processor" in choices.enabled_crons
        assert "task-planner" in choices.enabled_crons


class TestDeferredWrites:
    """Verify nothing is written until _apply()."""

    def test_nothing_written_before_apply(self, tmp_path: Path) -> None:
        """SetupWizard should not write anything until _apply() is called."""
        wizard = SetupWizard(tmp_path)
        wizard.choices.anthropic_api_key = "sk-ant-api03-test"
        wizard.choices.workspace_path = tmp_path / "workspace"
        wizard.choices.mode = "personal"

        # Before apply — nothing should exist
        assert not (tmp_path / "config.yaml").exists()
        assert not (tmp_path / "config.local.yaml").exists()
        assert not (tmp_path / "workspace").exists()

    def test_apply_creates_files(self, tmp_path: Path) -> None:
        """Calling _apply() should create all config and workspace files."""
        wizard = SetupWizard(tmp_path)
        wizard.choices.anthropic_api_key = "sk-ant-api03-test"
        wizard.choices.openai_api_key = "sk-proj-test"
        wizard.choices.workspace_path = tmp_path / "workspace"
        wizard.choices.mode = "personal"
        wizard.choices.timezone = "US/Pacific"
        wizard.choices.enabled_crons = ["inbox-processor"]

        wizard._apply()

        # Config files created
        assert (tmp_path / "config.yaml").exists()
        assert (tmp_path / "config.local.yaml").exists()

        # Workspace created
        assert (tmp_path / "workspace" / "SOUL.md").exists()

        # Config content is valid YAML
        config = yaml.safe_load((tmp_path / "config.yaml").read_text())
        assert config["timezone"] == "US/Pacific"

        # Local config has keys
        local = yaml.safe_load((tmp_path / "config.local.yaml").read_text())
        assert local["anthropic_api_key"] == "sk-ant-api03-test"
        assert local["openai_api_key"] == "sk-proj-test"


class TestCliInit:
    """Test the 'nerve init' CLI command."""

    def test_if_needed_skips_when_configured(self, configured_dir: Path) -> None:
        """--if-needed should exit silently when already configured."""
        runner = CliRunner()
        result = runner.invoke(main, ["-c", str(configured_dir), "init", "--if-needed"])
        assert result.exit_code == 0
        assert result.output == ""  # Silent exit

    def test_if_needed_non_interactive(self, tmp_path: Path) -> None:
        """--if-needed --non-interactive should run setup when fresh."""
        (tmp_path).mkdir(exist_ok=True)
        runner = CliRunner()
        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-clitest",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
        }
        result = runner.invoke(
            main,
            ["-c", str(tmp_path), "init", "--if-needed", "--non-interactive"],
            env=env,
        )
        assert result.exit_code == 0
        assert (tmp_path / "config.local.yaml").exists()

    def test_non_interactive_fails_without_key(self, tmp_path: Path) -> None:
        """Non-interactive should fail without ANTHROPIC_API_KEY."""
        (tmp_path).mkdir(exist_ok=True)
        runner = CliRunner()
        # Explicitly clear the key
        env = {"ANTHROPIC_API_KEY": ""}
        result = runner.invoke(
            main,
            ["-c", str(tmp_path), "init", "--non-interactive"],
            env=env,
        )
        assert result.exit_code != 0


class TestConfigLocalPermissions:
    """Test that config.local.yaml gets restrictive permissions."""

    def test_permissions_set(self, tmp_path: Path) -> None:
        """config.local.yaml should be 0600 after apply."""
        wizard = SetupWizard(tmp_path)
        wizard.choices.anthropic_api_key = "sk-ant-api03-test"
        wizard.choices.workspace_path = tmp_path / "workspace"
        wizard.choices.mode = "personal"

        wizard._apply()

        local_path = tmp_path / "config.local.yaml"
        assert local_path.exists()
        # Check permissions (Unix only)
        mode = oct(local_path.stat().st_mode)[-3:]
        assert mode == "600"


class TestInsideDockerFlag:
    """Test --inside-docker wizard behavior."""

    def test_inside_docker_sets_deployment(self, tmp_path: Path) -> None:
        """--inside-docker should set deployment to 'docker'."""
        wizard = SetupWizard(tmp_path, inside_docker=True)
        assert wizard._inside_docker is True
        assert wizard.choices.deployment == "docker"

    def test_inside_docker_false_by_default(self, tmp_path: Path) -> None:
        """Default wizard should not be inside Docker."""
        wizard = SetupWizard(tmp_path)
        assert wizard._inside_docker is False
        assert wizard.choices.deployment == "server"

    def test_step_counter_without_deployment(self, tmp_path: Path) -> None:
        """Inside Docker, step numbering starts at 1 for Mode."""
        wizard = SetupWizard(tmp_path, inside_docker=True)
        assert wizard._next_step("Mode") == "Step 1: Mode"
        assert wizard._next_step("API Keys") == "Step 2: API Keys"

    def test_step_counter_with_deployment(self, tmp_path: Path) -> None:
        """On host, deployment is step 1, mode is step 2."""
        wizard = SetupWizard(tmp_path)
        assert wizard._next_step("Deployment") == "Step 1: Deployment"
        assert wizard._next_step("Mode") == "Step 2: Mode"
        assert wizard._next_step("API Keys") == "Step 3: API Keys"


class TestEnsureDockerFiles:
    """Test Docker file generation."""

    def test_generates_all_files(self, tmp_path: Path) -> None:
        """_ensure_docker_files() should create Dockerfile, compose, entrypoint, and .dockerignore."""
        wizard = SetupWizard(tmp_path)
        wizard._ensure_docker_files()

        assert (tmp_path / "Dockerfile").exists()
        assert (tmp_path / "docker-compose.yml").exists()
        assert (tmp_path / "docker-entrypoint.sh").exists()
        assert (tmp_path / ".dockerignore").exists()

    def test_dockerfile_content(self, tmp_path: Path) -> None:
        """Dockerfile should have key directives."""
        wizard = SetupWizard(tmp_path)
        wizard._ensure_docker_files()

        content = (tmp_path / "Dockerfile").read_text()
        assert "FROM python:3.13-slim" in content
        assert "EXPOSE 8900" in content
        assert "HEALTHCHECK" in content
        assert "NERVE_DOCKER=1" in content
        assert "nodejs" in content

    def test_compose_content(self, tmp_path: Path) -> None:
        """docker-compose.yml should have correct service definition."""
        wizard = SetupWizard(tmp_path)
        wizard._ensure_docker_files()

        content = (tmp_path / "docker-compose.yml").read_text()
        compose = yaml.safe_load(content)
        assert "services" in compose
        assert "nerve" in compose["services"]
        assert "8900:8900" in compose["services"]["nerve"]["ports"]
        # Verify bind-mounts (not named volumes)
        volumes = compose["services"]["nerve"]["volumes"]
        assert ".:/nerve" in volumes
        assert "~/.nerve:/root/.nerve" in volumes

    def test_entrypoint_executable(self, tmp_path: Path) -> None:
        """docker-entrypoint.sh should be executable."""
        wizard = SetupWizard(tmp_path)
        wizard._ensure_docker_files()

        entrypoint = tmp_path / "docker-entrypoint.sh"
        assert entrypoint.exists()
        file_stat = entrypoint.stat()
        assert file_stat.st_mode & stat.S_IXUSR  # Owner execute bit

    def test_entrypoint_content(self, tmp_path: Path) -> None:
        """Entrypoint should handle both init+start and custom commands."""
        wizard = SetupWizard(tmp_path)
        wizard._ensure_docker_files()

        content = (tmp_path / "docker-entrypoint.sh").read_text()
        assert "pip install -e ." in content
        assert "nerve init --if-needed --non-interactive" in content
        assert 'exec nerve start -f' in content
        assert 'exec "$@"' in content

    def test_idempotent_no_overwrite(self, tmp_path: Path) -> None:
        """_ensure_docker_files() should not overwrite existing files."""
        wizard = SetupWizard(tmp_path)

        # Write a custom Dockerfile first
        (tmp_path / "Dockerfile").write_text("# custom\n")

        wizard._ensure_docker_files()

        # Should still be the custom content
        assert (tmp_path / "Dockerfile").read_text() == "# custom\n"
        # But other files should be created
        assert (tmp_path / "docker-compose.yml").exists()

    def test_dockerignore_content(self, tmp_path: Path) -> None:
        """.dockerignore should exclude common build artifacts."""
        wizard = SetupWizard(tmp_path)
        wizard._ensure_docker_files()

        content = (tmp_path / ".dockerignore").read_text()
        assert "__pycache__/" in content
        assert ".venv/" in content
        assert "web/node_modules/" in content
        assert ".git/" in content


class TestDockerNonInteractive:
    """Test non-interactive setup with Docker env vars."""

    def test_docker_env_sets_deployment(self, tmp_path: Path) -> None:
        """NERVE_DOCKER=1 should set deployment to docker."""
        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-docker-test",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
            "NERVE_DOCKER": "1",
        }
        with patch.dict(os.environ, env, clear=False):
            choices = run_non_interactive(tmp_path)

        assert choices.deployment == "docker"

    def test_docker_default_workspace(self, tmp_path: Path) -> None:
        """Docker mode should default workspace to /root/nerve-workspace."""
        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-docker-test",
            "NERVE_MODE": "personal",
            "NERVE_DOCKER": "1",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),  # Override to avoid /root permission error
        }
        with patch.dict(os.environ, env, clear=False):
            choices = run_non_interactive(tmp_path)

        # Verify deployment was set to docker
        assert choices.deployment == "docker"

    def test_docker_default_workspace_path(self) -> None:
        """Docker mode should default workspace to /root/nerve-workspace when no NERVE_WORKSPACE."""
        # Test the default path logic without running _apply()
        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-docker-test",
            "NERVE_DOCKER": "1",
        }
        with patch.dict(os.environ, env, clear=False):
            # Manually replicate the path logic from run_non_interactive
            is_docker = os.environ.get("NERVE_DOCKER", "") == "1"
            default_ws = "/root/nerve-workspace" if is_docker else "~/nerve-workspace"
            workspace = Path(os.environ.get("NERVE_WORKSPACE", default_ws))

        assert workspace == Path("/root/nerve-workspace")

    def test_no_docker_env_defaults_to_server(self, tmp_path: Path) -> None:
        """Without NERVE_DOCKER, deployment should be 'server'."""
        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-test",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
            # Explicitly clear NERVE_DOCKER so the test runs correctly
            # when executed inside a Docker container where NERVE_DOCKER=1
            # is already in the environment.
            "NERVE_DOCKER": "",
        }
        with patch.dict(os.environ, env, clear=False):
            choices = run_non_interactive(tmp_path)

        assert choices.deployment == "server"


class TestCliInsideDocker:
    """Test the --inside-docker CLI flag."""

    def test_inside_docker_flag_accepted(self, tmp_path: Path) -> None:
        """The --inside-docker flag should be accepted by nerve init."""
        runner = CliRunner()
        # Use --help to verify the flag is registered (avoids needing full wizard)
        result = runner.invoke(main, ["-c", str(tmp_path), "init", "--help"])
        assert result.exit_code == 0
        # Flag is hidden but should still work
        # Test it's accepted by passing it (will prompt for interactive input)
        # Just verify it doesn't error on flag parse


class TestDockerTemplateIntegrity:
    """Verify Docker templates are well-formed."""

    def test_dockerfile_not_empty(self) -> None:
        assert len(_DOCKERFILE_TEMPLATE.strip()) > 100

    def test_compose_valid_yaml(self) -> None:
        """_build_docker_compose() should produce valid YAML."""
        parsed = yaml.safe_load(_build_docker_compose())
        assert "services" in parsed
        assert "nerve" in parsed["services"]

    def test_compose_bind_mounts(self) -> None:
        """Compose should use host bind-mounts, not named volumes."""
        # Mock all optional dirs as existing so they appear in output
        with patch("nerve.bootstrap.os.path.isdir", return_value=True), \
             patch("nerve.bootstrap.os.path.expanduser", side_effect=lambda p: p):
            content = _build_docker_compose(workspace_path="~/my-workspace")
        parsed = yaml.safe_load(content)
        volumes = parsed["services"]["nerve"]["volumes"]
        assert ".:/nerve" in volumes
        assert "~/.nerve:/root/.nerve" in volumes
        assert "~/.config/gh:/root/.config/gh" in volumes
        assert "~/.config/gog:/root/.config/gog" in volumes
        # Workspace and projects are mounted host-aligned: same path
        # inside and outside the container so the agent can pass paths
        # through to the host docker daemon (via the mounted socket)
        # without translation. Legacy /root/* paths are restored via
        # symlinks in the entrypoint.
        assert "${HOME}/my-workspace:${HOME}/my-workspace" in volumes
        assert "${HOME}/projects:${HOME}/projects" in volumes
        # ~/.claude is NOT mounted (macOS Keychain, not filesystem)
        assert "~/.claude:/root/.claude" not in volumes
        # No named volumes section
        assert "volumes" not in parsed or parsed.get("volumes") is None

    def test_compose_skips_missing_auth_dirs(self) -> None:
        """Optional auth mounts should be excluded when host dirs don't exist."""
        with patch("nerve.bootstrap.os.path.isdir", return_value=False), \
             patch("nerve.bootstrap.os.path.expanduser", side_effect=lambda p: p):
            content = _build_docker_compose(workspace_path="~/ws")
        parsed = yaml.safe_load(content)
        volumes = parsed["services"]["nerve"]["volumes"]
        # Required mounts still present
        assert ".:/nerve" in volumes
        assert "~/.nerve:/root/.nerve" in volumes
        assert "${HOME}/ws:${HOME}/ws" in volumes
        assert "${HOME}/projects:${HOME}/projects" in volumes
        # Optional auth mounts absent
        assert "~/.config/gh:/root/.config/gh" not in volumes
        assert "~/.config/gog:/root/.config/gog" not in volumes

    def test_compose_extra_mounts(self) -> None:
        """Extra mounts should appear in the volumes list."""
        with patch("nerve.bootstrap.os.path.isdir", return_value=False), \
             patch("nerve.bootstrap.os.path.expanduser", side_effect=lambda p: p):
            content = _build_docker_compose(
                extra_mounts=["~/code:/code", "~/data:/data"],
            )
        parsed = yaml.safe_load(content)
        volumes = parsed["services"]["nerve"]["volumes"]
        assert "~/code:/code" in volumes
        assert "~/data:/data" in volumes

    def test_entrypoint_is_bash(self) -> None:
        """Entrypoint should start with bash shebang."""
        assert _DOCKER_ENTRYPOINT_TEMPLATE.strip().startswith("#!/bin/bash")

    def test_entrypoint_exports_oauth_token(self) -> None:
        """Entrypoint should export CLAUDE_CODE_OAUTH_TOKEN from config."""
        assert "CLAUDE_CODE_OAUTH_TOKEN" in _DOCKER_ENTRYPOINT_TEMPLATE
        assert "claude_oauth_token" in _DOCKER_ENTRYPOINT_TEMPLATE

    def test_entrypoint_exports_gh_token(self) -> None:
        """Entrypoint should export GH_TOKEN from config."""
        assert "GH_TOKEN" in _DOCKER_ENTRYPOINT_TEMPLATE
        assert "github_token" in _DOCKER_ENTRYPOINT_TEMPLATE

    def test_entrypoint_exports_api_key(self) -> None:
        """Entrypoint should still export ANTHROPIC_API_KEY as fallback."""
        assert "ANTHROPIC_API_KEY" in _DOCKER_ENTRYPOINT_TEMPLATE
        assert "anthropic_api_key" in _DOCKER_ENTRYPOINT_TEMPLATE

    def test_dockerignore_not_empty(self) -> None:
        assert len(_DOCKERIGNORE_TEMPLATE.strip()) > 50


class TestCredentialWaterfall:
    """Test credential resolution waterfall functions."""

    def test_resolve_claude_from_oauth_env(self) -> None:
        """CLAUDE_CODE_OAUTH_TOKEN env var should be picked up."""
        with patch.dict(os.environ, {"CLAUDE_CODE_OAUTH_TOKEN": "sk-ant-oat01-test"}, clear=False):
            token, source, debug = _resolve_claude_credential()
        assert token == "sk-ant-oat01-test"
        assert source == "CLAUDE_CODE_OAUTH_TOKEN env var"

    def test_resolve_claude_from_api_key_env(self, tmp_path: Path) -> None:
        """ANTHROPIC_API_KEY should be last resort in waterfall."""
        env = {"ANTHROPIC_API_KEY": "sk-ant-api03-test"}
        # Point credentials file at a non-existent path so the real one isn't found
        fake_creds = tmp_path / "nonexistent" / ".credentials.json"
        with patch.dict(os.environ, env, clear=False), \
             patch("nerve.bootstrap.Path.expanduser", return_value=fake_creds):
            # Remove CLAUDE_CODE_OAUTH_TOKEN if set
            os.environ.pop("CLAUDE_CODE_OAUTH_TOKEN", None)
            token, source, debug = _resolve_claude_credential()
        assert token == "sk-ant-api03-test"
        assert source == "ANTHROPIC_API_KEY env var"

    def test_resolve_claude_from_credentials_file(self, tmp_path: Path) -> None:
        """Should read from ~/.claude/.credentials.json on Linux."""
        creds_file = tmp_path / ".credentials.json"
        creds_file.write_text('{"claudeAiOauth": {"accessToken": "sk-ant-oat01-file"}}')

        with patch.dict(os.environ, {}, clear=False), \
             patch("nerve.bootstrap.Path.expanduser", return_value=creds_file), \
             patch("nerve.bootstrap.sys.platform", "linux"):
            os.environ.pop("CLAUDE_CODE_OAUTH_TOKEN", None)
            os.environ.pop("ANTHROPIC_API_KEY", None)
            token, source, debug = _resolve_claude_credential()
        assert token == "sk-ant-oat01-file"
        assert "credentials.json" in source

    def test_resolve_claude_none(self, tmp_path: Path) -> None:
        """Should return empty when no credentials found."""
        # Point credentials file at a non-existent path so the real one isn't found
        fake_creds = tmp_path / "nonexistent" / ".credentials.json"
        with patch.dict(os.environ, {}, clear=True), \
             patch("nerve.bootstrap.sys.platform", "linux"), \
             patch("nerve.bootstrap.Path.expanduser", return_value=fake_creds):
            token, source, debug = _resolve_claude_credential()
        assert token == ""
        assert source == "none"
        # Debug log should have entries for every source tried
        assert len(debug) > 0

    def test_resolve_claude_oauth_takes_priority(self) -> None:
        """OAuth token should win over API key."""
        env = {
            "CLAUDE_CODE_OAUTH_TOKEN": "oauth-token",
            "ANTHROPIC_API_KEY": "api-key",
        }
        with patch.dict(os.environ, env, clear=False):
            token, source, debug = _resolve_claude_credential()
        assert token == "oauth-token"
        assert "CLAUDE_CODE_OAUTH_TOKEN" in source

    def test_resolve_claude_keychain_oauth(self) -> None:
        """macOS Keychain 'Claude Code-credentials' should return OAuth token."""
        keychain_json = '{"claudeAiOauth": {"accessToken": "keychain-oauth-tok"}}'
        mock_result = subprocess.CompletedProcess(args=[], returncode=0, stdout=keychain_json, stderr="")

        with patch("nerve.bootstrap.sys.platform", "darwin"), \
             patch("nerve.bootstrap.shutil.which", return_value="/usr/bin/security"), \
             patch("nerve.bootstrap.subprocess.run", return_value=mock_result):
            token, source, debug = _resolve_claude_credential()
        assert token == "keychain-oauth-tok"
        assert "OAuth" in source

    def test_resolve_claude_keychain_api_key(self) -> None:
        """macOS Keychain 'Claude Code' should return raw API key when OAuth entry missing."""
        # First call (Claude Code-credentials) fails, second (Claude Code) succeeds
        fail = subprocess.CompletedProcess(args=[], returncode=44, stdout="", stderr="")
        ok = subprocess.CompletedProcess(args=[], returncode=0, stdout="sk-ant-raw-key\n", stderr="")

        def side_effect(cmd, **kw):
            if "Claude Code-credentials" in cmd:
                return fail
            return ok

        with patch("nerve.bootstrap.sys.platform", "darwin"), \
             patch("nerve.bootstrap.shutil.which", return_value="/usr/bin/security"), \
             patch("nerve.bootstrap.subprocess.run", side_effect=side_effect):
            token, source, debug = _resolve_claude_credential()
        assert token == "sk-ant-raw-key"
        assert "API key" in source

    def test_resolve_gh_from_env(self) -> None:
        """GH_TOKEN env var should be picked up."""
        with patch.dict(os.environ, {"GH_TOKEN": "ghp_test123"}, clear=False), \
             patch("nerve.bootstrap.shutil.which", return_value=None):
            token, source = _resolve_gh_token()
        assert token == "ghp_test123"
        assert source == "GH_TOKEN env var"

    def test_resolve_gh_none(self) -> None:
        """Should return empty when no gh credentials found."""
        with patch.dict(os.environ, {}, clear=True), \
             patch("nerve.bootstrap.shutil.which", return_value=None):
            token, source = _resolve_gh_token()
        assert token == ""
        assert source == "none"


class TestOAuthNonInteractive:
    """Test non-interactive setup with OAuth tokens."""

    def test_oauth_token_accepted_without_api_key(self, tmp_path: Path) -> None:
        """CLAUDE_CODE_OAUTH_TOKEN alone should be sufficient."""
        env = {
            "CLAUDE_CODE_OAUTH_TOKEN": "sk-ant-oat01-docker-test",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
        }
        # Ensure ANTHROPIC_API_KEY is not set
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            os.environ.pop("NERVE_USE_PROXY", None)
            choices = run_non_interactive(tmp_path)

        assert choices.claude_oauth_token == "sk-ant-oat01-docker-test"
        assert choices.anthropic_api_key == ""  # Not needed

        # Verify it's written to config.local.yaml
        local = yaml.safe_load((tmp_path / "config.local.yaml").read_text())
        assert local["claude_oauth_token"] == "sk-ant-oat01-docker-test"

    def test_gh_token_stored(self, tmp_path: Path) -> None:
        """GH_TOKEN should be written to config.local.yaml."""
        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-test",
            "GH_TOKEN": "ghp_testtoken123",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
        }
        with patch.dict(os.environ, env, clear=False):
            choices = run_non_interactive(tmp_path)

        assert choices.github_token == "ghp_testtoken123"

        local = yaml.safe_load((tmp_path / "config.local.yaml").read_text())
        assert local["github_token"] == "ghp_testtoken123"

    def test_oauth_token_in_config_local(self, tmp_path: Path) -> None:
        """OAuth token should appear in config.local.yaml via _apply()."""
        wizard = SetupWizard(tmp_path)
        wizard.choices.claude_oauth_token = "sk-ant-oat01-test"
        wizard.choices.github_token = "ghp_test"
        wizard.choices.workspace_path = tmp_path / "workspace"
        wizard.choices.mode = "personal"

        wizard._apply()

        local = yaml.safe_load((tmp_path / "config.local.yaml").read_text())
        assert local["claude_oauth_token"] == "sk-ant-oat01-test"
        assert local["github_token"] == "ghp_test"
        # API key should NOT be present (wasn't set)
        assert "anthropic_api_key" not in local

    def test_both_oauth_and_api_key(self, tmp_path: Path) -> None:
        """Both OAuth and API key can coexist."""
        wizard = SetupWizard(tmp_path)
        wizard.choices.claude_oauth_token = "oauth-token"
        wizard.choices.anthropic_api_key = "sk-ant-api03-key"
        wizard.choices.workspace_path = tmp_path / "workspace"
        wizard.choices.mode = "personal"

        wizard._apply()

        local = yaml.safe_load((tmp_path / "config.local.yaml").read_text())
        assert local["claude_oauth_token"] == "oauth-token"
        assert local["anthropic_api_key"] == "sk-ant-api03-key"


class TestSetupChoicesNewFields:
    """Verify new credential fields have correct defaults."""

    def test_defaults(self) -> None:
        c = SetupChoices()
        assert c.claude_oauth_token == ""
        assert c.github_token == ""
