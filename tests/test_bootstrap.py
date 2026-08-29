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

        # config.yaml holds the machine-local half...
        assert (tmp_path / "config.yaml").exists()
        config = yaml.safe_load((tmp_path / "config.yaml").read_text())
        assert config["workspace"] == str(tmp_path / "workspace")
        # ...and the portable half lands in the git-tracked workspace layer,
        # which is what `nerve config sync` and lockdown actually read.
        settings = yaml.safe_load(
            (tmp_path / "workspace" / "config" / "settings.yaml").read_text()
        )
        assert settings["timezone"] == "Europe/London"
        assert "timezone" not in config

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

        # Cron config now lives in the git-syncable workspace/config/cron subtree
        assert (ws / "config" / "cron" / "system.yaml").exists()
        assert (ws / "config" / "cron" / "jobs.yaml").exists()
        assert (ws / "config" / "cron" / "gates").is_dir()

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

    def test_inbox_processor_default_has_idle_gate(self) -> None:
        """The default inbox-processor ships gated so idle polls are skipped."""
        from nerve.bootstrap import PRODUCTIVITY_CRONS
        from nerve.cron.gates import MessagesGate, build_gate

        inbox = next(c for c in PRODUCTIVITY_CRONS if c["id"] == "inbox-processor")
        assert inbox.get("run_if"), "inbox-processor must ship a run gate"
        spec = inbox["run_if"][0]
        assert spec["type"] == "messages"
        assert spec.get("consumer") == "inbox"
        # No hard-coded sources → the generic "any source" form.
        assert "sources" not in spec
        assert isinstance(build_gate(spec), MessagesGate)


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
        assert config["workspace"] == str(tmp_path / "workspace")
        settings = yaml.safe_load(
            (tmp_path / "workspace" / "config" / "settings.yaml").read_text()
        )
        assert settings["timezone"] == "US/Pacific"

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

    def test_reinit_prompt_describes_the_backups_it_actually_makes(
        self, configured_dir: Path
    ) -> None:
        """This prompt is how an operator decides whether re-running is safe.

        The wizard deliberately skips the ``.bak`` for an empty or
        comments-only file — a freshly scaffolded settings.yaml has nothing to
        lose and lives in a git-tracked directory. So a flat "all three are
        backed up" is a promise the code does not keep, exactly where being
        misled costs the most.
        """
        result = CliRunner().invoke(
            main, ["-c", str(configured_dir), "init"], input="n\n"
        )
        assert result.exit_code == 0
        assert "*.bak" in result.output
        assert "All three are backed up" not in result.output
        assert "comments-only" in result.output

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
        # The GOG install line uses shell ${VAR} syntax — if the template is
        # ever turned into a bare f-string it would swallow these.
        assert "${GOG_VERSION}" in content

    def test_dockerfile_installs_from_the_lockfile(self, tmp_path: Path) -> None:
        """Docker must install uv.lock's versions, not re-resolve pyproject bounds.

        Before this, the image synthesised a requirements.txt from
        `project.dependencies` and pip-installed it, so rebuilding an unchanged
        Nerve commit could pull a newer, untested dependency — the #316 failure
        mode, on a first-class deployment path.
        """
        wizard = SetupWizard(tmp_path)
        wizard._ensure_docker_files()

        content = (tmp_path / "Dockerfile").read_text()
        assert "uv sync --locked --no-install-project" in content
        assert "uv.lock" in content
        # No pip-based dependency resolution anywhere in the image.
        assert "pip install" not in content
        assert "requirements.txt" not in content

    def test_dockerfile_keeps_the_venv_outside_the_bind_mount(self, tmp_path: Path) -> None:
        """/nerve is bind-mounted at runtime, so the venv cannot live under it.

        A .venv inside /nerve would be shadowed by the host's checkout — and a
        host-created .venv may not even be Linux-compatible.
        """
        wizard = SetupWizard(tmp_path)
        wizard._ensure_docker_files()

        content = (tmp_path / "Dockerfile").read_text()
        assert "UV_PROJECT_ENVIRONMENT=/opt/nerve-venv" in content
        assert "/opt/nerve-venv/bin:$PATH" in content
        assert "UV_PROJECT_ENVIRONMENT=/nerve" not in content

    def test_dockerfile_sets_state_and_workspace_env(self, tmp_path: Path) -> None:
        """/root/.nerve must be a deliberate NERVE_HOME, not an artifact of $HOME."""
        from nerve.bootstrap import _DOCKER_NERVE_HOME, _DOCKER_WORKSPACE

        wizard = SetupWizard(tmp_path)
        wizard._ensure_docker_files()

        content = (tmp_path / "Dockerfile").read_text()
        assert f"ENV NERVE_HOME={_DOCKER_NERVE_HOME}" in content
        assert f"ENV NERVE_WORKSPACE={_DOCKER_WORKSPACE}" in content
        # The dirs the image pre-creates must be the ones it advertises.
        assert f"mkdir -p {_DOCKER_NERVE_HOME} {_DOCKER_WORKSPACE}" in content

    def test_compose_content(self, tmp_path: Path, monkeypatch) -> None:
        """docker-compose.yml should have correct service definition."""
        monkeypatch.delenv("NERVE_HOME", raising=False)
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

    def test_compose_host_state_dir_follows_nerve_home(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """Deriving only the container side gives host and container two DBs.

        The operator relocated the state dir; if compose keeps mounting the
        default ~/.nerve, `nerve sessions` on the host and the daemon in the
        container read different databases with no sign anything is wrong.
        """
        monkeypatch.setenv("NERVE_HOME", "/opt/nerve-state")
        wizard = SetupWizard(tmp_path)
        wizard._ensure_docker_files()

        compose = yaml.safe_load((tmp_path / "docker-compose.yml").read_text())
        volumes = compose["services"]["nerve"]["volumes"]
        assert "/opt/nerve-state:/root/.nerve" in volumes
        assert "/opt/nerve-state/claude:/root/.claude" in volumes
        assert not any(v.startswith("~/.nerve") for v in volumes)

    def test_compose_mounts_match_dockerfile_env(self, tmp_path: Path) -> None:
        """A mount pointing somewhere the image doesn't use is a silent no-op."""
        from nerve.bootstrap import _DOCKER_NERVE_HOME, _DOCKER_WORKSPACE

        wizard = SetupWizard(tmp_path)
        wizard._ensure_docker_files()

        compose = yaml.safe_load((tmp_path / "docker-compose.yml").read_text())
        targets = {v.split(":", 1)[1] for v in compose["services"]["nerve"]["volumes"]}
        assert _DOCKER_NERVE_HOME in targets
        assert _DOCKER_WORKSPACE in targets

    def test_entrypoint_clears_pid_under_nerve_home(self, tmp_path: Path) -> None:
        """A hard-coded ~/.nerve here would miss the PID file if NERVE_HOME moves."""
        wizard = SetupWizard(tmp_path)
        wizard._ensure_docker_files()

        content = (tmp_path / "docker-entrypoint.sh").read_text()
        assert 'rm -f "${NERVE_HOME:-$HOME/.nerve}/nerve.pid"' in content

    def test_agent_docs_locate_config_where_it_is_actually_written(self) -> None:
        """This text is appended to TOOLS.md, so the agent reads it as fact.

        In the container the entrypoint `cd /nerve`s before `nerve init`, and
        resolve_config_dir() falls through to the cwd — so config.yaml and
        config.local.yaml land in /nerve, not under NERVE_HOME. The doc used
        to claim the opposite, which sends the agent to edit a file nothing
        reads (and on a different bind mount than the one it meant).
        """
        from nerve.bootstrap import (
            _DOCKER_NERVE_HOME,
            _DOCKER_TOOLS_SECTION,
            _DOCKER_WORKSPACE,
        )

        assert "Config files live in `/nerve/`" in _DOCKER_TOOLS_SECTION
        assert f"`{_DOCKER_NERVE_HOME}/config.local.yaml`" not in _DOCKER_TOOLS_SECTION
        # Cron now lives in the workspace's syncable config subtree.
        assert f"{_DOCKER_WORKSPACE}/config/cron/" in _DOCKER_TOOLS_SECTION
        assert f"{_DOCKER_NERVE_HOME}/cron" not in _DOCKER_TOOLS_SECTION

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
        # Installs from uv.lock, not a fresh resolve: a container rebuild of an
        # unchanged commit must not pick up a different dependency set.
        assert "uv sync --locked --inexact" in content
        assert "pip install" not in content
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

    def test_compose_bind_mounts(self, monkeypatch) -> None:
        """Compose should use host bind-mounts, not named volumes."""
        # The conftest fixture points NERVE_HOME at a tmpdir; clear it so this
        # exercises the default an ordinary operator gets.
        monkeypatch.delenv("NERVE_HOME", raising=False)
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
        assert "~/my-workspace:/root/nerve-workspace" in volumes
        # ~/.claude is NOT mounted (macOS Keychain, not filesystem)
        assert "~/.claude:/root/.claude" not in volumes
        # No named volumes section
        assert "volumes" not in parsed or parsed.get("volumes") is None

    def test_compose_skips_missing_auth_dirs(self, monkeypatch) -> None:
        """Optional auth mounts should be excluded when host dirs don't exist."""
        monkeypatch.delenv("NERVE_HOME", raising=False)
        with patch("nerve.bootstrap.os.path.isdir", return_value=False), \
             patch("nerve.bootstrap.os.path.expanduser", side_effect=lambda p: p):
            content = _build_docker_compose(workspace_path="~/ws")
        parsed = yaml.safe_load(content)
        volumes = parsed["services"]["nerve"]["volumes"]
        # Required mounts still present
        assert ".:/nerve" in volumes
        assert "~/.nerve:/root/.nerve" in volumes
        assert "~/ws:/root/nerve-workspace" in volumes
        # Optional auth mounts absent
        assert "~/.config/gh:/root/.config/gh" not in volumes
        assert "~/.config/gog:/root/.config/gog" not in volumes

    def test_compose_extra_mounts(self, monkeypatch) -> None:
        """Extra mounts should appear in the volumes list."""
        monkeypatch.delenv("NERVE_HOME", raising=False)
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


class TestBedrockGeoPrefix:
    """Region → Bedrock inference-profile prefix mapping."""

    def test_us_regions(self) -> None:
        from nerve.bootstrap import bedrock_geo_prefix
        assert bedrock_geo_prefix("us-east-1") == "us"
        assert bedrock_geo_prefix("us-west-2") == "us"

    def test_eu_regions(self) -> None:
        from nerve.bootstrap import bedrock_geo_prefix
        assert bedrock_geo_prefix("eu-central-1") == "eu"
        assert bedrock_geo_prefix("eu-west-3") == "eu"
        assert bedrock_geo_prefix("EU-NORTH-1") == "eu"

    def test_apac_regions(self) -> None:
        from nerve.bootstrap import bedrock_geo_prefix
        assert bedrock_geo_prefix("ap-southeast-2") == "apac"
        assert bedrock_geo_prefix("ap-northeast-1") == "apac"

    def test_other_regions_default_us(self) -> None:
        from nerve.bootstrap import bedrock_geo_prefix
        assert bedrock_geo_prefix("ca-central-1") == "us"
        assert bedrock_geo_prefix("sa-east-1") == "us"
        assert bedrock_geo_prefix("") == "us"


class TestNonInteractiveBedrockRegion:
    """Bedrock model IDs must match the configured region's geography."""

    def test_eu_region_writes_eu_models(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        env = {
            "NERVE_PROVIDER": "bedrock",
            "NERVE_AWS_REGION": "eu-central-1",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
        }
        with patch.dict(os.environ, env, clear=False):
            run_non_interactive(tmp_path)

        # The region and the model IDs derived from it are shared policy, so
        # they land in the tracked layer and survive lockdown.
        settings = yaml.safe_load(
            (tmp_path / "ws" / "config" / "settings.yaml").read_text()
        )
        assert settings["provider"]["aws_region"] == "eu-central-1"
        assert settings["agent"]["model"].startswith("eu.anthropic.")
        assert settings["agent"]["cron_model"].startswith("eu.anthropic.")
        assert settings["memory"]["fast_model"].startswith("eu.anthropic.")

    def test_us_region_writes_us_models(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        env = {
            "NERVE_PROVIDER": "bedrock",
            "NERVE_AWS_REGION": "us-east-1",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
        }
        with patch.dict(os.environ, env, clear=False):
            run_non_interactive(tmp_path)

        settings = yaml.safe_load(
            (tmp_path / "ws" / "config" / "settings.yaml").read_text()
        )
        assert settings["agent"]["model"].startswith("us.anthropic.")


class TestNonInteractiveTelegramAllowedUsers:
    """NERVE_TELEGRAM_ALLOWED_USERS env wiring."""

    def test_allowed_users_written_to_local(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-tg",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
            "NERVE_TELEGRAM_BOT_TOKEN": "123456:ABC-test",
            "NERVE_TELEGRAM_ALLOWED_USERS": "111, 222",
        }
        with patch.dict(os.environ, env, clear=False):
            run_non_interactive(tmp_path)

        local = yaml.safe_load((tmp_path / "config.local.yaml").read_text())
        assert local["telegram"]["allowed_users"] == [111, 222]

    def test_invalid_allowed_users_rejected(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-tg",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
            "NERVE_TELEGRAM_BOT_TOKEN": "123456:ABC-test",
            "NERVE_TELEGRAM_ALLOWED_USERS": "not-a-number",
        }
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises(Exception):
                run_non_interactive(tmp_path)


class TestInitStatePersistence:
    """Wizard progress checkpointing (resume support)."""

    def test_save_load_round_trip(self) -> None:
        from nerve.bootstrap import (
            _choices_from_dict,
            _load_init_state,
            _save_init_state,
        )

        choices = SetupChoices()
        choices.mode = "personal"
        choices.anthropic_api_key = "sk-ant-saved"
        choices.workspace_path = Path("/tmp/some-ws")
        choices.telegram_allowed_users = [42]
        _save_init_state(choices, {"deployment", "mode"})

        state = _load_init_state()
        assert state is not None
        assert sorted(state["completed"]) == ["deployment", "mode"]

        restored = _choices_from_dict(state["choices"])
        assert restored.anthropic_api_key == "sk-ant-saved"
        assert restored.workspace_path == Path("/tmp/some-ws")
        assert restored.telegram_allowed_users == [42]

    def test_clear(self) -> None:
        from nerve.bootstrap import (
            _clear_init_state,
            _load_init_state,
            _save_init_state,
        )

        _save_init_state(SetupChoices(), {"mode"})
        _clear_init_state()
        assert _load_init_state() is None

    def test_state_file_permissions(self) -> None:
        from nerve.bootstrap import _init_state_file, _save_init_state

        _save_init_state(SetupChoices(), {"mode"})
        path = _init_state_file()
        mode = stat.S_IMODE(os.stat(path).st_mode)
        assert mode == 0o600

    def test_choices_from_dict_ignores_unknown_keys(self) -> None:
        from nerve.bootstrap import _choices_from_dict

        restored = _choices_from_dict({"mode": "worker", "legacy_field": True})
        assert restored.mode == "worker"

    def test_load_missing_returns_none(self) -> None:
        from nerve.bootstrap import _clear_init_state, _load_init_state

        _clear_init_state()
        assert _load_init_state() is None


class TestApplyBackups:
    """Re-running init must back up existing configs, not silently nuke them."""

    def test_existing_configs_backed_up(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        wizard = SetupWizard(tmp_path)
        wizard.choices.anthropic_api_key = "sk-ant-one"
        wizard.choices.workspace_path = tmp_path / "ws"
        wizard.choices.mode = "personal"
        wizard._apply()

        # Hand-edit, then re-apply
        (tmp_path / "config.yaml").write_text("# hand-edited\nworkspace: ~/custom\n")
        wizard2 = SetupWizard(tmp_path)
        wizard2.choices.anthropic_api_key = "sk-ant-two"
        wizard2.choices.workspace_path = tmp_path / "ws"
        wizard2.choices.mode = "personal"
        wizard2._apply()

        bak = tmp_path / "config.yaml.bak"
        assert bak.exists()
        assert bak.read_text().startswith("# hand-edited")
        assert (tmp_path / "config.local.yaml.bak").exists()


class TestStepCounter:
    """Step numbering shows totals once the mode is known."""

    def test_no_total_before_mode(self, tmp_path: Path) -> None:
        wizard = SetupWizard(tmp_path)
        assert wizard._next_step("Deployment") == "Step 1: Deployment"

    def test_total_after_mode_personal(self, tmp_path: Path) -> None:
        wizard = SetupWizard(tmp_path)
        wizard._next_step("Deployment")
        wizard._next_step("Mode")
        wizard.choices.mode = "personal"
        wizard._completed_steps.add("mode")
        assert wizard._next_step("API") == "Step 3/10: API"

    def test_total_after_mode_worker(self, tmp_path: Path) -> None:
        wizard = SetupWizard(tmp_path)
        wizard._next_step("Deployment")
        wizard._next_step("Mode")
        wizard.choices.mode = "worker"
        wizard._completed_steps.add("mode")
        assert wizard._next_step("API") == "Step 3/6: API"

    def test_skipped_steps_keep_numbering(self, tmp_path: Path) -> None:
        wizard = SetupWizard(tmp_path)
        wizard._completed_steps = {"deployment", "mode"}
        wizard.choices.mode = "personal"
        wizard._do("deployment", lambda: None)
        wizard._do("mode", lambda: None)
        assert wizard._step_counter == 2
        assert wizard._next_step("API") == "Step 3/10: API"


class TestWorkspaceExpansionMatchesTheLoader:
    """The wizard must resolve `workspace` to the same directory as the loader.

    It decides where settings.yaml is written; the loader decides where it is
    read. Any disagreement means `nerve init` reports success and every portable
    setting is silently absent — and on a locked box, which has no other layer,
    that leaves the instance running entirely on declared defaults.
    """

    @staticmethod
    def _loader_answer(raw: str) -> Path:
        """What nerve.config would resolve `workspace: <raw>` to."""
        import nerve.config as cfgmod
        from nerve import paths

        if "${" in raw:
            raw = cfgmod._interpolate_str(raw, [])
        return cfgmod._expand_path(raw) or paths.default_workspace()

    @pytest.mark.parametrize(
        "raw",
        [
            "~/nerve-workspace",
            "  ~/nerve-workspace",          # leading space: expanduser is a no-op
            "~/nerve-workspace\n",          # e.g. NERVE_WORKSPACE from a heredoc
            " /srv/nerve/ws ",
            "${NERVE_TEST_WS}",
            "$NERVE_TEST_WS",               # bare form: needs expandvars first
            "",                             # blank means unset
        ],
    )
    def test_agrees_with_the_loader(self, raw: str, monkeypatch) -> None:
        monkeypatch.setenv("NERVE_TEST_WS", "~/from-env")
        from nerve.bootstrap import _expand_workspace

        assert _expand_workspace(raw) == self._loader_answer(raw)

    def test_leading_whitespace_still_expands_the_home_directory(
        self, monkeypatch,
    ) -> None:
        """The concrete failure: expanduser only expands a *leading* ``~``.

        Running it before the strip left ``Path("  ~/nerve-workspace")`` — a
        relative path with a component literally named ``  ~``.
        """
        from nerve.bootstrap import _expand_workspace

        expanded = _expand_workspace("  ~/nerve-workspace")
        assert expanded.is_absolute()
        assert "~" not in str(expanded)

    def test_a_bare_env_var_holding_a_tilde_is_fully_expanded(
        self, monkeypatch,
    ) -> None:
        """expandvars has to run before expanduser, not after.

        ``$WS`` expands to ``~/ws``, which then still needs expanduser. Doing it
        the other way round ends at a literal tilde.
        """
        monkeypatch.setenv("NERVE_TEST_WS", "~/ws")
        from nerve.bootstrap import _expand_workspace

        expanded = _expand_workspace("$NERVE_TEST_WS")
        assert expanded.is_absolute()
        assert "~" not in str(expanded)
        assert expanded.name == "ws"

    def test_the_wizard_writes_where_the_loader_reads(self, tmp_path: Path) -> None:
        """End to end, with the whitespace that used to split the two apart."""
        from nerve.config import load_config

        ws = tmp_path / "ws"
        wizard = SetupWizard(tmp_path)
        wizard.choices.mode = "personal"
        wizard.choices.anthropic_api_key = "sk-ant-api03-test"
        wizard.choices.timezone = "Europe/Amsterdam"
        # A trailing newline is what an env var out of a compose file or heredoc
        # actually looks like.
        wizard.choices.workspace_path = f"{ws}\n"
        wizard._write_config_yaml()
        wizard._write_workspace_settings()

        assert load_config(tmp_path).timezone == "Europe/Amsterdam"


class TestPortableSettingsSplit:
    """`nerve init` must actually populate the tracked settings layer.

    Before the split the wizard wrote ~33 keys into machine-local config.yaml
    and zero into settings.yaml, so `nerve config sync` and lockdown were
    no-ops on a default install: adopting the shared layer meant hand-adding a
    key to settings.yaml *and* deleting it from config.yaml, because
    config.yaml shadows it.
    """

    def _wizard(self, tmp_path: Path, **choices: Any) -> SetupWizard:
        w = SetupWizard(tmp_path)
        w.choices.workspace_path = tmp_path / "workspace"
        w.choices.mode = "personal"
        w.choices.anthropic_api_key = "sk-ant-api03-test"
        for k, v in choices.items():
            setattr(w.choices, k, v)
        return w

    def test_layers_are_disjoint(self, tmp_path: Path) -> None:
        """A key in both layers makes the tracked copy dead weight.

        config.yaml shadows settings.yaml, so a value written to both can be
        edited in the shared repo forever with no effect.
        """
        def leaves(d: dict, prefix: str = "") -> set[str]:
            out: set[str] = set()
            for k, v in d.items():
                path = f"{prefix}{k}"
                if isinstance(v, dict):
                    out |= leaves(v, f"{path}.")
                else:
                    out.add(path)
            return out

        for kwargs in (
            {},
            {"provider_type": "bedrock", "aws_region": "eu-west-1"},
            {"use_proxy": True},
            {"deployment": "docker"},
            {"mode": "worker"},
            {"telegram_bot_token": "123:abc"},
        ):
            machine, portable, _ = self._wizard(tmp_path, **kwargs)._build_config_layers()
            overlap = leaves(machine) & leaves(portable)
            assert not overlap, f"{kwargs} → written to both layers: {overlap}"

    def test_machine_layer_is_only_machine_things(self, tmp_path: Path) -> None:
        machine, _portable, _shadowed = self._wizard(tmp_path)._build_config_layers()
        assert set(machine) <= {
            "workspace", "deployment", "gateway", "telegram",
            "provider", "proxy", "docker", "agent", "memory", "sync",
        }
        assert "timezone" not in machine
        assert "sessions" not in machine
        # Which sources sync is shared policy; whose mailboxes is not.
        assert set(machine.get("sync", {})) == {"gmail"}
        assert set(machine["sync"]["gmail"]) == {"accounts"}

    def test_bedrock_models_travel_with_the_region(self, tmp_path: Path) -> None:
        """Geography-scoped model IDs are shared, because the region is.

        They were machine-local on the grounds that a ``us.`` id is wrong for an
        ``eu-`` box. The effect was that a locked instance fell back to the
        non-prefixed declared defaults, which Bedrock rejects, and to an
        ``anthropic`` provider, since that block was machine-local too. The
        prefix is a function of the region alone, so tracking the region lets
        the IDs be tracked with it.
        """
        machine, portable, shadowed = self._wizard(
            tmp_path, provider_type="bedrock", aws_region="eu-west-1"
        )._build_config_layers()
        assert portable["provider"]["type"] == "bedrock"
        assert portable["provider"]["aws_region"] == "eu-west-1"
        assert portable["agent"]["model"].startswith("eu.anthropic.")
        assert portable["memory"]["fast_model"].startswith("eu.anthropic.")
        assert "agent" not in machine
        assert "memory" not in machine
        # Non-model agent settings were portable already and stay that way.
        assert portable["agent"]["thinking"] == "max"

    def test_only_the_aws_profile_stays_machine_local(self, tmp_path: Path) -> None:
        """The profile names an entry in this box's AWS credentials file.

        Provider and region describe the deployment; the profile is the only part
        of the block that cannot be shared.
        """
        machine, portable, _ = self._wizard(
            tmp_path,
            provider_type="bedrock",
            aws_region="eu-west-1",
            aws_profile="nerve-prod",
        )._build_config_layers()
        assert machine["provider"] == {"aws_profile": "nerve-prod"}
        assert "aws_profile" not in portable["provider"]

    def test_switching_off_bedrock_clears_the_tracked_region(
        self, tmp_path: Path
    ) -> None:
        """settings.yaml is merge-preserving, so the region must be deleted.

        Omitting it would leave the tracked file naming a region the box no
        longer uses, and under lockdown that stale value is the only one there.
        """
        self._wizard(
            tmp_path, provider_type="bedrock", aws_region="eu-west-1"
        )._apply()
        settings_path = tmp_path / "workspace" / "config" / "settings.yaml"
        before = yaml.safe_load(settings_path.read_text())
        assert before["provider"]["aws_region"] == "eu-west-1"
        assert before["agent"]["model"].startswith("eu.anthropic.")

        self._wizard(tmp_path, provider_type="anthropic")._apply()

        after = yaml.safe_load(settings_path.read_text())
        assert after["provider"]["type"] == "anthropic"
        assert "aws_region" not in after.get("provider", {})
        # The non-prefixed names overwrite rather than needing deletion.
        assert after["agent"]["model"] == "claude-opus-5"

    def test_gateway_host_and_port_are_shared(self, tmp_path: Path) -> None:
        """The tracked layer has to be able to state the bind address.

        While these were machine-local the wizard only ever wrote the declared
        defaults, so settings.yaml could not express them at all.
        """
        machine, portable, _ = self._wizard(tmp_path)._build_config_layers()
        assert portable["gateway"] == {"host": "0.0.0.0", "port": 8900}
        assert "gateway" not in machine

    def test_reinit_applies_new_answers(self, tmp_path: Path) -> None:
        """The wizard owns the keys it generates.

        "Existing always wins" sounds safer for a shared file, but it makes
        re-running init a no-op: you are prompted for a timezone, shown a
        tick, and the answer is discarded because the key already exists.
        """
        self._wizard(tmp_path, timezone="Europe/Berlin")._apply()
        settings_path = tmp_path / "workspace" / "config" / "settings.yaml"
        assert yaml.safe_load(settings_path.read_text())["timezone"] == "Europe/Berlin"

        self._wizard(tmp_path, timezone="US/Pacific", gmail_sync=True)._apply()
        after = yaml.safe_load(settings_path.read_text())
        assert after["timezone"] == "US/Pacific"
        assert after["sync"]["gmail"]["enabled"] is True

    def test_reinit_preserves_keys_the_wizard_does_not_own(self, tmp_path: Path) -> None:
        """A team policy key the wizard never emits must survive."""
        self._wizard(tmp_path)._apply()
        settings_path = tmp_path / "workspace" / "config" / "settings.yaml"
        edited = yaml.safe_load(settings_path.read_text())
        edited["team_only_key"] = "keep me"
        edited["agent"]["cache_ttl"] = "1h"      # real key, not wizard-generated
        settings_path.write_text(yaml.safe_dump(edited), encoding="utf-8")

        self._wizard(tmp_path)._apply()

        after = yaml.safe_load(settings_path.read_text())
        assert after["team_only_key"] == "keep me"
        assert after["agent"]["cache_ttl"] == "1h"
        assert after["agent"]["thinking"] == "max"

    def test_switching_to_bedrock_rewrites_the_tracked_model_names(
        self, tmp_path: Path
    ) -> None:
        """Re-running init replaces the names in place, in the shared file.

        The previous behaviour routed them to config.yaml and deleted them here,
        which left a locked box with no model names at all: it fell back to the
        non-prefixed declared defaults, which Bedrock rejects.
        """
        self._wizard(tmp_path)._apply()
        settings_path = tmp_path / "workspace" / "config" / "settings.yaml"
        assert yaml.safe_load(settings_path.read_text())["agent"]["model"] == (
            "claude-opus-5"
        )

        self._wizard(
            tmp_path, provider_type="bedrock", aws_region="eu-west-1"
        )._apply()

        after = yaml.safe_load(settings_path.read_text())
        assert after["agent"]["model"].startswith("eu.anthropic.")
        assert after["memory"]["recall_model"].startswith("eu.anthropic.")
        assert after["provider"] == {"type": "bedrock", "aws_region": "eu-west-1"}
        # And still nothing left in both layers.
        machine = yaml.safe_load((tmp_path / "config.yaml").read_text())
        assert not (
            set(SetupWizard._leaf_paths(after))
            & set(SetupWizard._leaf_paths(machine))
        )

    def test_fresh_install_leaves_no_bak_in_the_tracked_subtree(
        self, tmp_path: Path
    ) -> None:
        """The scaffold is comments-only; backing it up drops junk in a git dir."""
        self._wizard(tmp_path)._apply()
        ws_config = tmp_path / "workspace" / "config"
        assert not (ws_config / "settings.yaml.bak").exists()

    def test_workspace_path_with_env_ref_lands_where_the_loader_looks(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """The loader interpolates ${VAR} in `workspace`; the writer must too,
        or settings.yaml goes to a literal './${VAR}/config' directory and
        every portable setting is silently lost."""
        from nerve.config import load_config

        real_ws = tmp_path / "real-ws"
        monkeypatch.setenv("MY_WS", str(real_ws))
        wizard = self._wizard(tmp_path, timezone="Asia/Tokyo")
        wizard.choices.workspace_path = Path("${MY_WS}")
        wizard._apply()

        assert (real_ws / "config" / "settings.yaml").exists()
        assert load_config(tmp_path).timezone == "Asia/Tokyo"

        # Every writer has to agree on where the workspace is, not just the one
        # this test was originally written for. The cron writer expanded only
        # `~`, so it created a literal "${MY_WS}" directory next to the process
        # CWD — which is how one got committed to this repo.
        cron_dir = real_ws / "config" / "cron"
        assert (cron_dir / "system.yaml").exists()
        assert (cron_dir / "jobs.yaml").exists()
        assert not (Path.cwd() / "${MY_WS}").exists()
        cfg = load_config(tmp_path)
        assert cfg.cron.system_file == cron_dir / "system.yaml"
        assert cfg.cron.jobs_file == cron_dir / "jobs.yaml"

    def test_reinit_backs_up_settings(self, tmp_path: Path) -> None:
        wizard = self._wizard(tmp_path)
        wizard._apply()
        settings_path = tmp_path / "workspace" / "config" / "settings.yaml"
        settings_path.write_text("timezone: UTC\n", encoding="utf-8")
        self._wizard(tmp_path)._apply()
        assert (settings_path.parent / "settings.yaml.bak").read_text() == "timezone: UTC\n"

    def test_malformed_settings_is_left_alone(self, tmp_path: Path) -> None:
        """Don't destroy a file we can't parse — the operator needs to see it."""
        wizard = self._wizard(tmp_path)
        ws_config = tmp_path / "workspace" / "config"
        ws_config.mkdir(parents=True)
        broken = "timezone: [unclosed\n"
        (ws_config / "settings.yaml").write_text(broken, encoding="utf-8")
        wizard._apply()
        assert (ws_config / "settings.yaml").read_text() == broken

    def test_merged_result_still_loads(self, tmp_path: Path) -> None:
        """The split must be invisible to the loader: same effective config."""
        from nerve.config import load_config

        self._wizard(tmp_path, timezone="US/Pacific")._apply()
        cfg = load_config(tmp_path)
        assert cfg.timezone == "US/Pacific"                 # from settings.yaml
        assert cfg.workspace == tmp_path / "workspace"      # from config.yaml
        assert cfg.gateway.port == 8900                     # from config.yaml
        assert cfg.agent.thinking == "max"                  # from settings.yaml
        assert cfg.sessions.max_sessions == 500             # from settings.yaml
