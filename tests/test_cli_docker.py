"""Tests for Docker-aware CLI commands."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import click

from nerve.cli import _is_docker_mode, _find_compose_file, _docker_compose


@dataclass
class FakeConfig:
    deployment: str = "server"


class TestIsDockerMode:
    """Test Docker mode detection."""

    def test_server_deployment(self) -> None:
        """Server deployment should not be Docker mode."""
        assert _is_docker_mode(FakeConfig(deployment="server")) is False

    def test_docker_deployment(self) -> None:
        """Docker deployment on host should be Docker mode."""
        with patch.dict(os.environ, {}, clear=False):
            # Make sure NERVE_DOCKER is not set
            os.environ.pop("NERVE_DOCKER", None)
            assert _is_docker_mode(FakeConfig(deployment="docker")) is True

    def test_docker_inside_container(self) -> None:
        """Inside the container (NERVE_DOCKER=1), should NOT proxy."""
        with patch.dict(os.environ, {"NERVE_DOCKER": "1"}, clear=False):
            assert _is_docker_mode(FakeConfig(deployment="docker")) is False

    def test_no_deployment_attr(self) -> None:
        """Config without deployment attr should default to server."""
        config = MagicMock(spec=[])  # No attributes
        assert _is_docker_mode(config) is False

    def test_default_config(self) -> None:
        """Default FakeConfig should be server mode."""
        assert _is_docker_mode(FakeConfig()) is False


class TestFindComposeFile:
    """Test docker-compose.yml location."""

    def test_found(self, tmp_path: Path) -> None:
        """Should return path when docker-compose.yml exists."""
        (tmp_path / "docker-compose.yml").write_text("services:\n  nerve:\n")
        result = _find_compose_file(tmp_path)
        assert result == tmp_path / "docker-compose.yml"

    def test_not_found(self, tmp_path: Path) -> None:
        """Should raise ClickException when not found."""
        with pytest.raises(click.ClickException, match="docker-compose.yml not found"):
            _find_compose_file(tmp_path)


class TestDockerCompose:
    """Test docker compose command execution."""

    def test_subprocess_run(self, tmp_path: Path) -> None:
        """Non-streaming commands should use subprocess.run."""
        (tmp_path / "docker-compose.yml").write_text("services:\n  nerve:\n")

        with patch("nerve.cli.subprocess.run") as mock_run, \
             patch("nerve.cli.shutil.which", return_value="/usr/bin/docker"):
            mock_run.return_value = MagicMock(returncode=0)
            rc = _docker_compose(tmp_path, ["up", "-d"])

            assert rc == 0
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert cmd[:2] == ["docker", "compose"]
            assert "-f" in cmd
            assert "up" in cmd
            assert "-d" in cmd

    def test_replace_process(self, tmp_path: Path) -> None:
        """Streaming commands should use os.execvp."""
        (tmp_path / "docker-compose.yml").write_text("services:\n  nerve:\n")

        with patch("nerve.cli.os.execvp") as mock_execvp, \
             patch("nerve.cli.shutil.which", return_value="/usr/bin/docker"):
            _docker_compose(tmp_path, ["logs", "-f"], replace_process=True)

            mock_execvp.assert_called_once()
            cmd = mock_execvp.call_args[0][1]
            assert "logs" in cmd
            assert "-f" in cmd

    def test_docker_not_found(self, tmp_path: Path) -> None:
        """Should raise ClickException when docker is not installed."""
        (tmp_path / "docker-compose.yml").write_text("services:\n  nerve:\n")

        with patch("nerve.cli.shutil.which", return_value=None):
            with pytest.raises(click.ClickException, match="Docker not found"):
                _docker_compose(tmp_path, ["up"])

    def test_compose_file_not_found(self, tmp_path: Path) -> None:
        """Should raise when docker-compose.yml is missing."""
        with pytest.raises(click.ClickException, match="docker-compose.yml not found"):
            _docker_compose(tmp_path, ["up"])


class TestConfigDeploymentField:
    """Test that NerveConfig loads deployment field."""

    def test_default(self) -> None:
        """Default deployment should be 'server'."""
        from nerve.config import NerveConfig
        config = NerveConfig.from_dict({})
        assert config.deployment == "server"

    def test_docker(self) -> None:
        """Should read 'docker' from config dict."""
        from nerve.config import NerveConfig
        config = NerveConfig.from_dict({"deployment": "docker"})
        assert config.deployment == "docker"

    def test_server_explicit(self) -> None:
        """Should read explicit 'server' from config dict."""
        from nerve.config import NerveConfig
        config = NerveConfig.from_dict({"deployment": "server"})
        assert config.deployment == "server"


class TestDeploymentPersistence:
    """Test that bootstrap persists deployment in config.yaml."""

    def test_server_deployment_in_config(self, tmp_path: Path) -> None:
        """Server deployment should be written to config.yaml."""
        import yaml
        from nerve.bootstrap import SetupWizard

        wizard = SetupWizard(tmp_path)
        wizard.choices.anthropic_api_key = "sk-ant-api03-test"
        wizard.choices.workspace_path = tmp_path / "workspace"
        wizard.choices.mode = "personal"
        wizard.choices.deployment = "server"

        wizard._apply()

        config = yaml.safe_load((tmp_path / "config.yaml").read_text())
        assert config["deployment"] == "server"

    def test_docker_deployment_in_config(self, tmp_path: Path) -> None:
        """Docker deployment should be written to config.yaml."""
        import yaml
        from nerve.bootstrap import SetupWizard

        wizard = SetupWizard(tmp_path, inside_docker=True)
        wizard.choices.anthropic_api_key = "sk-ant-api03-test"
        wizard.choices.workspace_path = tmp_path / "workspace"
        wizard.choices.mode = "personal"

        wizard._apply()

        config = yaml.safe_load((tmp_path / "config.yaml").read_text())
        assert config["deployment"] == "docker"
