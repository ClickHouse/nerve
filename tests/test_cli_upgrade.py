"""Tests for the ``nerve upgrade`` command."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import click
from click.testing import CliRunner

from nerve.cli import _find_source_root, _pip_install_cmd, upgrade


@dataclass
class FakeConfig:
    deployment: str = "server"


def _invoke(
    runner: CliRunner,
    config_dir: Path,
    args: list[str],
    config: FakeConfig | None = None,
) -> "click.testing.Result":
    """Invoke the upgrade command with a preset context.

    The top-level ``main`` group loads config from disk, which is unnecessary
    for unit tests.  Instead we call the ``upgrade`` command directly with a
    manually-populated context object.
    """
    obj = {
        "config": config or FakeConfig(),
        "config_dir": str(config_dir),
        "verbose": False,
    }
    return runner.invoke(upgrade, args, obj=obj, standalone_mode=False)


class TestFindSourceRoot:
    """`_find_source_root` should point at the checkout containing pyproject.toml."""

    def test_points_at_pyproject(self) -> None:
        root = _find_source_root()
        # The repo we're running from should have a pyproject.toml at its root.
        assert (root / "pyproject.toml").exists()
        # And the ``nerve`` package should live one level down.
        assert (root / "nerve" / "cli.py").exists()


class TestPipInstallCmd:
    """`_pip_install_cmd` prefers uv when available, falls back to pip."""

    def test_uses_uv_when_available(self, tmp_path: Path) -> None:
        with patch("nerve.cli.shutil.which", return_value="/usr/local/bin/uv"):
            cmd = _pip_install_cmd(tmp_path)
        assert cmd[0] == "/usr/local/bin/uv"
        assert cmd[1:4] == ["pip", "install", "-e"]
        assert str(tmp_path) in cmd
        # uv needs to know which interpreter to install into
        assert "--python" in cmd
        assert sys.executable in cmd

    def test_falls_back_to_pip(self, tmp_path: Path) -> None:
        with patch("nerve.cli.shutil.which", return_value=None):
            cmd = _pip_install_cmd(tmp_path)
        assert cmd[:3] == [sys.executable, "-m", "pip"]
        assert "install" in cmd
        assert "-e" in cmd
        assert str(tmp_path) in cmd


class TestUpgradeCommand:
    """End-to-end tests for the ``nerve upgrade`` click command."""

    def _make_source_root(self, tmp_path: Path, with_git: bool = True, with_web: bool = True) -> Path:
        """Fabricate a minimal nerve-looking checkout under tmp_path."""
        (tmp_path / "pyproject.toml").write_text("[project]\nname = 'nerve'\n")
        (tmp_path / "nerve").mkdir()
        (tmp_path / "nerve" / "__init__.py").write_text("")
        if with_git:
            (tmp_path / ".git").mkdir()
        if with_web:
            (tmp_path / "web").mkdir()
            (tmp_path / "web" / "package.json").write_text("{}")
        return tmp_path

    def test_docker_mode_bails_out(self, tmp_path: Path) -> None:
        """Docker deployments should be told to pull the image instead."""
        runner = CliRunner()
        # Patch subprocess.run so we can assert no install/build was attempted
        # if the docker-mode guard somehow fails to short-circuit.
        with patch("nerve.cli.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = _invoke(
                runner,
                tmp_path,
                [],
                config=FakeConfig(deployment="docker"),
            )
        assert "docker compose" in result.output.lower()
        # No subprocess calls should have fired — docker mode must short-circuit
        # before any git/pip/npm work.
        mock_run.assert_not_called()

    def test_full_flow_runs_all_steps(self, tmp_path: Path) -> None:
        """Default flow: git pull → pip install → npm install → npm run build."""
        source_root = self._make_source_root(tmp_path)
        runner = CliRunner()

        with patch("nerve.cli._find_source_root", return_value=source_root), \
             patch("nerve.cli.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"), \
             patch("nerve.cli.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = _invoke(runner, tmp_path, [])

        assert result.exit_code == 0, result.output
        # Collect the commands that were invoked (first positional arg)
        invoked = [call.args[0] for call in mock_run.call_args_list]
        assert ["git", "pull", "--ff-only"] in invoked
        # pip-install step: exact command depends on which() mock, which
        # returns "/usr/bin/uv" → uv pip install ...
        assert any(cmd[:4] == ["/usr/bin/uv", "pip", "install", "-e"] for cmd in invoked)
        assert ["npm", "install"] in invoked
        assert ["npm", "run", "build"] in invoked

    def test_no_pull_skips_git(self, tmp_path: Path) -> None:
        source_root = self._make_source_root(tmp_path)
        runner = CliRunner()

        with patch("nerve.cli._find_source_root", return_value=source_root), \
             patch("nerve.cli.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"), \
             patch("nerve.cli.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = _invoke(runner, tmp_path, ["--no-pull"])

        assert result.exit_code == 0, result.output
        invoked = [call.args[0] for call in mock_run.call_args_list]
        assert not any(cmd[:2] == ["git", "pull"] for cmd in invoked)

    def test_no_deps_skips_pip_install(self, tmp_path: Path) -> None:
        source_root = self._make_source_root(tmp_path)
        runner = CliRunner()

        with patch("nerve.cli._find_source_root", return_value=source_root), \
             patch("nerve.cli.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"), \
             patch("nerve.cli.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = _invoke(runner, tmp_path, ["--no-deps"])

        assert result.exit_code == 0, result.output
        invoked = [call.args[0] for call in mock_run.call_args_list]
        assert not any("pip" in str(cmd) and "install" in cmd for cmd in invoked)

    def test_no_frontend_skips_npm(self, tmp_path: Path) -> None:
        source_root = self._make_source_root(tmp_path)
        runner = CliRunner()

        with patch("nerve.cli._find_source_root", return_value=source_root), \
             patch("nerve.cli.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"), \
             patch("nerve.cli.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = _invoke(runner, tmp_path, ["--no-frontend"])

        assert result.exit_code == 0, result.output
        invoked = [call.args[0] for call in mock_run.call_args_list]
        assert not any(cmd[:1] == ["npm"] for cmd in invoked)

    def test_missing_pyproject_raises(self, tmp_path: Path) -> None:
        """If the source root doesn't look like a nerve checkout, bail out."""
        (tmp_path / "nerve").mkdir()
        runner = CliRunner()
        with patch("nerve.cli._find_source_root", return_value=tmp_path):
            result = _invoke(runner, tmp_path, [])
        assert result.exit_code != 0
        assert isinstance(result.exception, click.ClickException)
        assert "pyproject.toml" in str(result.exception.message)

    def test_missing_git_dir_skips_pull(self, tmp_path: Path) -> None:
        """Source checkout without .git should skip git pull, not fail."""
        source_root = self._make_source_root(tmp_path, with_git=False)
        runner = CliRunner()

        with patch("nerve.cli._find_source_root", return_value=source_root), \
             patch("nerve.cli.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"), \
             patch("nerve.cli.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = _invoke(runner, tmp_path, [])

        assert result.exit_code == 0, result.output
        invoked = [call.args[0] for call in mock_run.call_args_list]
        assert not any(cmd[:2] == ["git", "pull"] for cmd in invoked)
        # But pip install + npm build should still run
        assert any(cmd == ["npm", "install"] for cmd in invoked)

    def test_missing_web_dir_skips_frontend(self, tmp_path: Path) -> None:
        """No web/ directory means no frontend to build."""
        source_root = self._make_source_root(tmp_path, with_web=False)
        runner = CliRunner()

        with patch("nerve.cli._find_source_root", return_value=source_root), \
             patch("nerve.cli.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"), \
             patch("nerve.cli.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = _invoke(runner, tmp_path, [])

        assert result.exit_code == 0, result.output
        invoked = [call.args[0] for call in mock_run.call_args_list]
        assert not any(cmd[:1] == ["npm"] for cmd in invoked)

    def test_git_pull_failure_aborts(self, tmp_path: Path) -> None:
        """A failed git pull should raise ClickException and stop the upgrade."""
        source_root = self._make_source_root(tmp_path)
        runner = CliRunner()

        def run_side_effect(cmd, **kwargs):
            if cmd[:2] == ["git", "pull"]:
                return MagicMock(returncode=1)
            return MagicMock(returncode=0)

        with patch("nerve.cli._find_source_root", return_value=source_root), \
             patch("nerve.cli.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"), \
             patch("nerve.cli.subprocess.run", side_effect=run_side_effect) as mock_run:
            result = _invoke(runner, tmp_path, [])

        assert result.exit_code != 0
        assert isinstance(result.exception, click.ClickException)
        # Later steps must not have been attempted
        invoked = [call.args[0] for call in mock_run.call_args_list]
        assert not any("pip" in str(cmd) and "install" in cmd for cmd in invoked)
        assert not any(cmd[:1] == ["npm"] for cmd in invoked)

    def test_missing_npm_raises(self, tmp_path: Path) -> None:
        """Frontend rebuild requires npm; missing binary is a hard error."""
        source_root = self._make_source_root(tmp_path)
        runner = CliRunner()

        # npm is not installed, everything else is
        def which_fake(name: str) -> str | None:
            if name == "npm":
                return None
            return f"/usr/bin/{name}"

        with patch("nerve.cli._find_source_root", return_value=source_root), \
             patch("nerve.cli.shutil.which", side_effect=which_fake), \
             patch("nerve.cli.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = _invoke(runner, tmp_path, [])

        assert result.exit_code != 0
        assert isinstance(result.exception, click.ClickException)
        assert "npm" in str(result.exception.message)
