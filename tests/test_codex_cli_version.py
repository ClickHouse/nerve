"""The Codex CLI version: the tested range, and the warning for a CLI that is
older than the version Nerve is tested with.

The catalog that the app-server returns depends on the CLI version. An older
CLI in the tested range runs, but its catalog can be without the models of
``codex.models``, and then the model menu shows only ``codex.model``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
from click.testing import CliRunner

from nerve.agent.backends.codex import CodexBackend
from nerve.agent.backends.codex.backend import TESTED_CLI_VERSION, cli_version_problem
from nerve.cli import doctor_report, main
from nerve.config import CodexConfig
from tests.test_codex_appserver import _config, _deps, _mode
from tests.test_cli_codex import preflight  # noqa: F401  (fixture)

META = Path(__file__).parent / "fixtures" / "codex_schema_meta.json"
MIN, MAX = CodexConfig().min_version, CodexConfig().max_version
TESTED = TESTED_CLI_VERSION


def _version(value: str) -> tuple[int, ...]:
    return tuple(int(part) for part in value.split("."))


def test_the_tested_version_is_the_version_of_the_schema_fixture():
    """A change of the tested range regenerates the fixture. The constant
    must follow it, or the warning names the wrong version."""
    meta = json.loads(META.read_text(encoding="utf-8"))
    assert TESTED_CLI_VERSION == meta["codex_cli_version"]
    defaults = CodexConfig()
    assert (
        _version(defaults.min_version)
        <= _version(TESTED_CLI_VERSION)
        < _version(defaults.max_version)
    )


class TestCliVersionProblem:
    RANGE = (MIN, MAX)

    @pytest.mark.parametrize("output", [
        f"codex-cli {MIN}", "codex-cli 0.155.1", "codex-cli 0.171.0",
    ])
    def test_an_older_cli_in_the_range_gets_a_warning(self, output):
        problem = cli_version_problem(output, *self.RANGE)
        assert problem is not None
        fatal, message = problem
        assert fatal is False
        found = output.split()[-1]
        assert message.startswith(f"Codex CLI {found} is older than {TESTED}")
        assert f"install @openai/codex {TESTED} or a newer version below {MAX}" in message

    @pytest.mark.parametrize("output", [f"codex-cli {TESTED}", f"codex-cli {TESTED.rsplit('.', 1)[0]}.9"])
    def test_the_tested_cli_or_newer_is_good(self, output):
        assert cli_version_problem(output, *self.RANGE) is None

    @pytest.mark.parametrize("output", ["codex-cli 0.153.0", f"codex-cli {MAX}"])
    def test_a_cli_outside_the_range_is_fatal(self, output):
        assert cli_version_problem(output, *self.RANGE) == (
            True,
            f"Unsupported {output}; Nerve tested Codex versions >={MIN}, <{MAX}",
        )

    def test_output_without_a_version_is_fatal(self):
        assert cli_version_problem("codex", *self.RANGE) == (
            True, "Could not parse Codex CLI version from 'codex'",
        )

    def test_no_warning_when_the_range_ends_below_the_tested_version(self):
        """The operator chose an older range: no install advice can be in it."""
        assert cli_version_problem("codex-cli 0.155.1", "0.153.1", "0.160.0") is None


@pytest.mark.asyncio
class TestBackend:
    async def test_preflight_returns_the_warning_and_logs_it_once(
        self, tmp_path, monkeypatch, caplog,
    ):
        _mode(monkeypatch, "basic")
        monkeypatch.setenv("FAKE_CODEX_VERSION", "0.155.1")
        backend = CodexBackend(_deps(_config(tmp_path)))
        with caplog.at_level(logging.WARNING, logger="nerve.agent.backends.codex.backend"):
            status = await backend.preflight(force=True)
            await backend.preflight(force=True)
        assert status["available"] is True
        assert status["version_warning"].startswith(
            f"Codex CLI 0.155.1 is older than {TESTED}",
        )
        logged = [r for r in caplog.records if "is older than" in r.getMessage()]
        assert len(logged) == 1

    async def test_preflight_has_no_warning_for_the_tested_cli(self, tmp_path, monkeypatch):
        _mode(monkeypatch, "basic")
        monkeypatch.setenv("FAKE_CODEX_VERSION", TESTED_CLI_VERSION)
        backend = CodexBackend(_deps(_config(tmp_path)))
        status = await backend.preflight(force=True)
        assert status["available"] is True
        assert status["version_warning"] is None

    async def test_a_newer_cli_clears_the_warning(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FAKE_CODEX_VERSION", "0.155.1")
        backend = CodexBackend(_deps(_config(tmp_path)))
        await backend._check_cli_version()
        assert backend._cli_warning is not None
        monkeypatch.setenv("FAKE_CODEX_VERSION", TESTED_CLI_VERSION)
        await backend._check_cli_version()
        assert backend._cli_warning is None


class TestDoctor:
    def test_nerve_doctor_warns_about_an_older_cli(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FAKE_CODEX_VERSION", "0.155.1")
        report = doctor_report(_config(tmp_path))
        assert f"[WARN] Codex CLI 0.155.1 is older than {TESTED}" in report

    def test_nerve_doctor_shows_the_tested_cli_as_ok(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FAKE_CODEX_VERSION", TESTED_CLI_VERSION)
        report = doctor_report(_config(tmp_path))
        assert f"[OK] Codex CLI: codex-cli {TESTED_CLI_VERSION}" in report
        assert "is older than" not in report

    def test_nerve_doctor_warns_about_a_cli_outside_the_range(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FAKE_CODEX_VERSION", MAX)
        report = doctor_report(_config(tmp_path))
        assert f"[WARN] Unsupported codex-cli {MAX}" in report

    def test_nerve_doctor_has_no_codex_line_without_the_cli(self, tmp_path):
        report = doctor_report(_config(tmp_path, bin_path=str(tmp_path / "no-codex")))
        assert "Codex CLI" not in report

    def test_nerve_codex_doctor_shows_the_warning(self, tmp_path, preflight):  # noqa: F811
        preflight.update({"version_warning": "Codex CLI 0.155.1 is older than 0.171.0"})
        (tmp_path / "config.yaml").write_text(
            f"codex:\n  home_dir: {tmp_path / 'codex-home'}\n", encoding="utf-8",
        )
        result = CliRunner().invoke(main, ["-c", str(tmp_path), "codex", "doctor"])
        assert result.exit_code == 0, result.output
        assert "[WARN] Codex CLI 0.155.1 is older than 0.171.0" in result.output
