"""`nerve codex doctor` — the one backend the CLI builds itself.

Everywhere else a backend is constructed by the engine, which assembles
:class:`~nerve.agent.backends.BackendDeps`. The doctor assembles them by hand,
so it is the only caller that can get the shape wrong, and nothing exercised
it — which is how it came to hand over a config object where the backend
expects a callable returning one.

The backend here is real. Only the step that needs the codex binary on PATH is
stubbed, so construction and config resolution are what these tests run.
"""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from nerve.agent.backends.codex.backend import CodexBackend
from nerve.cli import main


def _config(tmp_path, extra=""):
    """A config dir whose codex home is under tmp_path, not the real one."""
    (tmp_path / "config.yaml").write_text(
        f"codex:\n  home_dir: {tmp_path / 'codex-home'}\n" + extra,
        encoding="utf-8",
    )
    return tmp_path


@pytest.fixture
def preflight(monkeypatch):
    """Stub preflight; the returned dict is what the command reports on."""
    status = {
        "available": True, "version": "0.9.9", "auth": "chatgpt",
        "models": ["gpt-5-codex"],
    }

    async def fake(self, *, force=False):
        return status

    monkeypatch.setattr(CodexBackend, "preflight", fake)
    return status


class TestDoctorCommand:
    def test_it_runs(self, tmp_path, preflight):
        """The regression guard: constructing the backend used to raise
        TypeError before the command had done anything at all."""
        result = CliRunner().invoke(main, ["-c", str(_config(tmp_path)), "codex", "doctor"])
        assert result.exit_code == 0, result.output
        assert "Traceback" not in result.output
        assert "0.9.9" in result.output

    def test_the_backend_resolves_the_config_the_cli_loaded(self, tmp_path, preflight):
        """Construction creates CODEX_HOME, so the directory appearing under
        tmp_path says the deps callable returned this config and not a default."""
        cfg = _config(tmp_path)
        result = CliRunner().invoke(main, ["-c", str(cfg), "codex", "doctor"])
        assert result.exit_code == 0, result.output
        assert (tmp_path / "codex-home").is_dir()

    def test_an_unavailable_cli_exits_non_zero(self, tmp_path, preflight):
        preflight.clear()
        preflight.update({"available": False, "reason": "codex not found"})
        result = CliRunner().invoke(main, ["-c", str(_config(tmp_path)), "codex", "doctor"])
        assert result.exit_code == 1
        assert "codex not found" in result.output

    def test_an_auth_mismatch_exits_non_zero(self, tmp_path, preflight):
        """Available but authenticated as something other than the configured
        mode: usable by hand, wrong for the daemon, so not a pass."""
        preflight.update({"auth_mismatch": True, "configured_auth": "api_key"})
        result = CliRunner().invoke(main, ["-c", str(_config(tmp_path)), "codex", "doctor"])
        assert result.exit_code == 1
        assert "auth mismatch" in result.output

    def test_json_output_is_parseable(self, tmp_path, preflight):
        result = CliRunner().invoke(
            main, ["-c", str(_config(tmp_path)), "codex", "doctor", "--json-output"]
        )
        assert result.exit_code == 0, result.output
        report = json.loads(result.output)
        assert report["preflight"]["version"] == "0.9.9"
        assert report["recoverable_runs"] == []
