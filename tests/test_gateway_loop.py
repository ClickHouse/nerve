"""``gateway.loop`` — which event loop uvicorn runs the gateway on.

The setting exists because uvicorn's default (``auto``) picks uvloop whenever it
is installed, and uvloop spawns subprocesses with a full ``fork()``. A gateway
that has loaded its memory index is large, so every spawn then blocks the loop
for seconds. The stdlib loop spawns with ``vfork()`` and is the default here.

These tests pin three things: the default is the stdlib loop, the value reaches
``uvicorn.Config`` verbatim (an ``auto`` sneaking back in would be invisible at
runtime — the process still serves, just slowly), and a typo is refused at load
rather than handed to uvicorn, whose own error names the option but not the file.
"""

from __future__ import annotations

import pytest

from nerve.config import GatewayConfig, NerveConfig, load_config


class TestGatewayLoopSetting:
    def test_default_is_the_stdlib_loop(self):
        assert GatewayConfig().loop == "asyncio"
        assert GatewayConfig.from_dict({}).loop == "asyncio"
        assert NerveConfig().gateway.loop == "asyncio"

    @pytest.mark.parametrize("value", ["asyncio", "uvloop", "auto"])
    def test_every_uvicorn_loop_is_accepted(self, value):
        assert GatewayConfig.from_dict({"loop": value}).loop == value

    def test_case_and_whitespace_are_normalised(self):
        assert GatewayConfig.from_dict({"loop": " UVLoop "}).loop == "uvloop"

    @pytest.mark.parametrize("blank", ["", "   ", None])
    def test_blank_means_unset(self, blank):
        assert GatewayConfig.from_dict({"loop": blank}).loop == "asyncio"

    def test_unknown_loop_is_refused_by_name(self):
        with pytest.raises(ValueError, match=r"gateway\.loop.*'trio'"):
            GatewayConfig.from_dict({"loop": "trio"})

    def test_loaded_from_config_file(self, tmp_path):
        (tmp_path / "config.yaml").write_text(
            "gateway:\n  loop: uvloop\n", encoding="utf-8"
        )
        assert load_config(tmp_path).gateway.loop == "uvloop"

    def test_env_reference_with_default(self, tmp_path, monkeypatch):
        """``${VAR:-default}`` works for this key like for ``gateway.host``."""
        monkeypatch.delenv("NERVE_TEST_LOOP", raising=False)
        (tmp_path / "config.yaml").write_text(
            'gateway:\n  loop: "${NERVE_TEST_LOOP:-auto}"\n', encoding="utf-8"
        )
        assert load_config(tmp_path).gateway.loop == "auto"
        monkeypatch.setenv("NERVE_TEST_LOOP", "asyncio")
        assert load_config(tmp_path).gateway.loop == "asyncio"


class TestRunServerHandsTheLoopToUvicorn:
    @pytest.fixture
    def captured(self, monkeypatch):
        """Run ``run_server`` against a fake uvicorn and a fake app factory."""
        import uvicorn

        from nerve.gateway import server as gw

        class _FakeServer:
            started = True

            def __init__(self, config):
                pass

            def run(self):
                pass

        calls: list[dict] = []
        monkeypatch.setattr(uvicorn, "Config", lambda app, **kw: calls.append(kw))
        monkeypatch.setattr(gw, "_DrainingServer", _FakeServer)
        monkeypatch.setattr(gw, "create_app", lambda: object())
        return calls

    def test_default_config_runs_on_asyncio(self, captured):
        from nerve.gateway.server import run_server

        run_server(NerveConfig())
        assert len(captured) == 1
        assert captured[0]["loop"] == "asyncio"

    def test_configured_loop_is_passed_verbatim(self, captured):
        from nerve.gateway.server import run_server

        config = NerveConfig()
        config.gateway.loop = "uvloop"
        run_server(config)
        assert captured[0]["loop"] == "uvloop"

    def test_loop_is_always_explicit(self, captured):
        """Never fall through to uvicorn's ``auto``: the whole point of the key."""
        from nerve.gateway.server import run_server

        run_server(NerveConfig())
        assert "loop" in captured[0]
        assert captured[0]["loop"] != "auto"


class TestReloadReportsIt:
    def test_a_changed_loop_needs_a_restart(self):
        from nerve.config_reload import restart_required

        old, new = NerveConfig(), NerveConfig()
        new.gateway.loop = "uvloop"
        report = restart_required(old, new)
        assert any(entry.startswith("gateway.loop") for entry in report), report

    def test_an_unchanged_loop_is_quiet(self):
        from nerve.config_reload import restart_required

        assert restart_required(NerveConfig(), NerveConfig()) == []
