"""HTTP route tests for ``GET /api/models`` (gateway/routes/models.py).

Pattern mirrors test_workflow_routes.py: a minimal FastAPI app with the
real router, auth disabled via an empty jwt_secret, and a fake engine
carrying stub backends. The route must advertise the full selectable
Claude model list — not just the configured default — so the composer's
model picker renders for Claude sessions the same way it does for Codex,
and that list must include whatever the Anthropic Models API reports for
the configured credentials (nerve/models_catalog.py).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest


class FakeCodexBackend:
    def __init__(self, preflight: dict[str, Any]):
        self._preflight = preflight

    async def preflight(self) -> dict[str, Any]:
        return self._preflight


@pytest.fixture(autouse=True)
def _clean_catalog_cache():
    """Keep the process-wide discovery cache out of the route tests."""
    from nerve import models_catalog

    models_catalog.reset_cache()
    yield
    models_catalog.reset_cache()


@pytest.fixture
def stub_discovery(monkeypatch):
    """Make the Anthropic Models API return a fixed catalog (no network)."""
    from nerve import models_catalog

    def _stub(models: list[str]):
        async def _get_models(config):
            return list(models)

        monkeypatch.setattr(models_catalog, "get_models", _get_models)

    return _stub


@pytest.fixture
def make_client():
    """Factory: build a TestClient around a config + codex preflight stub."""
    import nerve.config as cfg_mod

    created: list[Any] = []

    def _make(cfg=None, codex_preflight: dict[str, Any] | None = None):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from nerve.config import NerveConfig
        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.models import router as models_router

        cfg = cfg or NerveConfig()
        # Auth dependency reads get_config().auth.jwt_secret — no secret
        # makes require_auth a no-op.
        cfg.auth.jwt_secret = ""
        cfg_mod._config = cfg

        backends: dict[str, Any] = {}
        if codex_preflight is not None:
            backends["codex"] = FakeCodexBackend(codex_preflight)
        engine = SimpleNamespace(_backends=backends)
        init_deps(engine=engine, db=None)  # type: ignore[arg-type]

        app = FastAPI()
        app.include_router(models_router)
        client = TestClient(app)
        created.append(client)
        return client

    yield _make

    cfg_mod._config = None  # leave global state clean for other tests


class TestListModels:
    def test_claude_advertises_full_selectable_list(self, make_client):
        """The picker needs >1 Claude entry — not just the default model."""
        from nerve.config import get_config

        client = make_client()
        data = client.get("/api/models").json()

        claude_models = [m for m in data["models"] if m["backend"] == "claude"]
        assert [m["id"] for m in claude_models] == get_config().claude_models
        assert len(claude_models) > 1
        assert all(m["provider"] == "anthropic" for m in claude_models)
        # The default leads the list and matches the advertised default.
        assert claude_models[0]["id"] == data["defaults"]["claude"]

    def test_claude_backend_option_lists_models(self, make_client):
        from nerve.config import get_config

        client = make_client()
        data = client.get("/api/models").json()

        claude_opt = next(
            o for o in data["backends"]["options"] if o["id"] == "claude"
        )
        assert claude_opt["models"] == get_config().claude_models
        assert claude_opt["available"] is True

    def test_configured_models_reach_the_response(self, make_client):
        from nerve.config import AgentConfig, NerveConfig

        cfg = NerveConfig(agent=AgentConfig.from_dict({
            "model": "claude-fable-5",
            "models": ["claude-opus-5", "claude-haiku-4-5-20251001"],
        }))
        client = make_client(cfg=cfg)
        data = client.get("/api/models").json()

        claude_ids = [
            m["id"] for m in data["models"] if m["backend"] == "claude"
        ]
        assert claude_ids == [
            "claude-fable-5", "claude-opus-5", "claude-haiku-4-5-20251001",
        ]

    def test_discovered_models_reach_the_picker(self, make_client, stub_discovery):
        """A model released after DEFAULT_CLAUDE_MODELS still shows up."""
        from nerve.config import AgentConfig, NerveConfig

        stub_discovery([
            "claude-opus-5", "claude-fable-5", "claude-sonnet-4-6",
        ])
        cfg = NerveConfig(agent=AgentConfig.from_dict({"model": "claude-opus-5"}))
        data = make_client(cfg=cfg).get("/api/models").json()

        claude_ids = [
            m["id"] for m in data["models"] if m["backend"] == "claude"
        ]
        assert claude_ids == [
            "claude-opus-5", "claude-fable-5", "claude-sonnet-4-6",
        ]

    def test_configured_default_leads_the_discovered_list(
        self, make_client, stub_discovery,
    ):
        """agent.model always heads the picker, wherever it sits in the API order."""
        from nerve.config import AgentConfig, NerveConfig

        stub_discovery(["claude-opus-5", "claude-fable-5"])
        cfg = NerveConfig(agent=AgentConfig.from_dict({"model": "claude-fable-5"}))
        data = make_client(cfg=cfg).get("/api/models").json()

        claude_ids = [
            m["id"] for m in data["models"] if m["backend"] == "claude"
        ]
        assert claude_ids == ["claude-fable-5", "claude-opus-5"]
        assert data["defaults"]["claude"] == "claude-fable-5"

    def test_explicit_models_win_over_discovery(self, make_client, stub_discovery):
        from nerve.config import AgentConfig, NerveConfig

        stub_discovery(["claude-opus-5", "claude-fable-5", "claude-sonnet-4-6"])
        cfg = NerveConfig(agent=AgentConfig.from_dict({
            "model": "claude-opus-5",
            "models": ["claude-sonnet-4-6"],
        }))
        data = make_client(cfg=cfg).get("/api/models").json()

        claude_ids = [
            m["id"] for m in data["models"] if m["backend"] == "claude"
        ]
        assert claude_ids == ["claude-opus-5", "claude-sonnet-4-6"]

    def test_discovery_disabled_falls_back_to_builtin(
        self, make_client, stub_discovery,
    ):
        from nerve.config import DEFAULT_CLAUDE_MODELS, AgentConfig, NerveConfig

        stub_discovery(["claude-fable-5"])  # must be ignored
        cfg = NerveConfig(agent=AgentConfig.from_dict({
            "model": "claude-opus-5", "model_discovery": False,
        }))
        data = make_client(cfg=cfg).get("/api/models").json()

        claude_ids = [
            m["id"] for m in data["models"] if m["backend"] == "claude"
        ]
        assert claude_ids[0] == "claude-opus-5"
        assert "claude-fable-5" not in claude_ids
        assert set(DEFAULT_CLAUDE_MODELS) <= set(claude_ids)

    def test_discovery_failure_falls_back_to_builtin(self, make_client, monkeypatch):
        """An unreachable Models API must not empty the picker."""
        from nerve import models_catalog
        from nerve.config import DEFAULT_CLAUDE_MODELS

        async def _boom(config):
            return []

        monkeypatch.setattr(models_catalog, "get_models", _boom)
        data = make_client().get("/api/models").json()

        claude_ids = [
            m["id"] for m in data["models"] if m["backend"] == "claude"
        ]
        assert set(DEFAULT_CLAUDE_MODELS) <= set(claude_ids)

    def test_codex_models_from_preflight(self, make_client):
        client = make_client(codex_preflight={
            "available": True, "models": ["gpt-5.6-sol", "gpt-5.6-mini"],
        })
        data = client.get("/api/models").json()

        codex_models = [m for m in data["models"] if m["backend"] == "codex"]
        assert [m["id"] for m in codex_models] == ["gpt-5.6-sol", "gpt-5.6-mini"]
        assert all(m["provider"] == "openai" for m in codex_models)

    def test_codex_unavailable_lists_no_codex_models(self, make_client):
        client = make_client(codex_preflight={
            "available": False, "reason": "codex binary not found",
        })
        data = client.get("/api/models").json()

        assert [m for m in data["models"] if m["backend"] == "codex"] == []
        codex_opt = next(
            o for o in data["backends"]["options"] if o["id"] == "codex"
        )
        assert codex_opt["available"] is False
        assert codex_opt["reason"] == "codex binary not found"
