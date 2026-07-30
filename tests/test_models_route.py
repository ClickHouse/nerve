"""HTTP route tests for ``GET /api/models`` (gateway/routes/models.py).

Pattern mirrors test_workflow_routes.py: a minimal FastAPI app with the
real router, auth disabled via an empty jwt_secret, and a fake engine
carrying stub backends. The route must advertise the full selectable
Claude model list (config.claude_models) — not just the configured
default — so the composer's model picker renders for Claude sessions
the same way it does for Codex.
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
