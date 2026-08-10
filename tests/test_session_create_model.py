"""HTTP route tests for the composer model picker's session endpoints.

POST /api/sessions: the picker sends its choice at session creation so the
session row (and the header's model badge) carries the picked model from
the first render — instead of the backend default until the first turn
resolves the override.

PATCH /api/sessions/{id}: the picker re-points ONE existing session's
model (per-chat, not a global preference); the engine reads the row each
turn, so the switch applies on that session's next message only.

Pattern mirrors test_workflow_routes.py.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio

from nerve.agent.backends.base import BackendError
from nerve.db import Database


class FakeClaudeBackend:
    """Accepts any model — no validate_model, like the real claude backend."""

    name = "claude"

    def default_model(self, source: str) -> str:
        return "claude-fable-5"


class FakeCodexBackend:
    name = "codex"

    def default_model(self, source: str) -> str:
        return "gpt-5.6-sol"

    async def preflight(self) -> dict[str, Any]:
        return {"available": True, "models": ["gpt-5.6-sol"]}

    async def validate_model(self, model: str) -> None:
        if model != "gpt-5.6-sol":
            raise BackendError(f"Model {model!r} is not available")


class FakeSessionManager:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def get_or_create(self, session_id: str, **kwargs) -> dict:
        return await self.db.create_session(
            session_id,
            title=kwargs.get("title"),
            source=kwargs.get("source", "web"),
            metadata=kwargs.get("metadata"),
            backend=kwargs.get("backend", "claude"),
            model=kwargs.get("model"),
            cwd=kwargs.get("cwd"),
        )


@pytest_asyncio.fixture
async def setup(db: Database, tmp_path):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import nerve.config as cfg_mod
    from nerve.config import NerveConfig
    from nerve.gateway.routes._deps import init_deps
    from nerve.gateway.routes.sessions import router as sessions_router

    cfg = NerveConfig()
    cfg.workspace = tmp_path
    cfg.auth.jwt_secret = ""  # require_auth becomes a no-op
    cfg_mod._config = cfg

    engine = SimpleNamespace(
        _backends={
            "claude": FakeClaudeBackend(),
            "codex": FakeCodexBackend(),
        },
        config=cfg,
        sessions=FakeSessionManager(db),
    )
    init_deps(engine=engine, db=db)  # type: ignore[arg-type]

    app = FastAPI()
    app.include_router(sessions_router)
    yield SimpleNamespace(client=TestClient(app), db=db)

    cfg_mod._config = None


@pytest.mark.asyncio
class TestCreateSessionModel:
    async def test_model_persisted_at_creation(self, setup):
        resp = setup.client.post(
            "/api/sessions",
            json={"backend": "claude", "model": "claude-opus-5"},
        )
        assert resp.status_code == 200
        session = resp.json()
        assert session["model"] == "claude-opus-5"
        row = await setup.db.get_session(session["id"])
        assert row["model"] == "claude-opus-5"

    async def test_omitted_model_falls_back_to_backend_default(self, setup):
        resp = setup.client.post("/api/sessions", json={"backend": "claude"})
        assert resp.status_code == 200
        assert resp.json()["model"] == "claude-fable-5"

    async def test_blank_model_falls_back_to_backend_default(self, setup):
        resp = setup.client.post(
            "/api/sessions", json={"backend": "claude", "model": "   "},
        )
        assert resp.status_code == 200
        assert resp.json()["model"] == "claude-fable-5"

    async def test_codex_validates_model(self, setup):
        resp = setup.client.post(
            "/api/sessions",
            json={"backend": "codex", "model": "claude-opus-5"},
        )
        assert resp.status_code == 400
        assert "not available" in resp.json()["detail"]

    async def test_codex_accepts_known_model(self, setup):
        resp = setup.client.post(
            "/api/sessions",
            json={"backend": "codex", "model": "gpt-5.6-sol"},
        )
        assert resp.status_code == 200
        assert resp.json()["model"] == "gpt-5.6-sol"


@pytest.mark.asyncio
class TestPatchSessionModel:
    """PATCH /api/sessions/{id} with a model — the per-chat picker path."""

    async def _create(self, setup, backend: str = "claude", model: str | None = None) -> dict:
        payload: dict = {"backend": backend}
        if model:
            payload["model"] = model
        resp = setup.client.post("/api/sessions", json=payload)
        assert resp.status_code == 200
        return resp.json()

    async def test_patch_model_repoints_session(self, setup):
        session = await self._create(setup, model="claude-fable-5")
        resp = setup.client.patch(
            f"/api/sessions/{session['id']}", json={"model": "claude-opus-5"},
        )
        assert resp.status_code == 200
        assert resp.json()["model"] == "claude-opus-5"
        row = await setup.db.get_session(session["id"])
        assert row["model"] == "claude-opus-5"

    async def test_patch_model_only_touches_target_session(self, setup):
        """The regression this endpoint exists for: a pick in one chat must
        never change any other chat's model."""
        a = await self._create(setup, model="claude-fable-5")
        b = await self._create(setup, model="claude-fable-5")
        resp = setup.client.patch(
            f"/api/sessions/{a['id']}", json={"model": "claude-opus-5"},
        )
        assert resp.status_code == 200
        assert (await setup.db.get_session(a["id"]))["model"] == "claude-opus-5"
        assert (await setup.db.get_session(b["id"]))["model"] == "claude-fable-5"

    async def test_patch_blank_model_rejected(self, setup):
        session = await self._create(setup, model="claude-fable-5")
        resp = setup.client.patch(
            f"/api/sessions/{session['id']}", json={"model": "   "},
        )
        assert resp.status_code == 400
        assert (await setup.db.get_session(session["id"]))["model"] == "claude-fable-5"

    async def test_patch_model_validated_by_session_backend(self, setup):
        session = await self._create(setup, backend="codex", model="gpt-5.6-sol")
        resp = setup.client.patch(
            f"/api/sessions/{session['id']}", json={"model": "claude-opus-5"},
        )
        assert resp.status_code == 400
        assert "not available" in resp.json()["detail"]
        assert (await setup.db.get_session(session["id"]))["model"] == "gpt-5.6-sol"

    async def test_patch_model_codex_accepts_known(self, setup):
        session = await self._create(setup, backend="codex", model="gpt-5.6-sol")
        resp = setup.client.patch(
            f"/api/sessions/{session['id']}", json={"model": "gpt-5.6-sol"},
        )
        assert resp.status_code == 200
        assert resp.json()["model"] == "gpt-5.6-sol"

    async def test_patch_model_unknown_session_404(self, setup):
        resp = setup.client.patch(
            "/api/sessions/nonexist", json={"model": "claude-opus-5"},
        )
        assert resp.status_code == 404

    async def test_patch_title_alone_still_works(self, setup):
        session = await self._create(setup, model="claude-fable-5")
        resp = setup.client.patch(
            f"/api/sessions/{session['id']}", json={"title": "renamed"},
        )
        assert resp.status_code == 200
        row = await setup.db.get_session(session["id"])
        assert row["title"] == "renamed"
        assert row["model"] == "claude-fable-5"
