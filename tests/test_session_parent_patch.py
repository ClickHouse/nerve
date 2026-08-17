"""HTTP tests for the sidebar drag-to-nest PATCH (``parent_session_id``).

Covers the write path added for the session-hierarchy feature in
``gateway/routes/sessions.py``: setting a parent, clearing it (null →
top-level), and the guards — self-parent, unknown parent, cycle, and the
fork-window (a session still in ``created`` status can't be nested, so a
display drag never turns a not-yet-started session into a fork).

Harness mirrors TestSidebarListRoutes in test_sidebar_pagination_api.py: a
minimal FastAPI app with the real router, auth disabled via an empty jwt_secret.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import pytest_asyncio

from nerve.agent.sessions import SessionManager
from nerve.db import Database


@pytest.mark.asyncio
class TestSessionParentPatch:
    @pytest_asyncio.fixture
    async def setup(self, db: Database):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import nerve.config as cfg_mod
        from nerve.config import NerveConfig
        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.sessions import router as sessions_router

        cfg = NerveConfig()
        cfg.auth.jwt_secret = ""      # require_auth becomes a no-op
        cfg_mod._config = cfg

        sm = SessionManager(db)
        engine = SimpleNamespace(config=cfg, sessions=sm)
        init_deps(engine=engine, db=db)  # type: ignore[arg-type]

        app = FastAPI()
        app.include_router(sessions_router)
        yield SimpleNamespace(client=TestClient(app), db=db, sm=sm, cfg=cfg)

        cfg_mod._config = None

    async def _mk(self, setup, sid: str, *, started: bool = True) -> None:
        """Create a web session. By default move it out of 'created' so it is
        nestable — mirrors any session that has run at least once (the only
        kind the sidebar exposes as a stable, draggable row)."""
        await setup.sm.get_or_create(sid, source="web")
        if started:
            await setup.db.update_session_fields(sid, {"status": "idle"})

    async def test_set_then_clear_parent(self, setup):
        await self._mk(setup, "parent")
        await self._mk(setup, "child")

        r = setup.client.patch("/api/sessions/child", json={"parent_session_id": "parent"})
        assert r.status_code == 200
        assert r.json()["parent_session_id"] == "parent"

        # It also rides in the feed payload the sidebar reads (SELECT * → dict).
        feed = setup.client.get("/api/sessions").json()["sessions"]
        child = next(s for s in feed if s["id"] == "child")
        assert child["parent_session_id"] == "parent"

        # null clears it → back to top-level.
        r = setup.client.patch("/api/sessions/child", json={"parent_session_id": None})
        assert r.status_code == 200
        assert r.json()["parent_session_id"] is None

    async def test_self_parent_rejected(self, setup):
        await self._mk(setup, "solo")
        r = setup.client.patch("/api/sessions/solo", json={"parent_session_id": "solo"})
        assert r.status_code == 400

    async def test_unknown_parent_rejected(self, setup):
        await self._mk(setup, "child")
        r = setup.client.patch("/api/sessions/child", json={"parent_session_id": "ghost"})
        assert r.status_code == 404

    async def test_cycle_rejected(self, setup):
        # b becomes a child of a; making a a child of b would close a loop.
        await self._mk(setup, "a")
        await self._mk(setup, "b")
        assert setup.client.patch(
            "/api/sessions/b", json={"parent_session_id": "a"}).status_code == 200
        r = setup.client.patch("/api/sessions/a", json={"parent_session_id": "b"})
        assert r.status_code == 400

    async def test_created_session_cannot_be_nested(self, setup):
        # A brand-new session still in 'created' is fork-eligible; refuse to
        # re-parent it so a drag can never turn it into a fork.
        await self._mk(setup, "parent")
        await self._mk(setup, "fresh", started=False)  # stays 'created'
        r = setup.client.patch("/api/sessions/fresh", json={"parent_session_id": "parent"})
        assert r.status_code == 409

    async def test_deep_chain_allowed(self, setup):
        # Arbitrary depth is fine: a → b → c.
        for sid in ("a", "b", "c"):
            await self._mk(setup, sid)
        assert setup.client.patch(
            "/api/sessions/b", json={"parent_session_id": "a"}).status_code == 200
        assert setup.client.patch(
            "/api/sessions/c", json={"parent_session_id": "b"}).status_code == 200
        feed = {s["id"]: s for s in setup.client.get("/api/sessions").json()["sessions"]}
        assert feed["b"]["parent_session_id"] == "a"
        assert feed["c"]["parent_session_id"] == "b"
