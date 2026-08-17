"""HTTP tests for the sidebar's three lists (``gateway/routes/sessions.py``)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import pytest_asyncio

from nerve.agent.sessions import SessionManager
from nerve.db import Database


@pytest.mark.asyncio
class TestSidebarListRoutes:
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

    async def _seed(self, sm: SessionManager, db: Database, *, chats=0, crons=0,
                    archived=0, starred=0) -> None:
        for i in range(chats):
            await sm.get_or_create(f"chat-{i:03d}", source="web")
        for i in range(crons):
            await sm.get_or_create(f"cron-{i:03d}", source="cron")
        for i in range(archived):
            await sm.get_or_create(f"arch-{i:03d}", source="web")
            await sm.archive_session(f"arch-{i:03d}")
        for i in range(starred):
            await sm.get_or_create(f"star-{i:03d}", source="web")
            await db.update_session_fields(f"star-{i:03d}", {"starred": 1})

    # ── Default page size ────────────────────────────────────────────────

    async def test_default_page_size_is_fifty_and_paginates(self, setup):
        """Out of the box (no config), the feed pages at 50 and says so."""
        assert setup.cfg.sessions.sidebar_page_size == 50
        await self._seed(setup.sm, setup.db, chats=60)

        body = setup.client.get("/api/sessions").json()
        assert len(body["sessions"]) == 50
        assert body["has_more"] is True
        assert body["next_offset"] == 50

        rest = setup.client.get(f"/api/sessions?offset={body['next_offset']}").json()
        assert len(rest["sessions"]) == 10
        assert rest["has_more"] is False
        first_ids = {s["id"] for s in body["sessions"]}
        assert first_ids.isdisjoint({s["id"] for s in rest["sessions"]})
        assert len(first_ids | {s["id"] for s in rest["sessions"]}) == 60

    async def test_unlimited_only_when_configured(self, setup):
        """0 is opt-in: it returns everything and never offers another page."""
        await self._seed(setup.sm, setup.db, chats=60)
        setup.cfg.sessions.sidebar_page_size = 0

        body = setup.client.get("/api/sessions").json()
        assert len(body["sessions"]) == 60
        assert body["has_more"] is False

    async def test_cron_never_consumes_the_feed_window(self, setup):
        """The regression this rework fixes: cron rows are counted and paged separately, so they cannot displace conversations."""
        await self._seed(setup.sm, setup.db, chats=10, crons=200)

        body = setup.client.get("/api/sessions").json()
        assert len(body["sessions"]) == 10
        assert body["has_more"] is False
        assert all(s["source"] == "web" for s in body["sessions"])
        assert body["system_count"] == 200

    async def test_starred_ride_along_whole_on_page_one(self, setup):
        """Starred rows are off-budget: all of them, plus a full page."""
        setup.cfg.sessions.sidebar_page_size = 5
        await self._seed(setup.sm, setup.db, chats=12, starred=7)

        body = setup.client.get("/api/sessions").json()
        assert len([s for s in body["sessions"] if s["starred"]]) == 7
        assert len([s for s in body["sessions"] if not s["starred"]]) == 5
        assert body["has_more"] is True

        # Later pages are conversations only — starred are not resent.
        page2 = setup.client.get("/api/sessions?offset=5").json()
        assert all(not s["starred"] for s in page2["sessions"])

    # ── Lazy groups ──────────────────────────────────────────────────────

    async def test_counts_ride_on_the_feed_so_collapsed_groups_cost_nothing(self, setup):
        await self._seed(setup.sm, setup.db, chats=2, crons=3, archived=4)

        body = setup.client.get("/api/sessions").json()
        assert body["system_count"] == 3
        assert body["archived_count"] == 4
        # …and neither group's rows are in the feed.
        assert {s["id"] for s in body["sessions"]} == {"chat-000", "chat-001"}

    @pytest.mark.parametrize("group,total", [("system", 12), ("archived", 12)])
    async def test_group_pages_are_disjoint_and_terminate(self, setup, group, total):
        setup.cfg.sessions.sidebar_page_size = 5
        kwargs = {"crons": total} if group == "system" else {"archived": total}
        await self._seed(setup.sm, setup.db, **kwargs)

        seen: list[str] = []
        offset, guard = 0, 0
        while True:
            guard += 1
            assert guard < 10, "pagination did not terminate"
            page = setup.client.get(f"/api/sessions/{group}?offset={offset}").json()
            seen += [s["id"] for s in page["sessions"]]
            if not page["has_more"]:
                break
            offset = page["next_offset"]
        assert len(seen) == len(set(seen)) == total
