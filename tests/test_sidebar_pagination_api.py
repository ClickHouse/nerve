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
    async def setup(self, db: Database, bypass_auth):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import nerve.config as cfg_mod
        from nerve.config import NerveConfig
        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.sessions import router as sessions_router

        cfg = NerveConfig()
        cfg_mod._config = cfg

        sm = SessionManager(db)
        engine = SimpleNamespace(
            config=cfg, sessions=sm,
            # _decorate asks the engine for live background work per row.
            has_live_background_tasks=lambda sid: False,
        )
        init_deps(engine=engine, db=db)  # type: ignore[arg-type]

        app = FastAPI()
        app.include_router(sessions_router)
        bypass_auth(app)  # these routes are not about auth
        yield SimpleNamespace(client=TestClient(app), db=db, sm=sm, cfg=cfg)

        cfg_mod._config = None

    async def _seed(self, sm: SessionManager, db: Database, *, chats=0, crons=0,
                    archived=0, starred=0) -> None:
        for i in range(chats):
            await sm.get_or_create(f"chat-{i:03d}", source="web", actor=None)
        for i in range(crons):
            await sm.get_or_create(f"cron-{i:03d}", source="cron", actor=None)
        for i in range(archived):
            await sm.get_or_create(f"arch-{i:03d}", source="web", actor=None)
            await sm.archive_session(f"arch-{i:03d}")
        for i in range(starred):
            await sm.get_or_create(f"star-{i:03d}", source="web", actor=None)
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

    # ── Needs you ────────────────────────────────────────────────────────

    async def _ask(self, db: Database, nid: str, session_id: str, kind: str = "question") -> None:
        await db.create_notification(nid, session_id, kind, f"{kind} {nid}")

    async def _plan(self, db: Database, plan_id: str, session_id: str) -> None:
        await db.create_plan(plan_id, "task-1", "Do the thing.", session_id=session_id)

    async def test_every_row_carries_attention(self, setup):
        await self._seed(setup.sm, setup.db, chats=2)
        await self._ask(setup.db, "n1", "chat-000")
        await self._ask(setup.db, "n2", "chat-000")
        await self._ask(setup.db, "n3", "chat-000", kind="approval")
        await self._ask(setup.db, "n4", "chat-000", kind="notify")
        await self._plan(setup.db, "plan-1", "chat-000")

        rows = {s["id"]: s for s in setup.client.get("/api/sessions").json()["sessions"]}
        assert rows["chat-000"]["attention"] == {
            "questions": 2, "approvals": 1, "plans": 1,
            "awaiting_input": False, "review": False,
        }
        assert rows["chat-001"]["attention"] == {
            "questions": 0, "approvals": 0, "plans": 0,
            "awaiting_input": False, "review": False,
        }

    async def test_page_one_adds_system_sessions_that_need_you(self, setup):
        """A cron session with a pending plan is not in the feed, yet page 1 returns it."""
        await self._seed(setup.sm, setup.db, chats=2, crons=3)
        await self._plan(setup.db, "plan-1", "cron-001")

        body = setup.client.get("/api/sessions").json()
        ids = [s["id"] for s in body["sessions"]]
        assert sorted(ids) == ["chat-000", "chat-001", "cron-001"]
        cron = next(s for s in body["sessions"] if s["id"] == "cron-001")
        assert cron["attention"]["plans"] == 1
        # The group badge and the page window do not change.
        assert body["system_count"] == 3
        assert body["next_offset"] == 2

    async def test_page_one_adds_a_waiting_session_from_a_later_page(self, setup):
        setup.cfg.sessions.sidebar_page_size = 5
        await self._seed(setup.sm, setup.db, chats=8)
        page2 = setup.client.get("/api/sessions?offset=5").json()["sessions"]
        later = page2[-1]["id"]
        await self._ask(setup.db, "n1", later)

        body = setup.client.get("/api/sessions").json()
        assert later in {s["id"] for s in body["sessions"]}
        assert len(body["sessions"]) == 6
        # Later pages never add rows.
        again = setup.client.get("/api/sessions?offset=5").json()["sessions"]
        assert [s["id"] for s in again] == [s["id"] for s in page2]

    async def test_archived_and_unknown_sessions_are_not_added(self, setup):
        await self._seed(setup.sm, setup.db, chats=1, archived=1)
        await self._ask(setup.db, "n1", "arch-000")
        await self._ask(setup.db, "n2", "no-such-session")

        ids = [s["id"] for s in setup.client.get("/api/sessions").json()["sessions"]]
        assert ids == ["chat-000"]

    async def test_a_handled_item_leaves_needs_you(self, setup):
        await self._seed(setup.sm, setup.db, chats=1, crons=1)
        await self._ask(setup.db, "n1", "cron-000")
        await self._plan(setup.db, "plan-1", "cron-000")
        assert "cron-000" in {s["id"] for s in setup.client.get("/api/sessions").json()["sessions"]}

        await setup.db.answer_notification("n1", "yes", "web")
        await setup.db.update_plan("plan-1", status="implementing")

        ids = [s["id"] for s in setup.client.get("/api/sessions").json()["sessions"]]
        assert ids == ["chat-000"]

    async def test_awaiting_input_and_review_count_as_needs_you(self, setup, monkeypatch):
        import nerve.gateway.routes.sessions as routes

        await self._seed(setup.sm, setup.db, chats=1, crons=2)
        monkeypatch.setattr(routes, "get_awaiting_ids", lambda: {"cron-000"})

        async def loops(_deps):
            return {"cron-001": {"id": "rvl-1", "status": "awaiting_user"}}
        monkeypatch.setattr(routes, "_newest_review_loops", loops)

        rows = {s["id"]: s for s in setup.client.get("/api/sessions").json()["sessions"]}
        assert set(rows) == {"chat-000", "cron-000", "cron-001"}
        assert rows["cron-000"]["attention"]["awaiting_input"] is True
        assert rows["cron-000"]["awaiting_input"] is True
        assert rows["cron-001"]["attention"]["review"] is True
        assert rows["cron-001"]["review_loop"]["status"] == "awaiting_user"

    async def test_lazy_groups_carry_attention_without_extra_rows(self, setup):
        await self._seed(setup.sm, setup.db, chats=1, crons=1)
        await self._ask(setup.db, "n1", "chat-000", kind="approval")

        system = setup.client.get("/api/sessions/system").json()["sessions"]
        assert [s["id"] for s in system] == ["cron-000"]
        assert system[0]["attention"]["approvals"] == 0


@pytest.mark.asyncio
class TestAttentionCounts:
    async def test_pending_asks_are_counted_per_session_and_type(self, db: Database):
        for nid, sid, kind in [
            ("n1", "s1", "question"), ("n2", "s1", "question"), ("n3", "s1", "approval"),
            ("n4", "s2", "approval"), ("n5", "s2", "notify"), ("n6", "s3", "question"),
        ]:
            await db.create_notification(nid, sid, kind, "title")
        await db.update_notification("n6", status="expired")

        assert await db.count_pending_by_session() == {
            "s1": {"question": 2, "approval": 1},
            "s2": {"approval": 1},
        }

    async def test_pending_plans_are_counted_per_session(self, db: Database):
        await db.create_plan("p1", "t1", "c", session_id="s1")
        await db.create_plan("p2", "t2", "c", session_id="s1")
        await db.create_plan("p3", "t3", "c", session_id="s2")
        await db.create_plan("p4", "t4", "c")
        await db.update_plan("p3", status="declined")

        assert await db.count_pending_plans_by_session() == {"s1": 2}

    async def test_plan_count_reads_only_the_index(self, db: Database):
        async with db.db.execute(
            "EXPLAIN QUERY PLAN SELECT session_id, COUNT(*) FROM plans"
            " WHERE status = 'pending' AND session_id IS NOT NULL GROUP BY session_id"
        ) as cursor:
            plan = " ".join(str(row[-1]) for row in await cursor.fetchall())
        assert "COVERING INDEX idx_plans_status_session" in plan
