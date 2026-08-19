"""Tests for *parked* sessions — pending work with no turn in flight.

A session can finish a turn and still not be done: a wakeup is scheduled,
or a background task is running inside its CLI. The sidebar keeps such a
session in its "Running" group (with its own indicator colour) instead of
letting it fall back to idle, and the continuation turn must not re-sort
the list under the user.

Three layers:
  * ``MessageStore.add_message(bump_updated_at=False)`` — record a message
    without moving the session's sidebar position.
  * ``AgentEngine.pending_wakeup_at`` / ``_broadcast_session_running`` —
    the live signal shipped with every run-state transition.
  * ``GET /api/sessions`` — the same two bits on every sidebar row, so a
    reload rehydrates the parked state.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from nerve.agent.engine import AgentEngine
from nerve.agent.sessions import SessionManager
from nerve.db import Database


def _future(minutes: int = 10) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()


# --------------------------------------------------------------------------- #
#  add_message(bump_updated_at=...)                                            #
# --------------------------------------------------------------------------- #

class TestUpdatedAtFreeze:
    @pytest_asyncio.fixture
    async def seeded(self, db: Database):
        await db.create_session("s1", source="web")
        await db.add_message("s1", "user", "hello")
        return db

    @pytest.mark.asyncio
    async def test_default_bumps_updated_at(self, seeded):
        before = (await seeded.get_session("s1"))["updated_at"]
        await seeded.add_message("s1", "assistant", "hi")
        after = await seeded.get_session("s1")
        assert after["updated_at"] >= before
        assert after["message_count"] == 2

    @pytest.mark.asyncio
    async def test_frozen_message_keeps_sidebar_position(self, seeded):
        before = await seeded.get_session("s1")
        await seeded.add_message(
            "s1", "assistant", "woke up on my own", bump_updated_at=False,
        )
        after = await seeded.get_session("s1")
        # The row is real (it counts) but the "last activity" clock is untouched.
        assert after["updated_at"] == before["updated_at"]
        assert after["message_count"] == before["message_count"] + 1
        assert len(await seeded.get_messages("s1")) == 2

    @pytest.mark.asyncio
    async def test_frozen_turn_does_not_reorder_the_feed(self, db: Database):
        """The whole point: a continuation must not jump the queue."""
        sm = SessionManager(db)
        for sid in ("older", "newer"):
            await sm.get_or_create(sid, source="web")
            await db.add_message(sid, "user", "hi")

        def ids() -> list[str]:
            return [s["id"] for s in order]

        order = await db.list_conversation_sessions()
        assert ids() == ["newer", "older"]

        # A fired wakeup continues "older" — it stays put...
        await db.add_message(
            "older", "assistant", "loop tick", bump_updated_at=False,
        )
        order = await db.list_conversation_sessions()
        assert ids() == ["newer", "older"]

        # ...while a turn the user actually asked for still floats it up.
        await db.add_message("older", "assistant", "answer")
        order = await db.list_conversation_sessions()
        assert ids() == ["older", "newer"]


# --------------------------------------------------------------------------- #
#  Engine: pending-work signal                                                 #
# --------------------------------------------------------------------------- #

class TestEnginePendingWork:
    @pytest_asyncio.fixture
    async def engine(self, db: Database):
        eng = AgentEngine.__new__(AgentEngine)
        eng.db = db
        eng._bg_task_registry = {}
        await db.create_session("s1", source="web")
        return eng

    @pytest.mark.asyncio
    async def test_pending_wakeup_at_none_when_nothing_scheduled(self, engine):
        assert await engine.pending_wakeup_at("s1") is None

    @pytest.mark.asyncio
    async def test_pending_wakeup_at_returns_fire_time(self, engine):
        fire_at = _future()
        await engine.db.add_wakeup("s1", prompt="tick", fire_at=fire_at)
        assert await engine.pending_wakeup_at("s1") == fire_at

    @pytest.mark.asyncio
    async def test_fired_wakeup_no_longer_pending(self, engine):
        wid = await engine.db.add_wakeup("s1", prompt="tick", fire_at=_future())
        await engine.db.claim_wakeup(wid)
        assert await engine.pending_wakeup_at("s1") is None

    @pytest.mark.asyncio
    async def test_lookup_failure_degrades_to_none(self, engine):
        """An enrichment read must never take down a listing or a broadcast."""
        engine.db = SimpleNamespace(
            list_pending_wakeups=AsyncMock(side_effect=RuntimeError("db gone")),
        )
        assert await engine.pending_wakeup_at("s1") is None

    @pytest.mark.asyncio
    async def test_broadcast_carries_pending_work(self, engine):
        await engine.db.add_wakeup("s1", prompt="tick", fire_at=_future())
        engine._bg_task_registry["s1"] = {
            "t1": {"task_id": "t1", "status": "running"},
        }
        events: list[dict] = []
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast = AsyncMock(side_effect=lambda ch, m: events.append((ch, m)))
            await engine._broadcast_session_running("s1", False)

        channel, msg = events[0]
        assert channel == "__global__"
        assert msg["type"] == "session_running"
        assert msg["is_running"] is False
        assert msg["pending_wakeup_at"] is not None
        assert msg["has_background_tasks"] is True

    @pytest.mark.asyncio
    async def test_broadcast_reports_idle_when_nothing_pending(self, engine):
        events: list[dict] = []
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast = AsyncMock(side_effect=lambda ch, m: events.append(m))
            await engine._broadcast_session_running("s1", False)

        assert events[0]["pending_wakeup_at"] is None
        assert events[0]["has_background_tasks"] is False

    @pytest.mark.asyncio
    async def test_settled_background_task_is_not_live(self, engine):
        engine._bg_task_registry["s1"] = {
            "t1": {"task_id": "t1", "status": "completed"},
        }
        assert engine.has_live_background_tasks("s1") is False


# --------------------------------------------------------------------------- #
#  Sidebar rows expose the parked state                                        #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
class TestSidebarParkedFields:
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
        live_bg: set[str] = set()
        engine = SimpleNamespace(
            config=cfg, sessions=sm,
            has_live_background_tasks=lambda sid: sid in live_bg,
        )
        init_deps(engine=engine, db=db)  # type: ignore[arg-type]

        app = FastAPI()
        app.include_router(sessions_router)
        yield SimpleNamespace(
            client=TestClient(app), db=db, sm=sm, cfg=cfg, live_bg=live_bg,
        )

        cfg_mod._config = None

    async def test_rows_report_no_pending_work_by_default(self, setup):
        await setup.sm.get_or_create("plain", source="web")
        row = setup.client.get("/api/sessions").json()["sessions"][0]
        assert row["pending_wakeup_at"] is None
        assert row["has_background_tasks"] is False

    async def test_scheduled_wakeup_rides_on_the_row(self, setup):
        await setup.sm.get_or_create("looping", source="web")
        fire_at = _future()
        await setup.db.add_wakeup("looping", prompt="tick", fire_at=fire_at)

        rows = {s["id"]: s for s in setup.client.get("/api/sessions").json()["sessions"]}
        assert rows["looping"]["pending_wakeup_at"] == fire_at
        # Not executing right now — the row is parked, not running.
        assert rows["looping"]["is_running"] is False

    async def test_background_task_rides_on_the_row(self, setup):
        await setup.sm.get_or_create("busy", source="web")
        setup.live_bg.add("busy")

        rows = {s["id"]: s for s in setup.client.get("/api/sessions").json()["sessions"]}
        assert rows["busy"]["has_background_tasks"] is True

    async def test_search_results_carry_the_same_bits(self, setup):
        await setup.sm.get_or_create("findme", source="web")
        await setup.db.update_session_title("findme", "findme")
        await setup.db.add_wakeup("findme", prompt="tick", fire_at=_future())

        rows = setup.client.get("/api/sessions/search?q=findme").json()["sessions"]
        assert rows and rows[0]["pending_wakeup_at"] is not None
        assert rows[0]["has_background_tasks"] is False

    async def test_system_sessions_carry_the_same_bits(self, setup):
        await setup.sm.get_or_create("cron-1", source="cron")
        await setup.db.add_wakeup("cron-1", prompt="tick", fire_at=_future())

        rows = setup.client.get("/api/sessions/system").json()["sessions"]
        assert rows and rows[0]["pending_wakeup_at"] is not None
