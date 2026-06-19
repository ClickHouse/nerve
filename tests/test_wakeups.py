"""Tests for the ScheduleWakeup harness.

Covers three layers:
  * ``AgentEngine._wakeup_fire_at`` — delaySeconds clamping.
  * ``WakeupStore`` — persistence, de-dup, atomic claim, FK cascade.
  * ``CronService._sweep_wakeups`` — firing due wakeups via engine.run,
    skipping busy sessions, and not double-firing.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve.agent.engine import AgentEngine
from nerve.cron.service import CronService, _resolve_wakeup_prompt


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _past() -> str:
    return _iso(datetime.now(timezone.utc) - timedelta(minutes=5))


def _future() -> str:
    return _iso(datetime.now(timezone.utc) + timedelta(hours=1))


# --------------------------------------------------------------------------- #
#  _wakeup_fire_at — clamping                                                  #
# --------------------------------------------------------------------------- #

class TestWakeupFireAt:
    @staticmethod
    def _delay(value) -> float:
        fire_at = AgentEngine._wakeup_fire_at(value)
        return (datetime.fromisoformat(fire_at) - datetime.now(timezone.utc)).total_seconds()

    @pytest.mark.parametrize("value,expected", [
        (30, 60),        # below min -> clamp up
        (60, 60),        # exact min
        (120, 120),      # in range
        (3600, 3600),    # exact max
        (5000, 3600),    # above max -> clamp down
        (90.7, 91),      # rounded
    ])
    def test_numeric_clamping(self, value, expected):
        assert abs(self._delay(value) - expected) <= 2

    @pytest.mark.parametrize("value,expected", [
        (float("inf"), 3600),
        (float("-inf"), 60),
        (float("nan"), 60),
        (None, 60),
        ("not-a-number", 60),
    ])
    def test_non_finite_and_invalid(self, value, expected):
        assert abs(self._delay(value) - expected) <= 2

    def test_returns_iso_utc(self):
        fire_at = AgentEngine._wakeup_fire_at(120)
        parsed = datetime.fromisoformat(fire_at)
        assert parsed.tzinfo is not None
        assert parsed.utcoffset() == timedelta(0)


# --------------------------------------------------------------------------- #
#  WakeupStore                                                                 #
# --------------------------------------------------------------------------- #

class TestWakeupStore:
    @pytest_asyncio.fixture
    async def seeded_db(self, db):
        await db.create_session("s1", source="web")
        await db.create_session("s2", source="web")
        return db

    @pytest.mark.asyncio
    async def test_add_and_get_due(self, seeded_db):
        await seeded_db.add_wakeup("s1", prompt="ping", fire_at=_past(), reason="r")
        await seeded_db.add_wakeup("s2", prompt="later", fire_at=_future())

        due = await seeded_db.get_due_wakeups(_iso(datetime.now(timezone.utc)))
        assert len(due) == 1
        assert due[0]["session_id"] == "s1"
        assert due[0]["prompt"] == "ping"
        assert due[0]["reason"] == "r"
        assert due[0]["status"] == "pending"

    @pytest.mark.asyncio
    async def test_add_replaces_prior_pending(self, seeded_db):
        await seeded_db.add_wakeup("s1", prompt="first", fire_at=_past())
        await seeded_db.add_wakeup("s1", prompt="second", fire_at=_past())

        pending = await seeded_db.list_pending_wakeups("s1")
        assert len(pending) == 1
        assert pending[0]["prompt"] == "second"

    @pytest.mark.asyncio
    async def test_claim_is_atomic(self, seeded_db):
        wid = await seeded_db.add_wakeup("s1", prompt="ping", fire_at=_past())
        assert await seeded_db.claim_wakeup(wid) is True
        # Second claim of the same row loses — guarantees single fire.
        assert await seeded_db.claim_wakeup(wid) is False
        # No longer pending / not returned as due.
        assert await seeded_db.get_due_wakeups(_iso(datetime.now(timezone.utc))) == []

    @pytest.mark.asyncio
    async def test_cancel_for_session(self, seeded_db):
        await seeded_db.add_wakeup("s1", prompt="ping", fire_at=_past())
        removed = await seeded_db.cancel_wakeups_for_session("s1")
        assert removed == 1
        assert await seeded_db.list_pending_wakeups("s1") == []

    @pytest.mark.asyncio
    async def test_list_pending_scoping(self, seeded_db):
        await seeded_db.add_wakeup("s1", prompt="a", fire_at=_future())
        await seeded_db.add_wakeup("s2", prompt="b", fire_at=_future())
        assert len(await seeded_db.list_pending_wakeups()) == 2
        assert len(await seeded_db.list_pending_wakeups("s1")) == 1

    @pytest.mark.asyncio
    async def test_cascade_on_session_delete(self, seeded_db):
        await seeded_db.add_wakeup("s1", prompt="ping", fire_at=_past())
        await seeded_db.db.execute("DELETE FROM sessions WHERE id = ?", ("s1",))
        await seeded_db.db.commit()
        assert await seeded_db.list_pending_wakeups() == []


# --------------------------------------------------------------------------- #
#  CronService._sweep_wakeups                                                  #
# --------------------------------------------------------------------------- #

class TestWakeupSweep:
    @pytest_asyncio.fixture
    async def svc(self, db):
        await db.create_session("s1", source="web")
        config = MagicMock()
        engine = AsyncMock()
        # is_running is synchronous — default to "not running".
        engine.sessions = MagicMock()
        engine.sessions.is_running = MagicMock(return_value=False)
        engine.run = AsyncMock(return_value="ok")
        return CronService(config, engine, db)

    @pytest.mark.asyncio
    async def test_fires_due_wakeup(self, svc):
        await svc.db.add_wakeup("s1", prompt="do the thing", fire_at=_past())

        await svc._sweep_wakeups()
        await asyncio.sleep(0.05)  # let the dispatched run task execute

        svc.engine.run.assert_awaited_once()
        kwargs = svc.engine.run.await_args.kwargs
        assert kwargs["session_id"] == "s1"
        assert kwargs["user_message"] == "do the thing"
        assert kwargs["source"] == "wakeup"
        assert kwargs["internal"] is True
        # Claimed — no longer pending.
        assert await svc.db.list_pending_wakeups("s1") == []

    @pytest.mark.asyncio
    async def test_skips_running_session(self, svc):
        svc.engine.sessions.is_running = MagicMock(return_value=True)
        await svc.db.add_wakeup("s1", prompt="ping", fire_at=_past())

        await svc._sweep_wakeups()
        await asyncio.sleep(0.05)

        svc.engine.run.assert_not_called()
        # Still pending — retried on a later sweep when the session is free.
        assert len(await svc.db.list_pending_wakeups("s1")) == 1

    @pytest.mark.asyncio
    async def test_ignores_future_wakeup(self, svc):
        await svc.db.add_wakeup("s1", prompt="ping", fire_at=_future())
        await svc._sweep_wakeups()
        await asyncio.sleep(0.05)
        svc.engine.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_double_fire_across_sweeps(self, svc):
        await svc.db.add_wakeup("s1", prompt="ping", fire_at=_past())
        await svc._sweep_wakeups()
        await svc._sweep_wakeups()  # second sweep finds nothing to fire
        await asyncio.sleep(0.05)
        svc.engine.run.assert_awaited_once()


class TestRecordWakeup:
    """The capture path the PostToolUse hook delegates to."""

    @pytest_asyncio.fixture
    async def seeded_db(self, db):
        await db.create_session("s1", source="web")
        return db

    @pytest.mark.asyncio
    async def test_records_from_tool_input(self, seeded_db):
        wid = await AgentEngine._record_wakeup(
            seeded_db, "s1",
            {"delaySeconds": 120, "prompt": "keep going", "reason": "loop"},
        )
        assert wid is not None
        pending = await seeded_db.list_pending_wakeups("s1")
        assert len(pending) == 1
        assert pending[0]["prompt"] == "keep going"
        assert pending[0]["reason"] == "loop"
        # fire_at ~120s out, clamped within [60, 3600].
        delta = (datetime.fromisoformat(pending[0]["fire_at"]) - datetime.now(timezone.utc)).total_seconds()
        assert 60 <= delta <= 3600

    @pytest.mark.asyncio
    async def test_empty_prompt_is_ignored(self, seeded_db):
        wid = await AgentEngine._record_wakeup(seeded_db, "s1", {"delaySeconds": 60, "prompt": "   "})
        assert wid is None
        assert await seeded_db.list_pending_wakeups("s1") == []

    @pytest.mark.asyncio
    async def test_missing_delay_defaults_to_min(self, seeded_db):
        await AgentEngine._record_wakeup(seeded_db, "s1", {"prompt": "ping"})
        pending = await seeded_db.list_pending_wakeups("s1")
        delta = (datetime.fromisoformat(pending[0]["fire_at"]) - datetime.now(timezone.utc)).total_seconds()
        assert abs(delta - 60) <= 2


class TestResolveWakeupPrompt:
    def test_passthrough(self):
        assert _resolve_wakeup_prompt("keep monitoring X") == "keep monitoring X"

    @pytest.mark.parametrize("sentinel", [
        "<<autonomous-loop>>",
        "<<autonomous-loop-dynamic>>",
    ])
    def test_sentinels_resolved(self, sentinel):
        resolved = _resolve_wakeup_prompt(sentinel)
        assert resolved != sentinel
        assert "wakeup" in resolved.lower()
