"""Tests for notification silences — deterministic alert suppression.

Covers the v035 store, the service-level matcher + silence/force branches,
the agent feedback loop on the ``notify`` tool, the ``notification_silence``
management tool (+ cache invalidation), and the questions/approvals
exemption.

Runs against a fresh per-test SQLite (the shared ``db`` fixture) with the
streaming broadcaster + agent engine stubbed so behavior is asserted in
isolation. ``_fanout`` is mocked to detect (non-)delivery without touching
any channel.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve.agent.tools.handlers.notifications import (
    notification_silence_handler,
    notify_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig, NotificationsConfig
from nerve.db import Database
from nerve.notifications.service import NotificationService


# ----------------------------------------------------------------------
#  Fixtures
# ----------------------------------------------------------------------


@pytest.fixture
def fake_config(tmp_path) -> NerveConfig:
    cfg = NerveConfig()
    cfg.workspace = tmp_path
    cfg.notifications = NotificationsConfig(
        channels=["web"],
        telegram_chat_id=None,
        default_expiry_hours=48,
        priority_prefixes={"high": "", "urgent": ""},
    )
    return cfg


@pytest.fixture
def fake_engine() -> MagicMock:
    engine = MagicMock()
    engine.sessions = MagicMock()
    engine.sessions.is_running.return_value = False
    engine.router = MagicMock()
    engine.router.get_channel.return_value = None
    engine.run = AsyncMock()
    return engine


@pytest.fixture
def patch_broadcaster(monkeypatch) -> list:
    captured: list = []

    class _FakeBroadcaster:
        async def broadcast(self, channel: str, message: dict) -> None:
            captured.append((channel, message))

    from nerve.agent import streaming
    monkeypatch.setattr(streaming, "broadcaster", _FakeBroadcaster())
    return captured


@pytest_asyncio.fixture
async def svc(db: Database, fake_config, fake_engine, patch_broadcaster):
    await db.create_session("s1")
    return NotificationService(fake_config, db, fake_engine)


def _meta(notif: dict) -> dict:
    raw = notif.get("metadata")
    if not raw:
        return {}
    return raw if isinstance(raw, dict) else json.loads(raw)


# ----------------------------------------------------------------------
#  Schema / store
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestSchemaAndStore:
    async def test_v035_table_exists(self, db: Database):
        async with db.db.execute(
            "PRAGMA table_info(notification_silences)"
        ) as cur:
            cols = {row[1] async for row in cur}
        for expected in (
            "id", "pattern", "action", "reason", "created_by", "created_at",
            "expires_at", "hit_count", "last_hit_at", "override_count",
            "last_override_at", "enabled",
        ):
            assert expected in cols

    async def test_create_and_get_silence(self, db: Database):
        row = await db.create_silence(
            silence_id="sil-1", pattern="foo", reason="benign",
            created_by="s1",
        )
        assert row["id"] == "sil-1"
        assert row["pattern"] == "foo"
        assert row["hit_count"] == 0
        assert row["override_count"] == 0
        assert row["enabled"] == 1
        assert row["expires_at"] is None

    async def test_record_hit_returns_postincrement(self, db: Database):
        await db.create_silence(silence_id="sil-1", pattern="x")
        assert await db.record_silence_hit("sil-1") == 1
        assert await db.record_silence_hit("sil-1") == 2
        row = await db.get_silence("sil-1")
        assert row["hit_count"] == 2
        assert row["last_hit_at"] is not None

    async def test_record_override_returns_postincrement(self, db: Database):
        await db.create_silence(silence_id="sil-1", pattern="x")
        assert await db.record_silence_override("sil-1") == 1
        row = await db.get_silence("sil-1")
        assert row["override_count"] == 1
        assert row["last_override_at"] is not None

    async def test_delete_silence(self, db: Database):
        await db.create_silence(silence_id="sil-1", pattern="x")
        assert await db.delete_silence("sil-1") is True
        assert await db.get_silence("sil-1") is None
        assert await db.delete_silence("sil-1") is False

    async def test_get_active_skips_expired_and_disabled(self, db: Database):
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        await db.create_silence(silence_id="sil-live", pattern="a")
        await db.create_silence(
            silence_id="sil-expired", pattern="b", expires_at=past,
        )
        await db.create_silence(silence_id="sil-off", pattern="c")
        await db.db.execute(
            "UPDATE notification_silences SET enabled = 0 WHERE id = ?",
            ("sil-off",),
        )
        await db.db.commit()
        active = {r["id"] for r in await db.get_active_silences()}
        assert active == {"sil-live"}

    async def test_create_notification_status_default_is_pending(self, db: Database):
        await db.create_session("s2")
        await db.create_notification(
            notification_id="n1", session_id="s2", type="notify", title="t",
        )
        notif = await db.get_notification("n1")
        assert notif["status"] == "pending"

    async def test_create_notification_explicit_status(self, db: Database):
        await db.create_session("s2")
        await db.create_notification(
            notification_id="n1", session_id="s2", type="notify", title="t",
            status="silenced",
        )
        notif = await db.get_notification("n1")
        assert notif["status"] == "silenced"


# ----------------------------------------------------------------------
#  Matcher
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestMatcher:
    async def test_case_insensitive(self, svc, db: Database):
        await db.create_silence(silence_id="sil-1", pattern="widget")
        match = await svc._match_silence("WIDGET alert", "")
        assert match is not None
        assert match["id"] == "sil-1"

    async def test_matches_body_not_just_title(self, svc, db: Database):
        await db.create_silence(silence_id="sil-1", pattern="staging")
        assert await svc._match_silence("sign-in", "device from staging") is not None

    async def test_no_match_returns_none(self, svc, db: Database):
        await db.create_silence(silence_id="sil-1", pattern="widget")
        assert await svc._match_silence("payment failed", "urgent") is None

    async def test_first_rule_wins(self, svc, db: Database):
        # sil-a created first → oldest-first precedence → it wins.
        await db.create_silence(silence_id="sil-a", pattern="alpha")
        await db.create_silence(silence_id="sil-b", pattern="alpha.*beta")
        match = await svc._match_silence("alpha beta", "")
        assert match["id"] == "sil-a"

    async def test_expired_rule_not_matched(self, svc, db: Database):
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        await db.create_silence(
            silence_id="sil-1", pattern="widget", expires_at=past,
        )
        assert await svc._match_silence("widget", "") is None

    async def test_invalid_regex_fails_open(self, svc, db: Database):
        # A pattern that fails to compile is dropped, never blocks delivery.
        await db.create_silence(silence_id="sil-bad", pattern="(unclosed")
        await db.create_silence(silence_id="sil-ok", pattern="widget")
        match = await svc._match_silence("widget", "")
        assert match is not None
        assert match["id"] == "sil-ok"

    async def test_cache_invalidation(self, svc, db: Database):
        # Prime the (empty) cache, then add a rule + invalidate.
        assert await svc._match_silence("widget", "") is None
        await db.create_silence(silence_id="sil-1", pattern="widget")
        # Stale cache: still no match until invalidated.
        assert await svc._match_silence("widget", "") is None
        svc.invalidate_silence_cache()
        assert await svc._match_silence("widget", "") is not None


# ----------------------------------------------------------------------
#  Silence path (force=False, match)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestSilencePath:
    async def test_silenced_row_persisted_not_delivered(
        self, svc, db: Database, patch_broadcaster,
    ):
        await db.create_silence(
            silence_id="sil-1", pattern="widget", reason="known benign",
        )
        svc._fanout = AsyncMock()
        nid = await svc.send_notification(
            session_id="s1", title="Widget alert", body="from staging",
            priority="high",
        )
        notif = await db.get_notification(nid)
        assert notif["status"] == "silenced"
        # priority is NEVER modified
        assert notif["priority"] == "high"
        # not delivered: _fanout never invoked
        svc._fanout.assert_not_called()
        # metadata carries the full match context
        meta = _meta(notif)
        assert meta["silenced_by"] == "sil-1"
        assert meta["silence_reason"] == "known benign"
        assert meta["silence_action"] == "silence"
        assert meta["silence_pattern"] == "widget"
        assert meta["silence_hit_count"] == 1

    async def test_hit_count_increments(self, svc, db: Database, patch_broadcaster):
        await db.create_silence(silence_id="sil-1", pattern="widget")
        svc._fanout = AsyncMock()
        await svc.send_notification(session_id="s1", title="widget", body="")
        n2 = await svc.send_notification(session_id="s1", title="widget x", body="")
        assert _meta(await db.get_notification(n2))["silence_hit_count"] == 2
        assert (await db.get_silence("sil-1"))["hit_count"] == 2

    async def test_web_broadcast_emitted_silenced(
        self, svc, db: Database, patch_broadcaster,
    ):
        await db.create_silence(silence_id="sil-1", pattern="widget", reason="r")
        svc._fanout = AsyncMock()
        nid = await svc.send_notification(session_id="s1", title="widget", body="")
        msgs = [m for _, m in patch_broadcaster if m.get("type") == "notification"]
        assert msgs
        assert msgs[0]["silenced"] is True
        assert msgs[0]["notification_id"] == nid
        assert msgs[0]["silence_reason"] == "r"
        # marked delivered to web so the row appears in the web list
        row = await db.get_notification(nid)
        assert json.loads(row["channels_delivered"]) == ["web"]


# ----------------------------------------------------------------------
#  Force path (force=True)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestForcePath:
    async def test_force_delivers_over_match(
        self, svc, db: Database, patch_broadcaster,
    ):
        await db.create_silence(silence_id="sil-1", pattern="widget")
        svc._fanout = AsyncMock()
        nid = await svc.send_notification(
            session_id="s1", title="widget", body="", force=True,
        )
        notif = await db.get_notification(nid)
        # delivered (not silenced)
        assert notif["status"] != "silenced"
        svc._fanout.assert_called_once()
        # override recorded + stamped
        meta = _meta(notif)
        assert meta["force_sent_over_silence"] == "sil-1"
        assert meta["force_override_count"] == 1
        assert (await db.get_silence("sil-1"))["override_count"] == 1

    async def test_force_no_match_no_override(
        self, svc, db: Database, patch_broadcaster,
    ):
        await db.create_silence(silence_id="sil-1", pattern="widget")
        svc._fanout = AsyncMock()
        nid = await svc.send_notification(
            session_id="s1", title="payment failed", body="", force=True,
        )
        notif = await db.get_notification(nid)
        assert notif["status"] != "silenced"
        svc._fanout.assert_called_once()
        assert _meta(notif) == {}
        assert (await db.get_silence("sil-1"))["override_count"] == 0

    async def test_normal_send_no_match_delivers(
        self, svc, db: Database, patch_broadcaster,
    ):
        svc._fanout = AsyncMock()
        nid = await svc.send_notification(session_id="s1", title="hello", body="")
        notif = await db.get_notification(nid)
        assert notif["status"] == "pending"
        svc._fanout.assert_called_once()


# ----------------------------------------------------------------------
#  Exemptions: questions / approvals are never silenced
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestExemptions:
    async def test_question_not_silenced(self, svc, db: Database, patch_broadcaster):
        await db.create_silence(silence_id="sil-1", pattern="widget")
        svc._fanout = AsyncMock()
        result = await svc.ask_question(
            session_id="s1", title="widget?", body="confirm",
        )
        notif = await db.get_notification(result["notification_id"])
        assert notif["type"] == "question"
        assert notif["status"] == "pending"
        svc._fanout.assert_called_once()

    async def test_approval_not_silenced(self, svc, db: Database, patch_broadcaster):
        await db.create_silence(silence_id="sil-1", pattern="widget")
        svc._fanout = AsyncMock()
        result = await svc.propose_action(
            session_id="s1", target_kind="mechanical-action",
            target_id="t-1", title="widget",
        )
        notif = await db.get_notification(result["notification_id"])
        assert notif["type"] == "approval"
        assert notif["status"] == "pending"
        svc._fanout.assert_called_once()


# ----------------------------------------------------------------------
#  notify tool: agent feedback loop
# ----------------------------------------------------------------------


def _ctx(svc, db) -> ToolContext:
    return ToolContext(session_id="s1", db=db, notification_service=svc)


def _text(result) -> str:
    return result.content[0]["text"]


@pytest.mark.asyncio
class TestNotifyToolFeedback:
    async def test_plain_send_returns_sent(self, svc, db: Database, patch_broadcaster):
        svc._fanout = AsyncMock()
        result = await notify_handler(_ctx(svc, db), {"body": "all good"})
        assert "Notification sent" in _text(result)
        assert "SILENCED" not in _text(result)

    async def test_silenced_send_returns_force_instruction(
        self, svc, db: Database, patch_broadcaster,
    ):
        await db.create_silence(
            silence_id="sil-1", pattern="widget", reason="known benign",
        )
        svc._fanout = AsyncMock()
        result = await notify_handler(
            _ctx(svc, db), {"title": "Widget alert", "body": "staging"},
        )
        text = _text(result)
        assert "SILENCED" in text
        assert "sil-1" in text
        assert "known benign" in text
        assert "widget" in text
        assert "force=true" in text

    async def test_force_over_match_returns_override_confirmation(
        self, svc, db: Database, patch_broadcaster,
    ):
        await db.create_silence(silence_id="sil-1", pattern="widget")
        svc._fanout = AsyncMock()
        result = await notify_handler(
            _ctx(svc, db),
            {"title": "widget", "body": "x", "force": True},
        )
        text = _text(result)
        assert "force-delivered over silence sil-1" in text
        assert "override #1" in text


# ----------------------------------------------------------------------
#  notification_silence management tool (+ cache invalidation)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestSilenceTool:
    async def test_add_creates_rule_and_invalidates_cache(
        self, svc, db: Database, patch_broadcaster,
    ):
        # Prime cache empty.
        assert await svc._match_silence("widget", "") is None
        result = await notification_silence_handler(
            _ctx(svc, db),
            {"op": "add", "pattern": "widget", "reason": "benign"},
        )
        assert "Silence created" in _text(result)
        # Cache invalidated by the tool → rule effective immediately.
        assert await svc._match_silence("widget", "") is not None
        assert len(await db.list_silences()) == 1

    async def test_add_rejects_invalid_regex(self, svc, db: Database):
        result = await notification_silence_handler(
            _ctx(svc, db), {"op": "add", "pattern": "(unclosed"},
        )
        assert "invalid regex" in _text(result)
        assert await db.list_silences() == []

    async def test_add_requires_pattern(self, svc, db: Database):
        result = await notification_silence_handler(
            _ctx(svc, db), {"op": "add", "pattern": ""},
        )
        assert "required" in _text(result)

    async def test_add_with_ttl_sets_expiry(self, svc, db: Database):
        await notification_silence_handler(
            _ctx(svc, db),
            {"op": "add", "pattern": "x", "ttl_hours": 5},
        )
        rows = await db.list_silences()
        assert rows[0]["expires_at"] is not None

    async def test_add_example_echo(self, svc, db: Database):
        result = await notification_silence_handler(
            _ctx(svc, db),
            {"op": "add", "pattern": "widget", "example": "Widget alert"},
        )
        assert "WOULD match" in _text(result)

    async def test_list_shows_counts(self, svc, db: Database):
        await db.create_silence(silence_id="sil-1", pattern="widget", reason="r")
        await db.record_silence_hit("sil-1")
        result = await notification_silence_handler(_ctx(svc, db), {"op": "list"})
        text = _text(result)
        assert "sil-1" in text
        assert "widget" in text
        assert "1 hits" in text

    async def test_list_empty(self, svc, db: Database):
        result = await notification_silence_handler(_ctx(svc, db), {"op": "list"})
        assert "No active notification silences" in _text(result)

    async def test_remove_deletes_and_invalidates(
        self, svc, db: Database, patch_broadcaster,
    ):
        await db.create_silence(silence_id="sil-1", pattern="widget")
        svc.invalidate_silence_cache()
        assert await svc._match_silence("widget", "") is not None
        result = await notification_silence_handler(
            _ctx(svc, db), {"op": "remove", "silence_id": "sil-1"},
        )
        assert "Silence removed" in _text(result)
        assert await svc._match_silence("widget", "") is None

    async def test_remove_unknown_id(self, svc, db: Database):
        result = await notification_silence_handler(
            _ctx(svc, db), {"op": "remove", "silence_id": "sil-nope"},
        )
        assert "No silence found" in _text(result)

    async def test_unknown_op(self, svc, db: Database):
        result = await notification_silence_handler(_ctx(svc, db), {"op": "frob"})
        assert "must be one of" in _text(result)
