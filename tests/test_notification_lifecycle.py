"""Tests for the notification snooze/re-delivery + expiry-reporting lifecycle.

Closes the loop that shipped half-built with the ``approval`` kind:

- v037 migration adds ``redeliver_at`` / ``redelivery_count``.
- Snoozing stamps ``redeliver_at`` (and pushes ``expires_at`` past it)
  instead of just delaying a silent expiry.
- ``NotificationService.redeliver_due`` (the periodic maintenance tick)
  fans snoozed rows back out with a fresh card, up to
  ``config.notifications.max_redeliveries`` cycles.
- ``NotificationService.expire_stale`` now *reports* every expired
  question/approval: session injection for questions, audit event for
  approvals, a ``notification_expired`` web broadcast for both.
  ``notify``-kind expiry stays silent.

Fixture style mirrors ``test_notifications_actionable``: fresh SQLite
per test, stubbed broadcaster + engine.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.config import NerveConfig, NotificationsConfig
from nerve.db import Database
from nerve.notifications import handlers as _handlers
from nerve.notifications.service import NotificationService

from tests.test_notifications_actionable import (
    _MINIMAL_HELPER_SRC,
    read_audit_jsonl,
)


# ----------------------------------------------------------------------
#  Fixtures
# ----------------------------------------------------------------------


@pytest.fixture
def fake_config(tmp_path: Path) -> NerveConfig:
    """Minimal NerveConfig with workspace + notifications config wired."""
    cfg = NerveConfig()
    cfg.workspace = tmp_path
    cfg.notifications = NotificationsConfig(
        channels=["web"],          # skip telegram in unit tests
        telegram_chat_id=None,
        default_expiry_hours=48,
        max_redeliveries=3,
        priority_prefixes={"high": "", "urgent": ""},
    )
    return cfg


@pytest.fixture
def fake_engine() -> MagicMock:
    """An engine stub with the minimum surface the service touches."""
    engine = MagicMock()
    engine.sessions = MagicMock()
    engine.sessions.is_running.return_value = False
    engine.router = MagicMock()
    engine.router.get_channel.return_value = None
    engine.run = AsyncMock()
    return engine


@pytest.fixture
def patch_broadcaster(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict]]:
    """Capture broadcaster.broadcast() calls instead of hitting any WS."""
    captured: list[tuple[str, dict]] = []

    class _FakeBroadcaster:
        async def broadcast(self, channel: str, message: dict) -> None:
            captured.append((channel, message))

    from nerve.agent import streaming
    monkeypatch.setattr(streaming, "broadcaster", _FakeBroadcaster())
    return captured


@pytest.fixture
def audit_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Workspace with just the ``_mechanical_action.py`` audit helper.

    Enough for ``_append_approval_audit`` (expiry reporting) — the
    dispatcher's ``mechanical-action.sh`` is not needed because these
    tests never route a live approve/decline through the shell script.
    """
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    (scripts_dir / "_mechanical_action.py").write_text(_MINIMAL_HELPER_SRC)
    monkeypatch.setenv("NERVE_WORKSPACE_PATH", str(tmp_path))
    monkeypatch.setenv(
        "NERVE_MECHANICAL_STATE_DIR",
        str(tmp_path / ".nerve" / "mechanical-actions"),
    )
    return tmp_path


def _iso(delta_hours: float) -> str:
    """ISO timestamp ``delta_hours`` from now (negative = past)."""
    return (
        datetime.now(timezone.utc) + timedelta(hours=delta_hours)
    ).isoformat()


def _snooze_dispatcher(snooze_until: str):
    """A dispatcher that always answers "snooze until <ts>"."""
    def dispatch(notification, target_id, decision, config):
        return _handlers.DispatchResult(
            ok=True,
            audit_event={
                "event": "approval-acted",
                "notification_id": notification.get("id", ""),
                "target_kind": "lifecycle-test",
                "target_id": target_id,
                "decision": decision,
                "ok": True,
            },
            snooze_until=snooze_until,
        )
    return dispatch


async def _make_approval(
    svc: NotificationService, db: Database, session_id: str = "s1",
    target_kind: str = "lifecycle-test", **kwargs,
) -> str:
    result = await svc.propose_action(
        session_id=session_id,
        target_kind=target_kind,
        target_id="prop-1",
        title=kwargs.pop("title", "approve the thing"),
        **kwargs,
    )
    return result["notification_id"]


# ----------------------------------------------------------------------
#  Migration / schema
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestSchema:
    async def test_v037_columns_exist_with_defaults(self, db: Database):
        async with db.db.execute("PRAGMA table_info(notifications)") as cur:
            cols = {row[1] async for row in cur}
        assert "redeliver_at" in cols
        assert "redelivery_count" in cols

        # Old-style insert (no new columns touched) → sane defaults.
        await db.create_session("s1")
        await db.create_notification(
            notification_id="n1", session_id="s1",
            type="question", title="t",
        )
        notif = await db.get_notification("n1")
        assert notif["redeliver_at"] is None
        assert notif["redelivery_count"] == 0

    async def test_v045_delivery_scope_exists(self, db: Database):
        async with db.db.execute(
            "PRAGMA table_info(notification_deliveries)",
        ) as cur:
            cols = {row[1] async for row in cur}
        assert {"notification_id", "channel", "target", "message_id"} <= cols

    async def test_slack_is_a_default_notification_channel(self):
        # Cards reach nobody unless the transport is in this list, and the
        # list replaces the default rather than extending it.
        from nerve.config import NotificationsConfig

        assert NotificationsConfig().channels == ["web", "telegram", "slack"]
        assert NotificationsConfig.from_dict({}).channels == [
            "web", "telegram", "slack",
        ]
        assert NotificationsConfig.from_dict(
            {"channels": ["web"]},
        ).channels == ["web"]


@pytest.mark.asyncio
class TestScopedAnswers:
    async def test_latest_delivery_can_move_between_targets(
        self, db: Database,
    ):
        await db.create_session("s1", source="external")
        await db.create_notification("n1", "s1", "question", "Question")
        await db.record_notification_delivery(
            "n1", "slack", target="C0123ABC", message_id="1.0",
        )
        await db.record_notification_delivery(
            "n1", "slack", target="C0456DEF", message_id="2.0",
        )

        delivery = await db.get_latest_notification_delivery("n1", "slack")

        assert delivery
        assert delivery["target"] == "C0456DEF"
        assert delivery["message_id"] == "2.0"

    async def test_latest_question_is_scoped_to_delivery_target(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        svc = NotificationService(fake_config, db, fake_engine)
        await db.create_session("wanted-session", source="external")
        await db.create_session("other-session", source="external")
        await db.create_notification(
            "wanted", "wanted-session", "question", "Wanted question",
        )
        await db.record_notification_delivery(
            "wanted", "telegram", target="100", message_id="1",
        )
        await db.create_notification(
            "newer-other", "other-session", "question", "Other question",
        )
        await db.record_notification_delivery(
            "newer-other", "telegram", target="200", message_id="2",
        )

        result = await svc.answer_latest_question(
            "yes", channel="telegram", target="100", actor="42",
        )

        assert result and result["id"] == "wanted"
        assert (await db.get_notification("newer-other"))["status"] == "pending"
        metadata = json.loads(result["metadata"])
        assert metadata["answered_by_actor"] == "42"

    async def test_explicit_answer_rejects_a_different_delivery_target(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        svc = NotificationService(fake_config, db, fake_engine)
        await db.create_session("s1", source="external")
        await db.create_notification("n1", "s1", "question", "Question")
        await db.record_notification_delivery("n1", "telegram", target="100")

        result = await svc.answer_delivered_notification(
            "n1", "yes", channel="telegram", target="200", actor="42",
        )

        assert result is None
        assert (await db.get_notification("n1"))["status"] == "pending"


@pytest.mark.asyncio
class TestSlackDeliveryBoundary:
    async def test_service_records_the_reference_returned_by_the_channel(
        self, db: Database, fake_config, fake_engine,
    ):
        await db.create_session("s1", source="external")
        await db.create_notification("n1", "s1", "question", "Question")
        channel = MagicMock(is_available=True)
        channel.post_notification = AsyncMock(
            return_value=("C0456DEF", "1.0"),
        )
        fake_engine.router.get_channel.return_value = channel
        service = NotificationService(fake_config, db, fake_engine)

        message_id = await service._deliver_slack(
            "n1", "s1", "question", "Question", "Body", "normal", ["yes"],
        )

        assert message_id == "1.0"
        delivery = await db.get_notification_delivery(
            "n1", "slack", "C0456DEF",
        )
        assert delivery and delivery["message_id"] == "1.0"
        options = channel.post_notification.await_args.args[2]
        assert options == [("yes", "yes")]

    async def test_quiescing_channel_is_not_used(
        self, db: Database, fake_config, fake_engine,
    ):
        channel = MagicMock(is_available=False)
        channel.post_notification = AsyncMock()
        fake_engine.router.get_channel.return_value = channel
        service = NotificationService(fake_config, db, fake_engine)

        message_id = await service._deliver_slack(
            "n1", "s1", "notify", "Notice", "Body", "normal", None,
        )

        assert message_id is None
        channel.post_notification.assert_not_awaited()

    async def test_expiry_uses_the_latest_recorded_target(
        self, db: Database, fake_config, fake_engine,
    ):
        await db.create_session("s1", source="external")
        await db.create_notification("n1", "s1", "question", "Question")
        await db.record_notification_delivery(
            "n1", "slack", target="C0123ABC", message_id="1.0",
        )
        await db.record_notification_delivery(
            "n1", "slack", target="C0456DEF", message_id="2.0",
        )
        channel = MagicMock(is_available=True)
        channel.expire_notification = AsyncMock()
        fake_engine.router.get_channel.return_value = channel
        service = NotificationService(fake_config, db, fake_engine)

        await service._edit_slack_expired(await db.get_notification("n1"))

        channel.expire_notification.assert_awaited_once()
        args = channel.expire_notification.await_args.args
        assert args[:2] == ("C0456DEF", "2.0")
        assert args[2].endswith("⏰ Expired unanswered")


# ----------------------------------------------------------------------
#  Snooze semantics
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestSnooze:
    async def test_snooze_sets_redeliver_at_and_pushes_expiry(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        """Snooze = redeliver_at stamped, expiry pushed PAST the
        re-delivery time, row stays pending."""
        snooze_until = _iso(24)
        _handlers.register("lifecycle-test", _snooze_dispatcher(snooze_until))
        svc = NotificationService(fake_config, db, fake_engine)
        await db.create_session("s1")
        nid = await _make_approval(svc, db, expiry_hours=2)

        ok = await svc.handle_answer(nid, "snooze_24h", "web")
        assert ok is True

        notif = await db.get_notification(nid)
        assert notif["status"] == "pending"
        assert notif["redeliver_at"] == snooze_until
        assert notif["answer"] is None
        # expires_at = snooze_until + default_expiry_hours: cannot die
        # before it resurfaces.
        assert notif["expires_at"] > notif["redeliver_at"]

        # Broadcast carries the snoozed state + timestamp for the card.
        answered = [
            m for _, m in patch_broadcaster
            if m.get("type") == "notification_answered"
            and m.get("notification_id") == nid
        ]
        assert answered and answered[0]["approval_status"] == "snoozed"
        assert answered[0]["snooze_until"] == snooze_until

    async def test_db_snooze_rejects_non_pending(self, db: Database):
        await db.create_session("s1")
        await db.create_notification(
            notification_id="n1", session_id="s1", type="approval", title="t",
        )
        await db.answer_notification("n1", "approve", "web")
        assert await db.snooze_notification("n1", _iso(24), _iso(72)) is False


# ----------------------------------------------------------------------
#  Re-delivery tick
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestRedelivery:
    async def test_tick_redelivers_due_snoozed_row(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        _handlers.register("lifecycle-test", _snooze_dispatcher(_iso(-0.5)))
        svc = NotificationService(fake_config, db, fake_engine)
        await db.create_session("s1")
        nid = await _make_approval(svc, db)
        await svc.handle_answer(nid, "snooze_24h", "web")
        patch_broadcaster.clear()

        redelivered = await svc.redeliver_due()
        assert redelivered == 1

        notif = await db.get_notification(nid)
        assert notif["status"] == "pending"
        assert notif["redeliver_at"] is None          # consumed
        assert notif["redelivery_count"] == 1
        # Fresh expiry window for the fresh card.
        assert notif["expires_at"] > _iso(47)

        # Web fanout fired again, flagged as a re-delivery, with the
        # original options + labels intact.
        fanouts = [
            m for _, m in patch_broadcaster
            if m.get("type") == "notification"
            and m.get("notification_id") == nid
        ]
        assert len(fanouts) == 1
        assert fanouts[0]["redelivered"] is True
        assert fanouts[0]["redelivery_count"] == 1
        assert fanouts[0]["options"] == ["approve", "decline", "snooze_24h"]
        assert fanouts[0]["option_labels"]["snooze_24h"] == "Snooze 24h"

    async def test_not_due_rows_are_left_alone(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        _handlers.register("lifecycle-test", _snooze_dispatcher(_iso(24)))
        svc = NotificationService(fake_config, db, fake_engine)
        await db.create_session("s1")
        nid = await _make_approval(svc, db)
        await svc.handle_answer(nid, "snooze_24h", "web")

        assert await svc.redeliver_due() == 0
        notif = await db.get_notification(nid)
        assert notif["redeliver_at"] is not None
        assert notif["redelivery_count"] == 0

    async def test_resnooze_after_redelivery_second_cycle(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        """Snooze is repeatable: each click buys another cycle."""
        _handlers.register("lifecycle-test", _snooze_dispatcher(_iso(-1)))
        svc = NotificationService(fake_config, db, fake_engine)
        await db.create_session("s1")
        nid = await _make_approval(svc, db)

        await svc.handle_answer(nid, "snooze_24h", "web")
        assert await svc.redeliver_due() == 1
        # User snoozes the re-delivered card again.
        ok = await svc.handle_answer(nid, "snooze_24h", "web")
        assert ok is True
        notif = await db.get_notification(nid)
        assert notif["status"] == "pending"
        assert notif["redeliver_at"] is not None

        assert await svc.redeliver_due() == 1
        notif = await db.get_notification(nid)
        assert notif["redelivery_count"] == 2
        assert notif["redeliver_at"] is None

    async def test_cap_reached_expires_with_report_no_resend(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
        audit_workspace: Path,
    ):
        _handlers.register("lifecycle-test", _snooze_dispatcher(_iso(-1)))
        fake_config.notifications.max_redeliveries = 2
        svc = NotificationService(fake_config, db, fake_engine)
        await db.create_session("s1")
        nid = await _make_approval(svc, db)

        for cycle in range(2):
            await svc.handle_answer(nid, "snooze_24h", "web")
            assert await svc.redeliver_due() == 1, f"cycle {cycle}"

        # Third snooze hits the cap: tick expires instead of re-sending.
        await svc.handle_answer(nid, "snooze_24h", "web")
        patch_broadcaster.clear()
        assert await svc.redeliver_due() == 0

        notif = await db.get_notification(nid)
        assert notif["status"] == "expired"
        assert notif["redelivery_count"] == 2

        # No new fanout, but the expiry was reported to the web...
        fanouts = [
            m for _, m in patch_broadcaster if m.get("type") == "notification"
        ]
        assert not fanouts
        expired_events = [
            m for _, m in patch_broadcaster
            if m.get("type") == "notification_expired"
            and m.get("notification_id") == nid
        ]
        assert expired_events

        # ...and to the mechanical-actions audit log.
        events = read_audit_jsonl(
            audit_workspace / ".nerve" / "mechanical-actions",
        )
        expired = [e for e in events if e.get("event") == "approval-expired"]
        assert any(
            e.get("notification_id") == nid
            and e.get("redelivery_count") == 2
            for e in expired
        )

    async def test_row_due_for_both_redelivery_and_expiry_survives(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        """Ordering guarantee: redeliver-before-expire means a row whose
        redeliver_at AND expires_at both passed gets its last chance."""
        svc = NotificationService(fake_config, db, fake_engine)
        await db.create_session("s1")
        await db.create_notification(
            notification_id="n1", session_id="s1", type="approval",
            title="both due", options=["approve", "decline", "snooze_24h"],
            expires_at=_iso(-1),
            target_kind="lifecycle-test", target_id="x",
        )
        await db.update_notification("n1", redeliver_at=_iso(-2))

        # Maintenance-tick order: redeliver first, then expire.
        assert await svc.redeliver_due() == 1
        assert await svc.expire_stale() == 0

        notif = await db.get_notification("n1")
        assert notif["status"] == "pending"      # survived, re-delivered
        assert notif["redelivery_count"] == 1
        assert notif["expires_at"] > _iso(0)     # expiry window restarted


# ----------------------------------------------------------------------
#  Expiry reporting
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestExpiryReporting:
    async def _expired_question(
        self, db: Database, svc: NotificationService,
        session_id: str = "s1", title: str = "pick one",
    ) -> str:
        result = await svc.ask_question(
            session_id=session_id, title=title, options=["yes", "no"],
        )
        nid = result["notification_id"]
        await db.update_notification(nid, expires_at=_iso(-1))
        return nid

    async def test_expired_question_injected_into_origin_session(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        nid = await self._expired_question(db, svc)
        # The origin session is mid-turn: injection must STILL be
        # dispatched (the per-session lock serializes it) — this is
        # deliberately not handle_answer's is_running skip.
        fake_engine.sessions.is_running.return_value = True

        assert await svc.expire_stale() == 1
        await asyncio.sleep(0)  # let the fire-and-forget task start

        notif = await db.get_notification(nid)
        assert notif["status"] == "expired"

        fake_engine.run.assert_called_once()
        kwargs = fake_engine.run.call_args.kwargs
        assert kwargs["session_id"] == "s1"
        assert kwargs["source"] == "notification:expiry"
        assert kwargs["internal"] is True
        assert "pick one" in kwargs["user_message"]
        assert "expired unanswered" in kwargs["user_message"].lower()

        expired_events = [
            m for ch, m in patch_broadcaster
            if m.get("type") == "notification_expired" and ch == "__global__"
        ]
        assert [e["notification_id"] for e in expired_events] == [nid]

    async def test_multiple_questions_same_session_single_injection(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        await self._expired_question(db, svc, title="first question")
        await self._expired_question(db, svc, title="second question")

        assert await svc.expire_stale() == 2
        await asyncio.sleep(0)

        fake_engine.run.assert_called_once()
        msg = fake_engine.run.call_args.kwargs["user_message"]
        assert "first question" in msg
        assert "second question" in msg

    async def test_external_session_broadcast_only(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        await db.create_session("sat-1", source="external")
        svc = NotificationService(fake_config, db, fake_engine)
        nid = await self._expired_question(db, svc, session_id="sat-1")

        assert await svc.expire_stale() == 1
        await asyncio.sleep(0)

        fake_engine.run.assert_not_called()
        assert any(
            m.get("type") == "notification_expired"
            and m.get("notification_id") == nid
            for _, m in patch_broadcaster
        )

    async def test_archived_and_missing_sessions_broadcast_only(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        await db.create_session("s1", status="archived")
        svc = NotificationService(fake_config, db, fake_engine)
        await self._expired_question(db, svc, session_id="s1")
        # A question whose session row vanished entirely.
        await db.create_notification(
            notification_id="ghost-q", session_id="no-such-session",
            type="question", title="orphan", expires_at=_iso(-1),
        )

        assert await svc.expire_stale() == 2
        await asyncio.sleep(0)

        fake_engine.run.assert_not_called()
        expired_ids = {
            m.get("notification_id") for _, m in patch_broadcaster
            if m.get("type") == "notification_expired"
        }
        assert "ghost-q" in expired_ids

    async def test_expired_approval_audits_no_injection(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
        audit_workspace: Path,
    ):
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        nid = await _make_approval(svc, db)
        await db.update_notification(nid, expires_at=_iso(-1))

        assert await svc.expire_stale() == 1
        await asyncio.sleep(0)

        fake_engine.run.assert_not_called()      # approvals never inject
        events = read_audit_jsonl(
            audit_workspace / ".nerve" / "mechanical-actions",
        )
        expired = [e for e in events if e.get("event") == "approval-expired"]
        assert any(
            e.get("notification_id") == nid
            and e.get("target_kind") == "lifecycle-test"
            and e.get("target_id") == "prop-1"
            for e in expired
        )

    async def test_notify_kind_expiry_stays_silent(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        await db.create_notification(
            notification_id="fyi-1", session_id="s1",
            type="notify", title="fyi", expires_at=_iso(-1),
        )

        assert await svc.expire_stale() == 1
        await asyncio.sleep(0)

        notif = await db.get_notification("fyi-1")
        assert notif["status"] == "expired"
        fake_engine.run.assert_not_called()
        assert not [
            m for _, m in patch_broadcaster
            if m.get("type") == "notification_expired"
        ]

    async def test_expiry_edits_telegram_card(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        """Best-effort Telegram edit: expired card gets the status line
        and loses its (dead) inline keyboard."""
        bot = MagicMock()
        bot.edit_message_text = AsyncMock()
        channel = MagicMock()
        channel._app.bot = bot
        fake_engine.router.get_channel.return_value = channel

        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        nid = await self._expired_question(db, svc, title="tg question")
        await db.update_notification(
            nid, telegram_message_id="4242", telegram_chat_id="1001",
        )

        assert await svc.expire_stale() == 1

        bot.edit_message_text.assert_awaited_once()
        kwargs = bot.edit_message_text.await_args.kwargs
        assert kwargs["chat_id"] == 1001
        assert kwargs["message_id"] == 4242
        assert "Expired unanswered" in kwargs["text"]

    async def test_telegram_edit_failure_is_swallowed(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        """Telegram refuses edits on >48h-old messages — expiry must
        still complete."""
        bot = MagicMock()
        bot.edit_message_text = AsyncMock(side_effect=RuntimeError("too old"))
        channel = MagicMock()
        channel._app.bot = bot
        fake_engine.router.get_channel.return_value = channel

        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        nid = await self._expired_question(db, svc)
        await db.update_notification(
            nid, telegram_message_id="4242", telegram_chat_id="1001",
        )

        assert await svc.expire_stale() == 1
        notif = await db.get_notification(nid)
        assert notif["status"] == "expired"
        # HTML attempt + plain-text fallback, both swallowed.
        assert bot.edit_message_text.await_count == 2


# ----------------------------------------------------------------------
#  Answer attribution
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestAnswerAttribution:
    """A shared workspace needs to know which member approved an action.

    ``answered_by`` names the transport and the injection path routes on
    it, so the person travels beside it in the row's metadata rather than
    inside the same string.
    """

    async def test_a_question_answer_records_the_actor(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        result = await svc.ask_question(session_id="s1", title="pick one")

        assert await svc.handle_answer(
            result["notification_id"], "yes", "slack", actor="U0123ABC",
        )
        notif = await db.get_notification(result["notification_id"])
        assert notif["answered_by"] == "slack"
        assert json.loads(notif["metadata"])["answered_by_actor"] == "U0123ABC"

    async def test_the_transport_still_names_the_channel_on_its_own(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        # engine.run is handed answered_by as the channel name, so folding
        # the member id into that string would route the reply nowhere.
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        result = await svc.ask_question(session_id="s1", title="pick one")

        await svc.handle_answer(
            result["notification_id"], "yes", "slack", actor="U0123ABC",
        )
        await asyncio.sleep(0)
        kwargs = fake_engine.run.call_args.kwargs
        assert kwargs["channel"] == "slack"
        assert kwargs["source"] == "notification:slack"

    async def test_an_answer_with_no_actor_leaves_the_metadata_alone(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        result = await svc.ask_question(session_id="s1", title="pick one")

        await svc.handle_answer(result["notification_id"], "yes", "web")
        notif = await db.get_notification(result["notification_id"])
        assert "answered_by_actor" not in json.loads(notif["metadata"])

    async def test_existing_metadata_survives_the_answer(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        # The column also carries option_labels, which the Telegram and web
        # renderers read after the row is answered.
        await db.create_session("s1")
        await db.create_notification(
            notification_id="n1", session_id="s1", type="question",
            title="t", metadata={"option_labels": {"yes": "Ship it"}},
        )
        svc = NotificationService(fake_config, db, fake_engine)

        await svc.handle_answer("n1", "yes", "slack", actor="U0123ABC")
        metadata = json.loads((await db.get_notification("n1"))["metadata"])
        assert metadata["option_labels"] == {"yes": "Ship it"}
        assert metadata["answered_by_actor"] == "U0123ABC"

    async def test_the_actor_reaches_the_web_broadcast(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
    ):
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        result = await svc.ask_question(session_id="s1", title="pick one")

        await svc.handle_answer(
            result["notification_id"], "yes", "slack", actor="U0123ABC",
        )
        answered = [
            m for _, m in patch_broadcaster
            if m.get("type") == "notification_answered"
        ]
        assert answered
        assert answered[-1]["answered_by"] == "slack"
        assert answered[-1]["answered_by_actor"] == "U0123ABC"

    async def test_an_approval_audit_record_names_the_actor(
        self, db: Database, fake_config, fake_engine, patch_broadcaster,
        audit_workspace,
    ):
        await db.create_session("s1")
        _handlers.register("attribution-test", lambda *a: _handlers.DispatchResult(
            ok=True,
            audit_event={
                "event": "approval-acted",
                "target_kind": "attribution-test",
                "decision": "approve",
                "ok": True,
            },
        ))
        svc = NotificationService(fake_config, db, fake_engine)
        nid = await _make_approval(
            svc, db, target_kind="attribution-test",
        )

        await svc.handle_answer(nid, "approve", "slack", actor="U0123ABC")
        records = read_audit_jsonl(
            audit_workspace / ".nerve" / "mechanical-actions",
        )
        acted = [r for r in records if r.get("event") == "approval-acted"]
        assert acted
        assert acted[-1]["answered_by"] == "slack"
        assert acted[-1]["answered_by_actor"] == "U0123ABC"
