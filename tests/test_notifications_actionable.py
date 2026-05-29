"""Tests for the ``approval`` notification kind.

PR 1 of the actionable-inbox series:
- v029 migration adds ``target_kind`` and ``target_id`` columns.
- ``NotificationService.propose_action`` files a row of type=approval.
- ``handle_answer`` dispatches the user's decision through the
  ``nerve.notifications.handlers`` registry.
- The legacy ``type=question`` answer-injection path stays untouched.

These tests run against a fresh in-memory SQLite per test and stub
out the streaming broadcaster + agent engine so we can assert
behavior in isolation.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.config import NerveConfig, NotificationsConfig
from nerve.db import Database
from nerve.notifications import handlers as _handlers
from nerve.notifications.service import NotificationService


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


_MINIMAL_HELPER_SRC = textwrap.dedent(
    '''\
    """Minimal mechanical-action helper used only by the test fixture.

    Mirrors the audit + queue surface the notification service touches
    so the dispatcher can shell into a stub script and append an audit
    record without dragging in any out-of-tree files.

    Honors ``$NERVE_MECHANICAL_STATE_DIR`` so each test can point the
    helper at its own temp directory.
    """

    from __future__ import annotations

    import json
    import os
    from datetime import datetime, timezone
    from pathlib import Path

    _OVERRIDE = os.environ.get("NERVE_MECHANICAL_STATE_DIR")
    STATE_DIR = (
        Path(_OVERRIDE).expanduser() if _OVERRIDE
        else Path("~/.nerve/mechanical-actions").expanduser()
    )
    QUEUE_DIR = STATE_DIR / "queue"
    DECISIONS_DIR = STATE_DIR / "decisions"
    AUDIT_LOG = STATE_DIR / "audit.jsonl"

    VALID_EVENTS = {
        "proposed", "approved", "declined",
        "auto-execute", "executed", "failed",
        "snoozed", "approval-acted",
    }


    def utc_now_iso() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


    def ensure_dirs(state_dir: Path | None = None):
        s = Path(state_dir) if state_dir else STATE_DIR
        (s / "queue").mkdir(parents=True, exist_ok=True)
        (s / "decisions").mkdir(parents=True, exist_ok=True)
        return s / "queue", s / "decisions", s / "audit.jsonl"


    def append_audit(event, audit_log=None):
        log = Path(audit_log) if audit_log else AUDIT_LOG
        log.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(event, separators=(",", ":")) + "\\n"
        fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            os.write(fd, line.encode("utf-8"))
        finally:
            os.close(fd)


    def read_audit(audit_log=None):
        log = Path(audit_log) if audit_log else AUDIT_LOG
        if not log.is_file():
            return []
        out = []
        for line in log.read_text().splitlines():
            line = line.strip()
            if line:
                out.append(json.loads(line))
        return out
    '''
)


@pytest.fixture
def workspace_with_scripts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create a synthetic ``scripts/`` layout the dispatcher can shell into.

    Drops a stub ``mechanical-action.sh`` that records its args + exits
    with ``$MECHACTION_EXIT`` and a minimal ``_mechanical_action.py``
    audit helper. We deliberately avoid copying any out-of-tree file so
    the test stays self-contained.
    """
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()

    helper_path = scripts_dir / "_mechanical_action.py"
    helper_path.write_text(_MINIMAL_HELPER_SRC)

    # A predictable stub: writes its args to a sibling log, returns the
    # exit code embedded in $MECHACTION_EXIT (default 0).
    log_path = tmp_path / "mechanical-action.log"
    stub = textwrap.dedent(f"""\
        #!/usr/bin/env bash
        echo "$@" >> "{log_path}"
        exit ${{MECHACTION_EXIT:-0}}
    """)
    sh_path = scripts_dir / "mechanical-action.sh"
    sh_path.write_text(stub)
    sh_path.chmod(0o755)

    monkeypatch.setenv("NERVE_WORKSPACE_PATH", str(tmp_path))
    monkeypatch.setenv(
        "NERVE_MECHANICAL_STATE_DIR",
        str(tmp_path / ".nerve" / "mechanical-actions"),
    )
    return tmp_path


def read_audit_jsonl(state_dir: Path) -> list[dict[str, Any]]:
    """Read every record from the mechanical-actions audit log."""
    audit_log = state_dir / "audit.jsonl"
    if not audit_log.is_file():
        return []
    out: list[dict[str, Any]] = []
    for line in audit_log.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        out.append(json.loads(line))
    return out


# ----------------------------------------------------------------------
#  Schema / store
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestSchemaAndStore:
    async def test_v029_columns_exist(self, db: Database):
        async with db.db.execute("PRAGMA table_info(notifications)") as cur:
            cols = {row[1] async for row in cur}
        assert "target_kind" in cols
        assert "target_id" in cols

    async def test_create_notification_default_target_columns_null(self, db: Database):
        await db.create_session("s1")
        await db.create_notification(
            notification_id="n1", session_id="s1",
            type="notify", title="hello",
        )
        notif = await db.get_notification("n1")
        assert notif is not None
        assert notif["target_kind"] is None
        assert notif["target_id"] is None

    async def test_create_notification_with_target(self, db: Database):
        await db.create_session("s1")
        await db.create_notification(
            notification_id="n1", session_id="s1",
            type="approval", title="approve me",
            target_kind="mechanical-action",
            target_id="20260519T143906Z-d2e62e",
        )
        notif = await db.get_notification("n1")
        assert notif["target_kind"] == "mechanical-action"
        assert notif["target_id"] == "20260519T143906Z-d2e62e"
        assert notif["type"] == "approval"

    async def test_snooze_notification_advances_expiry(self, db: Database):
        await db.create_session("s1")
        future = (
            datetime.now(timezone.utc) + timedelta(hours=1)
        ).isoformat()
        await db.create_notification(
            notification_id="n1", session_id="s1",
            type="approval", title="t",
            expires_at=future,
        )
        new_expiry = (
            datetime.now(timezone.utc) + timedelta(hours=24)
        ).isoformat()
        ok = await db.snooze_notification("n1", new_expiry)
        assert ok is True
        notif = await db.get_notification("n1")
        assert notif["expires_at"] == new_expiry
        assert notif["status"] == "pending"

    async def test_snooze_notification_rejects_non_pending(self, db: Database):
        await db.create_session("s1")
        await db.create_notification(
            notification_id="n1", session_id="s1", type="approval", title="t",
        )
        await db.answer_notification("n1", "approve", "web")
        new_expiry = (
            datetime.now(timezone.utc) + timedelta(hours=24)
        ).isoformat()
        assert await db.snooze_notification("n1", new_expiry) is False


# ----------------------------------------------------------------------
#  propose_action
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestProposeAction:
    async def test_propose_action_creates_approval_row(
        self,
        db: Database,
        fake_config: NerveConfig,
        fake_engine: MagicMock,
        patch_broadcaster: list,
    ):
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        result = await svc.propose_action(
            session_id="s1",
            target_kind="mechanical-action",
            target_id="test-123",
            title="approve fix-pack",
        )
        notif = await db.get_notification(result["notification_id"])
        assert notif is not None
        assert notif["type"] == "approval"
        assert notif["target_kind"] == "mechanical-action"
        assert notif["target_id"] == "test-123"
        assert notif["priority"] == "high"
        # Options stored as the canonical value list.
        stored_opts = json.loads(notif["options"])
        assert stored_opts == ["approve", "decline", "snooze_24h"]
        # option_labels live in metadata so the web side can render
        # without re-parsing options.
        meta = json.loads(notif["metadata"])
        assert meta["option_labels"]["approve"] == "Approve"
        assert meta["option_labels"]["snooze_24h"] == "Snooze 24h"
        assert meta["target_kind"] == "mechanical-action"

    async def test_propose_action_rejects_empty_options(
        self,
        db: Database,
        fake_config: NerveConfig,
        fake_engine: MagicMock,
        patch_broadcaster: list,
    ):
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        with pytest.raises(ValueError):
            await svc.propose_action(
                session_id="s1",
                target_kind="mechanical-action",
                target_id="t",
                title="t",
                options=[],
            )

    async def test_propose_action_custom_options_round_trip(
        self,
        db: Database,
        fake_config: NerveConfig,
        fake_engine: MagicMock,
        patch_broadcaster: list,
    ):
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        result = await svc.propose_action(
            session_id="s1",
            target_kind="mechanical-action",
            target_id="x",
            title="custom",
            options=[
                {"label": "Yes please", "value": "yes"},
                {"label": "No thanks", "value": "no"},
            ],
        )
        notif = await db.get_notification(result["notification_id"])
        assert json.loads(notif["options"]) == ["yes", "no"]
        meta = json.loads(notif["metadata"])
        assert meta["option_labels"] == {
            "yes": "Yes please", "no": "No thanks",
        }


# ----------------------------------------------------------------------
#  handle_answer dispatch path
# ----------------------------------------------------------------------


@pytest.mark.asyncio
class TestHandleAnswerApproval:
    async def test_approve_invokes_dispatcher_and_writes_audit(
        self,
        db: Database,
        fake_config: NerveConfig,
        fake_engine: MagicMock,
        patch_broadcaster: list,
        workspace_with_scripts: Path,
    ):
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        result = await svc.propose_action(
            session_id="s1",
            target_kind="mechanical-action",
            target_id="prop-1",
            title="run lint",
        )
        nid = result["notification_id"]

        ok = await svc.handle_answer(nid, "approve", "web")
        assert ok is True

        notif = await db.get_notification(nid)
        assert notif["status"] == "answered"
        assert notif["answer"] == "approve"

        # Audit log: an ``approval-acted`` event arrived in the
        # state-dir-scoped audit log. The minimal helper honors
        # NERVE_MECHANICAL_STATE_DIR (set by the fixture) so each test
        # writes to its own isolated audit.jsonl.
        state_dir = (
            workspace_with_scripts / ".nerve" / "mechanical-actions"
        )
        events = read_audit_jsonl(state_dir)
        acted = [e for e in events if e.get("event") == "approval-acted"]
        assert any(
            e.get("notification_id") == nid
            and e.get("decision") == "approve"
            and e.get("ok") is True
            for e in acted
        )

        # Broadcast fired with approval_status="answered".
        approval_broadcasts = [
            m for _, m in patch_broadcaster
            if m.get("type") == "notification_answered"
            and m.get("notification_id") == nid
        ]
        assert approval_broadcasts
        assert approval_broadcasts[0]["approval_status"] == "answered"
        assert approval_broadcasts[0]["dispatch_ok"] is True
        # Importantly, no ``answer_injected`` should fire. The answer
        # routes through the dispatcher, not back into the session.
        injected = [
            m for _, m in patch_broadcaster
            if m.get("type") == "answer_injected"
        ]
        assert not injected

    async def test_snooze_keeps_pending_and_advances_expiry(
        self,
        db: Database,
        fake_config: NerveConfig,
        fake_engine: MagicMock,
        patch_broadcaster: list,
        workspace_with_scripts: Path,
    ):
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        result = await svc.propose_action(
            session_id="s1",
            target_kind="mechanical-action",
            target_id="prop-2",
            title="snooze me",
            expiry_hours=2,
        )
        nid = result["notification_id"]

        before = await db.get_notification(nid)
        prior_expiry = before["expires_at"]

        ok = await svc.handle_answer(nid, "snooze_24h", "web")
        assert ok is True

        after = await db.get_notification(nid)
        assert after["status"] == "pending"
        assert after["expires_at"] is not None
        # Expiry advanced forward; sanity check it is not the original.
        assert after["expires_at"] != prior_expiry
        # And no answer recorded (snooze is not a final answer).
        assert after["answer"] is None

        approval_broadcasts = [
            m for _, m in patch_broadcaster
            if m.get("type") == "notification_answered"
            and m.get("notification_id") == nid
        ]
        assert approval_broadcasts[0]["approval_status"] == "snoozed"

    async def test_decline_marks_answered_with_decline(
        self,
        db: Database,
        fake_config: NerveConfig,
        fake_engine: MagicMock,
        patch_broadcaster: list,
        workspace_with_scripts: Path,
    ):
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        result = await svc.propose_action(
            session_id="s1",
            target_kind="mechanical-action",
            target_id="prop-3",
            title="decline me",
        )
        nid = result["notification_id"]

        ok = await svc.handle_answer(nid, "decline", "web")
        assert ok is True

        notif = await db.get_notification(nid)
        assert notif["status"] == "answered"
        assert notif["answer"] == "decline"

    async def test_unknown_target_kind_marks_answered_without_dispatch(
        self,
        db: Database,
        fake_config: NerveConfig,
        fake_engine: MagicMock,
        patch_broadcaster: list,
        workspace_with_scripts: Path,
    ):
        """If a row has a target_kind no dispatcher knows about, we still
        flip the status so the row doesn't get re-delivered, and the
        audit log records the no-dispatcher state.
        """
        await db.create_session("s1")
        await db.create_notification(
            notification_id="orphan-1",
            session_id="s1",
            type="approval",
            title="orphan",
            target_kind="never-registered",
            target_id="x",
        )
        svc = NotificationService(fake_config, db, fake_engine)
        ok = await svc.handle_answer("orphan-1", "approve", "web")
        assert ok is True
        notif = await db.get_notification("orphan-1")
        assert notif["status"] == "answered"

    async def test_legacy_question_path_still_injects_answer(
        self,
        db: Database,
        fake_config: NerveConfig,
        fake_engine: MagicMock,
        patch_broadcaster: list,
    ):
        """Type=question (no target_kind) must keep flowing through the
        session-injection path, untouched by the approval dispatch.
        """
        await db.create_session("s1")
        svc = NotificationService(fake_config, db, fake_engine)
        result = await svc.ask_question(
            session_id="s1",
            title="legacy",
            body="ask me anything",
            options=["yes", "no"],
        )
        nid = result["notification_id"]

        ok = await svc.handle_answer(nid, "yes", "web")
        assert ok is True

        notif = await db.get_notification(nid)
        assert notif["status"] == "answered"
        assert notif["answer"] == "yes"

        # Confirm we broadcast the session-scoped answer_injected event,
        # AND queued a run on the engine (since the session is not
        # currently running per the fake_engine fixture).
        injected = [
            m for _, m in patch_broadcaster
            if m.get("type") == "answer_injected"
            and m.get("notification_id") == nid
        ]
        assert injected
        # Wait for any fire-and-forget answer task to settle.
        await asyncio.sleep(0)
        fake_engine.run.assert_called()


# ----------------------------------------------------------------------
#  Handler registry sanity
# ----------------------------------------------------------------------


class TestHandlerRegistry:
    def test_mechanical_action_dispatcher_registered(self):
        assert "mechanical-action" in _handlers.known_kinds()

    def test_default_approval_options(self):
        opts = _handlers.default_approval_options()
        values = {o["value"] for o in opts}
        assert values == {"approve", "decline", "snooze_24h"}

    def test_dispatcher_rejects_unsupported_decision(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        # Even with a valid workspace, an unknown decision should fail
        # cleanly with an audit_event marking the rejection.
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "mechanical-action.sh").write_text("#!/usr/bin/env bash\nexit 0\n")
        (scripts_dir / "mechanical-action.sh").chmod(0o755)
        monkeypatch.setenv("NERVE_WORKSPACE_PATH", str(tmp_path))

        result = _handlers._dispatch_mechanical_action(
            {"id": "n-1"}, "x", "rubberstamp", None,
        )
        assert result.ok is False
        assert "unsupported decision" in result.audit_event.get("error", "")
