"""Archived and deleted sessions stay closed (ClickHouse/nerve#671).

A session in ``archived`` leaves that state only through unarchive. A run
never creates a row for a deleted session id. Work that targets an archived or
deleted session is refused or cancelled. The tests drive the real
``AgentEngine`` (turn loop, run lock, client build, SQLite store) with a
scripted backend in place of the CLI.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from fastapi import HTTPException

from nerve.agent.backends import events as ev
from nerve.agent.backends.base import BackendCapabilities
from nerve.agent.engine import AgentEngine, SessionClosedError
from nerve.agent.sessions import BUSY_RUNNING_REASON, SessionBusyError, SessionManager
from nerve.config import NerveConfig, NotificationsConfig
from nerve.cron.service import CronService
from nerve.identity import attribution_for
from nerve.notifications.service import NotificationService

from tests.actor_rows import ensure_system_principal


@pytest_asyncio.fixture
async def db(db):  # noqa: F811 - the conftest database, with the system actor
    await ensure_system_principal(db)
    return db


@pytest.fixture(autouse=True)
def broadcasts(monkeypatch):
    """Every broadcast of the test, as ``(channel, message)``."""
    captured: list[tuple[str, dict]] = []

    async def _broadcast(channel, message, *args, **kwargs):
        captured.append((channel, message))

    from nerve.agent.streaming import broadcaster
    monkeypatch.setattr(broadcaster, "broadcast", _broadcast)
    return captured


class _Client:
    """A runtime client: each turn streams one text delta, then ends, or
    waits until ``release`` when ``hold`` is set."""

    def __init__(self, native_id: str, spec) -> None:
        self.spec = spec
        self._native_id = native_id
        self.model = "claude-test"
        self.turns: list[str] = []
        self.hold = False
        self.release = asyncio.Event()
        self.closed = asyncio.Event()
        self.got_content = asyncio.Event()
        self.disconnect_gate: asyncio.Event | None = None

    @property
    def native_session_id(self) -> str:
        return self._native_id

    async def connect(self) -> None:
        pass

    async def start_turn(self, turn) -> None:
        self.turns.append(turn.text)

    async def receive_turn(self):
        yield ev.TextDelta(text=f"reply-from-{self._native_id}")
        self.got_content.set()
        if self.hold:
            await self.release.wait()
        yield ev.TurnCompleted(
            native_session_id=self._native_id,
            usage=ev.NormalizedUsage(input_tokens=1, output_tokens=1),
        )

    async def interrupt(self, **_kw) -> None:
        self.release.set()

    def is_alive(self) -> bool:
        return not self.closed.is_set()

    async def disconnect(self, timeout: float = 5.0) -> None:
        if self.disconnect_gate is not None:
            await self.disconnect_gate.wait()
        self.closed.set()
        self.release.set()

    def try_receive_idle_events(self):
        return None

    def buffer_used(self) -> int:
        return 0


class _Backend:
    name = "claude"

    def __init__(self) -> None:
        self.capabilities = BackendCapabilities(
            supports_idle_stream=False, supports_cache_ttl=False,
        )
        self.clients: list[_Client] = []
        self.hold_new_clients = False

    def default_model(self, source):
        return "claude-test"

    def excluded_tools(self):
        return set()

    def validate_resume_target(self, native_id, cwd):
        return True

    async def create_client(self, spec):
        client = _Client(f"native-{len(self.clients)}", spec)
        client.hold = self.hold_new_clients
        self.clients.append(client)
        return client


def _engine(tmp_path, db, *, max_concurrent: int = 3) -> tuple[AgentEngine, _Backend]:
    ws = tmp_path / "ws"
    ws.mkdir(exist_ok=True)
    cfg = NerveConfig.from_dict({
        "workspace": str(ws),
        "agent": {
            "backend": "claude", "effort": "high", "cron_effort": "low",
            "max_concurrent": max_concurrent,
        },
    })
    engine = AgentEngine(cfg, db)
    backend = _Backend()
    engine._backends["claude"] = backend
    return engine, backend


def _cron(engine: AgentEngine, db) -> CronService:
    config = MagicMock()
    config.timezone = "UTC"
    return CronService(config, engine, db)


def _notifications(engine: AgentEngine, db, tmp_path: Path) -> NotificationService:
    cfg = NerveConfig()
    cfg.workspace = tmp_path
    cfg.notifications = NotificationsConfig(
        channels=["web"], telegram_chat_id=None, default_expiry_hours=48,
        priority_prefixes={"high": "", "urgent": ""},
    )
    service = NotificationService(cfg, db, engine)
    engine.set_notification_service(service)
    return service


async def _until(pred, timeout: float = 5.0) -> None:
    async def _loop():
        while not pred():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_loop(), timeout=timeout)


def _past() -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()


async def _events(db, sid: str) -> list[str]:
    return [e["event_type"] for e in await db.get_session_events(sid, limit=100)]


async def _new_session(engine: AgentEngine, db, sid: str) -> None:
    """A web session with one completed turn (it has a native conversation)."""
    await engine.sessions.get_or_create(sid, source="web", backend="claude", actor=None)
    await engine.run(sid, "first message", source="web", channel="web", actor=None)


def _queue_removals(broadcasts) -> list[dict]:
    return [
        removed
        for _channel, msg in broadcasts if msg.get("type") == "queue_update"
        for removed in msg.get("removed") or ()
    ]


# --------------------------------------------------------------------------- #
# Archive: wakeups, status writes, the run lock
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_archive_deletes_pending_wakeups_and_the_sweep_fires_nothing(tmp_path, db):
    engine, backend = _engine(tmp_path, db)
    sid = "s-loop"
    await _new_session(engine, db, sid)
    await db.add_wakeup(sid, prompt="check the deploy", fire_at=_past())

    result = await engine.sessions.archive_session_cascade(sid)

    assert result["archived"] == [sid]
    assert await db.list_pending_wakeups(sid) == []
    await _cron(engine, db)._sweep_wakeups()
    await asyncio.sleep(0.05)
    assert len(backend.clients) == 1
    row = await db.get_session(sid)
    assert row["status"] == "archived"
    assert "unarchived" not in await _events(db, sid)


@pytest.mark.asyncio
async def test_a_wakeup_of_an_archived_session_is_refused(tmp_path, db, broadcasts):
    """A wakeup that is pending on an archived session runs nothing. (The
    archive deletes pending wakeups, so this one is added after it.)"""
    engine, backend = _engine(tmp_path, db)
    sid = "s-old-archive"
    await _new_session(engine, db, sid)
    await engine.sessions.archive_session(sid)
    await db.add_wakeup(sid, prompt="check the deploy", fire_at=_past())
    messages_before = len(await db.get_messages(sid))

    with pytest.raises(SessionClosedError) as info:
        await engine.run(sid, "check the deploy", source="wakeup", internal=True, actor=None)

    assert info.value.reason == "archived"
    assert len(backend.clients) == 1
    assert len(await db.get_messages(sid)) == messages_before
    assert (await db.get_session(sid))["status"] == "archived"
    assert any(
        msg.get("type") == "error" and "archived" in msg.get("error", "")
        for _c, msg in broadcasts
    )


@pytest.mark.asyncio
async def test_runtime_status_writes_keep_an_archived_session_archived(tmp_path, db):
    engine, _backend = _engine(tmp_path, db)
    sid = "s-marks"
    await engine.sessions.get_or_create(sid, source="web", actor=None)
    await engine.sessions.archive_session(sid)
    before = await _events(db, sid)

    assert await engine.sessions.mark_active(sid, sdk_session_id="native-x") is False
    assert await engine.sessions.mark_idle(sid) is False
    assert await engine.sessions.mark_stopped(sid) is False
    assert await engine.sessions.mark_error(sid, "boom") is False

    row = await db.get_session(sid)
    assert row["status"] == "archived"
    assert row["sdk_session_id"] is None
    assert await _events(db, sid) == before


@pytest.mark.asyncio
async def test_runtime_status_writes_still_work_for_an_open_session(tmp_path, db):
    engine, _backend = _engine(tmp_path, db)
    sid = "s-open"
    await engine.sessions.get_or_create(sid, source="web", actor=None)

    assert await engine.sessions.mark_active(sid, sdk_session_id="native-x") is True
    assert (await db.get_session(sid))["status"] == "active"
    assert await engine.sessions.mark_idle(sid) is True
    assert (await db.get_session(sid))["status"] == "idle"
    assert "started" in await _events(db, sid)


@pytest.mark.asyncio
async def test_archive_is_refused_while_a_turn_waits_for_a_concurrency_slot(tmp_path, db):
    engine, backend = _engine(tmp_path, db, max_concurrent=1)
    sid = "s-slot"
    await engine.sessions.get_or_create(sid, source="web", backend="claude", actor=None)
    # Another session holds the only slot.
    await engine._semaphore.acquire()
    turn = asyncio.create_task(
        engine.run(sid, "waits for a slot", source="web", channel="web", actor=None),
    )
    await _until(lambda: engine.sessions.turn_task(sid) is not None)
    assert not engine.sessions.is_running(sid)

    with pytest.raises(SessionBusyError) as info:
        await engine.sessions.archive_session(sid)
    assert str(info.value) == BUSY_RUNNING_REASON
    result = await engine.sessions.archive_session_cascade(sid)
    assert result["archived"] == []
    assert result["skipped"] == [{"id": sid, "reason": BUSY_RUNNING_REASON}]

    engine._semaphore.release()
    await asyncio.wait_for(turn, timeout=5)
    assert (await db.get_session(sid))["status"] == "active"
    assert backend.clients[0].turns


@pytest.mark.asyncio
async def test_archive_is_refused_while_a_registered_task_runs(tmp_path, db):
    """A workflow leg between two of its turns: its task is registered for
    the session, and nothing holds the run lock."""
    engine, _backend = _engine(tmp_path, db)
    sid = "s-leg"
    await _new_session(engine, db, sid)
    leg = asyncio.create_task(asyncio.Event().wait())
    engine.register_task(sid, leg)
    try:
        assert not engine._session_locks[sid].locked()
        with pytest.raises(SessionBusyError) as info:
            await engine.sessions.archive_session(sid)
        assert str(info.value) == BUSY_RUNNING_REASON
    finally:
        leg.cancel()
    await asyncio.sleep(0)
    assert await engine.sessions.archive_session(sid) is True


@pytest.mark.asyncio
async def test_archive_is_refused_while_anything_holds_the_run_lock(tmp_path, db):
    """An autonomous turn, an idle drain or a queued message holds the run
    lock with no turn task: the archive is refused all the same."""
    engine, _backend = _engine(tmp_path, db)
    sid = "s-run-lock"
    await _new_session(engine, db, sid)

    async with engine._holding_run_lock(sid, engine._session_locks[sid]):
        assert engine.sessions.turn_task(sid) is None
        with pytest.raises(SessionBusyError) as info:
            await engine.sessions.archive_session(sid)
        assert str(info.value) == BUSY_RUNNING_REASON
        result = await engine.sessions.archive_session_cascade(sid)
        assert result["skipped"] == [{"id": sid, "reason": BUSY_RUNNING_REASON}]

    assert await engine.sessions.archive_session(sid) is True


def test_closing_marks_nest_and_clear():
    manager = SessionManager(MagicMock())
    with manager.closing("s", "deleted"):
        with manager.closing("s", "archived"):
            assert manager.closing_reason("s") == "deleted"
        assert manager.closing_reason("s") == "deleted"
    assert manager.closing_reason("s") is None
    assert manager._closing == {}


@pytest.mark.asyncio
async def test_a_queued_turn_is_refused_when_the_session_was_archived_meanwhile(
    tmp_path, db, broadcasts,
):
    """The check after the run lock: a session that another path archives
    (as the Codex sync does for its mirror sessions) while a run waits."""
    engine, backend = _engine(tmp_path, db)
    sid = "s-queued"
    await _new_session(engine, db, sid)
    lock = engine._session_locks[sid]
    await lock.acquire()
    turn = asyncio.create_task(
        engine.run(sid, "queued message", source="web", channel="web",
                   actor=None, client_msg_id="m-1"),
    )
    await _until(lambda: len(engine.queue_view(sid)) == 1)

    await db.change_session_status(sid, "archived", event_type="archived")
    engine._pass_run_lock(sid, lock)

    with pytest.raises(SessionClosedError):
        await asyncio.wait_for(turn, timeout=5)
    assert len(backend.clients) == 1
    assert (await db.get_session(sid))["status"] == "archived"
    users = [m["content"] for m in await db.get_messages(sid) if m["role"] == "user"]
    assert "queued message" not in users
    removal = _queue_removals(broadcasts)[-1]
    assert (removal["client_msg_id"], removal["reason"]) == ("m-1", "refused")
    assert any(
        msg.get("type") == "error" and msg.get("error") == str(SessionClosedError("archived"))
        for _c, msg in broadcasts
    )
    assert not lock.locked()


@pytest.mark.asyncio
async def test_a_turn_that_starts_during_an_archive_is_refused_before_it_stores_anything(
    tmp_path, db,
):
    """The archive mark: a turn that takes the run lock while the archive
    disconnects the old client is refused before its message is stored."""
    engine, backend = _engine(tmp_path, db)
    sid = "s-mid-archive"
    await _new_session(engine, db, sid)
    gate = asyncio.Event()
    backend.clients[0].disconnect_gate = gate
    archive = asyncio.create_task(engine.sessions.archive_session(sid))
    await _until(lambda: engine.sessions.get_client(sid) is None)
    assert engine.sessions.closing_reason(sid) == "archived"

    with pytest.raises(SessionClosedError):
        await asyncio.wait_for(
            engine.run(sid, "sent during the archive", source="web",
                       channel="web", actor=None),
            timeout=5,
        )
    gate.set()

    assert await asyncio.wait_for(archive, timeout=5) is True
    assert engine.sessions.closing_reason(sid) is None
    assert len(backend.clients) == 1
    users = [m["content"] for m in await db.get_messages(sid) if m["role"] == "user"]
    assert "sent during the archive" not in users
    assert (await db.get_session(sid))["status"] == "archived"


# --------------------------------------------------------------------------- #
# Channels and direct messages
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_a_channel_message_after_an_archive_gets_a_new_session(tmp_path, db):
    engine, _backend = _engine(tmp_path, db)
    key = "telegram:42"
    first = await engine.sessions.get_active_session(key, source="telegram", actor=None)
    await engine.sessions.mark_active(first)  # last activity: now
    assert await engine.sessions.get_active_session(key, source="telegram", actor=None) == first

    await engine.sessions.archive_session(first)
    second = await engine.sessions.get_active_session(key, source="telegram", actor=None)

    assert second != first
    assert (await db.get_channel_session(key))["session_id"] == second
    assert (await db.get_session(first))["status"] == "archived"


@pytest.mark.asyncio
async def test_the_router_sends_a_refused_message_again_to_a_new_session(tmp_path, db):
    from nerve.channels.base import InboundMessage
    from nerve.channels.router import ChannelRouter

    router = ChannelRouter(MagicMock())
    calls: list[str] = []

    async def _handle(msg):
        calls.append(msg.text)
        if len(calls) == 1:
            raise SessionClosedError("archived")
        return "answer"

    router._handle_message = _handle
    msg = InboundMessage(
        channel_name="telegram", channel_key="telegram:42", sender_id="42",
        text="hello",
    )
    assert await router.handle_message(msg) == "answer"
    assert calls == ["hello", "hello"]

    # An explicit target is not sent anywhere else.
    calls.clear()
    explicit = InboundMessage(
        channel_name="telegram", channel_key="telegram:42", sender_id="42",
        text="hello", session_id="s-explicit",
    )
    with pytest.raises(SessionClosedError):
        await router.handle_message(explicit)
    assert calls == ["hello"]


@pytest.mark.asyncio
async def test_a_persons_message_unarchives_the_session_in_their_name(tmp_path, db):
    engine, backend = _engine(tmp_path, db)
    sid = "s-reopen"
    await _new_session(engine, db, sid)
    await engine.sessions.archive_session(sid)
    person = db.system_actor

    assert await engine.reopen_archived(sid, attribution=attribution_for(person)) is True
    assert await engine.reopen_archived(sid, attribution=attribution_for(person)) is False
    await engine.run(sid, "continue please", source="web", channel="web", actor=None)

    row = await db.get_session(sid)
    assert row["status"] == "active"
    assert row["archived_at"] is None
    events = await db.get_session_events(sid, limit=100)
    unarchived = [e for e in events if e["event_type"] == "unarchived"]
    assert len(unarchived) == 1
    assert unarchived[0]["actor_id"] == person.actor_id
    assert "continue please" in backend.clients[-1].turns[-1]


@pytest.mark.asyncio
async def test_the_chat_api_unarchives_an_archived_session_it_names(tmp_path, db):
    from nerve.gateway.routes import _deps as deps_mod
    from nerve.gateway.routes import sessions as routes

    engine, _backend = _engine(tmp_path, db)
    sid = "s-api"
    await _new_session(engine, db, sid)
    await engine.sessions.archive_session(sid)

    saved = deps_mod._deps
    deps_mod.init_deps(engine, db)
    try:
        req = routes.MessageRequest(session_id=sid, message="from the API")
        response = await routes.chat(req, actor=db.system_actor)
    finally:
        deps_mod._deps = saved

    assert response.session_id == sid
    assert (await db.get_session(sid))["status"] == "active"
    assert "unarchived" in await _events(db, sid)


@pytest.mark.asyncio
async def test_run_later_into_an_archived_session_unarchives_it(tmp_path, db):
    from nerve.gateway.routes import _deps as deps_mod
    from nerve.gateway.routes import sessions as routes

    engine, _backend = _engine(tmp_path, db)
    sid = "s-later"
    await _new_session(engine, db, sid)
    await engine.sessions.archive_session(sid)

    saved = deps_mod._deps
    deps_mod.init_deps(engine, db)
    try:
        req = routes.RunLaterRequest(session_id=sid, message="do it later", delay="30m")
        result = await routes.run_later(req, actor=db.system_actor)
    finally:
        deps_mod._deps = saved

    assert result["scheduled"] is True
    assert (await db.get_session(sid))["status"] == "idle"
    assert len(await db.list_pending_wakeups(sid)) == 1
    assert "unarchived" in await _events(db, sid)


@pytest.mark.asyncio
async def test_unstarring_an_archived_session_runs_no_hook(tmp_path, db, monkeypatch):
    from nerve.gateway.routes import _deps as deps_mod
    from nerve.gateway.routes import sessions as routes

    engine, _backend = _engine(tmp_path, db)
    engine.config.sessions.star_project_hook = True
    monkeypatch.setattr(routes, "get_config", lambda: engine.config)
    sid = "s-starred"
    await engine.sessions.get_or_create(sid, source="web", actor=None)
    await db.change_session_fields(sid, {"starred": 1})
    await engine.sessions.archive_session(sid)
    runs = AsyncMock()
    engine.run = runs

    saved = deps_mod._deps
    deps_mod.init_deps(engine, db)
    try:
        await routes.update_session(sid, {"starred": False}, actor=db.system_actor)
    finally:
        deps_mod._deps = saved
    await asyncio.sleep(0)

    runs.assert_not_called()
    row = await db.get_session(sid)
    assert row["status"] == "archived" and not row["starred"]


# --------------------------------------------------------------------------- #
# Delete
# --------------------------------------------------------------------------- #

async def _delete(engine, db, sid):
    from nerve.gateway.routes import _deps as deps_mod
    from nerve.gateway.routes import sessions as routes

    saved = deps_mod._deps
    deps_mod.init_deps(engine, db)
    try:
        return await routes.delete_session(sid, actor=db.system_actor)
    finally:
        deps_mod._deps = saved


@pytest.mark.asyncio
async def test_delete_is_refused_while_a_turn_runs_and_one_waits(tmp_path, db):
    engine, backend = _engine(tmp_path, db)
    sid = "s-busy"
    await engine.sessions.get_or_create(sid, source="web", backend="claude", actor=None)
    backend.hold_new_clients = True
    turn1 = asyncio.create_task(
        engine.run(sid, "long task", source="web", channel="web", actor=None),
    )
    await _until(lambda: bool(backend.clients) and backend.clients[0].got_content.is_set())
    turn2 = asyncio.create_task(
        engine.run(sid, "queued follow-up", source="web", channel="web", actor=None),
    )
    await _until(lambda: len(engine.queue_view(sid)) == 1)

    with pytest.raises(HTTPException) as info:
        await _delete(engine, db, sid)

    assert info.value.status_code == 409
    assert await db.get_session(sid) is not None
    assert await db.get_session_deletion(sid) is None
    backend.clients[0].release.set()
    await asyncio.wait_for(turn1, timeout=5)
    await asyncio.wait_for(turn2, timeout=5)
    users = [m["content"] for m in await db.get_messages(sid) if m["role"] == "user"]
    assert users == ["long task", "queued follow-up"]


@pytest.mark.asyncio
async def test_delete_is_refused_while_the_run_lock_is_held_without_a_turn(tmp_path, db):
    """The queue handoff, an autonomous turn or an idle drain: the run lock
    is held, and no turn task is set."""
    engine, _backend = _engine(tmp_path, db)
    sid = "s-handoff"
    await _new_session(engine, db, sid)

    async with engine._holding_run_lock(sid, engine._session_locks[sid]):
        with pytest.raises(HTTPException) as info:
            await _delete(engine, db, sid)
    assert info.value.status_code == 409
    assert await db.get_session(sid) is not None


@pytest.mark.asyncio
async def test_the_chat_api_does_not_create_a_deleted_session_again(tmp_path, db):
    from nerve.gateway.routes import _deps as deps_mod
    from nerve.gateway.routes import sessions as routes

    engine, backend = _engine(tmp_path, db)
    sid = "s-api-deleted"
    await _new_session(engine, db, sid)
    await _delete(engine, db, sid)

    saved = deps_mod._deps
    deps_mod.init_deps(engine, db)
    try:
        req = routes.MessageRequest(session_id=sid, message="still there?")
        with pytest.raises(SessionClosedError) as info:
            await routes.chat(req, actor=db.system_actor)
    finally:
        deps_mod._deps = saved

    assert info.value.reason == "deleted"
    assert await db.get_session(sid) is None
    assert len(backend.clients) == 1


@pytest.mark.asyncio
async def test_a_run_never_creates_a_deleted_session_again(tmp_path, db):
    engine, backend = _engine(tmp_path, db)
    sid = "s-gone"
    await _new_session(engine, db, sid)
    assert await _delete(engine, db, sid) == {"deleted": True}

    with pytest.raises(SessionClosedError) as info:
        await engine.run(sid, "[Answer to: Proceed?]\n\nyes", source="notification:web",
                         actor=None)

    assert info.value.reason == "deleted"
    assert await db.get_session(sid) is None
    assert len(backend.clients) == 1


@pytest.mark.asyncio
async def test_a_run_during_a_delete_is_refused(tmp_path, db):
    """The delete mark: a run that takes the run lock while the delete
    disconnects the client is refused and creates nothing."""
    engine, backend = _engine(tmp_path, db)
    sid = "s-deleting"
    await _new_session(engine, db, sid)
    gate = asyncio.Event()
    backend.clients[0].disconnect_gate = gate
    delete = asyncio.create_task(_delete(engine, db, sid))
    await _until(lambda: engine.sessions.get_client(sid) is None)
    assert engine.sessions.closing_reason(sid) == "deleted"

    with pytest.raises(SessionClosedError) as info:
        await asyncio.wait_for(
            engine.run(sid, "a message", source="web", channel="web", actor=None),
            timeout=5,
        )
    gate.set()

    assert info.value.reason == "deleted"
    assert await asyncio.wait_for(delete, timeout=5) == {"deleted": True}
    assert engine.sessions.closing_reason(sid) is None
    assert await db.get_session(sid) is None
    assert len(backend.clients) == 1


# --------------------------------------------------------------------------- #
# Questions
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_archive_expires_questions_with_a_report_and_keeps_approvals(
    tmp_path, db, broadcasts,
):
    engine, _backend = _engine(tmp_path, db)
    service = _notifications(engine, db, tmp_path)
    sid = "s-asks"
    await engine.sessions.get_or_create(sid, source="web", actor=None)
    question = await service.ask_question(sid, "Proceed?", options=["yes", "no"])
    # An approval acts on its target, not on the session: it stays open.
    await db.create_notification(
        notification_id="n-approval", session_id=sid, type="approval",
        title="Merge?", target_kind="mechanical-action", target_id="a-1",
    )
    runs = AsyncMock()
    engine.run = runs  # an expiry note would be a run

    await engine.sessions.archive_session(sid)

    row = await db.get_notification(question["notification_id"])
    assert row["status"] == "expired"
    assert any(
        msg.get("type") == "notification_expired"
        and msg.get("notification_id") == question["notification_id"]
        for _c, msg in broadcasts
    )
    runs.assert_not_called()
    assert (await db.get_notification("n-approval"))["status"] == "pending"


@pytest.mark.asyncio
async def test_delete_expires_questions(tmp_path, db, broadcasts):
    engine, _backend = _engine(tmp_path, db)
    service = _notifications(engine, db, tmp_path)
    sid = "s-asks-deleted"
    await engine.sessions.get_or_create(sid, source="web", actor=None)
    question = await service.ask_question(sid, "Proceed?", options=["yes", "no"])

    await _delete(engine, db, sid)

    assert (await db.get_notification(question["notification_id"]))["status"] == "expired"
    assert any(msg.get("type") == "notification_expired" for _c, msg in broadcasts)


@pytest.mark.asyncio
@pytest.mark.parametrize("close", ["delete", "archive"])
async def test_an_answer_to_a_closed_session_is_stored_and_starts_no_turn(
    tmp_path, db, close,
):
    engine, _backend = _engine(tmp_path, db)
    service = _notifications(engine, db, tmp_path)
    sid = "s-answered"
    await engine.sessions.get_or_create(sid, source="web", actor=None)
    question = await service.ask_question(sid, "Delete branch?", options=["yes", "no"])
    nid = question["notification_id"]
    # Close the session without the archive or delete paths, which would
    # expire the question.
    if close == "delete":
        await db.delete_session(sid)
    else:
        await db.change_session_status(sid, "archived", event_type="archived")
    runs = AsyncMock()
    engine.run = runs

    assert await service.handle_answer(nid, "yes", answered_by="web") is True
    await asyncio.sleep(0)

    runs.assert_not_called()
    assert (await db.get_notification(nid))["status"] == "answered"
    if close == "delete":
        assert await db.get_session(sid) is None
    else:
        assert (await db.get_session(sid))["status"] == "archived"


@pytest.mark.asyncio
async def test_idle_cleanup_expires_questions_only_after_the_archive(tmp_path, db):
    engine, _backend = _engine(tmp_path, db)
    service = _notifications(engine, db, tmp_path)
    old = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    busy, idle = "s-busy", "s-idle"
    questions = {}
    for sid in (busy, idle):
        await engine.sessions.get_or_create(sid, source="web", actor=None)
        await db.update_session_fields(sid, {"status": "idle", "last_activity_at": old})
        await db.db.execute("UPDATE sessions SET updated_at = ? WHERE id = ?", (old, sid))
        await db.db.commit()
        questions[sid] = (await service.ask_question(sid, "Proceed?", options=["yes"]))["notification_id"]
    # A turn of the busy session holds its run lock, so its archive fails.
    pending_turn = asyncio.create_task(asyncio.Event().wait())
    engine.sessions.set_turn_task(busy, pending_turn)

    try:
        stats = await engine.sessions.run_cleanup(interactive_archive_after_hours=1)
    finally:
        pending_turn.cancel()

    assert stats["archived_interactive"] == 1
    assert (await db.get_session(busy))["status"] == "idle"
    assert (await db.get_notification(questions[busy]))["status"] == "pending"
    assert (await db.get_session(idle))["status"] == "archived"
    assert (await db.get_notification(questions[idle]))["status"] == "expired"
