"""Drain before shutdown: stop accepting new turns, let running turns end.

The first SIGTERM or SIGINT closes the engine's turn gate and waits up to
``gateway.drain_timeout_seconds`` for running turns, while the server still
serves. These tests pin each part: the gate in ``AgentEngine.run``, the wait
in ``AgentEngine.drain``, the signal handling in ``_DrainingServer``, the
schedulers that must not start work into a closed gate, and what each
ingress tells the user when the gate is closed.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import uvicorn
from fastapi.testclient import TestClient
from sse_starlette.sse import AppStatus

from nerve.agent.engine import AgentEngine, NotAcceptingTurnsError
from nerve.agent.streaming import broadcaster
from nerve.channels.base import BaseChannel, ChannelCapability, InboundMessage
from nerve.channels.router import ChannelRouter
from nerve.config import AuthConfig, GatewayConfig, NerveConfig, set_config
from nerve.cron.jobs import CronJob
from nerve.cron.service import CronService
from nerve.gateway import server as gw
from nerve.gateway.auth import create_session_token, pin_jwt_secret
from nerve.workflows.service import WorkflowRunService

_MESSAGE = str(NotAcceptingTurnsError())


def _make_engine() -> AgentEngine:
    """AgentEngine with mocked config/db, and a fast drain poll."""
    config = MagicMock()
    config.agent.max_concurrent = 3
    config.mcp_servers = []
    engine = AgentEngine(config, AsyncMock())
    engine._broadcast_session_running = AsyncMock()
    engine._DRAIN_POLL_SECONDS = 0.01
    return engine


def _blocking_turns(engine: AgentEngine) -> tuple[asyncio.Event, list[str]]:
    """Make each turn wait for the returned event. Records each message."""
    release = asyncio.Event()
    started: list[str] = []

    async def _run_inner(session_id, user_message, *args, **kwargs):
        started.append(user_message)
        await release.wait()
        return "done"

    engine._run_inner = _run_inner
    return release, started


async def _until(predicate, within: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while not predicate():
        if loop.time() > deadline:
            pytest.fail("condition not met in time")
        await asyncio.sleep(0.005)


@contextlib.asynccontextmanager
async def _listening(session_id: str):
    """Collect what the broadcaster sends to the session's clients."""
    events: list[dict] = []

    async def _collect(_sid, message):
        events.append(message)

    await broadcaster.register(session_id, "test-drain", _collect)
    try:
        yield events
    finally:
        await broadcaster.unregister(session_id, "test-drain")


# ---------------------------------------------------------------------------
# AgentEngine: the gate and the wait
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestEngineGate:
    async def test_accepts_turns_until_told_otherwise(self):
        engine = _make_engine()
        assert engine.accepting_turns
        engine.stop_accepting_turns()
        assert not engine.accepting_turns

    async def test_a_new_turn_is_refused_and_the_clients_are_told(self):
        engine = _make_engine()
        engine._run_inner = AsyncMock(return_value="done")
        engine.stop_accepting_turns()

        async with _listening("s-refused") as events:
            with pytest.raises(NotAcceptingTurnsError):
                await engine.run("s-refused", "hello", actor=None)

        engine._run_inner.assert_not_awaited()
        assert events == [
            {"type": "error", "session_id": "s-refused", "error": _MESSAGE},
        ]
        assert not engine.is_session_running("s-refused")
        assert not broadcaster.is_buffering("s-refused")

    async def test_a_turn_queued_behind_the_session_lock_is_refused(self):
        engine = _make_engine()
        release, started = _blocking_turns(engine)

        first = asyncio.create_task(engine.run("s-queue", "first", actor=None))
        await _until(lambda: engine.is_session_running("s-queue"))
        second = asyncio.create_task(engine.run("s-queue", "second", actor=None))
        # Let the second turn pass the first check and wait for the lock.
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        engine.stop_accepting_turns()
        release.set()

        assert await first == "done"
        with pytest.raises(NotAcceptingTurnsError):
            await second
        assert started == ["first"]
        assert not broadcaster.is_buffering("s-queue")


@pytest.mark.asyncio
class TestEngineDrain:
    async def test_nothing_running_returns_at_once(self):
        engine = _make_engine()
        assert await engine.drain(5) is True
        assert not engine.accepting_turns

    async def test_waits_for_the_running_turn_to_end(self):
        engine = _make_engine()
        release, _ = _blocking_turns(engine)
        turn = asyncio.create_task(engine.run("s-wait", "work", actor=None))
        await _until(lambda: engine.is_session_running("s-wait"))

        drain = asyncio.create_task(engine.drain(5))
        await asyncio.sleep(0.05)
        assert not drain.done()

        release.set()
        assert await drain is True
        assert await turn == "done"

    async def test_timeout_leaves_the_turn_running(self):
        engine = _make_engine()
        release, _ = _blocking_turns(engine)
        turn = asyncio.create_task(engine.run("s-slow", "work", actor=None))
        await _until(lambda: engine.is_session_running("s-slow"))

        assert await engine.drain(0.05) is False
        assert engine.is_session_running("s-slow")

        release.set()
        assert await turn == "done"


# ---------------------------------------------------------------------------
# _DrainingServer: signal handling
# ---------------------------------------------------------------------------


async def _asgi_app(scope, receive, send):  # pragma: no cover - never served
    pass


class _FakeDrainEngine:
    def __init__(self) -> None:
        self.timeouts: list[float] = []
        self.release = asyncio.Event()
        self.error: Exception | None = None

    async def drain(self, timeout: float) -> bool:
        self.timeouts.append(timeout)
        if self.error is not None:
            raise self.error
        await self.release.wait()
        return True


@pytest.fixture
def drain_server(monkeypatch):
    # sse_starlette patches uvicorn's Server.handle_exit to set this
    # process-wide flag, which ends every SSE stream. Restore it after the
    # test so later MCP tests still get their responses.
    monkeypatch.setattr(AppStatus, "should_exit", False)

    def _make(drain_timeout: int = 30):
        engine = _FakeDrainEngine()
        config = NerveConfig()
        config.gateway.drain_timeout_seconds = drain_timeout
        monkeypatch.setattr(gw, "_engine", engine)
        monkeypatch.setattr(gw, "get_config", lambda: config)
        server = gw._DrainingServer(uvicorn.Config(_asgi_app, log_config=None))
        return server, engine

    return _make


@pytest.mark.asyncio
class TestDrainingServer:
    async def test_first_signal_drains_before_the_exit(self, drain_server):
        server, engine = drain_server(30)

        server.handle_exit(signal.SIGTERM, None)
        assert not server.should_exit
        assert await server.on_tick(1) is False
        await _until(lambda: engine.timeouts)
        assert engine.timeouts == [30]
        assert not server.should_exit

        engine.release.set()
        await server._drain_task
        assert server.should_exit
        assert not server.force_exit

    async def test_sse_streams_stay_open_until_the_drain_ends(self, drain_server):
        server, engine = drain_server(30)
        server.handle_exit(signal.SIGTERM, None)
        await server.on_tick(1)
        await _until(lambda: engine.timeouts)
        assert AppStatus.should_exit is False

        engine.release.set()
        await server._drain_task
        assert AppStatus.should_exit is True

    async def test_second_signal_exits_without_waiting(self, drain_server):
        server, engine = drain_server(30)
        server.handle_exit(signal.SIGTERM, None)
        await server.on_tick(1)
        await _until(lambda: engine.timeouts)

        server.handle_exit(signal.SIGTERM, None)
        assert server.should_exit

        # The uvicorn shutdown cancels the drain and still runs the lifespan
        # shutdown.
        server.servers = []
        server.lifespan = SimpleNamespace(shutdown=AsyncMock())
        await server.shutdown()
        assert server._drain_task.cancelled()
        server.lifespan.shutdown.assert_awaited_once()

    async def test_drain_that_ends_after_a_second_signal_does_not_force_exit(
        self, drain_server,
    ):
        server, engine = drain_server(30)
        server.handle_exit(signal.SIGINT, None)
        await server.on_tick(1)
        await _until(lambda: engine.timeouts)
        server.handle_exit(signal.SIGINT, None)

        engine.release.set()
        await server._drain_task
        assert server.should_exit
        assert not server.force_exit

    async def test_zero_timeout_exits_at_the_next_tick(self, drain_server):
        server, engine = drain_server(0)
        server.handle_exit(signal.SIGTERM, None)
        await server.on_tick(1)
        await server._drain_task
        assert server.should_exit
        assert engine.timeouts == []

    async def test_a_failed_drain_still_exits(self, drain_server):
        server, engine = drain_server(30)
        engine.error = RuntimeError("boom")
        server.handle_exit(signal.SIGTERM, None)
        await server.on_tick(1)
        await server._drain_task
        assert server.should_exit


def test_run_server_exits_with_startup_failure_when_never_started(monkeypatch):
    monkeypatch.setattr(uvicorn, "Config", lambda app, **kw: SimpleNamespace(**kw))
    monkeypatch.setattr(gw, "create_app", lambda: _asgi_app)
    monkeypatch.setattr(gw._DrainingServer, "run", lambda self: None)
    with pytest.raises(SystemExit) as exc:
        gw.run_server(NerveConfig())
    assert exc.value.code == 3


# ---------------------------------------------------------------------------
# Schedulers start nothing into a closed gate
# ---------------------------------------------------------------------------


def _cron_service(*, accepting: bool) -> CronService:
    config = MagicMock()
    config.timezone = "UTC"
    engine = AsyncMock()
    engine.accepting_turns = accepting
    return CronService(config, engine, AsyncMock())


def _job() -> CronJob:
    return CronJob(id="nightly", schedule="1h", prompt="do stuff")


@pytest.mark.asyncio
class TestSchedulersDuringDrain:
    async def test_a_due_cron_job_is_skipped_without_a_log_row(self):
        svc = _cron_service(accepting=False)
        await svc._run_job_inner(_job())
        # No row: the catch-up after the restart sees the job as overdue.
        svc.db.log_cron_start.assert_not_awaited()
        svc.engine.run_cron.assert_not_awaited()

    async def test_a_manual_trigger_is_refused(self):
        svc = _cron_service(accepting=False)
        svc._jobs = [_job()]
        with pytest.raises(NotAcceptingTurnsError):
            await svc.run_job("nightly")
        svc.db.log_cron_start.assert_not_awaited()

    async def test_due_wakeups_stay_pending(self):
        svc = _cron_service(accepting=False)
        await svc._sweep_wakeups()
        svc.db.get_due_wakeups.assert_not_awaited()
        svc.db.claim_wakeup.assert_not_awaited()

    async def test_queued_workflow_runs_stay_pending(self):
        svc = WorkflowRunService(
            MagicMock(), AsyncMock(), MagicMock(accepting_turns=False),
        )
        await svc._maybe_dispatch()
        svc.db.next_pending_workflow_runs.assert_not_awaited()
        svc.db.transition_workflow_run.assert_not_awaited()


# ---------------------------------------------------------------------------
# Ingress: the WebSocket and REST answers
# ---------------------------------------------------------------------------

_SECRET = "test-secret-for-drain-before-shutdown"
_SESSION = "a-session-id"


class _ClosedGateEngine:
    """Enough engine for the socket handler and /api/chat, gate closed."""

    accepting_turns = False

    def __init__(self, db):
        self.db = db
        self.router = SimpleNamespace(
            get_last_session=self._session, switch_session=AsyncMock(),
        )
        self.sessions = SimpleNamespace(get_active_session=self._session)
        self.run = AsyncMock(side_effect=NotAcceptingTurnsError())

    async def _session(self, *_args, **_kwargs):
        return _SESSION

    def is_session_running(self, *_args, **_kwargs):
        return False


@contextlib.asynccontextmanager
async def _no_lifespan(_app):
    yield


@pytest.fixture
def instance(tmp_path, open_identity_db, monkeypatch):
    from nerve.gateway.routes import _deps as deps_module

    config_dir = tmp_path / "config"
    workspace = tmp_path / "workspace"
    config_dir.mkdir()
    workspace.mkdir()
    set_config(NerveConfig(
        auth=AuthConfig(jwt_secret=_SECRET),
        config_dir=config_dir,
        workspace=workspace,
    ))
    pin_jwt_secret(_SECRET)
    app = gw.create_app()
    app.router.lifespan_context = _no_lifespan
    with TestClient(app) as client:
        database, identity = client.portal.call(open_identity_db, tmp_path / "nerve.db")
        engine = _ClosedGateEngine(database)
        monkeypatch.setattr(
            deps_module, "_deps", deps_module.RouteDeps(engine=engine, db=database),
        )
        monkeypatch.setattr(gw, "_engine", engine, raising=False)
        token = create_session_token(_SECRET, identity.owner_account_id)
        try:
            yield SimpleNamespace(client=client, engine=engine, token=token)
        finally:
            client.portal.call(database.close)
    set_config(NerveConfig())


def test_websocket_message_gets_an_error_and_starts_no_turn(instance):
    with instance.client.websocket_connect(f"/ws?token={instance.token}") as ws:
        assert ws.receive_json()["type"] == "session_switched"
        ws.send_json({"type": "message", "content": "hello"})
        assert ws.receive_json() == {
            "type": "error", "session_id": _SESSION, "error": _MESSAGE,
        }
        # The socket still works: only new turns are refused.
        ws.send_json({"type": "ping"})
        assert ws.receive_json() == {"type": "pong"}
    instance.engine.run.assert_not_awaited()


class _TextChannel(BaseChannel):
    """A channel with text only: the stream adapter sends errors as text."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    @property
    def name(self) -> str:
        return "telegram"

    @property
    def capabilities(self) -> ChannelCapability:
        return ChannelCapability.SEND_TEXT

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, message) -> None:
        self.sent.append(message.text)


@pytest.mark.asyncio
async def test_channel_message_is_refused_with_one_error(monkeypatch):
    """The stream adapter sends the error. The Telegram handler sends nothing."""
    monkeypatch.setattr(ChannelRouter, "BATCH_DEBOUNCE", 0)
    engine = _make_engine()
    engine.stop_accepting_turns()
    router = ChannelRouter(engine)
    channel = _TextChannel()
    router.register(channel)

    with pytest.raises(NotAcceptingTurnsError):
        await router.handle_message(InboundMessage(
            channel_name="telegram", channel_key="telegram:1", sender_id="1",
            text="hello", session_id="s-telegram",
        ))
    assert channel.sent == [f"Error: {_MESSAGE}"]


def test_rest_chat_returns_503(instance):
    response = instance.client.post(
        "/api/chat",
        json={"message": "hello", "session_id": _SESSION},
        headers={"Authorization": f"Bearer {instance.token}"},
    )
    assert response.status_code == 503
    assert response.json() == {"detail": _MESSAGE}


# ---------------------------------------------------------------------------
# Config and CLI
# ---------------------------------------------------------------------------


class TestDrainTimeoutSetting:
    def test_default_is_no_wait(self):
        assert GatewayConfig().drain_timeout_seconds == 0
        assert GatewayConfig.from_dict({}).drain_timeout_seconds == 0

    def test_string_from_an_env_reference_becomes_an_int(self):
        assert GatewayConfig.from_dict(
            {"drain_timeout_seconds": "120"},
        ).drain_timeout_seconds == 120


class TestCliShutdownWait:
    def test_default_waits_15_seconds(self):
        from nerve.cli import _shutdown_wait_ticks

        assert _shutdown_wait_ticks(NerveConfig()) == 30

    def test_wait_covers_the_drain(self):
        from nerve.cli import _shutdown_wait_ticks

        config = NerveConfig()
        config.gateway.drain_timeout_seconds = 45
        assert _shutdown_wait_ticks(config) == 120
