"""The context breakdown and Compact: the Claude client's context report,
``GET /api/sessions/{id}/context`` and ``POST /api/sessions/{id}/compact``
against a fake live client."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from nerve.agent.backends import events as ev
from nerve.agent.backends.base import SessionSpec, TransportDiedError
from nerve.agent.backends.claude import ClaudeClient, context_breakdown
from nerve.agent.engine import AgentEngine
from nerve.agent.streaming import broadcaster
from nerve.config import NerveConfig

SID = "s-ctx1"
CONTEXT_KEYS = {
    "session_id", "live", "can_compact", "total_tokens", "max_tokens",
    "auto_compact_threshold", "categories",
}

# The CLI's report for a short session (shape of get_context_usage).
REPORT = {
    "model": "claude-test", "totalTokens": 20237, "maxTokens": 200000,
    "rawMaxTokens": 200000, "percentage": 10, "isAutoCompactEnabled": True,
    "autoCompactThreshold": 167000,
    "categories": [
        {"name": "System prompt", "tokens": 170, "color": "promptBorder"},
        {"name": "System tools", "tokens": 15149, "color": "inactive"},
        {"name": "System tools (deferred)", "tokens": 14369, "isDeferred": True},
        {"name": "MCP tools", "tokens": 0},
        {"name": "Skills", "tokens": 1715},
        {"name": "Messages", "tokens": 3203},
        {"name": "Autocompact buffer", "tokens": 33000},
        {"name": "Free space", "tokens": 146763},
    ],
    "mcpTools": [], "memoryFiles": [], "agents": [],
}
BREAKDOWN = {
    "total_tokens": 20237, "max_tokens": 200000, "auto_compact_threshold": 167000,
    "categories": [
        {"name": "System prompt", "tokens": 170},
        {"name": "System tools", "tokens": 15149},
        {"name": "Skills", "tokens": 1715},
        {"name": "Messages", "tokens": 3203},
    ],
}
COMPACTED = {**BREAKDOWN, "total_tokens": 17862, "categories": [
    *BREAKDOWN["categories"][:3], {"name": "Messages", "tokens": 828},
]}


# --------------------------------------------------------------------------- #
#  The Claude client                                                           #
# --------------------------------------------------------------------------- #


def test_the_breakdown_keeps_only_the_categories_in_the_total():
    out = context_breakdown(REPORT)
    assert out == BREAKDOWN
    assert sum(c["tokens"] for c in out["categories"]) == out["total_tokens"]


def test_the_breakdown_has_no_threshold_when_auto_compact_is_off():
    out = context_breakdown({**REPORT, "isAutoCompactEnabled": False})
    assert out["auto_compact_threshold"] is None


class _FakeSDK:
    def __init__(self, *, error: Exception | None = None):
        self.queries: list = []
        self._error = error

    async def get_context_usage(self):
        if self._error:
            raise self._error
        return REPORT

    async def query(self, prompt):
        if self._error:
            raise self._error
        self.queries.append(prompt)


def _claude(sdk: _FakeSDK) -> ClaudeClient:
    client = ClaudeClient.__new__(ClaudeClient)
    client._spec = SessionSpec(
        session_id=SID, source="web", model="m", effort="high", system_prompt="", cwd="/tmp",
    )
    client._sdk = sdk
    return client


@pytest.mark.asyncio
async def test_the_client_reports_the_breakdown():
    assert await _claude(_FakeSDK()).context_usage() == BREAKDOWN


@pytest.mark.asyncio
async def test_compact_sends_the_slash_command_unescaped():
    sdk = _FakeSDK()
    await _claude(sdk).start_compact()
    assert sdk.queries == ["/compact"]


@pytest.mark.asyncio
async def test_a_dead_transport_raises_transport_died():
    from claude_agent_sdk._errors import CLIConnectionError

    client = _claude(_FakeSDK(error=CLIConnectionError("Not connected")))
    with pytest.raises(TransportDiedError):
        await client.context_usage()
    with pytest.raises(TransportDiedError):
        await client.start_compact()


# --------------------------------------------------------------------------- #
#  The routes                                                                  #
# --------------------------------------------------------------------------- #


class _Client:
    """A live runtime: ``frames`` is what the compact turn yields."""

    native_session_id = "native-1"
    model = "claude-test"

    def __init__(self, frames: list | None = None, *, hang: bool = False):
        self.frames = frames or []
        self.hang = hang
        self.report = BREAKDOWN
        self.calls: list[str] = []
        self.interrupted = asyncio.Event()

    async def context_usage(self):
        self.calls.append("context_usage")
        return self.report

    async def start_compact(self):
        self.calls.append("start_compact")

    async def receive_turn(self):
        if self.hang:
            await asyncio.Event().wait()
        for event in self.frames:
            if isinstance(event, ev.SystemEvent) and event.subtype == "compact_boundary":
                self.report = COMPACTED
            yield event

    async def interrupt(self, *, cancel_queued: bool = False):
        self.interrupted.set()

    def is_alive(self) -> bool:
        return True

    async def disconnect(self, timeout: float = 5.0) -> None:
        pass

    def try_receive_idle_events(self):
        return None

    def buffer_used(self) -> int:
        return 0


def _compact_frames() -> list:
    return [
        ev.SystemEvent("status", {"status": "compacting"}),
        ev.SystemEvent("status", {"status": None, "compact_result": "success"}),
        ev.SystemEvent("init", {}),
        ev.SystemEvent("compact_boundary", {"compact_metadata": {
            "trigger": "manual", "pre_tokens": 35177, "post_tokens": 2300,
        }}),
        # The CLI reports zero usage and its cumulative cost for a compact.
        ev.TurnCompleted(
            native_session_id="native-1", usage=ev.NormalizedUsage(),
            total_cost_usd=0.3564, num_turns=0,
        ),
    ]


@pytest_asyncio.fixture
async def api(db, tmp_path, bypass_auth, monkeypatch):
    from nerve.gateway.auth import require_auth
    from nerve.gateway.routes import _deps as deps_module
    from nerve.gateway.routes.session_context import router

    workspace = tmp_path / "ws"
    workspace.mkdir()
    cfg = NerveConfig.from_dict({"workspace": str(workspace), "agent": {"backend": "claude"}})
    engine = AgentEngine(cfg, db)
    engine._STOP_WAIT_SECONDS = 0.3
    monkeypatch.setattr(deps_module, "_deps", deps_module.RouteDeps(engine=engine, db=db))
    await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)
    await db.update_session_metadata(SID, {"last_usage": {
        "input_tokens": 10, "cache_read_input_tokens": 60000,
        "cache_creation_input_tokens": 6000, "output_tokens": 400,
        "max_context_tokens": 200000, "num_turns": 2,
    }, "_sdk_cumulative_cost": 0.2619})
    await db.update_session_fields(SID, {"total_cost_usd": 0.2619})
    sent: list[dict] = []

    async def _listen(_sid, msg):
        sent.append(msg)

    await broadcaster.register(SID, "context-test", _listen)
    await broadcaster.register("__global__", "context-test", _listen)
    app = FastAPI()
    app.include_router(router)
    bypass_auth(app)
    app.dependency_overrides[require_auth] = lambda: db.system_actor
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
        ) as client:
            yield SimpleNamespace(client=client, engine=engine, db=db, sent=sent)
    finally:
        await broadcaster.unregister(SID, "context-test")
        await broadcaster.unregister("__global__", "context-test")


def _detail(response: httpx.Response) -> dict:
    detail = response.json()["detail"]
    assert set(detail) == {"code", "message"}
    return detail


async def _last_usage(db) -> dict:
    session = await db.get_session(SID)
    return json.loads(session["metadata"])["last_usage"]


async def _compacted_events(db) -> list[dict]:
    rows = await db.get_session_events(SID)
    return [r for r in rows if r["event_type"] == "compacted"]


@pytest.mark.asyncio
async def test_context_of_a_live_claude_session_has_its_categories(api):
    api.engine.sessions.set_client(SID, _Client())
    body = (await api.client.get(f"/api/sessions/{SID}/context")).json()
    assert set(body) == CONTEXT_KEYS
    assert body == {"session_id": SID, "live": True, "can_compact": True, **BREAKDOWN}


@pytest.mark.asyncio
async def test_context_without_a_live_client_has_the_last_turn_totals(api):
    body = (await api.client.get(f"/api/sessions/{SID}/context")).json()
    assert body == {
        "session_id": SID, "live": False, "can_compact": False,
        "total_tokens": 33005, "max_tokens": 200000,
        "auto_compact_threshold": None, "categories": [],
    }


@pytest.mark.asyncio
async def test_context_of_a_codex_session_has_the_totals(api):
    api.engine.sessions.set_client(SID, _Client())
    api.engine._session_backends[SID] = "codex"
    body = (await api.client.get(f"/api/sessions/{SID}/context")).json()
    assert (body["live"], body["can_compact"], body["categories"]) == (False, False, [])
    assert body["total_tokens"] == 33005


@pytest.mark.asyncio
async def test_context_when_the_runtime_does_not_answer_has_the_totals(api):
    client = _Client()

    async def _fails():
        raise TransportDiedError("Not connected")

    client.context_usage = _fails
    api.engine.sessions.set_client(SID, client)
    body = (await api.client.get(f"/api/sessions/{SID}/context")).json()
    assert (body["live"], body["can_compact"], body["total_tokens"]) == (False, True, 33005)


@pytest.mark.asyncio
async def test_context_of_an_unknown_session_404(api):
    for response in (
        await api.client.get("/api/sessions/s-none/context"),
        await api.client.post("/api/sessions/s-none/compact"),
    ):
        assert response.status_code == 404
        assert _detail(response)["code"] == "session_not_found"


@pytest.mark.asyncio
async def test_compact_runs_as_a_turn_and_returns_the_new_context(api):
    client = _Client(_compact_frames())
    api.engine.sessions.set_client(SID, client)
    # A runtime about to be recycled by the idle sweep.
    api.engine.sessions._last_activity[SID] = asyncio.get_running_loop().time() - 3600
    assert SID in api.engine.sessions.get_idle_client_ids(1800)
    response = await api.client.post(f"/api/sessions/{SID}/compact")
    assert response.status_code == 200
    assert response.json() == {"session_id": SID, "live": True, "can_compact": True, **COMPACTED}
    assert client.calls[0] == "start_compact"

    # A turn: the session ran and stopped; done carries the measured use.
    running = [
        m["is_running"] for m in api.sent
        if m["type"] == "session_running" and m["session_id"] == SID
    ]
    assert running == [True, False]
    (done,) = [m for m in api.sent if m["type"] == "done"]
    assert done["usage"]["context_tokens"] == COMPACTED["total_tokens"]
    assert done["max_context_tokens"] == 200000 and done["num_turns"] == 1
    assert not api.engine.sessions.is_running(SID)
    assert SID not in api.engine.sessions.get_idle_client_ids(1800)

    # The context bar reads it after a reload.
    usage = await _last_usage(api.db)
    assert usage["context_tokens"] == COMPACTED["total_tokens"]
    assert usage["input_tokens"] == 0 and usage["max_context_tokens"] == 200000

    # The compact's cost is the session's, as for any turn.
    session = await api.db.get_session(SID)
    assert session["total_cost_usd"] == pytest.approx(0.3564)
    assert json.loads(session["metadata"])["_sdk_cumulative_cost"] == pytest.approx(0.3564)

    # Nothing in the transcript.
    assert await api.db.get_messages(SID) == []

    # One session event names who compacted, for whom.
    (event,) = await _compacted_events(api.db)
    assert event["actor_id"] == api.db.system_actor_id
    assert event["caused_by_actor_id"] == api.db.system_actor_id
    assert event["details"] is None


@pytest.mark.asyncio
async def test_compact_with_nothing_to_compact_shows_no_reply(api):
    client = _Client([
        ev.TextDelta(text="Error: No messages to compact"),
        ev.TurnCompleted(native_session_id="native-1", usage=ev.NormalizedUsage()),
    ])
    api.engine.sessions.set_client(SID, client)
    response = await api.client.post(f"/api/sessions/{SID}/compact")
    assert response.status_code == 409
    assert _detail(response) == {
        "code": "nothing_to_compact", "message": "Nothing to compact yet.",
    }
    assert not [m for m in api.sent if m["type"] == "token"]
    assert await api.db.get_messages(SID) == []
    # The bar shows the measured use, which did not change.
    (done,) = [m for m in api.sent if m["type"] == "done"]
    assert done["usage"]["context_tokens"] == BREAKDOWN["total_tokens"]
    assert await _compacted_events(api.db) == []


@pytest.mark.asyncio
async def test_a_failed_compact_is_502_and_shows_no_reply(api):
    api.engine.sessions.set_client(SID, _Client([
        ev.TurnCompleted(status="failed", error="API error (HTTP 529)"),
    ]))
    response = await api.client.post(f"/api/sessions/{SID}/compact")
    assert response.status_code == 502
    detail = _detail(response)
    assert detail["code"] == "failed" and "HTTP 529" in detail["message"]
    assert not [m for m in api.sent if m["type"] == "token"]
    assert await api.db.get_messages(SID) == []
    assert (await _last_usage(api.db))["context_tokens"] == BREAKDOWN["total_tokens"]
    assert await _compacted_events(api.db) == []


@pytest.mark.asyncio
async def test_compact_without_a_live_client_or_on_codex_is_409(api):
    response = await api.client.post(f"/api/sessions/{SID}/compact")
    assert response.status_code == 409
    assert _detail(response)["code"] == "unavailable"

    client = _Client(_compact_frames())
    api.engine.sessions.set_client(SID, client)
    api.engine._session_backends[SID] = "codex"
    response = await api.client.post(f"/api/sessions/{SID}/compact")
    assert _detail(response)["code"] == "unavailable"
    assert client.calls == []


@pytest.mark.asyncio
async def test_compact_while_a_turn_holds_the_run_lock_is_busy(api):
    client = _Client(_compact_frames())
    api.engine.sessions.set_client(SID, client)
    lock = api.engine._session_locks.setdefault(SID, asyncio.Lock())
    async with lock:
        response = await api.client.post(f"/api/sessions/{SID}/compact")
    assert response.status_code == 409
    assert _detail(response)["code"] == "busy"
    assert client.calls == []


@pytest.mark.asyncio
async def test_stop_ends_a_compact_that_hangs(api):
    client = _Client(hang=True)
    api.engine.sessions.set_client(SID, client)
    request = asyncio.create_task(api.client.post(f"/api/sessions/{SID}/compact"))
    for _ in range(200):
        if api.engine.sessions.turn_task(SID) is not None:
            break
        await asyncio.sleep(0.01)
    result = await api.engine.stop_turn(SID)
    assert result["turn"] in ("stopped", "killed")
    response = await request
    assert response.status_code == 409
    assert _detail(response)["code"] == "stopped"
    # The runtime can be mid-compact: the next message starts a new one.
    assert api.engine.sessions.get_client(SID) is None
    assert await _compacted_events(api.db) == []


@pytest.mark.asyncio
async def test_a_runtime_that_dies_during_a_compact_is_discarded(api):
    client = _Client()

    async def _dies():
        raise TransportDiedError("claude runtime ended the turn without a result")
        yield  # an async generator, like receive_turn

    client.receive_turn = _dies
    api.engine.sessions.set_client(SID, client)
    response = await api.client.post(f"/api/sessions/{SID}/compact")
    assert response.status_code == 502
    assert "without a result" in _detail(response)["message"]
    assert api.engine.sessions.get_client(SID) is None
    assert not api.engine.sessions.is_running(SID)
