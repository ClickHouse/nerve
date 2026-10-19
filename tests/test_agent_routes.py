"""The agent REST routes against the JSON shapes in docs/api.md.

Every key of every shape is asserted: the web client is written against the
contract, not against this implementation.
"""

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from nerve.agent.backends import events as ev
from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig

SID = "s-1234"

SNAPSHOT_KEYS = {"session_id", "agents", "summary", "capabilities", "queue", "generated_at"}
AGENT_VIEW_KEYS = {
    "id", "session_id", "backend", "kind", "task_id", "task_type",
    "spawn_tool_use_id", "parent_agent_id", "depth", "name", "label",
    "description", "agent_type", "command", "background", "ambient", "status",
    "end_reason", "error", "attention", "activity", "summary", "result_preview",
    "requested_model", "model", "model_changed", "tokens", "tool_uses",
    "cost_usd", "runs", "created_at", "started_at", "run_started_at",
    "last_activity_at", "ended_at", "stop_requested_at", "stopped_by_actor_id",
    "resumable", "controls", "workflow", "rev",
}
SUMMARY_KEYS = {
    "total", "live", "starting", "running", "stopping", "completed", "failed",
    "stopped", "interrupted", "lost", "attention", "shells_live", "tokens",
    "cost_usd", "oldest_live_started_at",
}
CAPABILITY_KEYS = {
    "stop_task", "transcript", "output", "interrupt_spares_background",
    "quiet_after_minutes",
}
CONTROL_KEYS = {"stop", "transcript", "output", "message"}
QUEUE_KEYS = {"queue_id", "client_msg_id", "preview", "source", "internal", "actor_id", "created_at"}
BRIEF_KEYS = {"id", "label", "kind", "status", "started_at", "tokens"}
ITEM_KEYS = {
    "tool_call": {"seq", "at", "kind", "tool_use_id", "tool", "input", "input_truncated", "child_agent_id"},
    "tool_result": {"seq", "at", "kind", "tool_use_id", "result", "is_error", "truncated", "full_size"},
    "text": {"seq", "at", "kind", "text"},
    "thinking": {"seq", "at", "kind", "text"},
    "lifecycle": {"seq", "at", "kind", "event", "run", "reason", "actor_id"},
}


class _Client:
    """A live runtime with per-task stop."""

    native_session_id = "native-1"
    model = "claude-test"

    def __init__(self, *, supports: bool = True) -> None:
        self._supports = supports
        self.calls: list[tuple] = []
        self.turn_started = asyncio.Event()
        self._turn_end: asyncio.Event | None = None

    @property
    def supports_task_stop(self) -> bool:
        return self._supports

    @property
    def interrupt_spares_background(self) -> bool:
        return True

    async def stop_task(self, task_id: str) -> None:
        self.calls.append(("stop_task", task_id))

    async def interrupt(self, *, cancel_queued: bool = False) -> None:
        self.calls.append(("interrupt", cancel_queued))
        if self._turn_end is not None:
            self._turn_end.set()

    async def start_turn(self, turn) -> None:
        pass

    async def receive_turn(self):
        end = self._turn_end = asyncio.Event()
        self.turn_started.set()
        await end.wait()
        yield ev.TurnCompleted(native_session_id="native-1")

    def is_alive(self) -> bool:
        return True

    async def disconnect(self, timeout: float = 5.0) -> None:
        pass

    def try_receive_idle_events(self):
        return None

    def buffer_used(self) -> int:
        return 0


@pytest_asyncio.fixture
async def api(db, tmp_path, bypass_auth, monkeypatch):
    from nerve.gateway.auth import require_auth
    from nerve.gateway.routes import _deps as deps_module
    from nerve.gateway.routes.agents import router as agents_router
    from nerve.gateway.routes.sessions import router as sessions_router

    workspace = tmp_path / "ws"
    workspace.mkdir()
    cfg = NerveConfig.from_dict({"workspace": str(workspace), "agent": {"backend": "claude"}})
    engine = AgentEngine(cfg, db)
    engine._STOP_WAIT_SECONDS = 0.3
    monkeypatch.setattr(deps_module, "_deps", deps_module.RouteDeps(engine=engine, db=db))
    await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)
    app = FastAPI()
    app.include_router(sessions_router)
    app.include_router(agents_router)
    bypass_auth(app)
    # Stops and queue entries record who asked: an actor that exists.
    app.dependency_overrides[require_auth] = lambda: db.system_actor
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
    ) as client:
        yield SimpleNamespace(client=client, engine=engine, db=db)


async def _agent(engine: AgentEngine, tool_use_id: str, task_id: str, *,
                 parent: str | None = None, sid: str = SID):
    call = ev.ToolUse(
        tool_use_id=tool_use_id, name="Agent",
        input={"description": f"agent {task_id}", "prompt": "do the work",
               "subagent_type": "general-purpose", "name": f"name-{task_id}"},
        parent_tool_use_id=parent,
    )
    if parent is None:
        await engine.agents.note_main_tool_use(sid, call)
    else:
        await engine.agents.ingest_sidechain(sid, call)
    await engine.agents.on_system_event(sid, ev.SystemEvent("task_started", {
        "task_id": task_id, "tool_use_id": tool_use_id, "task_type": "local_agent",
        "is_backgrounded": True,
    }))
    return next(r for r in engine.agents._sessions[sid].records.values() if r.task_id == task_id)


def _detail(response: httpx.Response) -> dict:
    body = response.json()
    assert set(body) == {"detail"}
    assert {"code", "message"} <= set(body["detail"])
    return body["detail"]


# --------------------------------------------------------------------------- #
#  GET /api/sessions/{sid}/agents                                              #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_fresh_session_has_an_empty_snapshot_with_every_key(api):
    response = await api.client.get(f"/api/sessions/{SID}/agents")
    assert response.status_code == 200
    snap = response.json()
    assert set(snap) == SNAPSHOT_KEYS
    assert snap["session_id"] == SID
    assert snap["agents"] == [] and snap["queue"] == []
    assert set(snap["summary"]) == SUMMARY_KEYS
    assert all(snap["summary"][k] == 0 for k in SUMMARY_KEYS - {"oldest_live_started_at"})
    assert snap["summary"]["oldest_live_started_at"] is None
    assert set(snap["capabilities"]) == CAPABILITY_KEYS
    assert snap["capabilities"]["quiet_after_minutes"] == 10
    assert snap["generated_at"].endswith("Z")


@pytest.mark.asyncio
async def test_the_snapshot_lists_agents_as_agent_views(api):
    api.engine.sessions.set_client(SID, _Client())
    rec = await _agent(api.engine, "toolu_A", "a1b2c3d4e5f6a7b8c")
    snap = (await api.client.get(f"/api/sessions/{SID}/agents")).json()
    (view,) = snap["agents"]
    assert set(view) == AGENT_VIEW_KEYS
    assert set(view["controls"]) == CONTROL_KEYS
    assert view["id"] == rec.id and view["id"].startswith("ag_")
    assert (view["kind"], view["status"], view["task_id"]) == ("agent", "running", "a1b2c3d4e5f6a7b8c")
    assert (view["label"], view["agent_type"]) == ("name-a1b2c3d4e5f6a7b8c", "general-purpose")
    assert view["controls"]["stop"] is True
    assert snap["capabilities"]["stop_task"] is True
    assert snap["summary"]["live"] == snap["summary"]["running"] == 1


@pytest.mark.asyncio
async def test_the_snapshot_limit_keeps_every_live_agent(api):
    live = await _agent(api.engine, "toolu_L", "alive0000001")
    for i in range(3):
        rec = await _agent(api.engine, f"toolu_{i}", f"adone000000{i}")
        await api.engine.agents.on_system_event(SID, ev.SystemEvent(
            "task_notification", {"task_id": rec.task_id, "status": "completed"},
        ))
    snap = (await api.client.get(f"/api/sessions/{SID}/agents?limit=2")).json()
    ids = [a["id"] for a in snap["agents"]]
    assert live.id in ids and len(ids) == 2
    assert snap["summary"]["total"] == 4      # the summary counts everything
    many = (await api.client.get(f"/api/sessions/{SID}/agents?limit=10000")).json()
    assert len(many["agents"]) == 4


@pytest.mark.asyncio
async def test_agents_of_an_unknown_session_404(api):
    response = await api.client.get("/api/sessions/s-unknown/agents")
    assert response.status_code == 404
    assert _detail(response)["code"] == "session_not_found"


# --------------------------------------------------------------------------- #
#  GET /api/sessions/{sid}/agents/{agent_id}                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_agent_detail(api):
    rec = await _agent(api.engine, "toolu_A", "a1b2c3d4e5f6a7b8c")
    rec.result_text = "Four files changed."
    body = (await api.client.get(f"/api/sessions/{SID}/agents/{rec.id}")).json()
    assert set(body) == {"agent", "prompt", "result_text"}
    assert set(body["agent"]) == AGENT_VIEW_KEYS
    assert (body["prompt"], body["result_text"]) == ("do the work", "Four files changed.")


@pytest.mark.asyncio
async def test_agent_detail_errors(api):
    missing = await api.client.get(f"/api/sessions/{SID}/agents/ag_000000000000")
    assert missing.status_code == 404
    assert _detail(missing)["code"] == "agent_not_found"

    # A fork shows its parent's spawn cards; their agents live in the parent.
    rec = await _agent(api.engine, "toolu_A", "a1b2c3d4e5f6a7b8c")
    await api.engine.sessions.get_or_create("fork-1", source="web", actor=None)
    other = await api.client.get(f"/api/sessions/fork-1/agents/{rec.id}")
    assert other.status_code == 404
    detail = _detail(other)
    assert detail["code"] == "agent_in_other_session"
    assert detail["owner_session_id"] == SID


@pytest.mark.asyncio
async def test_an_agent_known_only_to_the_database_names_its_session(api, tmp_path):
    rec = await _agent(api.engine, "toolu_A", "a1b2c3d4e5f6a7b8c")
    await api.engine.agents.writer.flush()
    # A restarted gateway: nothing loaded in memory.
    api.engine.agents._sessions.clear()
    await api.engine.sessions.get_or_create("fork-1", source="web", actor=None)
    other = await api.client.get(f"/api/sessions/fork-1/agents/{rec.id}")
    assert _detail(other)["owner_session_id"] == SID
    own = await api.client.get(f"/api/sessions/{SID}/agents/{rec.id}")
    assert own.status_code == 200 and own.json()["agent"]["id"] == rec.id


# --------------------------------------------------------------------------- #
#  GET .../transcript                                                          #
# --------------------------------------------------------------------------- #


async def _transcript_agent(engine: AgentEngine):
    """An agent with one item of each kind (seq 1 = started)."""
    rec = await _agent(engine, "toolu_A", "a1b2c3d4e5f6a7b8c")
    for event in (
        ev.ThinkingDelta(text="The fixture is stale.", parent_tool_use_id="toolu_A"),
        ev.ToolUse(tool_use_id="toolu_B", name="Bash", input={"command": "pytest -q"},
                   parent_tool_use_id="toolu_A"),
        ev.ToolResult(tool_use_id="toolu_B", content="212 passed, 1 failed",
                      parent_tool_use_id="toolu_A"),
        ev.TextDelta(text="One test fails; fixing it.", parent_tool_use_id="toolu_A"),
    ):
        await engine.agents.ingest_sidechain(SID, event)
    return rec


@pytest.mark.asyncio
async def test_transcript_items_follow_the_contract(api):
    rec = await _transcript_agent(api.engine)
    body = (await api.client.get(f"/api/sessions/{SID}/agents/{rec.id}/transcript")).json()
    assert set(body) == {"agent_id", "items", "first_seq", "last_seq", "has_more_before"}
    assert body["agent_id"] == rec.id
    items = body["items"]
    assert [i["seq"] for i in items] == [1, 2, 3, 4, 5]
    assert [i["kind"] for i in items] == ["lifecycle", "thinking", "tool_call", "tool_result", "text"]
    for item in items:
        assert set(item) == ITEM_KEYS[item["kind"]], item
        assert item["at"].endswith("Z")
    assert items[0]["event"] == "started"
    assert items[2]["input"] == {"command": "pytest -q"}
    assert items[3] == {**items[3], "result": "212 passed, 1 failed", "is_error": False,
                        "truncated": False, "full_size": 20}
    assert (body["first_seq"], body["last_seq"], body["has_more_before"]) == (1, 5, False)


@pytest.mark.asyncio
async def test_transcript_cursors_and_the_unflushed_merge(api):
    rec = await _transcript_agent(api.engine)
    await api.engine.agents.writer.flush()          # seq 1-5 in the database
    await api.engine.agents.ingest_sidechain(SID, ev.TextDelta(
        text="Fixed.", parent_tool_use_id="toolu_A",
    ))                                              # seq 6 still queued
    base = f"/api/sessions/{SID}/agents/{rec.id}/transcript"

    tail = (await api.client.get(base)).json()
    assert [i["seq"] for i in tail["items"]] == [1, 2, 3, 4, 5, 6]
    assert tail["items"][-1]["text"] == "Fixed."

    page = (await api.client.get(f"{base}?limit=2")).json()
    assert [i["seq"] for i in page["items"]] == [5, 6]
    assert page["has_more_before"] is True
    earlier = (await api.client.get(f"{base}?before_seq=5&limit=2")).json()
    assert [i["seq"] for i in earlier["items"]] == [3, 4]
    assert earlier["has_more_before"] is True
    later = (await api.client.get(f"{base}?after_seq=4&limit=500")).json()
    assert [i["seq"] for i in later["items"]] == [5, 6]
    empty = (await api.client.get(f"{base}?after_seq=6")).json()
    assert (empty["items"], empty["first_seq"], empty["last_seq"]) == ([], None, None)


@pytest.mark.asyncio
async def test_transcript_errors(api):
    rec = await _transcript_agent(api.engine)
    base = f"/api/sessions/{SID}/agents/{rec.id}/transcript"
    for query in ("?before_seq=3&after_seq=1", "?before_seq=abc", "?after_seq=-1"):
        response = await api.client.get(base + query)
        assert response.status_code == 400, query
        assert _detail(response)["code"] == "bad_cursor"
    missing = await api.client.get(f"/api/sessions/{SID}/agents/ag_000000000000/transcript")
    assert missing.status_code == 404
    assert _detail(missing)["code"] == "agent_not_found"


# --------------------------------------------------------------------------- #
#  POST .../agents/{agent_id}/stop                                             #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_stop_one_agent(api):
    client = _Client()
    api.engine.sessions.set_client(SID, client)
    parent = await _agent(api.engine, "toolu_P", "aparent0001")
    child = await _agent(api.engine, "toolu_C", "achild00001", parent="toolu_P")

    response = await api.client.post(f"/api/sessions/{SID}/agents/{parent.id}/stop", json={"cascade": True})
    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"agent", "stopping"}
    assert set(body["agent"]) == AGENT_VIEW_KEYS
    assert body["agent"]["status"] == "stopping"
    assert body["agent"]["stopped_by_actor_id"] == api.db.system_actor_id
    assert body["stopping"] == [child.id, parent.id]
    assert client.calls == [("stop_task", "achild00001"), ("stop_task", "aparent0001")]

    # Already ended: 200, nothing stopping.
    await api.engine.agents.on_system_event(SID, ev.SystemEvent(
        "task_updated", {"task_id": "aparent0001", "patch": {"status": "killed"}},
    ))
    again = await api.client.post(f"/api/sessions/{SID}/agents/{parent.id}/stop")
    assert again.status_code == 200
    assert again.json()["stopping"] == [] and again.json()["agent"]["status"] == "stopped"


@pytest.mark.asyncio
async def test_stop_without_cascade(api):
    client = _Client()
    api.engine.sessions.set_client(SID, client)
    parent = await _agent(api.engine, "toolu_P", "aparent0001")
    await _agent(api.engine, "toolu_C", "achild00001", parent="toolu_P")
    response = await api.client.post(
        f"/api/sessions/{SID}/agents/{parent.id}/stop", json={"cascade": False},
    )
    assert response.json()["stopping"] == [parent.id]


@pytest.mark.asyncio
async def test_stop_agent_errors(api):
    missing = await api.client.post(f"/api/sessions/{SID}/agents/ag_000000000000/stop")
    assert missing.status_code == 404
    assert _detail(missing)["code"] == "agent_not_found"

    rec = await _agent(api.engine, "toolu_A", "a1b2c3d4e5f6a7b8c")
    no_runtime = await api.client.post(f"/api/sessions/{SID}/agents/{rec.id}/stop")
    assert no_runtime.status_code == 409
    assert _detail(no_runtime)["code"] == "no_runtime"
    # ... and the record was reconciled.
    view = (await api.client.get(f"/api/sessions/{SID}/agents/{rec.id}")).json()["agent"]
    assert (view["status"], view["end_reason"]) == ("interrupted", "process_ended")

    api.engine.sessions.set_client(SID, _Client(supports=False))
    rec2 = await _agent(api.engine, "toolu_B", "acodex000001")
    unsupported = await api.client.post(f"/api/sessions/{SID}/agents/{rec2.id}/stop")
    assert unsupported.status_code == 501
    assert _detail(unsupported)["code"] == "unsupported"


# --------------------------------------------------------------------------- #
#  POST /api/sessions/{sid}/stop                                               #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_stop_scopes_return_their_results(api):
    client = _Client()
    api.engine.sessions.set_client(SID, client)
    rec = await _agent(api.engine, "toolu_A", "a1b2c3d4e5f6a7b8c")

    turn = await api.client.post(f"/api/sessions/{SID}/stop", json={"scope": "turn"})
    assert turn.status_code == 200
    assert turn.json() == {"scope": "turn", "turn": "not_running", "would_end": [], "shell_count": 0}
    default = await api.client.post(f"/api/sessions/{SID}/stop")
    assert default.json()["scope"] == "turn"

    stop_all = (await api.client.post(
        f"/api/sessions/{SID}/stop", json={"scope": "all", "include_shells": True},
    )).json()
    assert set(stop_all) == {"scope", "turn", "stopping", "unsupported"}
    assert (stop_all["scope"], stop_all["turn"]) == ("all", "not_running")
    assert stop_all["stopping"] == [rec.id] and stop_all["unsupported"] == []

    force = (await api.client.post(f"/api/sessions/{SID}/stop", json={"scope": "force"})).json()
    assert set(force) == {"scope", "turn", "ended"}
    assert (force["scope"], force["turn"]) == ("force", "not_running")
    assert force["ended"] == [rec.id]


@pytest.mark.asyncio
async def test_a_turn_stop_that_needs_confirmation_lists_what_would_end(api):
    class _Degraded(_Client):
        @property
        def interrupt_spares_background(self) -> bool:
            return False

    client = _Degraded()
    api.engine.sessions.set_client(SID, client)

    async def _get(sid, source, model, **_kw):
        return client

    api.engine._get_or_create_client = _get
    rec = await _agent(api.engine, "toolu_A", "a1b2c3d4e5f6a7b8c")
    run = asyncio.create_task(api.engine.run(SID, "hello", actor=None))
    await asyncio.wait_for(client.turn_started.wait(), timeout=5)
    try:
        body = (await api.client.post(f"/api/sessions/{SID}/stop", json={"scope": "turn"})).json()
        assert set(body) == {"scope", "turn", "would_end", "shell_count"}
        assert body["turn"] == "needs_confirmation"
        (brief,) = body["would_end"]
        assert set(brief) == BRIEF_KEYS and brief["id"] == rec.id
        assert client.calls == []
        confirmed = (await api.client.post(
            f"/api/sessions/{SID}/stop", json={"scope": "turn", "confirm_kill_background": True},
        )).json()
        assert confirmed["turn"] == "stopped"
    finally:
        if not run.done():
            run.cancel()
        with contextlib.suppress(BaseException):
            await run


@pytest.mark.asyncio
async def test_stop_errors(api):
    bad = await api.client.post(f"/api/sessions/{SID}/stop", json={"scope": "everything"})
    assert bad.status_code == 422
    assert _detail(bad)["code"] == "bad_scope"
    missing = await api.client.post("/api/sessions/s-unknown/stop", json={"scope": "turn"})
    assert missing.status_code == 404
    assert _detail(missing)["code"] == "session_not_found"


# --------------------------------------------------------------------------- #
#  The queue                                                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_queue_routes(api):
    client = _Client()

    async def _get(sid, source, model, **_kw):
        api.engine.sessions.set_client(sid, client)
        return client

    api.engine._get_or_create_client = _get
    me = api.db.system_actor
    first = asyncio.create_task(api.engine.run(SID, "first", actor=me))
    await asyncio.wait_for(client.turn_started.wait(), timeout=5)
    mine = asyncio.create_task(api.engine.run(SID, "queued text", actor=me, client_msg_id="m2"))
    relay = asyncio.create_task(api.engine.run(SID, "from a channel", actor=None, origin="telegram"))
    try:
        for _ in range(100):
            if len(api.engine.queue_view(SID)) == 2:
                break
            await asyncio.sleep(0.01)
        body = (await api.client.get(f"/api/sessions/{SID}/queue")).json()
        assert set(body) == {"items"}
        assert [set(i) for i in body["items"]] == [QUEUE_KEYS, QUEUE_KEYS]
        own, theirs = body["items"]
        assert (own["preview"], own["client_msg_id"], own["actor_id"]) == (
            "queued text", "m2", me.actor_id,
        )
        assert theirs["source"] == "telegram"

        not_owner = await api.client.delete(f"/api/sessions/{SID}/queue/{theirs['queue_id']}")
        assert not_owner.status_code == 403
        assert _detail(not_owner)["code"] == "not_owner"
        missing = await api.client.delete(f"/api/sessions/{SID}/queue/q_00000000")
        assert missing.status_code == 404
        assert _detail(missing)["code"] == "queue_entry_not_found"

        cancelled = await api.client.delete(f"/api/sessions/{SID}/queue/{own['queue_id']}")
        assert cancelled.status_code == 200
        assert cancelled.json() == {"cancelled": True, "content": "queued text"}
        assert await asyncio.wait_for(mine, timeout=5) == ""

        # The relay gets the lock once the first turn ends: too late to cancel.
        client._turn_end.set()
        await asyncio.wait_for(first, timeout=5)
        for _ in range(100):
            if api.engine.sessions.turn_task(SID) is relay:
                break
            await asyncio.sleep(0.01)
        started = await api.client.delete(f"/api/sessions/{SID}/queue/{theirs['queue_id']}")
        assert started.status_code == 409
        assert _detail(started)["code"] == "already_started"
        assert (await api.client.get("/api/sessions/s-unknown/queue")).status_code == 404
    finally:
        for task in (first, mine, relay):
            if client._turn_end is not None:
                client._turn_end.set()
            if not task.done():
                task.cancel()
            with contextlib.suppress(BaseException):
                await task


# --------------------------------------------------------------------------- #
#  Changed session endpoints                                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_session_lists_and_status_carry_background_count(api):
    await _agent(api.engine, "toolu_A", "a1b2c3d4e5f6a7b8c")
    await _agent(api.engine, "toolu_B", "a2b2c3d4e5f6a7b8c")
    sessions = (await api.client.get("/api/sessions")).json()["sessions"]
    row = next(s for s in sessions if s["id"] == SID)
    assert (row["has_background_tasks"], row["background_count"]) == (True, 2)
    status = (await api.client.get(f"/api/sessions/{SID}/status")).json()
    assert status["background_count"] == 2


@pytest.mark.asyncio
async def test_decorate_falls_back_without_a_count(api):
    from nerve.gateway.routes.sessions import _decorate

    deps = SimpleNamespace(
        engine=SimpleNamespace(
            sessions=SimpleNamespace(get_running_ids=lambda: set()),
            has_live_background_tasks=lambda sid: sid == "busy",
        ),
        db=api.db,
    )
    rows = await _decorate(deps, [{"id": "busy"}, {"id": "idle"}])
    assert [(r["has_background_tasks"], r["background_count"]) for r in rows] == [
        (True, 1), (False, 0),
    ]


@pytest.mark.asyncio
async def test_api_chat_passes_client_msg_id_and_origin(api, monkeypatch):
    seen: list[dict] = []

    async def _run(**kwargs):
        seen.append(kwargs)
        return "ok"

    monkeypatch.setattr(api.engine, "run", _run)
    response = await api.client.post("/api/chat", json={
        "message": "hi", "session_id": SID, "client_msg_id": "m-api-1",
    })
    assert response.json() == {"response": "ok", "session_id": SID}
    assert (seen[0]["client_msg_id"], seen[0]["origin"]) == ("m-api-1", "api")
    await api.client.post("/api/chat", json={"message": "hi", "session_id": SID})
    assert seen[1]["client_msg_id"] is None


@pytest.mark.asyncio
async def test_the_routes_are_registered_and_need_auth(tmp_path, open_identity_db, wire_identity_store, monkeypatch):
    """Through the production route assembler, with a real session token."""
    from nerve.config import set_config
    from nerve.gateway.auth import create_session_token, pin_jwt_secret
    from nerve.gateway.routes import _deps as deps_module, register_all_routes

    secret = "test-secret-for-agent-routes-0123456789abcdef"
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
        "auth": {"jwt_secret": secret},
    })
    config.workspace.mkdir(parents=True)
    set_config(config)
    pin_jwt_secret(secret)
    database, identity = await open_identity_db(tmp_path / "nerve.db")
    try:
        wire_identity_store(database)
        engine = AgentEngine(config, database)
        monkeypatch.setattr(deps_module, "_deps", deps_module.RouteDeps(engine=engine, db=database))
        await engine.sessions.get_or_create(SID, source="web", actor=None)
        app = FastAPI()
        app.include_router(register_all_routes())
        token = create_session_token(secret, identity.owner_account_id)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
        ) as client:
            anonymous = await client.get(f"/api/sessions/{SID}/agents")
            assert anonymous.status_code == 401
            headers = {"Authorization": f"Bearer {token}"}
            snap = await client.get(f"/api/sessions/{SID}/agents", headers=headers)
            assert snap.status_code == 200 and set(snap.json()) == SNAPSHOT_KEYS
            queue = await client.get(f"/api/sessions/{SID}/queue", headers=headers)
            assert queue.json() == {"items": []}
            stop = await client.post(f"/api/sessions/{SID}/stop", headers=headers, json={})
            assert stop.json() == {"scope": "turn", "turn": "not_running", "would_end": [], "shell_count": 0}
    finally:
        await database.close()
        set_config(NerveConfig())
