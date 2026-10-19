"""Agents die with their runtime.

Every client teardown goes through ``SessionManager.remove_client(reason=)``,
which reports it to the agent registry: the session's live agents become
``interrupted`` with the reason's end reason. Shutdown and boot reconcile
what bypasses it; delete forgets the records; archive refuses (or, forced,
interrupts) sessions whose agents still run.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from nerve.agent.backends import events as ev
from nerve.agent.backends.base import BackendCapabilities, TransportDiedError
from nerve.agent.engine import AgentEngine
from nerve.agent.sessions import SessionBusyError
from nerve.config import NerveConfig

SID = "s-1234"


class _Client:
    native_session_id = "native-1"

    def __init__(self, model: str = "model-a", *, alive: bool = True) -> None:
        self.model = model
        self._alive = alive
        self.disconnected = False

    def is_alive(self) -> bool:
        return self._alive

    async def interrupt(self, *, cancel_queued: bool = False) -> None:
        pass

    async def disconnect(self, timeout: float = 5.0) -> None:
        self.disconnected = True

    def try_receive_idle_events(self):
        return None

    def buffer_used(self) -> int:
        return 0


class _StubBackend:
    name = "claude"
    capabilities = BackendCapabilities(
        cost_is_cumulative=False, supports_idle_stream=False,
        supports_cache_ttl=False, interactive_builtins=False,
        supports_task_stop=True, supports_agent_transcripts=True,
    )

    def __init__(self) -> None:
        self.built: list[_Client] = []

    def default_model(self, source):
        return "model-a"

    def excluded_tools(self):
        return set()

    def validate_resume_target(self, native_id, cwd):
        return True

    async def create_client(self, spec):
        client = _Client(spec.model or "model-a")
        self.built.append(client)
        return client


def _engine(db, tmp_path) -> AgentEngine:
    workspace = tmp_path / "ws"
    workspace.mkdir(parents=True, exist_ok=True)
    cfg = NerveConfig.from_dict({"workspace": str(workspace), "agent": {"backend": "claude"}})
    return AgentEngine(cfg, db)


async def _live_agent(engine: AgentEngine, task_id: str = "a1b2c3d4e5f6a7b8c", sid: str = SID):
    await engine.agents.on_system_event(sid, ev.SystemEvent("task_started", {
        "task_id": task_id, "task_type": "local_agent", "description": "build-api",
        "is_backgrounded": True,
    }))
    return next(
        r for r in engine.agents._sessions[sid].records.values() if r.task_id == task_id
    )


# --------------------------------------------------------------------------- #
#  The remove_client choke point                                               #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
@pytest.mark.parametrize("reason,end_reason", [
    ("process_exit", "process_ended"),
    ("process_ended", "process_ended"),
    ("hung_runtime", "process_ended"),
    ("idle_reap", "process_ended"),
    ("deleted", "process_ended"),
    ("model_switch", "model_switch"),
    ("force_stop", "force_stop"),
    ("archived", "archived"),
    ("gateway_restart", "gateway_restart"),
])
async def test_each_remove_client_reason_maps_to_its_end_reason(
    db, tmp_path, reason, end_reason,
):
    engine = _engine(db, tmp_path)
    await engine.sessions.get_or_create(SID, source="web", actor=None)
    engine.sessions.set_client(SID, _Client())
    rec = await _live_agent(engine)

    engine.sessions.remove_client(SID, reason=reason)

    assert (rec.status, rec.end_reason) == ("interrupted", end_reason)
    assert rec.ended_at is not None
    assert engine.agents.has_live(SID) is False
    items = engine.agents.writer.pending_items(rec.id)
    assert items[-1]["kind"] == "lifecycle"
    assert (items[-1]["event"], items[-1]["reason"]) == ("interrupted", end_reason)


@pytest.mark.asyncio
async def test_removing_no_client_reconciles_nothing(db, tmp_path):
    engine = _engine(db, tmp_path)
    rec = await _live_agent(engine)
    assert engine.sessions.remove_client(SID, reason="process_exit") is None
    assert rec.status == "running"


@pytest.mark.asyncio
async def test_a_failing_hook_never_breaks_teardown(db, tmp_path):
    engine = _engine(db, tmp_path)
    client = _Client()
    engine.sessions.set_client(SID, client)

    def _boom(sid, reason):
        raise RuntimeError("hook failed")

    engine.sessions.on_client_removed = _boom
    assert engine.sessions.remove_client(SID) is client


@pytest.mark.asyncio
async def test_discard_client_passes_its_reason(db, tmp_path):
    engine = _engine(db, tmp_path)
    await engine.sessions.get_or_create(SID, source="web", actor=None)
    engine.sessions.set_client(SID, _Client())
    rec = await _live_agent(engine)
    await engine._discard_client(SID, background_memorize=True, reason="hung_runtime")
    assert (rec.status, rec.end_reason) == ("interrupted", "process_ended")
    # The default reason is a runtime that ended.
    engine.sessions.set_client(SID, _Client())
    rec2 = await _live_agent(engine, "a2b2c3d4e5f6a7b8c")
    await engine._discard_client(SID, background_memorize=True)
    assert (rec2.status, rec2.end_reason) == ("interrupted", "process_ended")


# --------------------------------------------------------------------------- #
#  Engine call sites                                                           #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_dead_client_is_recreated_and_its_agents_interrupted(db, tmp_path):
    engine = _engine(db, tmp_path)
    backend = engine._backends["claude"] = _StubBackend()
    await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)
    first = await engine._get_or_create_client(SID, "web", None)
    rec = await _live_agent(engine)
    first._alive = False

    second = await engine._get_or_create_client(SID, "web", None)

    assert second is not first and len(backend.built) == 2
    assert (rec.status, rec.end_reason) == ("interrupted", "process_ended")


@pytest.mark.asyncio
async def test_a_model_switch_waits_while_agents_run(db, tmp_path):
    engine = _engine(db, tmp_path)
    backend = engine._backends["claude"] = _StubBackend()
    await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)
    first = await engine._get_or_create_client(SID, "web", "model-a")
    rec = await _live_agent(engine)

    # Agents run: the bound client (and its model) is kept.
    kept = await engine._get_or_create_client(SID, "web", "model-b")
    assert kept is first and len(backend.built) == 1
    assert rec.status == "running"

    # Once they ended, the switch recreates the runtime.
    await engine.agents.on_system_event(SID, ev.SystemEvent(
        "task_notification", {"task_id": rec.task_id, "status": "completed"},
    ))
    switched = await engine._get_or_create_client(SID, "web", "model-b")
    assert switched is not first and switched.model == "model-b"


@pytest.mark.asyncio
async def test_a_model_switch_without_agents_interrupts_nothing_live(db, tmp_path):
    """A forced switch path: the registry hook reports model_switch."""
    engine = _engine(db, tmp_path)
    engine._backends["claude"] = _StubBackend()
    await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)
    await engine._get_or_create_client(SID, "web", "model-a")
    # An ambient housekeeping task is never counted as live, so it does not
    # hold the switch back, and it ends with the runtime.
    await engine.agents.on_system_event(SID, ev.SystemEvent("task_started", {
        "task_id": "dream0001", "task_type": "dream", "description": "dream",
    }))
    ambient = next(
        r for r in engine.agents._sessions[SID].records.values() if r.task_id == "dream0001"
    )
    assert ambient.ambient and not engine.agents.has_live(SID)
    await engine._get_or_create_client(SID, "web", "model-b")
    assert (ambient.status, ambient.end_reason) == ("interrupted", "model_switch")


class _CrashingClient(_Client):
    """A runtime that dies before any content (the crash-retry path)."""

    async def start_turn(self, turn) -> None:
        pass

    async def receive_turn(self):
        raise TransportDiedError("runtime died")
        yield  # pragma: no cover


class _OkClient(_Client):
    async def start_turn(self, turn) -> None:
        pass

    async def receive_turn(self):
        yield ev.TurnCompleted(native_session_id="native-2")


@pytest.mark.asyncio
async def test_a_crash_retry_interrupts_the_dead_runtimes_agents(db, tmp_path):
    engine = _engine(db, tmp_path)
    await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)
    clients = [_CrashingClient(), _OkClient()]
    served: list[_Client] = []

    async def _get(sid, source, model, **_kw):
        client = clients[min(len(served), 1)]
        served.append(client)
        engine.sessions.set_client(sid, client)
        return client

    engine._get_or_create_client = _get
    rec = await _live_agent(engine)
    await engine.run(SID, "hello", source="web", actor=None)
    assert len(served) == 2
    assert (rec.status, rec.end_reason) == ("interrupted", "process_ended")


@pytest.mark.asyncio
async def test_a_run_error_interrupts_the_runtimes_agents(db, tmp_path):
    engine = _engine(db, tmp_path)
    await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)

    class _Broken(_Client):
        async def start_turn(self, turn) -> None:
            raise RuntimeError("boom")

    client = _Broken()

    async def _get(sid, source, model, **_kw):
        engine.sessions.set_client(sid, client)
        return client

    engine._get_or_create_client = _get
    rec = await _live_agent(engine)
    out = await engine.run(SID, "hello", source="web", actor=None)
    assert out.startswith("Agent error")
    assert (rec.status, rec.end_reason) == ("interrupted", "process_ended")
    assert client.disconnected


@pytest.mark.asyncio
async def test_the_idle_sweep_reaps_only_sessions_without_agents(db, tmp_path):
    engine = _engine(db, tmp_path)
    engine.config.sessions.client_idle_timeout_minutes = 1
    for sid in ("busy", "free"):
        await engine.sessions.get_or_create(sid, source="web", actor=None)
        engine.sessions.set_client(sid, _Client())
    rec = await _live_agent(engine, sid="busy")
    reasons: list[tuple[str, str]] = []
    hook = engine.sessions.on_client_removed

    def _spy(sid, reason):
        reasons.append((sid, reason))
        hook(sid, reason)

    engine.sessions.on_client_removed = _spy
    assert await engine.run_idle_client_sweep() == 1
    assert reasons == [("free", "idle_reap")]
    assert rec.status == "running"


# --------------------------------------------------------------------------- #
#  Shutdown and boot                                                           #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_shutdown_marks_live_agents_gateway_restart(db, tmp_path):
    engine = _engine(db, tmp_path)
    await engine.sessions.get_or_create(SID, source="web", actor=None)
    client = _Client()
    engine.sessions.set_client(SID, client)
    rec = await _live_agent(engine)

    await engine.shutdown()

    assert (rec.status, rec.end_reason) == ("interrupted", "gateway_restart")
    assert client.disconnected
    # The final flush wrote it before the database closes.
    (row,) = await db.list_session_agents(SID)
    assert (row["status"], row["end_reason"]) == ("interrupted", "gateway_restart")


@pytest.mark.asyncio
async def test_boot_settles_rows_a_crash_left_live(db, tmp_path):
    engine = _engine(db, tmp_path)
    await engine.sessions.get_or_create(SID, source="web", actor=None)
    rec = await _live_agent(engine)
    await engine.agents.writer.flush()
    assert rec.status == "running"

    # A new process (the old one crashed without shutdown).
    fresh = _engine(db, tmp_path)
    await fresh.agents.start()
    try:
        (row,) = await db.list_session_agents(SID)
        assert (row["status"], row["end_reason"]) == ("interrupted", "gateway_restart")
        snap = await fresh.agent_snapshot(SID)
        assert snap["agents"][0]["status"] == "interrupted"
        assert snap["agents"][0]["attention"] == "interrupted"
    finally:
        await fresh.agents.close()


# --------------------------------------------------------------------------- #
#  Delete, archive, cleanup                                                    #
# --------------------------------------------------------------------------- #


def _app(engine: AgentEngine, db, bypass_auth) -> TestClient:
    from nerve.gateway.auth import require_auth
    from nerve.gateway.routes._deps import init_deps
    from nerve.gateway.routes.sessions import router as sessions_router

    init_deps(engine=engine, db=db)
    app = FastAPI()
    app.include_router(sessions_router)
    bypass_auth(app)
    # The deletes and archives record who made them: an actor that exists.
    app.dependency_overrides[require_auth] = lambda: db.system_actor
    return TestClient(app)


@pytest.mark.asyncio
async def test_delete_forgets_the_records(db, tmp_path, bypass_auth):
    engine = _engine(db, tmp_path)
    await engine.sessions.get_or_create(SID, source="web", actor=None)
    engine.sessions.set_client(SID, _Client())
    rec = await _live_agent(engine)
    await engine.agents.writer.flush()
    assert len(await db.list_session_agents(SID)) == 1
    # Queue a change that is not written yet.
    engine.agents.writer.mark_dirty(rec)

    client = _app(engine, db, bypass_auth)
    assert client.delete(f"/api/sessions/{SID}").json() == {"deleted": True}

    assert engine.agents.get(SID, rec.id) is None
    assert not engine.agents.writer.has_pending(SID)
    await engine.agents.writer.flush()   # no late write resurrects a row
    assert await db.list_session_agents(SID) == []
    assert await db.get_session_agent(rec.id) is None


@pytest.mark.asyncio
async def test_archive_skips_a_session_with_live_agents_unless_forced(
    db, tmp_path, bypass_auth,
):
    engine = _engine(db, tmp_path)
    await engine.sessions.get_or_create(SID, source="web", actor=None)
    client_obj = _Client()
    engine.sessions.set_client(SID, client_obj)
    rec = await _live_agent(engine)
    client = _app(engine, db, bypass_auth)

    body = client.post(f"/api/sessions/{SID}/archive").json()
    assert body == {
        "archived": False, "archived_ids": [],
        "skipped": [{"id": SID, "reason": "background agents running"}],
    }
    assert (await db.get_session(SID))["status"] != "archived"
    assert rec.status == "running"

    body = client.post(f"/api/sessions/{SID}/archive?force=true").json()
    assert body["archived_ids"] == [SID] and body["skipped"] == []
    assert (await db.get_session(SID))["status"] == "archived"
    assert (rec.status, rec.end_reason) == ("interrupted", "archived")
    assert client_obj.disconnected


@pytest.mark.asyncio
async def test_archive_session_raises_busy_without_force(db, tmp_path):
    engine = _engine(db, tmp_path)
    await engine.sessions.get_or_create(SID, source="web", actor=None)
    rec = await _live_agent(engine)
    with pytest.raises(SessionBusyError):
        await engine.sessions.archive_session(SID)
    # Forced, with no runtime left: the stale live record is settled too.
    assert await engine.sessions.archive_session(SID, force=True) is True
    assert (rec.status, rec.end_reason) == ("interrupted", "archived")


@pytest.mark.asyncio
async def test_cleanup_skips_busy_sessions(db, tmp_path):
    engine = _engine(db, tmp_path)
    for sid in ("stale-busy", "stale-free"):
        await engine.sessions.get_or_create(sid, source="cron", actor=None)
        await db.update_session_fields(sid, {"status": "idle"})
        await db.db.execute(
            "UPDATE sessions SET updated_at = '2020-01-01T00:00:00' WHERE id = ?", (sid,),
        )
    await db.db.commit()
    rec = await _live_agent(engine, sid="stale-busy")

    stats = await engine.sessions.run_cleanup(archive_after_days=30, max_sessions=1000)

    assert stats["archived_stale"] == 1
    assert (await db.get_session("stale-free"))["status"] == "archived"
    assert (await db.get_session("stale-busy"))["status"] == "idle"
    assert rec.status == "running"


@pytest.mark.asyncio
async def test_patch_model_is_refused_while_agents_run(db, tmp_path, bypass_auth):
    engine = _engine(db, tmp_path)
    await engine.sessions.get_or_create(SID, source="web", backend="claude", model="model-a", actor=None)
    rec = await _live_agent(engine)
    client = _app(engine, db, bypass_auth)

    response = client.patch(f"/api/sessions/{SID}", json={"model": "model-b"})
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["code"] == "live_background_agents"
    assert detail["live"] == 1 and detail["message"]
    # The same model is no switch.
    assert client.patch(f"/api/sessions/{SID}", json={"model": "model-a"}).status_code == 200

    await engine.agents.on_system_event(SID, ev.SystemEvent(
        "task_notification", {"task_id": rec.task_id, "status": "completed"},
    ))
    assert client.patch(f"/api/sessions/{SID}", json={"model": "model-b"}).status_code == 200
