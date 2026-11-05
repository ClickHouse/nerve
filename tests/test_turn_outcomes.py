"""How a turn ended reaches the engine's callers, cron logs and workflow runs.

A turn can end completed, failed (max turns, an API error, a crash) or
interrupted (an abort, a stop). That outcome is kept apart from session
health: a failed turn leaves the session resumable.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve.agent.backends.events import SystemEvent, TextDelta, TurnCompleted
from nerve.agent.engine import AgentEngine, TurnOutcome, TurnResult, TurnWatch
from nerve.agent.streaming import broadcaster
from nerve.config import NerveConfig
from nerve.cron.jobs import CronJob
from nerve.cron.service import CronService
from nerve.workflows.service import ENGINE_CLAUDE, WorkflowRunService

from tests.actor_rows import ensure_system_principal
from tests.test_engine import _ScriptedClient
from tests.test_workflow_runs import _drain, _make_config
from tests.test_workflow_runs import _make_engine as _make_mock_engine

pytestmark = pytest.mark.asyncio

MAX_TURNS = {
    "subtype": "error_max_turns", "is_error": True,
    "terminal_reason": "max_turns", "num_turns": 50,
}
ABORTED = {"is_error": True, "terminal_reason": "aborted_streaming"}
MAX_TURNS_ERROR = "max turns (50) exhausted"


@pytest_asyncio.fixture
async def db(db):  # noqa: F811 - the conftest database, with an identity
    await ensure_system_principal(db)
    return db


# --------------------------------------------------------------------------- #
#  Harness                                                                     #
# --------------------------------------------------------------------------- #


async def _engine(db, tmp_path, scripts, on_client=None):
    """A real AgentEngine that serves one _ScriptedClient per script, in order.

    Unlike ``tests.test_engine._engine_with_scripted_clients`` no session is
    created up front: cron and workflow runs create their own.
    """
    workspace = tmp_path / "ws"
    workspace.mkdir(parents=True, exist_ok=True)
    cfg = NerveConfig.from_dict({
        "workspace": str(workspace), "agent": {"backend": "claude"},
    })
    engine = AgentEngine(cfg, db)
    made: list[_ScriptedClient] = []

    async def _fake_get_or_create(sid, source, model, **kw):
        script = scripts[min(len(made), len(scripts) - 1)]
        client = _ScriptedClient(script, f"c{len(made)}")
        made.append(client)
        engine.sessions.set_client(sid, client)
        if on_client is not None:
            on_client(sid)
        return client

    engine._get_or_create_client = _fake_get_or_create
    return engine, made


async def _session(engine: AgentEngine, sid: str) -> None:
    await engine.sessions.get_or_create(
        sid, title="t", source="web", backend="claude", actor=None,
    )


async def _turn(engine: AgentEngine, sid: str, text: str = "hello") -> TurnResult:
    return await engine._run_turn(sid, text, source="web", channel="web", actor=None)


async def _last_assistant(db, sid: str) -> dict:
    rows = await db.get_messages(sid)
    return [r for r in rows if r["role"] == "assistant"][-1]


def _blocks(row: dict) -> list[dict]:
    blocks = row.get("blocks") or []
    return json.loads(blocks) if isinstance(blocks, str) else blocks


def _engine_warnings(caplog) -> list[str]:
    return [
        r.getMessage() for r in caplog.records
        if r.name == "nerve.agent.engine" and r.levelno == logging.WARNING
    ]


async def _token_seen(sid: str) -> asyncio.Event:
    """An event set by the first streamed token of ``sid``."""
    seen = asyncio.Event()

    async def _cb(_sid, msg):
        if msg.get("type") == "token":
            seen.set()

    await broadcaster.register(sid, "turn-outcome-test", _cb)
    return seen


class _IdleClient:
    """A live client between runs: one buffered batch, then ``then``.

    ``then``: "none" (stream ended), "timeout" (runtime silent) or "hang".
    """

    def __init__(self, batch: list, then: str = "none"):
        self._batches = [batch]
        self._then = then

    def try_receive_idle_events(self):
        return self._batches.pop(0) if self._batches else None

    async def receive_idle_events(self, timeout):
        if self._then == "timeout":
            raise asyncio.TimeoutError
        if self._then == "hang":
            await asyncio.Event().wait()
        return None


def _auto_batch(*tail) -> list:
    return [SystemEvent("init", {}), TextDelta("x"), *tail]


async def _drain_auto(engine: AgentEngine, sid: str, client) -> int:
    return await engine._drain_pending_messages(
        sid, client, "workflow", None, manage_framing=True,
    )


def _fail_assistant_write(engine: AgentEngine, *, hang: asyncio.Event | None = None):
    """Make the next assistant-message write raise (or hang, setting ``hang``)."""
    real_add = engine.sessions.add_message
    state = {"armed": True}

    async def _add(session_id, role, *args, **kwargs):
        if role == "assistant" and state["armed"]:
            state["armed"] = False
            if hang is not None:
                hang.set()
                await asyncio.Event().wait()
            raise RuntimeError("disk full")
        return await real_add(session_id, role, *args, **kwargs)

    engine.sessions.add_message = _add


# --------------------------------------------------------------------------- #
#  Engine                                                                      #
# --------------------------------------------------------------------------- #


async def test_failed_turn_is_reported_and_session_stays_resumable(db, tmp_path, caplog):
    engine, made = await _engine(db, tmp_path, [
        [("text", "partial"), ("result", "sdk-1", MAX_TURNS)],
        [("text", "fine"), ("result", "sdk-1")],
    ])
    await _session(engine, "e1")
    caplog.set_level(logging.WARNING, logger="nerve.agent.engine")

    with engine.watch_turns("e1") as watch:
        result = await _turn(engine, "e1")

    failed = TurnOutcome("failed", MAX_TURNS_ERROR)
    assert result.outcome == failed
    assert watch.outcome == failed
    note = f"⚠️ Turn failed: {MAX_TURNS_ERROR}"
    assert note in result.text
    assert note in (await _last_assistant(db, "e1"))["content"]
    warnings = _engine_warnings(caplog)
    assert len(warnings) == 1 and MAX_TURNS_ERROR in warnings[0]

    row = await db.get_session("e1")
    assert row["status"] == "active"
    assert row["sdk_session_id"] == "sdk-1"
    assert engine.sessions.get_client("e1") is made[0]
    assert (await _turn(engine, "e1", "again")).outcome == TurnOutcome()


async def test_interrupted_turn_gets_an_inline_notice(db, tmp_path, caplog):
    engine, _ = await _engine(db, tmp_path, [
        [("text", "partial"), ("result", "sdk-2", ABORTED)],
    ])
    await _session(engine, "e2")
    caplog.set_level(logging.WARNING, logger="nerve.agent.engine")

    result = await _turn(engine, "e2")

    assert result.outcome == TurnOutcome("interrupted", "aborted streaming")
    note = "⚠️ Turn interrupted: aborted streaming"
    assert note in result.text
    assert note in (await _last_assistant(db, "e2"))["content"]
    warnings = _engine_warnings(caplog)
    assert len(warnings) == 1 and "aborted streaming" in warnings[0]
    assert (await db.get_session("e2"))["status"] == "active"


async def test_completed_turn_has_no_notice(db, tmp_path, caplog):
    engine, _ = await _engine(db, tmp_path, [[("text", "all good"), ("result", "sdk-3")]])
    await _session(engine, "e3")
    caplog.set_level(logging.WARNING, logger="nerve.agent.engine")

    result = await _turn(engine, "e3")
    text = await engine.run("e3", "hello", source="web", channel="web", actor=None)

    assert result.outcome == TurnOutcome("completed", None)
    assert "⚠️" not in result.text
    assert isinstance(text, str) and text == result.text
    assert _engine_warnings(caplog) == []


async def test_crash_after_content_fails_the_turn_not_the_session(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[("text", "half an answer"), ("raise",)]])
    await _session(engine, "e4")

    result = await _turn(engine, "e4")

    assert result.outcome.status == "failed"
    assert result.outcome.error.startswith("Agent error:")
    # The crash path restores the session for resume, so its status cannot
    # carry the failure.
    assert (await db.get_session("e4"))["status"] == "active"


async def test_hard_cancel_interrupts_the_turn(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[("text", "x"), ("hang",)]])
    await _session(engine, "e5")
    seen = await _token_seen("e5")
    try:
        with engine.watch_turns("e5") as watch:
            task = asyncio.create_task(_turn(engine, "e5"))
            await asyncio.wait_for(seen.wait(), 5)
            task.cancel()
            result = await task
    finally:
        await broadcaster.unregister("e5", "turn-outcome-test")

    stopped = TurnOutcome("interrupted", "stopped by user")
    assert result.outcome == stopped
    assert watch.outcome == stopped
    assert result.text == "x\n\n[Stopped by user]"


async def test_stop_before_the_turn_starts(db, tmp_path):
    engine: AgentEngine | None = None

    def _stop(sid):
        engine.sessions.request_stop(sid)

    engine, _ = await _engine(db, tmp_path, [[("text", "never"), ("result", "s")]], on_client=_stop)
    await _session(engine, "e6")

    result = await _turn(engine, "e6")

    assert result == TurnResult(
        "", TurnOutcome("interrupted", "stopped before the turn started"),
    )


async def test_autonomous_turn_outcome_reaches_the_watch(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[]])
    await _session(engine, "e7")
    client = _IdleClient(_auto_batch(
        TurnCompleted(status="failed", error="API error (HTTP 529)"),
    ))

    with engine.watch_turns("e7") as watch:
        assert await _drain_auto(engine, "e7", client) == 1

    assert watch.outcome == TurnOutcome("failed", "API error (HTTP 529)")
    row = await _last_assistant(db, "e7")
    assert _blocks(row)[0] == {"type": "auto"}
    assert "⚠️ Turn failed: API error (HTTP 529)" in row["content"]


async def test_watches_are_scoped_and_cleaned_up(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [
        [("text", "b"), ("result", "sdk-b")],
        [("text", "a"), ("result", "sdk-a", MAX_TURNS)],
        [("text", "a2"), ("result", "sdk-a")],
    ])
    await _session(engine, "A")
    await _session(engine, "B")

    with engine.watch_turns("A") as other:
        await _turn(engine, "B")
    assert other.outcome is None

    with engine.watch_turns("A") as outer:
        with engine.watch_turns("A") as inner:
            await _turn(engine, "A")
        await _turn(engine, "A")
    assert inner.outcome == TurnOutcome("failed", MAX_TURNS_ERROR)
    assert outer.outcome == TurnOutcome()
    assert engine._turn_watches == {}

    with pytest.raises(RuntimeError):
        with engine.watch_turns("A"):
            raise RuntimeError("caller failed")
    assert engine._turn_watches == {}


async def test_unpersisted_autonomous_turn_is_failed(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[]])
    await _session(engine, "e9a")
    _fail_assistant_write(engine)
    client = _IdleClient(_auto_batch(TurnCompleted()))

    with engine.watch_turns("e9a") as watch:
        with pytest.raises(RuntimeError):
            await _drain_auto(engine, "e9a", client)

    assert watch.outcome == TurnOutcome("failed", "turn not persisted: disk full")


async def test_unpersisted_user_turn_is_failed(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[("text", "x"), ("result", "sdk-9")]])
    await _session(engine, "e9b")
    _fail_assistant_write(engine)

    with engine.watch_turns("e9b") as watch:
        with pytest.raises(RuntimeError):
            await engine.run("e9b", "hello", source="web", channel="web", actor=None)

    assert watch.outcome == TurnOutcome("failed", "turn not persisted: disk full")


async def test_cancel_while_saving_the_turn_is_interrupted(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[("text", "x"), ("result", "sdk-9")]])
    await _session(engine, "e9c")
    saving = asyncio.Event()
    _fail_assistant_write(engine, hang=saving)

    with engine.watch_turns("e9c") as watch:
        task = asyncio.create_task(
            engine.run("e9c", "hello", source="web", channel="web", actor=None),
        )
        await asyncio.wait_for(saving.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert watch.outcome == TurnOutcome("interrupted", "stopped while saving the turn")


async def test_autonomous_idle_timeout_is_interrupted(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[]])
    engine.config.agent.cli_idle_timeout_seconds = 1
    await _session(engine, "e10")

    with engine.watch_turns("e10") as watch:
        with pytest.raises(asyncio.TimeoutError):
            await _drain_auto(engine, "e10", _IdleClient(_auto_batch(), "timeout"))

    assert watch.outcome == TurnOutcome("interrupted", "runtime went silent")


async def test_autonomous_stream_end_is_failed(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[]])
    await _session(engine, "e11")

    with engine.watch_turns("e11") as watch:
        await _drain_auto(engine, "e11", _IdleClient(_auto_batch(), "none"))

    assert watch.outcome == TurnOutcome("failed", "agent stream ended mid-turn")
    assert (await _last_assistant(db, "e11"))["content"] == "x"


async def test_autonomous_cancel_is_interrupted(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[]])
    await _session(engine, "e12")
    seen = await _token_seen("e12")
    try:
        with engine.watch_turns("e12") as watch:
            task = asyncio.create_task(
                _drain_auto(engine, "e12", _IdleClient(_auto_batch(), "hang")),
            )
            await asyncio.wait_for(seen.wait(), 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        await broadcaster.unregister("e12", "turn-outcome-test")

    assert watch.outcome == TurnOutcome("interrupted", "stopped by user")
    assert (await _last_assistant(db, "e12"))["content"] == "x\n\n[Stopped by user]"


# --------------------------------------------------------------------------- #
#  Cron completion                                                             #
# --------------------------------------------------------------------------- #


async def _cron_logs(db, job_id: str) -> list[dict]:
    return sorted(await db.get_cron_logs(job_id), key=lambda r: r["id"])


async def test_cron_max_turns_run_is_logged_as_error(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[("text", "partial"), ("result", "s", MAX_TURNS)]])
    svc = CronService(engine.config, engine, db)

    await svc._run_job_inner(CronJob(id="c1", schedule="1h", prompt="do it"))

    [log] = await _cron_logs(db, "c1")
    assert log["status"] == "error"
    assert log["error"] == f"turn failed: {MAX_TURNS_ERROR}"
    assert f"⚠️ Turn failed: {MAX_TURNS_ERROR}" in log["output"]
    assert log["session_id"].startswith("cron:c1:")


async def test_cron_completed_run_is_logged_as_success(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[("text", "done"), ("result", "s")]])
    svc = CronService(engine.config, engine, db)

    await svc._run_job_inner(CronJob(id="c2", schedule="1h", prompt="do it"))

    [log] = await _cron_logs(db, "c2")
    assert log["status"] == "success"
    assert log["error"] is None


async def test_persistent_cron_interrupted_run_is_logged_as_error(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[("text", "partial"), ("result", "s", ABORTED)]])
    svc = CronService(engine.config, engine, db)
    job = CronJob(
        id="c3", schedule="1h", prompt="do it",
        session_mode="persistent", context_rotate_hours=0,
    )

    await svc._run_job_inner(job)

    [log] = await _cron_logs(db, "c3")
    assert log["status"] == "error"
    assert log["error"] == "turn interrupted: aborted streaming"


async def test_cron_crashed_run_is_logged_as_error(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[("text", "half an answer"), ("raise",)]])
    svc = CronService(engine.config, engine, db)

    await svc._run_job_inner(CronJob(id="c4", schedule="1h", prompt="do it"))

    [log] = await _cron_logs(db, "c4")
    assert log["status"] == "error"
    assert log["error"].startswith("turn failed: Agent error:")


async def test_overlapping_persistent_runs_each_log_their_own_outcome(db, tmp_path):
    engine, made = await _engine(db, tmp_path, [
        [("text", "seed"), ("result", "s")],
        [("text", "first"), ("result", "s", MAX_TURNS)],
        [("text", "second"), ("result", "s")],
    ])
    svc = CronService(engine.config, engine, db)
    job = CronJob(
        id="c5", schedule="1h", prompt="do it",
        session_mode="persistent", context_rotate_hours=0, lock=False,
    )
    await svc._run_job_inner(job)  # seeds the generation session both runs share
    [seed] = await _cron_logs(db, "c5")

    # Hold the first overlapping run inside its teardown until the second
    # run's turn has ended too, so both are in flight across both turn ends.
    real_teardown = engine._teardown_oneshot_client
    second_done = asyncio.Event()
    calls = {"n": 0}

    async def _teardown(sid, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            await asyncio.wait_for(second_done.wait(), 5)
        else:
            second_done.set()
        await real_teardown(sid, **kwargs)

    engine._teardown_oneshot_client = _teardown
    await asyncio.gather(svc._run_job_inner(job), svc._run_job_inner(job))

    assert len(made) == 3
    logs = [r for r in await _cron_logs(db, "c5") if r["id"] > seed["id"]]
    assert {r["session_id"] for r in logs} == {seed["session_id"]}
    assert sorted((r["status"], r["error"]) for r in logs) == [
        ("error", f"turn failed: {MAX_TURNS_ERROR}"),
        ("success", None),
    ]


# --------------------------------------------------------------------------- #
#  Workflow completion                                                         #
# --------------------------------------------------------------------------- #


async def _workflow_run(db, tmp_path, engine) -> dict:
    service = WorkflowRunService(_make_config(tmp_path), db, engine)
    run = await service.start_run(ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
    await _drain(service)
    return await service.get_run(run["id"])


async def test_workflow_max_turns_run_fails_and_session_stays_healthy(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[("text", "partial"), ("result", "s", MAX_TURNS)]])

    run = await _workflow_run(db, tmp_path, engine)

    assert run["status"] == "failed"
    assert run["error"] == f"turn failed: {MAX_TURNS_ERROR}"
    # The run's teardown parks the session idle; the failed turn did not mark
    # it errored or drop its resume id.
    session = await db.get_session(run["session_id"])
    assert session["status"] == "idle"
    assert session["sdk_session_id"] == "s"


async def test_workflow_completed_run_is_done(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[("text", "report"), ("result", "s")]])

    run = await _workflow_run(db, tmp_path, engine)

    assert run["status"] == "done"


async def test_workflow_crashed_run_fails(db, tmp_path):
    engine, _ = await _engine(db, tmp_path, [[("text", "half an answer"), ("raise",)]])

    run = await _workflow_run(db, tmp_path, engine)

    assert run["status"] == "failed"
    assert run["error"].startswith("turn failed: Agent error:")


def _watched_mock_engine(db, watch: TurnWatch) -> MagicMock:
    engine = _make_mock_engine(db)
    engine.watch_turns = lambda sid: contextlib.nullcontext(watch)
    return engine


def _busy_once(then):
    calls = {"n": 0}

    def _bg(session_id: str) -> bool:
        calls["n"] += 1
        if calls["n"] == 1:
            then()
            return True
        return False

    return MagicMock(side_effect=_bg)


async def _mock_workflow_run(db, tmp_path, engine) -> dict:
    service = WorkflowRunService(_make_config(tmp_path), db, engine)
    service.config.workflows.poll_interval_seconds = 1
    run = await service.start_run(ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
    await _drain(service, timeout=30.0)
    return await service.get_run(run["id"])


async def test_workflow_background_turn_failure_fails_the_run(db, tmp_path):
    watch = TurnWatch(TurnOutcome())
    engine = _watched_mock_engine(db, watch)

    def _bg_turn_failed():
        watch.outcome = TurnOutcome("failed", "API error (HTTP 529)")

    engine.has_live_background_tasks = _busy_once(_bg_turn_failed)

    run = await _mock_workflow_run(db, tmp_path, engine)

    assert run["status"] == "failed"
    assert run["error"] == "turn failed: API error (HTTP 529)"


async def test_workflow_latest_turn_outcome_wins(db, tmp_path):
    watch = TurnWatch(TurnOutcome())
    engine = _watched_mock_engine(db, watch)

    async def _launch_failed(**kwargs):
        watch.outcome = TurnOutcome("failed", "API error (HTTP 529)")
        return "launching turn"

    def _bg_turn_completed():
        watch.outcome = TurnOutcome()

    engine.run = AsyncMock(side_effect=_launch_failed)
    engine.has_live_background_tasks = _busy_once(_bg_turn_completed)

    run = await _mock_workflow_run(db, tmp_path, engine)

    assert run["status"] == "done"


async def test_workflow_stop_listener_still_kills(db, tmp_path):
    watch = TurnWatch(TurnOutcome())
    engine = _watched_mock_engine(db, watch)
    service = WorkflowRunService(_make_config(tmp_path), db, engine)

    async def _stopped(session_id, **kwargs):
        await service._on_session_stop(session_id)
        watch.outcome = TurnOutcome("interrupted", "stopped by user")
        return "partial"

    engine.run = AsyncMock(side_effect=_stopped)
    run = await service.start_run(ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
    await _drain(service)

    assert (await service.get_run(run["id"]))["status"] == "killed"


async def test_workflow_without_a_turn_outcome_fails_closed(db, tmp_path):
    engine = _watched_mock_engine(db, TurnWatch())

    run = await _mock_workflow_run(db, tmp_path, engine)

    assert run["status"] == "failed"
    assert run["error"] == "no turn outcome recorded"
