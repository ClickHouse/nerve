"""Tests for workflow runs — budget-capped multi-agent jobs.

Covers four layers:
  * ``WorkflowRunStore`` — persistence, list filters, CAS transitions.
  * ``UsageStore.get_session_effective_cost`` — the budget meter input.
  * ``WorkflowRunService`` — validation, dispatch queueing, execution,
    kill scoping, budget warn/kill, live spend estimates, restart recovery.
  * ``workflow_run_*`` tool handlers — direct ToolContext invocation.

The service is exercised WITHOUT ``start()`` in most tests (no monitor
loop): internal methods (``_tick``, ``_refresh_spend``,
``_maybe_dispatch``) are driven directly, and background execution is
synchronized by awaiting the tasks the service tracks in ``_exec_tasks``
/ ``_kill_tasks`` — never by sleeping.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve.agent.tools.handlers.workflow_runs import (
    workflow_run_kill_handler,
    workflow_run_list_handler,
    workflow_run_start_handler,
    workflow_run_status_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig
from nerve.db.usage import estimate_turn_cost
from nerve.workflows.service import (
    ENGINE_CLAUDE,
    ENGINE_CODEX,
    WorkflowRunError,
    WorkflowRunService,
)

# --------------------------------------------------------------------------- #
#  Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _make_config(tmp_path: Path) -> NerveConfig:
    """Config with journals isolated under tmp_path and no kill grace.

    conftest does NOT redirect ``~/.nerve/workflow-runs`` — every service
    test must point ``runs_dir`` at tmp_path so journals never touch the
    real home directory.
    """
    cfg = NerveConfig()
    cfg.workflows.runs_dir = tmp_path / "runs"
    cfg.workflows.kill_grace_seconds = 0
    return cfg


def _make_engine(db) -> MagicMock:
    """Fake AgentEngine with the surface the service touches.

    ``sessions.get_or_create`` persists a real session row because
    ``workflow_runs.session_id`` and ``session_usage.session_id`` carry
    FK constraints to ``sessions(id)`` (enforced — foreign_keys=ON).
    """
    engine = MagicMock()

    async def _get_or_create(session_id: str, **kwargs):
        await db.create_session(
            session_id,
            title=kwargs.get("title"),
            source=kwargs.get("source", "web"),
            backend=kwargs.get("backend", "claude"),
            model=kwargs.get("model"),
            cwd=kwargs.get("cwd"),
        )
        return {"id": session_id}

    engine.sessions.get_or_create = AsyncMock(side_effect=_get_or_create)
    engine.run = AsyncMock(return_value="final answer")
    engine.register_task = MagicMock()
    engine.stop_session = AsyncMock(return_value=True)
    engine._discard_client = AsyncMock()
    engine.is_session_running = MagicMock(return_value=False)
    engine.get_live_workflow_tokens = MagicMock(return_value=0)
    engine.notification_service = MagicMock()
    engine.notification_service.send_notification = AsyncMock()
    return engine


async def _drain(service: WorkflowRunService, timeout: float = 5.0) -> None:
    """Await spawned execute/kill tasks, including dispatch chains.

    ``_execute``'s finally block promotes queued runs (spawning new
    tasks), so loop until both trackers are empty.
    """
    deadline = time.monotonic() + timeout
    while service._exec_tasks or service._kill_tasks:
        tasks = list(service._exec_tasks.values()) + list(service._kill_tasks)
        remaining = deadline - time.monotonic()
        assert remaining > 0, "workflow tasks did not settle in time"
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True), timeout=remaining,
        )


async def _seed_running(db, run_id: str, budget, engine_kind: str = ENGINE_CLAUDE) -> str:
    """Insert a running run wired to a real session row; returns session id."""
    session_id = f"workflow:{run_id}"
    await db.create_workflow_run(run_id, engine_kind, {"prompt": "p"}, budget)
    await db.transition_workflow_run(run_id, "running", expect=("pending",))
    await db.create_session(session_id, source="workflow")
    await db.update_workflow_run(run_id, {"session_id": session_id})
    return session_id


def _notif_calls(engine) -> list:
    return engine.notification_service.send_notification.call_args_list


@pytest_asyncio.fixture
async def engine(db):
    return _make_engine(db)


@pytest_asyncio.fixture
async def service(db, engine, tmp_path):
    svc = WorkflowRunService(_make_config(tmp_path), db, engine)
    yield svc
    await _drain(svc)


# --------------------------------------------------------------------------- #
#  WorkflowRunStore                                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestWorkflowRunStore:
    @pytest_asyncio.fixture
    async def seeded_runs(self, db):
        """Four runs created oldest -> newest: pending, running, done, failed."""
        statuses = ["pending", "running", "done", "failed"]
        ids = []
        for i, status in enumerate(statuses):
            run_id = f"wfr-{i:08d}"
            await db.create_workflow_run(
                run_id, ENGINE_CLAUDE, {"prompt": f"task {i}"}, 1.0,
            )
            if status != "pending":
                await db.transition_workflow_run(
                    run_id, "running", expect=("pending",),
                )
            if status in ("done", "failed"):
                await db.transition_workflow_run(
                    run_id, status, expect=("running",),
                )
            ids.append(run_id)
        return ids

    async def test_create_and_get_round_trips_spec(self, db):
        spec = {"prompt": "fix issue #12", "model": "some-model", "cwd": "/tmp"}
        created = await db.create_workflow_run(
            "wfr-aaaa0001", ENGINE_CLAUDE, spec, 5.0,
            title="demo", created_by="alice", journal_dir="/tmp/journal",
        )
        assert created["status"] == "pending"
        assert created["spec"] == spec  # parsed dict, not a JSON string

        run = await db.get_workflow_run("wfr-aaaa0001")
        assert run["spec"] == spec
        assert run["engine"] == ENGINE_CLAUDE
        assert run["title"] == "demo"
        assert run["budget_usd"] == 5.0
        assert run["spent_usd"] == 0.0
        assert run["created_by"] == "alice"
        assert run["journal_dir"] == "/tmp/journal"
        assert run["created_at"] and run["updated_at"]
        assert run["started_at"] is None and run["finished_at"] is None

    async def test_get_missing_returns_none(self, db):
        assert await db.get_workflow_run("wfr-missing") is None

    async def test_list_newest_first(self, db, seeded_runs):
        runs = await db.list_workflow_runs()
        assert [r["id"] for r in runs] == list(reversed(seeded_runs))

    async def test_list_filter_active(self, db, seeded_runs):
        runs = await db.list_workflow_runs(status="active")
        assert {r["id"] for r in runs} == {seeded_runs[0], seeded_runs[1]}

    async def test_list_filter_exact_status(self, db, seeded_runs):
        runs = await db.list_workflow_runs(status="done")
        assert [r["id"] for r in runs] == [seeded_runs[2]]

    async def test_list_limit_offset(self, db, seeded_runs):
        page1 = await db.list_workflow_runs(limit=2)
        page2 = await db.list_workflow_runs(limit=2, offset=2)
        assert [r["id"] for r in page1] == [seeded_runs[3], seeded_runs[2]]
        assert [r["id"] for r in page2] == [seeded_runs[1], seeded_runs[0]]

    async def test_count(self, db, seeded_runs):
        assert await db.count_workflow_runs() == 4
        assert await db.count_workflow_runs("active") == 2
        assert await db.count_workflow_runs("running") == 1
        assert await db.count_workflow_runs("killed") == 0

    async def test_get_active_oldest_first(self, db, seeded_runs):
        active = await db.get_active_workflow_runs()
        assert [r["id"] for r in active] == [seeded_runs[0], seeded_runs[1]]

    async def test_transition_to_running_stamps_started_at(self, db):
        await db.create_workflow_run("wfr-cas00001", ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
        flipped = await db.transition_workflow_run(
            "wfr-cas00001", "running", expect=("pending",),
        )
        assert flipped is True
        run = await db.get_workflow_run("wfr-cas00001")
        assert run["status"] == "running"
        assert run["started_at"]
        assert run["finished_at"] is None

    async def test_transition_expect_mismatch_loses(self, db):
        await db.create_workflow_run("wfr-cas00002", ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
        assert await db.transition_workflow_run(
            "wfr-cas00002", "done", expect=("running",),
        ) is False
        assert (await db.get_workflow_run("wfr-cas00002"))["status"] == "pending"

    async def test_terminal_transition_single_winner(self, db):
        """Concurrent finishers: only the first CAS flips the row."""
        await db.create_workflow_run("wfr-cas00003", ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
        await db.transition_workflow_run("wfr-cas00003", "running", expect=("pending",))
        assert await db.transition_workflow_run(
            "wfr-cas00003", "done", expect=("running",), result="all good",
        ) is True
        assert await db.transition_workflow_run(
            "wfr-cas00003", "killed", expect=("running",), error="too late",
        ) is False
        run = await db.get_workflow_run("wfr-cas00003")
        assert run["status"] == "done"
        assert run["result"] == "all good"
        assert run["error"] is None
        assert run["finished_at"]

    async def test_transition_invalid_status_raises(self, db):
        with pytest.raises(ValueError, match="unknown workflow run status"):
            await db.transition_workflow_run("wfr-cas00004", "exploded")

    async def test_update_whitelist_ignores_unknown_fields(self, db):
        await db.create_workflow_run("wfr-upd00001", ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
        # Unknown-only update is a no-op (status is lifecycle-managed).
        assert await db.update_workflow_run(
            "wfr-upd00001", {"status": "done", "bogus": 1},
        ) is False
        run = await db.get_workflow_run("wfr-upd00001")
        assert run["status"] == "pending"
        # Whitelisted fields apply; unknown ones alongside are dropped.
        assert await db.update_workflow_run(
            "wfr-upd00001", {"spent_usd": 1.25, "bogus": 1},
        ) is True
        run = await db.get_workflow_run("wfr-upd00001")
        assert run["spent_usd"] == 1.25


# --------------------------------------------------------------------------- #
#  Budget meter input — get_session_effective_cost                             #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_get_session_effective_cost_billed_plus_estimate_fallback(db):
    await db.create_session("workflow:wfr-cost", source="workflow")
    # Billed turn: cost_usd wins even when an estimate is present.
    await db.record_turn_usage(
        "workflow:wfr-cost", 100, 50, 0, 0, 200000,
        cost_usd=0.5, cost_basis="sdk_delta", estimated_cost_usd=0.9,
    )
    # Codex subscription turn: cost_usd NULL, spend lives in the estimate.
    await db.record_turn_usage(
        "workflow:wfr-cost", 100, 50, 0, 0, 200000,
        cost_usd=None, cost_basis="chatgpt_credit", estimated_cost_usd=0.25,
    )
    # All-zero row contributes nothing.
    await db.record_turn_usage(
        "workflow:wfr-cost", 0, 0, 0, 0, 200000,
        cost_usd=0.0, estimated_cost_usd=None,
    )
    assert await db.get_session_effective_cost("workflow:wfr-cost") == pytest.approx(0.75)
    # Sessions with no usage rows meter at zero.
    assert await db.get_session_effective_cost("workflow:wfr-other") == 0.0


# --------------------------------------------------------------------------- #
#  Service — start_run validation                                              #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestStartRunValidation:
    async def test_unknown_engine_rejected(self, service):
        with pytest.raises(WorkflowRunError, match="unknown engine"):
            await service.start_run("mystery-engine", {"prompt": "p"}, 1.0)

    async def test_empty_prompt_rejected(self, service):
        with pytest.raises(WorkflowRunError, match="prompt"):
            await service.start_run(ENGINE_CLAUDE, {"prompt": "   "}, 1.0)

    @pytest.mark.parametrize("budget", [None, 0, -2.5])
    async def test_missing_or_nonpositive_budget_rejected(self, service, budget):
        with pytest.raises(WorkflowRunError, match="budget_usd is required"):
            await service.start_run(ENGINE_CLAUDE, {"prompt": "p"}, budget)

    async def test_unbudgeted_allowed_when_configured(self, service, db):
        service.config.workflows.allow_unbudgeted = True
        run = await service.start_run(ENGINE_CLAUDE, {"prompt": "p"}, None)
        assert run["budget_usd"] is None
        await _drain(service)
        assert (await db.get_workflow_run(run["id"]))["status"] == "done"

    async def test_bad_cwd_rejected(self, service, tmp_path):
        with pytest.raises(WorkflowRunError, match="cwd is not a directory"):
            await service.start_run(
                ENGINE_CLAUDE,
                {"prompt": "p", "cwd": str(tmp_path / "does-not-exist")},
                1.0,
            )

    async def test_start_creates_journal_and_row(self, service, db):
        run = await service.start_run(
            ENGINE_CLAUDE, {"prompt": "do the thing"}, 2.5, title="demo",
        )
        assert run["id"].startswith("wfr-")
        assert run["status"] in ("pending", "running")
        journal = Path(run["journal_dir"])
        assert journal.is_dir()
        assert journal.parent == service.runs_dir()
        events = (journal / "events.ndjson").read_text(encoding="utf-8")
        assert '"event": "created"' in events
        assert (journal / "run.json").exists()
        await _drain(service)


# --------------------------------------------------------------------------- #
#  Service — dispatch queue                                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_second_run_queues_until_slot_frees(db, engine, tmp_path):
    cfg = _make_config(tmp_path)
    cfg.workflows.max_concurrent_runs = 1
    svc = WorkflowRunService(cfg, db, engine)

    gate = asyncio.Event()

    async def blocking_run(**kwargs):
        await gate.wait()
        return "done"

    engine.run = AsyncMock(side_effect=blocking_run)

    first = await svc.start_run(ENGINE_CLAUDE, {"prompt": "first"}, 1.0)
    second = await svc.start_run(ENGINE_CLAUDE, {"prompt": "second"}, 1.0)
    assert first["status"] == "running"
    assert second["status"] == "pending"
    assert await db.count_workflow_runs("running") == 1

    # Extra dispatch while the slot is occupied must not promote anything.
    await svc._maybe_dispatch()
    assert (await db.get_workflow_run(second["id"]))["status"] == "pending"

    gate.set()
    await _drain(svc)

    assert (await db.get_workflow_run(first["id"]))["status"] == "done"
    assert (await db.get_workflow_run(second["id"]))["status"] == "done"


# --------------------------------------------------------------------------- #
#  Service — execution                                                         #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestExecute:
    async def test_happy_path_claude(self, service, db, engine):
        run = await service.start_run(
            ENGINE_CLAUDE, {"prompt": "summarize the repo"}, 5.0, title="sum",
        )
        await _drain(service)

        fresh = await db.get_workflow_run(run["id"])
        session_id = f"workflow:{run['id']}"
        assert fresh["status"] == "done"
        assert fresh["result"] == "final answer"
        assert fresh["session_id"] == session_id
        assert fresh["finished_at"]

        # Dedicated session on the claude backend, source "workflow".
        call = engine.sessions.get_or_create.call_args
        assert call.args[0] == session_id
        assert call.kwargs["source"] == "workflow"
        assert call.kwargs["backend"] == "claude"
        session = await db.get_session(session_id)
        assert session["source"] == "workflow"

        # Prompt carries the task text and the budget line.
        run_kwargs = engine.run.call_args.kwargs
        assert run_kwargs["session_id"] == session_id
        assert "summarize the repo" in run_kwargs["user_message"]
        assert "Hard budget: $5.00" in run_kwargs["user_message"]

        # Result journal + completion notification.
        result_md = Path(fresh["journal_dir"]) / "result.md"
        assert result_md.read_text(encoding="utf-8") == "final answer"
        assert any("finished" in c.kwargs["title"] for c in _notif_calls(engine))

    async def test_codex_engine_uses_codex_backend(self, service, db, engine):
        run = await service.start_run(
            ENGINE_CODEX, {"prompt": "parallelize the tests"}, 5.0,
        )
        await _drain(service)

        call = engine.sessions.get_or_create.call_args
        assert call.kwargs["backend"] == "codex"
        assert call.kwargs["model"] == service.config.codex.model
        assert (await db.get_workflow_run(run["id"]))["status"] == "done"
        assert "Ultracode" in engine.run.call_args.kwargs["user_message"]

    async def test_failure_records_error_and_notifies(self, service, db, engine):
        engine.run = AsyncMock(side_effect=RuntimeError("engine exploded"))
        run = await service.start_run(ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
        await _drain(service)

        fresh = await db.get_workflow_run(run["id"])
        assert fresh["status"] == "failed"
        assert "engine exploded" in fresh["error"]
        assert any(
            "failed" in c.kwargs["title"] and c.kwargs["priority"] == "high"
            for c in _notif_calls(engine)
        )

    async def test_public_run_trims_long_prompt(self, service):
        long_prompt = "x" * 900
        run = {"id": "wfr-trim0001", "spec": {"prompt": long_prompt}, "status": "pending"}
        out = service.public_run(run)
        assert len(out["spec"]["prompt"]) == 501
        assert out["spec"]["prompt"].endswith("…")
        # The stored run is never mutated.
        assert run["spec"]["prompt"] == long_prompt


# --------------------------------------------------------------------------- #
#  Service — kill                                                              #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestKillRun:
    async def test_kill_unknown_raises(self, service):
        with pytest.raises(WorkflowRunError, match="no such workflow run"):
            await service.kill_run("wfr-missing")

    async def test_kill_pending_skips_enforcement(self, service, db):
        await db.create_workflow_run("wfr-pend0001", ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
        out = await service.kill_run(
            "wfr-pend0001", reason="not needed", killed_by="alice",
        )
        assert out["status"] == "killed"
        assert "not needed" in out["error"]
        assert "alice" in out["error"]
        # Nothing was running — no stop sequence spawned.
        assert not service._kill_tasks

    async def test_kill_running_stops_own_session(self, service, db, engine):
        session_id = await _seed_running(db, "wfr-run00001", 1.0)
        out = await service.kill_run("wfr-run00001", reason="user request")
        assert out["status"] == "killed"
        assert "user request" in out["error"]

        await asyncio.gather(*list(service._kill_tasks))
        engine.stop_session.assert_awaited_once_with(session_id)
        engine._discard_client.assert_awaited_once_with(
            session_id, background_memorize=True,
        )

    async def test_kill_terminal_is_idempotent(self, service, db, engine):
        await db.create_workflow_run("wfr-done0001", ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
        await db.transition_workflow_run("wfr-done0001", "running", expect=("pending",))
        await db.transition_workflow_run(
            "wfr-done0001", "done", expect=("running",), result="ok",
        )
        out = await service.kill_run("wfr-done0001")
        assert out["status"] == "done"
        assert out["result"] == "ok"
        assert not service._kill_tasks
        engine.stop_session.assert_not_awaited()


# --------------------------------------------------------------------------- #
#  Service — budget monitor                                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestBudgetEnforcement:
    async def test_tick_kills_at_budget(self, service, db, engine):
        session_id = await _seed_running(db, "wfr-bud00001", 1.0)
        await db.record_turn_usage(
            session_id, 100, 50, 0, 0, 200000,
            cost_usd=1.5, cost_basis="sdk_delta",
        )

        await service._tick()

        run = await db.get_workflow_run("wfr-bud00001")
        assert run["status"] == "budget_exhausted"
        assert run["spent_usd"] == pytest.approx(1.5)
        assert "budget exhausted" in run["error"]
        assert any(
            "budget exhausted" in c.kwargs["title"] and c.kwargs["priority"] == "high"
            for c in _notif_calls(engine)
        )
        # Enforcement is scoped to the run's own session.
        await asyncio.gather(*list(service._kill_tasks))
        engine.stop_session.assert_awaited_once_with(session_id)
        engine._discard_client.assert_awaited_once_with(
            session_id, background_memorize=True,
        )

    async def test_tick_warns_once_at_warn_fraction(self, service, db, engine):
        session_id = await _seed_running(db, "wfr-warn0001", 10.0)
        await db.record_turn_usage(
            session_id, 100, 50, 0, 0, 200000,
            cost_usd=8.5, cost_basis="sdk_delta",
        )

        def warn_calls():
            return [c for c in _notif_calls(engine) if "% of budget" in c.kwargs["title"]]

        await service._tick()
        run = await db.get_workflow_run("wfr-warn0001")
        assert run["status"] == "running"  # warned, not killed
        assert run["warned_at"]
        assert len(warn_calls()) == 1

        # Second tick: warned_at is set — no duplicate warning.
        await service._tick()
        assert len(warn_calls()) == 1
        assert not service._kill_tasks

    async def test_tick_ignores_unbudgeted_runs(self, service, db, engine):
        session_id = await _seed_running(db, "wfr-nobud001", None)
        await db.record_turn_usage(
            session_id, 100, 50, 0, 0, 200000,
            cost_usd=99.0, cost_basis="sdk_delta",
        )

        await service._tick()

        run = await db.get_workflow_run("wfr-nobud001")
        assert run["status"] == "running"
        assert run["spent_usd"] == pytest.approx(99.0)  # metered, never enforced
        assert not service._kill_tasks

    async def test_refresh_spend_uses_live_claude_tokens(self, service, db, engine):
        session_id = await _seed_running(db, "wfr-live0001", 50.0)
        engine.get_live_workflow_tokens = MagicMock(return_value=200_000)

        spent = await service._refresh_spend("wfr-live0001")

        expected = estimate_turn_cost(
            {"output_tokens": 200_000}, service.config.agent.model,
        )
        assert spent == pytest.approx(expected)
        assert spent > 0
        engine.get_live_workflow_tokens.assert_called_once_with(session_id)
        run = await db.get_workflow_run("wfr-live0001")
        assert run["spent_usd"] == pytest.approx(spent)

        # final=True drops the in-flight estimate (recorded cost only).
        assert await service._refresh_spend("wfr-live0001", final=True) == 0.0


# --------------------------------------------------------------------------- #
#  Service — restart recovery                                                  #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_start_recovery_fails_orphaned_active_runs(db, engine, tmp_path):
    svc = WorkflowRunService(_make_config(tmp_path), db, engine)
    await db.create_workflow_run("wfr-orph0001", ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
    await db.create_workflow_run("wfr-orph0002", ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
    await db.transition_workflow_run("wfr-orph0002", "running", expect=("pending",))
    # Terminal rows are untouched by recovery.
    await db.create_workflow_run("wfr-orph0003", ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
    await db.transition_workflow_run("wfr-orph0003", "running", expect=("pending",))
    await db.transition_workflow_run(
        "wfr-orph0003", "done", expect=("running",), result="ok",
    )

    await svc.start()
    try:
        for run_id in ("wfr-orph0001", "wfr-orph0002"):
            run = await db.get_workflow_run(run_id)
            assert run["status"] == "failed"
            assert run["error"] == "interrupted by nerve restart"
        assert (await db.get_workflow_run("wfr-orph0003"))["status"] == "done"
        assert any(
            "interrupted by restart" in c.kwargs["title"]
            for c in _notif_calls(engine)
        )
    finally:
        await svc.stop()


# --------------------------------------------------------------------------- #
#  Tool handlers                                                               #
# --------------------------------------------------------------------------- #


def _ctx(db, engine, session_id: str = "sess-main") -> ToolContext:
    return ToolContext(session_id=session_id, db=db, engine=engine)


@pytest.mark.asyncio
class TestWorkflowRunTools:
    @pytest_asyncio.fixture
    async def tool_service(self, db, engine, tmp_path, monkeypatch):
        """Service installed as the module singleton the handlers resolve."""
        svc = WorkflowRunService(_make_config(tmp_path), db, engine)
        monkeypatch.setattr("nerve.workflows._service", svc)
        yield svc
        await _drain(svc)

    async def test_start_happy_path(self, tool_service, db, engine):
        result = await workflow_run_start_handler(_ctx(db, engine), {
            "engine": "claude-workflow",
            "prompt": "fix issue #12",
            "budget_usd": 3.0,
            "title": "bugfix",
        })
        assert not result.is_error
        text = result.content[0]["text"]
        assert "wfr-" in text
        assert "created" in text
        assert "$3.00" in text
        runs = await db.list_workflow_runs()
        assert len(runs) == 1
        assert runs[0]["created_by"] == "session:sess-main"
        await _drain(tool_service)

    async def test_start_validation_error_is_tool_error(self, tool_service, db, engine):
        result = await workflow_run_start_handler(_ctx(db, engine), {
            "engine": "claude-workflow", "prompt": "", "budget_usd": 3.0,
        })
        assert result.is_error
        assert "prompt" in result.content[0]["text"]

    async def test_service_disabled_is_error(self, db, engine, monkeypatch):
        monkeypatch.setattr("nerve.workflows._service", None)
        result = await workflow_run_start_handler(_ctx(db, engine), {
            "engine": "claude-workflow", "prompt": "p", "budget_usd": 1.0,
        })
        assert result.is_error
        assert "disabled" in result.content[0]["text"]

    async def test_engine_not_wired_is_error(self, tool_service, db):
        ctx = ToolContext(session_id="sess-main", db=db, engine=None)
        result = await workflow_run_status_handler(ctx, {"run_id": "wfr-x"})
        assert result.is_error
        assert "not wired" in result.content[0]["text"]

    async def test_external_session_rejected_for_start_and_kill_only(
        self, tool_service, db, engine,
    ):
        await db.create_session("ext-1", source="external")
        ctx = _ctx(db, engine, session_id="ext-1")

        start = await workflow_run_start_handler(ctx, {
            "engine": "claude-workflow", "prompt": "p", "budget_usd": 1.0,
        })
        assert start.is_error
        assert "external" in start.content[0]["text"]

        kill = await workflow_run_kill_handler(ctx, {"run_id": "wfr-whatever"})
        assert kill.is_error
        assert "external" in kill.content[0]["text"]

        # Inspection stays available to external satellites.
        await db.create_workflow_run("wfr-ext00001", ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
        listing = await workflow_run_list_handler(ctx, {})
        assert not listing.is_error
        assert "wfr-ext00001" in listing.content[0]["text"]
        status = await workflow_run_status_handler(ctx, {"run_id": "wfr-ext00001"})
        assert not status.is_error
        assert "wfr-ext00001" in status.content[0]["text"]

    async def test_status_returns_public_run_json(self, tool_service, db, engine):
        await db.create_workflow_run(
            "wfr-stat0001", ENGINE_CLAUDE, {"prompt": "p"}, 2.0, title="demo",
        )
        result = await workflow_run_status_handler(
            _ctx(db, engine), {"run_id": "wfr-stat0001"},
        )
        assert not result.is_error
        text = result.content[0]["text"]
        assert text.startswith("wfr-stat0001 [pending]")
        payload = json.loads(text.split("\n\n", 1)[1])
        assert payload["id"] == "wfr-stat0001"
        assert payload["spec"]["prompt"] == "p"

    async def test_status_unknown_run_is_error(self, tool_service, db, engine):
        result = await workflow_run_status_handler(
            _ctx(db, engine), {"run_id": "wfr-missing"},
        )
        assert result.is_error
        assert "no such workflow run" in result.content[0]["text"]

    async def test_kill_unknown_run_is_error(self, tool_service, db, engine):
        result = await workflow_run_kill_handler(
            _ctx(db, engine), {"run_id": "wfr-missing"},
        )
        assert result.is_error
        assert "no such workflow run" in result.content[0]["text"]

    async def test_kill_records_killer_session(self, tool_service, db, engine):
        await db.create_workflow_run("wfr-kill0001", ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
        result = await workflow_run_kill_handler(_ctx(db, engine), {
            "run_id": "wfr-kill0001", "reason": "changed my mind",
        })
        assert not result.is_error
        assert "'killed'" in result.content[0]["text"]
        run = await db.get_workflow_run("wfr-kill0001")
        assert run["status"] == "killed"
        assert "changed my mind" in run["error"]
        assert "session:sess-main" in run["error"]

    async def test_list_formatting_and_filters(self, tool_service, db, engine):
        empty = await workflow_run_list_handler(_ctx(db, engine), {})
        assert "No workflow runs" in empty.content[0]["text"]

        await db.create_workflow_run(
            "wfr-list0001", ENGINE_CLAUDE, {"prompt": "p"}, 2.0, title="demo run",
        )
        result = await workflow_run_list_handler(_ctx(db, engine), {})
        text = result.content[0]["text"]
        assert "1 workflow run(s):" in text
        assert "wfr-list0001" in text
        assert "[pending]" in text
        assert "demo run" in text
        assert "$2.00" in text

        none_match = await workflow_run_list_handler(
            _ctx(db, engine), {"status": "done"},
        )
        assert "No workflow runs with status 'done'" in none_match.content[0]["text"]
