"""Tests for the shared plan approve/decline helpers (``nerve.agent.plan_service``).

``approve_plan`` and ``decline_plan`` back three surfaces — the HTTP routes
(WebUI), the MCP ``plan_approve``/``plan_decline`` tools, and the Telegram
``/plans`` command. These pin the single behaviour contract so the surfaces
can't drift apart (the same rationale as ``test_plan_revise.py``).
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from nerve.agent import tools as tools_mod
from nerve.agent.plan_service import (
    PlanNotFound,
    PlanNotPending,
    TaskNotFound,
    approve_plan,
    decline_plan,
)
from nerve.db import Database
from nerve.identity import ACTOR_KIND_HUMAN, Actor

# A person approving through the WebUI. The fake session store below keeps
# actors in memory, so no actor row is needed.
APPROVER = Actor(
    actor_id="actor-approver", kind=ACTOR_KIND_HUMAN,
    account_id="account-approver", display_name="Approver",
)


class FakeSessionManager:
    """Records get_or_create calls so tests can assert the impl session."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def get_or_create(
        self, session_id, title=None, source="web", metadata=None,
        *, actor: Actor | None,
    ) -> dict:
        self.calls.append({
            "session_id": session_id, "title": title, "source": source,
            "actor": actor,
        })
        return {"id": session_id, "title": title or session_id, "source": source}


class FakeEngine:
    """Mimics AgentEngine.run + .sessions + .register_task for approve tests."""

    def __init__(self, db: Database) -> None:
        self.db = db
        self.sessions = FakeSessionManager()
        self.runs: list[dict] = []
        self.registered: list[str] = []
        self.run_event = asyncio.Event()

    async def run(
        self, session_id, user_message, source="web", *, actor: Actor | None,
    ) -> None:
        self.runs.append({
            "session_id": session_id, "user_message": user_message,
            "source": source, "actor": actor,
        })
        self.run_event.set()

    def register_task(self, session_id, task) -> None:
        self.registered.append(session_id)


async def _setup(
    db: Database, tmp_path, *, plan_type: str = "generic", status: str = "pending",
) -> tuple[FakeEngine, str]:
    task_id = "t-act"
    file_path = "task.md"
    (tmp_path / file_path).write_text("# Demo task\n\nBody text.\n", encoding="utf-8")
    await db.upsert_task(
        task_id=task_id, file_path=file_path, title="Demo task",
        status="pending", content=(tmp_path / file_path).read_text(),
    )
    await db.create_plan(
        plan_id="plan-act", task_id=task_id, content="step one; step two",
        session_id="sess-proposer", version=1, plan_type=plan_type,
    )
    if status != "pending":
        await db.update_plan("plan-act", status=status)

    engine = FakeEngine(db)
    tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)
    return engine, task_id


@pytest.mark.asyncio
class TestApprovePlan:
    async def test_spawns_impl_marks_implementing_and_moves_task(self, db, tmp_path):
        engine, task_id = await _setup(db, tmp_path)

        result = await approve_plan(db=db, engine=engine, plan_id="plan-act", actor=APPROVER)
        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)

        impl = result["impl_session_id"]
        assert impl.startswith("impl-")
        assert result["plan_id"] == "plan-act"
        assert result["task_id"] == task_id

        plan = await db.get_plan("plan-act")
        assert plan["status"] == "implementing"
        assert plan["impl_session_id"] == impl

        task = await db.get_task(task_id)
        assert task["status"] == "in_progress"

        # Impl session created, registered with the engine, run dispatched.
        assert engine.sessions.calls[0]["session_id"] == impl
        assert engine.registered == [impl]
        assert len(engine.runs) == 1
        prompt = engine.runs[0]["user_message"]
        assert "step one; step two" in prompt   # plan content
        assert "Demo task" in prompt            # task title
        assert "Body text." in prompt           # task file content threaded in

    async def test_skill_create_gets_skill_prompt(self, db, tmp_path):
        engine, _ = await _setup(db, tmp_path, plan_type="skill-create")
        await approve_plan(db=db, engine=engine, plan_id="plan-act", actor=APPROVER)
        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)
        assert "skill_create" in engine.runs[0]["user_message"]

    async def test_refuses_non_pending(self, db, tmp_path):
        engine, _ = await _setup(db, tmp_path, status="implementing")
        with pytest.raises(PlanNotPending):
            await approve_plan(db=db, engine=engine, plan_id="plan-act", actor=APPROVER)
        assert engine.runs == []

    async def test_raises_plan_not_found(self, db, tmp_path):
        engine, _ = await _setup(db, tmp_path)
        with pytest.raises(PlanNotFound):
            await approve_plan(db=db, engine=engine, plan_id="plan-missing", actor=APPROVER)
        assert engine.runs == []

    async def test_raises_task_not_found(self, db, tmp_path):
        engine, _ = await _setup(db, tmp_path)
        await db.db.execute("DELETE FROM tasks WHERE id = ?", ("t-act",))
        await db.db.commit()
        with pytest.raises(TaskNotFound):
            await approve_plan(db=db, engine=engine, plan_id="plan-act", actor=APPROVER)
        assert engine.runs == []


@pytest.mark.asyncio
class TestDeclinePlan:
    async def test_marks_declined_and_closes_task(self, db, tmp_path):
        engine, task_id = await _setup(db, tmp_path)

        result = await decline_plan(
            db=db, engine=engine, plan_id="plan-act", feedback="not now",
        )
        assert result["status"] == "declined"
        assert result["feedback"] == "not now"

        plan = await db.get_plan("plan-act")
        assert plan["status"] == "declined"
        assert plan["feedback"] == "not now"

        task = await db.get_task(task_id)
        assert task["status"] == "done"

    async def test_without_feedback_still_closes(self, db, tmp_path):
        engine, task_id = await _setup(db, tmp_path)
        result = await decline_plan(db=db, engine=engine, plan_id="plan-act")
        assert result["feedback"] == ""
        plan = await db.get_plan("plan-act")
        assert plan["status"] == "declined"
        task = await db.get_task(task_id)
        assert task["status"] == "done"

    async def test_refuses_non_pending(self, db, tmp_path):
        engine, _ = await _setup(db, tmp_path, status="declined")
        with pytest.raises(PlanNotPending):
            await decline_plan(db=db, engine=engine, plan_id="plan-act")

    async def test_raises_plan_not_found(self, db, tmp_path):
        engine, _ = await _setup(db, tmp_path)
        with pytest.raises(PlanNotFound):
            await decline_plan(db=db, engine=engine, plan_id="plan-missing")


def _hold_callers_in_get_task(db, monkeypatch, callers: int = 2) -> None:
    """Make every caller wait inside ``get_task`` until ``callers`` of them
    have passed the pending check, forcing the check-then-act interleaving."""
    real_get_task = db.get_task
    arrived = 0
    all_arrived = asyncio.Event()

    async def held_get_task(task_id):
        nonlocal arrived
        arrived += 1
        if arrived >= callers:
            all_arrived.set()
        await asyncio.wait_for(all_arrived.wait(), timeout=5.0)
        return await real_get_task(task_id)

    monkeypatch.setattr(db, "get_task", held_get_task)


@pytest.mark.asyncio
class TestConcurrentReview:
    async def test_double_approve_spawns_one_session(self, db, tmp_path, monkeypatch):
        engine, _ = await _setup(db, tmp_path)
        _hold_callers_in_get_task(db, monkeypatch)

        results = await asyncio.gather(
            approve_plan(db=db, engine=engine, plan_id="plan-act", actor=APPROVER),
            approve_plan(db=db, engine=engine, plan_id="plan-act", actor=APPROVER),
            return_exceptions=True,
        )
        wins = [r for r in results if isinstance(r, dict)]
        assert len(wins) == 1
        assert sum(isinstance(r, PlanNotPending) for r in results) == 1

        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)
        assert len(engine.sessions.calls) == 1
        assert len(engine.runs) == 1
        plan = await db.get_plan("plan-act")
        assert plan["impl_session_id"] == wins[0]["impl_session_id"]

    async def test_approve_racing_decline_has_one_winner(self, db, tmp_path, monkeypatch):
        engine, task_id = await _setup(db, tmp_path)
        _hold_callers_in_get_task(db, monkeypatch)

        approved, declined = await asyncio.gather(
            approve_plan(db=db, engine=engine, plan_id="plan-act", actor=APPROVER),
            decline_plan(db=db, engine=engine, plan_id="plan-act"),
            return_exceptions=True,
        )
        assert sum(isinstance(r, PlanNotPending) for r in (approved, declined)) == 1

        plan = await db.get_plan("plan-act")
        task = await db.get_task(task_id)
        if isinstance(approved, dict):
            assert (plan["status"], task["status"]) == ("implementing", "in_progress")
        else:
            assert (plan["status"], task["status"]) == ("declined", "done")
            assert engine.sessions.calls == []


@pytest.mark.asyncio
class TestFailureRecovery:
    async def test_failed_session_create_puts_plan_back_to_pending(self, db, tmp_path):
        engine, _ = await _setup(db, tmp_path)

        async def broken_get_or_create(*args, **kwargs):
            raise RuntimeError("session store unavailable")

        engine.sessions.get_or_create = broken_get_or_create
        with pytest.raises(RuntimeError):
            await approve_plan(db=db, engine=engine, plan_id="plan-act", actor=APPROVER)

        plan = await db.get_plan("plan-act")
        assert plan["status"] == "pending"
        assert plan["impl_session_id"] is None
        assert engine.runs == []

        # Once the failure clears, the same plan can be approved.
        engine.sessions = FakeSessionManager()
        result = await approve_plan(db=db, engine=engine, plan_id="plan-act", actor=APPROVER)
        plan = await db.get_plan("plan-act")
        assert plan["impl_session_id"] == result["impl_session_id"]

    async def test_failed_task_close_puts_plan_back_to_pending(
        self, db, tmp_path, monkeypatch,
    ):
        from nerve.agent.tools.handlers import tasks as task_handlers

        engine, task_id = await _setup(db, tmp_path)

        async def broken_task_done(ctx, args):
            raise RuntimeError("task store unavailable")

        monkeypatch.setattr(task_handlers, "task_done_handler", broken_task_done)
        with pytest.raises(RuntimeError):
            await decline_plan(db=db, engine=engine, plan_id="plan-act", feedback="no")

        plan = await db.get_plan("plan-act")
        assert plan["status"] == "pending"
        assert plan["feedback"] is None
        assert (await db.get_task(task_id))["status"] == "pending"


def _in_progress_by(events: list[dict]) -> list[str]:
    return [e["actor"] for e in events if e["to_status"] == "in_progress"]


@pytest.mark.asyncio
class TestAttribution:
    """Who each surface records: the implementation session's creator (an
    identity) and the label in the task's status history (a session id)."""

    async def test_session_is_the_approvers_and_the_run_is_nerves(self, db, tmp_path):
        engine, _ = await _setup(db, tmp_path)
        await approve_plan(db=db, engine=engine, plan_id="plan-act", actor=APPROVER)
        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)

        assert engine.sessions.calls[0]["actor"] is APPROVER
        assert engine.runs[0]["actor"] == db.system_actor

    async def test_status_history_records_the_caller_session(self, db, tmp_path):
        engine, task_id = await _setup(db, tmp_path)
        await approve_plan(
            db=db, engine=engine, plan_id="plan-act", actor=APPROVER,
            caller_session_id="sess-agent",
        )
        assert _in_progress_by(await db.list_task_events(task_id)) == ["sess-agent"]

    async def test_decline_status_history_defaults_to_system(self, db, tmp_path):
        engine, task_id = await _setup(db, tmp_path)
        await decline_plan(db=db, engine=engine, plan_id="plan-act")
        events = await db.list_task_events(task_id)
        assert [e["actor"] for e in events if e["to_status"] == "done"] == ["system"]

    async def test_mcp_tool_approval_is_nerves_own_work(self, db, tmp_path):
        from dataclasses import replace

        from nerve.agent.tools.handlers.plans import plan_approve_handler

        engine, task_id = await _setup(db, tmp_path)
        ctx = replace(tools_mod._legacy_ctx("sess-mcp"), db=db, engine=engine)
        await plan_approve_handler(ctx, {"plan_id": "plan-act"})

        assert engine.sessions.calls[0]["actor"] == db.system_actor
        assert _in_progress_by(await db.list_task_events(task_id)) == ["sess-mcp"]

    async def test_telegram_approval_is_unattributed(self, db, tmp_path):
        from nerve.channels.telegram import TelegramChannel

        async def noop(*args, **kwargs):
            return None

        engine, _ = await _setup(db, tmp_path)
        channel = TelegramChannel.__new__(TelegramChannel)   # only .router is used
        channel.router = SimpleNamespace(engine=engine)
        query = SimpleNamespace(answer=noop, edit_message_text=noop)

        await channel._do_plan_approve(query, "plan-act")
        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)

        assert (await db.get_plan("plan-act"))["status"] == "implementing"
        assert engine.sessions.calls[0]["actor"] is None
        assert engine.runs[0]["actor"] == db.system_actor
