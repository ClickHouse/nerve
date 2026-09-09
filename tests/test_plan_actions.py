"""Tests for the shared plan approve/decline helpers (``nerve.agent.plan_service``).

``approve_plan`` and ``decline_plan`` back three surfaces — the HTTP routes
(WebUI), the MCP ``plan_approve``/``plan_decline`` tools, and the Telegram
``/plans`` command. These pin the single behaviour contract so the surfaces
can't drift apart (the same rationale as ``test_plan_revise.py``).
"""

from __future__ import annotations

import asyncio

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


class FakeSessionManager:
    """Records get_or_create calls so tests can assert the impl session."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def get_or_create(
        self, session_id, title=None, source="web", metadata=None,
    ) -> dict:
        self.calls.append({"session_id": session_id, "title": title, "source": source})
        return {"id": session_id, "title": title or session_id, "source": source}


class FakeEngine:
    """Mimics AgentEngine.run + .sessions + .register_task for approve tests."""

    def __init__(self) -> None:
        self.sessions = FakeSessionManager()
        self.runs: list[dict] = []
        self.registered: list[str] = []
        self.run_event = asyncio.Event()

    async def run(self, session_id, user_message, source="web") -> None:
        self.runs.append(
            {"session_id": session_id, "user_message": user_message, "source": source}
        )
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

    engine = FakeEngine()
    tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)
    return engine, task_id


@pytest.mark.asyncio
class TestApprovePlan:
    async def test_spawns_impl_marks_implementing_and_moves_task(self, db, tmp_path):
        engine, task_id = await _setup(db, tmp_path)

        result = await approve_plan(db=db, engine=engine, plan_id="plan-act")
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
        await approve_plan(db=db, engine=engine, plan_id="plan-act")
        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)
        assert "skill_create" in engine.runs[0]["user_message"]

    async def test_refuses_non_pending(self, db, tmp_path):
        engine, _ = await _setup(db, tmp_path, status="implementing")
        with pytest.raises(PlanNotPending):
            await approve_plan(db=db, engine=engine, plan_id="plan-act")
        assert engine.runs == []

    async def test_raises_plan_not_found(self, db, tmp_path):
        engine, _ = await _setup(db, tmp_path)
        with pytest.raises(PlanNotFound):
            await approve_plan(db=db, engine=engine, plan_id="plan-missing")
        assert engine.runs == []

    async def test_raises_task_not_found(self, db, tmp_path):
        engine, _ = await _setup(db, tmp_path)
        await db.db.execute("DELETE FROM tasks WHERE id = ?", ("t-act",))
        await db.db.commit()
        with pytest.raises(TaskNotFound):
            await approve_plan(db=db, engine=engine, plan_id="plan-act")
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
