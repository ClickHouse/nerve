"""A pending plan moves its task to ``plan_review``, and task notes link the plan.

``plan_propose`` and ``plan_update`` move a ``pending``, ``backlog`` or
``in_progress`` task to the built-in ``plan_review`` status and never move a
task out of a custom status. Approve moves the task to ``in_progress``, a
revision request keeps ``plan_review``, and decline closes the task. The task
notes link to the plan and to the implementation session.
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from nerve.agent import tools as tools_mod
from nerve.agent.plan_service import approve_plan, decline_plan, request_plan_revision
from nerve.agent.tools.handlers.plans import plan_propose_handler, plan_update_handler
from nerve.agent.tools.registry import ToolContext
from tests.actor_rows import ensure_system_principal
from tests.test_plan_actions import FakeEngine

TASK_ID = "t-review"


@pytest_asyncio.fixture
async def db(db):  # noqa: F811 - the conftest database, with an identity
    await ensure_system_principal(db)
    return db


async def _task(db, tmp_path, status: str = "pending") -> None:
    path = tmp_path / "task.md"
    path.write_text("# Fix the encoder\n\nBody.\n\n## Updates\n", encoding="utf-8")
    await db.upsert_task(
        task_id=TASK_ID, file_path="task.md", title="Fix the encoder",
        status=status, content=path.read_text(),
    )


def _ctx(db, tmp_path, service=None) -> ToolContext:
    tools_mod.init_tools(workspace=tmp_path, db=db)
    return ToolContext(
        session_id="sess-planner", workspace=tmp_path, db=db,
        notification_service=service, actor=db.system_actor,
    )


async def _propose(ctx) -> tuple[str, str]:
    result = await plan_propose_handler(ctx, {"task_id": TASK_ID, "content": "Step one."})
    (plan,) = [p for p in await ctx.db.get_plans_for_task(TASK_ID) if p["status"] == "pending"]
    return plan["id"], result.content[0]["text"]


async def _status(db) -> str:
    return (await db.get_task(TASK_ID))["status"]


def _notes(tmp_path) -> str:
    return (tmp_path / "task.md").read_text(encoding="utf-8")


@pytest.mark.asyncio
class TestTaskStatus:
    @pytest.mark.parametrize("start", ["pending", "backlog", "in_progress"])
    async def test_propose_moves_the_task_to_plan_review(self, db, tmp_path, start):
        await _task(db, tmp_path, status=start)
        plan_id, text = await _propose(_ctx(db, tmp_path))
        assert await _status(db) == "plan_review"
        assert "Task moved to plan_review." in text
        assert f"Plan proposed: [{plan_id}](/plans/{plan_id}) (v1)" in _notes(tmp_path)

    async def test_propose_keeps_a_custom_status(self, db, tmp_path):
        await db.create_task_status(name="waiting_review", label="Waiting for review")
        await _task(db, tmp_path, status="waiting_review")
        _, text = await _propose(_ctx(db, tmp_path))
        assert await _status(db) == "waiting_review"
        assert "Task moved" not in text

    async def test_propose_keeps_a_status_set_after_the_read(self, db, tmp_path, monkeypatch):
        await _task(db, tmp_path)
        await db.create_task_status(name="blocked", label="Blocked")
        ctx = _ctx(db, tmp_path)
        real_get_task = db.get_task
        reads = 0

        async def get_task_then_change(task_id):
            # The first read of the move returns the pending row; another
            # writer then moves the task to a custom status.
            nonlocal reads
            row = await real_get_task(task_id)
            reads += 1
            if reads == 2:
                await db.transition_task(TASK_ID, "blocked", actor="other")
            return row

        monkeypatch.setattr(db, "get_task", get_task_then_change)
        await _propose(ctx)
        monkeypatch.setattr(db, "get_task", real_get_task)
        assert await _status(db) == "blocked"

    async def test_update_moves_the_task_and_links_both_versions(self, db, tmp_path):
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        old_id, _ = await _propose(ctx)
        await db.transition_task(TASK_ID, "in_progress", actor="test")

        result = await plan_update_handler(
            ctx, {"plan_id": old_id, "content": "Step two.", "feedback": "smaller"},
        )
        (new,) = [p for p in await db.get_plans_for_task(TASK_ID) if p["status"] == "pending"]
        assert await _status(db) == "plan_review"
        assert "Task moved to plan_review." in result.content[0]["text"]
        assert (
            f"Plan updated: [{old_id}](/plans/{old_id}) → "
            f"[{new['id']}](/plans/{new['id']}) (v2): smaller"
        ) in _notes(tmp_path)

    async def test_approve_moves_the_task_to_in_progress_with_a_session_link(
        self, db, tmp_path,
    ):
        await _task(db, tmp_path)
        plan_id, _ = await _propose(_ctx(db, tmp_path))
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)

        result = await approve_plan(
            db=db, engine=engine, plan_id=plan_id, actor=None, cause=None,
        )
        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)

        impl = result["impl_session_id"]
        assert await _status(db) == "in_progress"
        assert (
            f"Plan [{plan_id}](/plans/{plan_id}) approved. Implementation started "
            f"in session [{impl}](/chat/{impl})."
        ) in _notes(tmp_path)

    async def test_revision_request_keeps_plan_review(self, db, tmp_path):
        await _task(db, tmp_path)
        plan_id, _ = await _propose(_ctx(db, tmp_path))
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)

        await request_plan_revision(
            db=db, engine=engine, plan_id=plan_id, feedback="Add a test.",
            actor=None, cause=None,
        )
        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)

        assert await _status(db) == "plan_review"
        assert f"Revision requested for [{plan_id}](/plans/{plan_id}): Add a test." in (
            _notes(tmp_path)
        )

    async def test_decline_closes_the_task(self, db, tmp_path):
        await _task(db, tmp_path)
        plan_id, _ = await _propose(_ctx(db, tmp_path))
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)

        await decline_plan(
            db=db, engine=engine, plan_id=plan_id, feedback="Not now.",
            actor=None, cause=None,
        )

        task = await db.get_task(TASK_ID)
        assert task["status"] == "done"
        notes = (tmp_path / task["file_path"]).read_text(encoding="utf-8")
        assert f"Plan [{plan_id}](/plans/{plan_id}) declined: Not now." in notes
