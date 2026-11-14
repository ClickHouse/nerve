"""A pending plan moves its task to ``plan_review`` and sends one linked notification.

``plan_propose`` and ``plan_update`` move a ``pending``, ``backlog`` or
``in_progress`` task to the built-in ``plan_review`` status and never move a
task out of a custom status. Approve moves the task to ``in_progress``, a
revision request keeps ``plan_review``, and decline closes the task. Closing
the task closes the plan that waits for review, undecided, and reopening the
task puts it back. Each pending plan sends one notification that names the
plan and the task, and the task notes link to the plan and to the
implementation session.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve.agent import tools as tools_mod
from nerve.agent.plan_service import (
    PlanNotPending,
    approve_plan,
    decline_plan,
    record_plan_approval,
    request_plan_revision,
)
from nerve.agent.tools.handlers import plans as plan_handlers
from nerve.agent.tools.handlers.plans import plan_propose_handler, plan_update_handler
from nerve.agent.tools.handlers import tasks as task_handlers
from nerve.agent.tools.handlers.plans import plan_approve_handler
from nerve.agent.tools.handlers.tasks import (
    task_comment_handler,
    task_done_handler,
    task_update_handler,
)
from nerve.identity import ACTOR_KIND_HUMAN, Actor
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig, NotificationsConfig
from nerve.notifications.service import NotificationService
from tests.actor_rows import ensure_actor_row, ensure_system_principal
from tests.conftest import TEST_ACTOR
from tests.test_plan_actions import FakeEngine

TASK_ID = "t-review"


@pytest_asyncio.fixture
async def db(db):  # noqa: F811 - the conftest database, with an identity
    await ensure_system_principal(db)
    return db


@pytest.fixture
def broadcasts(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    captured: list[dict] = []

    class _FakeBroadcaster:
        async def broadcast(self, _channel: str, message: dict) -> None:
            captured.append(message)

    from nerve.agent import streaming
    monkeypatch.setattr(streaming, "broadcaster", _FakeBroadcaster())
    return captured


def _service(db, tmp_path, *, channels=("web",), engine=None) -> NotificationService:
    config = NerveConfig()
    config.workspace = tmp_path
    config.notifications = NotificationsConfig(
        channels=list(channels), telegram_chat_id=1001,
    )
    if engine is None:
        engine = MagicMock()
        engine.router.get_channel.return_value = None
    return NotificationService(config, db, engine)


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


async def _plan_notifications(db) -> list[dict]:
    return [
        n for n in await db.list_notifications()
        if n["title"].startswith("Plan ready for review")
    ]


@pytest.mark.asyncio
async def test_the_skill_extractors_plan_is_a_skill_proposal_either_way(db, tmp_path):
    """The built-in job states the plan type and creates its task with its
    source. Each alone makes the plan a ``skill-create`` plan, whose approval
    briefs a session to write the skill; a plan with neither is generic."""
    await _task(db, tmp_path)
    ctx = _ctx(db, tmp_path)

    async def proposed_as(**args) -> str:
        await plan_propose_handler(ctx, {"task_id": TASK_ID, "content": "Why and where to.", **args})
        (plan,) = [p for p in await db.get_plans_for_task(TASK_ID) if p["status"] == "pending"]
        await db.update_plan(plan["id"], status="declined")
        return plan["plan_type"]

    assert await proposed_as() == "generic"
    assert await proposed_as(plan_type="skill-create") == "skill-create"

    await db.upsert_task(
        task_id=TASK_ID, file_path="task.md", title="Fix the encoder", status="pending",
        content=(tmp_path / "task.md").read_text(), source="skill-extractor",
    )
    assert await proposed_as() == "skill-create"


@pytest.mark.asyncio
async def test_the_tag_lists_every_earlier_skill_proposal_whatever_became_of_it(db, tmp_path):
    """The skill extractor is told to propose nothing that it proposed before
    (its prompt, ``nerve.setup_writer``). It tags the task of each proposal
    and lists the tasks by the tag. The task stays when the plan is declined,
    and when the plan is approved and its session closes the task with no
    skill: a plan that ``plan_list`` shows neither by default nor as
    declined."""
    import re

    from nerve.agent.tools.handlers.plans import plan_list_handler
    from nerve.agent.tools.handlers.tasks import (
        task_create_handler, task_done_handler, task_list_handler,
    )

    engine = FakeEngine(db)
    tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)
    ctx = ToolContext(
        session_id="cron:skill-extractor", workspace=tmp_path, db=db,
        notification_service=None, actor=db.system_actor,
    )

    def text(result) -> str:
        return result.content[0]["text"]

    async def proposed(title: str) -> tuple[str, str]:
        created = await task_create_handler(ctx, {
            "title": title, "content": "What the skill is for.",
            "source": "skill-extractor", "tags": "skill-proposal",
        })
        task_id = re.match(r"Task created: (\S+)", text(created)).group(1)
        await plan_propose_handler(ctx, {
            "task_id": task_id, "content": "What, evidence, direction.", "plan_type": "skill-create",
        })
        (plan,) = [p for p in await db.get_plans_for_task(task_id) if p["status"] == "pending"]
        return task_id, plan["id"]

    in_review = "Release notes from merged pull requests"
    declined = "Weekly audit of dependencies"
    closed = "Triage of flaky tests"
    await proposed(in_review)
    _, plan_id = await proposed(declined)
    await decline_plan(db=db, engine=engine, plan_id=plan_id, actor=None, cause=None)
    task_id, plan_id = await proposed(closed)
    await approve_plan(db=db, engine=engine, plan_id=plan_id, actor=None, cause=None)
    await asyncio.wait_for(engine.run_event.wait(), timeout=5.0)
    await task_done_handler(ctx, {"task_id": task_id, "note": "No skill: the evidence does not support one."})
    assert (await db.get_plan(plan_id))["status"] == "done"
    await task_create_handler(ctx, {"title": "Fix the encoder", "content": "Body."})

    listed = text(await task_list_handler(ctx, {"status": "all", "tag": "skill-proposal"}))
    assert all(title in listed for title in (in_review, declined, closed))
    assert "Fix the encoder" not in listed
    # Without the status, the tasks that are done are left out.
    still_open = text(await task_list_handler(ctx, {"tag": "skill-proposal"}))
    assert in_review in still_open and declined not in still_open and closed not in still_open

    shown = text(await plan_list_handler(ctx, {}))
    assert in_review in shown and declined not in shown and closed not in shown
    shown = text(await plan_list_handler(ctx, {"status": "declined"}))
    assert declined in shown and closed not in shown


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


async def _close(ctx, how: str = "task_done", **args):
    """Close the task with the tool, or as the board and the task page do."""
    if how == "task_done":
        return await task_done_handler(ctx, {"task_id": TASK_ID, **args})
    return await task_update_handler(ctx, {"task_id": TASK_ID, "status": "done", **args})


async def _reopen(ctx, status: str = "pending", **args):
    """Reopen the task: ``task_update`` with a status that is not done."""
    return await task_update_handler(ctx, {"task_id": TASK_ID, "status": status, **args})


async def _person(db, tmp_path) -> ToolContext:
    """The context of a route that a person calls, as the board and the task
    page do: the person is the actor and the cause."""
    await ensure_actor_row(db, TEST_ACTOR)
    tools_mod.init_tools(workspace=tmp_path, db=db)
    return ToolContext(
        session_id="web", workspace=tmp_path, db=db, notification_service=None,
        actor=TEST_ACTOR, cause=TEST_ACTOR,
    )


async def _plan_state(db, plan_id: str) -> tuple:
    plan = await db.get_plan(plan_id)
    return plan["status"], plan["feedback"], plan["reviewed_at"], plan["reviewed_by_actor_id"]


CLOSES = pytest.mark.parametrize("how", ["task_done", "task_update"])


@pytest.mark.asyncio
class TestClosingTheTask:
    """A plan waits for review until somebody decides it. Closing its task
    takes it out of review without a decision: whoever closes the task wants
    no plan for it now, or did the work without one. The plan is ``closed``,
    and it is in review again when the task is reopened."""

    @CLOSES
    async def test_closing_the_task_closes_the_plan_in_review(self, db, tmp_path, how):
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)

        result = await _close(ctx, how, note="Not needed.")

        assert await _status(db) == "done"
        # Nobody decided the plan: no reviewer, no time of a review, no reason.
        assert await _plan_state(db, plan_id) == ("closed", None, None, None)
        text = result.content[0]["text"]
        assert text.startswith(f"Task {TASK_ID} marked as done.")
        assert plan_id in text and "closed with it" in text and "implement_here=true" in text
        assert "when the user reopens the task" in text
        notes = (tmp_path / (await db.get_task(TASK_ID))["file_path"]).read_text(encoding="utf-8")
        assert "DONE — Not needed." in notes
        assert (
            f"Plan [{plan_id}](/plans/{plan_id}) waited for review and was closed with the task."
        ) in notes

    @pytest.mark.parametrize("start", ["pending", "in_progress", "backlog", "waiting"])
    async def test_the_plan_is_closed_whatever_status_the_task_had(self, db, tmp_path, start):
        """A task that somebody moved on after the proposal, a custom status
        included, is closed like one in ``plan_review``."""
        if start == "waiting":
            await db.create_task_status(name="waiting", label="Waiting")
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        await db.transition_task(TASK_ID, start, actor="test")

        await _close(ctx)

        assert (await db.get_plan(plan_id))["status"] == "closed"

    async def test_a_close_with_a_revision_that_holds_closes_the_plan(self, db, tmp_path):
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        revision = (await db.get_task(TASK_ID))["revision"]

        result = await _close(ctx, expect_revision=revision)

        assert not result.is_error
        assert (await db.get_plan(plan_id))["status"] == "closed"

    async def test_a_task_without_its_file_closes_its_plan_too(self, db, tmp_path):
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        (tmp_path / "task.md").unlink()

        await _close(ctx)

        assert await _status(db) == "done"
        assert (await db.get_plan(plan_id))["status"] == "closed"

    async def test_a_close_that_is_refused_closes_no_plan(self, db, tmp_path):
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        stale = (await db.get_task(TASK_ID))["revision"] - 1

        result = await _close(ctx, expect_revision=stale)

        assert result.is_error
        assert await _status(db) == "plan_review"
        assert (await db.get_plan(plan_id))["status"] == "pending"

    async def test_a_task_without_a_plan_in_review_closes_as_before(self, db, tmp_path):
        await _task(db, tmp_path)

        result = await _close(_ctx(db, tmp_path))

        assert result.content[0]["text"] == f"Task {TASK_ID} marked as done."
        assert "Plan " not in _notes_of_done(tmp_path)

    async def test_every_plan_in_review_is_closed_and_named(self, db, tmp_path):
        """The tools keep one plan in review per task. Two calls at the same
        moment can leave two, and both go with the task."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        first, _ = await _propose(ctx)
        await db.create_plan(plan_id="plan-twin", task_id=TASK_ID, content="Same.", session_id="s")

        result = await _close(ctx)

        assert (await db.get_plan(first))["status"] == "closed"
        assert (await db.get_plan("plan-twin"))["status"] == "closed"
        text = result.content[0]["text"]
        assert first in text and "plan-twin" in text
        assert result.structured == {"plans_closed": [first, "plan-twin"]} or (
            result.structured == {"plans_closed": ["plan-twin", first]}
        )
        notes = _notes_of_done(tmp_path)
        assert f"[{first}](/plans/{first})" in notes and "[plan-twin](/plans/plan-twin)" in notes

    @pytest.mark.parametrize("stored", ["2999-01-01T00:00:00+00:00", "not a time", None])
    async def test_a_close_takes_every_plan_it_finds_in_review(self, db, tmp_path, stored):
        """The call that closes the task read the plan as in review, so the
        plan goes with it, whatever its row says about when it was proposed:
        a clock that was set back, a time that does not parse."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        await db.db.execute("UPDATE plans SET created_at = ? WHERE id = ?", (stored, plan_id))
        await db.db.commit()

        await _close(ctx)

        assert (await db.get_plan(plan_id))["status"] == "closed"

    async def test_a_note_that_cannot_be_written_does_not_undo_the_close(self, db, tmp_path, monkeypatch):
        from nerve.agent.tools.handlers import tasks as task_handlers

        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)

        async def broken_task_update(ctx, args):
            raise OSError("no space left on device")

        monkeypatch.setattr(task_handlers, "task_update_handler", broken_task_update)
        result = await task_done_handler(ctx, {"task_id": TASK_ID})

        assert not result.is_error and plan_id in result.content[0]["text"]
        assert await _status(db) == "done"
        assert (await db.get_plan(plan_id))["status"] == "closed"

    async def test_the_plan_notification_is_dismissed_by_whoever_closed(self, db, tmp_path, broadcasts):
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path, _service(db, tmp_path))
        await _propose(ctx)
        (before,) = await _plan_notifications(db)
        assert before["status"] == "pending"

        await _close(ctx)

        (after,) = await _plan_notifications(db)
        assert after["status"] == "dismissed"
        assert after["dismissed_by_actor_id"] == ctx.attribution.actor_id

    async def test_a_revision_request_is_kept_as_it_was(self, db, tmp_path):
        """The close decides nothing, so it changes nothing that a reviewer
        wrote: the request and who made it are there when the plan is back."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)
        await request_plan_revision(
            db=db, engine=engine, plan_id=plan_id, feedback="Add a test.", actor=None, cause=None,
        )
        before = await _plan_state(db, plan_id)

        await _close(ctx)

        assert await _plan_state(db, plan_id) == ("closed", *before[1:])
        assert before[1] == "Add a test."

    @pytest.mark.parametrize("status, after", [
        ("declined", "declined"), ("failed", "failed"), ("superseded", "superseded"),
        ("done", "done"), ("closed", "closed"), ("implementing", "done"),
    ])
    async def test_a_plan_that_is_not_in_review_is_as_before(self, db, tmp_path, status, after):
        """Only an implementing plan changes, to done, as it always did."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path, _service(db, tmp_path))
        plan_id, _ = await _propose(ctx)
        await db.update_plan(plan_id, status=status, feedback="Its own reason.")

        result = await _close(ctx)

        plan = await db.get_plan(plan_id)
        assert (plan["status"], plan["feedback"]) == (after, "Its own reason.")
        assert result.content[0]["text"] == f"Task {TASK_ID} marked as done."
        # No plan was closed, so no notification was dismissed.
        (notification,) = await _plan_notifications(db)
        assert notification["status"] == "pending"

    @CLOSES
    async def test_a_plan_proposed_for_a_task_that_is_done_waits_for_a_decision(
        self, db, tmp_path, how, broadcasts,
    ):
        """The plan tools leave a done task done. Closing it once more closes
        nothing that was proposed since the task was closed."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path, _service(db, tmp_path))
        await _close(ctx)
        plan_id, _ = await _propose(ctx)
        assert await _status(db) == "done"

        result = await _close(ctx, how)

        assert (await db.get_plan(plan_id))["status"] == "pending"
        assert result.content[0]["text"] == f"Task {TASK_ID} marked as done."
        (notification,) = await _plan_notifications(db)
        assert notification["status"] == "pending"

    async def test_closing_again_closes_a_plan_that_an_earlier_close_left(self, db, tmp_path):
        """A close whose plan write failed, or one made by a release from
        before this rule, leaves a done task with a plan in review that is
        older than the close. Closing the task once more repairs it."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        await _close(ctx)
        await db.update_plan(plan_id, status="pending")  # as the earlier close left it

        await _close(ctx)

        assert (await db.get_plan(plan_id))["status"] == "closed"

    async def test_a_plan_proposed_between_two_closes_goes_with_the_second(self, db, tmp_path):
        """The close that counts is the task's latest: a plan that was
        proposed after an earlier close, for the task as it was reopened,
        goes with the next one."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        await _close(ctx)
        await _reopen(ctx)
        plan_id, _ = await _propose(ctx)

        await _close(ctx)

        assert (await db.get_plan(plan_id))["status"] == "closed"

    @pytest.mark.parametrize("proposed, after", [
        ("at the close", "closed"), ("in the older stored form", "closed"),
        ("after the close", "pending"), ("at no readable time", "pending"),
    ])
    async def test_closing_again_goes_by_when_the_plan_was_proposed(self, db, tmp_path, proposed, after):
        """A plan row holds the time as ISO 8601 with its zone; one written
        by the table's default holds SQLite's own text, with none. A plan
        that was proposed in the instant of the close went with it. One
        whose time says nothing is taken as proposed since, and stays."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        await _close(ctx)
        stored = {
            "at the close": await db.get_task_closed_at(TASK_ID),
            "in the older stored form": "2026-01-02 03:04:05",
            "after the close": "2999-01-01T00:00:00+00:00",
            "at no readable time": "not a time",
        }[proposed]
        await db.db.execute(
            "UPDATE plans SET status = 'pending', created_at = ? WHERE id = ?", (stored, plan_id),
        )
        await db.db.commit()

        await _close(ctx)

        assert (await db.get_plan(plan_id))["status"] == after

    async def test_a_repair_goes_by_the_latest_close_of_the_task(self, db, tmp_path):
        """The task was closed, reopened and closed again, and the plan was
        proposed in between. It is older than the latest close, though not
        than the first, so closing once more takes it."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        await _close(ctx)
        await _reopen(ctx)
        plan_id, _ = await _propose(ctx)
        await _close(ctx)
        await db.update_plan(plan_id, status="pending")  # as a close that failed left it

        await _close(ctx)

        assert (await db.get_plan(plan_id))["status"] == "closed"

    async def test_the_last_close_is_the_one_written_last_whatever_the_clock_said(self, db, tmp_path):
        """The clock was ahead at the first close and was set back before the
        second. A plan that is proposed for the done task after the second
        close is newer than the task's last close: closing once more leaves
        it."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        await _close(ctx)
        (first,) = [e for e in await db.list_task_events(TASK_ID) if e["to_status"] == "done"]
        await db.db.execute(
            "UPDATE task_events SET created_at = ? WHERE id = ?",
            ("2999-01-01T00:00:00+00:00", first["id"]),
        )
        await db.db.commit()
        await _reopen(ctx)
        await _close(ctx)
        plan_id, _ = await _propose(ctx)

        await _close(ctx)

        assert (await db.get_plan(plan_id))["status"] == "pending"

    async def test_a_task_that_no_event_closed_closes_no_plan(self, db, tmp_path):
        """With no record of when the task was closed, nothing says that the
        plan is older than the close: it stays in review."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        await db.db.execute("UPDATE tasks SET status = 'done' WHERE id = ?", (TASK_ID,))
        await db.db.commit()
        assert await db.get_task_closed_at(TASK_ID) is None

        await _close(ctx)

        assert (await db.get_plan(plan_id))["status"] == "pending"

    async def test_an_approval_at_the_same_moment_stands(self, db, tmp_path, monkeypatch, broadcasts):
        """The close reads the plan as pending, and an approval claims it
        before the close writes: the close changes nothing of the plan."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path, _service(db, tmp_path))
        plan_id, _ = await _propose(ctx)
        real_plans = db.get_plans_for_task

        async def then_it_is_approved(task_id):
            plans = await real_plans(task_id)
            await db.update_plan(plan_id, status="implementing", impl_session_id="impl-other")
            return plans

        monkeypatch.setattr(db, "get_plans_for_task", then_it_is_approved)
        result = await _close(ctx)
        monkeypatch.setattr(db, "get_plans_for_task", real_plans)

        assert (await db.get_plan(plan_id))["status"] == "implementing"
        assert result.content[0]["text"] == f"Task {TASK_ID} marked as done."
        (notification,) = await _plan_notifications(db)
        assert notification["status"] == "pending"

    async def test_a_declined_plan_keeps_the_reason_it_was_declined_for(self, db, tmp_path):
        """Declining closes the task. The plan was decided before the close,
        and the close adds nothing to it."""
        await _task(db, tmp_path)
        plan_id, _ = await _propose(_ctx(db, tmp_path))
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)

        await decline_plan(
            db=db, engine=engine, plan_id=plan_id, feedback="Not now.", actor=None, cause=None,
        )

        plan = await db.get_plan(plan_id)
        assert await _status(db) == "done"
        assert (plan["status"], plan["feedback"]) == ("declined", "Not now.")


def _notes_of_done(tmp_path) -> str:
    """The task file after the close, which moved it into done/."""
    (path,) = list(tmp_path.rglob("done/task.md"))
    return path.read_text(encoding="utf-8")


@pytest.mark.asyncio
class TestReopeningTheTask:
    """The close decided nothing, so a person who reopens the task has the
    plan back: a card dragged into the Done lane and back is as it was, plan
    in review. An agent that reopens a task does not put a plan in front of
    the person again; it is told that the plan stays closed."""

    @pytest.mark.parametrize("status", ["pending", "plan_review", "in_progress"])
    async def test_a_person_who_reopens_the_task_has_its_plan_in_review_again(
        self, db, tmp_path, status,
    ):
        await _task(db, tmp_path)
        agent, person = _ctx(db, tmp_path), await _person(db, tmp_path)
        plan_id, _ = await _propose(agent)
        before = await _plan_state(db, plan_id)
        await _close(person)

        result = await _reopen(person, status)

        assert await _status(db) == status
        assert await _plan_state(db, plan_id) == before
        text = result.content[0]["text"]
        assert plan_id in text and "waits for review again" in text
        notes = (tmp_path / (await db.get_task(TASK_ID))["file_path"]).read_text(encoding="utf-8")
        assert (
            f"Plan [{plan_id}](/plans/{plan_id}) is in review again: it was closed with the task."
        ) in notes

    @pytest.mark.parametrize("closed_by", ["the agent", "a person"])
    async def test_an_agent_that_reopens_the_task_leaves_the_plan_closed(self, db, tmp_path, closed_by):
        """Whoever closed it. The agent is told how the plan comes back."""
        await _task(db, tmp_path)
        agent, person = _ctx(db, tmp_path), await _person(db, tmp_path)
        plan_id, _ = await _propose(agent)
        await _close(agent if closed_by == "the agent" else person)

        result = await _reopen(agent)

        assert await _status(db) == "pending"
        assert (await db.get_plan(plan_id))["status"] == "closed"
        text = result.content[0]["text"]
        assert plan_id in text and "stays closed" in text and "plan_propose" in text
        notes = (tmp_path / (await db.get_task(TASK_ID))["file_path"]).read_text(encoding="utf-8")
        assert "is in review again" not in notes

    async def test_a_reopen_with_no_actor_leaves_the_plan_closed(self, db, tmp_path):
        """The plan service runs the task handlers with no actor for a chat
        channel that names nobody: nobody is not a person."""
        await _task(db, tmp_path)
        agent = _ctx(db, tmp_path)
        plan_id, _ = await _propose(agent)
        await _close(agent)
        nobody = ToolContext(session_id="system", workspace=tmp_path, db=db)

        result = await _reopen(nobody)

        assert (await db.get_plan(plan_id))["status"] == "closed"
        assert "stays closed" in result.content[0]["text"]

    async def test_an_agent_in_a_session_that_a_person_started_leaves_the_plan_closed(self, db, tmp_path):
        """The cause of a chat session is the person. The actor of its tool
        calls is still the agent."""
        await _task(db, tmp_path)
        await ensure_actor_row(db, TEST_ACTOR)
        agent = _ctx(db, tmp_path)
        plan_id, _ = await _propose(agent)
        await _close(agent)
        chat = ToolContext(
            session_id="chat-1", workspace=tmp_path, db=db, actor=db.system_actor,
            cause=TEST_ACTOR.actor_id,
        )

        await _reopen(chat)

        assert (await db.get_plan(plan_id))["status"] == "closed"

    async def test_a_person_without_a_local_account_has_the_plan_back(self, db, tmp_path):
        """Where another service signs people in, a person has no account
        here, and is a person all the same."""
        named = Actor(
            actor_id="7b1c2d3e-0000-4000-8000-0000000000aa", kind=ACTOR_KIND_HUMAN,
            account_id=None, display_name="Signed in elsewhere",
        )
        await ensure_actor_row(db, named)
        await _task(db, tmp_path)
        agent = _ctx(db, tmp_path)
        plan_id, _ = await _propose(agent)
        await _close(agent)
        person = ToolContext(session_id="web", workspace=tmp_path, db=db, actor=named, cause=named)

        await _reopen(person)

        assert (await db.get_plan(plan_id))["status"] == "pending"

    async def test_the_agent_can_propose_the_plan_again(self, db, tmp_path, broadcasts):
        await _task(db, tmp_path)
        agent = _ctx(db, tmp_path, _service(db, tmp_path))
        old_id, _ = await _propose(agent)
        await _close(agent)
        await _reopen(agent)

        new_id, text = await _propose(agent)

        assert "Awaiting human review" in text
        assert [(await db.get_plan(i))["status"] for i in (old_id, new_id)] == ["closed", "pending"]
        # The person hears of the new one; the first one's was dismissed at the close.
        waiting = [n for n in await _plan_notifications(db) if n["status"] == "pending"]
        assert [n["metadata"] and new_id in str(n["metadata"]) for n in waiting] == [True]

    async def test_a_decision_can_be_made_on_the_plan_that_came_back(self, db, tmp_path):
        await _task(db, tmp_path)
        agent, person = _ctx(db, tmp_path), await _person(db, tmp_path)
        plan_id, _ = await _propose(agent)
        await _close(person)
        await _reopen(person, "plan_review")
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)

        await approve_plan(db=db, engine=engine, plan_id=plan_id, actor=None, cause=None)
        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)

        assert (await db.get_plan(plan_id))["status"] == "implementing"
        assert await _status(db) == "in_progress"

    @pytest.mark.parametrize("became", ["pending", "declined", "implementing", "done", "closed"])
    async def test_a_plan_that_a_newer_one_replaced_stays_closed(self, db, tmp_path, became):
        """A plan was proposed for the task while it was done. It has taken
        the place of the one that was closed, whatever became of it."""
        await _task(db, tmp_path)
        agent, person = _ctx(db, tmp_path), await _person(db, tmp_path)
        old_id, _ = await _propose(agent)
        await _close(person)
        new_id, _ = await _propose(agent)
        await db.update_plan(new_id, status=became)

        result = await _reopen(person)

        assert (await db.get_plan(old_id))["status"] == "closed"
        # The newer plan comes back only if it was itself closed with the task.
        expected = "pending" if became == "closed" else became
        assert (await db.get_plan(new_id))["status"] == expected
        assert old_id not in result.content[0]["text"]

    async def test_a_plan_whose_approval_was_recorded_at_that_moment_stays_done(
        self, db, tmp_path, monkeypatch,
    ):
        """The reopen reads the plan as closed, and the record of its approval
        lands before the reopen writes: the plan is done, and stays so."""
        await _task(db, tmp_path)
        agent, person = _ctx(db, tmp_path), await _person(db, tmp_path)
        plan_id, _ = await _propose(agent)
        await _close(person)
        real_plans = db.get_plans_for_task

        async def then_the_approval_is_recorded(task_id):
            plans = await real_plans(task_id)
            await db.update_plan(plan_id, status="done")
            return plans

        monkeypatch.setattr(db, "get_plans_for_task", then_the_approval_is_recorded)
        result = await _reopen(person)
        monkeypatch.setattr(db, "get_plans_for_task", real_plans)

        assert (await db.get_plan(plan_id))["status"] == "done"
        assert plan_id not in result.content[0]["text"]

    @pytest.mark.parametrize("status", ["declined", "done", "failed", "superseded"])
    async def test_a_plan_that_was_decided_stays_decided(self, db, tmp_path, status):
        await _task(db, tmp_path)
        agent, person = _ctx(db, tmp_path), await _person(db, tmp_path)
        plan_id, _ = await _propose(agent)
        await db.update_plan(plan_id, status=status)
        await _close(person)

        result = await _reopen(person)

        assert (await db.get_plan(plan_id))["status"] == status
        assert result.content[0]["text"] == f"Task {TASK_ID} reopened (status: pending)."

    async def test_the_notification_is_not_sent_again(self, db, tmp_path, broadcasts):
        """The plan is back on the Plans page and on the task's own page,
        where the person who reopened the task is looking."""
        await _task(db, tmp_path)
        agent = _ctx(db, tmp_path, _service(db, tmp_path))
        person = await _person(db, tmp_path)
        await _propose(agent)
        await _close(person)

        await _reopen(person)

        (notification,) = await _plan_notifications(db)
        assert notification["status"] == "dismissed"

    @pytest.mark.parametrize("who", ["a person", "the agent"])
    async def test_a_reopen_with_other_edits_and_a_token_writes_them_all(self, db, tmp_path, who):
        """The caller's token guards the reopen, which writes the status and,
        when the plan comes back, a note. The other edits are fenced on the
        row as the reopen left it, however many writes that took."""
        await _task(db, tmp_path)
        agent, person = _ctx(db, tmp_path), await _person(db, tmp_path)
        plan_id, _ = await _propose(agent)
        await _close(person)
        ctx = person if who == "a person" else agent
        revision = (await db.get_task(TASK_ID))["revision"]

        result = await _reopen(ctx, "in_progress", title="A better title", expect_revision=revision)

        task = await db.get_task(TASK_ID)
        text = result.content[0]["text"]
        assert not result.is_error, text
        assert (task["status"], task["title"]) == ("in_progress", "A better title")
        # The result of the whole call still says what became of the plan.
        assert text.startswith(f"Task {TASK_ID} updated.") and plan_id in text
        assert ("waits for review again" in text) is (who == "a person")
        assert ("stays closed" in text) is (who == "the agent")

    async def test_without_a_token_the_other_edits_are_not_fenced(self, db, tmp_path, monkeypatch):
        """The revision that the reopen hands on fences the other edits only
        for a caller that sent a token."""
        await _task(db, tmp_path)
        agent, person = _ctx(db, tmp_path), await _person(db, tmp_path)
        await _propose(agent)
        await _close(person)
        real_reopen = task_handlers.task_reopen_handler

        async def reopen_then_somebody_writes(*args, **kwargs):
            reopened = await real_reopen(*args, **kwargs)
            await db.update_task_tags(TASK_ID, "moved-on")
            return reopened

        monkeypatch.setattr(task_handlers, "task_reopen_handler", reopen_then_somebody_writes)
        result = await _reopen(person, "in_progress", title="A better title")

        assert not result.is_error, result.content[0]["text"]
        assert (await db.get_task(TASK_ID))["title"] == "A better title"

    async def test_a_write_after_the_reopen_refuses_the_other_edits(self, db, tmp_path, monkeypatch):
        """The reopen wrote twice, the status and the note. A write that
        lands after it still refuses the other edits, and the refusal says
        what became of the plan."""
        await _task(db, tmp_path)
        agent, person = _ctx(db, tmp_path), await _person(db, tmp_path)
        plan_id, _ = await _propose(agent)
        await _close(person)
        token = (await db.get_task(TASK_ID))["revision"]
        real_reopen = task_handlers.task_reopen_handler

        async def reopen_then_somebody_writes(*args, **kwargs):
            reopened = await real_reopen(*args, **kwargs)
            await db.update_task_tags(TASK_ID, "moved-on")
            return reopened

        monkeypatch.setattr(task_handlers, "task_reopen_handler", reopen_then_somebody_writes)
        result = await _reopen(person, "in_progress", title="A better title", expect_revision=token)

        text = result.content[0]["text"]
        assert result.is_error and "reopened" in text
        assert plan_id in text and "waits for review again" in text
        task = await db.get_task(TASK_ID)
        assert (task["status"], task["title"]) == ("in_progress", "Fix the encoder")
        assert (await db.get_plan(plan_id))["status"] == "pending"

    @pytest.mark.parametrize("who", ["a person", "the agent"])
    async def test_a_write_during_the_reopen_refuses_the_other_edits(self, db, tmp_path, monkeypatch, who):
        """The caller read one revision. A comment lands after the reopen's
        status write and before the reopen ends. The caller has not seen it,
        so its other edits are not written: the reopen works out the
        revision that its own writes left, and does not read the row's."""
        await _task(db, tmp_path)
        agent, person = _ctx(db, tmp_path), await _person(db, tmp_path)
        await _propose(agent)
        await _close(person)
        ctx = person if who == "a person" else agent
        token = (await db.get_task(TASK_ID))["revision"]
        real_emit = task_handlers._emit_task_event
        fired: list = []

        async def emit_after_a_comment(c, task_id, event):
            if not fired and (await db.get_task(task_id))["status"] != "done":
                fired.append(1)
                await task_comment_handler(person, {"task_id": task_id, "body": "Wait: keep the title."})
            return await real_emit(c, task_id, event)

        monkeypatch.setattr(task_handlers, "_emit_task_event", emit_after_a_comment)
        result = await _reopen(ctx, "in_progress", title="Renamed", expect_revision=token)

        assert fired
        assert result.is_error, result.content[0]["text"]
        assert (await db.get_task(TASK_ID))["title"] == "Fix the encoder"

    async def test_a_stale_token_reopens_nothing_and_brings_no_plan_back(self, db, tmp_path):
        await _task(db, tmp_path)
        agent, person = _ctx(db, tmp_path), await _person(db, tmp_path)
        plan_id, _ = await _propose(agent)
        await _close(person)
        stale = (await db.get_task(TASK_ID))["revision"] - 1

        result = await _reopen(person, expect_revision=stale)

        assert result.is_error
        assert await _status(db) == "done"
        assert (await db.get_plan(plan_id))["status"] == "closed"


@pytest.mark.asyncio
class TestApprovalRecordedAfterTheClose:
    """The user approved a plan in the chat, and the session did the work and
    closed the task before it recorded the approval. The plan was closed with
    the task, undecided, and the record is still taken."""

    async def test_the_plan_of_the_done_task_becomes_done(self, db, tmp_path):
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        await _close(ctx)
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)

        result = await record_plan_approval(
            db=db, engine=engine, plan_id=plan_id, session_id="sess-planner",
            actor=db.system_actor, cause=None,
        )

        plan = await db.get_plan(plan_id)
        assert result["status"] == "done"
        assert (plan["status"], plan["impl_session_id"]) == ("done", "sess-planner")
        assert plan["reviewed_at"] is not None
        assert await _status(db) == "done"

    async def test_it_can_be_recorded_after_an_agent_reopened_the_task(self, db, tmp_path):
        """The session did the work, closed the task and reopened it for one
        more thing. Its reopen left the plan closed, on a task that is open:
        the record is taken, and the plan is being implemented."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)
        ctx.engine = engine
        await task_done_handler(ctx, {"task_id": TASK_ID, "note": "Implemented as the plan says."})
        await _reopen(ctx, "in_progress", note="One more thing.")

        result = await plan_approve_handler(ctx, {"plan_id": plan_id, "implement_here": True})

        assert "approved and linked to this session" in result.content[0]["text"]
        assert (await db.get_plan(plan_id))["status"] == "implementing"

    async def test_a_record_that_loses_to_a_close_says_to_call_again(self, db, tmp_path, monkeypatch):
        """The task is closed between the read of the plan and the write: the
        plan is closed now, and the same call again records it."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)
        real_get_task = db.get_task
        once: list = []

        async def then_the_task_is_closed(task_id):
            task = await real_get_task(task_id)
            if not once:
                once.append(1)
                monkeypatch.setattr(db, "get_task", real_get_task)
                await task_done_handler(ctx, {"task_id": task_id})
            return task

        monkeypatch.setattr(db, "get_task", then_the_task_is_closed)
        with pytest.raises(PlanNotPending, match="Call again"):
            await record_plan_approval(
                db=db, engine=engine, plan_id=plan_id, session_id="sess-planner",
                actor=db.system_actor, cause=None,
            )
        assert (await db.get_plan(plan_id))["status"] == "closed"

        recorded = await record_plan_approval(
            db=db, engine=engine, plan_id=plan_id, session_id="sess-planner",
            actor=db.system_actor, cause=None,
        )
        assert recorded["status"] == "done"

    async def test_a_record_that_loses_to_a_decision_is_refused_for_good(self, db, tmp_path, monkeypatch):
        """Only a plan that went with its task can be recorded by a call
        again. One that was declined meanwhile was decided."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)
        real_get_task = db.get_task

        async def then_the_plan_is_declined(task_id):
            task = await real_get_task(task_id)
            await db.update_plan(plan_id, status="declined")
            return task

        monkeypatch.setattr(db, "get_task", then_the_plan_is_declined)
        with pytest.raises(PlanNotPending) as refused:
            await record_plan_approval(
                db=db, engine=engine, plan_id=plan_id, session_id="sess-planner",
                actor=db.system_actor, cause=None,
            )
        assert "Call again" not in str(refused.value) and "declined" in str(refused.value)

    async def test_a_record_that_fails_leaves_the_plan_closed(self, db, tmp_path, monkeypatch):
        """The plan goes back to what it was, which is not ``pending`` here:
        put back in review, it would wait on a task that is done."""
        from nerve.agent.tools.handlers import tasks as task_handlers

        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        await _close(ctx)
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)

        async def broken_task_update(ctx, args):
            raise RuntimeError("task store unavailable")

        monkeypatch.setattr(task_handlers, "task_update_handler", broken_task_update)
        with pytest.raises(RuntimeError):
            await record_plan_approval(
                db=db, engine=engine, plan_id=plan_id, session_id="sess-planner",
                actor=db.system_actor, cause=None,
            )

        assert await _plan_state(db, plan_id) == ("closed", None, None, None)

    async def test_a_record_that_loses_to_a_reopen_says_to_call_again(self, db, tmp_path, monkeypatch):
        """A person reopens the task between the read of the plan and the
        write: the plan is in review again, and the record did not land."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        await _close(ctx)
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)
        real_get_task = db.get_task

        async def then_the_task_is_reopened(task_id):
            task = await real_get_task(task_id)
            await db.update_plan(plan_id, status="pending")
            return task

        monkeypatch.setattr(db, "get_task", then_the_task_is_reopened)
        with pytest.raises(PlanNotPending, match="Call again"):
            await record_plan_approval(
                db=db, engine=engine, plan_id=plan_id, session_id="sess-planner",
                actor=db.system_actor, cause=None,
            )
        monkeypatch.setattr(db, "get_task", real_get_task)

        assert (await db.get_plan(plan_id))["status"] == "pending"

    async def test_a_closed_plan_that_a_newer_one_replaced_is_refused(self, db, tmp_path):
        """The task was reopened with a newer plan, so the closed one did not
        come back. It is not the plan that anybody approved."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        old_id, _ = await _propose(ctx)
        await _close(ctx)
        await _propose(ctx)
        await _reopen(await _person(db, tmp_path))
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)

        with pytest.raises(PlanNotPending, match="a newer plan of the task has taken its place"):
            await record_plan_approval(
                db=db, engine=engine, plan_id=old_id, session_id="sess-planner",
                actor=db.system_actor, cause=None,
            )
        assert (await db.get_plan(old_id))["status"] == "closed"

    @pytest.mark.parametrize("decide", ["approve", "decline", "revise", "update"])
    async def test_no_other_decision_is_taken_on_a_closed_plan(self, db, tmp_path, decide):
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path)
        plan_id, _ = await _propose(ctx)
        await _close(ctx)
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)

        if decide == "update":
            result = await plan_update_handler(ctx, {"plan_id": plan_id, "content": "Again."})
            assert "'closed'" in result.content[0]["text"]
        else:
            call = {
                "approve": lambda: approve_plan(db=db, engine=engine, plan_id=plan_id, actor=None, cause=None),
                "decline": lambda: decline_plan(db=db, engine=engine, plan_id=plan_id, actor=None, cause=None),
                "revise": lambda: request_plan_revision(
                    db=db, engine=engine, plan_id=plan_id, feedback="More.", actor=None, cause=None,
                ),
            }[decide]
            with pytest.raises(PlanNotPending, match="'closed'"):
                await call()
        assert (await db.get_plan(plan_id))["status"] == "closed"


@pytest.mark.asyncio
class TestAPlanWrittenWhileItsTaskIsClosed:
    """``plan_propose`` and ``plan_update`` write the plan before they take
    the task's lock for the note. A close in between takes the new plan with
    it, and the tool must not announce a plan that is no longer in review."""

    @staticmethod
    def _closed_by_then(monkeypatch, db):
        real = plan_handlers._move_to_plan_review

        async def then_the_task_is_closed(ctx, task_id):
            moved = await real(ctx, task_id)
            await task_done_handler(ctx, {"task_id": task_id})
            return moved

        monkeypatch.setattr(plan_handlers, "_move_to_plan_review", then_the_task_is_closed)

    async def test_propose_says_so_and_sends_no_notification(self, db, tmp_path, monkeypatch, broadcasts):
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path, _service(db, tmp_path))
        self._closed_by_then(monkeypatch, db)

        result = await plan_propose_handler(ctx, {"task_id": TASK_ID, "content": "Step one."})

        (plan,) = await db.get_plans_for_task(TASK_ID)
        text = result.content[0]["text"]
        assert plan["status"] == "closed"
        assert plan["id"] in text and "closed with it" in text and "Awaiting" not in text
        assert await _plan_notifications(db) == []

    async def test_update_says_so_and_sends_no_notification(self, db, tmp_path, monkeypatch, broadcasts):
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path, _service(db, tmp_path))
        old_id, _ = await _propose(ctx)
        self._closed_by_then(monkeypatch, db)

        result = await plan_update_handler(ctx, {"plan_id": old_id, "content": "Step two."})

        (new,) = [p for p in await db.get_plans_for_task(TASK_ID) if p["id"] != old_id]
        text = result.content[0]["text"]
        assert new["status"] == "closed"
        assert new["id"] in text and "closed with it" in text
        # The notification of the first version was dismissed by the close;
        # none was sent for the second.
        assert [n["status"] for n in await _plan_notifications(db)] == ["dismissed"]

    async def test_a_plan_that_was_approved_meanwhile_is_not_called_closed(
        self, db, tmp_path, monkeypatch, broadcasts,
    ):
        """Only a close is reported that way. An approval in the same moment
        leaves the call as it always was."""
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path, _service(db, tmp_path))
        real = plan_handlers._move_to_plan_review

        async def then_the_plan_is_approved(ctx, task_id):
            moved = await real(ctx, task_id)
            (plan,) = await db.get_plans_for_task(task_id)
            await db.update_plan(plan["id"], status="implementing")
            return moved

        monkeypatch.setattr(plan_handlers, "_move_to_plan_review", then_the_plan_is_approved)
        result = await plan_propose_handler(ctx, {"task_id": TASK_ID, "content": "Step one."})

        assert "closed with it" not in result.content[0]["text"]


@pytest.mark.asyncio
class TestNotification:
    async def test_propose_sends_one_notification_that_names_the_plan(
        self, db, tmp_path, broadcasts,
    ):
        await _task(db, tmp_path)
        plan_id, text = await _propose(_ctx(db, tmp_path, _service(db, tmp_path)))

        (row,) = await db.list_notifications()
        assert row["type"] == "notify"
        assert row["title"] == "Plan ready for review: Fix the encoder"
        assert row["body"] == f"Version 1 of plan {plan_id} waits for your decision."
        assert row["priority"] == "high"
        assert json.loads(row["metadata"]) == {"plan_id": plan_id, "task_id": TASK_ID}
        (event,) = broadcasts
        assert event["metadata"] == {"plan_id": plan_id, "task_id": TASK_ID}
        assert "Do not send another notification about this plan." in text

    async def test_update_notifies_for_the_new_version(self, db, tmp_path, broadcasts):
        await _task(db, tmp_path)
        ctx = _ctx(db, tmp_path, _service(db, tmp_path))
        old_id, _ = await _propose(ctx)
        await plan_update_handler(ctx, {"plan_id": old_id, "content": "Step two."})
        (new,) = [p for p in await db.get_plans_for_task(TASK_ID) if p["status"] == "pending"]

        rows = await _plan_notifications(db)
        assert [(json.loads(r["metadata"])["plan_id"], r["status"]) for r in rows] == [
            (new["id"], "pending"), (old_id, "dismissed"),
        ]
        assert rows[0]["body"].startswith("Version 2 ")

    @pytest.mark.parametrize("decision", ["approve", "decline"])
    async def test_a_decision_dismisses_the_plan_notification(
        self, db, tmp_path, broadcasts, decision,
    ):
        await _task(db, tmp_path)
        await db.create_notification("notif-other", "s1", "notify", "Build finished")
        plan_id, _ = await _propose(_ctx(db, tmp_path, _service(db, tmp_path)))
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)

        if decision == "approve":
            await approve_plan(db=db, engine=engine, plan_id=plan_id, actor=None, cause=None)
        else:
            await decline_plan(db=db, engine=engine, plan_id=plan_id, actor=None, cause=None)

        assert [r["status"] for r in await _plan_notifications(db)] == ["dismissed"]
        assert (await db.get_notification("notif-other"))["status"] == "pending"

    async def test_quiet_hours_send_it_without_sound(
        self, db, tmp_path, broadcasts, monkeypatch,
    ):
        from nerve.notifications import quiet_hours

        await _task(db, tmp_path)
        monkeypatch.setattr(quiet_hours, "in_quiet_hours", lambda config, now=None: True)
        await _propose(_ctx(db, tmp_path, _service(db, tmp_path)))
        assert broadcasts[0]["priority"] == "high"
        assert broadcasts[0]["silent"] is True

    async def test_a_silenced_notification_keeps_the_plan(self, db, tmp_path, broadcasts):
        await _task(db, tmp_path)
        await db.create_silence("sil-1", pattern="plan ready")
        plan_id, _ = await _propose(_ctx(db, tmp_path, _service(db, tmp_path)))

        (row,) = await db.list_notifications()
        assert row["status"] == "silenced"
        assert json.loads(row["metadata"])["plan_id"] == plan_id
        assert broadcasts[0]["metadata"] == {"plan_id": plan_id, "task_id": TASK_ID}

    async def test_without_a_notification_service_the_plan_is_still_proposed(
        self, db, tmp_path,
    ):
        await _task(db, tmp_path)
        plan_id, text = await _propose(_ctx(db, tmp_path))
        assert plan_id.startswith("plan-")
        assert "notification" not in text


@pytest.mark.asyncio
class TestTelegramButton:
    @staticmethod
    def _engine(bot) -> MagicMock:
        engine = MagicMock()
        telegram = MagicMock()
        telegram._app.bot = bot
        engine.router.get_channel.return_value = telegram
        return engine

    async def test_a_plan_notification_opens_the_plan(self, db, tmp_path, broadcasts):
        bot = MagicMock()
        bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=77))
        service = _service(db, tmp_path, channels=("telegram",), engine=self._engine(bot))
        await service.send_notification(
            "s1", title="Plan ready", metadata={"plan_id": "plan-abc", "task_id": "t1"},
        )
        markup = bot.send_message.await_args.kwargs["reply_markup"]
        (button,) = [b for row in markup.inline_keyboard for b in row]
        assert (button.text, button.callback_data) == ("Open plan", "plan:view:plan-abc")

    async def test_a_plain_notification_has_no_button(self, db, tmp_path, broadcasts):
        bot = MagicMock()
        bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=78))
        service = _service(db, tmp_path, channels=("telegram",), engine=self._engine(bot))
        await service.send_notification("s1", title="Build finished")
        assert bot.send_message.await_args.kwargs["reply_markup"] is None
