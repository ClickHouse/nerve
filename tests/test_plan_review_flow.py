"""A pending plan moves its task to ``plan_review`` and sends one linked notification.

``plan_propose`` and ``plan_update`` move a ``pending``, ``backlog`` or
``in_progress`` task to the built-in ``plan_review`` status and never move a
task out of a custom status. Approve moves the task to ``in_progress``, a
revision request keeps ``plan_review``, and decline closes the task. Each
pending plan sends one notification that names the plan and the task, and the
task notes link to the plan and to the implementation session.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve.agent import tools as tools_mod
from nerve.agent.plan_service import approve_plan, decline_plan, request_plan_revision
from nerve.agent.tools.handlers.plans import plan_propose_handler, plan_update_handler
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig, NotificationsConfig
from nerve.notifications.service import NotificationService
from tests.actor_rows import ensure_system_principal
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
