"""The data of the plan page: the plan summary and the implementation session.

``plan_propose`` and ``plan_update`` take an optional ``summary`` and store it
in ``plans.summary``. ``plan_update`` does not copy the summary of the version
it replaces. ``GET /api/plans/{id}`` returns the summary and, for an approved
plan, the implementation session's title and live state. A failed
implementation turn marks the plan ``failed``; a later ``task_done`` from the
same session marks it ``done``.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from nerve.agent import streaming
from nerve.agent import tools as tools_mod
from nerve.agent.engine import TurnOutcome
from nerve.agent.plan_service import approve_plan
from nerve.agent.tools.handlers.plans import (
    PLAN_PROPOSE_SPEC,
    plan_propose_handler,
    plan_read_handler,
    plan_update_handler,
)
from nerve.agent.tools.handlers.tasks import task_done_handler
from nerve.agent.tools.registry import ToolContext, ToolRegistry
from nerve.agent.tools.schemas import (
    PLAN_PROPOSE_SCHEMA,
    PLAN_SUMMARY_MAX_CHARS,
    PLAN_UPDATE_SCHEMA,
)
from nerve.db.migrations import v062_plan_summary
from nerve.gateway.routes import plans as plans_routes
from nerve.gateway.routes._deps import init_deps
from nerve.mcp_server.server import build_mcp_server
from tests import test_plan_actions as plan_tests
from tests.actor_rows import ensure_system_principal
from tests.test_mcp_server import _invoke_call_tool

TASK_ID = "t-summary"
SUMMARY = "Adds a dark mode toggle to the settings page.\nOne PR in example/repo."


@pytest_asyncio.fixture
async def db(db):  # noqa: F811 - the conftest database, with an identity
    await ensure_system_principal(db)
    return db


@pytest_asyncio.fixture
async def ctx(db, tmp_path) -> ToolContext:
    (tmp_path / "task.md").write_text("# Add dark mode\n\n## Updates\n", encoding="utf-8")
    await db.upsert_task(
        task_id=TASK_ID, file_path="task.md", title="Add dark mode",
        status="pending", content="# Add dark mode\n",
    )
    tools_mod.init_tools(workspace=tmp_path, db=db)
    return ToolContext(
        session_id="sess-planner", workspace=tmp_path, db=db, actor=db.system_actor,
    )


async def _pending(db) -> dict:
    (plan,) = [p for p in await db.get_plans_for_task(TASK_ID) if p["status"] == "pending"]
    return plan


def test_both_tools_offer_a_summary():
    for schema in (PLAN_PROPOSE_SCHEMA, PLAN_UPDATE_SCHEMA):
        prop = schema["properties"]["summary"]
        assert prop["type"] == "string"
        # No maxLength: the handler clips a longer summary, the call goes on.
        assert "maxLength" not in prop
        assert "summary" not in schema["required"]


@pytest.mark.asyncio
async def test_the_migration_adds_the_column_and_reruns(db):
    await v062_plan_summary.up(db.db)
    async with db.db.execute("PRAGMA table_info(plans)") as cursor:
        columns = [row[1] for row in await cursor.fetchall()]
    assert columns.count("summary") == 1


@pytest.mark.asyncio
class TestTools:
    async def test_propose_stores_the_summary(self, db, ctx):
        result = await plan_propose_handler(ctx, {
            "task_id": TASK_ID, "content": "# Plan\n\nStep one.", "summary": f"  {SUMMARY}  ",
        })
        assert (await _pending(db))["summary"] == SUMMARY
        assert "clipped" not in result.content[0]["text"]

        read = await plan_read_handler(ctx, {"plan_id": (await _pending(db))["id"]})
        assert f"Summary: {SUMMARY}" in read.content[0]["text"]

    async def test_propose_without_a_summary_stores_null(self, db, ctx):
        await plan_propose_handler(ctx, {"task_id": TASK_ID, "content": "Step one.", "summary": "  "})
        assert (await _pending(db))["summary"] is None

    async def test_a_long_summary_is_clipped_and_the_agent_is_told(self, db, ctx):
        # Through the MCP endpoint, which checks the arguments against the
        # input schema before the handler runs.
        registry = ToolRegistry()
        registry.register(PLAN_PROPOSE_SPEC)

        async def resolve(_rctx=None) -> ToolContext:
            return ctx

        server = build_mcp_server(registry, ctx_resolver=resolve)
        result = await _invoke_call_tool(server, "plan_propose", {
            "task_id": TASK_ID, "content": "Step one.", "summary": "word " * 200,
        })
        assert result.is_error is False
        summary = (await _pending(db))["summary"]
        assert len(summary) <= PLAN_SUMMARY_MAX_CHARS
        assert summary.endswith("…")
        assert f"clipped to {PLAN_SUMMARY_MAX_CHARS} characters" in result.content[0].text

    async def test_update_stores_its_own_summary_and_copies_none(self, db, ctx):
        await plan_propose_handler(ctx, {"task_id": TASK_ID, "content": "v1", "summary": SUMMARY})
        first = await _pending(db)

        await plan_update_handler(ctx, {"plan_id": first["id"], "content": "v2"})
        second = await _pending(db)
        assert second["summary"] is None

        await plan_update_handler(ctx, {"plan_id": second["id"], "content": "v3", "summary": "Now two PRs."})
        assert (await _pending(db))["summary"] == "Now two PRs."


class _Engine:
    """The engine reads of the route: running turns, wake-ups, background work."""

    def __init__(self) -> None:
        self.running: set[str] = set()
        self.wakeups: dict[str, dict] = {}
        self.background: set[str] = set()

    def is_session_running(self, session_id: str) -> bool:
        return session_id in self.running

    async def pending_wakeup(self, session_id: str) -> dict | None:
        return self.wakeups.get(session_id)

    def has_live_background_tasks(self, session_id: str) -> bool:
        return session_id in self.background


@pytest_asyncio.fixture
async def page(db, ctx, bypass_auth, monkeypatch):
    await plan_propose_handler(ctx, {"task_id": TASK_ID, "content": "Step one.", "summary": SUMMARY})
    engine = _Engine()
    awaiting: set[str] = set()
    monkeypatch.setattr(plans_routes, "get_awaiting_ids", lambda: awaiting)
    init_deps(engine=engine, db=db)  # type: ignore[arg-type]
    app = FastAPI()
    app.include_router(plans_routes.router)
    bypass_auth(app)
    plan = await _pending(db)
    client = TestClient(app)
    return SimpleNamespace(
        db=db, engine=engine, awaiting=awaiting, plan_id=plan["id"],
        get=lambda: client.get(f"/api/plans/{plan['id']}").json(),
    )


@pytest.mark.asyncio
class TestPlanRoute:
    async def test_a_pending_plan_has_its_summary_and_no_session(self, page):
        body = page.get()
        assert body["summary"] == SUMMARY
        assert body["impl_session"] is None

    async def test_an_approved_plan_has_the_session_state(self, page):
        await page.db.create_session("impl-1", title="Implement: Add dark mode", actor=None)
        await page.db.update_plan(page.plan_id, status="implementing", impl_session_id="impl-1")

        def state() -> tuple:
            s = page.get()["impl_session"]
            return s["is_running"], s["awaiting_input"], s["parked"]

        session = page.get()["impl_session"]
        assert session["id"] == "impl-1"
        assert session["title"] == "Implement: Add dark mode"
        assert session["updated_at"]
        assert state() == (False, False, False)

        page.engine.running.add("impl-1")
        page.awaiting.add("impl-1")
        assert state() == (True, True, False)

        page.engine.running.clear()
        page.awaiting.clear()
        page.engine.wakeups["impl-1"] = {"fire_at": "2026-01-01T10:00:00+00:00"}
        assert state() == (False, False, True)

        page.engine.wakeups.clear()
        page.engine.background.add("impl-1")
        assert state() == (False, False, True)

    async def test_a_deleted_session_gives_no_session(self, page):
        await page.db.update_plan(page.plan_id, status="implementing", impl_session_id="impl-gone")
        assert page.get()["impl_session"] is None


async def _until(read, expected, within: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while await read() != expected:
        if loop.time() > deadline:
            pytest.fail(f"never became {expected!r}")
        await asyncio.sleep(0.005)


@pytest.mark.asyncio
class TestFailedImplementation:
    """``engine.run`` returns normally for a failed turn; its outcome counts."""

    @pytest_asyncio.fixture
    async def approved(self, db, tmp_path, monkeypatch):
        engine, task_id = await plan_tests._setup(db, tmp_path)
        events: list[tuple[str, str, str]] = []

        async def record(task, event):
            # The plan status when the broadcast goes out.
            events.append((task["id"], event, (await db.get_plan("plan-act"))["status"]))

        monkeypatch.setattr(streaming, "emit_task_event", record)

        async def approve(outcome: TurnOutcome) -> str:
            engine.turn_outcome = outcome
            result = await approve_plan(db=db, engine=engine, plan_id="plan-act", **plan_tests.BY_APPROVER)
            events.clear()  # the approval's own task write; the run has not started yet
            await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)
            return result["impl_session_id"]

        return SimpleNamespace(db=db, engine=engine, task_id=task_id, events=events, approve=approve,
                               status=lambda: _status_of(db, "plan-act"))

    async def test_a_failed_turn_marks_the_plan_failed_then_broadcasts(self, approved):
        await approved.approve(TurnOutcome("failed", "Agent error: boom"))
        await _until(approved.status, "failed")
        await _until(lambda: _len(approved.events), 1)
        assert approved.events == [(approved.task_id, "updated", "failed")]

    async def test_a_completed_turn_keeps_the_plan_implementing(self, approved):
        await approved.approve(TurnOutcome("completed"))
        await asyncio.sleep(0.05)
        assert await approved.status() == "implementing"
        assert approved.events == []

    async def test_a_plan_done_in_the_failed_turn_stays_done(self, approved):
        async def done_then_fail(session_id, *args, **kwargs):
            await approved.db.update_plan("plan-act", status="done")
            approved.engine.run_event.set()
            raise RuntimeError("crashed after task_done")

        approved.engine.run = done_then_fail
        await approved.approve(TurnOutcome())
        await asyncio.sleep(0.05)
        assert await approved.status() == "done"

    async def test_task_done_from_the_implementation_session_marks_it_done(self, approved, tmp_path):
        impl = await approved.approve(TurnOutcome("failed", "Agent error: boom"))
        await _until(approved.status, "failed")

        ctx = ToolContext(session_id=impl, workspace=tmp_path, db=approved.db, actor=approved.db.system_actor)
        await task_done_handler(ctx, {"task_id": approved.task_id, "note": "Finished on the second try."})
        assert await approved.status() == "done"

    async def test_task_done_from_another_session_keeps_it_failed(self, approved, tmp_path):
        await approved.approve(TurnOutcome("failed", "Agent error: boom"))
        await _until(approved.status, "failed")

        ctx = ToolContext(session_id="web", workspace=tmp_path, db=approved.db, actor=approved.db.system_actor)
        await task_done_handler(ctx, {"task_id": approved.task_id, "note": ""})
        assert await approved.status() == "failed"


async def _status_of(db, plan_id: str) -> str:
    return (await db.get_plan(plan_id))["status"]


async def _len(items: list) -> int:
    return len(items)
