"""Tests for the shared plan-revision helper (``nerve.agent.plan_service``).

Covers both the MCP ``plan_revise`` tool and the HTTP
``/api/plans/{plan_id}/revise`` route, since both surfaces delegate to
the same helper. The bug that motivated this module: the HTTP route
used to instruct the planner to call ``plan_propose`` — which now
refuses when a pending plan already exists — silently no-op'ing every
UI revise click. These tests pin the behaviour so the divergence can't
come back.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio

from nerve.agent import tools as tools_mod
from nerve.agent.plan_service import (
    PlanNotFound,
    PlanNotPending,
    TaskNotFound,
    request_plan_revision,
)
from nerve.db import Database


class FakeSessionManager:
    """Records get_or_create calls so tests can assert routing decisions."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def get_or_create(
        self,
        session_id: str,
        title: str | None = None,
        source: str = "web",
        metadata: dict | None = None,
    ) -> dict:
        self.calls.append({
            "session_id": session_id,
            "title": title,
            "source": source,
        })
        return {"id": session_id, "title": title or session_id, "source": source}


class FakeEngine:
    """Mimics AgentEngine.run + .sessions for revision dispatch tests."""

    def __init__(self) -> None:
        self.sessions = FakeSessionManager()
        self.runs: list[dict[str, Any]] = []
        # Event flipped after every run() call so tests can await
        # dispatch deterministically instead of sleeping.
        self.run_event = asyncio.Event()

    async def run(
        self,
        session_id: str,
        user_message: str,
        source: str = "web",
    ) -> None:
        self.runs.append({
            "session_id": session_id,
            "user_message": user_message,
            "source": source,
        })
        self.run_event.set()


async def _setup(db: Database, tmp_path) -> tuple[FakeEngine, str]:
    """Create a task on disk + DB and a pending plan, then wire tools."""
    task_id = "t-revise"
    file_path = "task.md"
    task_md = tmp_path / file_path
    task_md.write_text("# Demo task\n\nBody.\n", encoding="utf-8")

    await db.upsert_task(
        task_id=task_id, file_path=file_path, title="Demo task",
        status="pending", content=task_md.read_text(),
    )
    await db.create_plan(
        plan_id="plan-orig", task_id=task_id, content="v1 plan",
        session_id="sess-proposer", version=1, plan_type="generic",
    )

    engine = FakeEngine()
    tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)
    return engine, task_id


@pytest.mark.asyncio
class TestRequestPlanRevision:
    """The shared helper exposes a single behaviour contract — verify it."""

    async def test_dispatches_plan_update_prompt(self, db: Database, tmp_path):
        """The bug fix: planner is told to call plan_update, NOT plan_propose."""
        engine, _ = await _setup(db, tmp_path)

        result = await request_plan_revision(
            db=db, engine=engine,
            plan_id="plan-orig", feedback="be more specific about edge cases",
        )

        # Wait for the background dispatch task to actually call engine.run.
        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)

        assert result["plan_id"] == "plan-orig"
        assert result["task_id"] == "t-revise"
        assert result["status"] == "revision_requested"

        assert len(engine.runs) == 1
        prompt = engine.runs[0]["user_message"]
        # Crucial: plan_update with the plan_id, NOT plan_propose with the task_id.
        assert 'plan_update(plan_id="plan-orig"' in prompt
        assert "plan_propose(" not in prompt
        # Feedback is embedded verbatim.
        assert "be more specific about edge cases" in prompt

    async def test_stores_feedback_and_writes_task_note(self, db: Database, tmp_path):
        engine, task_id = await _setup(db, tmp_path)

        await request_plan_revision(
            db=db, engine=engine,
            plan_id="plan-orig", feedback="needs more tests",
        )
        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)

        # Feedback persisted on the plan; plan stays pending until the
        # planner supersedes it via plan_update.
        plan = await db.get_plan("plan-orig")
        assert plan["feedback"] == "needs more tests"
        assert plan["status"] == "pending"

        # Task markdown got a revision note appended.
        task = await db.get_task(task_id)
        task_md = tmp_path / task["file_path"]
        body = task_md.read_text(encoding="utf-8")
        assert "Revision requested for plan-orig" in body
        assert "needs more tests" in body

    async def test_routes_to_proposer_session(self, db: Database, tmp_path):
        engine, _ = await _setup(db, tmp_path)

        await request_plan_revision(
            db=db, engine=engine,
            plan_id="plan-orig", feedback="rev",
        )
        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)

        # The plan recorded session_id="sess-proposer" at creation — dispatch
        # must route the feedback there, not to the cron fallback.
        assert engine.runs[0]["session_id"] == "sess-proposer"
        assert engine.sessions.calls[0]["session_id"] == "sess-proposer"

    async def test_falls_back_to_cron_planner_when_session_unknown(
        self, db: Database, tmp_path,
    ):
        """Plans without a proposer session (older rows, manual inserts)
        should route to the cron planner so revisions don't silently drop."""
        await _setup(db, tmp_path)
        # Null out session_id to simulate a legacy plan.
        await db.update_plan("plan-orig", session_id=None)
        engine = FakeEngine()
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)

        await request_plan_revision(
            db=db, engine=engine,
            plan_id="plan-orig", feedback="rev",
        )
        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)

        assert engine.runs[0]["session_id"] == "cron:task-planner"
        # The fallback session should be created with a friendly title.
        assert engine.sessions.calls[0]["title"] == "Cron: task-planner"

    async def test_refuses_non_pending_plan(self, db: Database, tmp_path):
        engine, _ = await _setup(db, tmp_path)
        await db.update_plan("plan-orig", status="declined")

        with pytest.raises(PlanNotPending):
            await request_plan_revision(
                db=db, engine=engine,
                plan_id="plan-orig", feedback="rev",
            )
        # Nothing should have been dispatched.
        assert engine.runs == []

    async def test_refuses_superseded_plan(self, db: Database, tmp_path):
        """A plan already replaced by plan_update can't be revised again."""
        engine, _ = await _setup(db, tmp_path)
        await db.update_plan("plan-orig", status="superseded")

        with pytest.raises(PlanNotPending):
            await request_plan_revision(
                db=db, engine=engine,
                plan_id="plan-orig", feedback="rev",
            )

    async def test_raises_plan_not_found(self, db: Database, tmp_path):
        engine, _ = await _setup(db, tmp_path)

        with pytest.raises(PlanNotFound):
            await request_plan_revision(
                db=db, engine=engine,
                plan_id="plan-does-not-exist", feedback="rev",
            )
        assert engine.runs == []

    async def test_raises_task_not_found(self, db: Database, tmp_path):
        """If the underlying task was deleted out from under the plan, surface it."""
        engine, _ = await _setup(db, tmp_path)
        # Delete the task row directly — simulates an orphaned plan.
        await db.db.execute("DELETE FROM tasks WHERE id = ?", ("t-revise",))
        await db.db.commit()

        with pytest.raises(TaskNotFound):
            await request_plan_revision(
                db=db, engine=engine,
                plan_id="plan-orig", feedback="rev",
            )
        assert engine.runs == []

    async def test_empty_feedback_raises_value_error(self, db: Database, tmp_path):
        engine, _ = await _setup(db, tmp_path)

        with pytest.raises(ValueError):
            await request_plan_revision(
                db=db, engine=engine,
                plan_id="plan-orig", feedback="   ",
            )


@pytest.mark.asyncio
class TestPlanReviseTool:
    """The MCP plan_revise tool is now a thin wrapper — verify the shape
    contracts the agents depend on are still intact."""

    async def test_tool_dispatches_plan_update_prompt(self, db: Database, tmp_path):
        from nerve.agent.tools import plan_revise

        engine, _ = await _setup(db, tmp_path)

        result = await plan_revise.handler({
            "plan_id": "plan-orig",
            "feedback": "tighten the test plan",
        })
        await asyncio.wait_for(engine.run_event.wait(), timeout=1.0)

        text = result["content"][0]["text"]
        assert "Revision requested for plan-orig" in text
        assert "sess-proposer" in text  # routed to original proposer
        assert 'plan_update(plan_id="plan-orig"' in engine.runs[0]["user_message"]

    async def test_tool_returns_text_on_missing_plan(self, db: Database, tmp_path):
        from nerve.agent.tools import plan_revise

        engine, _ = await _setup(db, tmp_path)

        result = await plan_revise.handler({
            "plan_id": "plan-missing",
            "feedback": "rev",
        })
        text = result["content"][0]["text"]
        assert "Plan not found" in text
        assert engine.runs == []

    async def test_tool_returns_text_on_non_pending(self, db: Database, tmp_path):
        from nerve.agent.tools import plan_revise

        engine, _ = await _setup(db, tmp_path)
        await db.update_plan("plan-orig", status="declined")

        result = await plan_revise.handler({
            "plan_id": "plan-orig",
            "feedback": "rev",
        })
        text = result["content"][0]["text"]
        assert "declined" in text
        assert "only pending" in text
        assert engine.runs == []

    async def test_tool_rejects_empty_feedback(self, db: Database, tmp_path):
        from nerve.agent.tools import plan_revise

        engine, _ = await _setup(db, tmp_path)

        result = await plan_revise.handler({
            "plan_id": "plan-orig",
            "feedback": "   ",
        })
        text = result["content"][0]["text"]
        assert "required" in text.lower()
        assert engine.runs == []


@pytest.mark.asyncio
class TestHttpReviseRoute:
    """End-to-end check that the FastAPI route maps helper exceptions
    to the right status codes and dispatches the same plan_update prompt."""

    @pytest_asyncio.fixture
    async def app_setup(self, db: Database, tmp_path):
        """Build a minimal FastAPI app wired to a fake engine + this DB.

        We bypass auth (no jwt_secret) and use TestClient for sync HTTP.
        """
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from nerve.config import NerveConfig
        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.plans import router as plans_router

        # Auth dependency reads get_config().auth.jwt_secret — set up a
        # config with no secret so require_auth is a no-op.
        import nerve.config as cfg_mod
        cfg = NerveConfig()
        cfg.workspace = tmp_path
        cfg.auth.jwt_secret = ""
        cfg_mod._config = cfg

        engine, task_id = await _setup(db, tmp_path)
        init_deps(engine=engine, db=db)  # type: ignore[arg-type]

        app = FastAPI()
        app.include_router(plans_router)
        client = TestClient(app)

        yield SimpleNamespace(client=client, engine=engine, db=db, task_id=task_id)

        cfg_mod._config = None  # leave global state clean for other tests

    async def test_revise_returns_409_for_declined_plan(self, app_setup):
        await app_setup.db.update_plan("plan-orig", status="declined")

        resp = app_setup.client.post(
            "/api/plans/plan-orig/revise",
            json={"feedback": "should fail with 409"},
        )
        assert resp.status_code == 409
        assert "declined" in resp.json()["detail"]
        assert app_setup.engine.runs == []

    async def test_revise_returns_404_for_unknown_plan(self, app_setup):
        resp = app_setup.client.post(
            "/api/plans/plan-missing/revise",
            json={"feedback": "rev"},
        )
        assert resp.status_code == 404
        assert app_setup.engine.runs == []

    async def test_revise_returns_400_for_empty_feedback(self, app_setup):
        resp = app_setup.client.post(
            "/api/plans/plan-orig/revise",
            json={"feedback": "   "},
        )
        assert resp.status_code == 400
        assert app_setup.engine.runs == []

    async def test_revise_dispatches_plan_update_prompt(self, app_setup):
        resp = app_setup.client.post(
            "/api/plans/plan-orig/revise",
            json={"feedback": "smoke test"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["plan_id"] == "plan-orig"
        assert body["session_id"] == "sess-proposer"
        assert body["status"] == "revision_requested"

        # The dispatch happens via asyncio.create_task inside the helper
        # — TestClient runs the request synchronously but the engine.run
        # task still completes on the same event loop before TestClient
        # tears it down. Wait for the event to be safe.
        await asyncio.wait_for(app_setup.engine.run_event.wait(), timeout=1.0)

        assert len(app_setup.engine.runs) == 1
        prompt = app_setup.engine.runs[0]["user_message"]
        assert 'plan_update(plan_id="plan-orig"' in prompt
        assert "plan_propose(" not in prompt
