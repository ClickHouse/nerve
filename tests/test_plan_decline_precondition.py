"""The HTTP decline surface must refuse a non-pending plan, like its siblings.

``PATCH /api/plans/{plan_id}`` used to write a caller-supplied status with no
precondition of any kind, while all four sibling surfaces (tool
plan_decline/plan_approve/plan_update and ``POST .../approve``) refuse a
non-pending plan. So a ``declined`` PATCH against a plan under active
implementation succeeded: the store recorded ``declined`` and ``task_done``
moved the task file into ``done/`` under a still-running session. No
concurrency is involved: the tests here are plain sequential calls.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio

from nerve.db import Database

# ``approved`` is here although no writer produces it: the same unvalidated
# route can store it, and it is advertised as a plan status in three places.
NON_PENDING = ["approved", "implementing", "declined", "superseded", "done", "failed"]


class FakeEngine:
    """Minimal engine for the route's tool-invocation path.

    ``PATCH`` on decline reaches ``task_done`` through
    ``get_tool_registry().invoke(...)`` and ``build_route_tool_context()``,
    which read ``.registry``, ``.config`` and the bridge attributes. A real
    registry is used so ``task_done`` genuinely runs and the "task was not
    closed" assertions mean something.
    """

    def __init__(self, config: Any, db: Database) -> None:
        from nerve.agent.tools import build_default_registry

        self.config = config
        self.db = db
        self.registry = build_default_registry()
        self._memory_bridge = None
        self._xmemory_bridge = None
        self._skill_manager = None
        self.runs: list[dict[str, Any]] = []

    async def run(self, session_id: str, user_message: str, source: str = "web") -> None:
        self.runs.append({"session_id": session_id, "user_message": user_message})


@pytest.mark.asyncio
class TestHttpDeclinePrecondition:
    @pytest_asyncio.fixture
    async def app_setup(self, db: Database, tmp_path):
        """FastAPI app + a task on disk under ``memory/tasks/active/``.

        The on-disk task is what makes the side-effect assertions real:
        ``task_done`` moves the file into ``done/``, so "was the task closed"
        is observable in both the DB row and the filesystem.
        """
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import nerve.config as cfg_mod
        from nerve.agent import tools as tools_mod
        from nerve.config import NerveConfig
        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.plans import router as plans_router

        cfg = NerveConfig()
        cfg.workspace = tmp_path
        cfg.auth.jwt_secret = ""  # require_auth becomes a no-op
        cfg_mod._config = cfg

        task_id = "t-decline"
        rel_path = "memory/tasks/active/t-decline.md"
        task_md = tmp_path / rel_path
        task_md.parent.mkdir(parents=True, exist_ok=True)
        task_md.write_text("# Demo task\n\nBody.\n", encoding="utf-8")

        await db.upsert_task(
            task_id=task_id, file_path=rel_path, title="Demo task",
            status="pending", content=task_md.read_text(encoding="utf-8"),
        )
        await db.create_plan(
            plan_id="plan-1", task_id=task_id, content="the plan",
            session_id="sess-proposer", version=1, plan_type="generic",
        )

        engine = FakeEngine(cfg, db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)
        init_deps(engine=engine, db=db)  # type: ignore[arg-type]

        app = FastAPI()
        app.include_router(plans_router)

        yield SimpleNamespace(
            client=TestClient(app), db=db, engine=engine,
            task_id=task_id, workspace=tmp_path, task_md=task_md,
        )

        cfg_mod._config = None

    async def _assert_task_untouched(self, s) -> None:
        """The task must still be open, on disk, and outside ``done/``."""
        task = await s.db.get_task(s.task_id)
        assert task["status"] == "pending"
        assert task["file_path"] == "memory/tasks/active/t-decline.md"
        assert s.task_md.exists()
        assert not (s.workspace / "memory" / "tasks" / "done" / "t-decline.md").exists()

    async def test_http_decline_of_an_implementing_plan_returns_409(self, app_setup):
        """The reported defect: declining a plan under active implementation."""
        s = app_setup
        await s.db.update_plan("plan-1", status="implementing", impl_session_id="impl-live")

        resp = s.client.patch("/api/plans/plan-1", json={"status": "declined"})

        assert resp.status_code == 409
        assert "implementing" in resp.json()["detail"]
        # The row is untouched: status AND the live session pointer.
        plan = await s.db.get_plan("plan-1")
        assert plan["status"] == "implementing"
        assert plan["impl_session_id"] == "impl-live"
        # And the side effect never ran. "Returned 409" and "closed the task
        # anyway" are indistinguishable from the status code alone.
        await self._assert_task_untouched(s)

    async def test_http_decline_of_a_pending_plan_still_works(self, app_setup):
        """Regression guard: the happy path is unchanged."""
        s = app_setup

        resp = s.client.patch(
            "/api/plans/plan-1", json={"status": "declined", "feedback": "not now"},
        )

        assert resp.status_code == 200
        assert resp.json() == {"plan_id": "plan-1", "updated": True}
        plan = await s.db.get_plan("plan-1")
        assert plan["status"] == "declined"
        assert plan["feedback"] == "not now"
        assert plan["reviewed_at"]
        # The task was closed, with the feedback in the note.
        task = await s.db.get_task(s.task_id)
        assert task["status"] == "done"
        done_md = s.workspace / "memory" / "tasks" / "done" / "t-decline.md"
        assert done_md.exists()
        assert "not now" in done_md.read_text(encoding="utf-8")
        assert not s.task_md.exists()

    @pytest.mark.parametrize("status", NON_PENDING)
    async def test_http_decline_is_refused_for_every_non_pending_status(
        self, app_setup, status: str,
    ):
        """Covers every status the tool surface would also refuse."""
        s = app_setup
        await s.db.update_plan("plan-1", status=status)

        resp = s.client.patch("/api/plans/plan-1", json={"status": "declined"})

        assert resp.status_code == 409
        assert f"Plan is '{status}'" in resp.json()["detail"]
        plan = await s.db.get_plan("plan-1")
        assert plan["status"] == status
        await self._assert_task_untouched(s)

    @pytest.mark.parametrize("status", NON_PENDING)
    async def test_the_two_decline_surfaces_agree(self, app_setup, status: str):
        """Parity: the tool handler and the route must make the same call.

        This is the assertion that would have caught the original divergence,
        so it is the one that keeps it from coming back.
        """
        from nerve.agent.tools.handlers.plans import plan_decline_handler
        from nerve.gateway.routes._deps import build_route_tool_context

        s = app_setup
        await s.db.update_plan("plan-1", status=status)

        tool_result = await plan_decline_handler(
            build_route_tool_context(), {"plan_id": "plan-1"},
        )
        tool_refused = "only pending plans can be declined" in tool_result.content[0]["text"]

        resp = s.client.patch("/api/plans/plan-1", json={"status": "declined"})
        route_refused = resp.status_code == 409

        assert tool_refused == route_refused, (
            f"surface divergence on status={status!r}: "
            f"tool refused={tool_refused}, route refused={route_refused}"
        )
        assert tool_refused, f"both surfaces should refuse a {status!r} plan"
        # Neither surface may have written anything.
        plan = await s.db.get_plan("plan-1")
        assert plan["status"] == status
        await self._assert_task_untouched(s)

    async def test_other_statuses_are_unaffected(self, app_setup):
        """The guard covers ``declined`` only, deliberately, not by oversight.

        ``declined`` is the only PATCH-reachable transition with a
        task-closing side effect. Widening this into a general status whitelist
        is a separate concern; this test pins the scope so a future reader does
        not mistake it for a gap.
        """
        s = app_setup
        await s.db.update_plan("plan-1", status="implementing", impl_session_id="impl-live")

        resp = s.client.patch("/api/plans/plan-1", json={"status": "superseded"})

        assert resp.status_code == 200
        plan = await s.db.get_plan("plan-1")
        assert plan["status"] == "superseded"
        # No task closure on this path either way.
        await self._assert_task_untouched(s)
