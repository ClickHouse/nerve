"""HTTP route tests for ``/api/workflow-runs`` (gateway/routes/workflow_runs.py).

Pattern mirrors ``TestHttpReviseRoute`` in test_plan_revise.py: a minimal
FastAPI app with the real router, auth disabled via an empty jwt_secret,
and a REAL :class:`WorkflowRunService` installed as the module singleton,
so the routes exercise genuine validation/lifecycle against a fake engine.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from nerve.db import Database


class FakeSessionManager:
    """Records get_or_create calls; the service passes backend/model/cwd.

    Persists a real sessions row — ``workflow_runs.session_id`` is a
    foreign key, so the service's session_id update needs one.
    """

    def __init__(self, db: Database) -> None:
        self.db = db
        self.calls: list[dict[str, Any]] = []

    async def get_or_create(self, session_id: str, **kwargs) -> dict:
        self.calls.append({"session_id": session_id, **kwargs})
        return await self.db.create_session(
            session_id,
            title=kwargs.get("title"),
            source=kwargs.get("source", "workflow"),
            backend=kwargs.get("backend", "claude"),
            model=kwargs.get("model"),
            cwd=kwargs.get("cwd"),
        )


class FakeEngine:
    """The slice of AgentEngine that WorkflowRunService touches."""

    def __init__(self, db: Database) -> None:
        self.sessions = FakeSessionManager(db)
        self.run = AsyncMock(return_value="workflow finished: all tasks complete")
        self.stop_session = AsyncMock()
        self._discard_client = AsyncMock()
        self.notification_service = None

    def register_task(self, session_id: str, task: asyncio.Task) -> None:
        pass

    def is_session_running(self, session_id: str) -> bool:
        return False

    def get_live_workflow_tokens(self, session_id: str) -> int:
        return 0


async def _settle(service) -> None:
    """Await spawned ``_execute`` tasks that live on the current loop.

    Tasks spawned during a TestClient request run on the request portal's
    own loop and are already finalized (and popped from ``_exec_tasks``)
    by the time the request returns — so this only really waits in the
    direct ``service.start_run(...)`` case.
    """
    tasks = [t for t in service._exec_tasks.values() if not t.done()]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
class TestWorkflowRunRoutes:
    @pytest_asyncio.fixture
    async def setup(self, db: Database, tmp_path, monkeypatch):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import nerve.config as cfg_mod
        import nerve.workflows as workflows_mod
        from nerve.config import NerveConfig
        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.workflow_runs import router as wf_router
        from nerve.workflows.service import WorkflowRunService

        # Auth dependency reads get_config().auth.jwt_secret — install a
        # config with no secret so require_auth is a no-op.
        cfg = NerveConfig()
        cfg.workspace = tmp_path
        cfg.auth.jwt_secret = ""
        cfg.workflows.runs_dir = tmp_path
        cfg.workflows.kill_grace_seconds = 0
        cfg_mod._config = cfg

        engine = FakeEngine(db)
        init_deps(engine=engine, db=db)  # type: ignore[arg-type]

        service = WorkflowRunService(cfg, db, engine)  # type: ignore[arg-type]
        monkeypatch.setattr(workflows_mod, "_service", service)

        app = FastAPI()
        app.include_router(wf_router)
        client = TestClient(app)

        yield SimpleNamespace(
            client=client, service=service, engine=engine, db=db, cfg=cfg,
        )

        await _settle(service)
        cfg_mod._config = None  # leave global state clean for other tests

    async def test_list_empty(self, setup):
        resp = setup.client.get("/api/workflow-runs")
        assert resp.status_code == 200
        assert resp.json() == {"runs": [], "total": 0}

    async def test_create_run(self, setup):
        resp = setup.client.post("/api/workflow-runs", json={
            "engine": "claude-workflow",
            "prompt": "fix issue #12 in myorg/myrepo",
            "budget_usd": 5.0,
            "title": "demo run",
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["id"].startswith("wfr-")
        assert body["status"] in ("pending", "running")
        assert body["budget_usd"] == 5.0
        assert body["created_by"] == "api"
        assert body["spec"]["prompt"] == "fix issue #12 in myorg/myrepo"

        await _settle(setup.service)

        resp = setup.client.get("/api/workflow-runs")
        assert resp.status_code == 200
        listing = resp.json()
        assert listing["total"] == 1
        assert listing["runs"][0]["id"] == body["id"]

    async def test_create_bad_engine_returns_400(self, setup):
        resp = setup.client.post("/api/workflow-runs", json={
            "engine": "not-an-engine",
            "prompt": "do things",
            "budget_usd": 1.0,
        })
        assert resp.status_code == 400
        assert "unknown engine" in resp.json()["detail"]

    async def test_create_missing_budget_returns_400(self, setup):
        # allow_unbudgeted defaults to False — omitting budget_usd is a 400.
        resp = setup.client.post("/api/workflow-runs", json={
            "engine": "claude-workflow",
            "prompt": "do things",
        })
        assert resp.status_code == 400
        assert "budget" in resp.json()["detail"]

    async def test_get_unknown_returns_404(self, setup):
        resp = setup.client.get("/api/workflow-runs/wfr-deadbeef")
        assert resp.status_code == 404

    async def test_kill_unknown_returns_404(self, setup):
        resp = setup.client.post(
            "/api/workflow-runs/wfr-deadbeef/kill", json={"reason": "n/a"},
        )
        assert resp.status_code == 404

    async def test_kill_created_run_lands_terminal(self, setup):
        resp = setup.client.post("/api/workflow-runs", json={
            "engine": "claude-workflow",
            "prompt": "long-running job",
            "budget_usd": 2.0,
        })
        assert resp.status_code == 200
        run_id = resp.json()["id"]
        await _settle(setup.service)

        resp = setup.client.post(
            f"/api/workflow-runs/{run_id}/kill", json={"reason": "operator abort"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == run_id
        assert body["status"] in ("killed", "done", "failed", "budget_exhausted")

        # Idempotent on terminal runs: a second kill is still a 200.
        resp = setup.client.post(f"/api/workflow-runs/{run_id}/kill", json={})
        assert resp.status_code == 200
        assert resp.json()["status"] == body["status"]

    async def test_journal_after_run_executes(self, setup):
        # Start directly on this loop so the exec task settles deterministically.
        run = await setup.service.start_run(
            engine_kind="claude-workflow",
            spec={"prompt": "summarize the repo"},
            budget_usd=3.0,
            title="journal demo",
        )
        await _settle(setup.service)

        fresh = await setup.service.get_run(run["id"])
        assert fresh is not None
        assert fresh["status"] == "done"

        resp = setup.client.get(f"/api/workflow-runs/{run['id']}/journal")
        assert resp.status_code == 200
        body = resp.json()
        events = [e.get("event") for e in body["events"]]
        assert "created" in events
        assert "started" in events
        assert "done" in events
        assert body["run_json"]["id"] == run["id"]
        assert body["has_result"] is True
        assert "workflow finished" in body["result"]

        # Status filter flows through to the list + count.
        resp = setup.client.get("/api/workflow-runs", params={"status": "done"})
        assert resp.status_code == 200
        assert resp.json()["total"] == 1

    async def test_journal_unknown_run_returns_404(self, setup):
        resp = setup.client.get("/api/workflow-runs/wfr-deadbeef/journal")
        assert resp.status_code == 404

    async def test_journal_dir_outside_runs_dir_is_refused(self, setup, tmp_path):
        run = await setup.service.start_run(
            engine_kind="claude-workflow",
            spec={"prompt": "traversal check"},
            budget_usd=1.0,
        )
        await _settle(setup.service)

        # Point the row at a directory outside runs_dir; the endpoint must
        # refuse to read it and return the empty journal shape.
        outside = tmp_path.parent
        await setup.db.update_workflow_run(run["id"], {"journal_dir": str(outside)})
        resp = setup.client.get(f"/api/workflow-runs/{run['id']}/journal")
        assert resp.status_code == 200
        assert resp.json() == {
            "run_json": None, "events": [], "has_result": False, "result": "",
        }

    async def test_singleton_reset_returns_503(self, setup):
        import nerve.workflows as workflows_mod

        workflows_mod.reset_workflow_run_service()

        resp = setup.client.get("/api/workflow-runs")
        assert resp.status_code == 503
        assert "disabled" in resp.json()["detail"]

        resp = setup.client.post("/api/workflow-runs", json={
            "engine": "claude-workflow", "prompt": "x", "budget_usd": 1.0,
        })
        assert resp.status_code == 503
