"""Cron history projects workflow outcomes without changing schedule anchors."""

import sqlite3
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

import nerve.workflows
from nerve.cron.jobs import CronJob
from nerve.cron.service import CronService
from nerve.gateway.auth import require_auth
from nerve.gateway.routes import cron


async def _launch(db, run_id="wfr-test", job_id="batch", *, created_by=None):
    await db.create_workflow_run(
        run_id, "codex-ultracode", {}, 1.0,
        created_by=created_by or f"cron:{job_id}",
    )
    log_id = await db.log_cron_start(job_id)
    await db.set_cron_log_session(log_id, f"workflow:{run_id}")
    await db.log_cron_finish(log_id, "success", output="workflow queued")
    return log_id


@pytest.mark.asyncio
async def test_failed_workflow_replaces_launch_success_with_full_duration(db):
    log_id = await _launch(db)
    await db._write(
        "UPDATE cron_logs SET started_at = ?, finished_at = ? WHERE id = ?",
        ("2025-01-01 10:00:00", "2025-01-01T10:00:00.050+00:00", log_id),
    )
    await db.transition_workflow_run(
        "wfr-test", "failed", error="initialize timeout in test worker",
        result="partial output", finished_at="2025-01-01T10:02:03+00:00",
    )

    [log] = await db.get_cron_logs("batch")
    assert log["id"] == log_id
    assert log["workflow_id"] == "wfr-test"
    assert log["status"] == "failed"
    assert log["error"] == "initialize timeout in test worker"
    assert log["output"] == "partial output"
    start = datetime.fromisoformat(log["started_at"]).replace(tzinfo=None)
    finish = datetime.fromisoformat(log["finished_at"]).replace(tzinfo=None)
    assert (finish - start).total_seconds() == 123
    assert log["dispatch"] == {
        "status": "success", "finished_at": "2025-01-01T10:00:00.050+00:00",
        "output": "workflow queued", "error": None,
    }
    # These are the old columns, with no migration or stored outcome rewrite.
    assert (await db.get_last_successful_cron_run("batch"))["status"] == "success"
    await db.close()
    await db.connect()
    assert await db.get_cron_logs("batch") == [log]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["pending", "running", "done", "failed", "killed", "budget_exhausted"])
async def test_every_workflow_state_and_schedule_queries(db, status):
    await _launch(db)
    anchor = await db.get_last_successful_cron_run("batch")
    recent = await db.get_recent_cron_runs()
    if status != "pending":
        await db.transition_workflow_run("wfr-test", status)
    [log] = await db.get_cron_logs()
    assert log["status"] == status
    assert bool(log["finished_at"]) == (status not in ("pending", "running"))
    assert log["output"] is None  # Never leak the old launch summary as a result.
    assert log["dispatch"]["finished_at"] == anchor["finished_at"]
    assert await db.get_last_successful_cron_run("batch") == anchor
    assert await db.get_recent_cron_runs() == recent


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["cron:another-job", "session:batch", "missing"])
async def test_missing_or_foreign_workflow_is_unavailable(db, origin):
    log_id = await _launch(db, created_by=origin)
    await db.transition_workflow_run("wfr-test", "done", result="other result")
    if origin == "missing":
        await db._write("DELETE FROM workflow_runs WHERE id = ?", ("wfr-test",))
    [log] = await db.get_cron_logs()
    assert log["id"] == log_id
    assert log["status"] == "unavailable"
    assert log["finished_at"] is None
    assert log["output"] is None
    assert log["error"] == "Workflow result is unavailable."
    assert log["dispatch"]["status"] == "success"


@pytest.mark.asyncio
async def test_linked_inflight_dispatch_shows_workflow(db):
    log_id = await _launch(db)
    await db._write("UPDATE cron_logs SET status = NULL WHERE id = ?", (log_id,))
    [log] = await db.get_cron_logs()
    assert log["status"] == "pending"


@pytest.mark.asyncio
async def test_post_launch_write_error_cannot_hide_the_running_workflow(db, monkeypatch):
    run = await db.create_workflow_run(
        "wfr-test", "codex-ultracode", {}, 1.0, created_by="cron:batch",
    )
    workflow_service = SimpleNamespace(start_run=AsyncMock(return_value=run))
    monkeypatch.setattr(nerve.workflows, "_service", workflow_service)
    finish = db.log_cron_finish
    attempts = []

    async def transient_finish(log_id, status, **kwargs):
        attempts.append(status)
        if status == "success":
            raise sqlite3.OperationalError("database is locked")
        await finish(log_id, status, **kwargs)

    monkeypatch.setattr(db, "log_cron_finish", transient_finish)
    service = CronService(SimpleNamespace(timezone="UTC"), AsyncMock(), db)
    job = CronJob(id="batch", schedule="1h", lock=False, workflow={
        "engine": "codex-ultracode", "prompt": "sample task", "budget_usd": 1.0,
    })
    log_id = await db.log_cron_start(job.id)
    await service._start_workflow_run(job, log_id)
    assert attempts == ["success", "error"]

    for status in ("pending", "running", "done"):
        if status != "pending":
            await db.transition_workflow_run("wfr-test", status)
        [log] = await db.get_cron_logs()
        assert log["status"] == status
        assert log["error"] is None
        assert log["dispatch"]["status"] == "error"
        assert log["dispatch"]["error"] == "database is locked"


@pytest.mark.asyncio
async def test_plain_source_lock_skip_and_prelaunch_errors_keep_their_result(db):
    for job, status, output, error, session in [
        ("plain", "success", "completed", None, "cron:plain"),
        ("source:sample", "success", "2 ingested", None, None),
        ("locked", "success", "skipped: workflow run is still active (lock)", None, None),
        ("bad-launch", "error", None, "workflow runs disabled", None),
    ]:
        log_id = await db.log_cron_start(job)
        await db.log_cron_finish(log_id, status, output, error, session)
        [log] = await db.get_cron_logs(job)
        assert (log["status"], log["output"], log["error"]) == (status, output, error)
        assert log["finished_at"]
        assert "workflow_id" not in log
    await db.log_cron_missed("missed", "2025-01-01 10:00:00")
    assert (await db.get_cron_logs("missed"))[0]["status"] == "missed"


@pytest.mark.asyncio
async def test_api_projection_filter_pagination_and_single_join(db, monkeypatch):
    ids = [await _launch(db, f"wfr-{i}", "batch") for i in range(4)]
    await _launch(db, "wfr-other", "other")
    await db.transition_workflow_run("wfr-2", "failed", error="worker failed")
    # Deterministic ordering even for equal timestamps.
    await db._write("UPDATE cron_logs SET started_at = '2025-01-01 10:00:00'")
    app = FastAPI()
    app.include_router(cron.router)
    app.dependency_overrides[require_auth] = lambda: None
    monkeypatch.setattr(cron, "get_deps", lambda: SimpleNamespace(db=db))
    statements = []
    await db.db.set_trace_callback(statements.append)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/cron/logs?job_id=batch&limit=2&offset=1")
    await db.db.set_trace_callback(None)
    assert response.status_code == 200
    data = response.json()
    assert (data["total"], data["limit"], data["offset"]) == (4, 2, 1)
    assert [log["id"] for log in data["logs"]] == [ids[2], ids[1]]
    assert data["logs"][0]["status"] == "failed"
    assert data["logs"][0]["error"] == "worker failed"
    assert len([sql for sql in statements if "workflow_runs" in sql]) == 1
