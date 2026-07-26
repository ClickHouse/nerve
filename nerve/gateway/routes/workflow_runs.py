"""Workflow run routes — start/inspect/kill budget-capped multi-agent jobs.

Thin HTTP veneer over :class:`nerve.workflows.service.WorkflowRunService`;
all validation and lifecycle logic lives in the service. The journal
endpoint reads the run's on-disk journal directly (bounded reads), never
executing anything.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.gateway.auth import require_auth

logger = logging.getLogger(__name__)

router = APIRouter()

# Bounded journal reads: cap run.json size, tail events.ndjson/result.md.
_RUN_JSON_MAX_BYTES = 256 * 1024
_EVENTS_TAIL_BYTES = 256 * 1024
_EVENTS_MAX_LINES = 200
_RESULT_MAX_BYTES = 64 * 1024


class WorkflowRunCreateRequest(BaseModel):
    engine: str
    prompt: str
    budget_usd: float | None = None
    title: str = ""
    model: str = ""
    effort: str = ""
    cwd: str = ""


class WorkflowRunKillRequest(BaseModel):
    reason: str = ""


def _service():
    """Resolve the live singleton; 503 when workflow runs are disabled."""
    from nerve.workflows import get_workflow_run_service

    service = get_workflow_run_service()
    if service is None:
        raise HTTPException(status_code=503, detail="workflow runs disabled")
    return service


@router.get("/api/workflow-runs")
async def list_workflow_runs(
    status: str = "",
    limit: int = 50,
    offset: int = 0,
    user: dict = Depends(require_auth),
):
    """List runs (newest first). ``status``: 'active', an exact status, or empty."""
    service = _service()
    limit = max(1, min(limit, 200))
    offset = max(0, offset)
    status_filter = status or None
    runs = await service.list_runs(status=status_filter, limit=limit, offset=offset)
    total = await service.db.count_workflow_runs(status_filter)
    return {"runs": [service.public_run(r) for r in runs], "total": total}


@router.post("/api/workflow-runs")
async def create_workflow_run(
    req: WorkflowRunCreateRequest, user: dict = Depends(require_auth),
):
    """Start a run; returns its wire shape (usually already 'running')."""
    service = _service()
    from nerve.workflows.service import WorkflowRunError

    spec = {
        "prompt": req.prompt,
        "model": req.model,
        "effort": req.effort,
        "cwd": req.cwd,
    }
    try:
        run = await service.start_run(
            engine_kind=req.engine,
            spec=spec,
            budget_usd=req.budget_usd,
            title=req.title,
            created_by="api",
        )
    except WorkflowRunError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return service.public_run(run)


@router.get("/api/workflow-runs/{run_id}")
async def get_workflow_run(run_id: str, user: dict = Depends(require_auth)):
    service = _service()
    run = await service.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Workflow run not found")
    return service.public_run(run)


@router.post("/api/workflow-runs/{run_id}/kill")
async def kill_workflow_run(
    run_id: str,
    req: WorkflowRunKillRequest = WorkflowRunKillRequest(),
    user: dict = Depends(require_auth),
):
    """Kill a run (idempotent on terminal runs); scoped to its own session."""
    service = _service()
    from nerve.workflows.service import WorkflowRunError

    try:
        run = await service.kill_run(run_id, reason=req.reason, killed_by="api")
    except WorkflowRunError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return service.public_run(run)


@router.get("/api/workflow-runs/{run_id}/journal")
async def get_workflow_run_journal(run_id: str, user: dict = Depends(require_auth)):
    """Bounded read of the run's journal dir (run.json / events / result)."""
    service = _service()
    run = await service.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Workflow run not found")
    journal_dir = _safe_journal_dir(service, run)
    if journal_dir is None:
        return {"run_json": None, "events": [], "has_result": False, "result": ""}
    return await asyncio.to_thread(_read_journal, journal_dir)


def _safe_journal_dir(service, run: dict) -> Path | None:
    """Resolve the run row's journal_dir; refuse paths outside runs_dir.

    The journal_dir column is service-written, but resolving and
    containment-checking it here means a tampered row can never turn this
    endpoint into an arbitrary-directory reader.
    """
    raw = run.get("journal_dir")
    if not raw:
        return None
    try:
        root = service.runs_dir().resolve()
        path = Path(str(raw)).expanduser().resolve()
    except OSError:
        return None
    if path == root or not path.is_relative_to(root):
        return None
    return path


def _read_journal(journal_dir: Path) -> dict:
    """Sync journal read (runs in a thread). Missing files -> empty values."""
    run_json = None
    path = journal_dir / "run.json"
    try:
        if path.is_file() and path.stat().st_size <= _RUN_JSON_MAX_BYTES:
            run_json = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        run_json = None

    events: list[dict] = []
    path = journal_dir / "events.ndjson"
    try:
        if path.is_file():
            with path.open("rb") as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - _EVENTS_TAIL_BYTES))
                data = f.read(_EVENTS_TAIL_BYTES)
            # A tail cut mid-line simply fails to parse and is skipped.
            for line in data.decode("utf-8", errors="replace").splitlines()[-_EVENTS_MAX_LINES:]:
                line = line.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except ValueError:
                    continue
                if isinstance(parsed, dict):
                    events.append(parsed)
    except OSError:
        events = []

    has_result = False
    result = ""
    path = journal_dir / "result.md"
    try:
        if path.is_file():
            has_result = True
            size = path.stat().st_size
            with path.open("rb") as f:
                if size > _RESULT_MAX_BYTES:
                    f.seek(size - _RESULT_MAX_BYTES)
                result = f.read(_RESULT_MAX_BYTES).decode("utf-8", errors="replace")
    except OSError:
        has_result = False
        result = ""

    return {
        "run_json": run_json,
        "events": events,
        "has_result": has_result,
        "result": result,
    }
