"""Cron job routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

router = APIRouter()


@router.get("/api/cron/jobs")
async def list_cron_jobs(user: dict = Depends(require_auth)):
    """List all registered cron/source jobs with schedule and next run."""
    from nerve.gateway.server import _cron_service

    if not _cron_service:
        return {"jobs": []}

    jobs = await _cron_service.list_jobs()
    return {"jobs": jobs}


@router.post("/api/cron/jobs/{job_id}/trigger")
async def trigger_cron_job(job_id: str, user: dict = Depends(require_auth)):
    """Manually trigger a specific cron job or source runner."""
    from nerve.gateway.server import _cron_service

    if not _cron_service:
        raise HTTPException(status_code=503, detail="Cron service not available")

    # Source runner
    runners = getattr(_cron_service, "_source_runners", [])
    runner = next((r for r in runners if r.job_id == job_id), None)
    if runner:
        await _cron_service._run_source_wrapper(runner)
        return {"job_id": job_id, "triggered": True}

    # Regular cron job
    try:
        await _cron_service.run_job(job_id)
        return {"job_id": job_id, "triggered": True}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/api/cron/jobs/{job_id}/rotate")
async def rotate_cron_session(job_id: str, user: dict = Depends(require_auth)):
    """Force-rotate a persistent cron session's context."""
    from nerve.gateway.server import _cron_service

    if not _cron_service:
        raise HTTPException(status_code=503, detail="Cron service not available")

    try:
        result = await _cron_service.rotate_session(job_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/api/cron/logs")
async def get_cron_logs(
    job_id: str = "",
    limit: int = 50,
    offset: int = 0,
    user: dict = Depends(require_auth),
):
    deps = get_deps()
    limit = max(1, min(limit, 200))
    offset = max(0, offset)
    logs = await deps.db.get_cron_logs(
        job_id=job_id or None, limit=limit, offset=offset,
    )
    total = await deps.db.count_cron_logs(job_id=job_id or None)
    return {"logs": logs, "total": total, "limit": limit, "offset": offset}
