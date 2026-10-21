"""Cron job routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.identity import Actor, attribution_for

router = APIRouter(dependencies=[Depends(require_auth)])


@router.get("/api/cron/jobs")
async def list_cron_jobs():
    """List all registered cron/source jobs with schedule and next run."""
    from nerve.gateway.server import _cron_service

    if not _cron_service:
        return {"jobs": []}

    jobs = await _cron_service.list_jobs()
    return {"jobs": jobs}


@router.get("/api/cron/schedule")
async def cron_schedule():
    """The next work the instance will start on its own, and what each job's
    gates forecast for its next run (see ``CronService.schedule_report``)."""
    from nerve.gateway.server import _cron_service

    if not _cron_service:
        return {"next": None, "jobs": []}

    return await _cron_service.schedule_report()


@router.post("/api/cron/reload")
async def reload_cron_jobs():
    """Re-read cron config and apply changes to the scheduler without a restart."""
    from nerve.config import ConfigError
    from nerve.gateway.server import _cron_service

    if not _cron_service:
        raise HTTPException(status_code=503, detail="Cron service not available")

    try:
        result = await _cron_service.reload()
    except ConfigError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"reloaded": True, **result}


@router.post("/api/cron/jobs/{job_id}/trigger")
async def trigger_cron_job(job_id: str, actor: Actor = Depends(require_auth)):
    """Manually trigger a specific cron job or source runner.

    The run log records the person as actor and cause. A job's turn runs as
    the system actor on their behalf.
    """
    from nerve.gateway.server import _cron_service

    if not _cron_service:
        raise HTTPException(status_code=503, detail="Cron service not available")

    attribution = attribution_for(actor)
    # Source runner
    runners = getattr(_cron_service, "_source_runners", [])
    runner = next((r for r in runners if r.job_id == job_id), None)
    if runner:
        await _cron_service._run_source_wrapper(runner, attribution)
        return {"job_id": job_id, "triggered": True}

    # Regular cron job
    try:
        await _cron_service.run_job(job_id, attribution=attribution)
        return {"job_id": job_id, "triggered": True}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/api/cron/jobs/{job_id}/rotate")
async def rotate_cron_session(job_id: str):
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
):
    deps = get_deps()
    limit = max(1, min(limit, 200))
    offset = max(0, offset)
    logs = await deps.db.get_cron_logs(
        job_id=job_id or None, limit=limit, offset=offset,
    )
    total = await deps.db.count_cron_logs(job_id=job_id or None)
    return {"logs": logs, "total": total, "limit": limit, "offset": offset}
