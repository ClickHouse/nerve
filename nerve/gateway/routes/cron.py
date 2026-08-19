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


@router.post("/api/cron/reload")
async def reload_cron_jobs(user: dict = Depends(require_auth)):
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


@router.post("/api/cron/jobs/{job_id}/enable")
async def enable_cron_job(job_id: str, user: dict = Depends(require_auth)):
    """Enable a cron job in its cron file and schedule it, without a restart."""
    return await _set_cron_job_enabled(job_id, True)


@router.post("/api/cron/jobs/{job_id}/disable")
async def disable_cron_job(job_id: str, user: dict = Depends(require_auth)):
    """Disable a cron job in its cron file and unschedule it, without a restart."""
    return await _set_cron_job_enabled(job_id, False)


async def _set_cron_job_enabled(job_id: str, enabled: bool) -> dict:
    """Shared body of the two toggle routes.

    Status codes follow what the caller can do about it: 404 for a job that is
    not there, 403 when lockdown reserves the file for a reviewed PR, and 400 for
    a file this edit cannot be expressed in (flow style, no ``jobs`` list) or a
    reload the new config cannot satisfy. None of the three is worth a retry —
    each is fixed by editing YAML.
    """
    from nerve.config import ConfigError, LockdownError
    from nerve.cron.jobs import JobEditError
    from nerve.gateway.server import _cron_service

    if not _cron_service:
        raise HTTPException(status_code=503, detail="Cron service not available")

    try:
        return await _cron_service.set_job_enabled(job_id, enabled)
    except LockdownError as e:
        raise HTTPException(status_code=403, detail=str(e)) from e
    # Before the bare ValueError below, not after: ConfigError subclasses
    # ValueError (and InvalidScheduleError subclasses ConfigError), so the
    # broad clause first would report a schedule typo as a missing job.
    except (JobEditError, ConfigError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


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
