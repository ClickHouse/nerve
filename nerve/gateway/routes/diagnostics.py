"""Diagnostics and memorization sweep routes."""

from __future__ import annotations

import asyncio
import logging
import platform
import shutil
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

from nerve.config import get_config
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.observability.langfuse import get_status as langfuse_status

logger = logging.getLogger(__name__)

router = APIRouter()


async def _per_source_status(db, source: str) -> tuple[str, dict]:
    """Fetch (cursor, last_run) for one source in parallel-friendly form."""
    cursor, last_run = await asyncio.gather(
        db.get_sync_cursor(source),
        db.get_last_source_run(source),
    )
    return source, {
        "cursor": cursor,
        "last_run": last_run.get("ran_at") if last_run else None,
        "records_fetched": last_run.get("records_fetched", 0) if last_run else 0,
        "records_processed": last_run.get("records_processed", 0) if last_run else 0,
        "error": last_run.get("error") if last_run else None,
    }


@router.get("/api/diagnostics")
async def diagnostics(user: dict = Depends(require_auth)):
    """System health and status information."""
    deps = get_deps()
    config = get_config()
    from nerve.gateway.server import _cron_service, _memorize_stats

    # Memory usage
    try:
        import resource
        mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        mem_mb = 0

    # Disk usage (sync, fast)
    disk = shutil.disk_usage(str(config.workspace))

    # Phase 1: independent top-level queries in parallel.
    # Wrapped with return_exceptions so one slow/failing query doesn't sink
    # the whole page; each branch falls back to a sensible default below.
    (
        cron_logs_res,
        known_sources_res,
        sessions_count_res,
        pending_sessions_res,
        tasks_health_res,
        usage_summary_res,
        cache_stats_res,
        daily_usage_res,
        source_usage_res,
        model_usage_res,
    ) = await asyncio.gather(
        deps.db.get_cron_logs(limit=10),
        deps.db.get_known_source_names(),
        deps.db.count_sessions(),
        deps.db.get_sessions_needing_memorization(),
        deps.db.get_task_health_stats(),
        deps.db.get_usage_summary(days=7),
        deps.db.get_cache_hit_rate(days=7),
        deps.db.get_usage_by_period(days=7),
        deps.db.get_usage_by_source(days=7),
        deps.db.get_usage_by_model(days=7),
        return_exceptions=True,
    )

    def _ok(v, default):
        if isinstance(v, BaseException):
            logger.debug("Diagnostics sub-query failed: %r", v)
            return default
        return v

    cron_logs = _ok(cron_logs_res, [])
    known_sources: set[str] = set(_ok(known_sources_res, set()))
    sessions_count = _ok(sessions_count_res, 0)
    pending_sessions = _ok(pending_sessions_res, [])
    tasks_health = _ok(tasks_health_res, {})

    usage_data: dict = {}
    if not any(
        isinstance(v, BaseException)
        for v in (usage_summary_res, cache_stats_res, daily_usage_res,
                  source_usage_res, model_usage_res)
    ):
        usage_data = {
            "last_7d": usage_summary_res,
            "cache_hit_rate": cache_stats_res,
            "daily": daily_usage_res,
            "by_source": source_usage_res,
            "by_model": model_usage_res,
        }

    # Augment known sources with registered runners (includes sources that
    # haven't logged a run yet).
    if _cron_service and hasattr(_cron_service, "_source_runners"):
        for runner in _cron_service._source_runners:
            known_sources.add(runner.source.source_name)

    # Phase 2: per-source status — fan out in parallel instead of N sequential
    # await chains. Each source's two queries are themselves gathered.
    sync_status: dict = {}
    if known_sources:
        try:
            results = await asyncio.gather(
                *(_per_source_status(deps.db, src) for src in sorted(known_sources)),
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, BaseException):
                    logger.debug("Per-source diagnostics failed: %r", r)
                    continue
                src, status = r
                sync_status[src] = status
        except Exception:
            logger.exception("Failed to collect per-source diagnostics")

    return {
        "system": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "hostname": platform.node(),
            "memory_mb": round(mem_mb, 1),
            "disk_total_gb": round(disk.total / (1024**3), 1),
            "disk_free_gb": round(disk.free / (1024**3), 1),
        },
        "workspace": str(config.workspace),
        "sessions_count": sessions_count,
        "sync": sync_status,
        "recent_cron_logs": cron_logs,
        "tasks": tasks_health,
        "memorization": {
            **_memorize_stats,
            "sessions_pending": len(pending_sessions),
        },
        "usage": usage_data,
        "langfuse": langfuse_status(),
    }


@router.get("/api/observability/status")
async def observability_status(user: dict = Depends(require_auth)):
    """Lightweight status endpoint — used by the chat UI to render a
    "View in Langfuse" deep-link when observability is configured.

    Kept separate from /api/diagnostics so the chat page can poll it
    without paying for the full diagnostics fan-out.
    """
    return {"langfuse": langfuse_status()}


@router.post("/api/memorization/sweep")
async def trigger_memorization_sweep(user: dict = Depends(require_auth)):
    """Manually trigger a memorization sweep."""
    deps = get_deps()
    if not deps.engine:
        raise HTTPException(status_code=503, detail="Engine not available")

    from nerve.gateway.server import _memorize_stats

    result = await deps.engine.run_memorization_sweep()
    _memorize_stats["last_run_at"] = datetime.now(timezone.utc).isoformat()
    _memorize_stats["last_result"] = result
    _memorize_stats["total_runs"] += 1
    return result
