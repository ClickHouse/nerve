"""Source sync and inbox routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.identity import Actor

router = APIRouter()


@router.post("/api/sources/{source_name}/sync")
async def trigger_single_source_sync(source_name: str, actor: Actor = Depends(require_auth)):
    """Manually trigger sync for a specific source."""
    from nerve.gateway.server import _cron_service
    deps = get_deps()

    if not _cron_service:
        raise HTTPException(status_code=503, detail="Cron service not available")

    runners = getattr(_cron_service, "_source_runners", [])
    runner = next((r for r in runners if r.source.source_name == source_name), None)
    if not runner:
        available = [r.source.source_name for r in runners]
        raise HTTPException(status_code=404, detail=f"Source not found: {source_name}. Available: {available}")

    result = await runner.run()

    # Log it
    await deps.db.log_source_run(
        source=source_name,
        records_fetched=result.records_ingested,
        records_processed=result.records_ingested,
        error=result.error,
    )

    return {
        "source": source_name,
        "records_ingested": result.records_ingested,
        "error": result.error,
    }


@router.post("/api/sources/sync-all")
async def trigger_all_sources_sync(actor: Actor = Depends(require_auth)):
    """Manually trigger sync for all registered sources."""
    from nerve.gateway.server import _cron_service
    deps = get_deps()

    if not _cron_service:
        raise HTTPException(status_code=503, detail="Cron service not available")

    runners = getattr(_cron_service, "_source_runners", [])
    results = {}
    for runner in runners:
        name = runner.source.source_name
        result = await runner.run()
        await deps.db.log_source_run(
            source=name,
            records_fetched=result.records_ingested,
            records_processed=result.records_ingested,
            error=result.error,
        )
        results[name] = {
            "records_ingested": result.records_ingested,
            "error": result.error,
        }

    return {"results": results}


# --- Source inbox ---

@router.get("/api/sources/messages")
async def list_source_messages(
    source: str = "", limit: int = 50, before: str = "",
    session: str = "",
    actor: Actor = Depends(require_auth),
):
    """Paginated list of source inbox messages, newest first."""
    deps = get_deps()
    capped_limit = min(limit, 200)
    rows, has_more = await deps.db.list_source_messages(
        source=source or None,
        limit=capped_limit,
        before_ts=before or None,
        run_session_id=session or None,
    )
    return {"messages": rows, "has_more": has_more}


@router.get("/api/sources/messages/{source:path}/{msg_id}")
async def get_source_message(source: str, msg_id: str, actor: Actor = Depends(require_auth)):
    """Get a single source message with full content and processed_content."""
    deps = get_deps()
    msg = await deps.db.get_source_message(source, msg_id)
    if not msg:
        raise HTTPException(status_code=404, detail="Message not found")
    return msg


@router.delete("/api/sources/messages")
async def purge_source_messages(source: str = "", actor: Actor = Depends(require_auth)):
    """Purge source messages. If source specified, only that source; otherwise all."""
    deps = get_deps()
    deleted = await deps.db.delete_source_messages(source=source or None)
    return {"deleted": deleted}


@router.get("/api/sources/overview")
async def source_overview(actor: Actor = Depends(require_auth)):
    """Combined overview: message counts, storage, cursor status, 1h/24h stats."""
    deps = get_deps()
    from nerve.gateway.server import _cron_service

    # Gather all data in parallel-ish calls
    counts = await deps.db.get_source_message_counts()
    storage = await deps.db.get_source_messages_storage()
    stats_1h = await deps.db.get_source_stats(hours=1)
    stats_24h = await deps.db.get_source_stats(hours=24)

    # Collect all known source names
    known_sources: set[str] = set(counts.keys()) | set(storage.keys()) | set(stats_1h.keys()) | set(stats_24h.keys())

    # From registered runners
    if _cron_service and hasattr(_cron_service, "_source_runners"):
        for runner in _cron_service._source_runners:
            known_sources.add(runner.source.source_name)

    # From DB cursors
    try:
        known_sources |= await deps.db.get_known_source_names()
    except Exception:
        pass

    sources = {}
    total_messages = 0
    total_storage = 0

    for src in sorted(known_sources):
        cursor = await deps.db.get_sync_cursor(src)
        last_run = await deps.db.get_last_source_run(src)
        msg_count = counts.get(src, 0)
        src_storage = storage.get(src, {})
        total_messages += msg_count
        total_storage += src_storage.get("bytes", 0)

        empty_stats = {"runs": 0, "fetched": 0, "processed": 0, "errors": 0, "last_run_at": None}
        sources[src] = {
            "message_count": msg_count,
            "storage_bytes": src_storage.get("bytes", 0),
            "cursor": cursor,
            "last_run_at": last_run.get("ran_at") if last_run else None,
            "last_error": last_run.get("error") if last_run else None,
            "stats_1h": stats_1h.get(src, empty_stats),
            "stats_24h": stats_24h.get(src, empty_stats),
        }

    return {
        "sources": sources,
        "total_messages": total_messages,
        "total_storage_bytes": total_storage,
    }


@router.get("/api/sources/runs")
async def list_source_runs(
    source: str = "", limit: int = 50,
    actor: Actor = Depends(require_auth),
):
    """Source run history with session links."""
    deps = get_deps()
    runs = await deps.db.get_source_run_log(
        source=source or None,
        limit=min(limit, 200),
    )
    return {"runs": runs}


@router.get("/api/sources/stats")
async def source_stats(hours: int = 24, actor: Actor = Depends(require_auth)):
    """Per-source aggregate stats for the last N hours."""
    deps = get_deps()
    stats = await deps.db.get_source_stats(hours=min(hours, 168))  # Cap at 7 days
    return {"stats": stats, "hours": hours}


@router.get("/api/sources/consumers")
async def get_consumer_cursors(consumer: str | None = None, actor: Actor = Depends(require_auth)):
    """List active consumer cursors with unread counts."""
    deps = get_deps()
    cursors = await deps.db.list_consumer_cursors(consumer=consumer)
    return {"consumers": cursors}


@router.get("/api/sources/health")
async def get_source_health(actor: Actor = Depends(require_auth)):
    """Per-source circuit breaker health state."""
    from nerve.gateway.server import _cron_service

    health: dict[str, dict] = {}
    runners = getattr(_cron_service, "_source_runners", [])
    for runner in runners:
        h = runner.health
        health[runner.source.source_name] = {
            "state": h.state,
            "consecutive_failures": h.consecutive_failures,
            "last_error": h.last_error,
            "last_error_at": h.last_error_at.isoformat() if h.last_error_at else None,
            "last_success_at": h.last_success_at.isoformat() if h.last_success_at else None,
            "backoff_until": h.backoff_until.isoformat() if h.backoff_until else None,
        }
    return {"health": health}
