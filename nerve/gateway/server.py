"""FastAPI application — HTTP API, WebSocket endpoint, static file serving.

Single entry point for the entire Nerve gateway.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from nerve.agent.engine import AgentEngine
from nerve.agent.streaming import broadcaster
from nerve.config import NerveConfig, get_config
from nerve.db import Database, init_db, close_db
from nerve.gateway.auth import authenticate_websocket
from nerve.gateway.routes import (
    init_deps,
    register_all_routes,
    set_external_agents_sync,
    set_notification_service,
)
from nerve.mcp_server import build_manager as _build_mcp_manager, mount_deferred as _mount_mcp_deferred
from nerve.observability.langfuse import (
    flush as langfuse_flush,
    init_langfuse,
)

logger = logging.getLogger(__name__)

# Global references
_engine: AgentEngine | None = None
_cron_service = None  # CronService
# StreamableHTTPSessionManager assigned during lifespan when
# config.mcp_endpoint.enabled. The /mcp/v1 mount handler reads it; until
# lifespan finishes building it, the mount returns 503.
_mcp_manager = None
# CodexThreadSyncService assigned during lifespan when sync.codex.enabled.
# Exposed on the diagnostics endpoint so the UI can render per-origin
# health without piercing the lifespan closure.
_codex_thread_sync = None
# External-agents SyncService — re-renders ~/.codex/AGENTS.md,
# ~/.claude/CLAUDE.md, etc. when source files change. Built in the
# lifespan once the config is loaded so the periodic sweep starts the
# moment the gateway accepts traffic.
_external_agents_sync = None

# Memorization sweep stats (updated by background task, read by diagnostics)
_memorize_stats: dict = {
    "last_run_at": None,
    "last_result": None,
    "total_runs": 0,
    "total_errors": 0,
    "interval_minutes": 30,
}


def get_engine() -> AgentEngine:
    if _engine is None:
        raise RuntimeError("Engine not initialized")
    return _engine


def get_codex_thread_sync():
    """Return the running :class:`CodexThreadSyncService`, if any.

    Used by ``/api/diagnostics`` so the UI can render per-origin
    health for the Codex thread sync. Returns ``None`` when the
    feature is disabled or hasn't finished starting up.
    """
    return _codex_thread_sync


async def _send_session_status(
    websocket: WebSocket,
    session_id: str,
    is_running: bool,
    session_record: dict | None,
) -> None:
    """Send a ``session_status`` event to the freshly-bound listener.

    Called from the initial WS handshake (only when a turn is in flight so an
    idle client doesn't get a no-op message) and from ``switch_session``
    (always, to refresh client-side ``is_running``/``status``). When the
    session is running, the accumulated stream buffer is attached so the
    client can rebuild ``streamingBlocks``, panels, todos, and interaction
    state without waiting for new events.
    """
    status_msg: dict = {
        "type": "session_status",
        "session_id": session_id,
        "is_running": is_running,
        "status": session_record.get("status") if session_record else "unknown",
    }
    if is_running:
        status_msg["buffered_events"] = broadcaster.get_buffer(session_id)
    await websocket.send_json(status_msg)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan — initialize DB, engine, channels on startup."""
    global _engine, _mcp_manager
    config = get_config()

    # Clear CLAUDECODE env var to prevent nested session detection by claude-agent-sdk
    os.environ.pop("CLAUDECODE", None)

    # Start CLIProxyAPI if enabled (must be up before engine/memU initializes)
    proxy_service = None
    if config.proxy.enabled:
        from nerve.proxy.service import ProxyService
        proxy_service = ProxyService(config)
        try:
            await proxy_service.start()
            logger.info("CLIProxyAPI proxy started on port %d", config.proxy.port)
        except Exception as e:
            logger.error("CLIProxyAPI proxy failed to start: %s", e)
            raise

    # Initialize database
    db_path = Path("~/.nerve/nerve.db").expanduser()
    db = await init_db(db_path, workspace=config.workspace)
    logger.info("Database initialized at %s", db_path)

    # Optional Langfuse observability — must be set up BEFORE the engine
    # creates SDK clients so the configure_claude_agent_sdk() patches are
    # in place when the SDK initializes its OTEL tracer provider. Failures
    # are logged inside init_langfuse() and never propagate.
    init_langfuse(config)

    # Initialize agent engine
    _engine = AgentEngine(config, db)
    await _engine.initialize()

    # Wire up routes
    init_deps(_engine, db)

    # Initialize notification service. The engine has a setter so the
    # per-session ``ToolContext`` constructed inside ``engine.run()``
    # picks up the live reference. We also seed the legacy module
    # global on ``nerve.agent.tools`` so older test fixtures that patch
    # ``tools._notification_service`` directly continue to work.
    from nerve.notifications.service import NotificationService
    from nerve.agent import tools as agent_tools

    notification_service = NotificationService(config, db, _engine)
    _engine.set_notification_service(notification_service)
    agent_tools._notification_service = notification_service
    set_notification_service(notification_service)

    # Start Telegram bot if enabled
    telegram_channel = None
    if config.telegram.enabled and config.telegram.bot_token:
        from nerve.channels.telegram import TelegramChannel
        telegram_channel = TelegramChannel(config, _engine.router)
        telegram_channel.set_notification_service(notification_service)
        _engine.register_channel(telegram_channel)
        await telegram_channel.start()
        logger.info("Telegram bot started")

    # Start cron service
    global _cron_service
    cron_task = None
    try:
        from nerve.cron.service import CronService
        cron = CronService(config, _engine, db)
        await cron.start()
        cron_task = cron
        _cron_service = cron
        logger.info("Cron service started")

        # Wire notification service to source runners for health alerts
        for runner in cron._source_runners:
            runner.set_notification_service(notification_service)

        # Register cron jobs that suppress the session label in notifications
        for job in cron._jobs:
            if not job.show_session_label:
                notification_service.hide_session_label_for(f"cron:{job.id}")
    except Exception as e:
        logger.warning("Cron service failed to start: %s", e)

    # Periodic session cleanup (every 6 hours)
    async def _periodic_cleanup():
        while True:
            await asyncio.sleep(6 * 3600)
            try:
                if _engine:
                    stats = await _engine.sessions.run_cleanup(
                        archive_after_days=config.sessions.archive_after_days,
                        max_sessions=config.sessions.max_sessions,
                    )
                    if stats.get("archived_stale") or stats.get("archived_overflow"):
                        logger.info("Session cleanup: %s", stats)
            except Exception as e:
                logger.error("Session cleanup failed: %s", e)

            # Clean up expired source messages (TTL)
            try:
                deleted = await db.cleanup_expired_messages()
                if deleted:
                    logger.info("Cleaned up %d expired source messages", deleted)
            except Exception as e:
                logger.error("Source message cleanup failed: %s", e)

    cleanup_task = asyncio.create_task(_periodic_cleanup())

    # Periodic memorization sweep
    _memorize_stats["interval_minutes"] = config.sessions.memorize_interval_minutes

    async def _periodic_memorize():
        from datetime import datetime, timezone
        while True:
            await asyncio.sleep(config.sessions.memorize_interval_minutes * 60)
            try:
                if _engine:
                    result = await _engine.run_memorization_sweep()
                    _memorize_stats["last_run_at"] = datetime.now(timezone.utc).isoformat()
                    _memorize_stats["last_result"] = result
                    _memorize_stats["total_runs"] += 1
            except Exception as e:
                logger.error("Memorization sweep failed: %s", e)
                _memorize_stats["total_errors"] += 1
                _memorize_stats["last_result"] = {"error": str(e)}

    memorize_task = asyncio.create_task(_periodic_memorize())

    # Periodic idle client sweep (every 5 minutes)
    async def _periodic_idle_sweep():
        while True:
            await asyncio.sleep(5 * 60)
            try:
                if _engine:
                    await _engine.run_idle_client_sweep()
            except Exception as e:
                logger.error("Idle client sweep failed: %s", e)

    idle_sweep_task = asyncio.create_task(_periodic_idle_sweep())

    # Periodic notification expiry (every 15 minutes)
    async def _periodic_notify_expiry():
        while True:
            await asyncio.sleep(15 * 60)
            try:
                expired = await notification_service.expire_stale()
                if expired:
                    logger.info("Expired %d stale notifications", expired)
            except Exception as e:
                logger.error("Notification expiry failed: %s", e)

    notify_expiry_task = asyncio.create_task(_periodic_notify_expiry())

    # Periodic backup (opt-in). Hourly tick; runs a bundle when the newest
    # one in the target dir is older than interval_hours (or none exists).
    # The heavy work (consistent DB snapshots + tar) runs in a thread so it
    # never blocks the event loop. Failures notify high-priority — a backup
    # that fails silently is worse than no backup at all.
    async def _periodic_backup():
        from nerve import backup as backup_mod

        bcfg = config.backup
        nerve_dir = Path("~/.nerve").expanduser()
        interval_s = max(1, bcfg.interval_hours) * 3600
        target = Path(bcfg.target_dir).expanduser() if bcfg.target_dir else None
        while True:
            await asyncio.sleep(3600)  # hourly tick
            if not bcfg.enabled or target is None:
                continue
            try:
                age = await asyncio.to_thread(
                    backup_mod.latest_bundle_age_seconds, target,
                )
                if age is not None and age < interval_s:
                    continue  # not due yet

                result = await asyncio.to_thread(
                    backup_mod.create_backup,
                    nerve_dir,
                    config.workspace,
                    target,
                    config_dir=config.config_dir,
                    include_workspace=bcfg.include_workspace,
                    include_secrets=True,
                    workspace_excludes=bcfg.workspace_excludes,
                )
                deleted = await asyncio.to_thread(
                    backup_mod.prune, target, bcfg.retention_count,
                )
                size_str = (
                    f"{result.size / (1024 ** 3):.1f} GB"
                    if result.size >= 1024 ** 3
                    else f"{result.size / (1024 ** 2):.0f} MB"
                )
                logger.info(
                    "Scheduled backup OK: %s (%s, pruned %d)",
                    result.path.name, size_str, len(deleted),
                )
                if bcfg.notify_on_success:
                    await notification_service.send_notification(
                        session_id="system",
                        title="💾 Backup OK",
                        body=(
                            f"{size_str}, {len(backup_mod.list_bundles(target))} "
                            f"kept ({result.file_count} files)"
                        ),
                        priority="low",
                    )
            except Exception as e:
                logger.error("Scheduled backup failed: %s", e, exc_info=True)
                if bcfg.notify_on_failure:
                    try:
                        await notification_service.send_notification(
                            session_id="system",
                            title="⚠️ Nerve backup FAILED",
                            body=f"{e}\n\nTarget: {target}",
                            priority="high",
                        )
                    except Exception as ne:
                        logger.error("Backup failure notify failed: %s", ne)

    backup_task = asyncio.create_task(_periodic_backup())

    # Start the external-agents sync service. It re-renders
    # ~/.codex/AGENTS.md, ~/.claude/CLAUDE.md, etc. from the workspace
    # identity files on a timer (config.external_agents.sync_interval_minutes).
    # Failure here is non-fatal: external agents just won't receive
    # automatic updates, but the gateway and MCP endpoint still work.
    global _external_agents_sync
    if config.external_agents.enabled and config.external_agents.targets:
        try:
            from nerve.external_agents.sync_service import SyncService
            _external_agents_sync = SyncService(config)
            await _external_agents_sync.start()
            set_external_agents_sync(_external_agents_sync)
            logger.info(
                "External-agents sync started (%d target(s), interval=%dm)",
                len(config.external_agents.targets),
                config.external_agents.sync_interval_minutes,
            )
        except Exception as e:
            logger.error("Failed to start external-agents sync: %s", e, exc_info=True)
            _external_agents_sync = None

    # Start the Codex thread sync service if enabled. Background tasks
    # are spawned by the service itself — we only need to keep the
    # handle so shutdown can cancel them cleanly.
    global _codex_thread_sync
    try:
        from nerve.sources.codex_threads import build_service as _build_codex_sync
        codex_sync = _build_codex_sync(config, db, broadcaster=broadcaster)
        if codex_sync is not None:
            await codex_sync.start()
            _codex_thread_sync = codex_sync
    except Exception as e:
        logger.error("Failed to start Codex thread sync: %s", e, exc_info=True)
        _codex_thread_sync = None

    # Start the external MCP manager if enabled — its run() context
    # owns the task group for in-flight connections. The deferred mount
    # added in create_app() reads the manager from _mcp_manager once
    # it's set, so the /mcp/v1 path is wired BEFORE the SPA catch-all
    # but only becomes reachable after we enter run() below.
    mcp_run_ctx = None
    if config.mcp_endpoint.enabled:
        try:
            _mcp_manager = _build_mcp_manager(_engine, _engine.registry, config)
            mcp_run_ctx = _mcp_manager.run()
            await mcp_run_ctx.__aenter__()
            logger.info(
                "MCP endpoint live at %s (include_hoa=%s)",
                config.mcp_endpoint.path, config.mcp_endpoint.include_hoa,
            )
        except Exception as e:
            logger.error("Failed to start MCP endpoint: %s", e)
            mcp_run_ctx = None
            _mcp_manager = None

    logger.info("Nerve started on %s:%d", config.gateway.host, config.gateway.port)

    # Send startup notification to the user (Telegram only, silent)
    try:
        await notification_service.send_notification(
            session_id="system",
            title=f"Nerve started (pid {os.getpid()})",
            priority="low",
            channels=["telegram"],
            silent=True,
        )
    except Exception as e:
        logger.error("Failed to send startup notification: %s", e)

    yield

    # Shutdown: stop MCP manager first so in-flight requests finish
    # before we tear down the engine they depend on.
    if mcp_run_ctx is not None:
        try:
            await mcp_run_ctx.__aexit__(None, None, None)
        except Exception as e:
            logger.warning("MCP manager shutdown raised: %s", e)
        _mcp_manager = None

    # Stop Codex thread sync. Origins each get a CancelledError; the
    # service awaits them before returning so cursors are flushed.
    if _codex_thread_sync is not None:
        try:
            await _codex_thread_sync.stop()
        except Exception as e:
            logger.warning("Codex thread sync shutdown raised: %s", e)
        _codex_thread_sync = None

    # Stop the external-agents sync service. Cheap — it just cancels
    # the periodic loop; no per-file cleanup needed because every write
    # is already atomic (temp + rename).
    if _external_agents_sync is not None:
        try:
            await _external_agents_sync.stop()
        except Exception as e:
            logger.warning("External-agents sync shutdown raised: %s", e)
        _external_agents_sync = None

    # Shutdown: stop telegram FIRST, before cancelling background tasks.
    # Background task cancellation propagates through anyio cancel scopes
    # (Starlette runs the lifespan in an anyio context), which can kill
    # the telegram polling task before we get a chance to stop it cleanly.
    if telegram_channel:
        await telegram_channel.stop()
    if cron_task:
        await cron_task.stop()

    notify_expiry_task.cancel()
    backup_task.cancel()
    idle_sweep_task.cancel()
    memorize_task.cancel()
    cleanup_task.cancel()
    await _engine.shutdown()
    # Flush Langfuse spans last — after the engine has reported its final
    # ResultMessage and any in-flight memU spans have completed. ``flush``
    # is sync and may block on the network, so push it to a thread.
    try:
        await asyncio.to_thread(langfuse_flush)
    except Exception as e:
        logger.debug("Langfuse flush during shutdown failed: %s", e)
    await close_db()
    if proxy_service:
        await proxy_service.stop()
    logger.info("Nerve shut down")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Nerve",
        description="Personal AI Assistant",
        version="0.1.0",
        lifespan=lifespan,
    )

    # CORS for development
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Compress JSON responses. Sessions with heavy tool-call blobs can
    # easily emit 1+ MB payloads on /api/sessions/{id}/messages, and most
    # of that compresses ~3-4x. minimum_size=1024 skips tiny responses
    # where the framing overhead would dominate.
    app.add_middleware(GZipMiddleware, minimum_size=1024)

    # REST routes
    app.include_router(register_all_routes())

    # External MCP endpoint (deferred mount — registers /mcp/v1 BEFORE the
    # SPA catch-all so the path isn't shadowed; the manager itself is
    # built in lifespan once the engine is live).
    config = get_config()
    _mount_mcp_deferred(app, config, lambda: _mcp_manager)

    # WebSocket endpoint
    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()

        # Authenticate
        if not await authenticate_websocket(websocket):
            await websocket.close(code=4001, reason="Unauthorized")
            return

        client_id = str(uuid.uuid4())[:8]
        router = _engine.router
        # Reuse the last session for this channel (no sticky period).
        # Only create a brand-new session if none exist at all.
        active_session = await router.get_last_session("web:default")
        if not active_session:
            active_session = await router.get_active_session(
                "web:default", source="web",
            )
        logger.info("WebSocket connected: %s (session: %s)", client_id, active_session)

        # Register as broadcast listener for the active session
        async def ws_broadcast(session_id: str, message: dict):
            try:
                await websocket.send_json(message)
            except Exception:
                pass

        await broadcaster.register(active_session, client_id, ws_broadcast)
        # Also register on __global__ channel for cross-session notifications
        await broadcaster.register("__global__", f"global:{client_id}", ws_broadcast)

        # Inform the client which session they're connected to
        await websocket.send_json({
            "type": "session_switched",
            "session_id": active_session,
        })

        # If a turn is mid-flight (page reload, transient WS drop, sticky
        # reconnect after a network blip), replay the broadcaster buffer so
        # the freshly-bound listener can rebuild the in-flight stream
        # without waiting for new events. Idle sessions get nothing here;
        # they hydrate via REST + the existing ``session_switched`` event.
        if broadcaster.is_buffering(active_session):
            is_running = _engine.is_session_running(active_session)
            session_record = await _engine.db.get_session(active_session)
            await _send_session_status(
                websocket, active_session, is_running, session_record,
            )

        try:
            while True:
                data = await websocket.receive_json()
                msg_type = data.get("type", "")

                if msg_type == "message":
                    # User sent a chat message
                    user_text = data.get("content", "")
                    session_id = data.get("session_id", active_session)
                    file_ids = data.get("file_ids", [])

                    if session_id != active_session:
                        # Switch sessions
                        await broadcaster.unregister(active_session, client_id)
                        active_session = session_id
                        await broadcaster.register(active_session, client_id, ws_broadcast)
                        await router.switch_session("web:default", session_id)

                    # Load uploaded files if any
                    images = None
                    image_refs = None
                    if file_ids:
                        images, image_refs = await _load_uploaded_files(
                            _engine.db, file_ids,
                        )

                    # Run agent in background, store task for stop support
                    task = asyncio.create_task(
                        _engine.run(
                            session_id=session_id,
                            user_message=user_text,
                            source="web",
                            channel="web",
                            images=images or None,
                            image_refs=image_refs or None,
                        )
                    )
                    _engine.register_task(session_id, task)

                elif msg_type == "stop":
                    # User wants to stop the running agent
                    session_id = data.get("session_id", active_session)
                    stopped = await _engine.stop_session(session_id)
                    if not stopped:
                        await websocket.send_json({
                            "type": "error",
                            "session_id": session_id,
                            "error": "No running task to stop",
                        })

                elif msg_type == "switch_session":
                    new_session = data.get("session_id", active_session)
                    await broadcaster.unregister(active_session, client_id)
                    active_session = new_session
                    await broadcaster.register(active_session, client_id, ws_broadcast)
                    # Persist channel mapping so next page load resumes this session
                    await router.switch_session("web:default", new_session)

                    # Send session status (running/idle + buffered events for
                    # reconnect). Unlike the initial-bind branch, we always
                    # ship a status here so the client can flip its
                    # ``isStreaming`` / ``status`` for the newly-selected
                    # session even when the session is idle.
                    is_running = _engine.is_session_running(new_session)
                    session_record = await _engine.db.get_session(new_session)
                    await _send_session_status(
                        websocket, new_session, is_running, session_record,
                    )

                    await websocket.send_json({
                        "type": "session_switched",
                        "session_id": new_session,
                    })

                elif msg_type == "fork":
                    source_id = data.get("session_id", active_session)
                    at_msg = data.get("at_message_id")
                    title = data.get("title")
                    try:
                        fork = await _engine.fork_session(
                            source_id, at_msg, title,
                        )
                        await websocket.send_json({
                            "type": "session_forked",
                            "source_id": source_id,
                            "fork_id": fork["id"],
                            "title": fork.get("title", ""),
                        })
                    except Exception as e:
                        await websocket.send_json({
                            "type": "error",
                            "session_id": source_id,
                            "error": f"Fork failed: {e}",
                        })

                elif msg_type == "resume":
                    session_id = data.get("session_id", active_session)
                    try:
                        await _engine.resume_session(session_id)
                        await websocket.send_json({
                            "type": "session_resumed",
                            "session_id": session_id,
                        })
                    except Exception as e:
                        await websocket.send_json({
                            "type": "error",
                            "session_id": session_id,
                            "error": f"Resume failed: {e}",
                        })

                elif msg_type == "answer_interaction":
                    # User responded to an interactive tool (AskUserQuestion, etc.)
                    session_id = data.get("session_id", active_session)
                    await router.handle_interaction_response(
                        session_id=session_id,
                        interaction_id=data.get("interaction_id", ""),
                        result=data.get("result"),
                        denied=data.get("denied", False),
                        deny_message=data.get("message", ""),
                    )

                elif msg_type == "ping":
                    await websocket.send_json({"type": "pong"})

        except WebSocketDisconnect:
            logger.info("WebSocket disconnected: %s", client_id)
        except Exception as e:
            logger.warning("WebSocket error for %s: %s", client_id, e)
        finally:
            await broadcaster.unregister(active_session, client_id)
            await broadcaster.unregister("__global__", f"global:{client_id}")

    # Health check (no auth required) — must be before static mount
    @app.get("/health")
    async def health():
        return {"status": "ok", "version": "0.1.0"}

    # Serve static web UI files if built
    web_dist = Path(__file__).parent.parent.parent / "web" / "dist"
    if web_dist.exists():
        from fastapi.responses import FileResponse

        # Mount static assets (js, css, etc.)
        app.mount("/assets", StaticFiles(directory=str(web_dist / "assets")), name="assets")

        # SPA catch-all: serve index.html for any non-API, non-asset route
        @app.get("/{path:path}")
        async def spa_fallback(path: str):
            # Serve actual files if they exist (favicon, etc.)
            file_path = web_dist / path
            if file_path.is_file():
                return FileResponse(str(file_path))
            # Otherwise serve index.html for SPA routing
            return FileResponse(str(web_dist / "index.html"))

    return app


async def _load_uploaded_files(
    db: Database, file_ids: list[str],
) -> tuple[list[dict], list[dict]]:
    """Load uploaded files from DB/disk into the engine image format.

    Returns:
        (images, image_refs) where images is the list for engine.run(images=...)
        and image_refs is metadata for storing in the user message blocks column.
    """
    import base64

    records = await db.get_uploaded_files_by_ids(file_ids)
    images: list[dict] = []
    image_refs: list[dict] = []

    for rec in records:
        disk_path = Path(rec["disk_path"])
        if not disk_path.exists():
            logger.warning("Uploaded file not found on disk: %s", disk_path)
            continue

        # Multi-MB reads + base64 of uploads are blocking — off the loop.
        data = await asyncio.to_thread(disk_path.read_bytes)
        file_type = rec["file_type"]
        media_type = rec["media_type"]
        file_id = rec["id"]
        filename = rec["filename"]

        if file_type in ("image", "pdf"):
            b64 = await asyncio.to_thread(
                lambda d=data: base64.b64encode(d).decode("utf-8"),
            )
            images.append({
                "type": "base64",
                "media_type": media_type,
                "data": b64,
            })
            image_refs.append({
                "type": "image" if file_type == "image" else "file",
                "url": f"/api/files/uploads/{file_id}",
                "filename": filename,
                "media_type": media_type,
            })
        else:
            # Text file — will be appended to user message by the engine
            try:
                text_content = data.decode("utf-8")
            except UnicodeDecodeError:
                text_content = f"[Binary file: {filename}, {len(data)} bytes]"
            images.append({
                "type": "text_file",
                "filename": filename,
                "content": text_content,
            })
            image_refs.append({
                "type": "file",
                "url": f"/api/files/uploads/{file_id}",
                "filename": filename,
                "media_type": media_type,
            })

    return images, image_refs


def run_server(config: NerveConfig | None = None) -> None:
    """Run the Nerve server with uvicorn."""
    import uvicorn

    if config is None:
        config = get_config()

    ssl_config = {}
    if config.gateway.ssl.enabled:
        ssl_config = {
            "ssl_certfile": str(config.gateway.ssl.cert),
            "ssl_keyfile": str(config.gateway.ssl.key),
        }

    uvicorn.run(
        create_app(),
        host=config.gateway.host,
        port=config.gateway.port,
        log_level="info",
        **ssl_config,
    )
