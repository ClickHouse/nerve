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
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from nerve.agent.engine import AgentEngine
from nerve.agent.streaming import broadcaster
from nerve.config import NerveConfig, get_config
from nerve.db import Database, init_db, close_db
from nerve.gateway.auth import authenticate_websocket
from nerve.gateway.routes import init_deps, register_all_routes, set_notification_service

logger = logging.getLogger(__name__)

# Global references
_engine: AgentEngine | None = None
_cron_service = None  # CronService

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan — initialize DB, engine, channels on startup."""
    global _engine
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
    db = await init_db(db_path)
    logger.info("Database initialized at %s", db_path)

    # Initialize agent engine
    _engine = AgentEngine(config, db)
    await _engine.initialize()

    # Wire up routes
    init_deps(_engine, db)

    # Initialize notification service
    from nerve.notifications.service import NotificationService
    from nerve.agent import tools as agent_tools

    notification_service = NotificationService(config, db, _engine)
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

    # Shutdown: stop telegram FIRST, before cancelling background tasks.
    # Background task cancellation propagates through anyio cancel scopes
    # (Starlette runs the lifespan in an anyio context), which can kill
    # the telegram polling task before we get a chance to stop it cleanly.
    if telegram_channel:
        await telegram_channel.stop()
    if cron_task:
        await cron_task.stop()

    notify_expiry_task.cancel()
    idle_sweep_task.cancel()
    memorize_task.cancel()
    cleanup_task.cancel()
    await _engine.shutdown()
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

    # REST routes
    app.include_router(register_all_routes())

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

                    # Send session status (running/idle + buffered events for reconnect)
                    is_running = _engine.is_session_running(new_session)
                    session_record = await _engine.db.get_session(new_session)
                    status_msg: dict = {
                        "type": "session_status",
                        "session_id": new_session,
                        "is_running": is_running,
                        "status": session_record.get("status") if session_record else "unknown",
                    }
                    if is_running:
                        status_msg["buffered_events"] = broadcaster.get_buffer(new_session)
                    await websocket.send_json(status_msg)

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

        data = disk_path.read_bytes()
        file_type = rec["file_type"]
        media_type = rec["media_type"]
        file_id = rec["id"]
        filename = rec["filename"]

        if file_type in ("image", "pdf"):
            b64 = base64.b64encode(data).decode("utf-8")
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
