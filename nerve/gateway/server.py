"""FastAPI application — HTTP API, WebSocket endpoint, static file serving.

Single entry point for the entire Nerve gateway.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import os
import re
import socket
import sys
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Any, Callable

import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles

from nerve import paths
from nerve.agent.agents.emitter import agent_channel
from nerve.agent.engine import AgentEngine, NotAcceptingTurnsError
from nerve.agent.streaming import broadcaster
from nerve.config import NerveConfig, get_config
from nerve.db import Database, init_db, close_db
from nerve.gateway.auth import (
    ACTOR_CONTEXT_HEADER,
    AUTH_MODE_EXTERNAL,
    SESSION_TOKEN_HEADER,
    auth_mode_from_env,
    authenticate_websocket,
    identity_store,
    is_external_mode,
    pin_auth_mode,
)
from nerve.identity import (
    Actor,
    ActorResolutionError,
    actor_for_account,
    attribution_for,
)
from nerve.gateway.routes import (
    init_deps,
    register_all_routes,
    set_external_agents_sync,
    set_notification_service,
)
from nerve.mcp_server import build_manager as _build_mcp_manager, mount_deferred as _mount_mcp_deferred
from nerve.mcp_server.loopback import McpLoopbackServer
from nerve.observability.langfuse import (
    flush as langfuse_flush,
    init_langfuse,
)
from nerve.platform.activity import LEASE_BACKUP, keep_awake
from nerve.setup_checks import scrub_secrets as _scrub
from nerve.utils.aio import stop_background_task

logger = logging.getLogger(__name__)

# Global references
_engine: AgentEngine | None = None
_cron_service = None  # CronService
_workflow_run_service = None  # WorkflowRunService (nerve.workflows)
_review_loop_service = None  # ReviewLoopService (nerve.workflows.review_loop)
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
# HostedChannelRuntime assigned during lifespan when a provider is in hosted
# mode. The channel stream endpoint refuses every upgrade while it is None.
_hosted_channels = None
# ActivityReporter (nerve.platform.activity), assigned during lifespan only
# when the platform's local activity socket exists.
_activity_reporter = None

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


@dataclass(frozen=True, slots=True)
class WebSocketConnection:
    """What a live WebSocket is, fixed at accept and never rewritten.

    The record is frozen because a socket may stay open for hours. Mutable
    identity could change halfway through a conversation: the actor
    resolved at accept is the actor every message on this connection is
    attributed to. Account admission state may close a stale socket, but never
    rewrites the actor stored here; a new connection resolves from scratch.

    Session selection stays in the handler because clients switch sessions over
    the same socket.
    """

    client_id: str
    actor: Actor


async def _accept_websocket(websocket: WebSocket) -> WebSocketConnection | None:
    """Accept a socket, authenticate it, and fix its actor for good.

    The one place a WebSocket's identity is decided. Returns ``None`` after
    closing the socket when the credential is missing, invalid, or names no
    account this instance can act for (a disabled account, or a pre-account
    token on an install that now has two).
    """
    await websocket.accept()
    actor = await authenticate_websocket(websocket)
    if actor is None:
        await websocket.close(code=4001, reason="Unauthorized")
        return None
    return WebSocketConnection(client_id=str(uuid.uuid4())[:8], actor=actor)


# Open sockets by client id, so that disabling an account can close its sockets.
# Its size is the client connection count the activity reporter sends.
_live_sockets: dict[str, tuple[WebSocketConnection, WebSocket]] = {}


def _connections_changed() -> None:
    """Tell the activity reporter, if one runs, that a client came or went."""
    reporter = _activity_reporter
    if reporter is not None:
        reporter.notify()


# Runs started by WebSocket messages. The loop only keeps weak references to
# tasks; these keep them alive until they finish (Stop finds a run through
# the session's turn task, not through this set).
_ws_runs: set[asyncio.Task] = set()


def _ws_run_done(task: asyncio.Task) -> None:
    """Forget a finished WebSocket run. A run refused by a drain is no error."""
    _ws_runs.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if isinstance(exc, NotAcceptingTurnsError):
        # The engine already told the session's clients.
        logger.info("Draining: refused a queued web turn")
    elif exc is not None:
        logger.error("Web turn failed", exc_info=exc)

# Agent transcript channels one socket may follow at once; subscribing to one
# more drops the oldest.
MAX_AGENT_SUBSCRIPTIONS = 8
_MAX_CLIENT_MSG_ID = 128


def _client_msg_id(raw: Any) -> str | None:
    """The sender's message id from a WebSocket frame, or ``None``."""
    if not isinstance(raw, str):
        return None
    raw = raw.strip()
    return raw if raw and len(raw) <= _MAX_CLIENT_MSG_ID else None

WS_ACCOUNT_DISABLED_CODE = 1008
WS_ACCOUNT_DISABLED_REASON = "Account disabled"


async def _account_enabled(actor: Actor) -> bool:
    """Whether the actor may still use a socket. The system principal may.

    In external mode, a person that the gateway names has no local account,
    and the gateway decides access.
    """
    if actor.is_system:
        return True
    if is_external_mode() and actor.account_id is None:
        return True
    store = identity_store()
    if store is None or not actor.account_id:
        return False
    try:
        await actor_for_account(store, actor.account_id)
    except ActorResolutionError:
        return False
    return True


async def close_account_sockets(account_id: str) -> None:
    """Close every open WebSocket of one account."""
    for client_id, (connection, websocket) in list(_live_sockets.items()):
        if connection.actor.account_id != account_id:
            continue
        _live_sockets.pop(client_id, None)
        _connections_changed()
        try:
            await websocket.close(
                code=WS_ACCOUNT_DISABLED_CODE, reason=WS_ACCOUNT_DISABLED_REASON,
            )
        except Exception as e:  # noqa: BLE001 - it may be closed already
            logger.debug("WebSocket %s could not be closed: %s", client_id, e)


async def _agent_snapshot(session_id: str) -> dict:
    """The session's ``AgentSnapshot`` (an empty one when the engine cannot
    answer: ``session_status`` always carries the key)."""
    from nerve.agent.agents.registry import empty_snapshot

    snapshot = getattr(_engine, "agent_snapshot", None)
    if snapshot is not None:
        try:
            return await snapshot(session_id)
        except Exception:  # noqa: BLE001 — a status must still go out
            logger.warning("agent snapshot failed for %s", session_id, exc_info=True)
    return empty_snapshot(session_id)


async def _send_session_status(
    websocket: WebSocket,
    session_id: str,
    is_running: bool,
    session_record: dict | None,
    *,
    agents: dict | None = None,
) -> None:
    """Send a ``session_status`` event to the freshly-bound listener.

    Sent on every bind: the initial WS handshake and ``switch_session``, so
    the client refreshes ``is_running``/``status`` and hydrates the agent
    roster (``agents``, the session's ``AgentSnapshot``) even when no turn
    runs. When the session is running, the accumulated stream buffer is
    attached so the client can rebuild ``streamingBlocks``, panels, todos,
    and interaction state without waiting for new events.
    """
    status_msg: dict = {
        "type": "session_status",
        "session_id": session_id,
        "is_running": is_running,
        "status": session_record.get("status") if session_record else "unknown",
    }
    if is_running:
        status_msg["buffered_events"] = broadcaster.get_buffer(session_id)
    status_msg["agents"] = agents if agents is not None else await _agent_snapshot(session_id)
    await websocket.send_json(status_msg)


async def _periodic_db_retention(db) -> None:
    """Opt-in DB retention loop: compact old memorized messages' blocks/thinking
    JSON, prune append-only telemetry + file snapshots, checkpoint the WAL.

    The file shrink (VACUUM) is an explicit operator step (``nerve db vacuum``),
    never on this loop.

    Every setting is read from the current config each cycle, so the interval and
    the retention windows follow a config reload, and switching retention off
    stops the work from the next tick. Switching it *on* needs a restart: with no
    task running there is nothing left to notice the flag.
    """
    if not get_config().retention.enabled:
        return
    while True:
        retention = get_config().retention
        await asyncio.sleep(retention.interval_hours * 3600)
        retention = get_config().retention
        if not retention.enabled:
            continue
        try:
            report = await db.run_retention(
                retention_days=retention.retention_days,
                retention_full_days=retention.retention_full_days,
            )
            if (
                report.get("messages_compacted")
                or report.get("telemetry_deleted")
                or report.get("snapshots_deleted")
            ):
                logger.info("DB retention: %s", report)
        except Exception as e:
            logger.error("DB retention failed: %s", e)


async def _periodic_backup(notification_service) -> None:
    """Opt-in backup loop: hourly tick, runs a bundle when the newest one in the
    target dir is older than ``backup.interval_hours`` (or none exists).

    The heavy work (consistent DB snapshots + tar) runs in a thread so it never
    blocks the event loop. Failures notify high-priority — a backup that fails
    silently is worse than no backup at all.

    The tick runs whether or not backups are on and reads the current config each
    time, so every ``backup.*`` setting — enabling it included — takes effect
    without a restart.
    """
    from nerve import backup as backup_mod

    nerve_dir = paths.nerve_home()
    while True:
        await asyncio.sleep(3600)  # hourly tick
        config = get_config()
        bcfg = config.backup
        interval_s = max(1, bcfg.interval_hours) * 3600
        target = Path(bcfg.target_dir).expanduser() if bcfg.target_dir else None
        if not bcfg.enabled or target is None:
            continue
        try:
            age = await asyncio.to_thread(
                backup_mod.latest_bundle_age_seconds, target,
            )
            if age is not None and age < interval_s:
                continue  # not due yet

            with keep_awake(LEASE_BACKUP):
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan — initialize DB, engine, channels on startup."""
    global _engine, _mcp_manager, _hosted_channels, _activity_reporter
    config = get_config()
    # Create the state directory owner-only before anything writes to it.
    # Database.connect refuses a state directory that other users can write to.
    paths.ensure_nerve_home()

    # Remember the configuration this process starts with: "is a restart
    # pending" is answered by comparing it with what is on disk later. And
    # retire the setup checklist's notes from the previous process — its
    # restart debts and start-up errors describe a process that no longer runs.
    from nerve import config_reload, setup_state

    config_reload.remember_startup_config(config)
    try:
        _setup_notes = setup_state.load_state()
        if setup_state.retire_on_boot(_setup_notes):
            setup_state.save_state(_setup_notes)
    except Exception as e:  # noqa: BLE001 — the wizard's scratch file must never stop a boot
        logger.warning("Setup state could not be retired at boot: %s", e)

    # Clear CLAUDECODE env var to prevent nested session detection by claude-agent-sdk
    os.environ.pop("CLAUDECODE", None)

    # If startup fails before the ``yield``, the shutdown code below it does
    # not run. So each resource registers its stop function as soon as it
    # starts, and _unwind_startup calls them in reverse order. Otherwise a
    # failure could leave the detached proxy subprocess holding its port.
    startup_cleanups: list[tuple[str, Callable[[], Any]]] = []

    async def _unwind_startup() -> None:
        # Catch BaseException: this often runs during a cancellation, and a
        # CancelledError from one stop must not skip the stops after it.
        for label, stop in reversed(startup_cleanups):
            try:
                result = stop()
                if inspect.isawaitable(result):
                    await result
            except BaseException as e:  # noqa: BLE001 - one failed stop must not skip the rest
                logger.warning("Startup unwind: stopping %s raised: %r", label, e)

    try:
        # Open the database first. It is the step most likely to fail (unsafe
        # state files, a migration, the identity bootstrap), and it does not
        # need the proxy, so such a failure leaves nothing to clean up.
        db_path = paths.db_path()
        db = await init_db(db_path, workspace=config.workspace)
        startup_cleanups.append(("database", close_db))
        logger.info("Database initialized at %s", db_path)

        # Create the owner account and signing secret if they do not exist.
        # This must run before anything mints or checks a token. A failure
        # stops startup: without a signing secret, authentication cannot work.
        from nerve.migrate import bootstrap_identity

        identity_report = await bootstrap_identity(db, config)
        for action in identity_report.identity_actions:
            logger.info("Identity bootstrap: %s", action)

        # Create the setup token before serving. Never log it: `nerve status`
        # reads it from the database. External mode has no local setup, so a
        # token left from local mode is deleted.
        from nerve import setup_token

        await setup_token.ensure_setup_token(
            db,
            unclaimed=(
                not is_external_mode()
                and await setup_token.instance_is_unclaimed(db, config)
            ),
        )

        # Start CLIProxyAPI if enabled (must be up before engine/memU initializes)
        proxy_service = None
        if config.proxy.enabled:
            from nerve.proxy.service import ProxyService
            proxy_service = ProxyService(config)
            # Register the stop before the await: start() creates the
            # subprocess and then waits for health, and a cancellation during
            # that wait must still stop it. stop() does nothing if no process
            # exists.
            startup_cleanups.append(("CLIProxyAPI proxy", proxy_service.stop))
            try:
                await proxy_service.start()
                logger.info("CLIProxyAPI proxy started on port %d", config.proxy.port)
            except Exception as e:
                logger.error("CLIProxyAPI proxy failed to start: %s", e)
                raise
        elif config.ollama.enabled:
            # Ollama needs the proxy as its Anthropic↔OpenAI translation layer.
            logger.warning(
                "ollama.enabled is true but proxy.enabled is false — Ollama "
                "models require the CLIProxyAPI proxy and will NOT be offered. "
                "Set proxy.enabled: true to use local Ollama models.",
            )

        # Optional Langfuse observability — must be set up BEFORE the engine
        # creates SDK clients so the configure_claude_agent_sdk() patches are
        # in place when the SDK initializes its OTEL tracer provider. Failures
        # are logged inside init_langfuse() and never propagate.
        init_langfuse(config)

        # Initialize agent engine. Register the shutdown before the await:
        # initialize() starts memU's non-daemon thread before database writes
        # that can fail, and that thread would keep a failed process alive.
        # shutdown() does nothing on an engine that did not start.
        _engine = AgentEngine(config, db)
        startup_cleanups.append(("agent engine", _engine.shutdown))
        await _engine.initialize()

        # Wire up routes
        init_deps(_engine, db)

        # Prime the Anthropic model catalog so the composer's model picker
        # offers every model these credentials can reach (instead of a built-in
        # list that goes stale on each release). Off the critical path and
        # best-effort: until it lands — or if it fails — the picker falls back
        # to the configured/built-in list. Runs after the proxy is up, since
        # discovery goes through it when proxy.enabled.
        models_prime_task = None
        if config.agent.model_discovery:
            from nerve import models_catalog

            models_prime_task = asyncio.create_task(models_catalog.prime(config))
            startup_cleanups.append(("models_prime_task", models_prime_task.cancel))

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

        # Start the external MCP manager and its Codex-facing loopback listener
        # before cron scheduling. The loopback listener serves the ASGI app
        # directly and is independent of the public gateway socket/TLS setup.
        mcp_run_ctx = None
        mcp_loopback_server = None
        if config.mcp_endpoint.enabled:
            try:
                _mcp_manager = _build_mcp_manager(_engine, _engine.registry, config)
                mcp_run_ctx = _mcp_manager.run()
                await mcp_run_ctx.__aenter__()
                startup_cleanups.append(
                    ("MCP manager", lambda: mcp_run_ctx.__aexit__(None, None, None)),
                )
                logger.info(
                    "MCP endpoint live at %s (include_hoa=%s)",
                    config.mcp_endpoint.path, config.mcp_endpoint.include_hoa,
                )
            except Exception as e:
                logger.error("Failed to start MCP endpoint: %s", e)
                mcp_run_ctx = None
                _mcp_manager = None

            if _mcp_manager is not None:
                try:
                    from nerve.gateway.routes.codex import router as codex_router

                    loopback_app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
                    _mount_mcp_deferred(
                        loopback_app, config, lambda: _mcp_manager,
                    )
                    loopback_app.include_router(codex_router)
                    mcp_loopback_server = await McpLoopbackServer.start(loopback_app)
                    startup_cleanups.append(
                        ("MCP loopback listener", mcp_loopback_server.close),
                    )
                    _engine.set_mcp_loopback_port(mcp_loopback_server.port)
                    logger.info(
                        "Codex MCP loopback listener live on 127.0.0.1:%d",
                        mcp_loopback_server.port,
                    )
                except Exception as e:
                    logger.error("Failed to start Codex MCP loopback listener: %s", e)

        # Start Telegram bot if enabled. A channel must never take the gateway
        # down: ``TelegramChannel.start()`` runs ``Application.initialize()``,
        # which calls getMe, so a revoked token would otherwise abort the whole
        # lifespan — including the restart the setup wizard itself ends with.
        # The failure is logged with the token scrubbed (python-telegram-bot
        # embeds it in the message), recorded for the setup page and `nerve
        # doctor`, and the gateway keeps booting without the channel.
        telegram_channel = None
        if config.telegram.enabled and config.telegram.bot_token:
            from nerve.channels.telegram import TelegramChannel
            # get_config, not the object read above: the channel resolves config per
            # use so a reload reaches the reads that happen per update (dm_policy).
            channel = TelegramChannel(get_config, _engine.router)
            channel.set_notification_service(notification_service)
            try:
                await channel.start()
            except Exception as exc:  # noqa: BLE001 — a channel must never take the gateway down
                text = _scrub(f"{type(exc).__name__}: {exc}", config.telegram.bot_token)
                logger.error("[ERR] Telegram bot failed to start: %s", text)
                try:
                    setup_state.record_boot_error("telegram", text)
                except Exception as e:  # noqa: BLE001
                    logger.warning("Could not record the Telegram start failure: %s", e)
                try:
                    await channel.stop()
                except Exception:  # noqa: BLE001 — half-built application
                    pass
            else:
                telegram_channel = channel
                # Registered only after a successful start, so a half-initialised
                # channel is never reachable from the engine.
                _engine.register_channel(telegram_channel)
                startup_cleanups.append(("Telegram bot", telegram_channel.stop))
                logger.info("Telegram bot started")

        # Install the lifecycle owner even when Slack starts disabled, so a later
        # reload can enable it without rebuilding gateway state.
        from nerve.channels.slack_runtime import SlackRuntime

        slack_runtime = SlackRuntime(_engine.router, notification_service)
        _engine.register_channel_runtime("slack", slack_runtime)
        startup_cleanups.append(("Slack runtime", slack_runtime.shutdown))
        try:
            slack_outcome = await slack_runtime.reconcile(config)
            if slack_outcome:
                logger.info("Slack bot %s", slack_outcome)
        except Exception as e:
            # Slack is optional; its runtime leaves a failed candidate absent and
            # retryable while the web UI and other channels continue starting.
            logger.error("Slack bot failed to start: %s", e, exc_info=True)

        # Hosted channels and sources: a shared gateway holds the provider
        # connections, polls the hosted sources, and streams admitted events to
        # /_internal/channel/v1/stream. Optional like Slack: a failure leaves
        # the endpoint refusing upgrades.
        from nerve.channels.hosted.runtime import HostedChannelRuntime, hosted_providers

        if hosted_providers(config):
            try:
                runtime = HostedChannelRuntime(
                    config, _engine.router, get_config,
                    notification_service=notification_service,
                    db=db,
                )
                await runtime.start()
                startup_cleanups.append(("hosted channels", runtime.stop))
                _hosted_channels = runtime
                _engine.register_channel_runtime("hosted", runtime)
            except Exception as e:
                logger.error("Hosted channels failed to start: %s", e, exc_info=True)

        # Start cron service
        global _cron_service
        cron_task = None
        ws_sync_task = None
        ws_sync_stop = None
        try:
            from nerve.cron.service import CronService
            cron = CronService(config, _engine, db)
            # Wire health-alert notifications before start() so source runners built
            # during start (and any later reload) pick it up.
            cron.notification_service = notification_service
            await cron.start()
            cron_task = cron
            startup_cleanups.append(("cron service", cron.stop))
            _cron_service = cron
            logger.info("Cron service started")
            if _hosted_channels is not None:
                # A hosted source's records reach the inbox as they arrive, and
                # the jobs gated on that source run soon after.
                _hosted_channels.set_delivery_listener(cron.on_source_delivered)

            # Register cron jobs that suppress the session label in notifications
            for job in cron._jobs:
                if not job.show_session_label:
                    notification_service.hide_session_label_for(f"cron:{job.id}")
        except Exception as e:
            logger.warning("Cron service failed to start: %s", e)

        # Report activity to the platform's local activity socket, when this
        # machine has one (nerve/platform/activity.py): running turns, client
        # connections, the next scheduled task, and keep-awake leases. Optional
        # in every way: a reporter that fails leaves reporting off, never the
        # gateway down.
        try:
            from nerve.platform.activity import ActivityReporter

            reporter = ActivityReporter(
                config, _engine, db, cron_task,
                connection_count=lambda: len(_live_sockets),
            )
            startup_cleanups.append(("activity reporter", reporter.stop))
            if await reporter.start():
                _activity_reporter = reporter
        except Exception:  # reporting must never stop the gateway
            logger.exception("Activity reporter failed to start")

        # Start the workflow-run service (budget-capped multi-agent jobs).
        # After notification_service wiring so budget alerts can deliver, and
        # after engine init so runs execute against live backends. A startup
        # failure here must be LOUD, not a silent warning — runs are
        # budget-enforced only while this service is alive.
        global _workflow_run_service
        try:
            from nerve.workflows import init_workflow_run_service

            _workflow_run_service = init_workflow_run_service(config, db, _engine)
            if _workflow_run_service is not None:
                await _workflow_run_service.start()
                startup_cleanups.append(
                    ("workflow run service", _workflow_run_service.stop),
                )
                logger.info("Workflow run service started")
                if _activity_reporter is not None:
                    try:
                        _activity_reporter.watch_workflow_runs(_workflow_run_service)
                    except Exception as e:  # noqa: BLE001 — reporting is optional
                        logger.error("Activity reporter cannot watch workflow runs: %s", e)
        except Exception as e:
            logger.error("Workflow run service failed to start: %s", e)
            _workflow_run_service = None
            try:
                # Drop the module singleton too — otherwise REST/MCP/cron keep
                # reaching a monitor-less service and "budgeted" runs start with
                # no budget enforcement (fail-open).
                from nerve.workflows import reset_workflow_run_service

                reset_workflow_run_service()
            except Exception:
                pass
            try:
                await notification_service.send_notification(
                    session_id="system",
                    title="Workflow run service failed to start",
                    body=f"Budget-capped workflow runs are unavailable: {e}",
                    priority="high",
                )
            except Exception:
                pass

        # Review loops ride on workflow runs. STRICTLY after
        # workflow_run_service.start(): the runs orphan-recovery pass must
        # finish before the loop recovery pass reads leg statuses (and the
        # completion listener must not observe recovery transitions).
        global _review_loop_service
        if _workflow_run_service is not None:
            try:
                from nerve.workflows import init_review_loop_service

                _review_loop_service = init_review_loop_service(
                    get_config, db, _engine, _workflow_run_service,
                )
                if _review_loop_service is not None:
                    await _review_loop_service.start()
                    startup_cleanups.append(
                        ("review loop service", _review_loop_service.stop),
                    )
                    logger.info("Review loop service started")
            except Exception as e:
                logger.error("Review loop service failed to start: %s", e)
                _review_loop_service = None
                try:
                    # Drop the module singleton too — routes/MCP must see the
                    # feature as unavailable, not a half-started zombie whose
                    # worker/reconcile tasks never came up.
                    from nerve.workflows import reset_review_loop_service

                    reset_review_loop_service()
                except Exception:
                    pass
                try:
                    await notification_service.send_notification(
                        session_id="system",
                        title="Review loop service failed to start",
                        body=f"Review loops are unavailable: {e}",
                        priority="high",
                    )
                except Exception:
                    pass

        # One-shot cleanup of retired houseofagents artifacts. Gated on the
        # NERVE-MANAGED binary existing (our own bin/ is ours): a standalone
        # houseofagents install the user runs outside Nerve keeps its
        # ~/.config/houseofagents/config.toml untouched. When it was ours, the
        # config.toml holds plaintext API keys Nerve wrote — park it out of the
        # way; the binary is re-downloadable and just deleted. Best-effort —
        # never blocks startup. The two Nerve-owned paths go through the path
        # provider so a NERVE_HOME install cleans up its own artifacts instead of
        # inspecting a directory it never wrote to; the houseofagents config path
        # is that tool's own and stays literal.
        try:
            hoa_binary = paths.nerve_path("bin", "houseofagents")
            if hoa_binary.exists():
                hoa_config = Path("~/.config/houseofagents/config.toml").expanduser()
                if hoa_config.exists() and not hoa_config.is_symlink():
                    retired_dir = paths.nerve_path("houseofagents-retired")
                    retired_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
                    hoa_config.rename(retired_dir / "config.toml.bak")
                    logger.info(
                        "houseofagents retired: moved %s to %s",
                        hoa_config, retired_dir / "config.toml.bak",
                    )
                hoa_binary.unlink()
                logger.info("houseofagents retired: deleted binary %s", hoa_binary)
        except Exception as e:
            logger.warning("houseofagents artifact cleanup failed: %s", e)

        # Periodically pull the workspace from its git remote and apply (opt-in).
        if config.workspace_sync.enabled:
            from nerve.sync_service import run_periodic_sync
            ws_sync_stop = asyncio.Event()
            ws_sync_task = asyncio.create_task(
                run_periodic_sync(config, _engine, _cron_service, ws_sync_stop)
            )
            startup_cleanups.append(("workspace sync", ws_sync_task.cancel))

        # Periodic session cleanup. Default cadence is every 6 hours (unchanged);
        # it tightens to hourly only when the opt-in interactive idle auto-close
        # (sessions.interactive_archive_after_hours > 0) is enabled and needs finer resolution.
        #
        # This and the loops below re-read get_config() per cycle rather than closing
        # over the start-up object: a config reload replaces that object, and a loop
        # holding the old one would keep applying settings the operator has already
        # changed, with nothing to show for it.
        async def _periodic_cleanup():
            while True:
                interval = (
                    3600
                    if get_config().sessions.interactive_archive_after_hours > 0
                    else 6 * 3600
                )
                await asyncio.sleep(interval)
                try:
                    if _engine:
                        sessions = get_config().sessions
                        stats = await _engine.sessions.run_cleanup(
                            archive_after_days=sessions.archive_after_days,
                            max_sessions=sessions.max_sessions,
                            interactive_archive_after_hours=sessions.interactive_archive_after_hours,
                        )
                        if (
                            stats.get("archived_stale")
                            or stats.get("archived_overflow")
                            or stats.get("archived_interactive")
                        ):
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
        startup_cleanups.append(("cleanup_task", cleanup_task.cancel))

        # Periodic memorization sweep
        _memorize_stats["interval_minutes"] = config.sessions.memorize_interval_minutes

        async def _periodic_memorize():
            from datetime import datetime, timezone
            while True:
                interval_minutes = get_config().sessions.memorize_interval_minutes
                # Report the current reloadable interval in diagnostics.
                _memorize_stats["interval_minutes"] = interval_minutes
                await asyncio.sleep(interval_minutes * 60)
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
        startup_cleanups.append(("memorize_task", memorize_task.cancel))

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
        startup_cleanups.append(("idle_sweep_task", idle_sweep_task.cancel))

        # Periodic notification maintenance (every 15 minutes): re-deliver
        # snoozed rows, then expire stale ones. Ordered so a row whose
        # redeliver_at AND expires_at both passed gets its last chance
        # (re-delivery restarts the expiry window) instead of dying.
        async def _periodic_notify_maintenance():
            while True:
                await asyncio.sleep(15 * 60)
                try:
                    redelivered = await notification_service.redeliver_due()
                    if redelivered:
                        logger.info(
                            "Re-delivered %d snoozed notifications", redelivered,
                        )
                except Exception as e:
                    logger.error("Notification re-delivery failed: %s", e)
                try:
                    expired = await notification_service.expire_stale()
                    if expired:
                        logger.info("Expired %d stale notifications", expired)
                except Exception as e:
                    logger.error("Notification expiry failed: %s", e)

        notify_maintenance_task = asyncio.create_task(_periodic_notify_maintenance())
        startup_cleanups.append(("notify_maintenance_task", notify_maintenance_task.cancel))

        # Both opt-in, both no-ops unless their config says otherwise; see their
        # docstrings for which of their settings survive a config reload.
        db_retention_task = asyncio.create_task(_periodic_db_retention(db))
        startup_cleanups.append(("db_retention_task", db_retention_task.cancel))
        backup_task = asyncio.create_task(_periodic_backup(notification_service))
        startup_cleanups.append(("backup_task", backup_task.cancel))

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
                startup_cleanups.append(
                    ("external-agents sync", _external_agents_sync.stop),
                )
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
                startup_cleanups.append(("Codex thread sync", codex_sync.stop))
                _codex_thread_sync = codex_sync
        except Exception as e:
            logger.error("Failed to start Codex thread sync: %s", e, exc_info=True)
            _codex_thread_sync = None

        logger.info("Nerve started on %s:%d", config.gateway.host, config.gateway.port)

        # Send startup notification to the user (Telegram only, silent). Skipped
        # — not warned about — when Telegram is off or failed to start: an
        # unconfigured instance must boot quietly.
        if telegram_channel is not None:
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

        # Re-drive any sessions enrolled via `nerve restart --resume`. Runs as a
        # background task so a (possibly long) resumed turn never blocks startup;
        # the engine, notification service and channels are all wired by now.
        asyncio.create_task(_engine.resume_enrolled_sessions())
    except BaseException:
        # The shutdown code after the yield will not run, so stop what started
        # here, then re-raise the original error.
        logger.error(
            "Startup failed; stopping what had already started", exc_info=True,
        )
        await _unwind_startup()
        raise

    yield

    # Refuse new turns while the services below stop. The server's drain
    # (see _DrainingServer) usually did this already.
    _engine.stop_accepting_turns()

    # Stop the loopback listener before the manager so no new requests
    # arrive while the MCP task group is shutting down.
    if mcp_loopback_server is not None:
        try:
            await mcp_loopback_server.close()
        except Exception as e:
            logger.warning("MCP loopback listener shutdown raised: %s", e)
        if _engine is not None:
            _engine.set_mcp_loopback_port(None)

    # Shutdown: stop MCP manager before the engine it depends on.
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

    # Stop the external-agents sync service. It exits through its own stop event
    # so a sweep in flight finishes the whole target set rather than stopping
    # partway down it; cancellation is the backstop. Individual writes need no
    # cleanup — each is atomic (temp + rename).
    if _external_agents_sync is not None:
        try:
            await _external_agents_sync.stop()
        except Exception as e:
            logger.warning("External-agents sync shutdown raised: %s", e)
        _external_agents_sync = None

    # Shutdown: stop the chat channels FIRST, before cancelling background
    # tasks. Background task cancellation propagates through anyio cancel
    # scopes (Starlette runs the lifespan in an anyio context), which can kill
    # the polling and socket tasks before we get a chance to stop them cleanly.
    if _hosted_channels is not None:
        try:
            await _hosted_channels.stop()
        except Exception as e:
            logger.warning("Hosted channel shutdown raised: %s", e)
        _hosted_channels = None
    if telegram_channel:
        await telegram_channel.stop()
    await slack_runtime.shutdown()
    if ws_sync_task:
        # Exit through the loop's own stop path rather than cancelling it where
        # it stands: a cycle interrupted between the merge and the reload leaves
        # the workspace on the new commit with the daemon still running the old
        # config. The git phase runs in a worker thread and so is out of reach of
        # cancellation either way; the bounded wait is for the reload that
        # follows it. Cancellation is the backstop, not the mechanism.
        await stop_background_task(ws_sync_task, ws_sync_stop, "Workspace sync")
    # Before the cron service and the engine it reads from. Its last report
    # says nothing runs any more; its leases are released.
    if _activity_reporter is not None:
        try:
            await _activity_reporter.stop()
        except Exception as e:  # noqa: BLE001
            logger.warning("Activity reporter shutdown raised: %s", e)
        _activity_reporter = None
    if cron_task:
        await cron_task.stop()
    if _review_loop_service is not None:
        try:
            await _review_loop_service.stop()
        except Exception as e:
            logger.warning("Review loop service shutdown raised: %s", e)
        _review_loop_service = None
    if _workflow_run_service is not None:
        try:
            await _workflow_run_service.stop()
        except Exception as e:
            logger.warning("Workflow run service shutdown raised: %s", e)
        _workflow_run_service = None

    db_retention_task.cancel()
    notify_maintenance_task.cancel()
    backup_task.cancel()
    idle_sweep_task.cancel()
    memorize_task.cancel()
    cleanup_task.cancel()
    if models_prime_task is not None and not models_prime_task.done():
        models_prime_task.cancel()
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
    """Create and configure the FastAPI application.

    Reads ``NERVE_AUTH_MODE`` once and pins it before any route is
    registered. An unknown value raises :class:`ConfigError` and stops
    startup.
    """
    pin_auth_mode(auth_mode_from_env())
    if is_external_mode():
        logger.warning(
            "Authentication mode is %s: Nerve trusts the %s header without a "
            "signature check. Only the gateway must be able to reach this server.",
            AUTH_MODE_EXTERNAL, ACTOR_CONTEXT_HEADER,
        )

    app = FastAPI(
        title="Nerve",
        description="Personal AI Assistant",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Render a lockdown violation as a clean 403 instead of an opaque 500.
    from fastapi.responses import JSONResponse

    from nerve.config import LockdownError

    @app.exception_handler(LockdownError)
    async def _lockdown_handler(request, exc: LockdownError):  # noqa: ANN001
        return JSONResponse(status_code=403, content={"detail": str(exc)})

    # A malformed skill id is the caller's mistake, not a server fault, and the
    # id arrives as a path segment so it is trivially reachable.
    from nerve.skills.manager import SkillIdError

    @app.exception_handler(SkillIdError)
    async def _skill_id_handler(request, exc: SkillIdError):  # noqa: ANN001
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    # Setup field errors render as ``{"detail", "field"}`` so the wizard can
    # attach the message to the input it is about.
    from nerve.gateway.routes.setup import install_handlers as _install_setup_handlers

    _install_setup_handlers(app)

    # A route that starts a turn during a drain: the turn did not run.
    @app.exception_handler(NotAcceptingTurnsError)
    async def _not_accepting_turns_handler(  # noqa: ANN001
        request, exc: NotAcceptingTurnsError,
    ):
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    # FastAPI validation errors echo the rejected input, which can include the
    # setup token. Return them without the input.
    @app.exception_handler(RequestValidationError)
    async def _request_validation_handler(  # noqa: ANN001
        request, exc: RequestValidationError,
    ):
        errors = [
            {key: value for key, value in error.items() if key != "input"}
            for error in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content={"detail": jsonable_encoder(errors)},
        )

    # Sliding session tokens. `require_auth` re-mints a session token once it
    # is past half its lifetime and stashes it on request.state; hand it back
    # on the response so the browser can swap it in. Net effect: a tab in
    # continuous use is never logged out, and the configured
    # `auth.jwt_expiry_hours` becomes an idle timeout instead of a hard
    # egg timer that fires mid-typing.
    @app.middleware("http")
    async def _slide_session_token(request: Request, call_next):  # noqa: ANN001
        response = await call_next(request)
        token = getattr(request.state, "refreshed_token", None)
        if token:
            response.headers[SESSION_TOKEN_HEADER] = token
        return response

    # CORS for development
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        # Browsers hide non-safelisted response headers from JS unless the
        # server explicitly exposes them — without this the refreshed token
        # is invisible to fetch() on any cross-origin (dev) setup.
        expose_headers=[SESSION_TOKEN_HEADER],
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
        # Accept, authenticate and resolve the actor — once, here. Nothing
        # below re-reads identity: this connection acts as `connection.actor`
        # until it closes.
        connection = await _accept_websocket(websocket)
        if connection is None:
            return

        client_id = connection.client_id
        # Register, then check the account. A disable that commits before the
        # registration is caught by the check; one after it closes this socket.
        _live_sockets[client_id] = (connection, websocket)
        _connections_changed()
        active_session: str | None = None
        # Agent transcript channels this socket follows, oldest first.
        agent_subs: OrderedDict[str, None] = OrderedDict()

        async def _drop_agent_subs() -> None:
            for channel in list(agent_subs):
                await broadcaster.unregister(channel, client_id)
            agent_subs.clear()

        try:
            if not await _account_enabled(connection.actor):
                await websocket.close(
                    code=WS_ACCOUNT_DISABLED_CODE,
                    reason=WS_ACCOUNT_DISABLED_REASON,
                )
                return

            router = _engine.router
            # Reuse the last session for this channel (no sticky period).
            # Only create a brand-new session if none exist at all.
            active_session = await router.get_last_session("web:default")
            if not active_session:
                # Not through the router: this ingress knows *who* connected,
                # so the session it mints belongs to that person rather than
                # to the web channel in general.
                active_session = await _engine.sessions.get_active_session(
                    "web:default", source="web", actor=connection.actor,
                )
            logger.info(
                "WebSocket connected: %s (session: %s)", client_id, active_session,
            )

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

            # Always send the session's status: the agent roster hydrates from
            # it even when no turn runs, and when a turn is mid-flight (page
            # reload, transient WS drop, sticky reconnect after a network
            # blip) it carries the broadcaster buffer so the listener can
            # rebuild the in-flight stream without waiting for new events.
            is_running = _engine.is_session_running(active_session)
            session_record = await _engine.db.get_session(active_session)
            await _send_session_status(
                websocket, active_session, is_running, session_record,
            )

            while True:
                data = await websocket.receive_json()
                msg_type = data.get("type", "")

                if msg_type == "message":
                    # User sent a chat message
                    user_text = data.get("content", "")
                    session_id = data.get("session_id", active_session)
                    file_ids = data.get("file_ids", [])
                    # Optional per-message model override from the composer's
                    # model picker (Anthropic default or a local Ollama model).
                    selected_model = data.get("model") or None
                    # The sender's id for the message: a resend after a
                    # dropped socket runs once, and every tab matches the
                    # queue entry and the started turn by it.
                    client_msg_id = _client_msg_id(data.get("client_msg_id"))

                    if session_id != active_session:
                        # Switch sessions
                        await _drop_agent_subs()
                        await broadcaster.unregister(active_session, client_id)
                        active_session = session_id
                        await broadcaster.register(active_session, client_id, ws_broadcast)
                        await router.switch_session("web:default", session_id)

                    # During a drain, tell this client only. Other tabs get
                    # no echo of a message that will not run. With the
                    # client_msg_id, a tab that queued the message behind a
                    # running turn keeps its view of that turn.
                    if not _engine.accepting_turns:
                        await websocket.send_json({
                            "type": "error",
                            "session_id": session_id,
                            "error": str(NotAcceptingTurnsError()),
                            "client_msg_id": client_msg_id,
                        })
                        continue

                    # Load uploaded files if any
                    images = None
                    image_refs = None
                    if file_ids:
                        images, image_refs = await _load_uploaded_files(
                            _engine.db, file_ids,
                        )

                    # Echo the message to every *other* client of this session so
                    # parallel tabs render the user bubble live (the sender already
                    # showed it optimistically). engine.run persists it, so reloads
                    # get it from history regardless.
                    # Carry the sender's actor so another tab can label the live
                    # bubble. After reload, the UI resolves the persisted actor_id.
                    await broadcaster.broadcast(session_id, {
                        "type": "user_message",
                        "session_id": session_id,
                        "content": user_text,
                        "blocks": image_refs or None,
                        "actor_id": connection.actor.actor_id,
                        "client_msg_id": client_msg_id,
                    }, exclude=client_id)

                    # Run the agent in the background. While another turn
                    # holds the session's run lock the run waits in the
                    # session's queue (visible to every tab, cancellable by
                    # its sender); it becomes the Stop target only once it
                    # holds the lock. The message is this connection's
                    # person, fixed at accept — never re-read mid-stream.
                    task = asyncio.create_task(
                        _engine.run(
                            session_id=session_id,
                            user_message=user_text,
                            source="web",
                            channel="web",
                            model=selected_model,
                            images=images or None,
                            image_refs=image_refs or None,
                            actor=connection.actor,
                            client_msg_id=client_msg_id,
                            origin="web",
                        )
                    )
                    _ws_runs.add(task)
                    task.add_done_callback(_ws_run_done)

                elif msg_type == "stop":
                    # User wants to stop the running agent
                    session_id = data.get("session_id", active_session)
                    stopped = await _engine.stop_session(
                        session_id,
                        attribution=attribution_for(connection.actor),
                    )
                    if not stopped:
                        await websocket.send_json({
                            "type": "error",
                            "session_id": session_id,
                            "error": "No running task to stop",
                        })

                elif msg_type in ("agent_subscribe", "agent_unsubscribe"):
                    # Follow (or stop following) one agent's live transcript:
                    # agent_event frames on its own channel.
                    session_id = data.get("session_id") or active_session
                    agent_id = str(data.get("agent_id") or "")
                    channel = agent_channel(session_id, agent_id)
                    if msg_type == "agent_unsubscribe":
                        # Idempotent: a tab closing an agent after a switch
                        # gets no error.
                        if channel in agent_subs:
                            agent_subs.pop(channel, None)
                            await broadcaster.unregister(channel, client_id)
                        continue
                    found = False
                    if session_id == active_session and agent_id:
                        await _engine.agents.ensure_loaded(session_id)
                        found = _engine.agents.get(session_id, agent_id) is not None
                    if not found:
                        await websocket.send_json({
                            "type": "error",
                            "session_id": session_id,
                            "error": "agent_not_found",
                        })
                        continue
                    if channel in agent_subs:
                        agent_subs.move_to_end(channel)
                        continue
                    await broadcaster.register(channel, client_id, ws_broadcast)
                    agent_subs[channel] = None
                    while len(agent_subs) > MAX_AGENT_SUBSCRIPTIONS:
                        oldest, _ = agent_subs.popitem(last=False)
                        await broadcaster.unregister(oldest, client_id)

                elif msg_type == "switch_session":
                    new_session = data.get("session_id", active_session)
                    await _drop_agent_subs()
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
                            actor=connection.actor,
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
                        await _engine.resume_session(
                            session_id,
                            attribution=attribution_for(connection.actor),
                        )
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
            if _live_sockets.pop(client_id, None) is not None:
                _connections_changed()
            await _drop_agent_subs()
            if active_session is not None:
                await broadcaster.unregister(active_session, client_id)
            await broadcaster.unregister("__global__", f"global:{client_id}")

    # Gateway channel streams. A separate endpoint from /ws with its own
    # authentication: a gateway-signed token in the Authorization header,
    # never the web session token, a cookie, or a query parameter.
    from nerve.channels.hosted.manager import STREAM_PATH, refuse_upgrade

    @app.websocket(STREAM_PATH)
    async def channel_stream_endpoint(websocket: WebSocket):
        runtime = _hosted_channels
        if runtime is None:
            await refuse_upgrade(websocket, 404)
            return
        await runtime.serve(websocket)

    # Nothing under /_internal/ is a web UI route. Answer plain HTTP there with
    # 404 before the SPA catch-all would serve index.html for it.
    @app.api_route(
        "/_internal/{path:path}",
        methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        include_in_schema=False,
    )
    async def internal_not_found(path: str):
        from fastapi.responses import Response

        return Response(status_code=404)

    # Health check (no auth required) — must be before static mount.
    # ``generation`` is this process's boot id: the setup page polls it after
    # a restart to know when the *new* process is answering.
    from nerve import boot as _boot

    @app.get("/health")
    async def health():
        return {"status": "ok", "version": "0.1.0", "generation": _boot.boot_id()}

    # Favicon from the tracked config subtree (see config.workspace_favicon).
    # No auth: a browser asks for this before anyone has logged in, so requiring
    # a token would mean the login page never has an icon.
    #
    # Before the static mount for the same reason as /health, and the reason is
    # not cosmetic here: the SPA catch-all answers every unmatched path with
    # index.html, so /favicon.ico currently returns HTML with a 200 and the
    # browser is left to make sense of markup it asked for an image.
    #
    # Registered whether or not the frontend has been built. The favicon is
    # config, not build output, and an instance serving the API without a bundled
    # UI can still be someone's browser tab.
    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon():
        from fastapi.responses import FileResponse, Response

        from nerve.config import FAVICON_RESPONSE_HEADERS, workspace_favicon

        found = workspace_favicon(get_config().workspace)
        if found is None:
            return Response(status_code=404)
        path, content_type = found
        return FileResponse(
            str(path), media_type=content_type, headers=FAVICON_RESPONSE_HEADERS,
        )

    # Every API path that no router claimed answers 404 JSON — registered
    # after all routers (route order decides) and ahead of the SPA fallback,
    # so an unregistered route (the claim in external mode, a typo) never
    # becomes a 405 from the catch-all or a 200 carrying index.html that the
    # frontend's request() would try to parse as JSON.
    from fastapi.responses import JSONResponse

    @app.api_route(
        "/api/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
        include_in_schema=False,
    )
    async def api_not_found(path: str):
        return JSONResponse({"detail": "Not found"}, status_code=404)

    # Serve static web UI files if built
    web_dist = Path(__file__).parent.parent.parent / "web" / "dist"
    if web_dist.exists():
        from fastapi.responses import FileResponse

        # Mount static assets (js, css, etc.)
        app.mount("/assets", StaticFiles(directory=str(web_dist / "assets")), name="assets")

        # SPA catch-all: serve index.html for any non-API, non-asset route
        @app.get("/{path:path}")
        async def spa_fallback(path: str):
            # Serve actual built files if they exist (robots.txt, manifest, ...).
            # Not the favicon: that has its own route above and is served from
            # tracked config rather than the bundle, so it never gets here.
            file_path = web_dist / path
            if file_path.is_file():
                return FileResponse(str(file_path))
            # Otherwise serve index.html for SPA routing
            return FileResponse(str(web_dist / "index.html"))
    else:
        # No built UI: a dead URL after `nerve init` is the worst first
        # impression, so / and /setup answer with a static page that says what
        # to do. Inline CSS, no JavaScript, no token. /api/* is unaffected.
        from fastapi.responses import HTMLResponse

        @app.get("/", include_in_schema=False)
        @app.get("/setup{path:path}", include_in_schema=False)
        async def ui_not_built(path: str = ""):
            return HTMLResponse(_UI_NOT_BUILT_PAGE, status_code=200)

    return app


_UI_NOT_BUILT_PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Nerve — web UI not built</title>
<style>
body{font:16px/1.5 system-ui,-apple-system,Segoe UI,sans-serif;max-width:40rem;margin:4rem auto;padding:0 1.5rem;color:#1f2933;background:#f8fafc}
h1{font-size:1.4rem}code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;background:#e8edf2;border-radius:4px;padding:.1rem .35rem}
pre{padding:.75rem 1rem;overflow:auto}p{margin:.75rem 0}
</style></head><body>
<h1>Nerve is running, but its web UI has not been built yet</h1>
<p>The API answers; the page you asked for needs the built front end. On the server, run one of:</p>
<pre>cd web &amp;&amp; npm ci &amp;&amp; npm run build</pre>
<pre>nerve upgrade</pre>
<p>then reload this page. Nothing else needs to change.</p>
</body></html>
"""


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


class RedactTokenQuery(logging.Filter):
    """Drop ``token=<value>`` query values from uvicorn's access-log lines.

    The local-mode frontend opens ``/ws?token=<session jwt>`` and uvicorn
    logs the full request target, so without this the instance log would
    hold a live bearer credential in plain text after every browser session
    (``WebSocket /ws?token=… [accepted]``). The filter rewrites the record's
    arguments in place and never drops a line.
    """

    _PATTERN = re.compile(r"([?&](?:token|access_token)=)[^&\s\"']+")

    @classmethod
    def redact(cls, text: str) -> str:
        return cls._PATTERN.sub(r"\1<redacted>", text)

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if isinstance(args, tuple):
            record.args = tuple(self.redact(a) if isinstance(a, str) else a for a in args)
        elif isinstance(args, dict):
            record.args = {k: (self.redact(v) if isinstance(v, str) else v) for k, v in args.items()}
        if isinstance(record.msg, str) and "token=" in record.msg:
            record.msg = self.redact(record.msg)
        return True


#: Every logger uvicorn writes request lines through. HTTP access lines go to
#: ``uvicorn.access``; the WebSocket ``"WebSocket /ws?token=…" [accepted]``
#: line goes through ``uvicorn.error`` (uvicorn's protocol logger), so the
#: filter has to sit on all of them.
_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


def uvicorn_log_config() -> dict[str, Any]:
    """uvicorn's default logging config with :class:`RedactTokenQuery` on
    every uvicorn logger. A copy: uvicorn's own dict is never mutated."""
    import copy

    from uvicorn.config import LOGGING_CONFIG

    config = copy.deepcopy(LOGGING_CONFIG)
    config.setdefault("filters", {})["redact_token_query"] = {
        "()": "nerve.gateway.server.RedactTokenQuery",
    }
    loggers = config.setdefault("loggers", {})
    for name in _UVICORN_LOGGERS:
        entry = loggers.setdefault(name, {})
        entry["filters"] = [*entry.get("filters", []), "redact_token_query"]
    return config


class _DrainingServer(uvicorn.Server):
    """A uvicorn server that drains running turns before it shuts down.

    On the first SIGTERM or SIGINT, the engine stops accepting new turns and
    waits up to ``gateway.drain_timeout_seconds`` for the running turns to
    end. The server continues to serve during the wait, so clients still get
    the stream of a running turn. Then the usual uvicorn shutdown starts. A
    second signal starts it immediately.
    """

    def __init__(self, config: uvicorn.Config) -> None:
        super().__init__(config)
        self._drain_signal: int | None = None
        self._drain_task: asyncio.Task | None = None

    def handle_exit(self, sig: int, frame: FrameType | None) -> None:
        # This runs as a signal handler, so it only sets state. on_tick
        # starts the drain on the event loop. Do not call the base method
        # before the drain ends: sse_starlette patches it to close every SSE
        # stream, and a running Codex turn calls its MCP tools over SSE.
        if self._drain_signal is None and not self.should_exit:
            self._drain_signal = sig
            return
        super().handle_exit(sig, frame)

    async def on_tick(self, counter: int) -> bool:
        if self._drain_signal is not None and self._drain_task is None:
            self._drain_task = asyncio.create_task(self._drain())
        return await super().on_tick(counter)

    async def _drain(self) -> None:
        try:
            timeout = get_config().gateway.drain_timeout_seconds
            if _engine is not None and timeout > 0:
                logger.info(
                    "Shutdown requested: draining for up to %ss. "
                    "Send the signal again to shut down now.", timeout,
                )
                await _engine.drain(timeout)
        except Exception:
            logger.exception("Drain failed; shutting down now")
        # A second signal can set should_exit while the drain ends. Calling
        # handle_exit again then would force the exit and skip the lifespan
        # shutdown.
        if not self.should_exit:
            super().handle_exit(self._drain_signal, None)

    async def shutdown(self, sockets: list[socket.socket] | None = None) -> None:
        if self._drain_task is not None and not self._drain_task.done():
            self._drain_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._drain_task
        await super().shutdown(sockets)


def run_server(config: NerveConfig | None = None) -> None:
    """Run the Nerve server with uvicorn."""
    from uvicorn.main import STARTUP_FAILURE

    if config is None:
        config = get_config()

    ssl_config = {}
    if config.gateway.ssl.enabled:
        ssl_config = {
            "ssl_certfile": str(config.gateway.ssl.cert),
            "ssl_keyfile": str(config.gateway.ssl.key),
        }

    server = _DrainingServer(uvicorn.Config(
        create_app(),
        host=config.gateway.host,
        port=config.gateway.port,
        # Explicit, never uvicorn's ``auto``: that picks uvloop whenever it is
        # installed, and uvloop spawns subprocesses with a full ``fork()`` — see
        # GatewayConfig.loop for why that stalls a large gateway.
        loop=config.gateway.loop,
        log_level="info",
        log_config=uvicorn_log_config(),
        **ssl_config,
    ))
    # The same exit handling as uvicorn.run.
    try:
        server.run()
    except KeyboardInterrupt:
        pass
    if not server.started:
        sys.exit(STARTUP_FAILURE)
