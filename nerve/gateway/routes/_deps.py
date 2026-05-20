"""Centralized dependency container for route modules.

Replaces the module-level globals (_engine, _db, _notification_service)
that were previously scattered across routes.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine
    from nerve.agent.tools import ToolContext, ToolRegistry
    from nerve.db import Database
    from nerve.external_agents.sync_service import SyncService
    from nerve.notifications.service import NotificationService


@dataclass
class RouteDeps:
    engine: "AgentEngine"
    db: "Database"
    notification_service: "NotificationService | None" = None
    external_agents_sync: "SyncService | None" = None


def set_external_agents_sync(service: "SyncService | None") -> None:
    """Wire the external-agents sync service after lifespan creates it.

    Symmetric to :func:`set_notification_service`. Routes that expose
    sync status / manual re-sync read the live service through
    :func:`get_deps`.
    """
    if _deps is None:
        raise RuntimeError("init_deps() must be called before set_external_agents_sync()")
    _deps.external_agents_sync = service


_deps: RouteDeps | None = None


def init_deps(engine: "AgentEngine", db: "Database") -> None:
    """Initialize route dependencies. Called once during server startup."""
    global _deps
    _deps = RouteDeps(engine=engine, db=db)


def set_notification_service(service: "NotificationService") -> None:
    """Wire notification service after it's created."""
    if _deps is None:
        raise RuntimeError("init_deps() must be called before set_notification_service()")
    _deps.notification_service = service


def get_deps() -> RouteDeps:
    """Get the dependency container. Raises if not initialized."""
    if _deps is None:
        raise RuntimeError("Route dependencies not initialized. Call init_deps() first.")
    return _deps


def get_tool_registry() -> "ToolRegistry":
    """Return the live tool registry from the engine.

    HTTP routes that invoke tools (``task_create``, ``task_update``, etc.)
    use this to call into the unified handler surface, so business logic
    stays in one place instead of duplicated between the MCP path and the
    REST path.
    """
    return get_deps().engine.registry


def build_route_tool_context(session_id: str = "system") -> "ToolContext":
    """Construct a :class:`ToolContext` for an HTTP-route-initiated tool call.

    Routes don't belong to an LLM session, so we use a stable
    ``session_id`` value (``"system"`` by default) for any tool that
    reads ``ctx.session_id``. Notification-style tools are *not* invoked
    from routes today, but if they ever are they'll attribute to that
    well-known sentinel.
    """
    from nerve.agent.tools import ToolContext  # local import to avoid cycle

    deps = get_deps()
    engine = deps.engine
    return ToolContext(
        session_id=session_id,
        workspace=engine.config.workspace,
        db=deps.db,
        memory_bridge=engine._memory_bridge,
        config=engine.config,
        skill_manager=engine._skill_manager,
        engine=engine,
        notification_service=deps.notification_service,
    )
