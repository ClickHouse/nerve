"""Route assembler — collects all domain routers into one APIRouter.

Usage in server.py:
    from nerve.gateway.routes import register_all_routes, init_deps, set_notification_service
    init_deps(engine, db)
    app.include_router(register_all_routes())
"""

from __future__ import annotations

from fastapi import APIRouter

from nerve.gateway.routes._deps import get_deps, init_deps, set_notification_service
from nerve.gateway.routes import (
    auth,
    sessions,
    tasks,
    plans,
    skills,
    mcp_servers,
    memory,
    diagnostics,
    cron,
    sources,
    notifications,
    houseofagents,
    files,
)

__all__ = [
    "register_all_routes",
    "init_deps",
    "set_notification_service",
    "get_deps",
]


def register_all_routes() -> APIRouter:
    """Assemble and return the combined API router."""
    router = APIRouter()
    router.include_router(auth.router)
    router.include_router(sessions.router)
    router.include_router(tasks.router)
    router.include_router(plans.router)
    router.include_router(skills.router)
    router.include_router(mcp_servers.router)
    router.include_router(memory.router)
    router.include_router(diagnostics.router)
    router.include_router(cron.router)
    router.include_router(sources.router)
    router.include_router(notifications.router)
    router.include_router(houseofagents.router)
    router.include_router(files.router)
    return router
