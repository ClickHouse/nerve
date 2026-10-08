"""MCP server routes.

In external mode the organization manages the MCP servers through the MCP
gateway. The routes then list Nerve's own server and the servers of the
applied catalog, mark the catalog servers ``managed_by: "organization"``,
and refuse a reload of the configuration files.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.mcp_gateway import (
    BUILTIN_SERVER_NAME,
    MANAGED_BY_ORGANIZATION,
    MANAGED_RELOAD_DETAIL,
)

router = APIRouter(dependencies=[Depends(require_auth)])


def _managed_servers() -> dict | None:
    """Catalog servers by ID in external mode, ``None`` in local mode."""
    engine = get_deps().engine
    if engine is None or getattr(engine, "managed_mcp", False) is not True:
        return None
    return {server.id: server for server in engine.managed_mcp_servers() or ()}


def _present(row: dict, managed: dict | None) -> dict:
    """Add the organization fields to a server row in external mode."""
    if managed is None:
        return row
    server = managed.get(row["name"])
    if server is None:
        return {**row, "managed_by": None}
    return {
        **row,
        "managed_by": MANAGED_BY_ORGANIZATION,
        "display_name": server.display_name,
        "description": server.description,
    }


def _visible(name: str, managed: dict | None) -> bool:
    return managed is None or name == BUILTIN_SERVER_NAME or name in managed


@router.get("/api/mcp-servers")
async def list_mcp_servers():
    """List all MCP servers with aggregated usage stats."""
    deps = get_deps()
    servers = await deps.db.get_mcp_server_stats()
    managed = _managed_servers()
    if managed is None:
        return {"servers": servers}
    return {
        "servers": [
            _present(row, managed) for row in servers
            if _visible(row["name"], managed)
        ],
        "managed_by": MANAGED_BY_ORGANIZATION,
    }


@router.get("/api/mcp-servers/{server_name}")
async def get_mcp_server_detail(server_name: str):
    """Get detailed info for a specific MCP server."""
    deps = get_deps()
    managed = _managed_servers()
    stats_list = await deps.db.get_mcp_server_stats()
    server = next((s for s in stats_list if s["name"] == server_name), None)
    if not server or not _visible(server_name, managed):
        raise HTTPException(status_code=404, detail="MCP server not found")

    tools = await deps.db.get_mcp_tool_breakdown(server_name)
    usage = await deps.db.get_mcp_server_usage(server_name, limit=30)

    return {**_present(server, managed), "tools": tools, "recent_usage": usage}


@router.get("/api/mcp-servers/{server_name}/usage")
async def get_mcp_server_usage(
    server_name: str, limit: int = 50,
):
    """Get usage history for an MCP server."""
    deps = get_deps()
    usage = await deps.db.get_mcp_server_usage(server_name, limit=min(limit, 200))
    return {"usage": usage}


@router.post("/api/mcp-servers/reload")
async def reload_mcp_servers():
    """Re-read MCP server config from YAML files and refresh cache."""
    from nerve.config import ConfigError

    deps = get_deps()
    if _managed_servers() is not None:
        raise HTTPException(status_code=409, detail=MANAGED_RELOAD_DETAIL)
    try:
        servers = await deps.engine.reload_mcp_config()
    except ConfigError as e:
        # e.g. an unresolved required ${ENV_VAR} in config — report cleanly
        # instead of a 500 so the caller sees what to fix.
        raise HTTPException(status_code=400, detail=str(e)) from e
    stats = await deps.db.get_mcp_server_stats()
    return {"reloaded": len(servers), "servers": stats}
