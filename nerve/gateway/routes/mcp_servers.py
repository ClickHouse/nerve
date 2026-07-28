"""MCP server routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

router = APIRouter()


@router.get("/api/mcp-servers")
async def list_mcp_servers(user: dict = Depends(require_auth)):
    """List all MCP servers with aggregated usage stats."""
    deps = get_deps()
    servers = await deps.db.get_mcp_server_stats()
    return {"servers": servers}


@router.get("/api/mcp-servers/{server_name}")
async def get_mcp_server_detail(server_name: str, user: dict = Depends(require_auth)):
    """Get detailed info for a specific MCP server."""
    deps = get_deps()
    stats_list = await deps.db.get_mcp_server_stats()
    server = next((s for s in stats_list if s["name"] == server_name), None)
    if not server:
        raise HTTPException(status_code=404, detail="MCP server not found")

    tools = await deps.db.get_mcp_tool_breakdown(server_name)
    usage = await deps.db.get_mcp_server_usage(server_name, limit=30)

    return {**server, "tools": tools, "recent_usage": usage}


@router.get("/api/mcp-servers/{server_name}/usage")
async def get_mcp_server_usage(
    server_name: str, limit: int = 50, user: dict = Depends(require_auth),
):
    """Get usage history for an MCP server."""
    deps = get_deps()
    usage = await deps.db.get_mcp_server_usage(server_name, limit=min(limit, 200))
    return {"usage": usage}


@router.post("/api/mcp-servers/reload")
async def reload_mcp_servers(user: dict = Depends(require_auth)):
    """Re-read MCP server config from YAML files and refresh cache."""
    from nerve.config import ConfigError

    deps = get_deps()
    try:
        servers = await deps.engine.reload_mcp_config()
    except ConfigError as e:
        # e.g. an unresolved required ${ENV_VAR} in config — report cleanly
        # instead of a 500 so the caller sees what to fix.
        raise HTTPException(status_code=400, detail=str(e)) from e
    stats = await deps.db.get_mcp_server_stats()
    return {"reloaded": len(servers), "servers": stats}
