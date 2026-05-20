"""MCP admin tool handlers — nerve_api, mcp_reload.

``nerve_api`` is an in-process FastAPI client used by the agent to read
its own state without going through the network. The mini-app is built
lazily once and cached at module level (the routes themselves are
stateless — they fetch fresh state from the DB on every request).
"""

from __future__ import annotations

import json
import logging

import httpx

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    MCP_RELOAD_SCHEMA,
    NERVE_API_SCHEMA,
)

logger = logging.getLogger(__name__)


# Lazily-constructed in-process ASGI app for ``nerve_api``. Cached at
# module level so we don't rebuild the FastAPI router on every call.
_nerve_asgi_app = None


def _get_nerve_asgi_app():
    """Get (or lazily create) a minimal FastAPI app wired to the real router.

    Reuses the same route handlers — no duplication, no manual endpoint
    mirroring.
    """
    global _nerve_asgi_app
    if _nerve_asgi_app is None:
        from fastapi import FastAPI
        from nerve.gateway.routes import register_all_routes
        _nerve_asgi_app = FastAPI()
        _nerve_asgi_app.include_router(register_all_routes())
    return _nerve_asgi_app


async def nerve_api_handler(ctx: ToolContext, args: dict) -> ToolResult:
    """Query Nerve API via in-process ASGI transport — no HTTP round-trip."""
    endpoint = args.get("endpoint", "").strip().strip("/")
    if not endpoint:
        return ToolResult.text("Missing 'endpoint' parameter.")

    try:
        app = _get_nerve_asgi_app()

        # Generate an internal auth token
        from nerve.gateway.auth import create_token
        if ctx.config is not None:
            cfg = ctx.config
        else:
            from nerve.config import get_config
            cfg = get_config()

        headers = {}
        if cfg.auth.jwt_secret:
            token = create_token(cfg.auth.jwt_secret)
            headers["Authorization"] = f"Bearer {token}"

        method = (args.get("method") or "GET").upper()
        body = args.get("body")

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://nerve-internal",
        ) as client:
            resp = await client.request(
                method, f"/api/{endpoint}",
                headers=headers,
                content=body,
            )

        if resp.status_code >= 400:
            return ToolResult.text(f"HTTP {resp.status_code}: {resp.text}")

        try:
            data = resp.json()
            return ToolResult.text(json.dumps(data, indent=2, default=str))
        except Exception:
            return ToolResult.text(resp.text)
    except Exception as e:
        logger.error("nerve_api tool failed: %s", e)
        return ToolResult.text(f"Error: {e}")


async def mcp_reload_handler(ctx: ToolContext, args: dict) -> ToolResult:
    """Reload MCP server configs from YAML files."""
    if not ctx.engine:
        return ToolResult.text("Engine not available.")
    try:
        servers = await ctx.engine.reload_mcp_config()
        names = ["nerve (built-in)"] + [s.name for s in servers]
        return ToolResult.text(
            f"MCP config reloaded. {len(names)} server(s): {', '.join(names)}"
        )
    except Exception as e:
        logger.error("mcp_reload failed: %s", e)
        return ToolResult.text(f"Reload failed: {e}")


NERVE_API_SPEC = ToolSpec(
    name="nerve_api",
    description=(
        "Query the Nerve API directly (in-process, no HTTP). "
        "Use to inspect server state: sessions, MCP servers, diagnostics, cron jobs, skills, notifications, etc. "
        "Returns JSON data from internal DB queries."
    ),
    input_schema=NERVE_API_SCHEMA,
    handler=nerve_api_handler,
)

MCP_RELOAD_SPEC = ToolSpec(
    name="mcp_reload",
    description=(
        "Reload MCP server configuration from config files. "
        "Use after editing config.yaml to pick up new or changed external MCP servers. "
        "New sessions will use the updated config; existing sessions keep their current connections."
    ),
    input_schema=MCP_RELOAD_SCHEMA,
    handler=mcp_reload_handler,
)


MCP_ADMIN_SPECS = [
    NERVE_API_SPEC,
    MCP_RELOAD_SPEC,
]
