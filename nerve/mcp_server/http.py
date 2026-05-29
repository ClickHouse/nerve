"""Mount Nerve's MCP server on the gateway as a Streamable HTTP endpoint.

External MCP clients (Codex, Claude Code, Cursor) POST JSON-RPC frames
to ``/mcp/v1`` over HTTP with SSE for server→client streaming. The
:class:`StreamableHTTPSessionManager` from the MCP SDK handles transport
plumbing — session tracking, SSE framing, JSON-RPC validation — and
forwards parsed messages to a single :class:`mcp.server.Server` instance.

Two-step setup:

  1. :func:`mount_deferred` is called from ``create_app()`` and registers
     an ASGI sub-app at ``/mcp/v1`` whose body looks up the live manager
     from the supplied callable. Routes must be registered before the
     SPA catch-all in ``create_app``, but the manager only exists once
     the engine is built in the lifespan.
  2. :func:`build_manager` is called from the gateway lifespan once the
     engine is live; the returned manager's ``run()`` context owns the
     task group for in-flight connections and must stay open until
     shutdown.

Authentication wraps the manager: a missing or invalid JWT short-
circuits with 401 *before* any MCP frame is parsed, so an attacker
can't probe server capabilities anonymously.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Callable

from mcp.server.lowlevel.server import request_ctx
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.types import Receive, Scope, Send

from nerve.agent.tools import ToolContext, ToolRegistry
from nerve.mcp_server.audit import build_audit_writer
from nerve.mcp_server.auth import McpAuthError, authenticate_mcp
from nerve.mcp_server.server import build_mcp_server
from nerve.mcp_server.session import SatelliteSessionResolver

if TYPE_CHECKING:
    from fastapi import FastAPI

    from nerve.agent.engine import AgentEngine
    from nerve.config import NerveConfig

logger = logging.getLogger(__name__)


async def _send_status(send: Send, status: int, message: str) -> None:
    """Emit a small ASGI error response with a JSON body."""
    body = json.dumps({"error": message}).encode("utf-8")
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
    ]
    if status == 401:
        headers.append((b"www-authenticate", b'Bearer realm="nerve-mcp"'))
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": headers,
    })
    await send({"type": "http.response.body", "body": body, "more_body": False})


def _resolve_client_info() -> tuple[str | None, str | None, str | None]:
    """Read client metadata from the active MCP request context.

    Returns ``(client_name, mcp_session_id, request_path)`` — any field
    may be ``None`` if the corresponding data isn't available (e.g.
    during the very first ``initialize`` call the session may not yet
    have ``client_params``).
    """
    try:
        rctx = request_ctx.get()
    except LookupError:
        return None, None, None

    client_name: str | None = None
    if rctx.session and rctx.session.client_params:
        info = rctx.session.client_params.clientInfo
        if info is not None:
            client_name = info.name

    request = rctx.request
    mcp_session_id: str | None = None
    path: str | None = None
    if request is not None:
        try:
            mcp_session_id = request.headers.get("mcp-session-id")
            path = request.url.path
        except AttributeError:
            pass

    return client_name, mcp_session_id, path


def build_ctx_resolver(engine: "AgentEngine", resolver: SatelliteSessionResolver):
    """Build the per-call_tool ``ToolContext`` resolver closure.

    The Server's ``call_tool`` handler invokes this for every tool call
    to attribute the call to the correct satellite session. Per-call
    resolution is cheap (the satellite session id is deterministic and
    the underlying ``get_session`` / ``create_session`` check is O(1)
    on the indexed primary key).
    """

    async def _resolve() -> ToolContext:
        client_name, mcp_session_id, _ = _resolve_client_info()

        if mcp_session_id is None:
            # Stateless requests / pre-initialize calls can land here.
            # Fall back to a synthetic id so we still get a session row.
            mcp_session_id = "stateless"

        session_id = await resolver.resolve(
            client_name=client_name,
            mcp_session_id=mcp_session_id,
        )

        return ToolContext(
            session_id=session_id,
            workspace=engine.config.workspace,
            db=engine.db,
            memory_bridge=engine._memory_bridge,
            config=engine.config,
            skill_manager=engine._skill_manager,
            engine=engine,
            notification_service=engine.notification_service,
        )

    return _resolve


def build_manager(
    engine: "AgentEngine",
    registry: ToolRegistry,
    config: "NerveConfig",
) -> StreamableHTTPSessionManager:
    """Construct the :class:`StreamableHTTPSessionManager` for the endpoint.

    The returned manager is **not** yet running — callers must enter
    its ``run()`` context (typically via the gateway lifespan) before
    any HTTP request can be handled.
    """
    resolver = SatelliteSessionResolver(engine.db)
    ctx_resolver = build_ctx_resolver(engine, resolver)
    audit_writer = build_audit_writer(engine.db)

    server = build_mcp_server(
        registry,
        ctx_resolver=ctx_resolver,
        audit_writer=audit_writer,
        include_hoa=config.mcp_endpoint.include_hoa,
    )
    return StreamableHTTPSessionManager(
        app=server,
        event_store=None,         # No resumability — see plan v2 rationale
        json_response=False,
        stateless=False,
    )


def mount_deferred(
    app: "FastAPI",
    config: "NerveConfig",
    manager_getter: Callable[[], StreamableHTTPSessionManager | None],
) -> None:
    """Mount a deferred ASGI handler at the configured MCP path.

    The mount must happen during ``create_app()`` so it precedes the
    SPA catch-all route (``/{path:path}``). The handler body looks up
    the live manager via ``manager_getter`` each request — when the
    manager hasn't been built yet (between ``create_app`` and the
    lifespan starting), requests get a 503.
    """
    if not config.mcp_endpoint.enabled:
        return

    path = config.mcp_endpoint.path.rstrip("/") or "/mcp/v1"

    async def _mcp_asgi_app(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":  # pragma: no cover - HTTP only
            await _send_status(send, 400, "MCP endpoint requires HTTP")
            return
        try:
            authenticate_mcp(scope, config)
        except McpAuthError as e:
            await _send_status(send, 401, str(e))
            return

        manager = manager_getter()
        if manager is None:
            await _send_status(send, 503, "MCP server is starting up")
            return

        await manager.handle_request(scope, receive, send)

    app.mount(path, _mcp_asgi_app)
    logger.info("Mounted MCP server at %s (deferred manager)", path)


def mount_mcp_http(
    app: "FastAPI",
    engine: "AgentEngine",
    registry: ToolRegistry,
    config: "NerveConfig",
) -> StreamableHTTPSessionManager:
    """Build a manager AND mount it in one call (eager variant).

    Convenience for unit tests that want both steps in one expression.
    Production callers should prefer :func:`mount_deferred` +
    :func:`build_manager` so the lifespan controls the manager's
    ``run()`` context explicitly.
    """
    manager = build_manager(engine, registry, config)
    mount_deferred(app, config, lambda: manager)
    return manager
