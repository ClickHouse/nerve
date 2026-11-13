"""MCP Apps routes: what the web UI needs to show the view of an MCP tool
(see ``docs/mcp-apps.md``).

A tool call whose block carries ``mcp_app`` has a view. The web UI reads the
view's document, lists the tools of its server and calls a tool for it
through these routes, always for one session: the session's live Claude CLI
is what reaches the MCP servers.

Errors are ``{"detail": "<code or message>"}``. Every route but the config
answers 404 ``mcp_apps_disabled`` while the feature is off.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.mcp_apps import (
    NERVE_SERVER,
    UI_SCHEME,
    AppServer,
    McpAppsError,
    app_document,
    call_tool_result,
    load_document,
    store_document,
)

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_auth)])

NAME_MAX_CHARS = 255
URI_MAX_CHARS = 2048
# The body of a tool call: the arguments a view sends.
BODY_MAX_BYTES = 1024 * 1024

# What a server says when it does not have the resource (MCP's error code
# for it is -32002).
_NOT_FOUND = re.compile(r"not found|unknown resource|no such resource|-32002", re.IGNORECASE)


def _enabled(deps: Any) -> bool:
    """MCP Apps is on in the configuration and the gateway serves its
    sandbox document (decided at startup: on, in local mode)."""
    return bool(deps.engine.config.mcp_apps_active and deps.mcp_apps_serving)


def _require_enabled(deps: Any) -> None:
    if not _enabled(deps):
        raise HTTPException(status_code=404, detail="mcp_apps_disabled")


async def _require_session(deps: Any, session_id: str) -> None:
    if not await deps.db.get_session(session_id):
        raise HTTPException(status_code=404, detail="session_not_found")


def _server_name(raw: Any) -> str:
    """The ``server`` of a request. Nerve's own server is never part of MCP
    Apps, so it is unknown here."""
    if not isinstance(raw, str) or not raw.strip() or len(raw) > NAME_MAX_CHARS:
        raise HTTPException(status_code=400, detail="bad_server")
    if raw == NERVE_SERVER:
        raise HTTPException(status_code=404, detail="server_not_found")
    return raw


def _live_client(deps: Any, session_id: str) -> Any | None:
    """The session's live client when it can reach MCP servers for a view,
    else None: no client, one whose process is gone, or a runtime other than
    the Claude CLI."""
    client = deps.engine.sessions.get_client(session_id)
    if client is None or not hasattr(client, "mcp_app_servers"):
        return None
    is_alive = getattr(client, "is_alive", None)
    if is_alive is not None and not is_alive():
        return None
    return client


def _upstream_error(e: Exception) -> HTTPException:
    """The answer for a control request that failed: 504 when the runtime or
    the tool did not answer in time, else 502 with the runtime's words."""
    if isinstance(e, asyncio.TimeoutError):
        return HTTPException(status_code=504, detail=str(e) or "timed out")
    return HTTPException(status_code=502, detail=str(e) or type(e).__name__)


async def _connected_server(deps: Any, session_id: str, server: str) -> tuple[Any, AppServer]:
    """The session's live client and its MCP server ``server``, read now."""
    client = _live_client(deps, session_id)
    if client is None:
        raise HTTPException(status_code=409, detail="session_not_running")
    try:
        servers = await client.mcp_app_servers()
    except Exception as e:  # noqa: BLE001 — BackendError or a timeout
        raise _upstream_error(e) from e
    found = servers.get(server)
    if found is None:
        raise HTTPException(status_code=404, detail="server_not_found")
    if not found.connected:
        raise HTTPException(
            status_code=502,
            detail=f"MCP server {server!r} is not connected ({found.status or 'unknown'})",
        )
    return client, found


# --------------------------------------------------------------------------- #
#  Config                                                                      #
# --------------------------------------------------------------------------- #


@router.get("/api/mcp-apps/config")
async def mcp_apps_config():
    """Whether this instance shows MCP Apps (and widgets). The frame of a
    view loads the sandbox document from the address of this API, at
    ``/mcp-app-sandbox.html``."""
    return {"enabled": _enabled(get_deps())}


# --------------------------------------------------------------------------- #
#  The view's document                                                         #
# --------------------------------------------------------------------------- #


@router.get("/api/sessions/{session_id}/mcp-apps/resource")
async def read_app_resource(session_id: str, server: str = "", uri: str = ""):
    """The document of a view: the ``ui://`` resource ``uri`` of the MCP
    server ``server``, read through the session's live runtime.

    ``html`` is untrusted third-party HTML, for the sandbox frame only.
    ``csp`` and ``permissions`` are the resource's ``_meta.ui`` values.
    ``cached`` is true when the answer is the copy kept from an earlier read:
    the session has no live runtime, or its read just failed. A copy is
    served only for a server that the configuration has now: the HTML of a
    server that was taken out is not run again from an old chat.
    """
    deps = get_deps()
    _require_enabled(deps)
    server = _server_name(server)
    if not uri.startswith(UI_SCHEME) or len(uri) > URI_MAX_CHARS:
        raise HTTPException(status_code=400, detail="bad_uri")
    await _require_session(deps, session_id)

    client = _live_client(deps, session_id)
    if client is None:
        cached = await _kept_document(deps, server, uri)
        if cached is None:
            raise HTTPException(status_code=409, detail="session_not_running")
        return {**cached, "cached": True}
    try:
        document = app_document(await client.mcp_read_resource(server, uri), uri)
    except Exception as e:  # noqa: BLE001 — mapped below
        error = await _read_error(client, server, e)
        if error.status_code != 404:
            cached = await _kept_document(deps, server, uri)
            if cached is not None:
                logger.info(
                    "MCP Apps: %r of %r served from the cache (%r)",
                    uri, server, error.detail,
                )
                return {**cached, "cached": True}
        raise error from e
    await asyncio.to_thread(store_document, server, uri, document)
    return {**document, "cached": False}


async def _kept_document(deps: Any, server: str, uri: str) -> dict[str, Any] | None:
    """The copy kept of the document ``uri`` of ``server``, or None: there is
    none, or the configuration no longer has the server (or has it disabled).
    Without the second rule, taking a server out would not stop its views:
    every chat that showed one would go on running its HTML."""
    if not deps.engine.has_mcp_server(server):
        return None
    return await asyncio.to_thread(load_document, server, uri)


async def _read_error(client: Any, server: str, e: Exception) -> HTTPException:
    """The answer for a read that failed.

    404 when the runtime does not know the server or the server does not have
    the resource, else 502 with the reason. The runtime reports both as text,
    so the server is looked up in its status, and the resource is judged by
    the server's words (which the CLI passes on after ``mcp_read_resource:``).
    """
    if isinstance(e, McpAppsError):
        return HTTPException(status_code=e.status, detail=e.detail)
    if isinstance(e, asyncio.TimeoutError):
        return HTTPException(
            status_code=502, detail=f"MCP server {server!r} did not answer in time",
        )
    try:
        known = server in await client.mcp_app_servers()
    except Exception:  # noqa: BLE001 — the first failure is the one to report
        known = True
    if not known:
        return HTTPException(status_code=404, detail="server_not_found")
    message = str(e)
    if message.startswith("mcp_read_resource:") and _NOT_FOUND.search(message):
        return HTTPException(status_code=404, detail="resource_not_found")
    return HTTPException(status_code=502, detail=message or type(e).__name__)


# --------------------------------------------------------------------------- #
#  Tools                                                                       #
# --------------------------------------------------------------------------- #


@router.get("/api/sessions/{session_id}/mcp-apps/tools")
async def list_app_tools(session_id: str, server: str = ""):
    """The tools of the MCP server ``server``: for each, whether it only
    reads (``annotations.readOnlyHint``), who may call it (``visibility``:
    ``model``, ``app``) and the view it is linked to."""
    deps = get_deps()
    _require_enabled(deps)
    server = _server_name(server)
    await _require_session(deps, session_id)
    _, found = await _connected_server(deps, session_id, server)
    return {"tools": [tool.view() for tool in found.tools]}


@router.post("/api/sessions/{session_id}/mcp-apps/tool-call")
async def call_app_tool(session_id: str, request: Request):
    """Call a tool for a view: ``{"server", "tool", "arguments"}``. Answers
    the MCP ``CallToolResult``, with ``isError`` when the tool reported an
    error.

    Only a tool of a third-party server whose visibility includes ``app``.
    Which view may call which server, and what needs the person's consent,
    is the web UI's decision: this route cannot tell the views apart.
    """
    deps = get_deps()
    _require_enabled(deps)
    body = await _json_object(request)
    server = _server_name(body.get("server"))
    tool_name = body.get("tool")
    if not isinstance(tool_name, str) or not tool_name or len(tool_name) > NAME_MAX_CHARS:
        raise HTTPException(status_code=400, detail="bad_tool")
    arguments = body.get("arguments")
    if arguments is None:
        arguments = {}
    if not isinstance(arguments, dict):
        raise HTTPException(status_code=400, detail="bad_arguments")
    await _require_session(deps, session_id)

    client, found = await _connected_server(deps, session_id, server)
    tool = found.tool(tool_name)
    if tool is None:
        raise HTTPException(status_code=404, detail="tool_not_found")
    if not tool.app_visible:
        raise HTTPException(status_code=403, detail="tool_not_app_visible")
    logger.info(
        "MCP App tool call in session %s: %r of %r", session_id, tool_name, server,
    )
    try:
        answer = await client.mcp_call_tool(tool, arguments)
    except Exception as e:  # noqa: BLE001 — BackendError or a timeout
        raise _upstream_error(e) from e
    return call_tool_result(
        answer.get("content"),
        structured=answer.get("structuredContent"),
        is_error=bool(answer.get("isError")),
        meta=answer.get("_meta"),
    )


async def _json_object(request: Request) -> dict[str, Any]:
    """The request body as a JSON object of at most BODY_MAX_BYTES, or 400."""
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > BODY_MAX_BYTES:
            raise HTTPException(status_code=400, detail="body_too_large")
        chunks.append(chunk)
    try:
        body = json.loads(b"".join(chunks))
    except ValueError:
        raise HTTPException(status_code=400, detail="bad_json") from None
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="bad_json")
    return body
