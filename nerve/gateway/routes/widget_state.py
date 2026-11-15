"""Widget state routes: what the widgets of a chat saved (see
``docs/widgets.md`` and ``nerve/widget_state.py``).

The web chat stands in for the ``localStorage`` of a ```widget block. It
reads what the widgets of a session saved, in one request, before it draws
them, writes one widget's keys and values when they change, and deletes
them when the person resets the widget. Always for one session: no route
answers with another session's state.

Errors are ``{"detail": "<code>"}``. Every route answers 404
``widgets_disabled`` while the sandbox that draws widgets is off (the switch
of MCP Apps), and 404 ``session_not_found`` for a session that does not
exist.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.gateway.routes.mcp_apps import _enabled as _sandbox_active
from nerve.widget_state import (
    MAX_BODY_BYTES,
    MAX_CHAT_CHARS,
    MAX_CHAT_WIDGETS,
    MAX_WIDGET_CHARS,
    MAX_WIDGET_KEYS,
    WidgetStateError,
    is_widget_id,
    read_items,
)

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_auth)])

LIMITS = {
    "widget_chars": MAX_WIDGET_CHARS,
    "widget_keys": MAX_WIDGET_KEYS,
    "chat_chars": MAX_CHAT_CHARS,
    "chat_widgets": MAX_CHAT_WIDGETS,
}


def _answer(body: dict[str, Any]) -> Response:
    """The body as JSON in ASCII. A widget's texts go out as they came in:
    one with half a surrogate pair in it has no UTF-8 form."""
    return Response(
        content=json.dumps(body, ensure_ascii=True, separators=(",", ":")),
        media_type="application/json",
    )


async def _require(session_id: str) -> Any:
    """The route's dependencies, for a session that exists while widgets are drawn."""
    deps = get_deps()
    if not _sandbox_active(deps):
        raise HTTPException(status_code=404, detail="widgets_disabled")
    if not await deps.db.get_session(session_id):
        raise HTTPException(status_code=404, detail="session_not_found")
    return deps


def _widget_id(raw: str) -> str:
    if not is_widget_id(raw):
        raise HTTPException(status_code=400, detail="bad_widget_id")
    return raw


async def _json_object(request: Request) -> dict[str, Any]:
    """The request body as a JSON object of at most MAX_BODY_BYTES."""
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_BODY_BYTES:
            raise HTTPException(status_code=413, detail="body_too_large")
        chunks.append(chunk)
    try:
        body = json.loads(b"".join(chunks))
    except (ValueError, RecursionError):
        raise HTTPException(status_code=400, detail="bad_json") from None
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="bad_json")
    return body


@router.get("/api/sessions/{session_id}/widget-states")
async def list_widget_states(session_id: str):
    """What the widgets of a session saved, the one that changed last first,
    and the limits a write is held to."""
    deps = await _require(session_id)
    return _answer({
        "widgets": await deps.db.list_widget_states(session_id),
        "limits": LIMITS,
    })


@router.put("/api/sessions/{session_id}/widget-states/{widget_id}")
async def save_widget_state(session_id: str, widget_id: str, request: Request):
    """Keep the keys and values of one widget, in the place of what it had.
    An empty list forgets them."""
    deps = await _require(session_id)
    widget_id = _widget_id(widget_id)
    body = await _json_object(request)
    try:
        items, size = read_items(body.get("items"))
        saved = await deps.db.save_widget_state(session_id, widget_id, items, size)
    except WidgetStateError as e:
        raise HTTPException(status_code=e.status, detail=e.code) from None
    return _answer({"id": saved["id"], "size": saved["size"], "updated_at": saved["updated_at"]})


@router.delete("/api/sessions/{session_id}/widget-states/{widget_id}")
async def delete_widget_state(session_id: str, widget_id: str):
    """Forget what one widget saved. ``deleted`` is false when it had saved nothing."""
    deps = await _require(session_id)
    widget_id = _widget_id(widget_id)
    return _answer({"deleted": await deps.db.delete_widget_state(session_id, widget_id)})
