"""Context routes: what fills a session's context window, and Compact (see
``docs/api.md``).

Errors are ``{"detail": {"code": ..., "message": ...}}``, so clients branch
on ``code``.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from nerve.agent.engine import CompactError, NotAcceptingTurnsError
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.identity import Actor

router = APIRouter()

# The input-token fields of a turn's usage that fill the context window.
_CONTEXT_INPUT_FIELDS = (
    "input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens",
)


def _error(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(status_code=status, detail={"code": code, "message": message})


async def _require_session(deps: Any, session_id: str) -> dict:
    session = await deps.db.get_session(session_id)
    if not session:
        raise _error(404, "session_not_found", f"Session {session_id} not found")
    return session


def _last_turn_totals(session: dict) -> dict:
    """Totals from the session's last usage, as the context bar estimates
    them: the measured ``context_tokens`` after a compact, else the turn's
    input tokens divided by its model calls."""
    meta = json.loads(session.get("metadata") or "{}")
    usage = meta.get("last_usage") or {}
    total = usage.get("context_tokens")
    if total is None:
        used = sum(int(usage.get(k) or 0) for k in _CONTEXT_INPUT_FIELDS)
        total = round(used / (usage.get("num_turns") or 1))
    return {
        "total_tokens": int(total),
        "max_tokens": int(usage.get("max_context_tokens") or 0),
        "auto_compact_threshold": None,
        "categories": [],
    }


async def _context_view(deps: Any, session_id: str) -> dict:
    session = await _require_session(deps, session_id)
    live = await deps.engine.context_usage(session_id)
    return {
        "session_id": session_id,
        "live": live is not None,
        "can_compact": deps.engine.can_compact(session_id),
        **(live or _last_turn_totals(session)),
    }


@router.get("/api/sessions/{session_id}/context")
async def get_context(session_id: str, actor: Actor = Depends(require_auth)):
    """The context window by category from the live runtime, or the totals
    of the last turn when there is no live runtime that can tell them."""
    return await _context_view(get_deps(), session_id)


@router.post("/api/sessions/{session_id}/compact")
async def compact(session_id: str, actor: Actor = Depends(require_auth)):
    """Compact the conversation as a turn, then return the context again."""
    deps = get_deps()
    await _require_session(deps, session_id)
    try:
        await deps.engine.compact_session(session_id, actor=actor)
    except CompactError as e:
        raise _error(502 if e.code == "failed" else 409, e.code, str(e)) from e
    except NotAcceptingTurnsError as e:
        raise _error(503, "not_accepting_turns", str(e)) from e
    return await _context_view(deps, session_id)
