"""Agent routes: a session's sub-agents and background tasks, its stops, and
its turn queue (see ``docs/agents.md``).

Errors are ``{"detail": {"code": ..., "message": ...}}`` (plus extra keys
where noted), so clients branch on ``code``. Stops record who asked
(``attribution_for(actor)``).
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import tempfile
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException
from fastapi.responses import JSONResponse

from nerve.agent.agents.errors import AgentControlError
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.identity import Actor, attribution_for

logger = logging.getLogger(__name__)

router = APIRouter()

SNAPSHOT_LIMIT_MAX = 500
TRANSCRIPT_LIMIT_MAX = 500
OUTPUT_TAIL_DEFAULT = 65_536
OUTPUT_TAIL_MAX = 262_144
# A CLI task id (the output file is named after it).
_TASK_ID_RE = re.compile(r"^[a-z][0-9a-z]{7,31}$")


def _error(status: int, code: str, message: str, **extra: Any) -> HTTPException:
    return HTTPException(
        status_code=status, detail={"code": code, "message": message, **extra},
    )


def _control_error(e: AgentControlError) -> HTTPException:
    return HTTPException(status_code=e.status, detail=e.detail())


async def _require_session(deps: Any, session_id: str) -> None:
    if not await deps.db.get_session(session_id):
        raise _error(404, "session_not_found", f"Session {session_id} not found")


async def _require_agent(deps: Any, session_id: str, agent_id: str) -> Any:
    """The record, or 404 (``agent_in_other_session`` with
    ``owner_session_id`` when a fork inherited a spawn card of its parent)."""
    agents = deps.engine.agents
    await agents.ensure_loaded(session_id)
    rec = agents.get(session_id, agent_id)
    if rec is not None:
        return rec
    owner = await agents.owner_session(agent_id)
    if owner and owner != session_id:
        raise _error(
            404, "agent_in_other_session",
            f"Agent {agent_id} belongs to another session",
            owner_session_id=owner,
        )
    raise _error(404, "agent_not_found", f"Agent {agent_id} not found")


def _clamp(value: int, upper: int) -> int:
    return max(1, min(int(value), upper))


def _cursor(name: str, raw: str | None) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise _error(400, "bad_cursor", f"{name} must be an integer") from None
    if value < 0:
        raise _error(400, "bad_cursor", f"{name} must not be negative")
    return value


# --------------------------------------------------------------------------- #
#  The roster                                                                  #
# --------------------------------------------------------------------------- #


@router.get("/api/sessions/{session_id}/agents")
async def list_agents(
    session_id: str, limit: int = 200, actor: Actor = Depends(require_auth),
):
    """The session's ``AgentSnapshot``: every live agent plus the most recent
    ended ones (up to ``limit``, at most 500), summary, capabilities and the
    turn queue."""
    deps = get_deps()
    await _require_session(deps, session_id)
    return await deps.engine.agent_snapshot(
        session_id, limit=_clamp(limit, SNAPSHOT_LIMIT_MAX),
    )


@router.get("/api/sessions/{session_id}/agents/{agent_id}")
async def get_agent(
    session_id: str, agent_id: str, actor: Actor = Depends(require_auth),
):
    """One agent: its view plus the full brief (``prompt``) and final report
    (``result_text``)."""
    deps = get_deps()
    await _require_agent(deps, session_id, agent_id)
    detail = await deps.engine.agents.detail(session_id, agent_id)
    if detail is None:  # evicted meanwhile
        raise _error(404, "agent_not_found", f"Agent {agent_id} not found")
    return detail


@router.get("/api/sessions/{session_id}/agents/{agent_id}/transcript")
async def get_agent_transcript(
    session_id: str, agent_id: str,
    before_seq: str | None = None, after_seq: str | None = None,
    limit: int = 200, actor: Actor = Depends(require_auth),
):
    """A page of the agent's transcript, ascending by ``seq``: the tail (no
    cursor), the items below ``before_seq``, or those above ``after_seq``.
    Items still waiting to be written are included."""
    before = _cursor("before_seq", before_seq)
    after = _cursor("after_seq", after_seq)
    if before is not None and after is not None:
        raise _error(400, "bad_cursor", "Pass at most one of before_seq and after_seq")
    deps = get_deps()
    await _require_agent(deps, session_id, agent_id)
    return await deps.engine.agents.transcript(
        session_id, agent_id, before_seq=before, after_seq=after,
        limit=_clamp(limit, TRANSCRIPT_LIMIT_MAX),
    )


def _output_roots() -> list[str]:
    """Where the Claude CLI writes background task output: its per-user
    directory under the temp dir. The CLI uses ``/tmp`` even where Python's
    temp dir is elsewhere (macOS sets ``TMPDIR`` to a per-user folder), so
    both count."""
    uid = getattr(os, "getuid", lambda: 0)()
    return sorted({
        os.path.realpath(os.path.join(tempfile.gettempdir(), f"claude-{uid}")),
        os.path.realpath(f"/tmp/claude-{uid}"),
    })


def _safe_output_path(output_file: str | None, task_id: str | None) -> str | None:
    """The real path of a task's output file, or ``None`` unless it is
    ``<task_id>.output`` inside the CLI's temp root (no symlink out, no other
    file)."""
    if not output_file or not task_id or not _TASK_ID_RE.match(task_id):
        return None
    name = f"{task_id}.output"
    if os.path.basename(output_file) != name:
        return None
    real = os.path.realpath(output_file)
    if os.path.basename(real) != name:
        return None
    for root in _output_roots():
        if real.startswith(root + os.sep):
            return real
    return None


def _read_tail(path: str, tail_bytes: int) -> tuple[str, int, bool] | None:
    """``(text, size, truncated)`` of the last ``tail_bytes`` of a file, or
    ``None`` when it cannot be read."""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            start = max(0, size - tail_bytes)
            f.seek(start)
            data = f.read(tail_bytes)
    except OSError:
        return None
    if start:
        # Do not show half a character where the tail cut one.
        while data and 0x80 <= data[0] <= 0xBF:
            data = data[1:]
    return data.decode("utf-8", "replace"), size, start > 0


@router.get("/api/sessions/{session_id}/agents/{agent_id}/output")
async def get_agent_output(
    session_id: str, agent_id: str, tail_bytes: int = OUTPUT_TAIL_DEFAULT,
    actor: Actor = Depends(require_auth),
):
    """The tail of a background shell's or monitor's output file
    (``tail_bytes``, at most 262144). ``available`` is False when the file
    is gone or not where the CLI keeps task output."""
    deps = get_deps()
    rec = await _require_agent(deps, session_id, agent_id)
    if rec.kind not in ("shell", "monitor"):
        raise _error(409, "not_a_shell", "Only shells and monitors have an output file")
    path = _safe_output_path(rec.output_file, rec.task_id)
    result = None
    if path is not None:
        result = await asyncio.to_thread(_read_tail, path, _clamp(tail_bytes, OUTPUT_TAIL_MAX))
    if result is None:
        return {"agent_id": rec.id, "text": "", "size": 0, "truncated": False, "available": False}
    text, size, truncated = result
    return {
        "agent_id": rec.id, "text": text, "size": size,
        "truncated": truncated, "available": True,
    }


# --------------------------------------------------------------------------- #
#  Stops                                                                       #
# --------------------------------------------------------------------------- #


@router.post("/api/sessions/{session_id}/agents/{agent_id}/stop")
async def stop_agent(
    session_id: str, agent_id: str,
    body: dict | None = Body(default=None),
    actor: Actor = Depends(require_auth),
):
    """Stop one agent (with ``cascade``, also the agents it started). 202
    with the ids now stopping; 200 with ``stopping: []`` when it had ended."""
    deps = get_deps()
    cascade = True
    if isinstance(body, dict) and "cascade" in body:
        cascade = bool(body.get("cascade"))
    try:
        view, stopping = await deps.engine.stop_agent(
            session_id, agent_id, attribution=attribution_for(actor), cascade=cascade,
        )
    except AgentControlError as e:
        raise _control_error(e) from None
    return JSONResponse(
        status_code=202 if stopping else 200,
        content={"agent": view, "stopping": stopping},
    )


@router.post("/api/sessions/{session_id}/stop")
async def stop_session(
    session_id: str,
    body: dict | None = Body(default=None),
    actor: Actor = Depends(require_auth),
):
    """Stop the turn (``scope: "turn"``, the default; background agents keep
    running), every agent (``"all"``) or everything by force (``"force"``).
    Returns ``StopTurnResult`` / ``StopAllResult`` / ``ForceStopResult``."""
    deps = get_deps()
    await _require_session(deps, session_id)
    req = body if isinstance(body, dict) else {}
    scope = req.get("scope", "turn")
    attribution = attribution_for(actor)
    if scope == "turn":
        return await deps.engine.stop_turn(
            session_id, attribution=attribution,
            confirm_kill_background=bool(req.get("confirm_kill_background", False)),
        )
    if scope == "all":
        return await deps.engine.stop_all(
            session_id, attribution=attribution,
            include_shells=bool(req.get("include_shells", True)),
        )
    if scope == "force":
        return await deps.engine.force_stop(session_id, attribution=attribution)
    raise _error(422, "bad_scope", 'scope must be "turn", "all" or "force"')


# --------------------------------------------------------------------------- #
#  The turn queue                                                              #
# --------------------------------------------------------------------------- #


@router.get("/api/sessions/{session_id}/queue")
async def get_queue(session_id: str, actor: Actor = Depends(require_auth)):
    """Messages waiting for the session's current turn to end."""
    deps = get_deps()
    await _require_session(deps, session_id)
    return {"items": deps.engine.queue_view(session_id)}


@router.delete("/api/sessions/{session_id}/queue/{queue_id}")
async def cancel_queued(
    session_id: str, queue_id: str, actor: Actor = Depends(require_auth),
):
    """Cancel one of your own queued messages before it runs; its text comes
    back so the composer can restore it."""
    deps = get_deps()
    try:
        entry = deps.engine.cancel_queued_turn(session_id, queue_id, actor=actor)
    except AgentControlError as e:
        raise _control_error(e) from None
    # An internal entry's text is a system prompt (a scheduled run, a hook):
    # never handed to a client, its own owner included.
    return {"cancelled": True, "content": "" if entry.internal else entry.content}
