"""Session and chat routes."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.agent.backends.base import BackendError
from nerve.agent.interactive import get_awaiting_ids
from nerve.config import get_config
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

logger = logging.getLogger(__name__)

router = APIRouter()


async def _cleanup_uploaded_files(db: object, session_id: str) -> None:
    """Delete uploaded files from DB and disk for a deleted session."""
    try:
        disk_paths = await db.delete_uploaded_files(session_id)  # type: ignore[attr-defined]
        for p in disk_paths:
            try:
                Path(p).unlink(missing_ok=True)
            except Exception:
                pass
        # Try to remove session upload dir if empty
        config = get_config()
        session_dir = config.workspace / ".uploads" / session_id
        if session_dir.exists():
            try:
                session_dir.rmdir()
            except OSError:
                pass  # Not empty — that's fine
    except Exception as e:
        logger.warning("Failed to cleanup uploaded files for session %s: %s", session_id, e)


# --- Request/Response models ---

class MessageRequest(BaseModel):
    message: str
    session_id: str | None = None


class MessageResponse(BaseModel):
    response: str
    session_id: str


class ReviewLoopLegRequest(BaseModel):
    engine: str | None = None   # "claude-workflow" | "codex-ultracode"
    model: str | None = None
    effort: str | None = None


class ReviewLoopRequest(BaseModel):
    """Per-session review-loop config (see docs/review-loops.md)."""

    goal: str
    verifier: str
    budget_usd: float | None = None
    max_iterations: int | None = None
    criteria_adoption: str | None = None  # no | ask | auto
    implementer: ReviewLoopLegRequest | None = None
    verifier_leg: ReviewLoopLegRequest | None = None


class SessionCreateRequest(BaseModel):
    title: str | None = None
    source: str = "web"
    # Agent backend for this session ("claude" | "codex"). Persisted
    # immediately; metadata keeps the override only for old readers.
    backend: str | None = None
    # Model for this session's turns (the composer's picker choice).
    # Empty/None → the backend's default. Persisted at creation so the
    # header's model badge is right from the first render — per-message
    # overrides on the WS path can still change it later.
    model: str | None = None
    cwd: str | None = None
    # Optional review loop attached at creation: this session becomes the
    # loop's observer (milestones land here; cwd is the shared workspace).
    review_loop: ReviewLoopRequest | None = None


class ForkRequest(BaseModel):
    source_session_id: str
    at_message_id: str | None = None
    title: str | None = None


class RunLaterRequest(BaseModel):
    """Defer a composed prompt into an already-created session.

    The session is materialized by the caller first (the same
    ``ensureRealSession`` path a normal first send uses), so this only
    persists the pending message + acknowledgment and, for timed options,
    schedules the one-shot wakeup.
    """

    session_id: str
    message: str = ""
    # One of the four composer options: "30m" | "1h" | "24h" | "none".
    delay: str = "none"
    # Attachments already uploaded against the session (mirrors the shape
    # the composer sends on a normal message).
    file_ids: list[str] | None = None
    image_blocks: list[dict] | None = None


# --- Session endpoints ---

def _loop_summary(lp: dict) -> dict:
    return {
        "id": lp["id"],
        "status": lp["status"],
        "iteration": lp.get("iteration"),
        "max_iterations": lp.get("max_iterations"),
        "failure_reason": lp.get("failure_reason"),
        "budget_usd": lp.get("budget_usd"),
        "spent_usd": lp.get("spent_usd"),
        "updated_at": lp.get("updated_at"),
    }


async def _attach_review_loops(deps, sessions: list[dict]) -> None:
    """Stamp each observer session with its newest loop's summary — the
    sidebar icon, status dot, and header chip rehydrate from this (the
    review_loop_update WS event only covers the live tab)."""
    try:
        from nerve.workflows import get_review_loop_service

        if get_review_loop_service() is None:
            return
        loops = await deps.db.list_review_loops(limit=100)
    except Exception:  # noqa: BLE001 — enrichment must never break listing
        return
    newest: dict[str, dict] = {}
    for lp in loops:  # newest first
        sid = lp.get("session_id")
        if sid and sid not in newest:
            newest[sid] = lp
    for s in sessions:
        lp = newest.get(s["id"])
        if lp:
            s["review_loop"] = _loop_summary(lp)


def _page_size() -> int | None:
    """Sidebar page size from config; ``None`` when configured unlimited."""
    size = get_config().sessions.sidebar_page_size
    return size if size and size > 0 else None


async def _pending_wakeup_map(deps) -> dict[str, str]:
    """``session_id -> fire_at`` for every pending wakeup, in one query.

    ``ScheduleWakeup`` keeps at most one pending row per session, so a plain
    map is enough. Enrichment: a failure dims the indicator, it must never
    fail the listing.
    """
    try:
        rows = await deps.db.list_pending_wakeups()
    except Exception:  # noqa: BLE001 — enrichment must never break listing
        return {}
    return {r["session_id"]: r["fire_at"] for r in rows if r.get("session_id")}


async def _decorate(deps, sessions: list[dict]) -> list[dict]:
    """Attach the live per-row bits every sidebar list needs."""
    running_ids = deps.engine.sessions.get_running_ids()
    awaiting_ids = get_awaiting_ids()
    # Pending work: no turn is in flight, yet the session isn't idle either —
    # a wakeup is scheduled, or a background task is still running inside its
    # CLI. The sidebar keeps those parked sessions in the "Running" group
    # (with their own dot colour) instead of dropping them to idle.
    wakeup_at = await _pending_wakeup_map(deps)
    for s in sessions:
        s["is_running"] = s["id"] in running_ids
        s["awaiting_input"] = s["id"] in awaiting_ids
        s["pending_wakeup_at"] = wakeup_at.get(s["id"])
        s["has_background_tasks"] = deps.engine.has_live_background_tasks(s["id"])
    await _attach_review_loops(deps, sessions)
    return sessions


def _page_meta(page: list[dict], offset: int, total: int, limit: int | None) -> dict:
    """``has_more``/``next_offset`` for the client's '...' control."""
    seen = offset + len(page)
    return {"has_more": limit is not None and seen < total, "next_offset": seen}


@router.get("/api/sessions")
async def list_sessions(offset: int = 0, user: dict = Depends(require_auth)):
    """Sidebar feed: one page of conversations, plus every starred session (starred ride along in full on offset=0)."""
    deps = get_deps()
    limit = _page_size()
    page = await deps.engine.sessions.list_conversation_sessions(limit=limit, offset=offset)
    total = await deps.engine.sessions.count_conversation_sessions()
    sessions = page if offset else await deps.engine.sessions.list_starred_sessions() + page
    await _decorate(deps, sessions)
    return {
        "sessions": sessions,
        "archived_count": await deps.engine.sessions.count_archived_sessions(),
        "system_count": await deps.engine.sessions.count_system_sessions(),
        **_page_meta(page, offset, total, limit),
    }


@router.get("/api/sessions/search")
async def search_sessions(q: str, user: dict = Depends(require_auth)):
    """Search sessions by title across all non-archived sessions."""
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required")
    deps = get_deps()
    sessions = await deps.db.search_sessions(q.strip())
    await _decorate(deps, sessions)
    return {"sessions": sessions}


@router.get("/api/sessions/archived")
async def list_archived_sessions(offset: int = 0, user: dict = Depends(require_auth)):
    """One page of archived sessions — fetched only when the group is expanded."""
    deps = get_deps()
    limit = _page_size()
    page = await deps.engine.sessions.list_archived_sessions(limit=limit, offset=offset)
    total = await deps.engine.sessions.count_archived_sessions()
    await _decorate(deps, page)
    return {"sessions": page, **_page_meta(page, offset, total, limit)}


@router.get("/api/sessions/system")
async def list_system_sessions(offset: int = 0, user: dict = Depends(require_auth)):
    """One page of system (cron/hook) sessions — fetched only when expanded."""
    deps = get_deps()
    limit = _page_size()
    page = await deps.engine.sessions.list_system_sessions(limit=limit, offset=offset)
    total = await deps.engine.sessions.count_system_sessions()
    await _decorate(deps, page)
    return {"sessions": page, **_page_meta(page, offset, total, limit)}


@router.post("/api/sessions")
async def create_session(req: SessionCreateRequest, user: dict = Depends(require_auth)):
    deps = get_deps()
    metadata = None
    if req.backend:
        backend = req.backend.strip().lower()
        if backend not in deps.engine._backends:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown backend {backend!r} "
                       f"(known: {sorted(deps.engine._backends)})",
            )
        metadata = {"backend_override": backend}
    else:
        backend = deps.engine.config.agent.backend
    if backend == "codex":
        preflight = await deps.engine._backends["codex"].preflight()
        if not preflight.get("available"):
            raise HTTPException(
                status_code=503,
                detail=f"Codex is unavailable: {preflight.get('reason', 'preflight failed')}",
            )
    cwd = str(deps.engine.config.workspace)
    if req.cwd:
        path = Path(req.cwd).expanduser().resolve()
        if not path.is_dir():
            # Auto-create missing workdirs (review loops and chats may
            # target a fresh scratch directory).
            try:
                path.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot create working directory {path}: {e}",
                ) from e
        cwd = str(path)
    selected_backend = deps.engine._backends[backend]
    requested_model = (req.model or "").strip()
    if requested_model:
        # Same optional-validation seam the engine uses per turn: backends
        # that can cheaply reject a model (codex) do so here, before a row
        # with an unservable model is minted; claude accepts any ID (Ollama
        # models ride the claude backend, so the list is open-ended).
        validate_model = getattr(selected_backend, "validate_model", None)
        if validate_model is not None:
            try:
                await validate_model(requested_model)
            except BackendError as e:
                raise HTTPException(status_code=400, detail=str(e)) from e
    model = requested_model or selected_backend.default_model(req.source)
    session_id = str(uuid.uuid4())[:8]
    session = await deps.engine.sessions.get_or_create(
        session_id, title=req.title, source=req.source, metadata=metadata,
        backend=backend, model=model, cwd=cwd,
    )
    if req.review_loop is not None:
        session = await _attach_review_loop(deps, session, session_id, cwd, req)
    return session


async def _attach_review_loop(deps, session: dict, session_id: str, cwd: str, req: SessionCreateRequest) -> dict:
    """Create the review loop for a freshly created observer session. A
    rejected loop config rolls the session back — no stray empty sessions."""
    from nerve.workflows import get_review_loop_service
    from nerve.workflows.review_loop import ReviewLoopError

    service = get_review_loop_service()
    if service is None:
        await deps.db.delete_session(session_id)
        raise HTTPException(
            status_code=503,
            detail="review loops are disabled (workflows.review_loop.enabled)",
        )
    rl = req.review_loop
    try:
        loop = await service.create_loop(
            goal=rl.goal,
            verifier=rl.verifier,
            session_id=session_id,
            cwd=cwd,
            title=req.title or "",
            budget_usd=rl.budget_usd,
            max_iterations=rl.max_iterations,
            criteria_adoption=rl.criteria_adoption,
            implementer=(
                rl.implementer.model_dump(exclude_none=True)
                if rl.implementer else None
            ),
            verifier_leg=(
                rl.verifier_leg.model_dump(exclude_none=True)
                if rl.verifier_leg else None
            ),
            created_by=f"session:{session_id}",
        )
    except ReviewLoopError as e:
        await deps.db.delete_session(session_id)
        raise HTTPException(status_code=400, detail=str(e))
    try:
        meta = json.loads(session.get("metadata") or "{}")
        if not isinstance(meta, dict):
            meta = {}
        meta["review_loop_id"] = loop["id"]
        await deps.db.update_session_metadata(session_id, meta)
    except Exception:  # noqa: BLE001 — pointer metadata is best-effort
        logger.warning("review_loop_id metadata stamp failed for %s", session_id)
    session = dict(session)
    session["review_loop_id"] = loop["id"]
    return session


@router.get("/api/sessions/{session_id}")
async def get_session(session_id: str, user: dict = Depends(require_auth)):
    deps = get_deps()
    session = await deps.db.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session


@router.get("/api/sessions/{session_id}/messages")
async def get_messages(session_id: str, limit: int = 500, user: dict = Depends(require_auth)):
    deps = get_deps()
    messages = await deps.db.get_messages(session_id, limit=limit)
    session = await deps.db.get_session(session_id)

    # Return last_usage if stored (for context bar on session switch)
    last_usage = None
    if session:
        meta = json.loads(session.get("metadata") or "{}")
        last_usage = meta.get("last_usage")

    return {"messages": messages, "last_usage": last_usage}


async def _would_create_cycle(db, child_id: str, new_parent_id: str) -> bool:
    """True if making ``child_id`` a child of ``new_parent_id`` forms a loop.

    Walks up the ancestor chain from the proposed parent; reaching ``child_id``
    means the drop would nest a session under its own descendant. The seen-set
    also breaks any pre-existing cycle so the walk always terminates.
    """
    seen: set[str] = set()
    cursor: str | None = new_parent_id
    while cursor:
        if cursor == child_id:
            return True
        if cursor in seen:
            break
        seen.add(cursor)
        row = await db.get_session(cursor)
        cursor = row.get("parent_session_id") if row else None
    return False


@router.patch("/api/sessions/{session_id}")
async def update_session(session_id: str, req: dict, user: dict = Depends(require_auth)):
    """Update session fields (title, starred, model, parent_session_id).

    ``parent_session_id`` is the sidebar drag-to-nest relationship (null clears
    it → top-level). It is display-only: guarded against self/cycle links and
    against the ``created`` fork window so a drag never alters execution.

    ``model`` re-points THIS session only — the composer's picker is
    per-chat, not a global preference. The engine re-reads the session
    row each turn (per-message override -> ``sessions.model`` -> backend
    default), so the switch takes effect on the session's next message.

    When ``sessions.star_project_hook`` is enabled (default off), a ``starred``
    0<->1 transition fires a one-shot internal turn in that same session so the
    agent can promote it to a project (create its Task card) or archive it.
    """
    deps = get_deps()
    session = await deps.db.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    fields: dict = {}
    if "title" in req:
        fields["title"] = req["title"]
    if "starred" in req:
        fields["starred"] = 1 if req["starred"] else 0
    if "model" in req:
        requested_model = str(req["model"] or "").strip()
        if not requested_model:
            raise HTTPException(status_code=400, detail="model cannot be empty")
        # Backend is sticky on the session row — validate the pick against
        # it (same optional seam as session creation: codex can cheaply
        # reject; claude accepts any ID since Ollama models ride it).
        backend_id = session.get("backend") or deps.engine.config.agent.backend
        selected_backend = deps.engine._backends.get(backend_id)
        validate_model = getattr(selected_backend, "validate_model", None)
        if validate_model is not None:
            try:
                await validate_model(requested_model)
            except BackendError as e:
                raise HTTPException(status_code=400, detail=str(e)) from e
        fields["model"] = requested_model
    if "parent_session_id" in req:
        raw = req["parent_session_id"]
        new_parent = str(raw).strip() if raw not in (None, "") else None
        if new_parent is not None:
            if new_parent == session_id:
                raise HTTPException(status_code=400, detail="A session cannot be its own parent")
            if not await deps.db.get_session(new_parent):
                raise HTTPException(status_code=404, detail="Parent session not found")
            # Fork-window guard: the engine reads parent_session_id to fork a
            # brand-new session's first turn ONLY while status == 'created'.
            # Refuse to set a parent in that window so a display drag can never
            # turn a not-yet-started session into a fork. Anything already run
            # (everything draggable in the sidebar) is unaffected.
            if session.get("status") == "created":
                raise HTTPException(
                    status_code=409,
                    detail="Send a first message before nesting this session",
                )
            if await _would_create_cycle(deps.db, session_id, new_parent):
                raise HTTPException(status_code=400, detail="That drop would create a cycle")
        fields["parent_session_id"] = new_parent
    if not fields:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    old_starred = int(session.get("starred") or 0)
    # Starring an archived session restores it via the shared unarchive path (logs "unarchived", bumps updated_at) before the star write, so the star->project hook fires on a live session.
    if fields.get("starred") == 1 and session.get("status") == "archived":
        if deps.engine:
            await deps.engine.sessions.unarchive_session(session_id)
        else:
            fields["status"] = "idle"
            fields["archived_at"] = None
    await deps.db.update_session_fields(session_id, fields)
    updated = await deps.db.get_session(session_id)
    # Star = opt-in project registration (sessions.star_project_hook, default
    # off). On a starred transition fire a one-shot internal turn.
    if (get_config().sessions.star_project_hook and "starred" in fields
            and deps.engine and fields["starred"] != old_starred):
        promoted = fields["starred"] == 1
        trigger = (
            "[system:star-promote] This session was just STARRED, which registers it as a "
            "PROJECT. Per the multi-session protocol: create or refresh this project's Task card "
            "(the umbrella for this session and its children), register it in the task-heartbeat "
            "registry, then stop. Keep that card updated at the end of every future turn."
            if promoted else
            "[system:star-unpromote] This session was just UNSTARRED, so it is no longer a "
            "project. Archive its project Task (keep the ## Updates audit trail) and deregister it "
            "from the task-heartbeat registry, then stop."
        )
        try:
            asyncio.create_task(
                deps.engine.run(
                    session_id=session_id, user_message=trigger,
                    source="web", internal=True,
                )
            )
        except Exception as e:  # a hook failure must never break the PATCH
            logger.warning("star-project hook failed for %s: %s", session_id, e)
    return updated


@router.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str, user: dict = Depends(require_auth)):
    deps = get_deps()
    engine = deps.engine
    db = deps.db

    if engine:
        # Disconnect client
        client = engine.sessions.remove_client(session_id)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        # Snapshot messages for background memorization before deleting
        session = await db.get_session(session_id)
        connected_at = session.get("connected_at") if session else None
        messages = await db.get_messages(session_id, limit=10000) if connected_at else []
        # Delete from DB immediately (fast)
        await db.delete_session(session_id)
        # Cleanup uploaded files for this session
        await _cleanup_uploaded_files(db, session_id)
        # Memorize in background from snapshot
        if messages and connected_at and engine._memory_bridge and engine._memory_bridge.available:
            async def _bg_memorize():
                try:
                    context_msgs = []
                    for msg in messages:
                        created = msg.get("created_at", "")
                        if created:
                            norm = created if "T" in created else created.replace(" ", "T") + "Z"
                            if norm >= connected_at:
                                context_msgs.append(msg)
                    if context_msgs:
                        await engine._memory_bridge.memorize_conversation(session_id, context_msgs)
                        # Same window to xmemory (opt-in, fire-and-forget).
                        engine.schedule_xmemory_transcript(session_id, context_msgs)
                except Exception as e:
                    logger.warning("Background memorize for deleted session %s failed: %s", session_id, e)
            asyncio.create_task(_bg_memorize())
    else:
        await db.delete_session(session_id)
        await _cleanup_uploaded_files(db, session_id)
    return {"deleted": True}


@router.get("/api/sessions/{session_id}/status")
async def session_status(session_id: str, user: dict = Depends(require_auth)):
    """Enhanced session status with lifecycle info."""
    deps = get_deps()
    session = await deps.db.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    is_running = deps.engine.is_session_running(session_id)
    return {
        "session_id": session_id,
        "status": session.get("status", "unknown"),
        "is_running": is_running,
        "awaiting_input": session_id in get_awaiting_ids(),
        "sdk_session_id": session.get("sdk_session_id"),
        "connected_at": session.get("connected_at"),
        "parent_session_id": session.get("parent_session_id"),
        "message_count": session.get("message_count", 0),
        "total_cost_usd": session.get("total_cost_usd", 0),
    }


@router.post("/api/sessions/fork")
async def fork_session(req: ForkRequest, user: dict = Depends(require_auth)):
    """Fork a session, optionally from a specific message."""
    deps = get_deps()
    try:
        fork = await deps.engine.fork_session(
            req.source_session_id, req.at_message_id, req.title,
        )
        return fork
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# "Run later" — defer a prompt without spending tokens now. The three timed
# options map to a delay in seconds + the synthetic acknowledgment text; the
# manual option schedules nothing.
_RUN_LATER_TIMED = {
    "30m": (30 * 60, "Acknowledged — scheduled to run in 30 minutes."),
    "1h": (60 * 60, "Acknowledged — scheduled to run in 1 hour."),
    "24h": (24 * 60 * 60, "Acknowledged — scheduled to run in 24 hours."),
}
_RUN_LATER_MANUAL_ACK = "Acknowledged — saved to run manually later."


@router.post("/api/sessions/run-later")
async def run_later(req: RunLaterRequest, user: dict = Depends(require_auth)):
    """Defer a composed prompt: persist it as the session's pending user
    message plus a synthetic assistant acknowledgment (written directly, with
    no LLM call and zero token spend), and — for the three timed options —
    schedule a one-shot wakeup that fires ``engine.run`` on the stored prompt
    after the delay. The wakeup reuses the ScheduleWakeup substrate swept by
    ``CronService._sweep_wakeups``; the manual option ("Just accept it")
    schedules nothing and is run by the user later.
    """
    deps = get_deps()
    session = await deps.db.get_session(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    message = (req.message or "").strip()
    if not message and not req.file_ids:
        raise HTTPException(status_code=400, detail="Nothing to defer")

    delay = (req.delay or "none").strip().lower()
    if delay != "none" and delay not in _RUN_LATER_TIMED:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown delay {delay!r} (want one of: 30m, 1h, 24h, none)",
        )

    # Pending user message — mirror the block shape a normal send persists:
    # a text block plus one block per uploaded attachment. Resolve file_ids
    # to records so BOTH images and ordinary files are carried into the
    # deferred session (mirrors server._load_uploaded_files' image_refs:
    # image type -> "image" block, everything else -> "file" block). Fall
    # back to the caller-supplied image_blocks when no file_ids are given.
    blocks: list[dict] = []
    if message:
        blocks.append({"type": "text", "content": message})
    if req.file_ids:
        records = await deps.db.get_uploaded_files_by_ids(req.file_ids)
        by_id = {rec["id"]: rec for rec in records}
        for fid in req.file_ids:
            rec = by_id.get(fid)
            if not rec:
                continue
            url = f"/api/files/uploads/{fid}"
            if rec["file_type"] == "image":
                blocks.append({
                    "type": "image",
                    "url": url,
                    "filename": rec["filename"],
                    "media_type": rec["media_type"],
                })
            else:
                blocks.append({
                    "type": "file",
                    "url": url,
                    "filename": rec["filename"],
                    "media_type": rec["media_type"],
                })
    else:
        for img in req.image_blocks or []:
            blocks.append({
                "type": "image",
                "url": img.get("url"),
                "filename": img.get("filename"),
                "media_type": img.get("media_type"),
            })
    await deps.db.add_message(
        req.session_id, role="user", content=message,
        blocks=blocks or None, channel="web",
    )

    scheduled = delay != "none"
    fire_at: str | None = None
    if scheduled:
        seconds, ack = _RUN_LATER_TIMED[delay]
        # Compute fire_at directly (not via engine._wakeup_fire_at, which
        # clamps to <=3600s) so the 24h option works. The wakeup store keeps
        # a single pending wakeup per session, which suits a fresh session.
        fire_at = (
            datetime.now(timezone.utc) + timedelta(seconds=seconds)
        ).isoformat()
        await deps.db.add_wakeup(
            req.session_id, prompt=message, fire_at=fire_at, reason="run-later",
        )
    else:
        ack = _RUN_LATER_MANUAL_ACK

    # Synthetic acknowledgment — persisted as a plain assistant row, never
    # generated by the model (zero tokens to create the deferred session).
    await deps.db.add_message(
        req.session_id, role="assistant", content=ack, channel="web",
    )

    return {
        "session_id": req.session_id,
        "scheduled": scheduled,
        "ack": ack,
        "fire_at": fire_at,
    }


@router.post("/api/sessions/{session_id}/resume")
async def resume_session(session_id: str, user: dict = Depends(require_auth)):
    """Resume a stopped or idle session."""
    deps = get_deps()
    try:
        session = await deps.engine.resume_session(session_id)
        return session
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/sessions/{session_id}/archive")
async def archive_session(session_id: str, user: dict = Depends(require_auth)):
    """Archive a session and its entire descendant subtree (soft delete).

    Cascade is the default: children, grandchildren and deeper are archived
    bottom-up. Returns a structured report of which session ids were archived
    vs skipped (with reasons) so a still-running descendant never silently
    leaves an archived-parent-with-active-child orphan.
    """
    deps = get_deps()
    result = await deps.engine.sessions.archive_session_cascade(session_id)
    return {
        "archived": True,
        "archived_ids": result["archived"],
        "skipped": result["skipped"],
    }


@router.post("/api/sessions/{session_id}/unarchive")
async def unarchive_session(session_id: str, user: dict = Depends(require_auth)):
    """Restore an archived session (Archived group → Unarchive / Star)."""
    deps = get_deps()
    try:
        await deps.engine.sessions.unarchive_session(session_id)
        return {"unarchived": True}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/api/sessions/{session_id}/events")
async def get_session_events(
    session_id: str, limit: int = 50, user: dict = Depends(require_auth),
):
    """Get the lifecycle event log for a session."""
    deps = get_deps()
    events = await deps.db.get_session_events(session_id, limit=limit)
    return {"events": events}


# --- Modified files ---

@router.get("/api/sessions/{session_id}/modified-files")
async def get_modified_files(session_id: str, user: dict = Depends(require_auth)):
    """List files modified during a session with +/- stats."""
    deps = get_deps()
    session = await deps.db.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    snapshots = await deps.db.get_session_snapshots(session_id)
    if not snapshots:
        return {"files": [], "summary": {"total_files": 0, "total_additions": 0, "total_deletions": 0}}

    from nerve.gateway.diff import compute_quick_stats, shorten_path

    config = get_config()
    workspace = str(config.workspace)
    files = []
    total_add = 0
    total_del = 0

    for snap in snapshots:
        file_path = snap["file_path"]
        # Read current content from disk
        try:
            current = await asyncio.to_thread(
                Path(file_path).read_text, encoding="utf-8", errors="replace",
            )
        except (FileNotFoundError, OSError):
            current = None

        # Get original from DB (need full row with content)
        full_snap = await deps.db.get_file_snapshot(session_id, file_path)
        original = full_snap["original_content"] if full_snap else None

        stats = await asyncio.to_thread(compute_quick_stats, original, current)
        total_add += stats["additions"]
        total_del += stats["deletions"]

        # Determine status
        if original is None:
            status = "created"
        elif current is None:
            status = "deleted"
        elif original == current:
            status = "unchanged"
        else:
            status = "modified"

        files.append({
            "path": file_path,
            "short_path": shorten_path(file_path, workspace),
            "status": status,
            "stats": stats,
            "created_at": snap.get("created_at"),
        })

    # Filter out unchanged (file was snapshotted but then reverted)
    files = [f for f in files if f["status"] != "unchanged"]

    return {
        "files": files,
        "summary": {
            "total_files": len(files),
            "total_additions": total_add,
            "total_deletions": total_del,
        },
    }


@router.get("/api/sessions/{session_id}/file-diff")
async def get_file_diff(
    session_id: str,
    path: str,
    context: int = 4,
    user: dict = Depends(require_auth),
):
    """Compute a unified diff for a single file against its session baseline snapshot."""
    deps = get_deps()
    session = await deps.db.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    snap = await deps.db.get_file_snapshot(session_id, path)
    if not snap:
        raise HTTPException(status_code=404, detail="No snapshot found for this file in this session")

    original = snap["original_content"]  # None = file was created during session

    # Read current file from disk
    try:
        current = await asyncio.to_thread(
            Path(path).read_text, encoding="utf-8", errors="replace",
        )
    except (FileNotFoundError, OSError):
        current = None

    from nerve.gateway.diff import compute_file_diff

    config = get_config()
    # Diffing large files is CPU-bound — off the event loop.
    diff = await asyncio.to_thread(
        compute_file_diff,
        original_content=original,
        current_content=current,
        file_path=path,
        context_lines=context,
        workspace=str(config.workspace),
    )
    return diff


# --- Chat ---

@router.post("/api/chat", response_model=MessageResponse)
async def chat(req: MessageRequest, user: dict = Depends(require_auth)):
    """Send a message and get a response (non-streaming). Use WebSocket for streaming."""
    deps = get_deps()
    session_id = req.session_id
    if session_id is None:
        session_id = await deps.engine.sessions.get_active_session(
            "api:default", source="api",
        )
    response = await deps.engine.run(
        session_id=session_id,
        user_message=req.message,
        source="web",
        channel="web",
    )
    return MessageResponse(response=response, session_id=session_id)
