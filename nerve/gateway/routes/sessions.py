"""Session and chat routes."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

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


class SessionCreateRequest(BaseModel):
    title: str | None = None
    source: str = "web"


class ForkRequest(BaseModel):
    source_session_id: str
    at_message_id: str | None = None
    title: str | None = None


# --- Session endpoints ---

@router.get("/api/sessions")
async def list_sessions(user: dict = Depends(require_auth)):
    deps = get_deps()
    sessions = await deps.engine.sessions.list_sessions()
    running_ids = deps.engine.sessions.get_running_ids()
    for s in sessions:
        s["is_running"] = s["id"] in running_ids
    return {"sessions": sessions}


@router.get("/api/sessions/search")
async def search_sessions(q: str, user: dict = Depends(require_auth)):
    """Search sessions by title across all non-archived sessions."""
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required")
    deps = get_deps()
    sessions = await deps.db.search_sessions(q.strip())
    running_ids = deps.engine.sessions.get_running_ids()
    for s in sessions:
        s["is_running"] = s["id"] in running_ids
    return {"sessions": sessions}


@router.post("/api/sessions")
async def create_session(req: SessionCreateRequest, user: dict = Depends(require_auth)):
    deps = get_deps()
    session_id = str(uuid.uuid4())[:8]
    session = await deps.engine.sessions.get_or_create(
        session_id, title=req.title, source=req.source
    )
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


@router.patch("/api/sessions/{session_id}")
async def update_session(session_id: str, req: dict, user: dict = Depends(require_auth)):
    """Update session fields (title, starred)."""
    deps = get_deps()
    session = await deps.db.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    fields: dict = {}
    if "title" in req:
        fields["title"] = req["title"]
    if "starred" in req:
        fields["starred"] = 1 if req["starred"] else 0
    if not fields:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    await deps.db.update_session_fields(session_id, fields)
    updated = await deps.db.get_session(session_id)
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
    """Archive a session (soft delete)."""
    deps = get_deps()
    await deps.engine.sessions.archive_session(session_id)
    return {"archived": True}


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
            current = Path(file_path).read_text(encoding="utf-8", errors="replace")
        except (FileNotFoundError, OSError):
            current = None

        # Get original from DB (need full row with content)
        full_snap = await deps.db.get_file_snapshot(session_id, file_path)
        original = full_snap["original_content"] if full_snap else None

        stats = compute_quick_stats(original, current)
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
        current = Path(path).read_text(encoding="utf-8", errors="replace")
    except (FileNotFoundError, OSError):
        current = None

    from nerve.gateway.diff import compute_file_diff

    config = get_config()
    diff = compute_file_diff(
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
