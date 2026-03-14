"""REST API routes for sessions, tasks, memory, and diagnostics."""

from __future__ import annotations

import json
import logging
import os
import platform
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.config import get_config
from nerve.gateway.auth import create_token, require_auth, verify_password

logger = logging.getLogger(__name__)

router = APIRouter()

# These are set during server startup
_engine = None  # AgentEngine
_db = None  # Database
_notification_service = None  # NotificationService


def init_routes(engine, db):
    global _engine, _db
    _engine = engine
    _db = db


def set_notification_service(service):
    global _notification_service
    _notification_service = service


# --- Auth ---

class LoginRequest(BaseModel):
    password: str

class LoginResponse(BaseModel):
    token: str

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

class TaskCreateRequest(BaseModel):
    title: str
    content: str = ""
    source: str = "manual"
    source_url: str = ""
    deadline: str = ""

class TaskUpdateRequest(BaseModel):
    status: str = ""
    note: str = ""
    deadline: str = ""
    content: str = ""

class PlanUpdateRequest(BaseModel):
    status: str = ""        # decline
    feedback: str = ""

class PlanReviseRequest(BaseModel):
    feedback: str

class SkillCreateRequest(BaseModel):
    name: str
    description: str
    content: str = ""
    version: str = "1.0.0"

class SkillUpdateRequest(BaseModel):
    content: str

class SkillToggleRequest(BaseModel):
    enabled: bool


@router.post("/api/auth/login", response_model=LoginResponse)
async def login(req: LoginRequest):
    config = get_config()
    if not config.auth.password_hash or not config.auth.jwt_secret:
        # Dev mode — accept any password
        return LoginResponse(token=create_token(config.auth.jwt_secret or "dev-secret"))

    if not verify_password(req.password, config.auth.password_hash):
        raise HTTPException(status_code=401, detail="Invalid password")

    token = create_token(config.auth.jwt_secret)
    return LoginResponse(token=token)


@router.get("/api/auth/check")
async def check_auth(user: dict = Depends(require_auth)):
    return {"authenticated": True}


# --- Sessions ---

@router.get("/api/sessions")
async def list_sessions(user: dict = Depends(require_auth)):
    sessions = await _engine.sessions.list_sessions()
    # Annotate each session with real-time running status
    running_ids = _engine.sessions.get_running_ids()
    for s in sessions:
        s["is_running"] = s["id"] in running_ids
    return {"sessions": sessions}


@router.get("/api/sessions/search")
async def search_sessions(q: str, user: dict = Depends(require_auth)):
    """Search sessions by title across all non-archived sessions."""
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required")
    sessions = await _db.search_sessions(q.strip())
    running_ids = _engine.sessions.get_running_ids()
    for s in sessions:
        s["is_running"] = s["id"] in running_ids
    return {"sessions": sessions}


@router.post("/api/sessions")
async def create_session(req: SessionCreateRequest, user: dict = Depends(require_auth)):
    import uuid
    session_id = str(uuid.uuid4())[:8]
    session = await _engine.sessions.get_or_create(
        session_id, title=req.title, source=req.source
    )
    return session


@router.get("/api/sessions/{session_id}")
async def get_session(session_id: str, user: dict = Depends(require_auth)):
    session = await _db.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session


@router.get("/api/sessions/{session_id}/messages")
async def get_messages(session_id: str, limit: int = 500, user: dict = Depends(require_auth)):
    messages = await _db.get_messages(session_id, limit=limit)
    session = await _db.get_session(session_id) if _db else None

    # Return last_usage if stored (for context bar on session switch)
    last_usage = None
    if session:
        meta = json.loads(session.get("metadata") or "{}")
        last_usage = meta.get("last_usage")

    return {"messages": messages, "last_usage": last_usage}


@router.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str, user: dict = Depends(require_auth)):
    if _engine:
        # Disconnect client
        client = _engine.sessions.remove_client(session_id)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        # Snapshot messages for background memorization before deleting
        session = await _db.get_session(session_id)
        connected_at = session.get("connected_at") if session else None
        messages = await _db.get_messages(session_id, limit=10000) if connected_at else []
        # Delete from DB immediately (fast)
        await _db.delete_session(session_id)
        # Memorize in background from snapshot
        if messages and connected_at and _engine._memory_bridge and _engine._memory_bridge.available:
            import asyncio
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
                        await _engine._memory_bridge.memorize_conversation(session_id, context_msgs)
                except Exception as e:
                    logger.warning("Background memorize for deleted session %s failed: %s", session_id, e)
            asyncio.create_task(_bg_memorize())
    else:
        await _db.delete_session(session_id)
    return {"deleted": True}


@router.get("/api/sessions/{session_id}/status")
async def session_status(session_id: str, user: dict = Depends(require_auth)):
    """Enhanced session status with lifecycle info."""
    session = await _db.get_session(session_id) if _db else None
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    is_running = _engine.is_session_running(session_id) if _engine else False
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
    try:
        fork = await _engine.fork_session(
            req.source_session_id, req.at_message_id, req.title,
        )
        return fork
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/api/sessions/{session_id}/resume")
async def resume_session(session_id: str, user: dict = Depends(require_auth)):
    """Resume a stopped or idle session."""
    try:
        session = await _engine.resume_session(session_id)
        return session
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/sessions/{session_id}/archive")
async def archive_session(session_id: str, user: dict = Depends(require_auth)):
    """Archive a session (soft delete)."""
    await _engine.sessions.archive_session(session_id)
    return {"archived": True}


@router.get("/api/sessions/{session_id}/events")
async def get_session_events(
    session_id: str, limit: int = 50, user: dict = Depends(require_auth),
):
    """Get the lifecycle event log for a session."""
    events = await _db.get_session_events(session_id, limit=limit)
    return {"events": events}


# --- Modified files ---

@router.get("/api/sessions/{session_id}/modified-files")
async def get_modified_files(session_id: str, user: dict = Depends(require_auth)):
    """List files modified during a session with +/- stats."""
    session = await _db.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    snapshots = await _db.get_session_snapshots(session_id)
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
        full_snap = await _db.get_file_snapshot(session_id, file_path)
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
    session = await _db.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    snap = await _db.get_file_snapshot(session_id, path)
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
    session_id = req.session_id
    if session_id is None:
        session_id = await _engine.sessions.get_active_session(
            "api:default", source="api",
        )
    response = await _engine.run(
        session_id=session_id,
        user_message=req.message,
        source="web",
        channel="web",
    )
    return MessageResponse(response=response, session_id=session_id)


# --- Tasks ---

@router.get("/api/tasks")
async def list_tasks(status: str = "", user: dict = Depends(require_auth)):
    tasks = await _db.list_tasks(status=status or None)
    return {"tasks": tasks}


@router.get("/api/tasks/search")
async def search_tasks(q: str, status: str = "", user: dict = Depends(require_auth)):
    tasks = await _db.search_tasks(query=q, status=status or None)
    return {"tasks": tasks}


@router.post("/api/tasks")
async def create_task(req: TaskCreateRequest, user: dict = Depends(require_auth)):
    from nerve.agent.tools import task_create
    result = await task_create.handler({
        "title": req.title,
        "content": req.content,
        "source": req.source,
        "source_url": req.source_url,
        "deadline": req.deadline,
    })
    return result


@router.get("/api/tasks/{task_id}")
async def get_task(task_id: str, user: dict = Depends(require_auth)):
    task = await _db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    # Include markdown file content
    config = get_config()
    file_path = config.workspace / task["file_path"]
    content = ""
    if file_path.exists():
        content = file_path.read_text(encoding="utf-8")
    return {**dict(task), "content": content}


@router.patch("/api/tasks/{task_id}")
async def update_task(task_id: str, req: TaskUpdateRequest, user: dict = Depends(require_auth)):
    task = await _db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    # Update status/note/deadline via agent tool
    if req.status or req.note or req.deadline:
        from nerve.agent.tools import task_update
        await task_update.handler({
            "task_id": task_id,
            "status": req.status,
            "note": req.note,
            "deadline": req.deadline,
        })

    # Update full markdown content if provided
    if req.content:
        config = get_config()
        file_path = config.workspace / task["file_path"]
        if file_path.exists():
            file_path.write_text(req.content, encoding="utf-8")
            # Re-sync title from markdown to SQLite
            from nerve.tasks.models import parse_task_title, parse_task_frontmatter
            new_title = parse_task_title(req.content)
            fields = parse_task_frontmatter(req.content)
            await _db.upsert_task(
                task_id=task_id,
                file_path=task["file_path"],
                title=new_title,
                status=req.status or task["status"],
                source=task.get("source"),
                source_url=task.get("source_url"),
                deadline=fields.get("deadline") or task.get("deadline"),
            )

    return {"task_id": task_id, "updated": True}


# --- Plans ---

@router.get("/api/plans")
async def list_plans(status: str = "", task_id: str = "", user: dict = Depends(require_auth)):
    plans = await _db.list_plans(
        status=status or None,
        task_id=task_id or None,
    )
    return {"plans": plans}


@router.get("/api/plans/{plan_id}")
async def get_plan(plan_id: str, user: dict = Depends(require_auth)):
    plan = await _db.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    return plan


@router.patch("/api/plans/{plan_id}")
async def update_plan(plan_id: str, req: PlanUpdateRequest, user: dict = Depends(require_auth)):
    plan = await _db.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")

    fields = {}
    if req.status:
        fields["status"] = req.status
        fields["reviewed_at"] = datetime.now(timezone.utc).isoformat()
    if req.feedback:
        fields["feedback"] = req.feedback

    if fields:
        await _db.update_plan(plan_id, **fields)

    # Write task note on decline
    if req.status == "declined":
        from nerve.agent.tools import task_update as task_update_tool
        feedback_suffix = ""
        if req.feedback:
            feedback_suffix = f" — {req.feedback[:80]}{'...' if len(req.feedback) > 80 else ''}"
        await task_update_tool.handler({
            "task_id": plan["task_id"],
            "note": f"Plan declined: {plan_id}{feedback_suffix}",
        })

    return {"plan_id": plan_id, "updated": True}


@router.post("/api/plans/{plan_id}/revise")
async def revise_plan(plan_id: str, req: PlanReviseRequest, user: dict = Depends(require_auth)):
    """Send revision feedback to the persistent planner session."""
    plan = await _db.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    task = await _db.get_task(plan["task_id"])
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    # Store feedback on the plan
    await _db.update_plan(plan_id, feedback=req.feedback)

    # Write task note
    from nerve.agent.tools import task_update as task_update_tool
    feedback_summary = req.feedback[:80] + "..." if len(req.feedback) > 80 else req.feedback
    await task_update_tool.handler({
        "task_id": plan["task_id"],
        "note": f"Revision requested for {plan_id}: {feedback_summary}",
    })

    # Send revision request to the persistent planner session
    feedback_prompt = (
        f'Revise plan {plan_id} for task "{task["title"]}" based on this feedback:\n\n'
        f"{req.feedback}\n\n"
        f"Explore the codebase again if needed, then call "
        f'plan_propose(task_id="{plan["task_id"]}", content="...") with the revised plan.'
    )

    import asyncio
    session_id = "cron:task-planner"
    # Ensure the session exists
    await _engine.sessions.get_or_create(
        session_id, title="Cron: task-planner", source="cron",
    )
    asyncio.create_task(
        _engine.run(session_id=session_id, user_message=feedback_prompt, source="cron")
    )
    return {"plan_id": plan_id, "status": "revision_requested"}


@router.post("/api/plans/{plan_id}/approve")
async def approve_plan(plan_id: str, user: dict = Depends(require_auth)):
    """Approve a plan and spawn an implementation session."""
    plan = await _db.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")

    # Guard: only pending plans can be approved
    if plan["status"] != "pending":
        raise HTTPException(
            status_code=409,
            detail=f"Plan is '{plan['status']}', only 'pending' plans can be approved",
        )

    task = await _db.get_task(plan["task_id"])
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    now = datetime.now(timezone.utc).isoformat()
    plan_type = plan.get("plan_type", "generic")

    # Mark plan as implementing immediately (prevents double-approve)
    await _db.update_plan(plan_id, status="implementing", reviewed_at=now)

    # Create implementation session (visible in Chat UI)
    import uuid
    impl_session_id = f"impl-{str(uuid.uuid4())[:8]}"
    await _engine.sessions.get_or_create(
        impl_session_id, title=f"Implement: {task['title']}", source="web",
    )
    await _db.update_plan(plan_id, impl_session_id=impl_session_id)

    # Update task status + note
    from nerve.agent.tools import task_update as task_update_tool
    await task_update_tool.handler({
        "task_id": plan["task_id"],
        "status": "in_progress",
        "note": f"Plan approved — implementation started (session: {impl_session_id})",
    })

    # Read task file content for the implementation prompt
    config = get_config()
    task_content = ""
    if task.get("file_path"):
        task_file = config.workspace / task["file_path"]
        if task_file.exists():
            task_content = task_file.read_text(encoding="utf-8")

    # Build implementation prompt — skill-aware
    if plan_type in ("skill-create", "skill-update"):
        prompt = (
            f"You are implementing an approved plan for a skill task.\n\n"
            f"## Task: {task['title']}\n\n"
            f"### Task Content\n{task_content}\n\n"
            f"## Approved Plan\n{plan['content']}\n\n"
            f"## Instructions\n"
        )
        if plan_type == "skill-create":
            prompt += (
                "The plan contains a skill specification. "
                "Use the `skill_create` tool to create the skill. "
                "Extract the name, description, and content from the plan. "
                "If the plan contains a full SKILL.md with frontmatter, parse out the name and description "
                "from the frontmatter and use the body as the content.\n"
            )
        else:
            prompt += (
                "The plan contains a skill revision. "
                "Use the `skill_update` tool to update the existing skill. "
                "Pass the skill ID (directory name) as the name parameter and the full SKILL.md content "
                "(frontmatter + body).\n"
            )
        prompt += (
            "\nAfter the skill is created/updated, mark the task as done using "
            "`task_done` with a note describing what was done.\n"
        )
    else:
        prompt = (
            f"You are implementing an approved plan for a task.\n\n"
            f"## Task: {task['title']}\n\n"
            f"### Task Content\n{task_content}\n\n"
            f"## Approved Plan\n{plan['content']}\n\n"
            f"## Instructions\n"
            f"Follow the plan step by step. You have full tool access.\n"
            f"After implementation, verify your changes work correctly.\n"
            f"If you encounter issues not covered by the plan, use your judgment or ask the user.\n"
        )

    # Spawn implementation in background with error handling
    import asyncio

    async def _run_impl():
        try:
            await _engine.run(
                session_id=impl_session_id, user_message=prompt, source="web",
            )
        except Exception:
            logger.exception("Implementation session %s failed", impl_session_id)
            try:
                await _db.update_plan(plan_id, status="failed")
            except Exception:
                logger.exception("Failed to mark plan %s as failed", plan_id)

    asyncio.create_task(_run_impl())

    return {"plan_id": plan_id, "impl_session_id": impl_session_id}


@router.get("/api/tasks/{task_id}/plans")
async def get_task_plans(task_id: str, user: dict = Depends(require_auth)):
    plans = await _db.get_plans_for_task(task_id)
    return {"plans": plans}


# --- Skills ---

def _require_skill_manager():
    if not _engine or not hasattr(_engine, '_skill_manager') or not _engine._skill_manager:
        raise HTTPException(status_code=503, detail="Skills system not available")
    return _engine._skill_manager


@router.get("/api/skills")
async def list_skills(user: dict = Depends(require_auth)):
    """List all skills with usage stats."""
    skills = await _db.get_all_skills_with_stats()
    return {"skills": skills}


@router.get("/api/skills/stats")
async def skill_stats(user: dict = Depends(require_auth)):
    """Aggregate usage stats across all skills."""
    stats = await _db.get_skill_stats()
    return {"stats": stats}


@router.post("/api/skills/sync")
async def sync_skills(user: dict = Depends(require_auth)):
    """Re-scan filesystem and sync skills to DB."""
    mgr = _require_skill_manager()
    skills = await mgr.discover()
    return {"synced": len(skills), "skills": [{"id": s.id, "name": s.name} for s in skills]}


@router.get("/api/skills/{skill_id}")
async def get_skill_detail(skill_id: str, user: dict = Depends(require_auth)):
    """Get full skill content + metadata + usage stats."""
    mgr = _require_skill_manager()
    skill = await mgr.get_skill(skill_id)
    if not skill:
        raise HTTPException(status_code=404, detail="Skill not found")

    db_row = await _db.get_skill_row(skill_id)
    stats = await _db.get_skill_stats(skill_id)
    usage = await _db.get_skill_usage(skill_id, limit=20)
    refs = await mgr.list_references(skill_id)

    return {
        "id": skill.id,
        "name": skill.name,
        "description": skill.description,
        "version": skill.version,
        "enabled": db_row.get("enabled", True) if db_row else True,
        "user_invocable": skill.user_invocable,
        "model_invocable": skill.model_invocable,
        "allowed_tools": skill.allowed_tools,
        "has_references": skill.has_references,
        "has_scripts": skill.has_scripts,
        "has_assets": skill.has_assets,
        "content": skill.content,
        "raw": skill.raw,
        "references": refs,
        "stats": stats[0] if stats else {"total_invocations": 0, "success_count": 0, "avg_duration_ms": None, "last_used": None},
        "recent_usage": usage,
        "created_at": db_row.get("created_at") if db_row else None,
        "updated_at": db_row.get("updated_at") if db_row else None,
    }


@router.post("/api/skills")
async def create_skill(req: SkillCreateRequest, user: dict = Depends(require_auth)):
    """Create a new skill."""
    mgr = _require_skill_manager()
    skill = await mgr.create_skill(
        name=req.name, description=req.description,
        content=req.content, version=req.version,
    )
    return {"id": skill.id, "name": skill.name, "created": True}


@router.put("/api/skills/{skill_id}")
async def update_skill(skill_id: str, req: SkillUpdateRequest, user: dict = Depends(require_auth)):
    """Update a skill's SKILL.md content."""
    mgr = _require_skill_manager()
    skill = await mgr.update_skill(skill_id, req.content)
    if not skill:
        raise HTTPException(status_code=404, detail="Skill not found")
    return {"id": skill.id, "name": skill.name, "updated": True}


@router.delete("/api/skills/{skill_id}")
async def delete_skill(skill_id: str, user: dict = Depends(require_auth)):
    """Delete a skill."""
    mgr = _require_skill_manager()
    await mgr.delete_skill(skill_id)
    return {"id": skill_id, "deleted": True}


@router.patch("/api/skills/{skill_id}/toggle")
async def toggle_skill(skill_id: str, req: SkillToggleRequest, user: dict = Depends(require_auth)):
    """Enable or disable a skill."""
    mgr = _require_skill_manager()
    success = await mgr.toggle_skill(skill_id, req.enabled)
    if not success:
        raise HTTPException(status_code=404, detail="Skill not found")
    return {"id": skill_id, "enabled": req.enabled}


@router.get("/api/skills/{skill_id}/usage")
async def get_skill_usage(skill_id: str, limit: int = 50, user: dict = Depends(require_auth)):
    """Get usage history for a skill."""
    usage = await _db.get_skill_usage(skill_id, limit=min(limit, 200))
    stats = await _db.get_skill_stats(skill_id)
    return {
        "usage": usage,
        "stats": stats[0] if stats else {"total_invocations": 0, "success_count": 0, "avg_duration_ms": None, "last_used": None},
    }


# --- MCP Servers ---

@router.get("/api/mcp-servers")
async def list_mcp_servers(user: dict = Depends(require_auth)):
    """List all MCP servers with aggregated usage stats."""
    servers = await _db.get_mcp_server_stats()
    return {"servers": servers}


@router.get("/api/mcp-servers/{server_name}")
async def get_mcp_server_detail(server_name: str, user: dict = Depends(require_auth)):
    """Get detailed info for a specific MCP server."""
    stats_list = await _db.get_mcp_server_stats()
    server = next((s for s in stats_list if s["name"] == server_name), None)
    if not server:
        raise HTTPException(status_code=404, detail="MCP server not found")

    tools = await _db.get_mcp_tool_breakdown(server_name)
    usage = await _db.get_mcp_server_usage(server_name, limit=30)

    return {**server, "tools": tools, "recent_usage": usage}


@router.get("/api/mcp-servers/{server_name}/usage")
async def get_mcp_server_usage(
    server_name: str, limit: int = 50, user: dict = Depends(require_auth),
):
    """Get usage history for an MCP server."""
    usage = await _db.get_mcp_server_usage(server_name, limit=min(limit, 200))
    return {"usage": usage}


@router.post("/api/mcp-servers/reload")
async def reload_mcp_servers(user: dict = Depends(require_auth)):
    """Re-read MCP server config from YAML files and refresh cache."""
    servers = await _engine.reload_mcp_config()
    stats = await _db.get_mcp_server_stats()
    return {"reloaded": len(servers), "servers": stats}


# --- Memory files ---

@router.get("/api/memory/files")
async def list_memory_files(user: dict = Depends(require_auth)):
    """List markdown files in workspace memory directory."""
    config = get_config()
    memory_dir = config.workspace / "memory"
    files = []
    if memory_dir.exists():
        for f in sorted(memory_dir.rglob("*.md")):
            rel = f.relative_to(config.workspace)
            files.append({
                "path": str(rel),
                "name": f.name,
                "size": f.stat().st_size,
                "modified": datetime.fromtimestamp(f.stat().st_mtime, timezone.utc).isoformat(),
            })

    # Also include root-level md files
    for f in sorted(config.workspace.glob("*.md")):
        files.append({
            "path": f.name,
            "name": f.name,
            "size": f.stat().st_size,
            "modified": datetime.fromtimestamp(f.stat().st_mtime, timezone.utc).isoformat(),
        })

    return {"files": files}


@router.get("/api/memory/file/{file_path:path}")
async def read_memory_file(file_path: str, user: dict = Depends(require_auth)):
    config = get_config()
    full_path = config.workspace / file_path
    if not full_path.exists() or not full_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    # Prevent path traversal
    try:
        full_path.resolve().relative_to(config.workspace.resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")
    content = full_path.read_text(encoding="utf-8")
    return {"path": file_path, "content": content}


class FileWriteRequest(BaseModel):
    content: str

@router.put("/api/memory/file/{file_path:path}")
async def write_memory_file(file_path: str, req: FileWriteRequest, user: dict = Depends(require_auth)):
    config = get_config()
    full_path = config.workspace / file_path
    try:
        full_path.resolve().relative_to(config.workspace.resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")
    full_path.parent.mkdir(parents=True, exist_ok=True)
    full_path.write_text(req.content, encoding="utf-8")
    return {"path": file_path, "saved": True}


# --- memU semantic memory ---

@router.get("/api/memory/memu")
async def get_memu_data(user: dict = Depends(require_auth)):
    """Get memU categories and items for the memory UI."""
    if not _engine or not hasattr(_engine, '_memory_bridge') or not _engine._memory_bridge or not _engine._memory_bridge.available:
        return {"available": False, "categories": [], "items": [], "resources": []}

    import sqlite3
    config = get_config()
    dsn = config.memory.sqlite_dsn
    # Extract file path from sqlite:///path
    db_path = dsn.replace("sqlite:///", "")

    try:
        db = sqlite3.connect(db_path)
        db.row_factory = sqlite3.Row

        categories = []
        for row in db.execute("SELECT id, name, description, summary FROM memu_memory_categories ORDER BY name"):
            categories.append({
                "id": row["id"],
                "name": row["name"],
                "description": row["description"],
                "summary": row["summary"],
            })

        items = []
        for row in db.execute("SELECT id, memory_type, summary, resource_id, created_at, happened_at FROM memu_memory_items ORDER BY created_at DESC"):
            items.append({
                "id": row["id"],
                "memory_type": row["memory_type"],
                "summary": row["summary"],
                "resource_id": row["resource_id"],
                "created_at": row["created_at"],
                "happened_at": row["happened_at"],
            })

        resources = []
        for row in db.execute("SELECT id, url, modality, caption, created_at FROM memu_resources ORDER BY created_at DESC"):
            resources.append({
                "id": row["id"],
                "url": row["url"],
                "modality": row["modality"],
                "caption": row["caption"],
                "created_at": row["created_at"],
            })

        # Category-item links
        cat_items: dict[str, list[str]] = {}
        for row in db.execute("SELECT category_id, item_id FROM memu_category_items"):
            cat_items.setdefault(row["category_id"], []).append(row["item_id"])

        db.close()

        return {
            "available": True,
            "categories": categories,
            "items": items,
            "resources": resources,
            "category_items": cat_items,
        }
    except Exception as e:
        logger.error("Failed to read memU data: %s", e)
        return {"available": False, "categories": [], "items": [], "resources": [], "error": str(e)}


class CategoryCreateRequest(BaseModel):
    name: str
    description: str = ""

class ItemUpdateRequest(BaseModel):
    content: str | None = None
    memory_type: str | None = None
    categories: list[str] | None = None


def _require_memu():
    if not _engine or not hasattr(_engine, '_memory_bridge') or not _engine._memory_bridge or not _engine._memory_bridge.available:
        raise HTTPException(status_code=503, detail="Memory service not available")
    return _engine._memory_bridge


@router.post("/api/memory/memu/categories")
async def create_memu_category(req: CategoryCreateRequest, user: dict = Depends(require_auth)):
    """Create a new memU memory category at runtime."""
    bridge = _require_memu()
    success = await bridge.create_category(req.name, req.description, source="web_ui")
    if not success:
        raise HTTPException(status_code=500, detail="Failed to create category")
    return {"name": req.name, "created": True}


@router.patch("/api/memory/memu/items/{item_id}")
async def update_memu_item(item_id: str, req: ItemUpdateRequest, user: dict = Depends(require_auth)):
    """Update a memU memory item's content, type, or categories."""
    bridge = _require_memu()
    if req.content is None and req.memory_type is None and req.categories is None:
        raise HTTPException(status_code=400, detail="Nothing to update")
    success = await bridge.update_item(
        memory_id=item_id,
        content=req.content,
        memory_type=req.memory_type,
        categories=req.categories,
        source="web_ui",
    )
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update item")
    return {"id": item_id, "updated": True}


@router.delete("/api/memory/memu/items/{item_id}")
async def delete_memu_item(item_id: str, user: dict = Depends(require_auth)):
    """Delete a memU memory item."""
    bridge = _require_memu()
    success = await bridge.delete_item(memory_id=item_id, source="web_ui")
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete item")
    return {"id": item_id, "deleted": True}


class CategoryUpdateRequest(BaseModel):
    summary: str | None = None
    description: str | None = None


@router.patch("/api/memory/memu/categories/{category_id}")
async def update_memu_category(category_id: str, req: CategoryUpdateRequest, user: dict = Depends(require_auth)):
    """Update a memU category's summary or description (re-embeds)."""
    bridge = _require_memu()
    if req.summary is None and req.description is None:
        raise HTTPException(status_code=400, detail="Nothing to update")
    success = await bridge.update_category(
        category_id=category_id, summary=req.summary, description=req.description,
        source="web_ui",
    )
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update category")
    return {"id": category_id, "updated": True}


@router.get("/api/memory/memu/audit")
async def get_memu_audit_log(
    action: str = "", target_type: str = "", limit: int = 100, offset: int = 0,
    user: dict = Depends(require_auth),
):
    """Paginated audit log for memU operations."""
    if not _db:
        raise HTTPException(status_code=503, detail="Database not available")
    logs = await _db.get_audit_logs(
        action=action or None, target_type=target_type or None,
        limit=min(limit, 500), offset=offset,
    )
    return {"logs": logs, "offset": offset, "limit": limit}


@router.get("/api/memory/memu/health")
async def memu_health(user: dict = Depends(require_auth)):
    """memU memory service health and metrics."""
    if not _engine or not _engine._memory_bridge:
        return {"service_available": False}
    return await _engine._memory_bridge.get_health()


# --- Diagnostics ---

@router.get("/api/diagnostics")
async def diagnostics(user: dict = Depends(require_auth)):
    """System health and status information."""
    import shutil

    config = get_config()

    # Memory usage
    try:
        import resource
        mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        mem_mb = 0

    # Disk usage
    disk = shutil.disk_usage(str(config.workspace))

    # Cron logs (last 10)
    cron_logs = await _db.get_cron_logs(limit=10) if _db else []

    # Source status — discover from registered runners + DB history
    sync_status = {}
    from nerve.gateway.server import _cron_service
    if _db:
        try:
            # Collect all known source names: registered runners + DB entries
            known_sources: set[str] = set()

            # From registered runners (includes sources that haven't run yet)
            if _cron_service and hasattr(_cron_service, "_source_runners"):
                for runner in _cron_service._source_runners:
                    known_sources.add(runner.source.source_name)

            # From DB (includes sources that ran before but may no longer be configured)
            import sqlite3
            conn = sqlite3.connect(str(_db.db_path))
            for row in conn.execute("SELECT DISTINCT source FROM sync_cursors"):
                known_sources.add(row[0])
            for row in conn.execute("SELECT DISTINCT source FROM source_run_log"):
                known_sources.add(row[0])
            conn.close()

            for source in sorted(known_sources):
                cursor = await _db.get_sync_cursor(source)
                last_run = await _db.get_last_source_run(source)
                sync_status[source] = {
                    "cursor": cursor,
                    "last_run": last_run.get("ran_at") if last_run else None,
                    "records_fetched": last_run.get("records_fetched", 0) if last_run else 0,
                    "records_processed": last_run.get("records_processed", 0) if last_run else 0,
                    "error": last_run.get("error") if last_run else None,
                }
        except Exception:
            pass

    # Memorization sweep stats (from server.py global)
    from nerve.gateway.server import _memorize_stats

    # Count sessions needing memorization
    pending_count = 0
    if _db:
        try:
            pending = await _db.get_sessions_needing_memorization()
            pending_count = len(pending)
        except Exception:
            pass

    # Task / FTS health
    tasks_health = {}
    if _db:
        try:
            import sqlite3 as _sq
            conn = _sq.connect(str(_db.db_path))
            task_count = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            fts_count = conn.execute("SELECT COUNT(*) FROM tasks_fts").fetchone()[0]
            active_count = conn.execute("SELECT COUNT(*) FROM tasks WHERE status != 'done'").fetchone()[0]
            done_count = conn.execute("SELECT COUNT(*) FROM tasks WHERE status = 'done'").fetchone()[0]
            conn.close()
            tasks_health = {
                "total": task_count,
                "active": active_count,
                "done": done_count,
                "fts_indexed": fts_count,
                "fts_ok": task_count == fts_count,
            }
        except Exception:
            pass

    return {
        "system": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "hostname": platform.node(),
            "memory_mb": round(mem_mb, 1),
            "disk_total_gb": round(disk.total / (1024**3), 1),
            "disk_free_gb": round(disk.free / (1024**3), 1),
        },
        "workspace": str(config.workspace),
        "sessions_count": len(await _engine.sessions.list_sessions()),
        "sync": sync_status,
        "recent_cron_logs": cron_logs,
        "tasks": tasks_health,
        "memorization": {
            **_memorize_stats,
            "sessions_pending": pending_count,
        },
    }


@router.post("/api/memorization/sweep")
async def trigger_memorization_sweep(user: dict = Depends(require_auth)):
    """Manually trigger a memorization sweep."""
    if not _engine:
        raise HTTPException(status_code=503, detail="Engine not available")

    from nerve.gateway.server import _memorize_stats
    from datetime import datetime, timezone

    result = await _engine.run_memorization_sweep()
    _memorize_stats["last_run_at"] = datetime.now(timezone.utc).isoformat()
    _memorize_stats["last_result"] = result
    _memorize_stats["total_runs"] += 1
    return result


@router.get("/api/cron/jobs")
async def list_cron_jobs(user: dict = Depends(require_auth)):
    """List all registered cron/source jobs with schedule and next run."""
    from nerve.gateway.server import _cron_service

    if not _cron_service:
        return {"jobs": []}

    jobs = _cron_service.list_jobs()
    return {"jobs": jobs}


@router.post("/api/cron/jobs/{job_id}/trigger")
async def trigger_cron_job(job_id: str, user: dict = Depends(require_auth)):
    """Manually trigger a specific cron job or source runner."""
    from nerve.gateway.server import _cron_service

    if not _cron_service:
        raise HTTPException(status_code=503, detail="Cron service not available")

    # Source runner
    runners = getattr(_cron_service, "_source_runners", [])
    runner = next((r for r in runners if r.job_id == job_id), None)
    if runner:
        await _cron_service._run_source_wrapper(runner)
        return {"job_id": job_id, "triggered": True}

    # Regular cron job
    try:
        await _cron_service.run_job(job_id)
        return {"job_id": job_id, "triggered": True}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/api/cron/jobs/{job_id}/rotate")
async def rotate_cron_session(job_id: str, user: dict = Depends(require_auth)):
    """Force-rotate a persistent cron session's context."""
    from nerve.gateway.server import _cron_service

    if not _cron_service:
        raise HTTPException(status_code=503, detail="Cron service not available")

    try:
        result = await _cron_service.rotate_session(job_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/api/cron/logs")
async def get_cron_logs(job_id: str = "", limit: int = 50, user: dict = Depends(require_auth)):
    logs = await _db.get_cron_logs(job_id=job_id or None, limit=limit)
    return {"logs": logs}


@router.post("/api/sources/{source_name}/sync")
async def trigger_single_source_sync(source_name: str, user: dict = Depends(require_auth)):
    """Manually trigger sync for a specific source."""
    from nerve.gateway.server import _cron_service

    if not _cron_service:
        raise HTTPException(status_code=503, detail="Cron service not available")

    runners = getattr(_cron_service, "_source_runners", [])
    runner = next((r for r in runners if r.source.source_name == source_name), None)
    if not runner:
        available = [r.source.source_name for r in runners]
        raise HTTPException(status_code=404, detail=f"Source not found: {source_name}. Available: {available}")

    result = await runner.run()

    # Log it
    if _db:
        await _db.log_source_run(
            source=source_name,
            records_fetched=result.records_processed + result.records_skipped,
            records_processed=result.records_processed,
            error=result.error,
            session_id=result.session_id,
        )

    return {
        "source": source_name,
        "records_processed": result.records_processed,
        "records_skipped": result.records_skipped,
        "actions_taken": result.actions_taken,
        "error": result.error,
        "session_id": result.session_id,
    }


@router.post("/api/sources/sync-all")
async def trigger_all_sources_sync(user: dict = Depends(require_auth)):
    """Manually trigger sync for all registered sources."""
    from nerve.gateway.server import _cron_service

    if not _cron_service:
        raise HTTPException(status_code=503, detail="Cron service not available")

    runners = getattr(_cron_service, "_source_runners", [])
    results = {}
    for runner in runners:
        name = runner.source.source_name
        result = await runner.run()
        if _db:
            await _db.log_source_run(
                source=name,
                records_fetched=result.records_processed + result.records_skipped,
                records_processed=result.records_processed,
                error=result.error,
                session_id=result.session_id,
            )
        results[name] = {
            "records_processed": result.records_processed,
            "records_skipped": result.records_skipped,
            "error": result.error,
        }

    return {"results": results}


# --- Source inbox ---

@router.get("/api/sources/messages")
async def list_source_messages(
    source: str = "", limit: int = 50, before: str = "",
    session: str = "",
    user: dict = Depends(require_auth),
):
    """Paginated list of source inbox messages, newest first."""
    if not _db:
        raise HTTPException(status_code=503, detail="Database not available")
    capped_limit = min(limit, 200)
    rows, has_more = await _db.list_source_messages(
        source=source or None,
        limit=capped_limit,
        before_ts=before or None,
        run_session_id=session or None,
    )
    return {"messages": rows, "has_more": has_more}


@router.get("/api/sources/messages/{source:path}/{msg_id}")
async def get_source_message(source: str, msg_id: str, user: dict = Depends(require_auth)):
    """Get a single source message with full content and processed_content."""
    if not _db:
        raise HTTPException(status_code=503, detail="Database not available")
    msg = await _db.get_source_message(source, msg_id)
    if not msg:
        raise HTTPException(status_code=404, detail="Message not found")
    return msg


@router.delete("/api/sources/messages")
async def purge_source_messages(source: str = "", user: dict = Depends(require_auth)):
    """Purge source messages. If source specified, only that source; otherwise all."""
    if not _db:
        raise HTTPException(status_code=503, detail="Database not available")
    deleted = await _db.delete_source_messages(source=source or None)
    return {"deleted": deleted}


@router.get("/api/sources/overview")
async def source_overview(user: dict = Depends(require_auth)):
    """Combined overview: message counts, storage, cursor status, 1h/24h stats."""
    if not _db:
        raise HTTPException(status_code=503, detail="Database not available")

    from nerve.gateway.server import _cron_service

    # Gather all data in parallel-ish calls
    counts = await _db.get_source_message_counts()
    storage = await _db.get_source_messages_storage()
    stats_1h = await _db.get_source_stats(hours=1)
    stats_24h = await _db.get_source_stats(hours=24)

    # Collect all known source names
    known_sources: set[str] = set(counts.keys()) | set(storage.keys()) | set(stats_1h.keys()) | set(stats_24h.keys())

    # From registered runners
    if _cron_service and hasattr(_cron_service, "_source_runners"):
        for runner in _cron_service._source_runners:
            known_sources.add(runner.source.source_name)

    # From DB cursors
    import sqlite3
    try:
        conn = sqlite3.connect(str(_db.db_path))
        for row in conn.execute("SELECT DISTINCT source FROM sync_cursors"):
            known_sources.add(row[0])
        conn.close()
    except Exception:
        pass

    sources = {}
    total_messages = 0
    total_storage = 0

    for src in sorted(known_sources):
        cursor = await _db.get_sync_cursor(src)
        last_run = await _db.get_last_source_run(src)
        msg_count = counts.get(src, 0)
        src_storage = storage.get(src, {})
        total_messages += msg_count
        total_storage += src_storage.get("bytes", 0)

        empty_stats = {"runs": 0, "fetched": 0, "processed": 0, "errors": 0, "last_run_at": None}
        sources[src] = {
            "message_count": msg_count,
            "storage_bytes": src_storage.get("bytes", 0),
            "cursor": cursor,
            "last_run_at": last_run.get("ran_at") if last_run else None,
            "last_error": last_run.get("error") if last_run else None,
            "stats_1h": stats_1h.get(src, empty_stats),
            "stats_24h": stats_24h.get(src, empty_stats),
        }

    return {
        "sources": sources,
        "total_messages": total_messages,
        "total_storage_bytes": total_storage,
    }


@router.get("/api/sources/runs")
async def list_source_runs(
    source: str = "", limit: int = 50,
    user: dict = Depends(require_auth),
):
    """Source run history with session links."""
    if not _db:
        raise HTTPException(status_code=503, detail="Database not available")
    runs = await _db.get_source_run_log(
        source=source or None,
        limit=min(limit, 200),
    )
    return {"runs": runs}


@router.get("/api/sources/stats")
async def source_stats(hours: int = 24, user: dict = Depends(require_auth)):
    """Per-source aggregate stats for the last N hours."""
    if not _db:
        raise HTTPException(status_code=503, detail="Database not available")
    stats = await _db.get_source_stats(hours=min(hours, 168))  # Cap at 7 days
    return {"stats": stats, "hours": hours}


@router.get("/api/sources/consumers")
async def get_consumer_cursors(consumer: str | None = None, user: dict = Depends(require_auth)):
    """List active consumer cursors with unread counts."""
    if not _db:
        raise HTTPException(status_code=503, detail="Database not available")
    cursors = await _db.list_consumer_cursors(consumer=consumer)
    return {"consumers": cursors}


# --- Notifications ---

class NotificationAnswerRequest(BaseModel):
    answer: str


@router.get("/api/notifications")
async def list_notifications(
    status: str = "",
    type: str = "",
    session_id: str = "",
    limit: int = 50,
    user: dict = Depends(require_auth),
):
    if not _db:
        raise HTTPException(status_code=503, detail="Database not available")
    notifications = await _db.list_notifications(
        status=status or None,
        type=type or None,
        session_id=session_id or None,
        limit=min(limit, 200),
    )
    pending_count = await _db.count_pending_notifications()
    return {"notifications": notifications, "pending_count": pending_count}


@router.get("/api/notifications/{notification_id}")
async def get_notification(notification_id: str, user: dict = Depends(require_auth)):
    if not _db:
        raise HTTPException(status_code=503, detail="Database not available")
    notif = await _db.get_notification(notification_id)
    if not notif:
        raise HTTPException(status_code=404, detail="Notification not found")
    return notif


@router.post("/api/notifications/{notification_id}/answer")
async def answer_notification(
    notification_id: str,
    req: NotificationAnswerRequest,
    user: dict = Depends(require_auth),
):
    if not _notification_service:
        raise HTTPException(status_code=503, detail="Notification service not available")
    success = await _notification_service.handle_answer(
        notification_id=notification_id,
        answer=req.answer,
        answered_by="web",
    )
    if not success:
        raise HTTPException(status_code=409, detail="Notification already answered or not found")
    return {"notification_id": notification_id, "answered": True}


@router.post("/api/notifications/{notification_id}/dismiss")
async def dismiss_notification(
    notification_id: str,
    user: dict = Depends(require_auth),
):
    if not _notification_service:
        raise HTTPException(status_code=503, detail="Notification service not available")
    success = await _notification_service.handle_dismiss(notification_id)
    if not success:
        raise HTTPException(status_code=409, detail="Notification not pending")
    return {"notification_id": notification_id, "dismissed": True}


@router.post("/api/notifications/dismiss-all")
async def dismiss_all_notifications(user: dict = Depends(require_auth)):
    if not _db:
        raise HTTPException(status_code=503, detail="Database not available")
    count = await _db.dismiss_all_notifications()
    return {"dismissed": count}
