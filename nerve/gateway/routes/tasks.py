"""Task routes."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.config import get_config
from nerve.db.task_statuses import STATUS_NAME_RE, normalize_color
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import (
    build_route_tool_context,
    get_deps,
    get_tool_registry,
)

router = APIRouter()


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
    title: str = ""


class TaskStatusCreateRequest(BaseModel):
    name: str
    label: str = ""
    color: str = ""
    description: str = ""


class TaskStatusUpdateRequest(BaseModel):
    label: str | None = None
    color: str | None = None
    description: str | None = None
    sort_order: int | None = None


_ALLOWED_SORTS = {"deadline", "updated_at", "created_at"}


@router.get("/api/tasks")
async def list_tasks(
    status: str = "",
    sort: str = "deadline",
    limit: int = 50,
    offset: int = 0,
    user: dict = Depends(require_auth),
):
    deps = get_deps()
    # Clamp inputs to sensible bounds; silently fall back on unknown sorts.
    limit = max(1, min(limit, 200))
    offset = max(0, offset)
    if sort not in _ALLOWED_SORTS:
        sort = "deadline"

    status_filter = status or None
    tasks = await deps.db.list_tasks(
        status=status_filter, sort=sort, limit=limit, offset=offset,
    )
    total = await deps.db.count_tasks(status=status_filter)
    return {"tasks": tasks, "total": total, "limit": limit, "offset": offset}


@router.get("/api/tasks/search")
async def search_tasks(q: str, status: str = "", user: dict = Depends(require_auth)):
    deps = get_deps()
    # Search is relevance-ranked (BM25); pagination would fight the ranking,
    # so we return up to 100 hits and let the UI hide pagination when active.
    tasks = await deps.db.search_tasks(query=q, status=status or None, limit=100)
    return {"tasks": tasks, "total": len(tasks), "limit": len(tasks), "offset": 0}


@router.post("/api/tasks")
async def create_task(req: TaskCreateRequest, user: dict = Depends(require_auth)):
    # Route into the unified handler surface via the live registry — no
    # behavior duplication between the REST and MCP paths.
    result = await get_tool_registry().invoke(
        "task_create",
        build_route_tool_context(),
        {
            "title": req.title,
            "content": req.content,
            "source": req.source,
            "source_url": req.source_url,
            "deadline": req.deadline,
        },
    )
    return result.to_dict()


@router.get("/api/tasks/{task_id}")
async def get_task(task_id: str, user: dict = Depends(require_auth)):
    deps = get_deps()
    task = await deps.db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    # Include markdown file content
    config = get_config()
    file_path = config.workspace / task["file_path"]
    content = ""
    if file_path.exists():
        content = await asyncio.to_thread(file_path.read_text, encoding="utf-8")
    return {**dict(task), "content": content}


@router.patch("/api/tasks/{task_id}")
async def update_task(task_id: str, req: TaskUpdateRequest, user: dict = Depends(require_auth)):
    deps = get_deps()
    task = await deps.db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    # Write content FIRST — before status changes that may move/delete the file
    # (task_done moves the file to done/ and deletes the source)
    if req.content:
        config = get_config()
        file_path = config.workspace / task["file_path"]
        if file_path.exists():
            await asyncio.to_thread(
                file_path.write_text, req.content, encoding="utf-8",
            )
            # Re-sync title from markdown to SQLite
            from nerve.tasks.models import parse_task_title, parse_task_frontmatter
            new_title = parse_task_title(req.content)
            fields = parse_task_frontmatter(req.content)
            await deps.db.upsert_task(
                task_id=task_id,
                file_path=task["file_path"],
                title=new_title,
                status=req.status or task["status"],
                source=task.get("source"),
                source_url=task.get("source_url"),
                deadline=fields.get("deadline") or task.get("deadline"),
                tags=fields.get("tags") or task.get("tags", ""),
                content=req.content,
            )

    # Update status/note/deadline/title via the unified handler (may
    # move the file for "done" — the handler routes to task_done in that
    # case so the FTS index stays consistent).
    if req.status or req.note or req.deadline or req.title:
        await get_tool_registry().invoke(
            "task_update",
            build_route_tool_context(),
            {
                "task_id": task_id,
                "status": req.status,
                "note": req.note,
                "deadline": req.deadline,
                "title": req.title,
            },
        )

    return {"task_id": task_id, "updated": True}


# ── Configurable task statuses ───────────────────────────────────────────


@router.get("/api/task-statuses")
async def list_task_statuses(user: dict = Depends(require_auth)):
    deps = get_deps()
    return {"statuses": await deps.db.list_task_statuses()}


@router.post("/api/task-statuses")
async def create_task_status(
    req: TaskStatusCreateRequest, user: dict = Depends(require_auth),
):
    deps = get_deps()
    name = (req.name or "").strip().lower()
    if not STATUS_NAME_RE.match(name):
        raise HTTPException(
            status_code=422,
            detail="Status name must be lowercase letters, digits, and "
                   "underscores, starting with a letter or digit.",
        )
    if await deps.db.get_task_status_def(name):
        raise HTTPException(status_code=409, detail=f"Status '{name}' already exists")

    label = (req.label or "").strip() or name.replace("_", " ").title()
    color = normalize_color((req.color or "").strip())
    created = await deps.db.create_task_status(
        name=name, label=label, color=color,
        description=(req.description or "").strip(),
    )
    return created


@router.patch("/api/task-statuses/{name}")
async def update_task_status(
    name: str, req: TaskStatusUpdateRequest, user: dict = Depends(require_auth),
):
    deps = get_deps()
    existing = await deps.db.get_task_status_def(name)
    if not existing:
        raise HTTPException(status_code=404, detail="Status not found")

    await deps.db.update_task_status_def(
        name,
        label=req.label.strip() if req.label is not None else None,
        color=req.color if req.color is not None else None,
        description=req.description if req.description is not None else None,
        sort_order=req.sort_order,
    )
    return await deps.db.get_task_status_def(name)


@router.delete("/api/task-statuses/{name}")
async def delete_task_status(name: str, user: dict = Depends(require_auth)):
    deps = get_deps()
    existing = await deps.db.get_task_status_def(name)
    if not existing:
        raise HTTPException(status_code=404, detail="Status not found")
    if existing.get("is_system"):
        raise HTTPException(
            status_code=400,
            detail=f"'{name}' is a protected status and cannot be deleted",
        )
    in_use = await deps.db.count_tasks_with_status(name)
    if in_use > 0:
        raise HTTPException(
            status_code=409,
            detail=f"{in_use} task(s) use the '{name}' status. "
                   "Reassign them before deleting it.",
        )
    await deps.db.delete_task_status_def(name)
    return {"name": name, "deleted": True}
