"""Task routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.config import get_config
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

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


@router.get("/api/tasks")
async def list_tasks(status: str = "", user: dict = Depends(require_auth)):
    deps = get_deps()
    tasks = await deps.db.list_tasks(status=status or None)
    return {"tasks": tasks}


@router.get("/api/tasks/search")
async def search_tasks(q: str, status: str = "", user: dict = Depends(require_auth)):
    deps = get_deps()
    tasks = await deps.db.search_tasks(query=q, status=status or None)
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
    deps = get_deps()
    task = await deps.db.get_task(task_id)
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
            file_path.write_text(req.content, encoding="utf-8")
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

    # Update status/note/deadline/title via agent tool (may move file for "done")
    if req.status or req.note or req.deadline or req.title:
        from nerve.agent.tools import task_update
        await task_update.handler({
            "task_id": task_id,
            "status": req.status,
            "note": req.note,
            "deadline": req.deadline,
            "title": req.title,
        })

    return {"task_id": task_id, "updated": True}
