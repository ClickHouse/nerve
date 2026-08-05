"""Task routes."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.agent.streaming import emit_task_event
from nerve.config import ensure_path_not_tracked_config, get_config
from nerve.db.task_statuses import STATUS_NAME_RE, TERMINAL_STATUS, normalize_color
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
    tags: str = ""
    status: str = ""
    confirm_duplicate: bool = False


class TaskUpdateRequest(BaseModel):
    """Partial task edit.

    ``deadline`` and ``tags`` are ``None``-by-default rather than
    ``""``-by-default: the handler reads them by *presence*, so omitting
    the key leaves the field alone while sending an empty string clears
    it. The remaining fields keep truthiness semantics — there is no
    meaningful "clear" for a title, a status, or an appended note.
    """

    status: str = ""
    note: str = ""
    content: str = ""
    title: str = ""
    deadline: str | None = None
    tags: str | None = None


class TaskMoveRequest(BaseModel):
    """Place a task in a lane, relative to its new neighbours.

    ``before_id`` is the card that ends up directly above the moved task,
    ``after_id`` the one directly below; omit both to append to the lane.
    Ranks are resolved server-side from these anchors — see
    :meth:`nerve.db.tasks.TaskStore.move_task`.
    """

    status: str | None = None
    before_id: str | None = None
    after_id: str | None = None


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


_ALLOWED_SORTS = {"deadline", "updated_at", "created_at", "position"}

# Per-lane page size for the board. Done accumulates without bound and is
# collapsed by default in the UI, so it gets a tighter cap — every lane
# reports its true ``total`` so the column can offer "+N more".
_BOARD_LANE_LIMIT = 100
_BOARD_DONE_LANE_LIMIT = 25


@router.get("/api/tasks")
async def list_tasks(
    status: str = "",
    tag: str = "",
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
    tag_filter = tag.strip().lower() or None
    tasks = await deps.db.list_tasks(
        status=status_filter, tag=tag_filter, sort=sort, limit=limit, offset=offset,
    )
    total = await deps.db.count_tasks(status=status_filter, tag=tag_filter)
    return {"tasks": tasks, "total": total, "limit": limit, "offset": offset}


@router.get("/api/tasks/search")
async def search_tasks(q: str, status: str = "", user: dict = Depends(require_auth)):
    deps = get_deps()
    # Search is relevance-ranked (BM25); pagination would fight the ranking,
    # so we return up to 100 hits and let the UI hide pagination when active.
    tasks = await deps.db.search_tasks(query=q, status=status or None, limit=100)
    return {"tasks": tasks, "total": len(tasks), "limit": len(tasks), "offset": 0}


# ── Board ────────────────────────────────────────────────────────────────
#
# NOTE: /board and /tags must stay above /{task_id} — FastAPI matches in
# declaration order, so a dynamic segment declared first would swallow both.


@router.get("/api/tasks/board")
async def task_board(
    limit: int = _BOARD_LANE_LIMIT,
    tag: str = "",
    user: dict = Depends(require_auth),
):
    """Every lane in one round trip: statuses + their ordered tasks.

    The board needs one page of each configured status plus that status's
    full count. Fetching it here rather than as N parallel filtered calls
    keeps the lanes consistent with each other (a task that moves mid-load
    can't appear in two lanes or neither) and keeps the client from having
    to know the status list before it can start fetching.
    """
    deps = get_deps()
    limit = max(1, min(limit, 200))
    tag_filter = tag.strip().lower() or None

    statuses = await deps.db.list_task_statuses()
    # One GROUP BY covers every lane's total; only a tag filter (which the
    # grouped count can't express) needs the per-lane fallback.
    counts = {} if tag_filter else await deps.db.count_tasks_by_status()

    lanes = []
    for status_def in statuses:
        name = status_def["name"]
        lane_limit = (
            min(limit, _BOARD_DONE_LANE_LIMIT) if name == TERMINAL_STATUS else limit
        )
        tasks = await deps.db.list_tasks(
            status=name, tag=tag_filter, sort="position", limit=lane_limit,
        )
        total = (
            await deps.db.count_tasks(status=name, tag=tag_filter)
            if tag_filter
            else counts.get(name, 0)
        )
        lanes.append({"status": name, "total": total, "tasks": tasks})

    return {"statuses": statuses, "lanes": lanes}


@router.get("/api/tasks/tags")
async def list_task_tags(
    include_done: bool = False, user: dict = Depends(require_auth),
):
    """Tag facets for the board filter bar, most-used first."""
    deps = get_deps()
    return {"tags": await deps.db.distinct_task_tags(include_done=include_done)}


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
            "tags": req.tags,
            "status": req.status,
            "confirm_duplicate": req.confirm_duplicate,
        },
    )

    # The handler reports its outcome as data as well as prose; branch on
    # that rather than on the wording of the message.
    outcome = result.structured or {}
    message = result.text_content
    if not outcome.get("created"):
        # The duplicate guard is a refusal, not a success — say so with a
        # status code instead of a 200 carrying an apology. The body keeps
        # the near-matches so the client can offer "create anyway".
        #
        # An unknown status is the other way this refuses, and it is a bad
        # field rather than a collision: offering "create anyway" cannot fix
        # it. Give it the 422 that /move already returns for the same
        # mistake, so a client can tell the two apart by status code.
        reason = outcome.get("reason", "refused")
        raise HTTPException(
            status_code=422 if reason == "invalid_status" else 409,
            detail={
                "message": message,
                "reason": reason,
                "duplicates": outcome.get("duplicates", []),
            },
        )

    deps = get_deps()
    task = await deps.db.get_task(outcome["task_id"])
    return {"task": task, "message": message}


@router.post("/api/tasks/{task_id}/move")
async def move_task(
    task_id: str, req: TaskMoveRequest, user: dict = Depends(require_auth),
):
    """Reorder a task within a lane, or move it to another one."""
    deps = get_deps()
    task = await deps.db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    target = (req.status or "").strip().lower() or task["status"]
    if target not in await deps.db.task_status_names():
        raise HTTPException(status_code=422, detail=f"Unknown status: '{target}'")

    # A lane change is more than a column write — `done` owns its own
    # directory on disk. Hand the status flip to task_update, which routes
    # to task_done / task_reopen so the markdown travels with the status,
    # then stamp the rank on top of whatever it left behind.
    if target != task["status"]:
        result = await get_tool_registry().invoke(
            "task_update",
            build_route_tool_context(),
            {"task_id": task_id, "status": target},
        )
        if result.is_error:
            raise HTTPException(status_code=409, detail=result.text_content)

    moved = await deps.db.move_task(
        task_id, status=target, before_id=req.before_id, after_id=req.after_id,
    )
    if not moved:
        raise HTTPException(status_code=404, detail="Task not found")

    await emit_task_event(moved, "moved")
    return {"task": moved}


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
        # file_path comes from the DB row, which the task indexer only ever fills
        # from a glob of tasks/ — but it is still a stored path being joined to
        # the workspace and written to, so it gets the same guard as every other
        # caller-influenced write.
        ensure_path_not_tracked_config(file_path, "write")
        if file_path.exists():
            await asyncio.to_thread(
                file_path.write_text, req.content, encoding="utf-8",
            )
            # Re-sync title from markdown to SQLite
            from nerve.tasks.models import (
                parse_task_frontmatter,
                parse_task_title,
                parse_tags_string,
                tags_to_string,
            )
            new_title = parse_task_title(req.content)
            fields = parse_task_frontmatter(req.content)
            # Only the columns the saved markdown carries. The rest keep their
            # stored values, so nothing is read back off ``task`` here.
            edits: dict = {}
            if fields.get("deadline"):
                edits["deadline"] = fields["deadline"]
            if fields.get("tags"):
                edits["tags"] = tags_to_string(parse_tags_string(fields["tags"]))
            await deps.db.upsert_task(
                task_id=task_id,
                file_path=task["file_path"],
                title=new_title,
                status=req.status or task["status"],
                content=req.content,
                **edits,
            )

    # Update status/note/deadline/tags/title via the unified handler (may
    # move the file for "done" — the handler routes to task_done in that
    # case, and to task_reopen on the way back out, so the FTS index and
    # the file's directory stay consistent).
    #
    # Fields are added by presence, not truthiness: the handler reads
    # deadline/tags that way, so forwarding an unset field as "" would turn
    # "don't touch this" into "clear this".
    payload: dict = {"task_id": task_id}
    if req.status:
        payload["status"] = req.status
    if req.note:
        payload["note"] = req.note
    if req.title:
        payload["title"] = req.title
    if req.deadline is not None:
        payload["deadline"] = req.deadline
    if req.tags is not None:
        payload["tags"] = req.tags

    if len(payload) > 1:
        result = await get_tool_registry().invoke(
            "task_update", build_route_tool_context(), payload,
        )
        # Previously swallowed: an unknown status returned {"updated": true}
        # while nothing had changed. Surface the handler's own message.
        if result.is_error:
            raise HTTPException(status_code=400, detail=result.text_content)

    updated = await deps.db.get_task(task_id)
    await emit_task_event(updated, "updated")
    # ``task_id``/``updated`` are kept for existing callers; ``task`` is the
    # full post-write row so an optimistic client can reconcile without a
    # follow-up GET.
    return {"task": updated, "task_id": task_id, "updated": True}


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
