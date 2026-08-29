"""Task tool handlers — task_search, task_create, task_list, task_update,
task_read, task_write, task_done.

Handlers are pure functions of ``(ToolContext, args)`` and return
:class:`ToolResult`. They read only from ``ctx`` for their collaborator
references; the only module-level state retained is ``_tasks_read``,
which is intentionally process-wide (it gates ``task_write`` against
arbitrary overwrites and operates as a read-before-write guard across
all sessions).
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    TASK_CREATE_SCHEMA,
    TASK_DONE_SCHEMA,
    TASK_LIST_SCHEMA,
    TASK_READ_SCHEMA,
    TASK_SEARCH_SCHEMA,
    TASK_STATUS_CREATE_SCHEMA,
    TASK_STATUS_LIST_SCHEMA,
    TASK_UPDATE_SCHEMA,
    TASK_WRITE_SCHEMA,
)
from nerve.config import ensure_path_not_tracked_config
from nerve.db.task_statuses import (
    DEFAULT_STATUS,
    STATUS_NAME_RE,
    TERMINAL_STATUS,
    normalize_color,
    random_status_color,
)
from nerve.tasks.files import move_task_file

logger = logging.getLogger(__name__)


async def _format_status_reminder(ctx: ToolContext, header: str) -> str:
    """Build a reminder listing every configured status + its description."""
    statuses = await ctx.db.list_task_statuses() if ctx.db else []
    lines = [header, "", "Available statuses:"]
    for s in statuses:
        desc = (s.get("description") or "").strip() or "(no description)"
        lines.append(f"  - {s['name']}: {desc}")
    lines.append("")
    lines.append(
        "Pick one of the statuses above. Only create a new status "
        "(task_status_create) when configuring Nerve or when explicitly "
        "asked to add one — not for routine task management."
    )
    return "\n".join(lines)


async def _validate_status(ctx: ToolContext, status: str) -> str | None:
    """Return a reminder message if ``status`` is not configured, else None.

    With no DB available (tests/ad-hoc) validation is skipped.
    """
    if not ctx.db:
        return None
    names = await ctx.db.task_status_names()
    if status in names:
        return None
    return await _format_status_reminder(
        ctx, f"Invalid task status: '{status}'.",
    )


# Process-wide read-before-write guard. Tracks task IDs that have been
# read (via task_read) or created (via task_create) in this process
# lifetime. task_write refuses to overwrite unless the task is in this
# set. Kept process-wide rather than per-session so a one-shot tool flow
# that reads in one session and writes in another isn't blocked.
_tasks_read: set[str] = set()


def _make_task_id(title: str, ctx: ToolContext) -> str:
    """Generate a base task ID from date + slugified title.

    The slug is a 40-character truncation, so this is a *base* — two titles
    that share a long prefix, or differ only past the cut, produce the same
    string on the same day. :func:`_claim_task_id` resolves that.
    """
    tz: timezone | ZoneInfo
    if ctx.config is not None:
        try:
            tz = ZoneInfo(ctx.config.timezone)
        except Exception:
            tz = timezone.utc
    else:
        # Fallback for callers without a config (tests, ad-hoc).
        from nerve.config import get_config
        try:
            tz = ZoneInfo(get_config().timezone)
        except Exception:
            tz = timezone.utc
    date_prefix = datetime.now(tz).strftime("%Y-%m-%d")
    slug = title.lower().replace(" ", "-")[:40]
    slug = "".join(c for c in slug if c.isalnum() or c == "-")
    return f"{date_prefix}-{slug}"


def _task_dir(ctx: ToolContext) -> Path:
    """Resolve and ensure the active-task directory."""
    assert ctx.workspace is not None, "ToolContext.workspace is required"
    d = ctx.workspace / "memory" / "tasks" / "active"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _done_dir(ctx: ToolContext) -> Path:
    """Resolve and ensure the done-task directory."""
    assert ctx.workspace is not None, "ToolContext.workspace is required"
    d = ctx.workspace / "memory" / "tasks" / "done"
    d.mkdir(parents=True, exist_ok=True)
    return d


async def _emit_task_event(ctx: ToolContext, task_id: str, event: str) -> None:
    """Push the post-mutation row to every connected task board.

    Re-reads the row rather than assembling a dict from local variables so
    the broadcast always carries what actually landed in the database —
    including the columns the handler never touched (``position``, the
    refreshed ``updated_at``).
    """
    if not ctx.db:
        return
    from nerve.agent.streaming import emit_task_event

    await emit_task_event(await ctx.db.get_task(task_id), event)


# How many suffixed IDs a single base may spawn before task_create gives up.
# A day that legitimately needs a hundred tasks off one 40-character prefix
# is a title problem, and refusing beats generating ever-longer IDs.
_MAX_TASK_ID_SUFFIX = 99


def _write_new_file(path: Path, content: str) -> None:
    """Write ``content`` to ``path``, refusing to touch an existing file.

    Mode ``x`` is the point: the existence check and the create are one
    syscall, so two sessions racing for the same ID cannot both win it.
    Blocking. Call it through ``asyncio.to_thread``.
    """
    with path.open("x", encoding="utf-8") as fh:
        fh.write(content)


async def _claim_task_id(
    ctx: ToolContext, base_id: str, body: str,
) -> tuple[str, Path] | None:
    """Write ``body`` under the first free ID at or after ``base_id``.

    Returns the claimed ID and its path, or ``None`` when the base and every
    suffix up to :data:`_MAX_TASK_ID_SUFFIX` are taken.

    An ID is free only when no row holds it *and* neither active/ nor done/
    holds its file. Both halves matter. The row check keeps a new task from
    inheriting an existing task's history, since ``upsert_task`` would
    otherwise rewrite that row's title, status and file_path in place. The
    file checks hold the same line when the index and the tree disagree —
    and they cover done/, whose IDs stay reserved so a completed task can
    never be resurrected as a new pending one.
    """
    active_dir, done_dir = _task_dir(ctx), _done_dir(ctx)
    for n in range(1, _MAX_TASK_ID_SUFFIX + 1):
        candidate = base_id if n == 1 else f"{base_id}-{n}"
        if ctx.db and await ctx.db.get_task(candidate) is not None:
            continue
        if (done_dir / f"{candidate}.md").exists():
            continue
        path = active_dir / f"{candidate}.md"
        try:
            await asyncio.to_thread(_write_new_file, path, body)
        except FileExistsError:
            # Either an untracked leftover or a racing session got there
            # first. Both mean the ID is spoken for.
            continue
        return candidate, path
    return None


async def task_search_handler(ctx: ToolContext, args: dict) -> ToolResult:
    query = args["query"]
    raw_status = (args.get("status", "") or "").strip().lower()

    if raw_status in ("", "open", "active"):
        status = None  # all non-done
    elif raw_status in ("all", "any"):
        status = "all"
    else:
        status = raw_status  # specific: pending, in_progress, done, deferred

    tag = (args.get("tag", "") or "").strip().lower()

    if ctx.db:
        tasks = await ctx.db.search_tasks(query=query, status=status, tag=tag or None)
    else:
        tasks = []

    if not tasks:
        return ToolResult.text(f"No tasks matching '{query}'.")

    lines = []
    for t in tasks:
        tags_str = f" [{t['tags']}]" if t.get("tags") else ""
        deadline_str = f" (due: {t['deadline']})" if t.get("deadline") else ""
        lines.append(f"- [{t['status']}]{tags_str} {t['title']}{deadline_str} — {t['id']}")

    return ToolResult.text(
        f"Found {len(tasks)} task(s) matching '{query}':\n" + "\n".join(lines)
    )


async def _find_duplicate_tasks(ctx: ToolContext, title: str, source_url: str = "") -> list[dict]:
    """Check for existing tasks — source_url exact match first, fuzzy FTS fallback.

    Mirror of the legacy helper: prefer an exact ``source_url`` match
    (most reliable for source-generated tasks), then fall back to OR-based
    FTS with a relevance threshold that filters out weak shared-word hits.
    """
    if not ctx.db:
        return []
    if source_url:
        url_matches = await ctx.db.find_tasks_by_source_url(source_url, limit=10)
        if url_matches:
            return url_matches
    return await ctx.db.search_tasks_similar(
        query=title, limit=10, rank_threshold=-5.0,
    )


async def task_create_handler(ctx: ToolContext, args: dict) -> ToolResult:
    from nerve.tasks.models import parse_tags_string, tags_to_string

    title = args["title"]
    content = args.get("content", "")
    source = args.get("source", "manual")
    source_url = args.get("source_url", "")
    deadline = args.get("deadline", "")
    raw_tags = args.get("tags", "")
    tags = parse_tags_string(raw_tags)
    confirm = args.get("confirm_duplicate", False)
    status = (args.get("status", "") or "").strip().lower() or DEFAULT_STATUS

    # Reject unknown statuses up front with the list of valid options.
    err = await _validate_status(ctx, status)
    if err:
        return ToolResult.text(
            err, structured={"created": False, "reason": "invalid_status"},
        )

    # Duplicate check (skip if explicitly confirmed)
    if not confirm:
        dupes = await _find_duplicate_tasks(ctx, title, source_url=source_url)
        if dupes:
            lines = [f"⚠️ Found {len(dupes)} potentially similar task(s):"]
            for t in dupes:
                deadline_str = f" (due: {t['deadline']})" if t.get("deadline") else ""
                lines.append(f"  - [{t['status']}] {t['title']}{deadline_str} — {t['id']}")
            lines.append("")
            lines.append("Task NOT created. To create anyway, call task_create again with confirm_duplicate=true.")
            return ToolResult.text(
                "\n".join(lines),
                structured={
                    "created": False,
                    "reason": "duplicate",
                    "duplicates": [
                        {"id": t["id"], "title": t["title"], "status": t["status"]}
                        for t in dupes
                    ],
                },
            )

    md_parts = [f"# {title}\n"]
    if source_url:
        md_parts.append(f"**Source:** {source_url}")
    if deadline:
        md_parts.append(f"**Deadline:** {deadline}")
    if tags:
        md_parts.append(f"**Tags:** {', '.join(tags)}")
    md_parts.append(f"\n{content}\n")
    md_parts.append("\n## Updates\n")
    md_parts.append(f"- {datetime.now(timezone.utc).strftime('%Y-%m-%d')}: Created")

    # Create is create. The ID is derived from a truncated title, so it
    # collides on its own, and writing through a collision destroyed the
    # older task: its file was truncated to the new body and its row was
    # rewritten with the new title, status and content. A taken ID now
    # moves the new task to the next suffix instead.
    base_id = _make_task_id(title, ctx)
    claimed = await _claim_task_id(ctx, base_id, "\n".join(md_parts))
    if claimed is None:
        return ToolResult.text(
            f"Could not create task: the ID '{base_id}' and every suffix "
            f"through '-{_MAX_TASK_ID_SUFFIX}' are already taken. Use a "
            "title that differs within its first 40 characters.",
            is_error=True,
        )
    task_id, file_path = claimed

    if ctx.db:
        rel_path = str(file_path.relative_to(ctx.workspace)) if ctx.workspace else str(file_path)
        try:
            await ctx.db.upsert_task(
                task_id=task_id,
                file_path=rel_path,
                title=title,
                status=status,
                source=source,
                source_url=source_url or None,
                deadline=deadline or None,
                tags=tags_to_string(tags),
                content=content,
                actor=ctx.session_id,
            )
        except Exception:
            # A claimed ID is now sticky, so a half-done create would burn it:
            # the retry would take the next suffix and leave this file behind
            # forever. The file was created exclusively by this call, so
            # dropping it is safe and leaves the retry the ID it started with.
            await asyncio.to_thread(file_path.unlink, missing_ok=True)
            raise

    _tasks_read.add(task_id)
    await _emit_task_event(ctx, task_id, "created")

    # Creating directly in the terminal status: route through task_done so
    # the file is moved into done/ and stays consistent with the done-flow.
    if status == TERMINAL_STATUS and ctx.db:
        await task_done_handler(ctx, {"task_id": task_id, "note": ""})

    return ToolResult.text(
        f"Task created: {task_id} (status: {status})\nFile: {file_path}",
        structured={"created": True, "task_id": task_id, "status": status},
    )


async def task_list_handler(ctx: ToolContext, args: dict) -> ToolResult:
    raw_status = (args.get("status", "") or "").strip().lower()

    if raw_status in ("", "open", "active"):
        status = None  # all non-done
    elif raw_status in ("all", "any"):
        status = "all"  # everything including done
    else:
        status = raw_status  # specific: pending, in_progress, done, deferred

    tag = (args.get("tag", "") or "").strip().lower()
    limit = int(args.get("limit", 100))

    if ctx.db:
        tasks = await ctx.db.list_tasks(status=status, tag=tag or None, limit=limit)
    else:
        tasks = []

    if not tasks:
        return ToolResult.text("No tasks found.")

    lines = []
    for t in tasks:
        tags_str = f" [{t['tags']}]" if t.get("tags") else ""
        deadline_str = f" (due: {t['deadline']})" if t.get("deadline") else ""
        lines.append(f"- [{t['status']}]{tags_str} {t['title']}{deadline_str} — {t['id']}")

    return ToolResult.text("\n".join(lines))


async def task_update_handler(ctx: ToolContext, args: dict) -> ToolResult:
    from nerve.tasks.models import parse_tags_string, tags_to_string

    task_id = args["task_id"]
    status = (args.get("status", "") or "").strip().lower()
    note = args.get("note", "")
    new_title = (args.get("title", "") or "").strip()

    # ``deadline`` and ``tags`` use presence, not truthiness: an absent key
    # means "leave it alone", a present-but-empty one is an explicit clear.
    # Clearing has to be expressible — otherwise the board's detail modal
    # can add a deadline or a tag but never remove the last one. Every
    # in-tree caller omits these keys entirely, so nothing else changes.
    deadline = args.get("deadline")
    raw_tags = args.get("tags")
    if raw_tags is not None:
        raw_tags = raw_tags.strip()
    has_field_edits = (
        deadline is not None or raw_tags is not None or bool(new_title)
    )

    # Reject unknown statuses with the list of valid options.
    if status:
        err = await _validate_status(ctx, status)
        if err:
            return ToolResult.text(err, is_error=True)

    # Route done transitions through task_done to ensure file move + FTS sync
    if status == TERMINAL_STATUS:
        return await task_done_handler(ctx, {"task_id": task_id, "note": note})

    # ...and the mirror. A task leaving the terminal status has to have its
    # markdown moved back out of done/ *before* anything else edits it —
    # otherwise the row points into done/ with a non-done status, which
    # TaskManager.reindex() treats as an orphan and force-resets to done.
    if ctx.db and status:
        current = await ctx.db.get_task(task_id)
        if current and current.get("status") == TERMINAL_STATUS:
            reopened = await task_reopen_handler(
                ctx, {"task_id": task_id, "status": status, "note": note},
            )
            if reopened.is_error or not has_field_edits:
                return reopened
            # More fields to apply: fall through and re-read the row below
            # so those edits land on the file's new location, not the stale
            # done/ path. The note has already been recorded by the reopen.
            note = ""

    if ctx.db:
        task = await ctx.db.get_task(task_id)
        if not task:
            return ToolResult.text(f"Task not found: {task_id}", is_error=True)

        new_tags_str = ""
        if raw_tags is not None:
            current_tags = set(parse_tags_string(task.get("tags", "") or ""))
            if raw_tags.startswith("+") or raw_tags.startswith("-"):
                for part in raw_tags.split(","):
                    part = part.strip()
                    if part.startswith("+"):
                        current_tags.add(part[1:].strip().lower())
                    elif part.startswith("-"):
                        current_tags.discard(part[1:].strip().lower())
                new_tags_str = tags_to_string(list(current_tags))
            else:
                new_tags_str = tags_to_string(parse_tags_string(raw_tags))

        if ctx.workspace and (note or has_field_edits):
            file_path = ctx.workspace / task["file_path"]
            ensure_path_not_tracked_config(file_path, "write")
            if file_path.exists():
                content = await asyncio.to_thread(
                    file_path.read_text, encoding="utf-8",
                )
                if new_title:
                    content = re.sub(r"^# .+", f"# {new_title}", content, count=1)
                if note:
                    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    content += f"\n- {today}: {note}"
                if deadline:
                    if "**Deadline:**" in content:
                        content = re.sub(r"\*\*Deadline:\*\* .*", f"**Deadline:** {deadline}", content)
                    else:
                        content = content.replace("\n\n", f"\n**Deadline:** {deadline}\n\n", 1)
                elif deadline is not None:
                    # Explicit clear — drop the line entirely so the file
                    # stops projecting a deadline back onto the column.
                    content = re.sub(r"\*\*Deadline:\*\* .*\n?", "", content, count=1)
                if new_tags_str:
                    display_tags = ", ".join(parse_tags_string(new_tags_str))
                    if "**Tags:**" in content:
                        content = re.sub(r"\*\*Tags:\*\* .*", f"**Tags:** {display_tags}", content)
                    else:
                        # Insert after last frontmatter line (Source/Deadline) before content
                        content = re.sub(
                            r"(\*\*(?:Source|Deadline):\*\* [^\n]*\n)",
                            rf"\1**Tags:** {display_tags}\n",
                            content,
                            count=1,
                        )
                        if "**Tags:**" not in content:
                            content = content.replace("\n\n", f"\n**Tags:** {display_tags}\n\n", 1)
                elif raw_tags is not None:
                    # Every tag removed (an empty set, or "-last_tag").
                    content = re.sub(r"\*\*Tags:\*\* .*\n?", "", content, count=1)
                await asyncio.to_thread(
                    file_path.write_text, content, encoding="utf-8",
                )

                final_title = new_title or task["title"]
                final_status = status or task["status"]
                # Presence carries through to the row the same way it does to
                # the file above: an omitted column keeps its stored value, a
                # present-but-empty one clears it.
                edits: dict = {}
                if deadline is not None:
                    edits["deadline"] = deadline or None
                if raw_tags is not None:
                    edits["tags"] = new_tags_str
                await ctx.db.upsert_task(
                    task_id=task_id,
                    file_path=task["file_path"],
                    title=final_title,
                    status=final_status,
                    content=content,
                    actor=ctx.session_id,
                    **edits,
                )
                await _emit_task_event(ctx, task_id, "updated")
                return ToolResult.text(f"Task {task_id} updated.")

        # Fall back to metadata updates when no task file was changed.
        if status:
            await ctx.db.update_task_status(task_id, status, actor=ctx.session_id)
        if raw_tags is not None:
            await ctx.db.update_task_tags(task_id, new_tags_str)
        await _emit_task_event(ctx, task_id, "updated")

    return ToolResult.text(f"Task {task_id} updated.")


async def task_read_handler(ctx: ToolContext, args: dict) -> ToolResult:
    task_id = args["task_id"]

    if ctx.db:
        task = await ctx.db.get_task(task_id)
        if not task:
            return ToolResult.text(f"Task not found: {task_id}")

        if ctx.workspace:
            file_path = ctx.workspace / task["file_path"]
            # Deliberately unguarded. Lockdown makes the tracked config subtree
            # unwritable, not unreadable — it arrives from a repo the agent can
            # already read, and the HTTP GET for a task doesn't guard either.
            # Guarding here would refuse a read with a "Cannot write" message
            # the caller has no way to act on.
            if file_path.exists():
                content = await asyncio.to_thread(
                    file_path.read_text, encoding="utf-8",
                )
                _tasks_read.add(task_id)
                return ToolResult.text(content)

    return ToolResult.text(f"Task file not found for: {task_id}")


async def task_write_handler(ctx: ToolContext, args: dict) -> ToolResult:
    task_id = args["task_id"]
    new_content = args.get("content", "")

    if task_id not in _tasks_read:
        return ToolResult.text(
            f"Cannot write task {task_id}: you must call task_read first."
        )

    if not new_content.strip():
        return ToolResult.text("Cannot write empty content.")

    if not ctx.db:
        return ToolResult.text("Database not available.")

    task = await ctx.db.get_task(task_id)
    if not task:
        return ToolResult.text(f"Task not found: {task_id}")

    if not ctx.workspace:
        return ToolResult.text("Workspace not configured.")

    file_path = ctx.workspace / task["file_path"]
    ensure_path_not_tracked_config(file_path, "write")
    await asyncio.to_thread(file_path.write_text, new_content, encoding="utf-8")

    from nerve.tasks.models import (
        parse_task_frontmatter,
        parse_task_title,
        parse_tags_string,
        tags_to_string,
    )
    new_title = parse_task_title(new_content) or task["title"]
    frontmatter = parse_task_frontmatter(new_content)
    # The deadline column is a pure projection of the file's Deadline line, so it
    # comes from the content we just wrote, never from the pre-write snapshot.
    new_deadline = frontmatter.get("deadline", "")
    # Tags follow the reindex rule: the file wins whenever it carries the
    # field, and an absent field keeps the stored column.
    edits: dict = {}
    if "tags" in frontmatter:
        edits["tags"] = tags_to_string(parse_tags_string(frontmatter["tags"]))

    await ctx.db.upsert_task(
        task_id=task_id,
        file_path=task["file_path"],
        title=new_title,
        status=task["status"],
        deadline=new_deadline or None,
        content=new_content,
        **edits,
    )

    await _emit_task_event(ctx, task_id, "updated")
    return ToolResult.text(f"Task {task_id} written ({len(new_content)} chars).")


async def task_done_handler(ctx: ToolContext, args: dict) -> ToolResult:
    task_id = args["task_id"]
    note = args.get("note", "")

    if ctx.db:
        task = await ctx.db.get_task(task_id)
        if not task:
            return ToolResult.text(f"Task not found: {task_id}", is_error=True)

        # Done is a write like any other — it appends to the file and moves it
        # into done/, so a stored ``file_path`` inside the tracked config
        # subtree would rewrite config. Refuse before the status flip, or a
        # refusal leaves a task marked done whose file never moved.
        if ctx.workspace:
            ensure_path_not_tracked_config(ctx.workspace / task["file_path"], "move")

        await ctx.db.update_task_status(task_id, "done", actor=ctx.session_id)

        # Mark any implementing plans for this task as done
        implementing_plans = await ctx.db.get_plans_for_task(task_id)
        for p in implementing_plans:
            if p.get("status") == "implementing":
                await ctx.db.update_plan(p["id"], status="done")

        # Move file to done/
        if ctx.workspace:
            src = ctx.workspace / task["file_path"]
            if src.exists():
                content = await asyncio.to_thread(src.read_text, encoding="utf-8")
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                if note:
                    content += f"\n- {today}: DONE — {note}"
                else:
                    content += f"\n- {today}: DONE"

                dst = _done_dir(ctx) / src.name

                await asyncio.to_thread(move_task_file, src, dst, content)

                rel_path = str(dst.relative_to(ctx.workspace))
                await ctx.db.upsert_task(
                    task_id=task_id,
                    file_path=rel_path,
                    title=task["title"],
                    status="done",
                    content=content,
                )

        await _emit_task_event(ctx, task_id, "done")

    return ToolResult.text(f"Task {task_id} marked as done.")


async def task_reopen_handler(ctx: ToolContext, args: dict) -> ToolResult:
    """Mirror of ``task_done``: bring a completed task back to life.

    ``task_done`` only moves a file one way, into done/, and nothing moved it
    back. That was fine while the only way out of ``done`` was never taking
    it, but the board lets a card be dragged out of the Done lane. Without
    the file travelling with the status, the row points into done/ while
    claiming to be active, and
    ``TaskManager.reindex()`` classifies exactly that as an orphan and
    force-resets it to done — silently undoing the move on the next reindex.

    Not registered as a standalone tool: ``task_update`` routes here on its
    own when it sees a task leaving the terminal status, so the agent-facing
    surface stays the same size.
    """
    task_id = args["task_id"]
    status = (args.get("status", "") or "").strip().lower() or DEFAULT_STATUS
    note = args.get("note", "")

    if status == TERMINAL_STATUS:
        return ToolResult.text(
            "task_reopen needs a non-terminal status — use task_done to complete a task.",
            is_error=True,
        )

    err = await _validate_status(ctx, status)
    if err:
        return ToolResult.text(err, is_error=True)

    if not ctx.db:
        return ToolResult.text("Database not available.", is_error=True)

    task = await ctx.db.get_task(task_id)
    if not task:
        return ToolResult.text(f"Task not found: {task_id}", is_error=True)
    if task["status"] != TERMINAL_STATUS:
        return ToolResult.text(
            f"Task {task_id} is not done (status: {task['status']}) — nothing to reopen.",
            is_error=True,
        )

    # Same ordering rationale as task_done: the refusal has to land before
    # the status flip, or a rejected move leaves a task marked active whose
    # file never left done/.
    src = ctx.workspace / task["file_path"] if ctx.workspace else None
    if src is not None:
        ensure_path_not_tracked_config(src, "move")
        # A missing file is a refusal for that same reason. With nothing to
        # move, the flip on its own leaves the row claiming an active status
        # while file_path still points into done/ — and reindex() cannot
        # repair it, because it only walks files that exist.
        if not src.exists():
            return ToolResult.text(
                f"Task {task_id} is done but its file is missing "
                f"({task['file_path']}) — not reopening it.",
                is_error=True,
            )

    await ctx.db.update_task_status(task_id, status, actor=ctx.session_id)

    if src is not None:
        # Re-checked because the guard above is not atomic with the move.
        if src.exists():
            content = await asyncio.to_thread(src.read_text, encoding="utf-8")
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            suffix = f" — {note}" if note else ""
            content += f"\n- {today}: REOPENED ({status}){suffix}"

            dst = _task_dir(ctx) / src.name

            await asyncio.to_thread(move_task_file, src, dst, content)

            await ctx.db.upsert_task(
                task_id=task_id,
                file_path=str(dst.relative_to(ctx.workspace)),
                title=task["title"],
                status=status,
                content=content,
            )

    await _emit_task_event(ctx, task_id, "updated")
    return ToolResult.text(f"Task {task_id} reopened (status: {status}).")


async def task_status_list_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if not ctx.db:
        return ToolResult.text("Database not available.")
    statuses = await ctx.db.list_task_statuses()
    if not statuses:
        return ToolResult.text("No task statuses configured.")
    lines = ["Configured task statuses:"]
    for s in statuses:
        desc = (s.get("description") or "").strip() or "(no description)"
        flag = " [protected]" if s.get("is_system") else ""
        lines.append(f"  - {s['name']} ({s['color']}){flag}: {desc}")
    return ToolResult.text("\n".join(lines))


async def task_status_create_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if not ctx.db:
        return ToolResult.text("Database not available.")

    name = (args.get("name", "") or "").strip().lower()
    if not name:
        return ToolResult.text("A status 'name' is required.")
    if not STATUS_NAME_RE.match(name):
        return ToolResult.text(
            f"Invalid status name '{name}'. Use lowercase letters, digits, "
            "and underscores, starting with a letter or digit (e.g. 'in_review')."
        )

    existing = await ctx.db.get_task_status_def(name)
    if existing:
        return ToolResult.text(f"Status '{name}' already exists.")

    label = (args.get("label", "") or "").strip() or name.replace("_", " ").title()
    color = normalize_color((args.get("color", "") or "").strip() or random_status_color())
    description = (args.get("description", "") or "").strip()

    created = await ctx.db.create_task_status(
        name=name, label=label, color=color, description=description,
    )
    return ToolResult.text(
        f"Created task status '{created['name']}' "
        f"(label: {created['label']}, color: {created['color']})."
    )


# Spec exports for registry registration.
TASK_SEARCH_SPEC = ToolSpec(
    name="task_search",
    description="Search tasks by keyword in title, content, tags, or slug. Supports partial words and task ID lookup. Returns matching tasks ranked by relevance. Use this before creating tasks to check for duplicates.",
    input_schema=TASK_SEARCH_SCHEMA,
    handler=task_search_handler,
)

TASK_CREATE_SPEC = ToolSpec(
    name="task_create",
    description="Create a new task. Checks for duplicates first — if similar tasks exist, returns them and refuses unless confirm_duplicate=true.",
    input_schema=TASK_CREATE_SCHEMA,
    handler=task_create_handler,
)

TASK_LIST_SPEC = ToolSpec(
    name="task_list",
    description="List tasks with optional status and tag filters.",
    input_schema=TASK_LIST_SCHEMA,
    handler=task_list_handler,
)

TASK_UPDATE_SPEC = ToolSpec(
    name="task_update",
    description="Update a task's status, deadline, tags, title, or add an update note.",
    input_schema=TASK_UPDATE_SCHEMA,
    handler=task_update_handler,
)

TASK_READ_SPEC = ToolSpec(
    name="task_read",
    description="Read the full content of a task's markdown file.",
    input_schema=TASK_READ_SCHEMA,
    handler=task_read_handler,
)

TASK_WRITE_SPEC = ToolSpec(
    name="task_write",
    description=(
        "Overwrite a task's markdown file with new content. "
        "You MUST call task_read first — this tool refuses to write unless the task has been read in this session."
    ),
    input_schema=TASK_WRITE_SCHEMA,
    handler=task_write_handler,
)

TASK_DONE_SPEC = ToolSpec(
    name="task_done",
    description="Mark a task as done and move its file to the done/ directory.",
    input_schema=TASK_DONE_SCHEMA,
    handler=task_done_handler,
)

TASK_STATUS_LIST_SPEC = ToolSpec(
    name="task_status_list",
    description="List the configured task statuses with their colors and descriptions. Use this to discover valid status values for task_create/task_update.",
    input_schema=TASK_STATUS_LIST_SCHEMA,
    handler=task_status_list_handler,
)

TASK_STATUS_CREATE_SPEC = ToolSpec(
    name="task_status_create",
    description=(
        "Create a new configurable task status. "
        "⚠️ Use this ONLY when configuring Nerve (e.g. initial setup) or when "
        "the user explicitly asks you to add a status. Do NOT invent new "
        "statuses during routine task management — use an existing status "
        "from task_status_list instead. Color defaults to a random color when "
        "omitted; description is optional."
    ),
    input_schema=TASK_STATUS_CREATE_SCHEMA,
    handler=task_status_create_handler,
)


TASK_SPECS = [
    TASK_SEARCH_SPEC,
    TASK_CREATE_SPEC,
    TASK_LIST_SPEC,
    TASK_UPDATE_SPEC,
    TASK_READ_SPEC,
    TASK_WRITE_SPEC,
    TASK_DONE_SPEC,
    TASK_STATUS_LIST_SPEC,
    TASK_STATUS_CREATE_SPEC,
]
