"""Task tool handlers — task_search, task_create, task_list, task_update,
task_read, task_write, task_done, task_comment.

Handlers are pure functions of ``(ToolContext, args)`` and return
:class:`ToolResult`. They read only from ``ctx`` for their collaborator
references; the only module-level state retained is ``_tasks_read``,
which is intentionally process-wide (it gates ``task_write`` against
arbitrary overwrites and operates as a read-before-write guard across
all sessions).
"""

from __future__ import annotations

import asyncio
import functools
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from nerve.agent.plan_service import plan_link
from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    TASK_COMMENT_SCHEMA,
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
from nerve.db.plans import CLOSED_WITH_TASK
from nerve.db.tasks import KEEP
from nerve.db.task_statuses import (
    DEFAULT_STATUS,
    STATUS_NAME_RE,
    TERMINAL_STATUS,
    normalize_color,
    random_status_color,
)
from nerve.tasks.files import move_task_file, write_task_file
from nerve.tasks.locks import task_mutation
from nerve.tasks.similarity import SimilarTask

logger = logging.getLogger(__name__)


def _one_change_per_task(handler):
    """Run a handler that writes a task file and its row under the task's lock.

    See :mod:`nerve.tasks.locks`. The lock covers the handler's first read
    of the row, so the path that it writes is the path the row names.
    """

    @functools.wraps(handler)
    async def wrapper(ctx: ToolContext, args: dict) -> ToolResult:
        async with task_mutation(args["task_id"]):
            return await handler(ctx, args)

    return wrapper


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


def _expect_revision(args: dict) -> int | None:
    """The optional ``expect_revision`` fence, or ``None`` when it is absent.

    Raises ``ValueError`` for a value that is not an integer: dropping it
    would turn the caller's conditional write into an unconditional one.
    """
    raw = args.get("expect_revision")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"expect_revision must be an integer, got {raw!r}.") from None


def _revision_conflict(
    task_id: str, expected: int, *, reopened: ToolResult | None = None,
) -> ToolResult:
    if reopened is not None:
        return ToolResult.text(
            f"Task {task_id} was reopened, but it changed again before the other edits were "
            f"written, so they were not applied. Re-read it and retry them.{_about_the_plans(reopened)}",
            is_error=True,
        )
    return ToolResult.text(
        f"Task {task_id} changed since you read it (expected revision {expected}); "
        "nothing was written. Re-read it and retry.",
        is_error=True,
    )


# task_read appends this line so an agent can see the revision; task_write
# strips it so a read-edit-write round trip cannot persist it.
_REVISION_TRAILER_RE = re.compile(r"^<!-- nerve: revision=\d+[^\n]*-->[ \t]*(?:\n|\Z)", re.MULTILINE)

# task_read shows the task's comments between these two lines, and task_write
# strips the block for the same reason as the revision trailer. Each comment
# line is indented, and so is each file line that starts with ``<!-- nerve:``
# (_FILE_MARKER_LINE_RE), so in task_read's output only Nerve's own lines
# start with a marker. A file cannot fake comments or lose content to the strip.
_FILE_MARKER_LINE_RE = re.compile(r"^(?=<!-- nerve:)", re.MULTILINE)
_COMMENTS_START = "<!-- nerve: comments (index metadata, not file content) -->"
_COMMENTS_END = "<!-- nerve: end of comments -->"
_COMMENTS_BLOCK_RE = re.compile(
    rf"^{re.escape(_COMMENTS_START)}\n.*?^{re.escape(_COMMENTS_END)}[ \t]*(?:\n|\Z)",
    re.MULTILINE | re.DOTALL,
)

# How many of the newest comments task_read shows.
_READ_COMMENT_LIMIT = 50

# The longest comment body, in characters.
MAX_COMMENT_LENGTH = 10_000


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


# Title-similarity bands for the duplicate guard. The index scores the new
# title against every existing title by IDF-weighted token overlap (see
# nerve.tasks.similarity): 1.0 is the same words, 0 is nothing in common
# beyond the words every title has. Measured on a 12.6K-task worker corpus
# and a 900-task personal one: a re-filed or reworded copy of a task scores
# 0.85 and above; same-area tasks that share a template or a few rare words
# score 0.2–0.8 — worth a mention, not a refusal. A task that shares a rare
# identifier (a test name, an issue number) is mentioned whatever its score.
# A match that names a different ticket in its title, or tracks a different
# source_url, is a different item however alike the words: mentioned, never
# refused.
DUPLICATE_SIMILARITY = 0.85
RELATED_SIMILARITY = 0.5
MAX_DUPLICATES_LISTED = 10
MAX_RELATED_LISTED = 5


def _similar_entry(match: SimilarTask, *, same_source: bool = False) -> dict:
    return {
        "id": match.id,
        "title": match.title,
        "status": match.status,
        "deadline": match.row.get("deadline") or "",
        "similarity": match.score,
        "shared": list(match.shared[:6]),
        "shared_identifiers": list(match.shared_identifiers),
        "same_source": same_source,
        "distinct_by": "",
    }


def _format_similar(entry: dict) -> str:
    deadline = f" (due: {entry['deadline']})" if entry.get("deadline") else ""
    notes = []
    if entry.get("same_source"):
        notes.append("same source_url")
    notes.append(f"title similarity {entry['similarity']:.2f}")
    if entry.get("distinct_by"):
        notes.append(entry["distinct_by"])
    if entry.get("shared_identifiers"):
        notes.append("shares the identifier " + ", ".join(entry["shared_identifiers"]))
    elif entry.get("shared"):
        notes.append("shares: " + ", ".join(entry["shared"]))
    return f"  - [{entry['status']}] {entry['title']}{deadline} — {entry['id']}  ({'; '.join(notes)})"


async def _assess_duplicates(
    ctx: ToolContext, title: str, source_url: str = "",
) -> tuple[list[dict], list[dict]]:
    """What a new task with this title and source would double.

    Returns ``(duplicates, related)``. ``duplicates`` refuse the create:
    every task with the same ``source_url`` — the identity of a
    source-generated item, whose title the agent may word differently each
    time — and every task whose title is nearly the same. ``related`` are
    the remaining look-alikes above RELATED_SIMILARITY and the tasks that
    share a rare identifier with the title: reported with the outcome so
    the caller can link or merge, never a reason to refuse.
    """
    if not ctx.db:
        return [], []
    index = await ctx.db.title_index()
    duplicates: list[dict] = []
    seen: set[str] = set()
    if source_url:
        for row in await ctx.db.find_tasks_by_source_url(
            source_url, limit=MAX_DUPLICATES_LISTED,
        ):
            match = index.compare(title, row["id"])
            if match is None:
                # Created after the index was built; still the same source.
                entry = {
                    "id": row["id"], "title": row["title"], "status": row["status"],
                    "deadline": row.get("deadline") or "", "similarity": 0.0,
                    "shared": [], "shared_identifiers": [], "same_source": True,
                    "distinct_by": "",
                }
            else:
                entry = _similar_entry(match, same_source=True)
            duplicates.append(entry)
            seen.add(row["id"])
    related: list[dict] = []
    for match in index.similar(
        title, min_score=RELATED_SIMILARITY,
        limit=MAX_DUPLICATES_LISTED + MAX_RELATED_LISTED + len(seen),
    ):
        if match.id in seen:
            continue
        entry = _similar_entry(match)
        other_source = match.row.get("source_url") or ""
        if match.conflicting_refs:
            entry["distinct_by"] = "names a different ticket"
        elif source_url and other_source and other_source != source_url:
            entry["distinct_by"] = "different source_url"
        if match.score >= DUPLICATE_SIMILARITY and not entry["distinct_by"]:
            duplicates.append(entry)
        else:
            related.append(entry)
    duplicates.sort(key=lambda e: (not e["same_source"], -e["similarity"]))
    # A shared identifier is the stronger clue; keep those within the cap.
    related.sort(key=lambda e: (not e["shared_identifiers"], -e["similarity"]))
    return duplicates[:MAX_DUPLICATES_LISTED], related[:MAX_RELATED_LISTED]


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

    # The duplicate guard. A confirmed create skips the refusal, not the
    # look-up: the matches still travel with the result as related tasks.
    duplicates, related = await _assess_duplicates(ctx, title, source_url=source_url)
    if duplicates and not confirm:
        lines = [f"⚠️ Found {len(duplicates)} likely duplicate task(s):"]
        lines.extend(_format_similar(e) for e in duplicates)
        if related:
            lines.append("Related, not blocking:")
            lines.extend(_format_similar(e) for e in related)
        lines.append("")
        lines.append(
            "Task NOT created. If none of these is the same work item, call "
            "task_create again with confirm_duplicate=true."
        )
        return ToolResult.text(
            "\n".join(lines),
            structured={
                "created": False,
                "reason": "duplicate",
                "duplicates": duplicates,
                "related": related,
            },
        )
    mentions = (duplicates + related if confirm else related)[:MAX_RELATED_LISTED]

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
                attribution=ctx.attribution,
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

    text = f"Task created: {task_id} (status: {status})\nFile: {file_path}"
    structured: dict = {"created": True, "task_id": task_id, "status": status}
    if mentions:
        text += (
            "\nPossibly related tasks (created anyway — if one of them is the "
            "same work item, close this task as its duplicate):\n"
            + "\n".join(_format_similar(e) for e in mentions)
        )
        structured["related"] = mentions
    return ToolResult.text(text, structured=structured)


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


@_one_change_per_task
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
    try:
        expect_revision = _expect_revision(args)
    except ValueError as e:
        return ToolResult.text(str(e), is_error=True)

    # Reject unknown statuses with the list of valid options.
    if status:
        err = await _validate_status(ctx, status)
        if err:
            return ToolResult.text(err, is_error=True)

    # Route done transitions through task_done to ensure file move + FTS sync
    if status == TERMINAL_STATUS:
        return await task_done_handler(
            ctx, {"task_id": task_id, "note": note, "expect_revision": expect_revision},
        )

    # ...and the mirror. A task leaving the terminal status has to have its
    # markdown moved back out of done/ *before* anything else edits it —
    # otherwise the row points into done/ with a non-done status, which
    # TaskManager.reindex() treats as an orphan and force-resets to done.
    reopened: ToolResult | None = None
    if ctx.db and status:
        current = await ctx.db.get_task(task_id)
        if current and current.get("status") == TERMINAL_STATUS:
            reopened = await task_reopen_handler(
                ctx,
                {"task_id": task_id, "status": status, "note": note,
                 "expect_revision": expect_revision},
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
        if reopened and expect_revision is not None:
            # The caller's token guarded the reopen, which has written since:
            # the status, and a note when a plan came back with the task. The
            # other edits are fenced on the revision that those writes of the
            # reopen left, so a write of anybody else, during the reopen or
            # after it, still refuses them.
            expect_revision = reopened.structured["revision"]

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

                final_title = new_title or task["title"]
                # Presence carries through to the row the same way it does to
                # the file above: an omitted column keeps its stored value, a
                # present-but-empty one clears it.
                edits: dict = {}
                if deadline is not None:
                    edits["deadline"] = deadline or None
                if raw_tags is not None:
                    edits["tags"] = new_tags_str
                write = lambda: write_task_file(file_path, content)
                # KEEP for what this call does not change: the path belongs to
                # task_done / task_reopen, and a status that another writer
                # set after the read above must stay. A requested status is
                # applied in the write's transaction, from the stored one.
                kw = dict(
                    task_id=task_id, file_path=KEEP, title=final_title,
                    status=status or KEEP, content=content, actor=ctx.session_id,
                    attribution=ctx.attribution, **edits,
                )
                # Without a token the order stays file-then-row, as before. With
                # one the row is written first, so a refused caller writes no file.
                if expect_revision is None:
                    await asyncio.to_thread(write)
                    await ctx.db.upsert_task(**kw)
                else:
                    if not await ctx.db.upsert_task(**kw, expect_revision=expect_revision):
                        return _revision_conflict(task_id, expect_revision, reopened=reopened)
                    await asyncio.to_thread(write)
                await _emit_task_event(ctx, task_id, "updated")
                return ToolResult.text(f"Task {task_id} updated.{_about_the_plans(reopened)}")

        # Fall back to metadata updates when no task file was changed.
        if expect_revision is not None and (note or deadline is not None or new_title):
            if reopened is not None:
                return ToolResult.text(
                    f"Task {task_id} was reopened, but it has no task file to write, so its "
                    "other edits were not applied.",
                    is_error=True,
                )
            return ToolResult.text(
                f"Task {task_id} has no task file to write, so its note, title or deadline "
                "cannot be applied; nothing was written.",
                is_error=True,
            )
        applied = True
        if status:
            applied = await ctx.db.transition_task(
                task_id, status, expect_revision=expect_revision, actor=ctx.session_id,
                attribution=ctx.attribution,
                **({"tags": new_tags_str} if raw_tags is not None else {}),
            )
        elif raw_tags is not None:
            applied = await ctx.db.update_task_tags(
                task_id, new_tags_str, expect_revision=expect_revision,
            )
        if not applied and expect_revision is not None:
            return _revision_conflict(task_id, expect_revision, reopened=reopened)
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
                content = _FILE_MARKER_LINE_RE.sub("  ", await asyncio.to_thread(
                    file_path.read_text, encoding="utf-8",
                ))
                _tasks_read.add(task_id)
                # Read after the row: a comment added in between shows here
                # with the older revision, so a guarded write is refused and
                # the agent reads again. The other order could hide it.
                comments = (
                    await _format_comments(ctx, task)
                    if task.get("comment_count") else ""
                )
                # The adapters drop ``structured``, so the text carries it too.
                sep = "" if content.endswith("\n") else "\n"
                return ToolResult.text(
                    f"{content}{sep}{comments}<!-- nerve: revision={task['revision']} "
                    "(index metadata, not file content) -->",
                    structured={"revision": task["revision"]},
                )

    return ToolResult.text(f"Task file not found for: {task_id}")


async def _format_comments(ctx: ToolContext, task: dict) -> str:
    """The comments block that task_read adds after the file content."""
    comments = await ctx.db.list_task_comments(task["id"], limit=_READ_COMMENT_LIMIT)
    if not comments:
        return ""
    ids = {c[key] for c in comments for key in ("actor_id", "caused_by_actor_id")} - {None}
    refs = {actor_id: await ctx.db.get_actor_ref(actor_id) for actor_id in ids}

    lines = [_COMMENTS_START, f"## Comments ({task['comment_count']})"]
    older = task["comment_count"] - len(comments)
    if older > 0:
        lines.append(f"({older} older comments are not shown.)")
    for comment in comments:
        # "Alice", or "Nerve (agent) for Alice" when the agent wrote it in a
        # session that Alice started. The web UI uses the same rule.
        author = _actor_name(refs, comment["actor_id"])
        cause = refs.get(comment["caused_by_actor_id"])
        if cause and cause["id"] != comment["actor_id"] and cause["kind"] != "system":
            author += f" for {_actor_name(refs, cause['id'])}"
        when = datetime.fromisoformat(comment["created_at"]).strftime("%Y-%m-%d %H:%M UTC")
        lines.append(f"- {when}, {author}:")
        lines.extend(f"  {line}" for line in comment["body"].splitlines())
    lines.append(_COMMENTS_END)
    return "\n".join(lines) + "\n"


def _actor_name(refs: dict[str, dict | None], actor_id: str | None) -> str:
    """A display name for an ``actor_refs`` ID, with the web UI's fallbacks."""
    if actor_id is None:
        return "unknown"
    ref = refs.get(actor_id)
    if ref is None:
        return actor_id[:8]
    name = (ref["display_name"] or "").strip() or (ref.get("username") or "").strip()
    if ref["kind"] == "system":
        return f"{name or 'Nerve'} (agent)"
    return name or actor_id[:8]


async def task_comment_handler(ctx: ToolContext, args: dict) -> ToolResult:
    """Add a comment to a task as the caller (``ctx.attribution``).

    ``structured["reason"]`` names a refusal, so the REST route can choose a
    status code without parsing the message.
    """
    task_id = args["task_id"]
    body = (args.get("body") or "").strip()
    if not body:
        return ToolResult.text(
            "Cannot add an empty comment.", is_error=True, structured={"reason": "empty"},
        )
    if len(body) > MAX_COMMENT_LENGTH:
        return ToolResult.text(
            f"A comment can have at most {MAX_COMMENT_LENGTH} characters; "
            f"this one has {len(body)}.",
            is_error=True, structured={"reason": "too_long"},
        )
    if not ctx.db:
        return ToolResult.text("Database not available.", is_error=True)
    comment = await ctx.db.add_task_comment(task_id, body, attribution=ctx.attribution)
    if comment is None:
        return ToolResult.text(
            f"Task not found: {task_id}", is_error=True, structured={"reason": "not_found"},
        )
    await _emit_task_event(ctx, task_id, "commented")
    return ToolResult.text(
        f"Comment {comment['id']} added to task {task_id}.",
        structured={"comment": comment},
    )


@_one_change_per_task
async def task_write_handler(ctx: ToolContext, args: dict) -> ToolResult:
    task_id = args["task_id"]
    new_content = _REVISION_TRAILER_RE.sub(
        "", _COMMENTS_BLOCK_RE.sub("", args.get("content", "")),
    )

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
    await asyncio.to_thread(write_task_file, file_path, new_content)

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

    # task_write changes neither the path nor the status. KEEP reads both in
    # the write's transaction, so a move by another writer since the read
    # above stays.
    await ctx.db.upsert_task(
        task_id=task_id,
        file_path=KEEP,
        title=new_title,
        status=KEEP,
        deadline=new_deadline or None,
        content=new_content,
        attribution=ctx.attribution,
        **edits,
    )

    await _emit_task_event(ctx, task_id, "updated")
    return ToolResult.text(f"Task {task_id} written ({len(new_content)} chars).")


@_one_change_per_task
async def task_done_handler(ctx: ToolContext, args: dict) -> ToolResult:
    task_id = args["task_id"]
    note = args.get("note", "")
    try:
        expect_revision = _expect_revision(args)
    except ValueError as e:
        return ToolResult.text(str(e), is_error=True)

    closed: list[str] = []
    if ctx.db:
        task = await ctx.db.get_task(task_id)
        if not task:
            return ToolResult.text(f"Task not found: {task_id}", is_error=True)
        was_open = task.get("status") != TERMINAL_STATUS

        # Done is a write like any other — it appends to the file and moves it
        # into done/, so a stored ``file_path`` inside the tracked config
        # subtree would rewrite config. Refuse before the status flip, or a
        # refusal leaves a task marked done whose file never moved.
        if ctx.workspace:
            ensure_path_not_tracked_config(ctx.workspace / task["file_path"], "move")

        src = ctx.workspace / task["file_path"] if ctx.workspace else None
        if src is not None and src.exists():
            content = await asyncio.to_thread(src.read_text, encoding="utf-8")
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if note:
                content += f"\n- {today}: DONE — {note}"
            else:
                content += f"\n- {today}: DONE"

            dst = _done_dir(ctx) / src.name
            rel_path = str(dst.relative_to(ctx.workspace))
            # Without a token the file moves before the row is written. With one
            # the row is claimed first, so a refused caller moves nothing.
            if expect_revision is None:
                await asyncio.to_thread(move_task_file, src, dst, content)
                await ctx.db.transition_task(
                    task_id, "done", actor=ctx.session_id, attribution=ctx.attribution,
                    file_path=rel_path, content=content,
                )
            else:
                if not await ctx.db.transition_task(
                    task_id, "done", expect_revision=expect_revision,
                    actor=ctx.session_id, attribution=ctx.attribution,
                    file_path=rel_path, content=content,
                ):
                    return _revision_conflict(task_id, expect_revision)
                await asyncio.to_thread(move_task_file, src, dst, content)
        else:
            done = await ctx.db.transition_task(
                task_id, "done", expect_revision=expect_revision, actor=ctx.session_id,
                attribution=ctx.attribution,
            )
            if not done and expect_revision is not None:
                return _revision_conflict(task_id, expect_revision)

        # Mark any implementing plans for this task as done. A failed plan
        # is done too when its own implementation session closes the task:
        # a later turn there finished the work.
        #
        # A plan that still waits for review is closed with the task. Nobody
        # decided it, so it gets a status of its own and no reviewer, and a
        # person who reopens the task gets it back. The plan is written only
        # while it is still pending, so one that is approved at that moment
        # stays approved.
        #
        # A call that finds the task done already closed nothing new: a plan
        # that was proposed for a done task waits for a decision of its own.
        # What it does close is a plan that is older than the task's last
        # close, which that close left in review: its write failed, or it
        # was made by a release from before this rule.
        closed_at = None if was_open else _stored_time(await ctx.db.get_task_closed_at(task_id))
        for p in await ctx.db.get_plans_for_task(task_id):
            retried = p.get("status") == "failed" and p.get("impl_session_id") == ctx.session_id
            if p.get("status") == "implementing" or retried:
                await ctx.db.update_plan(p["id"], status="done")
            elif p.get("status") == "pending" and (was_open or _proposed_by(p, closed_at)):
                if await ctx.db.compare_and_update_plan(
                    p["id"], expect_status="pending", status=CLOSED_WITH_TASK,
                ):
                    closed.append(p["id"])
        if closed:
            await ctx.db.dismiss_plan_notifications(task_id, attribution=ctx.attribution)
            await _note_about_plans(
                ctx, task_id, closed, "waited for review and was closed with the task",
            )

        await _emit_task_event(ctx, task_id, "done")

    result = f"Task {task_id} marked as done."
    if closed:
        result += (
            f" Its plan in review ({', '.join(closed)}) was closed with it, undecided. "
            "If the user had approved the plan and this session did the work, record that "
            "with plan_approve (implement_here=true). The plan is in review again when the "
            "user reopens the task on the board or the task page."
        )
    return ToolResult.text(result, structured={"plans_closed": closed})


def _stored_time(value: object) -> datetime | None:
    """A stored timestamp as an aware time, UTC when it names no zone;
    ``None`` for one that is missing or does not parse."""
    if not value:
        return None
    try:
        when = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return when if when.tzinfo else when.replace(tzinfo=timezone.utc)


def _proposed_by(plan: dict, closed_at: datetime | None) -> bool:
    """Whether *plan* was proposed by the time its task was closed. A time
    that is not known, on either side, answers no: the plan stays in review."""
    proposed_at = _stored_time(plan.get("created_at"))
    return closed_at is not None and proposed_at is not None and proposed_at <= closed_at


async def _note_about_plans(
    ctx: ToolContext, task_id: str, plan_ids: list[str], what: str,
    *, expect_revision: int | None = None,
) -> bool:
    """Add a line about *plan_ids* to the task's notes, as every other change
    of a plan does. The plans are written already, so a note that fails is
    logged and nothing else.

    With *expect_revision* the note is first tried as a write from exactly
    that revision, and the answer is whether it landed as one. When the row
    has moved on, the note is written all the same, and the answer is no: the
    caller then knows that somebody else wrote in between.
    """
    links = ", ".join(plan_link(plan_id) for plan_id in plan_ids)
    args = {"task_id": task_id, "note": f"Plan {links} {what}."}
    try:
        if expect_revision is not None:
            fenced = await task_update_handler(ctx, {**args, "expect_revision": expect_revision})
            if not fenced.is_error:
                return True
        await task_update_handler(ctx, args)
    except Exception:  # noqa: BLE001 (the note is the least of the change)
        logger.warning("Could not note the plans %s on task %s", plan_ids, task_id, exc_info=True)
    return False


async def _plans_closed_with(ctx: ToolContext, task_id: str) -> list[str]:
    """The plans that were closed with the task and that no newer plan has
    replaced: more than one only if more than one was in review at the
    close. A plan that was proposed for the task since has taken the place
    of a closed one, whatever became of it."""
    plans = await ctx.db.get_plans_for_task(task_id)
    newest = max((p.get("version") or 0 for p in plans), default=0)
    return [
        p["id"] for p in plans
        if p.get("status") == CLOSED_WITH_TASK and (p.get("version") or 0) == newest
    ]


def _about_the_plans(reopened: ToolResult | None) -> str:
    """What a reopen said about the task's plan, for the result of a call
    that went on to other edits; empty when it said nothing."""
    said = (reopened.structured or {}) if reopened is not None else {}
    return said.get("about_plans", "")


@_one_change_per_task
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
    try:
        expect_revision = _expect_revision(args)
    except ValueError as e:
        return ToolResult.text(str(e), is_error=True)

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

    kw: dict = dict(
        expect=(TERMINAL_STATUS,), expect_revision=expect_revision, actor=ctx.session_id,
        attribution=ctx.attribution,
    )
    if src is not None:
        content = await asyncio.to_thread(src.read_text, encoding="utf-8")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        suffix = f" — {note}" if note else ""
        content += f"\n- {today}: REOPENED ({status}){suffix}"
        dst = _task_dir(ctx) / src.name
        kw.update(file_path=str(dst.relative_to(ctx.workspace)), content=content)
    # ``expect`` makes the "is it still done?" check above atomic with the flip, and the
    # row is claimed before any file work, so a refused reopen moves nothing.
    if not await ctx.db.transition_task(task_id, status, **kw):
        if expect_revision is not None:
            return _revision_conflict(task_id, expect_revision)
        return ToolResult.text(
            f"Task {task_id} changed before it could be reopened (it is no longer done); "
            "nothing was written.",
            is_error=True,
        )
    if src is not None:
        await asyncio.to_thread(move_task_file, src, dst, content)

    # The close decided nothing about a plan that was in review, so a person
    # who reopens the task has it back as it was: a card that is dragged into
    # the Done lane and back is the same card. An agent that reopens a task,
    # perhaps long after, does not put a plan in front of the person again
    # without a word: the plan stays closed, and the agent is told.
    about_plans = ""
    noted = False
    waiting = await _plans_closed_with(ctx, task_id)
    if waiting and ctx.actor is not None and ctx.actor.is_human:
        restored = [
            plan_id for plan_id in waiting
            if await ctx.db.compare_and_update_plan(
                plan_id, expect_status=CLOSED_WITH_TASK, status="pending",
            )
        ]
        if restored:
            noted = await _note_about_plans(
                ctx, task_id, restored, "is in review again: it was closed with the task",
                expect_revision=None if expect_revision is None else expect_revision + 1,
            )
            about_plans = (
                f" Its plan ({', '.join(restored)}), which was closed with the task, "
                "waits for review again."
            )
    elif waiting:
        about_plans = (
            f" Its plan ({', '.join(waiting)}) stays closed: it was closed with the task, "
            "undecided. If it is still wanted, propose it again with plan_propose; if the "
            "user had approved it and this session did the work, record that with "
            "plan_approve (implement_here=true)."
        )

    await _emit_task_event(ctx, task_id, "updated")
    # The revision that the writes of this reopen left, for a caller that
    # sent a token and goes on to other edits (see task_update): the status
    # write moved the row on by one, and the note, when it landed as the
    # next write, by one more. It is worked out, not read: read, it would
    # take a write of somebody else during the reopen for one of its own.
    left = None if expect_revision is None else expect_revision + 1 + (1 if noted else 0)
    return ToolResult.text(
        f"Task {task_id} reopened (status: {status}).{about_plans}",
        structured={"about_plans": about_plans, "revision": left},
    )


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
        attribution=ctx.attribution,
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
    description=(
        "Create a new task. Refuses when a task with the same source_url exists "
        "or an existing title is nearly the same, and lists them; pass "
        "confirm_duplicate=true to create anyway. Look-alike tasks that do not "
        "block are listed in the result as related — check them before starting."
    ),
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
    description=(
        "Update a task's status, deadline, tags, title, or add an update note. "
        "The status done closes the task as task_done does. "
        "Pass expect_revision (from task_read) to refuse the update, writing "
        "nothing, if the task has changed since."
    ),
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
    description=(
        "Mark a task as done and move its file to the done/ directory. "
        "A plan of the task that still waits for review is closed with it, "
        "undecided; if the user approved the plan and this session did the "
        "work, call plan_approve with implement_here=true before or after. "
        "Pass expect_revision (from task_read) to refuse the completion, "
        "writing nothing, if the task has changed since."
    ),
    input_schema=TASK_DONE_SCHEMA,
    handler=task_done_handler,
)

TASK_COMMENT_SPEC = ToolSpec(
    name="task_comment",
    description=(
        "Add a comment to a task. The comment shows in the task's comment list "
        "in the web UI, signed as you, and task_read shows it to agents. Use it "
        "for discussion with people about the task; use task_update's note for "
        "a progress line in the task file."
    ),
    input_schema=TASK_COMMENT_SCHEMA,
    handler=task_comment_handler,
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
    TASK_COMMENT_SPEC,
    TASK_STATUS_LIST_SPEC,
    TASK_STATUS_CREATE_SPEC,
]
