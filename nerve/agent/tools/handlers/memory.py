"""Memory tool handlers — memory_recall, conversation_history,
memory_records_by_date, memorize, memory_update, memory_delete,
category_update.

All handlers read collaborators (``memory_bridge``, ``config``) from
:class:`ToolContext`; nothing is stored at module level.
"""

from __future__ import annotations

import logging
import sqlite3
import tempfile
import time
from pathlib import Path

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    CATEGORY_UPDATE_SCHEMA,
    CONVERSATION_HISTORY_SCHEMA,
    MEMORIZE_SCHEMA,
    MEMORY_DELETE_SCHEMA,
    MEMORY_RECALL_SCHEMA,
    MEMORY_RECORDS_BY_DATE_SCHEMA,
    MEMORY_UPDATE_SCHEMA,
    SESSION_CONTEXT_SCHEMA,
)

logger = logging.getLogger(__name__)


def _resolve_memu_db_path(ctx: ToolContext) -> str:
    """Resolve the memU SQLite DB path from ctx.config (falls back to global config)."""
    if ctx.config is not None:
        return ctx.config.memory.sqlite_dsn.replace("sqlite:///", "")
    from nerve.config import get_config
    return get_config().memory.sqlite_dsn.replace("sqlite:///", "")


async def memory_recall_handler(ctx: ToolContext, args: dict) -> ToolResult:
    query = args["query"]
    limit = int(args.get("limit", 10))

    if ctx.memory_bridge:
        try:
            results = await ctx.memory_bridge.recall(query, limit=limit)
            if results:
                lines = [f"- [{m['type']}] (id:{m['id']}) {m['summary']}" for m in results]
                text = "\n".join(lines)
                return ToolResult.text(f"Recalled {len(results)} memories:\n\n{text}")
            return ToolResult.text("No relevant memories found.")
        except Exception as e:
            logger.error("Memory recall failed: %s", e)
            return ToolResult.text(f"Memory recall error: {e}")

    return ToolResult.text("Memory service not configured.")


async def session_context_handler(ctx: ToolContext, args: dict) -> ToolResult:
    """Return the dynamic startup context as a single combined message.

    Nerve-owned sessions get this context for free — the system prompt
    builder injects recalled memories, active skills, and session
    metadata. External MCP clients (Codex, Claude Code) need to fetch
    it explicitly because they have no system-prompt hook. AGENTS.md
    instructs them to call this tool as their first action.

    The recall query is biased by ``topic`` so the priors returned are
    actually relevant to the task the agent is about to attempt.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    topic = (args.get("topic") or "").strip()
    if not topic:
        return ToolResult.text(
            "session_context() requires a non-empty `topic` argument. "
            "Pass a short description of what you're about to work on.",
            is_error=True,
        )

    include_skills = bool(args.get("include_skills", True))
    memory_limit = int(args.get("memory_limit", 15))

    parts: list[str] = []

    # Session metadata (id, source, current time, workspace)
    session_record: dict | None = None
    if ctx.db is not None and ctx.session_id:
        try:
            session_record = await ctx.db.get_session(ctx.session_id)
        except Exception as e:  # pragma: no cover - defensive
            logger.debug("session_context: get_session failed: %s", e)

    tz_name = "UTC"
    if ctx.config is not None:
        tz_name = ctx.config.timezone or "UTC"
    try:
        now = datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M %Z")
    except Exception:
        now = datetime.now().strftime("%Y-%m-%d %H:%M")

    source = (session_record or {}).get("source", "external") if session_record else "external"
    workspace = str(ctx.workspace) if ctx.workspace else "(unknown)"

    parts.append(
        "# Session Context\n\n"
        f"- **Session ID:** {ctx.session_id or '(none)'}\n"
        f"- **Source:** {source}\n"
        f"- **Current time:** {now}\n"
        f"- **Workspace:** {workspace}\n"
        f"- **Topic:** {topic}"
    )

    # Recalled memories (biased by topic)
    if ctx.memory_bridge and ctx.memory_bridge.available:
        try:
            results = await ctx.memory_bridge.recall(
                f"context for {source} session: {topic}",
                limit=memory_limit,
            )
            if results:
                lines = [
                    f"- [{m['type']}] (id:{m['id']}) {m['summary']}"
                    for m in results
                ]
                parts.append(
                    f"# Recalled Memories ({len(results)} biased by topic)\n\n"
                    + "\n".join(lines)
                )
            else:
                parts.append("# Recalled Memories\n\n(none — fresh topic or empty memU)")
        except Exception as e:
            logger.warning("session_context recall failed: %s", e)
            parts.append(f"# Recalled Memories\n\n_recall error: {e}_")
    else:
        parts.append("# Recalled Memories\n\n_memory bridge not available_")

    # Active skills summary
    if include_skills and ctx.skill_manager is not None:
        try:
            summaries = await ctx.skill_manager.get_enabled_summaries()
            if summaries:
                lines = [
                    f"- **{s['name']}** (`{s['id']}`): {s['description']}"
                    for s in summaries
                ]
                parts.append(
                    f"# Active Skills ({len(summaries)})\n\n" + "\n".join(lines)
                )
            else:
                parts.append("# Active Skills\n\n(none enabled)")
        except Exception as e:
            logger.warning("session_context skill list failed: %s", e)

    return ToolResult.text("\n\n---\n\n".join(parts))


async def conversation_history_handler(ctx: ToolContext, args: dict) -> ToolResult:
    date = args["date"]
    end_date = args.get("end_date", "") or date
    limit = int(args.get("limit", 30))

    if not ctx.memory_bridge or not ctx.memory_bridge.available:
        return ToolResult.text("Memory service not available.")

    try:
        db_path = _resolve_memu_db_path(ctx)
        db = sqlite3.connect(db_path)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT id, memory_type, summary, happened_at FROM memu_memory_items "
            "WHERE happened_at IS NOT NULL "
            "AND date(happened_at) >= date(?) AND date(happened_at) <= date(?) "
            "ORDER BY happened_at DESC "
            "LIMIT ?",
            (date, end_date, limit),
        ).fetchall()
        db.close()

        if not rows:
            label = f"{date}" + (f" to {end_date}" if end_date != date else "")
            return ToolResult.text(f"No memories found for {label}.")

        lines = [f"- [{row['memory_type']}] (id:{row['id']}) {row['summary']}" for row in rows]
        header_range = f"{date}" + (f" to {end_date}" if end_date != date else "")
        return ToolResult.text(
            f"Memories from {header_range} ({len(rows)} items):\n\n" + "\n".join(lines)
        )
    except Exception as e:
        logger.error("Conversation history failed: %s", e)
        return ToolResult.text(f"Error: {e}")


async def memory_records_by_date_handler(ctx: ToolContext, args: dict) -> ToolResult:
    date = args["date"]
    end_date = args.get("end_date", "") or date
    limit = int(args.get("limit", 100))
    include_updated = args.get("updated", False)

    if not ctx.memory_bridge or not ctx.memory_bridge.available:
        return ToolResult.text("Memory service not available.")

    try:
        db_path = _resolve_memu_db_path(ctx)
        db = sqlite3.connect(db_path)
        db.row_factory = sqlite3.Row

        if include_updated:
            query = (
                "SELECT id, memory_type, summary, created_at, updated_at FROM memu_memory_items "
                "WHERE (date(created_at) >= date(?) AND date(created_at) <= date(?)) "
                "   OR (date(updated_at) >= date(?) AND date(updated_at) <= date(?) AND date(updated_at) != date(created_at)) "
                "ORDER BY created_at DESC "
                "LIMIT ?"
            )
            rows = db.execute(query, (date, end_date, date, end_date, limit)).fetchall()
        else:
            query = (
                "SELECT id, memory_type, summary, created_at, updated_at FROM memu_memory_items "
                "WHERE date(created_at) >= date(?) AND date(created_at) <= date(?) "
                "ORDER BY created_at DESC "
                "LIMIT ?"
            )
            rows = db.execute(query, (date, end_date, limit)).fetchall()

        db.close()

        if not rows:
            label = f"{date}" + (f" to {end_date}" if end_date != date else "")
            return ToolResult.text(f"No records created on {label}.")

        lines = []
        for row in rows:
            updated_marker = ""
            if row["updated_at"] and row["created_at"] and row["updated_at"] != row["created_at"]:
                updated_marker = " (updated)" if include_updated else ""
            lines.append(f"- [{row['memory_type']}] (id:{row['id']}) {row['summary']}{updated_marker}")

        label = f"{date}" + (f" to {end_date}" if end_date != date else "")
        header = f"Records from {label} ({len(rows)} items):"
        return ToolResult.text(f"{header}\n\n" + "\n".join(lines))
    except Exception as e:
        logger.error("Memory records by date failed: %s", e)
        return ToolResult.text(f"Error: {e}")


async def memorize_handler(ctx: ToolContext, args: dict) -> ToolResult:
    content = args["content"]
    memory_type = args.get("memory_type", "knowledge")

    if not ctx.memory_bridge or not ctx.memory_bridge.available:
        return ToolResult.text("Memory service not available.")

    try:
        mem_dir = Path("~/.nerve/memu-manual").expanduser()
        mem_dir.mkdir(parents=True, exist_ok=True)
        mem_path = mem_dir / f"memorize-{int(time.time())}.txt"
        mem_path.write_text(f"{memory_type}: {content}", encoding="utf-8")

        success = await ctx.memory_bridge.memorize_file(str(mem_path), modality="document")
        if success:
            return ToolResult.text(f"Memorized: {content}")
        return ToolResult.text("Failed to memorize.")
    except Exception as e:
        logger.error("Memorize failed: %s", e)
        return ToolResult.text(f"Error: {e}")


async def memory_update_handler(ctx: ToolContext, args: dict) -> ToolResult:
    memory_id = args["memory_id"]
    content = args.get("content", "") or None
    memory_type = args.get("memory_type", "") or None
    raw_cats = args.get("categories", "") or ""
    categories = [c.strip() for c in raw_cats.split(",") if c.strip()] or None

    if not content and not memory_type and not categories:
        return ToolResult.text(
            "Nothing to update — provide content, memory_type, or categories."
        )

    if not ctx.memory_bridge or not ctx.memory_bridge.available:
        return ToolResult.text("Memory service not available.")

    try:
        success = await ctx.memory_bridge.update_item(
            memory_id=memory_id,
            content=content,
            memory_type=memory_type,
            categories=categories,
            source="agent_tool",
        )
        if success:
            return ToolResult.text(f"Memory {memory_id} updated.")
        return ToolResult.text(f"Failed to update memory {memory_id}.")
    except Exception as e:
        logger.error("memory_update failed: %s", e)
        return ToolResult.text(f"Error: {e}")


async def memory_delete_handler(ctx: ToolContext, args: dict) -> ToolResult:
    memory_id = args["memory_id"]

    if not ctx.memory_bridge or not ctx.memory_bridge.available:
        return ToolResult.text("Memory service not available.")

    try:
        success = await ctx.memory_bridge.delete_item(memory_id=memory_id, source="agent_tool")
        if success:
            return ToolResult.text(f"Memory {memory_id} deleted.")
        return ToolResult.text(f"Failed to delete memory {memory_id}.")
    except Exception as e:
        logger.error("memory_delete failed: %s", e)
        return ToolResult.text(f"Error: {e}")


async def category_update_handler(ctx: ToolContext, args: dict) -> ToolResult:
    category_id = args["category_id"]
    summary = args.get("summary", "") or None
    description = args.get("description", "") or None

    if not summary and not description:
        return ToolResult.text("Nothing to update — provide summary or description.")

    if not ctx.memory_bridge or not ctx.memory_bridge.available:
        return ToolResult.text("Memory service not available.")

    try:
        success = await ctx.memory_bridge.update_category(
            category_id=category_id,
            summary=summary,
            description=description,
            source="agent_tool",
        )
        if success:
            return ToolResult.text(f"Category {category_id} updated and re-embedded.")
        return ToolResult.text(f"Failed to update category {category_id} (not found?).")
    except Exception as e:
        logger.error("category_update failed: %s", e)
        return ToolResult.text(f"Error: {e}")


MEMORY_RECALL_SPEC = ToolSpec(
    name="memory_recall",
    description="Recall relevant memories via semantic search (memU). Returns memories related to the query.",
    input_schema=MEMORY_RECALL_SCHEMA,
    handler=memory_recall_handler,
)

SESSION_CONTEXT_SPEC = ToolSpec(
    name="session_context",
    description=(
        "Return the dynamic startup context: recalled memU priors biased "
        "by the supplied topic, a summary of currently active skills, and "
        "session metadata (id, source, current time, workspace). External "
        "MCP clients (Codex, Claude Code) should call this as their first "
        "action in a fresh thread — it gives them parity with Nerve-owned "
        "sessions, which receive the same context via system-prompt "
        "injection."
    ),
    input_schema=SESSION_CONTEXT_SCHEMA,
    handler=session_context_handler,
)

CONVERSATION_HISTORY_SPEC = ToolSpec(
    name="conversation_history",
    description="Get memory items from a specific date or date range. Use for temporal queries like 'what did I do yesterday'.",
    input_schema=CONVERSATION_HISTORY_SCHEMA,
    handler=conversation_history_handler,
)

MEMORY_RECORDS_BY_DATE_SPEC = ToolSpec(
    name="memory_records_by_date",
    description=(
        "List ALL memory records created or updated on a given date (or date range). "
        "Returns every memory type (profile, event, knowledge, behavior) — unlike conversation_history which only returns events.\n\n"
        "Use this for memory maintenance and auditing: 'what records were saved today', 'review everything created yesterday'.\n"
        "Do NOT use this for 'what happened on date X' — use conversation_history for that (it filters by event date, not creation date)."
    ),
    input_schema=MEMORY_RECORDS_BY_DATE_SCHEMA,
    handler=memory_records_by_date_handler,
)

MEMORIZE_SPEC = ToolSpec(
    name="memorize",
    description=(
        "Save an important fact, preference, or instruction to long-term semantic memory (memU).\n\n"
        "Memory types:\n"
        "- profile: Stable personal facts — identity, preferences, relationships, work, living situation. Things that persist over time.\n"
        "- event: Specific occurrences with a date — purchases, meetings, milestones, emails received, tasks completed. Things that happened.\n"
        "- knowledge: Objective factual information — technical concepts, definitions, how things work. Not personal to the user.\n"
        "- behavior: Recurring patterns and routines — how the user solves problems, daily habits, preferred workflows. Must be repeated, not one-time.\n\n"
        "Use when someone says 'remember this' or when you learn something worth keeping."
    ),
    input_schema=MEMORIZE_SCHEMA,
    handler=memorize_handler,
)

MEMORY_UPDATE_SPEC = ToolSpec(
    name="memory_update",
    description="Update an existing memory item in memU. Use when a fact is outdated, needs correction, or should be recategorized.",
    input_schema=MEMORY_UPDATE_SCHEMA,
    handler=memory_update_handler,
)

MEMORY_DELETE_SPEC = ToolSpec(
    name="memory_delete",
    description="Delete a memory item from memU. Use when a memory is wrong, duplicate, or no longer relevant.",
    input_schema=MEMORY_DELETE_SCHEMA,
    handler=memory_delete_handler,
)

CATEGORY_UPDATE_SPEC = ToolSpec(
    name="category_update",
    description="Update a memU category's summary and/or description, then re-embed it. Use after manually editing category summaries to keep embeddings in sync. Get category IDs from memory_recall results (cat:ID format).",
    input_schema=CATEGORY_UPDATE_SCHEMA,
    handler=category_update_handler,
)


MEMORY_SPECS = [
    MEMORY_RECALL_SPEC,
    SESSION_CONTEXT_SPEC,
    CONVERSATION_HISTORY_SPEC,
    MEMORY_RECORDS_BY_DATE_SPEC,
    MEMORIZE_SPEC,
    MEMORY_UPDATE_SPEC,
    MEMORY_DELETE_SPEC,
    CATEGORY_UPDATE_SPEC,
]
