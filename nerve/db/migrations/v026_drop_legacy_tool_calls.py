"""V26: Backfill ``messages.blocks`` for legacy rows and drop ``tool_calls``.

V8 (``v008_message_blocks``) introduced ``messages.blocks`` as the
canonical interleaved representation of an assistant turn (thinking +
text + tool_call entries in original order). ``messages.tool_calls``
was kept around as a fallback for rows written before V8 — but the
write path was never updated, so every assistant message since V8
has been writing **both** columns with overlapping data.

The frontend (``web/src/utils/hydrateMessage.ts``) only consults
``tool_calls`` when ``blocks`` is missing. No backend code reads
``tool_calls`` at all (memorization uses only role/content/created_at).

This migration:
1. Reconstructs ``blocks`` for any row that has ``tool_calls`` but no
   ``blocks`` (the only group whose UI rendering currently depends on
   ``tool_calls``). The reconstruction follows the same ordering the
   frontend fallback used: thinking → tool_call entries → text.
   This loses interleaving for these old rows — but they were already
   rendered without interleaving, so we're matching prior UX exactly.
2. Drops the ``tool_calls`` column. SQLite has supported
   ``ALTER TABLE ... DROP COLUMN`` since 3.35 (2021), and Nerve already
   targets a recent SQLite via aiosqlite/Python ≥ 3.12.

After this migration the column is gone — callers must not pass a
``tool_calls=`` kwarg to ``Database.add_message`` anymore, and the
frontend hydration fallback can be deleted.
"""

from __future__ import annotations

import json
import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    # If the column is already gone (e.g. migration re-run on a DB built
    # from a newer baseline), there's nothing to do.
    async with db.execute("PRAGMA table_info(messages)") as cur:
        columns = {row[1] for row in await cur.fetchall()}
    if "tool_calls" not in columns:
        logger.info("V26: tool_calls column already absent, skipping")
        return

    # --- 1. Backfill blocks for legacy rows -----------------------------
    async with db.execute(
        "SELECT id, thinking, content, tool_calls FROM messages "
        "WHERE tool_calls IS NOT NULL AND blocks IS NULL"
    ) as cur:
        legacy_rows = await cur.fetchall()

    backfilled = 0
    for row_id, thinking, content, tool_calls_json in legacy_rows:
        try:
            tcs = json.loads(tool_calls_json) if tool_calls_json else []
        except (TypeError, json.JSONDecodeError):
            logger.warning(
                "V26: row %s has unparseable tool_calls JSON, skipping backfill",
                row_id,
            )
            continue

        blocks: list[dict] = []
        if thinking:
            blocks.append({"type": "thinking", "content": thinking})
        if isinstance(tcs, list):
            for tc in tcs:
                if isinstance(tc, dict):
                    # Stored block shape is {"type": "tool_call", **tc_dict}
                    # The legacy tool_calls dict already carries
                    # tool / input / tool_use_id / result / is_error keys.
                    blocks.append({"type": "tool_call", **tc})
        if content:
            blocks.append({"type": "text", "content": content})

        if not blocks:
            continue  # nothing to write — leave the row as-is

        await db.execute(
            "UPDATE messages SET blocks = ? WHERE id = ?",
            (json.dumps(blocks), row_id),
        )
        backfilled += 1

    if legacy_rows:
        logger.info(
            "V26: backfilled blocks for %d/%d legacy messages",
            backfilled, len(legacy_rows),
        )

    # --- 2. Drop the tool_calls column ----------------------------------
    await db.execute("ALTER TABLE messages DROP COLUMN tool_calls")
    await db.commit()
    logger.info("V26 migration: dropped legacy messages.tool_calls column")
