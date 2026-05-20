"""V28: Add external_id to messages for idempotent external ingest.

The Codex thread sync source (``nerve.sources.codex_threads``) and the
external MCP server both write messages into Nerve's ``messages`` table
for the *same* Codex thread. Without a stable de-dupe key, the same
``call_id`` ends up duplicated whenever both paths see it.

This migration adds an opaque ``external_id`` column plus a partial
unique index over ``(session_id, external_id)`` (NULL ignored, so native
Nerve messages — which never set this column — are unaffected). The
ingester uses ``INSERT OR IGNORE`` on this index for idempotent inserts.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    # Check if the column already exists — re-running migrations on an
    # existing DB shouldn't crash (mirrors how v026 handles legacy rows).
    cursor = await db.execute("PRAGMA table_info(messages)")
    cols = {row[1] for row in await cursor.fetchall()}
    if "external_id" not in cols:
        await db.execute("ALTER TABLE messages ADD COLUMN external_id TEXT")
    await db.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_external_id
           ON messages(session_id, external_id)
           WHERE external_id IS NOT NULL"""
    )
    logger.info("v028: added external_id column + partial unique index")
