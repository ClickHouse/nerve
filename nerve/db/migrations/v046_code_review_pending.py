"""V46: draft/pending flag on code-review comments.

Human review comments are staged as *pending* (drafts) until the reviewer hits
"Submit review", at which point the whole batch is delivered to the target
session as a single turn — instead of one turn per comment. This adds the
column that tracks that state. Additive + idempotent.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def _columns(db: aiosqlite.Connection, table: str) -> set[str]:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        return {str(row[1]) for row in await cursor.fetchall()}


async def up(db: aiosqlite.Connection) -> None:
    cols = await _columns(db, "code_review_comments")
    if "pending" not in cols:
        await db.execute(
            "ALTER TABLE code_review_comments ADD COLUMN pending INTEGER NOT NULL DEFAULT 0"
        )
    logger.info("v046: added code_review_comments.pending")
