"""V45: a per-row revision token for tasks.

The task tools read a row, edit the markdown, then write the row back from
that read, and ``upsert_task`` replaced the row unconditionally. A writer
landing in between was silently undone: a completion could revert to its old
status with ``file_path`` pointing back into active/.

``revision`` starts at 1 and every write to the row advances it by one, so a
caller can pass the value it read as ``expect_revision`` and have the write
refused once the row has moved on. ``updated_at`` cannot serve: it is a
wall-clock value rather than a counter, and not every write advances it.

``NOT NULL DEFAULT 1`` fills existing rows, so there is no backfill.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.execute("ALTER TABLE tasks ADD COLUMN revision INTEGER NOT NULL DEFAULT 1")
    logger.info("v045: added tasks.revision")
