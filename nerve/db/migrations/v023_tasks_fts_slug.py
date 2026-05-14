"""V23: Rebuild tasks_fts with task_id as a searchable column.

Previously task_id was marked UNINDEXED, making slug-based search impossible.
The new schema makes task_id searchable so queries like "distribution" match
the slug "2026-03-10-distribution-documentation".
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    # Drop and recreate FTS table with task_id as searchable column
    await db.execute("DROP TABLE IF EXISTS tasks_fts")
    await db.execute(
        "CREATE VIRTUAL TABLE tasks_fts USING fts5(task_id, title, content)"
    )
    # Seed from existing tasks — content will be filled on next reindex
    await db.execute(
        "INSERT INTO tasks_fts (task_id, title, content) "
        "SELECT id, title, '' FROM tasks"
    )
    await db.commit()
    logger.info("V23 migration: rebuilt tasks_fts with searchable task_id column")
