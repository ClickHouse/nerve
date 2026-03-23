"""V17: Add starred column to sessions table."""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.execute(
        "ALTER TABLE sessions ADD COLUMN starred BOOLEAN NOT NULL DEFAULT 0"
    )
    logger.info("V17 migration: added sessions.starred column")
