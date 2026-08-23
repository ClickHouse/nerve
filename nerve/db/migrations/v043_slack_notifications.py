"""V43: Slack delivery ids on notifications."""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

COLUMNS = (
    ("slack_message_id", "TEXT"),
    ("slack_channel_id", "TEXT"),
)


async def up(db: aiosqlite.Connection) -> None:
    cursor = await db.execute("PRAGMA table_info(notifications)")
    existing = {row[1] for row in await cursor.fetchall()}
    for name, decl in COLUMNS:
        if name in existing:
            continue
        await db.execute(
            f"ALTER TABLE notifications ADD COLUMN {name} {decl}",
        )
    logger.info("V43 migration: notifications carries Slack delivery ids")
