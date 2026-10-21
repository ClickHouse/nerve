"""V49: record when the installation's setup was completed.

``instance_setup`` holds at most one row, and the row means setup is complete.
Without a password and without the row, only the setup-token claim is
permitted. With the row and no password, the installation is passwordless by
choice. A password also completes setup, so it does not need the row.

An installation from before accounts has sessions and no accounts when this
runs, and it is marked complete: no password was its deliberate "dev mode"
choice. A new database has no sessions. A database that already has an account
keeps a passwordless account unclaimed.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS instance_setup (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    completed_at TEXT NOT NULL
);
"""


async def _any_row(db: aiosqlite.Connection, sql: str) -> bool:
    async with db.execute(sql) as cursor:
        return await cursor.fetchone() is not None


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    if await _any_row(db, "SELECT 1 FROM instance_setup"):
        return
    if await _any_row(db, "SELECT 1 FROM accounts LIMIT 1"):
        return
    if await _any_row(db, "SELECT 1 FROM sessions LIMIT 1"):
        await db.execute(
            "INSERT INTO instance_setup (id, completed_at) VALUES (1, ?)",
            (datetime.now(timezone.utc).isoformat(),),
        )
        logger.info("v049: installation from before accounts marked as set up")
