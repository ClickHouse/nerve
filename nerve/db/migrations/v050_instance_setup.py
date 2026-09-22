"""V50: record when the installation's setup was completed.

Setup belongs to the installation, and passwords belong to accounts. An
installation with no password is in one of two states:

* setup is not complete: only the setup-token claim is permitted;
* setup is complete: the operator chose a passwordless installation, and the
  sole account is admitted without a password.

``instance_setup`` holds at most one row. Its presence means setup is complete.
An account credential also means setup is complete, so an installation with a
password does not need the row.

An installation from before accounts existed is marked complete. There, no
password was the deliberate "dev mode" choice, and an upgrade must not lock it
behind a setup token. Such a database has sessions and no accounts when this
migration runs, because the owner account is created after the migrations. A
new database has no sessions. A database that already has accounts keeps its
passwordless account unclaimed: that is how it was created, or a backup without
secrets removed its credentials.
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
        logger.info("v050: installation from before accounts marked as set up")
