"""V65: where each session comes from.

Adds two nullable columns to ``sessions`` (see ``nerve/session_origin.py``):

- ``trigger``: the kind of event that started the session, from a closed
  vocabulary (``chat``, ``api``, ``slack``, ``cron``, ``plan`` and others).
  The code path that creates the session writes it once.
- ``origin_detail``: a JSON object with the details for display, for example
  the Slack channel or the cron job.

There is no backfill. A row with a NULL ``trigger`` gets a derived origin
when it is read. The column check makes a second run do nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

_COLUMNS = (("trigger", "TEXT"), ("origin_detail", "TEXT"))


async def up(db: aiosqlite.Connection) -> None:
    async with db.execute("PRAGMA table_info(sessions)") as cursor:
        present = {row[1] for row in await cursor.fetchall()}
    for name, kind in _COLUMNS:
        if name not in present:
            await db.execute(f"ALTER TABLE sessions ADD COLUMN {name} {kind}")
    logger.info("v065: sessions.trigger and sessions.origin_detail present")
