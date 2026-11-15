"""V62: a short summary on each plan.

Adds ``plans.summary``: the optional summary that the agent gives to
``plan_propose`` or ``plan_update``, at most ``PLAN_SUMMARY_MAX_CHARS``
characters (``nerve/agent/tools/handlers/plans.py``). The plan page shows it
under the title. NULL means that the agent gave no summary. Then the page
shows the first lines of the plan. The column check makes a second run do
nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def _has_column(db: aiosqlite.Connection, table: str, column: str) -> bool:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        rows = await cursor.fetchall()
    return any(row[1] == column for row in rows)


async def up(db: aiosqlite.Connection) -> None:
    if not await _has_column(db, "plans", "summary"):
        await db.execute("ALTER TABLE plans ADD COLUMN summary TEXT")
    logger.info("v062: plans.summary present")
