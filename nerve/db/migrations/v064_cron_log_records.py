"""V64: Count the records of each source run.

Adds ``cron_logs.records``: the number of records that a source run fetched
and kept, that is stored in the inbox plus dropped by the inbox guardrail. A
source run with ``records = 0`` and status ``success`` found nothing new. The
Cron page hides these runs by default (``GET /api/cron/logs?hide_empty=1``).

The column stays NULL for agent and workflow runs and for missed runs. For
the source runs that are already in the table, the count comes from their
output: "<N> ingested", then ", <M> dropped by guardrail" when the run
dropped records. The column check makes a second run do nothing.
"""

from __future__ import annotations

import aiosqlite


async def _has_column(db: aiosqlite.Connection, table: str, column: str) -> bool:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        rows = await cursor.fetchall()
    return any(row[1] == column for row in rows)


async def up(db: aiosqlite.Connection) -> None:
    if await _has_column(db, "cron_logs", "records"):
        return
    await db.execute("ALTER TABLE cron_logs ADD COLUMN records INTEGER")
    # CAST takes the leading integer of the text: "12 ingested, ..." -> 12,
    # and "3 dropped by guardrail" after " ingested, " (11 characters) -> 3.
    await db.execute(
        "UPDATE cron_logs SET records = CAST(output AS INTEGER) + CASE "
        "WHEN output GLOB '* ingested, [0-9]* dropped by guardrail*' "
        "THEN CAST(substr(output, instr(output, ' ingested, ') + 11) AS INTEGER) "
        "ELSE 0 END "
        "WHERE job_id LIKE 'source:%' AND output GLOB '[0-9]* ingested*'"
    )
