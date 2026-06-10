"""V33: Link cron run logs to their agent sessions.

Adds ``cron_logs.session_id`` so each run can be traced to the session it
executed in (``cron:<job>`` for persistent jobs, ``cron:<job>:<run>`` for
isolated per-run sessions). The web UI uses this to deep-link from a cron
run row to its chat page. Source-runner logs keep NULL — they have no
agent session.
"""

from __future__ import annotations

import aiosqlite


async def up(db: aiosqlite.Connection) -> None:
    await db.execute("ALTER TABLE cron_logs ADD COLUMN session_id TEXT")
