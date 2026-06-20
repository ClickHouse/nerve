"""V32: Session wakeups (ScheduleWakeup harness).

Adds a ``session_wakeups`` table that records self-scheduled wakeups
requested by the model via the ``ScheduleWakeup`` tool.

Background: the bundled Claude CLI fires ``ScheduleWakeup`` autonomously
inside its own subprocess, but Nerve only reads the SDK message stream
during an active ``engine.run()`` — so an autonomously-fired wakeup turn
lands in an unread buffer and then desyncs the next real turn. The fix
disables the CLI's own firing (``CLAUDE_CODE_DISABLE_CRON=1``) and makes
Nerve the harness: a PostToolUse hook records the request here, and a
periodic sweep fires it via ``engine.run(..., source="wakeup")``.

One-shot only. ``ScheduleWakeup`` is always a one-shot wakeup; if the
model wants to keep a loop alive it re-calls the tool each turn, which
re-inserts a fresh row. Recurring crons are intentionally NOT supported
here — Nerve has its own cron system and the CLI cron tools are removed.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS session_wakeups (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id  TEXT NOT NULL
                            REFERENCES sessions(id) ON DELETE CASCADE,
            prompt      TEXT NOT NULL,
            reason      TEXT NOT NULL DEFAULT '',
            fire_at     TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'pending',
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    # Sweep query is WHERE status = 'pending' AND fire_at <= ?
    await db.execute(
        """CREATE INDEX IF NOT EXISTS idx_session_wakeups_due
               ON session_wakeups (status, fire_at)"""
    )
    # Per-session de-dup / cascade lookups.
    await db.execute(
        """CREATE INDEX IF NOT EXISTS idx_session_wakeups_session
               ON session_wakeups (session_id)"""
    )
    logger.info("v031: created session_wakeups table")
