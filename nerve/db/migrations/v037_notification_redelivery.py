"""V37: Add re-delivery columns to notifications.

Completes the snooze lifecycle for ``question``/``approval`` rows. Until
now, snoozing only advanced ``expires_at`` — the row sat pending and
invisible until the expiry sweep silently killed it ("delayed silent
decline"). The two new columns let the periodic maintenance tick
re-surface snoozed rows with a fresh fanout:

- ``redeliver_at`` TEXT NULL: when set on a ``pending`` row, the
  maintenance tick re-fans-out the notification at/after this time
  (fresh Telegram card + web broadcast). NULL = no re-delivery queued.
- ``redelivery_count`` INTEGER NOT NULL DEFAULT 0: how many times the
  row has been re-delivered. Capped by
  ``config.notifications.max_redeliveries`` so a snooze loop can't nag
  forever — at the cap the row expires (with reporting) instead.

Existing rows are untouched (redeliver_at = NULL, count = 0), so
nothing is re-delivered retroactively.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.execute(
        "ALTER TABLE notifications ADD COLUMN redeliver_at TEXT"
    )
    await db.execute(
        "ALTER TABLE notifications ADD COLUMN redelivery_count "
        "INTEGER NOT NULL DEFAULT 0"
    )
    # The maintenance tick polls "pending rows whose redeliver_at is due"
    # every 15 minutes; keep that scan off the table.
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_notifications_redeliver "
        "ON notifications(status, redeliver_at)"
    )
    logger.info(
        "v037: added redeliver_at/redelivery_count to notifications + index"
    )
