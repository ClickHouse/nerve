"""V29: Add target_kind / target_id columns to notifications.

Extends the notification table to support the ``approval`` notification
kind: notifications that route to a server-side dispatcher when the user
answers them (e.g. approve / decline / snooze a queued mechanical
action). The existing ``type`` column gains a third valid value
(``approval``); the column itself stays TEXT so no schema change is
needed there.

The two new columns:

- ``target_kind`` TEXT NULL: dispatcher key (e.g. ``mechanical-action``,
  ``plan``). NULL for legacy ``notify`` / ``question`` rows, which means
  "no dispatch; fall through to the existing answer-injection path."
- ``target_id``   TEXT NULL: dispatcher-specific identifier (e.g. the
  mechanical-action proposal id). Read by the handler registry.

Existing rows are left untouched (target_kind = NULL), so the answer
path stays identical for every notification created before v29.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.execute(
        "ALTER TABLE notifications ADD COLUMN target_kind TEXT"
    )
    await db.execute(
        "ALTER TABLE notifications ADD COLUMN target_id TEXT"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_notifications_target "
        "ON notifications(target_kind, target_id)"
    )
    logger.info(
        "v029: added target_kind/target_id to notifications + index"
    )
