"""V68: the Web Push subscriptions of the panel.

Adds the table ``push_subscriptions``. One row is one browser that gets the
panel's push notifications:

- ``actor_id``: the person who turned push on in that browser. A person
  sees and removes only their own rows.
- ``endpoint``: the URL of the browser's push service for this browser. It
  is unique: when a different person turns push on in the same browser, the
  row moves to that person.
- ``p256dh`` and ``auth``: the browser's keys that encrypt each payload
  (base64url).
- ``level``: ``all`` or ``important``, the same values as
  ``notifications.levels``. The browser sends the level it uses for its
  alerts.
- ``user_agent``: the browser's ``User-Agent`` header, cut, so the person can
  tell their devices apart.
- ``created_at``: the time the person turned push on. ``last_success_at``:
  the last time a push service took a notification for the row (NULL
  before the first one).

The foreign key follows v048. ``CREATE ... IF NOT EXISTS`` makes a second
run do nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS push_subscriptions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_id        TEXT NOT NULL REFERENCES actor_refs(id),
    endpoint        TEXT NOT NULL UNIQUE,
    p256dh          TEXT NOT NULL,
    auth            TEXT NOT NULL,
    level           TEXT NOT NULL,
    user_agent      TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL,
    last_success_at TEXT
);

-- The list of one person's devices.
CREATE INDEX IF NOT EXISTS idx_push_subscriptions_actor
    ON push_subscriptions(actor_id);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    logger.info("v068: push_subscriptions ready")
