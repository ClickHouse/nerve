"""V36: Notification silences — deterministic suppression of known-benign alerts.

Adds the ``notification_silences`` table: a small set of regex rules that
the notification service consults before fanning a ``notify`` out to
Telegram + web. A matched notification is persisted (``status='silenced'``)
but **not delivered** — priority is never modified. The calling agent is
told its notification was silenced (with the reason + pattern) and can
re-send with ``force=true`` to override a match it judges incorrect.

This is the monitoring-system "silence" pattern: you don't rely on the
on-call engineer *remembering* an alert class is benign — you create a
rule. It sits at the one chokepoint every notification flows through
(``NotificationService.send_notification``), sibling to the source-level
ingestion guardrails.

Columns:

- ``id``               TEXT PK: ``sil-<8hex>``.
- ``pattern``          TEXT: case-insensitive regex, matched against
  ``title + "\\n" + body``.
- ``action``           TEXT: only ``silence`` is implemented; kept for
  audit / forward-compat.
- ``reason``           TEXT: why the rule exists; surfaced to the agent
  on every match.
- ``created_by``       TEXT: session_id that created it.
- ``created_at``       TEXT: ISO-8601 UTC.
- ``expires_at``       TEXT NULL: NULL = permanent (TTL support).
- ``hit_count``        INTEGER: times it silenced a delivery.
- ``last_hit_at``      TEXT NULL.
- ``override_count``   INTEGER: times an agent force-sent over this rule
  (a false-match signal — a climbing count means the pattern is too broad).
- ``last_override_at`` TEXT NULL.
- ``enabled``          INTEGER: 1 = active.

No change to the ``notifications`` table: ``status`` is TEXT, so
``'silenced'`` is just a new value.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS notification_silences (
    id               TEXT PRIMARY KEY,
    pattern          TEXT NOT NULL,
    action           TEXT NOT NULL DEFAULT 'silence',
    reason           TEXT DEFAULT '',
    created_by       TEXT DEFAULT '',
    created_at       TEXT NOT NULL,
    expires_at       TEXT,
    hit_count        INTEGER DEFAULT 0,
    last_hit_at      TEXT,
    override_count   INTEGER DEFAULT 0,
    last_override_at TEXT,
    enabled          INTEGER DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_notification_silences_active
    ON notification_silences(enabled, expires_at);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    logger.info("v035: created notification_silences table + index")
