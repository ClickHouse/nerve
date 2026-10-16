"""V55: record who answers or dismisses a notification, creates a silence, or writes or reviews a plan.

Adds eight nullable columns that hold an ``actor_refs.id``:

- ``notifications.answered_by_actor_id``: the person who answered a question
  or decided an approval through the web UI or REST. An answer through a chat
  channel (Telegram, Slack, a hosted channel) names no person and keeps it
  ``NULL``; its provider user ID stays in ``metadata.answered_by_actor``.
  On an approval that is snoozed and still pending, it names the person who
  snoozed it, until a later decision replaces it.
- ``notifications.dismissed_by_actor_id``: the person who dismissed the row.
- ``notification_silences.created_by_actor_id``: who created the rule. A
  person through the web UI or REST, or the system actor for the
  ``notification_silence`` tool.
- ``notification_silences.caused_by_actor_id``: on whose behalf the rule was
  created: the person, or the cause of the turn that called the tool.
- ``plans.created_by_actor_id``: who wrote this plan version. The system
  actor for ``plan_propose`` and ``plan_update``; each version written by
  ``plan_update`` carries its own value.
- ``plans.caused_by_actor_id``: on whose behalf the version was written: the
  cause of the turn that called the tool.
- ``plans.reviewed_by_actor_id``: who made the last review decision on the
  plan (approve, decline, a revision request, or a status or feedback change
  through REST). A person through the web UI or REST, or the system actor for
  a plan tool.
- ``plans.reviewed_caused_by_actor_id``: on whose behalf that decision was
  made.

The free-text columns stay and are written as before. ``answered_by`` is the
transport, ``notification_silences.created_by`` is the origin (``web`` or a
session ID).

**Existing rows stay NULL and nothing is backfilled.** NULL reads as "not
recorded". The free-text columns of an old row do not name a person, so a
backfill would invent attribution.

The foreign keys follow v048: an ID that resolves to no actor can never be
stored, and NULL is exempt from the check. Each column is added only when the
table does not have it yet, so a second run does nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

# (table, column, DDL type + constraints)
_COLUMNS = (
    ("notifications", "answered_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("notifications", "dismissed_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("notification_silences", "created_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("notification_silences", "caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("plans", "reviewed_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("plans", "reviewed_caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("plans", "created_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("plans", "caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
)


async def _has_column(db: aiosqlite.Connection, table: str, column: str) -> bool:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        rows = await cursor.fetchall()
    return any(row[1] == column for row in rows)


async def up(db: aiosqlite.Connection) -> None:
    added = []
    for table, column, ddl in _COLUMNS:
        if await _has_column(db, table, column):
            continue
        # SQLite allows a REFERENCES clause on ADD COLUMN when the default is
        # NULL, so the table is not rebuilt and every existing row is NULL.
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
        added.append(f"{table}.{column}")
    logger.info(
        "v055: notification, silence and plan attribution columns %s",
        ", ".join(added) if added else "already present",
    )
