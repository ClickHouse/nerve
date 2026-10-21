"""V60: the state of the onboarding conversation.

``onboarding`` holds at most one row (``id = 1``). No row means that
onboarding has not started. ``status`` is one of:

- ``requested``: setup finished while a restart was pending. The next start
  of the daemon opens the conversation.
- ``running``: the conversation is open in the web session ``session_id``.
- ``done``: the agent applied the setup that the person confirmed.
- ``skipped``: a person chose to skip onboarding.

``actor_id`` is the person who made the last change: who finished setup for
``requested`` and ``running``, who skipped for ``skipped``, the system actor
for ``done``. The foreign key follows v048. Idempotent.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS onboarding (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    status     TEXT NOT NULL,
    session_id TEXT,
    actor_id   TEXT REFERENCES actor_refs(id),
    updated_at TEXT NOT NULL
);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    logger.info("v060: onboarding table present")
