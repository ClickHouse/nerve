"""V60: the state of the onboarding.

``onboarding`` holds at most one row (``id = 1``). No row means that
onboarding has not started. ``kind`` is how the agent is set up:

- ``conversation``: in a web chat with a person (the setup page's Finish).
- ``autonomous``: in one session without a person, from the worker's task
  description (``nerve init --terminal`` or ``--non-interactive``).

``status`` is one of:

- ``requested``: the next start of the daemon opens the onboarding. Setup
  finished while a restart was pending, or ``nerve init`` set up a worker.
- ``running``: the onboarding runs in the session ``session_id``.
- ``done``: the agent applied the setup and marked onboarding done.
- ``skipped``: a person chose to skip onboarding.

``actor_id`` is the person who made the last change: who finished setup for
``requested`` and ``running`` (NULL after ``nerve init``), who skipped for
``skipped``, the system actor for ``done``. The foreign key follows v048.
Idempotent.
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
    updated_at TEXT NOT NULL,
    kind       TEXT NOT NULL DEFAULT 'conversation'
);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    logger.info("v060: onboarding table present")
