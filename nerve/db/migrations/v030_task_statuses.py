"""V30: Configurable task statuses.

Adds a ``task_statuses`` table holding the set of statuses a task may
take, each with a display label, color (hex), optional description, a
``sort_order`` for stable ordering, and an ``is_system`` flag.

Two statuses carry special semantics and are seeded as ``is_system=1``
(protected — cannot be deleted):

- ``pending``  — the default status assigned to new tasks.
- ``done``     — the terminal status; ``task_done`` moves the task file
                 into ``done/`` and these tasks are hidden from the
                 default ("active") list.

The other two previously-hardcoded statuses (``in_progress``,
``deferred``) are seeded as ordinary, deletable statuses.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


# name, label, color, description, is_system, sort_order
_SEED = [
    ("pending", "Pending", "#eab308",
     "Not started yet — the default status for new tasks.", 1, 0),
    ("in_progress", "In Progress", "#3b82f6",
     "Actively being worked on.", 0, 1),
    ("done", "Done", "#10b981",
     "Completed. The task file is moved to the done/ folder.", 1, 2),
    ("deferred", "Deferred", "#6b7280",
     "Postponed — not active right now but not dropped.", 0, 3),
]


async def up(db: aiosqlite.Connection) -> None:
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS task_statuses (
            name        TEXT PRIMARY KEY,
            label       TEXT NOT NULL,
            color       TEXT NOT NULL DEFAULT '#6b7280',
            description TEXT NOT NULL DEFAULT '',
            is_system   INTEGER NOT NULL DEFAULT 0,
            sort_order  INTEGER NOT NULL DEFAULT 0,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    for name, label, color, description, is_system, sort_order in _SEED:
        await db.execute(
            """INSERT OR IGNORE INTO task_statuses
                   (name, label, color, description, is_system, sort_order)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (name, label, color, description, is_system, sort_order),
        )
    logger.info("v030: created task_statuses table and seeded default statuses")
