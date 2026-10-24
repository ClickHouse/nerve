"""V59: the built-in task statuses ``backlog`` and ``plan_review``.

- ``backlog``: a task that is not ready for work. No built-in cron job or
  gate selects it, so the agent does not pick it up. The New Task dialog of
  the web UI uses it as its default. It is an ordinary status and can be
  deleted.
- ``plan_review``: a plan for the task waits for a human decision.
  ``plan_propose`` and ``plan_update`` move a ``pending``, ``backlog`` or
  ``in_progress`` task to it. It is protected (``is_system=1``), because the
  plan tools depend on it.

``backlog`` goes immediately before ``pending`` and ``plan_review``
immediately after it. The other statuses keep their relative order, and
``sort_order`` is written again from 0.

A status that already has one of these names is kept as it is, with its
label, color, description, protection and position.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

# name, label, color, description, is_system, offset from ``pending``
_NEW = (
    ("backlog", "Backlog", "#94a3b8",
     "Not ready for work yet. No built-in job picks it up.", 0, 0),
    ("plan_review", "Plan in review", "#8b5cf6",
     "A plan for this task waits for a human decision.", 1, 1),
)


async def up(db: aiosqlite.Connection) -> None:
    async with db.execute(
        "SELECT name FROM task_statuses ORDER BY sort_order ASC, name ASC"
    ) as cursor:
        order = [row[0] async for row in cursor]

    added = []
    for name, label, color, description, is_system, offset in _NEW:
        if name in order:
            continue
        await db.execute(
            """INSERT INTO task_statuses
                   (name, label, color, description, is_system, sort_order)
               VALUES (?, ?, ?, ?, ?, 0)""",
            (name, label, color, description, is_system),
        )
        anchor = order.index("pending") + offset if "pending" in order else len(order)
        order.insert(anchor, name)
        added.append(name)

    if added:
        await db.executemany(
            "UPDATE task_statuses SET sort_order = ? WHERE name = ?",
            [(i, name) for i, name in enumerate(order)],
        )
    logger.info(
        "v059: task statuses %s",
        "added " + ", ".join(added) if added else "already present",
    )
