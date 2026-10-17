"""Every attribution column that stores an ``actor_refs`` ID.

One ``(table, column)`` pair per line. A migration that adds an attribution
column appends its pair at the end. The tests compare this list with the
schema: each pair must be a column with a foreign key to ``actor_refs``, and
each such column must be in the list, except ``accounts.actor_id``, which
binds a login to its person and is not attribution.

``GET /api/actors`` must return every actor that one of these columns names.
"""

from __future__ import annotations

ACTOR_COLUMNS: tuple[tuple[str, str], ...] = (
    ("sessions", "created_by_actor_id"),
    ("messages", "actor_id"),
    ("task_events", "actor_id"),
    ("task_events", "caused_by_actor_id"),
    ("sessions", "caused_by_actor_id"),
    ("task_status_events", "actor_id"),
    ("task_status_events", "caused_by_actor_id"),
)
