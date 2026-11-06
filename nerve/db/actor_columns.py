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
    ("session_events", "actor_id"),
    ("session_events", "caused_by_actor_id"),
    ("session_deletions", "actor_id"),
    ("session_deletions", "caused_by_actor_id"),
    ("notifications", "answered_by_actor_id"),
    ("notifications", "dismissed_by_actor_id"),
    ("notification_silences", "created_by_actor_id"),
    ("notification_silences", "caused_by_actor_id"),
    ("plans", "reviewed_by_actor_id"),
    ("plans", "reviewed_caused_by_actor_id"),
    ("plans", "created_by_actor_id"),
    ("plans", "caused_by_actor_id"),
    ("workflow_runs", "created_by_actor_id"),
    ("workflow_runs", "caused_by_actor_id"),
    ("workflow_runs", "killed_by_actor_id"),
    ("workflow_runs", "killed_caused_by_actor_id"),
    ("review_loops", "created_by_actor_id"),
    ("review_loops", "caused_by_actor_id"),
    ("review_loops", "killed_by_actor_id"),
    ("review_loops", "killed_caused_by_actor_id"),
    ("review_loop_decisions", "actor_id"),
    ("review_loop_decisions", "caused_by_actor_id"),
    ("session_wakeups", "created_by_actor_id"),
    ("session_wakeups", "caused_by_actor_id"),
    ("cron_logs", "actor_id"),
    ("cron_logs", "caused_by_actor_id"),
    ("session_agents", "stopped_by_actor_id"),
    ("session_agents", "stopped_caused_by_actor_id"),
    ("onboarding", "actor_id"),
    ("task_comments", "actor_id"),
    ("task_comments", "caused_by_actor_id"),
)
