"""V33: Fix the tasks_fts join-key format — unify on the raw task_id.

A regression made ``upsert_task`` store the FTS join key as a space-normalized
slug (``task_id`` with hyphens replaced by spaces) while the reseed path and the
schema migrations kept storing the raw ``id``. The readers joined via
``f.task_id = REPLACE(t.id, '-', ' ')`` (space form), so only the handful of
tasks written *since* the regression were reachable — the rest were stranded in
raw-id form. In practice ~96% of tasks became invisible to FTS search
(``search_tasks`` strategy 2) and to dedup (``search_tasks_similar``, which has
no LIKE fallback). The space double-write also left duplicate rows, so the FTS
row count drifted above the task count and forced a reseed on every startup.

The fix unifies everyone on the raw ``id`` and joins on ``f.task_id = t.id``.
The FTS5 tokenizer still splits the hyphenated slug into individual words
(``2026-03-10-distribution`` → ``2026``, ``03``, ``10``, ``distribution``), so
slug search is fully preserved.

This migration clears ``tasks_fts`` so the startup FTS integrity check
(``Database._check_fts_integrity``) rebuilds it from disk with the correct
raw-id key and real content. Clearing also drops the stale mixed-format and
duplicate rows accumulated by the old double-write, reconciling the row count.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    # Empty the index; the row count now differs from the task count, so
    # _check_fts_integrity() reseeds on this same connect() with the correct
    # raw-id key and content read from the configured workspace.
    await db.execute("DELETE FROM tasks_fts")
    logger.info("V32 migration: cleared tasks_fts for raw-id rebuild on startup")
