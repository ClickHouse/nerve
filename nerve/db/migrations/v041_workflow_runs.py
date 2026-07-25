"""Workflow runs — budget-capped, tracked, killable multi-agent jobs.

A *workflow run* is a first-class orchestration job: a dedicated agent
session (Claude harness Workflow tool, or Codex Ultracode) wrapped in a
dollar budget enforced from Nerve's own usage metering, with scoped
lifecycle (kill affects only this run) and a durable journal directory.

``session_id`` references sessions(id) with ON DELETE SET NULL so run
history survives session deletion (the journal dir keeps the details).

Timestamps are TEXT written Python-side with ``utc_now_iso()`` — fixed
width UTC ISO strings so lexicographic order == chronological order.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS workflow_runs (
    id TEXT PRIMARY KEY,
    engine TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    spec JSON NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    budget_usd REAL,
    spent_usd REAL NOT NULL DEFAULT 0.0,
    warned_at TEXT,
    session_id TEXT REFERENCES sessions(id) ON DELETE SET NULL,
    journal_dir TEXT,
    created_by TEXT NOT NULL DEFAULT 'user',
    error TEXT,
    result TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_status
    ON workflow_runs(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_session
    ON workflow_runs(session_id);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    logger.info("v041: workflow_runs table created")
