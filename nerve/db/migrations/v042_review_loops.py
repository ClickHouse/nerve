"""Review loops — deterministic implement→verify cycles over workflow runs.

A *review loop* drives two alternating budget-capped workflow runs — an
implementer leg working toward a Goal prompt, and a verifier leg judging
the workspace against Verifier criteria — until the criteria pass or a
cap (iterations, dollars, no-progress) fires. The loop controller is a
server-side state machine (``nerve.workflows.review_loop``); agents only
produce artifacts and verdicts.

``review_loops`` is the loop state machine row. ``review_loop_attempts``
is the append-only dispatch ledger/outbox: an attempt row is written in
the same transaction as the loop-state CAS *before* the workflow run is
started (with a pre-generated run id), so a crash can never leave a paid
leg unlinked or a loop state with no child. Retries and verifier
schema-repair reruns are additional attempt rows for the same
``(loop_id, iteration, role)``.

``session_id`` references the observer session with ON DELETE SET NULL —
loop history survives session deletion.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS review_loops (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    session_id TEXT REFERENCES sessions(id) ON DELETE SET NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    failure_reason TEXT,
    goal_prompt TEXT NOT NULL,
    verifier_prompt TEXT NOT NULL,
    criteria_adoption TEXT NOT NULL DEFAULT 'no',
    criteria JSON NOT NULL DEFAULT '[]',
    implementer JSON NOT NULL DEFAULT '{}',
    verifier JSON NOT NULL DEFAULT '{}',
    cwd TEXT,
    max_iterations INTEGER NOT NULL DEFAULT 3,
    budget_usd REAL NOT NULL DEFAULT 0.0,
    spent_usd REAL NOT NULL DEFAULT 0.0,
    iteration INTEGER NOT NULL DEFAULT 0,
    current_run_id TEXT,
    escalation_count INTEGER NOT NULL DEFAULT 0,
    discovery_rounds INTEGER NOT NULL DEFAULT 0,
    warned_at TEXT,
    created_by TEXT NOT NULL DEFAULT 'user',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    ended_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_review_loops_status
    ON review_loops(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_review_loops_session
    ON review_loops(session_id);

CREATE TABLE IF NOT EXISTS review_loop_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    loop_id TEXT NOT NULL REFERENCES review_loops(id) ON DELETE CASCADE,
    iteration INTEGER NOT NULL,
    role TEXT NOT NULL,
    attempt_no INTEGER NOT NULL DEFAULT 1,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'dispatching',
    spend_usd REAL NOT NULL DEFAULT 0.0,
    verdict JSON,
    detail JSON,
    created_at TEXT NOT NULL,
    settled_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_review_loop_attempts_loop
    ON review_loop_attempts(loop_id, iteration, role, attempt_no);
CREATE UNIQUE INDEX IF NOT EXISTS uq_review_loop_attempts_run
    ON review_loop_attempts(run_id);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    logger.info("v042: review_loops + review_loop_attempts tables created")
