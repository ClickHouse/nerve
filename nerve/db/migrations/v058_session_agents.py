"""V58: sub-agents and background tasks of a session, with their transcripts.

``session_agents`` holds one row per agent record: a sub-agent, a workflow
run, a background shell or monitor, a teammate, ... that a session's runtime
started. The row keeps the brief (name, description, prompt, command), the
lifecycle (status, end reason, timestamps, who stopped it) and the progress
(activity, summary, tokens, live cost estimate). ``rev`` is a microsecond
revision that keeps increasing across restarts; clients keep the highest.

``session_agent_events`` holds each agent's transcript items (text, thinking,
tool calls and results, lifecycle), numbered per agent by ``seq`` from 1 with
no gaps. Items are capped in size when written. The table is append-only and
follows the opt-in telemetry retention (``nerve db prune``).

Both tables go with their session: ``delete_session`` removes the rows, and
the foreign keys cascade as a second line of defence. The two actor columns
reference ``actor_refs`` like every other attribution column (see
``nerve/db/actor_columns.py``).

Idempotent (``IF NOT EXISTS`` everywhere), so a second run does nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS session_agents (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    backend TEXT NOT NULL DEFAULT 'claude',
    kind TEXT NOT NULL,
    task_id TEXT,
    task_type TEXT,
    spawn_tool_use_id TEXT,
    route_ids TEXT NOT NULL DEFAULT '[]',
    parent_agent_id TEXT,
    depth INTEGER NOT NULL DEFAULT 1,
    name TEXT,
    description TEXT NOT NULL DEFAULT '',
    agent_type TEXT,
    prompt TEXT,
    command TEXT,
    requested_model TEXT,
    models TEXT NOT NULL DEFAULT '[]',
    model_changed_at TEXT,
    background INTEGER NOT NULL DEFAULT 1,
    ambient INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    end_reason TEXT,
    error TEXT,
    activity TEXT,
    summary TEXT,
    result_text TEXT,
    output_file TEXT,
    tokens INTEGER NOT NULL DEFAULT 0,
    tool_uses INTEGER NOT NULL DEFAULT 0,
    usage TEXT NOT NULL DEFAULT '{}',
    cost_usd REAL NOT NULL DEFAULT 0,
    runs INTEGER NOT NULL DEFAULT 1,
    workflow TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    run_started_at TEXT,
    last_activity_at TEXT,
    ended_at TEXT,
    stop_requested_at TEXT,
    stop_scope TEXT,
    stopped_by_actor_id TEXT REFERENCES actor_refs(id),
    stopped_caused_by_actor_id TEXT REFERENCES actor_refs(id),
    orchestrator_notified_at TEXT,
    rev INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_session_agents_task
    ON session_agents(session_id, task_id) WHERE task_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_session_agents_session
    ON session_agents(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_session_agents_live
    ON session_agents(status) WHERE status IN ('starting', 'running', 'stopping');

CREATE TABLE IF NOT EXISTS session_agent_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL REFERENCES session_agents(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (agent_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_session_agent_events_session
    ON session_agent_events(session_id);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    logger.info("v058: session_agents and session_agent_events tables created")
