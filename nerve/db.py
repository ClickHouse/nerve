"""SQLite database layer with async access and auto-migration.

Single DB file stores sessions, messages, tasks, sync state, and cron logs.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator

import aiosqlite

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 16

SCHEMA = """
-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

-- Sessions
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    title TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source TEXT,
    metadata JSON DEFAULT '{}'
);

-- Messages
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    role TEXT NOT NULL,
    content TEXT,
    thinking TEXT,
    tool_calls JSON,
    blocks JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    channel TEXT
);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, created_at);

-- Tasks
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    file_path TEXT NOT NULL,
    title TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    source TEXT,
    source_url TEXT,
    deadline TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    escalation_level INTEGER DEFAULT 0,
    last_reminded_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_deadline ON tasks(deadline);

-- Sync state (cursor-based checkpoints)
CREATE TABLE IF NOT EXISTS sync_cursors (
    source TEXT PRIMARY KEY,
    cursor TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cron logs
CREATE TABLE IF NOT EXISTS cron_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT,
    output TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_cron_logs_job ON cron_logs(job_id, started_at);
"""

SCHEMA_V2 = """
-- memU audit log
CREATE TABLE IF NOT EXISTS memu_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (datetime('now')),
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT,
    source TEXT,
    details TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON memu_audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_log_action ON memu_audit_log(action);
"""

SCHEMA_V3 = """
-- Persistent channel-to-session mapping (survives restarts)
CREATE TABLE IF NOT EXISTS channel_sessions (
    channel_key TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Session lifecycle event log (append-only audit trail)
CREATE TABLE IF NOT EXISTS session_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    event_type TEXT NOT NULL,
    details JSON,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_session_events_session ON session_events(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
CREATE INDEX IF NOT EXISTS idx_sessions_sdk_id ON sessions(sdk_session_id);
CREATE INDEX IF NOT EXISTS idx_sessions_updated ON sessions(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_parent ON sessions(parent_session_id);
"""

SCHEMA_V4 = """
-- Per-source run log for diagnostics
CREATE TABLE IF NOT EXISTS source_run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    ran_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_fetched INTEGER DEFAULT 0,
    records_processed INTEGER DEFAULT 0,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_source_run_log_source ON source_run_log(source, ran_at DESC);
"""

SCHEMA_V5 = """
-- Full-text search index for tasks (title + content)
CREATE VIRTUAL TABLE IF NOT EXISTS tasks_fts USING fts5(task_id UNINDEXED, title, content);
"""

SCHEMA_V6 = """
-- Index for fast source_url dedup lookups
CREATE INDEX IF NOT EXISTS idx_tasks_source_url ON tasks(source_url);
"""

SCHEMA_V7 = """
-- Source messages inbox (persistent storage with TTL)
CREATE TABLE IF NOT EXISTS source_messages (
    id TEXT NOT NULL,
    source TEXT NOT NULL,
    record_type TEXT NOT NULL,
    summary TEXT NOT NULL,
    content TEXT NOT NULL,
    processed_content TEXT,
    timestamp TEXT NOT NULL,
    metadata JSON,
    run_session_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT NOT NULL,
    PRIMARY KEY (source, id)
);
CREATE INDEX IF NOT EXISTS idx_source_messages_ts ON source_messages(source, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_source_messages_expires ON source_messages(expires_at);
CREATE INDEX IF NOT EXISTS idx_source_messages_created ON source_messages(created_at DESC);
"""

# New columns added to sessions table in V3 migration
SCHEMA_V11 = """
-- Consumer cursors for Kafka-like source consumption
CREATE TABLE IF NOT EXISTS consumer_cursors (
    consumer TEXT NOT NULL,
    source TEXT NOT NULL,
    cursor_seq INTEGER NOT NULL DEFAULT 0,
    session_id TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT,
    PRIMARY KEY (consumer, source)
);
"""

SCHEMA_V12 = """
-- Plans proposed by the planner agent for async human review
CREATE TABLE IF NOT EXISTS plans (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    session_id TEXT,
    impl_session_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    content TEXT NOT NULL,
    feedback TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    parent_plan_id TEXT,
    model TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_plans_task ON plans(task_id);
CREATE INDEX IF NOT EXISTS idx_plans_status ON plans(status);
"""

SCHEMA_V13 = """
-- Skills registry (filesystem is source of truth, DB indexes metadata + tracks usage)
CREATE TABLE IF NOT EXISTS skills (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    version TEXT DEFAULT '1.0.0',
    enabled BOOLEAN DEFAULT 1,
    user_invocable BOOLEAN DEFAULT 1,
    model_invocable BOOLEAN DEFAULT 1,
    allowed_tools TEXT,
    metadata JSON DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Skill usage tracking for statistics
CREATE TABLE IF NOT EXISTS skill_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    skill_id TEXT NOT NULL,
    session_id TEXT,
    invoked_by TEXT NOT NULL,
    duration_ms INTEGER,
    success BOOLEAN DEFAULT 1,
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_skill_usage_skill ON skill_usage(skill_id, created_at);
"""

SCHEMA_V14 = """
-- Async notifications and questions
CREATE TABLE IF NOT EXISTS notifications (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    type TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL DEFAULT '',
    priority TEXT NOT NULL DEFAULT 'normal',
    status TEXT NOT NULL DEFAULT 'pending',
    options JSON,
    answer TEXT,
    answered_by TEXT,
    answered_at TIMESTAMP,
    channels_delivered JSON DEFAULT '[]',
    telegram_message_id TEXT,
    telegram_chat_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    metadata JSON DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_notifications_session ON notifications(session_id);
CREATE INDEX IF NOT EXISTS idx_notifications_status ON notifications(status, created_at DESC);
"""

SCHEMA_V15 = """
ALTER TABLE plans ADD COLUMN plan_type TEXT DEFAULT 'generic';
"""

SCHEMA_V16 = """
-- MCP server registry (config is source of truth, DB tracks metadata + usage)
CREATE TABLE IF NOT EXISTS mcp_servers (
    name TEXT PRIMARY KEY,
    type TEXT NOT NULL DEFAULT 'stdio',
    enabled BOOLEAN DEFAULT 1,
    tool_count INTEGER DEFAULT 0,
    first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- MCP tool invocation tracking
CREATE TABLE IF NOT EXISTS mcp_tool_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    server_name TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    session_id TEXT,
    duration_ms INTEGER,
    success BOOLEAN DEFAULT 1,
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_mcp_tool_usage_server ON mcp_tool_usage(server_name, created_at);
CREATE INDEX IF NOT EXISTS idx_mcp_tool_usage_session ON mcp_tool_usage(session_id, created_at);
"""

_V3_SESSION_COLUMNS = [
    ("status", "TEXT NOT NULL DEFAULT 'idle'"),
    ("sdk_session_id", "TEXT"),
    ("parent_session_id", "TEXT"),
    ("forked_from_message", "TEXT"),
    ("connected_at", "TEXT"),
    ("last_activity_at", "TEXT"),
    ("archived_at", "TEXT"),
    ("message_count", "INTEGER DEFAULT 0"),
    ("total_cost_usd", "REAL DEFAULT 0.0"),
    ("last_memorized_at", "TEXT"),
]


class Database:
    """Async SQLite database wrapper."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    async def connect(self) -> None:
        """Open the database connection and apply migrations."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self.db_path))
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._migrate()

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._db

    @asynccontextmanager
    async def _atomic(self) -> AsyncIterator[None]:
        """Acquire write lock for multi-statement transactions.

        Ensures that once a coroutine begins a multi-statement write,
        no other coroutine can interleave writes before the commit.
        """
        async with self._write_lock:
            yield
            await self.db.commit()

    async def _migrate(self) -> None:
        """Apply schema migrations incrementally."""
        try:
            async with self.db.execute("SELECT MAX(version) FROM schema_version") as cursor:
                row = await cursor.fetchone()
                current = row[0] if row and row[0] else 0
        except Exception:
            current = 0

        if current < 1:
            await self.db.executescript(SCHEMA)
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (1,)
            )
            await self.db.commit()

        if current < 2:
            await self.db.executescript(SCHEMA_V2)
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (2,)
            )
            await self.db.commit()

        if current < 3:
            # Add new columns to sessions table (idempotent)
            for col_name, col_def in _V3_SESSION_COLUMNS:
                try:
                    await self.db.execute(
                        f"ALTER TABLE sessions ADD COLUMN {col_name} {col_def}"
                    )
                    logger.info("Added column sessions.%s", col_name)
                except sqlite3.OperationalError as e:
                    if "duplicate column" in str(e).lower():
                        logger.debug("Column sessions.%s already exists", col_name)
                    else:
                        raise

            # Verify all expected columns were added
            expected_cols = {name for name, _ in _V3_SESSION_COLUMNS}
            async with self.db.execute("PRAGMA table_info(sessions)") as cursor:
                actual_cols = {row[1] async for row in cursor}
            missing = expected_cols - actual_cols
            if missing:
                raise RuntimeError(
                    f"V3 migration failed: sessions table missing columns: {missing}"
                )

            # Create new tables and indexes
            await self.db.executescript(SCHEMA_V3)

            # Migrate data from metadata JSON blob to dedicated columns
            async with self.db.execute("SELECT id, metadata FROM sessions") as cursor:
                rows = [dict(row) async for row in cursor]
            for row in rows:
                meta = json.loads(row.get("metadata") or "{}")
                sdk_id = meta.get("sdk_session_id")
                conn_at = meta.get("connected_at")
                if sdk_id or conn_at:
                    sets, params = [], []
                    if sdk_id:
                        sets.append("sdk_session_id = ?")
                        params.append(sdk_id)
                    if conn_at:
                        sets.append("connected_at = ?")
                        params.append(conn_at)
                    params.append(row["id"])
                    await self.db.execute(
                        f"UPDATE sessions SET {', '.join(sets)} WHERE id = ?",
                        tuple(params),
                    )

            # Backfill message counts
            await self.db.execute("""
                UPDATE sessions SET message_count = (
                    SELECT COUNT(*) FROM messages WHERE messages.session_id = sessions.id
                )
            """)

            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (3,)
            )
            await self.db.commit()

        if current < 4:
            # Reset sync cursors — old JSONL line-number cursors are meaningless
            # in the new sources layer. Sources will re-establish proper cursors.
            await self.db.execute("DELETE FROM sync_cursors")
            await self.db.executescript(SCHEMA_V4)
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (4,)
            )
            await self.db.commit()
            logger.info("V4 migration: reset sync cursors, created source_run_log")

        if current < 5:
            await self.db.executescript(SCHEMA_V5)
            # Seed FTS from existing tasks (title only — next reindex fills content)
            await self.db.execute(
                "INSERT INTO tasks_fts (task_id, title, content) SELECT id, title, '' FROM tasks"
            )
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (5,)
            )
            await self.db.commit()
            logger.info("V5 migration: created tasks_fts full-text search index")

        if current < 6:
            await self.db.executescript(SCHEMA_V6)
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (6,)
            )
            await self.db.commit()
            logger.info("V6 migration: added source_url index")

        if current < 7:
            await self.db.executescript(SCHEMA_V7)
            # Add session_id column to source_run_log
            try:
                await self.db.execute(
                    "ALTER TABLE source_run_log ADD COLUMN session_id TEXT"
                )
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (7,)
            )
            await self.db.commit()
            logger.info("V7 migration: created source_messages table, added source_run_log.session_id")

        if current < 8:
            # Add blocks JSON column to messages for ordered block persistence
            try:
                await self.db.execute(
                    "ALTER TABLE messages ADD COLUMN blocks JSON"
                )
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (8,)
            )
            await self.db.commit()
            logger.info("V8 migration: added messages.blocks column")

        if current < 9:
            # Add raw_content column for storing original HTML email bodies
            try:
                await self.db.execute(
                    "ALTER TABLE source_messages ADD COLUMN raw_content TEXT"
                )
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (9,)
            )
            await self.db.commit()
            logger.info("V9 migration: added source_messages.raw_content column")

        if current < 10:
            # File snapshots for session modified files diff view
            await self.db.executescript("""
                CREATE TABLE IF NOT EXISTS session_file_snapshots (
                    session_id TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    original_content TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (session_id, file_path)
                );
                CREATE INDEX IF NOT EXISTS idx_file_snapshots_session
                    ON session_file_snapshots(session_id);
            """)
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (10,)
            )
            await self.db.commit()
            logger.info("V10 migration: created session_file_snapshots table")

        if current < 11:
            await self.db.executescript(SCHEMA_V11)
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (11,)
            )
            await self.db.commit()
            logger.info("V11 migration: created consumer_cursors table")

        if current < 12:
            await self.db.executescript(SCHEMA_V12)
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (12,)
            )
            await self.db.commit()
            logger.info("V12 migration: created plans table")

        if current < 13:
            await self.db.executescript(SCHEMA_V13)
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (13,)
            )
            await self.db.commit()
            logger.info("V13 migration: created skills and skill_usage tables")

        if current < 14:
            await self.db.executescript(SCHEMA_V14)
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (14,)
            )
            await self.db.commit()
            logger.info("V14 migration: created notifications table")

        if current < 15:
            await self.db.executescript(SCHEMA_V15)
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (15,)
            )
            await self.db.commit()
            logger.info("V15 migration: added plan_type column to plans table")

        if current < 16:
            await self.db.executescript(SCHEMA_V16)
            await self.db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (16,)
            )
            await self.db.commit()
            logger.info("V16 migration: created mcp_servers and mcp_tool_usage tables")

        # --- FTS integrity check (runs every startup) ---
        async with self.db.execute("SELECT COUNT(*) FROM tasks") as cur:
            task_count = (await cur.fetchone())[0]
        async with self.db.execute("SELECT COUNT(*) FROM tasks_fts") as cur:
            fts_count = (await cur.fetchone())[0]
        if task_count != fts_count:
            logger.warning(
                "FTS index mismatch: %d tasks vs %d FTS entries — reseeding",
                task_count, fts_count,
            )
            await self.db.execute("DELETE FROM tasks_fts")
            # Read content from disk files (source of truth) instead of seeding empty
            workspace = self.db_path.parent
            async with self.db.execute("SELECT id, title, file_path FROM tasks") as cur:
                rows = await cur.fetchall()
            for row in rows:
                content = ""
                try:
                    fp = workspace / row["file_path"]
                    if fp.exists():
                        content = fp.read_text(encoding="utf-8")
                except Exception as e:
                    logger.warning("Failed to read %s for FTS reseed: %s", row["file_path"], e)
                await self.db.execute(
                    "INSERT INTO tasks_fts (task_id, title, content) VALUES (?, ?, ?)",
                    (row["id"], row["title"], content),
                )
            await self.db.commit()
            logger.info("FTS reseeded with %d tasks (content from disk)", task_count)

        if current < SCHEMA_VERSION:
            logger.info("Database migrated to schema version %d", SCHEMA_VERSION)

    # --- Session operations ---

    async def create_session(
        self,
        session_id: str,
        title: str | None = None,
        source: str = "web",
        metadata: dict | None = None,
        status: str = "created",
        parent_session_id: str | None = None,
        forked_from_message: str | None = None,
    ) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            """INSERT OR IGNORE INTO sessions
               (id, title, source, metadata, status, parent_session_id,
                forked_from_message, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (session_id, title or session_id, source,
             json.dumps(metadata or {}), status,
             parent_session_id, forked_from_message, now, now),
        )
        await self.db.commit()
        return {
            "id": session_id, "title": title or session_id,
            "source": source, "status": status,
            "parent_session_id": parent_session_id,
        }

    async def get_session(self, session_id: str) -> dict | None:
        async with self.db.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_sessions(
        self, limit: int = 50, include_archived: bool = False,
    ) -> list[dict]:
        if include_archived:
            query = "SELECT * FROM sessions ORDER BY updated_at DESC LIMIT ?"
            params = (limit,)
        else:
            query = "SELECT * FROM sessions WHERE status != 'archived' ORDER BY updated_at DESC LIMIT ?"
            params = (limit,)
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]

    async def search_sessions(self, query: str, limit: int = 100) -> list[dict]:
        """Search sessions by title (LIKE match), across all non-archived sessions."""
        sql = (
            "SELECT * FROM sessions "
            "WHERE title LIKE ? AND status != 'archived' "
            "ORDER BY updated_at DESC LIMIT ?"
        )
        async with self.db.execute(sql, (f"%{query}%", limit)) as cursor:
            return [dict(row) async for row in cursor]

    async def touch_session(self, session_id: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "UPDATE sessions SET updated_at = ? WHERE id = ?", (now, session_id)
        )
        await self.db.commit()

    async def update_session_title(self, session_id: str, title: str) -> None:
        await self.db.execute(
            "UPDATE sessions SET title = ? WHERE id = ?", (title, session_id)
        )
        await self.db.commit()

    async def delete_session(self, session_id: str) -> None:
        async with self._atomic():
            await self.db.execute("DELETE FROM session_file_snapshots WHERE session_id = ?", (session_id,))
            await self.db.execute("DELETE FROM session_events WHERE session_id = ?", (session_id,))
            await self.db.execute("DELETE FROM channel_sessions WHERE session_id = ?", (session_id,))
            await self.db.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
            await self.db.execute("DELETE FROM sessions WHERE id = ?", (session_id,))

    async def update_session_metadata(self, session_id: str, metadata: dict) -> None:
        """Update the metadata JSON for a session.

        Also syncs dedicated columns (sdk_session_id, connected_at) for
        backward compatibility with callers that still use the metadata blob.
        """
        await self.db.execute(
            "UPDATE sessions SET metadata = ? WHERE id = ?",
            (json.dumps(metadata), session_id),
        )
        # Sync dedicated columns from metadata
        col_sync: dict[str, str | None] = {}
        if "sdk_session_id" in metadata:
            col_sync["sdk_session_id"] = metadata["sdk_session_id"]
        if "connected_at" in metadata:
            col_sync["connected_at"] = metadata["connected_at"]
        if col_sync:
            await self.update_session_fields(session_id, col_sync)
        await self.db.commit()

    async def update_session_fields(self, session_id: str, fields: dict) -> None:
        """Update specific session columns atomically. Merges, doesn't replace."""
        allowed = {
            "status", "sdk_session_id", "connected_at", "last_activity_at",
            "archived_at", "title", "message_count", "total_cost_usd",
            "parent_session_id", "forked_from_message", "last_memorized_at",
        }
        set_clauses: list[str] = []
        params: list = []
        for key, value in fields.items():
            if key in allowed:
                set_clauses.append(f"{key} = ?")
                params.append(value)
        if not set_clauses:
            return
        set_clauses.append("updated_at = ?")
        params.append(datetime.now(timezone.utc).isoformat())
        params.append(session_id)
        await self.db.execute(
            f"UPDATE sessions SET {', '.join(set_clauses)} WHERE id = ?",
            tuple(params),
        )
        await self.db.commit()

    async def get_sessions_with_metadata_key(self, key: str) -> list[dict]:
        """Find sessions whose metadata JSON contains a specific key.

        Legacy method — prefer querying dedicated columns instead.
        """
        async with self.db.execute("SELECT * FROM sessions") as cursor:
            results = []
            async for row in cursor:
                d = dict(row)
                meta = json.loads(d.get("metadata", "{}") or "{}")
                if key in meta:
                    d["_parsed_metadata"] = meta
                    results.append(d)
            return results

    async def archive_session(self, old_id: str, new_id: str) -> None:
        """Rename a session (move messages to new ID)."""
        async with self._atomic():
            await self.db.execute("UPDATE session_events SET session_id = ? WHERE session_id = ?", (new_id, old_id))
            await self.db.execute("UPDATE messages SET session_id = ? WHERE session_id = ?", (new_id, old_id))
            await self.db.execute("UPDATE sessions SET id = ? WHERE id = ?", (new_id, old_id))

    # --- Session lifecycle operations (V3) ---

    async def log_session_event(
        self, session_id: str, event_type: str, details: dict | None = None,
    ) -> int:
        """Log a session lifecycle event."""
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            "INSERT INTO session_events (session_id, event_type, details, created_at) VALUES (?, ?, ?, ?)",
            (session_id, event_type, json.dumps(details) if details else None, now),
        ) as cursor:
            event_id = cursor.lastrowid
        await self.db.commit()
        return event_id

    async def get_session_events(
        self, session_id: str, limit: int = 50,
    ) -> list[dict]:
        """Get lifecycle events for a session, newest first."""
        async with self.db.execute(
            "SELECT * FROM session_events WHERE session_id = ? ORDER BY created_at DESC LIMIT ?",
            (session_id, limit),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("details"):
                try:
                    row["details"] = json.loads(row["details"])
                except (json.JSONDecodeError, TypeError):
                    pass
        return rows

    # --- Channel session mapping (V3) ---

    async def get_channel_session(self, channel_key: str) -> dict | None:
        """Get the persisted session for a channel."""
        async with self.db.execute(
            "SELECT * FROM channel_sessions WHERE channel_key = ?", (channel_key,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def set_channel_session(self, channel_key: str, session_id: str) -> None:
        """Persist a channel-to-session mapping."""
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "INSERT OR REPLACE INTO channel_sessions (channel_key, session_id, updated_at) VALUES (?, ?, ?)",
            (channel_key, session_id, now),
        )
        await self.db.commit()

    # --- Session cleanup queries (V3) ---

    async def get_sessions_by_status(self, statuses: list[str]) -> list[dict]:
        """Find sessions with any of the given statuses."""
        placeholders = ",".join("?" for _ in statuses)
        async with self.db.execute(
            f"SELECT * FROM sessions WHERE status IN ({placeholders})",
            tuple(statuses),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_stale_sessions(
        self, before_iso: str, exclude_ids: list[str] | None = None,
    ) -> list[dict]:
        """Get idle/stopped/error sessions not updated since before_iso."""
        excludes = exclude_ids or []
        if excludes:
            placeholders = ",".join("?" for _ in excludes)
            query = f"""
                SELECT * FROM sessions
                WHERE status IN ('idle', 'stopped', 'error')
                AND updated_at < ?
                AND id NOT IN ({placeholders})
            """
            params = (before_iso, *excludes)
        else:
            query = """
                SELECT * FROM sessions
                WHERE status IN ('idle', 'stopped', 'error')
                AND updated_at < ?
            """
            params = (before_iso,)
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]

    async def count_active_sessions(self) -> int:
        """Count non-archived sessions."""
        async with self.db.execute(
            "SELECT COUNT(*) FROM sessions WHERE status != 'archived'"
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_oldest_sessions(
        self, count: int, exclude_ids: list[str] | None = None,
    ) -> list[dict]:
        """Get the oldest non-active, non-archived sessions for cleanup."""
        excludes = exclude_ids or []
        if excludes:
            placeholders = ",".join("?" for _ in excludes)
            query = f"""
                SELECT * FROM sessions
                WHERE status NOT IN ('active', 'archived')
                AND id NOT IN ({placeholders})
                ORDER BY updated_at ASC LIMIT ?
            """
            params = (*excludes, count)
        else:
            query = """
                SELECT * FROM sessions
                WHERE status NOT IN ('active', 'archived')
                ORDER BY updated_at ASC LIMIT ?
            """
            params = (count,)
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]

    async def increment_message_count(self, session_id: str) -> None:
        """Atomically increment the message counter for a session."""
        await self.db.execute(
            "UPDATE sessions SET message_count = COALESCE(message_count, 0) + 1 WHERE id = ?",
            (session_id,),
        )
        await self.db.commit()

    async def get_sessions_needing_memorization(self) -> list[dict]:
        """Find non-archived sessions that have un-memorized messages.

        Returns sessions where:
        - status is not 'archived'
        - message_count > 0
        - last_memorized_at is NULL (never memorized) OR
          messages exist with created_at > last_memorized_at

        The ``last_memorized_at`` value is normalised in the comparison to
        match SQLite's ``CURRENT_TIMESTAMP`` format (``YYYY-MM-DD HH:MM:SS``)
        so the string comparison works correctly regardless of the stored
        format (ISO 8601 with ``T``/``Z`` or plain space-separated).
        """
        async with self.db.execute("""
            SELECT s.* FROM sessions s
            WHERE s.status != 'archived'
            AND s.message_count > 0
            AND (
                s.last_memorized_at IS NULL
                OR EXISTS (
                    SELECT 1 FROM messages m
                    WHERE m.session_id = s.id
                    AND m.created_at > SUBSTR(
                        REPLACE(REPLACE(s.last_memorized_at, 'T', ' '), 'Z', ''),
                        1, 19
                    )
                )
            )
        """) as cursor:
            return [dict(row) async for row in cursor]

    # --- Message operations ---

    async def add_message(
        self,
        session_id: str,
        role: str,
        content: str,
        channel: str | None = None,
        thinking: str | None = None,
        tool_calls: list | None = None,
        blocks: list | None = None,
    ) -> int:
        async with self._atomic():
            async with self.db.execute(
                """INSERT INTO messages (session_id, role, content, thinking, tool_calls, blocks, channel)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (session_id, role, content, thinking,
                 json.dumps(tool_calls) if tool_calls else None,
                 json.dumps(blocks) if blocks else None,
                 channel),
            ) as cursor:
                msg_id = cursor.lastrowid
            # Update session timestamp and message counter
            now = datetime.now(timezone.utc).isoformat()
            await self.db.execute(
                "UPDATE sessions SET updated_at = ?, message_count = COALESCE(message_count, 0) + 1 WHERE id = ?",
                (now, session_id),
            )
        return msg_id

    async def get_messages(
        self, session_id: str, limit: int = 500, offset: int = 0
    ) -> list[dict]:
        async with self.db.execute(
            "SELECT * FROM (SELECT * FROM messages WHERE session_id = ? ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?) ORDER BY created_at ASC, id ASC",
            (session_id, limit, offset),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("tool_calls"):
                row["tool_calls"] = json.loads(row["tool_calls"])
            if row.get("blocks"):
                row["blocks"] = json.loads(row["blocks"])
        return rows

    # --- File snapshot operations ---

    async def save_file_snapshot(
        self, session_id: str, file_path: str, content: str | None,
    ) -> None:
        """Save original file content before agent modification.

        Uses INSERT OR IGNORE so only the first touch per session+file is stored.
        """
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            """INSERT OR IGNORE INTO session_file_snapshots
               (session_id, file_path, original_content, created_at)
               VALUES (?, ?, ?, ?)""",
            (session_id, file_path, content, now),
        )
        await self.db.commit()

    async def get_file_snapshot(
        self, session_id: str, file_path: str,
    ) -> dict | None:
        """Retrieve original file snapshot for a specific file."""
        async with self.db.execute(
            "SELECT * FROM session_file_snapshots WHERE session_id = ? AND file_path = ?",
            (session_id, file_path),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_session_snapshots(self, session_id: str) -> list[dict]:
        """Get all file snapshots for a session."""
        async with self.db.execute(
            "SELECT session_id, file_path, created_at FROM session_file_snapshots "
            "WHERE session_id = ? ORDER BY created_at ASC",
            (session_id,),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def delete_session_snapshots(self, session_id: str) -> None:
        """Delete all file snapshots for a session."""
        await self.db.execute(
            "DELETE FROM session_file_snapshots WHERE session_id = ?",
            (session_id,),
        )
        await self.db.commit()

    async def count_messages(self, session_id: str) -> int:
        async with self.db.execute(
            "SELECT COUNT(*) FROM messages WHERE session_id = ?", (session_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    # --- Task operations ---

    async def upsert_task(
        self,
        task_id: str,
        file_path: str,
        title: str,
        status: str = "pending",
        source: str | None = None,
        source_url: str | None = None,
        deadline: str | None = None,
        content: str = "",
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            await self.db.execute(
                """INSERT INTO tasks (id, file_path, title, status, source, source_url, deadline, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       title=excluded.title, status=excluded.status, source=excluded.source,
                       source_url=excluded.source_url, deadline=excluded.deadline, updated_at=?""",
                (task_id, file_path, title, status, source, source_url, deadline, now, now, now),
            )
            # Sync FTS index
            await self.db.execute("DELETE FROM tasks_fts WHERE task_id = ?", (task_id,))
            await self.db.execute(
                "INSERT INTO tasks_fts (task_id, title, content) VALUES (?, ?, ?)",
                (task_id, title, content),
            )

    async def get_task(self, task_id: str) -> dict | None:
        async with self.db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_tasks(self, status: str | None = None, limit: int = 100) -> list[dict]:
        if status == "all":
            query = "SELECT * FROM tasks ORDER BY deadline ASC NULLS LAST, created_at DESC LIMIT ?"
            params = (limit,)
        elif status:
            query = "SELECT * FROM tasks WHERE status = ? ORDER BY deadline ASC NULLS LAST, created_at DESC LIMIT ?"
            params = (status, limit)
        else:
            query = "SELECT * FROM tasks WHERE status != 'done' ORDER BY deadline ASC NULLS LAST, created_at DESC LIMIT ?"
            params = (limit,)
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]

    async def update_task_status(self, task_id: str, status: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?",
            (status, now, task_id),
        )
        await self.db.commit()

    # Short words and common stop words that add noise to FTS searches.
    _FTS_STOP_WORDS = frozenset({
        "a", "an", "the", "is", "at", "by", "on", "in", "to", "of", "for",
        "and", "or", "not", "it", "be", "as", "do", "if", "so", "no", "up",
        "my", "we", "he", "me",
    })

    @classmethod
    def _build_fts_query(cls, query: str, mode: str = "and") -> str:
        """Build an FTS5 query from a user search string.

        Args:
            query: Raw search text.
            mode: 'and' — all terms must match (strict, good for user search).
                  'or'  — any term can match (permissive, good for dedup).
        """
        import re
        clean = re.sub(r'["\*\(\)\-:/\\#]', " ", query)
        words = [
            w for w in clean.split()
            if w.strip() and len(w) > 1 and w.lower() not in cls._FTS_STOP_WORDS
        ]
        if not words:
            return ""
        joiner = " OR " if mode == "or" else " "
        return joiner.join(f'"{w}"' for w in words)

    async def search_tasks(
        self, query: str, status: str | None = None, limit: int = 20,
    ) -> list[dict]:
        """Search tasks using FTS5 full-text search on title and content.

        Args:
            query: Search words — tokenized and matched via FTS5.
            status: Filter by status. None = non-done, 'all' = everything.
            limit: Max results.
        """
        fts_query = self._build_fts_query(query)
        if not fts_query:
            return []

        conditions = ["t.id IN (SELECT task_id FROM tasks_fts WHERE tasks_fts MATCH ?)"]
        params: list = [fts_query]
        if status == "all":
            pass  # no status filter
        elif status:
            conditions.append("t.status = ?")
            params.append(status)
        else:
            conditions.append("t.status != 'done'")
        where = " AND ".join(conditions)
        params.append(limit)
        async with self.db.execute(
            f"SELECT t.* FROM tasks t WHERE {where} ORDER BY t.updated_at DESC LIMIT ?",
            tuple(params),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def search_tasks_similar(
        self, query: str, limit: int = 10,
    ) -> list[dict]:
        """Find tasks similar to query using OR semantics + FTS5 ranking.

        Unlike search_tasks (AND, strict), this uses OR (any word matches)
        and orders by BM25 relevance.  Designed for duplicate detection —
        searches all statuses including done.
        """
        fts_query = self._build_fts_query(query, mode="or")
        if not fts_query:
            return []

        async with self.db.execute(
            "SELECT t.* FROM tasks t "
            "JOIN tasks_fts f ON f.task_id = t.id "
            "WHERE tasks_fts MATCH ? "
            "ORDER BY f.rank LIMIT ?",
            (fts_query, limit),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def find_tasks_by_source_url(
        self, source_url: str, limit: int = 10,
    ) -> list[dict]:
        """Find tasks with an exact source_url match (most reliable dedup)."""
        async with self.db.execute(
            "SELECT * FROM tasks WHERE source_url = ? ORDER BY updated_at DESC LIMIT ?",
            (source_url, limit),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def rebuild_fts(self) -> None:
        """Clear the FTS index. Caller must re-populate via upsert_task()."""
        await self.db.execute("DELETE FROM tasks_fts")
        await self.db.commit()

    async def update_task_escalation(
        self, task_id: str, level: int, reminded_at: str | None = None
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "UPDATE tasks SET escalation_level = ?, last_reminded_at = ?, updated_at = ? WHERE id = ?",
            (level, reminded_at or now, now, task_id),
        )
        await self.db.commit()

    # --- Plan operations ---

    async def create_plan(
        self,
        plan_id: str,
        task_id: str,
        content: str,
        session_id: str | None = None,
        model: str | None = None,
        version: int = 1,
        parent_plan_id: str | None = None,
        plan_type: str = "generic",
    ) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            """INSERT INTO plans (id, task_id, session_id, content, model, version, parent_plan_id, plan_type, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (plan_id, task_id, session_id, content, model, version, parent_plan_id, plan_type, now),
        )
        await self.db.commit()
        return {"id": plan_id, "task_id": task_id, "version": version, "plan_type": plan_type}

    async def get_plan(self, plan_id: str) -> dict | None:
        async with self.db.execute(
            """SELECT p.*, t.title AS task_title
               FROM plans p LEFT JOIN tasks t ON p.task_id = t.id
               WHERE p.id = ?""",
            (plan_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_plans(
        self, status: str | None = None, task_id: str | None = None, limit: int = 100,
    ) -> list[dict]:
        conditions = []
        params: list = []
        if status:
            conditions.append("p.status = ?")
            params.append(status)
        if task_id:
            conditions.append("p.task_id = ?")
            params.append(task_id)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        async with self.db.execute(
            f"""SELECT p.*, t.title AS task_title
                FROM plans p LEFT JOIN tasks t ON p.task_id = t.id
                {where}
                ORDER BY p.created_at DESC LIMIT ?""",
            tuple(params),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def update_plan(self, plan_id: str, **fields) -> None:
        if not fields:
            return
        sets = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values())
        vals.append(plan_id)
        await self.db.execute(
            f"UPDATE plans SET {sets} WHERE id = ?", tuple(vals),
        )
        await self.db.commit()

    async def get_plans_for_task(self, task_id: str) -> list[dict]:
        async with self.db.execute(
            "SELECT * FROM plans WHERE task_id = ? ORDER BY version DESC",
            (task_id,),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_pending_plan_task_ids(self) -> list[str]:
        """Get task IDs that have a pending or implementing plan."""
        async with self.db.execute(
            "SELECT DISTINCT task_id FROM plans WHERE status IN ('pending', 'implementing')"
        ) as cursor:
            return [row[0] async for row in cursor]

    # --- Notification operations ---

    async def create_notification(
        self,
        notification_id: str,
        session_id: str,
        type: str,
        title: str,
        body: str = "",
        priority: str = "normal",
        options: list[str] | None = None,
        expires_at: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            """INSERT INTO notifications
               (id, session_id, type, title, body, priority, options, expires_at, metadata, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (notification_id, session_id, type, title, body, priority,
             json.dumps(options) if options else None,
             expires_at, json.dumps(metadata or {}), now),
        )
        await self.db.commit()
        return {"id": notification_id, "session_id": session_id, "type": type}

    async def get_notification(self, notification_id: str) -> dict | None:
        async with self.db.execute(
            """SELECT n.*, s.title AS session_title
               FROM notifications n
               LEFT JOIN sessions s ON n.session_id = s.id
               WHERE n.id = ?""",
            (notification_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_notifications(
        self, status: str | None = None, type: str | None = None,
        session_id: str | None = None, limit: int = 50,
    ) -> list[dict]:
        conditions = []
        params: list = []
        if status:
            conditions.append("n.status = ?")
            params.append(status)
        if type:
            conditions.append("n.type = ?")
            params.append(type)
        if session_id:
            conditions.append("n.session_id = ?")
            params.append(session_id)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        async with self.db.execute(
            f"""SELECT n.*, s.title AS session_title
                FROM notifications n
                LEFT JOIN sessions s ON n.session_id = s.id
                {where}
                ORDER BY n.created_at DESC LIMIT ?""",
            tuple(params),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def answer_notification(
        self, notification_id: str, answer: str, answered_by: str,
    ) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            async with self.db.execute(
                "SELECT id FROM notifications WHERE id = ? AND status = 'pending'",
                (notification_id,),
            ) as cursor:
                if not await cursor.fetchone():
                    return False
            await self.db.execute(
                """UPDATE notifications
                   SET answer = ?, answered_by = ?, answered_at = ?, status = 'answered'
                   WHERE id = ?""",
                (answer, answered_by, now, notification_id),
            )
        return True

    async def dismiss_notification(self, notification_id: str) -> bool:
        async with self._atomic():
            async with self.db.execute(
                "SELECT id FROM notifications WHERE id = ? AND status = 'pending'",
                (notification_id,),
            ) as cursor:
                if not await cursor.fetchone():
                    return False
            await self.db.execute(
                "UPDATE notifications SET status = 'dismissed' WHERE id = ?",
                (notification_id,),
            )
        return True

    async def dismiss_all_notifications(self) -> int:
        """Dismiss all pending non-question notifications. Returns count dismissed."""
        cursor = await self.db.execute(
            "UPDATE notifications SET status = 'dismissed' WHERE status = 'pending' AND type = 'notify'",
        )
        await self.db.commit()
        return cursor.rowcount

    async def expire_notifications(self) -> int:
        now = datetime.now(timezone.utc).isoformat()
        cursor = await self.db.execute(
            """UPDATE notifications SET status = 'expired'
               WHERE status = 'pending' AND expires_at IS NOT NULL AND expires_at < ?""",
            (now,),
        )
        await self.db.commit()
        return cursor.rowcount

    async def count_pending_notifications(self) -> int:
        async with self.db.execute(
            "SELECT COUNT(*) FROM notifications WHERE status = 'pending'"
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def update_notification(self, notification_id: str, **fields) -> None:
        if not fields:
            return
        sets = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values())
        vals.append(notification_id)
        await self.db.execute(
            f"UPDATE notifications SET {sets} WHERE id = ?", tuple(vals),
        )
        await self.db.commit()

    # --- Sync cursor operations ---

    async def get_sync_cursor(self, source: str) -> str | None:
        async with self.db.execute(
            "SELECT cursor FROM sync_cursors WHERE source = ?", (source,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def set_sync_cursor(self, source: str, cursor_value: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "INSERT OR REPLACE INTO sync_cursors (source, cursor, updated_at) VALUES (?, ?, ?)",
            (source, cursor_value, now),
        )
        await self.db.commit()

    # --- Source run log operations ---

    async def log_source_run(
        self,
        source: str,
        records_fetched: int = 0,
        records_processed: int = 0,
        error: str | None = None,
        session_id: str | None = None,
    ) -> int:
        """Log a source run with stats."""
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            "INSERT INTO source_run_log (source, ran_at, records_fetched, records_processed, error, session_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (source, now, records_fetched, records_processed, error, session_id),
        ) as cursor:
            log_id = cursor.lastrowid
        await self.db.commit()
        return log_id

    async def get_last_source_run(self, source: str) -> dict | None:
        """Get the most recent source run entry."""
        async with self.db.execute(
            "SELECT * FROM source_run_log WHERE source = ? ORDER BY ran_at DESC LIMIT 1",
            (source,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_source_run_stats(self, source: str, limit: int = 10) -> list[dict]:
        """Get recent source runs for diagnostics."""
        async with self.db.execute(
            "SELECT * FROM source_run_log WHERE source = ? ORDER BY ran_at DESC LIMIT ?",
            (source, limit),
        ) as cursor:
            return [dict(row) async for row in cursor]

    # --- Cron log operations ---

    async def log_cron_start(self, job_id: str) -> int:
        async with self.db.execute(
            "INSERT INTO cron_logs (job_id) VALUES (?)", (job_id,)
        ) as cursor:
            log_id = cursor.lastrowid
        await self.db.commit()
        return log_id

    async def log_cron_finish(
        self, log_id: int, status: str, output: str | None = None, error: str | None = None
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "UPDATE cron_logs SET finished_at = ?, status = ?, output = ?, error = ? WHERE id = ?",
            (now, status, output, error, log_id),
        )
        await self.db.commit()

    async def get_cron_logs(self, job_id: str | None = None, limit: int = 50) -> list[dict]:
        if job_id:
            query = "SELECT * FROM cron_logs WHERE job_id = ? ORDER BY started_at DESC LIMIT ?"
            params = (job_id, limit)
        else:
            query = "SELECT * FROM cron_logs ORDER BY started_at DESC LIMIT ?"
            params = (limit,)
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]

    async def get_last_user_message_time(self) -> str | None:
        """Get the timestamp of the most recent user message across non-system sessions."""
        async with self.db.execute(
            """SELECT MAX(m.created_at) FROM messages m
               JOIN sessions s ON m.session_id = s.id
               WHERE m.role = 'user'
               AND s.id NOT LIKE 'cron:%'
               AND s.id NOT LIKE 'hb:%'
               AND s.id NOT LIKE 'hook:%'""",
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else None

    async def get_last_successful_cron_run(self, job_id: str) -> dict | None:
        """Get the most recent successful cron_logs entry for a job."""
        async with self.db.execute(
            "SELECT * FROM cron_logs WHERE job_id = ? AND status = 'success' ORDER BY finished_at DESC LIMIT 1",
            (job_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_recent_cron_runs(self, hours: int = 6) -> list[dict]:
        """Get all successful cron runs within the last N hours."""
        async with self.db.execute(
            """SELECT job_id, finished_at FROM cron_logs
               WHERE status = 'success'
               AND finished_at > datetime('now', ? || ' hours')
               ORDER BY finished_at DESC""",
            (f"-{hours}",),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_last_telegram_channel_key(self) -> str | None:
        """Get the most recently updated telegram:* channel key."""
        async with self.db.execute(
            "SELECT channel_key FROM channel_sessions WHERE channel_key LIKE 'telegram:%' ORDER BY updated_at DESC LIMIT 1",
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    # --- Source messages inbox ---

    async def insert_source_messages(
        self,
        records: list,
        source: str,
        ttl_days: int = 7,
    ) -> int:
        """Bulk insert source records into the inbox. Returns count inserted."""
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        expires = (now + timedelta(days=ttl_days)).isoformat()
        now_iso = now.isoformat()
        inserted = 0
        async with self._atomic():
            for r in records:
                try:
                    await self.db.execute(
                        "INSERT OR IGNORE INTO source_messages "
                        "(id, source, record_type, summary, content, raw_content, timestamp, metadata, created_at, expires_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (r.id, source, r.record_type, r.summary, r.content,
                         getattr(r, 'raw_content', None),
                         r.timestamp, json.dumps(r.metadata) if r.metadata else None,
                         now_iso, expires),
                    )
                    inserted += 1
                except Exception as e:
                    logger.warning("Failed to insert source message %s: %s", r.id, e)
        return inserted

    async def update_source_messages_processed(
        self,
        source: str,
        ids: list[str],
        processed_map: dict[str, str],
    ) -> None:
        """Set processed_content on messages after condensation."""
        async with self._atomic():
            for msg_id in ids:
                content = processed_map.get(msg_id)
                if content is not None:
                    await self.db.execute(
                        "UPDATE source_messages SET processed_content = ? WHERE source = ? AND id = ?",
                        (content, source, msg_id),
                    )

    async def update_source_messages_session(
        self,
        source: str,
        ids: list[str],
        session_id: str,
    ) -> None:
        """Link messages to the cron session that processed them."""
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        await self.db.execute(
            f"UPDATE source_messages SET run_session_id = ? WHERE source = ? AND id IN ({placeholders})",
            (session_id, source, *ids),
        )
        await self.db.commit()

    # Normalize ISO 8601 timestamps for consistent sorting across sources.
    # Different sources use different suffixes: "+00:00", "Z", or none.
    # This expression strips the tz suffix so pure lexicographic ORDER BY works.
    _TS_SORT = "REPLACE(REPLACE(timestamp, '+00:00', ''), 'Z', '')"

    async def list_source_messages(
        self,
        source: str | None = None,
        limit: int = 50,
        before_ts: str | None = None,
        run_session_id: str | None = None,
    ) -> list[dict]:
        """Paginated list of source messages, newest first.

        Excludes processed_content for performance (use get_source_message for full detail).
        """
        conditions: list[str] = []
        params: list = []
        if source:
            conditions.append("source = ?")
            params.append(source)
        if run_session_id:
            conditions.append("run_session_id = ?")
            params.append(run_session_id)
        if before_ts:
            # Normalize the pagination cursor the same way as the sort key
            norm_before = before_ts.replace("+00:00", "").replace("Z", "")
            conditions.append(f"{self._TS_SORT} < ?")
            params.append(norm_before)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(limit + 1)  # fetch one extra to detect has_more
        async with self.db.execute(
            f"SELECT id, source, record_type, summary, timestamp, run_session_id, created_at "
            f"FROM source_messages {where} ORDER BY {self._TS_SORT} DESC LIMIT ?",
            tuple(params),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        has_more = len(rows) > limit
        return rows[:limit], has_more

    async def get_source_message(self, source: str, msg_id: str) -> dict | None:
        """Get a single source message with full content and metadata."""
        async with self.db.execute(
            "SELECT * FROM source_messages WHERE source = ? AND id = ?",
            (source, msg_id),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            d = dict(row)
            if d.get("metadata"):
                try:
                    d["metadata"] = json.loads(d["metadata"])
                except (json.JSONDecodeError, TypeError):
                    pass
            return d

    async def get_source_message_counts(self) -> dict[str, int]:
        """Get message counts per source."""
        async with self.db.execute(
            "SELECT source, COUNT(*) as cnt FROM source_messages GROUP BY source"
        ) as cursor:
            return {row[0]: row[1] async for row in cursor}

    async def get_source_messages_storage(self) -> dict[str, dict]:
        """Get storage stats per source: count and estimated bytes."""
        async with self.db.execute(
            "SELECT source, COUNT(*) as cnt, "
            "SUM(LENGTH(content) + COALESCE(LENGTH(processed_content), 0) + COALESCE(LENGTH(raw_content), 0)) as bytes "
            "FROM source_messages GROUP BY source"
        ) as cursor:
            result = {}
            async for row in cursor:
                result[row[0]] = {"count": row[1], "bytes": row[2] or 0}
            return result

    async def get_source_stats(self, hours: int = 24) -> dict[str, dict]:
        """Aggregate source_run_log stats per source for the last N hours."""
        async with self.db.execute(
            "SELECT source, COUNT(*) as runs, "
            "SUM(records_fetched) as fetched, SUM(records_processed) as processed, "
            "SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) as errors, "
            "MAX(ran_at) as last_run_at "
            "FROM source_run_log WHERE ran_at > datetime('now', ? || ' hours') "
            "GROUP BY source",
            (f"-{hours}",),
        ) as cursor:
            result = {}
            async for row in cursor:
                result[row[0]] = {
                    "runs": row[1],
                    "fetched": row[2] or 0,
                    "processed": row[3] or 0,
                    "errors": row[4] or 0,
                    "last_run_at": row[5],
                }
            return result

    async def get_source_run_log(
        self,
        source: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Get source run history with session_id for linking."""
        if source:
            query = "SELECT * FROM source_run_log WHERE source = ? ORDER BY ran_at DESC LIMIT ?"
            params = (source, limit)
        else:
            query = "SELECT * FROM source_run_log ORDER BY ran_at DESC LIMIT ?"
            params = (limit,)
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]

    async def delete_source_messages(self, source: str | None = None) -> int:
        """Purge source messages. If source is None, purge all. Returns count deleted."""
        if source:
            async with self.db.execute(
                "SELECT COUNT(*) FROM source_messages WHERE source = ?", (source,)
            ) as cursor:
                count = (await cursor.fetchone())[0]
            await self.db.execute("DELETE FROM source_messages WHERE source = ?", (source,))
        else:
            async with self.db.execute("SELECT COUNT(*) FROM source_messages") as cursor:
                count = (await cursor.fetchone())[0]
            await self.db.execute("DELETE FROM source_messages")
        await self.db.commit()
        return count

    async def cleanup_expired_messages(self) -> int:
        """Delete source messages past their TTL. Returns count deleted."""
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            "SELECT COUNT(*) FROM source_messages WHERE expires_at < ?", (now,)
        ) as cursor:
            count = (await cursor.fetchone())[0]
        if count > 0:
            await self.db.execute(
                "DELETE FROM source_messages WHERE expires_at < ?", (now,)
            )
            await self.db.commit()
        return count

    # --- Consumer cursors ---

    async def get_source_max_rowid(self, source: str) -> int:
        """Get current MAX(rowid) for a source. Returns 0 if no messages."""
        async with self.db.execute(
            "SELECT COALESCE(MAX(rowid), 0) FROM source_messages WHERE source = ?",
            (source,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_consumer_cursor(self, consumer: str, source: str) -> int:
        """Get cursor position for a consumer+source pair.

        If no cursor exists or it has expired, initializes to current
        MAX(rowid) for the source. New consumers see only future messages.
        """
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            "SELECT cursor_seq, expires_at FROM consumer_cursors WHERE consumer = ? AND source = ?",
            (consumer, source),
        ) as cursor:
            row = await cursor.fetchone()

        if row is not None:
            expires = row[1]
            if expires is None or expires > now:
                return row[0]
            # Expired — fall through to re-initialize

        # No cursor or expired: initialize to latest
        max_seq = await self.get_source_max_rowid(source)
        await self.set_consumer_cursor(consumer, source, max_seq)
        return max_seq

    async def set_consumer_cursor(
        self,
        consumer: str,
        source: str,
        cursor_seq: int,
        ttl_days: int = 2,
        session_id: str | None = None,
    ) -> None:
        """Advance cursor and refresh TTL. Links to session_id for UI tracking."""
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        expires = (now + timedelta(days=ttl_days)).isoformat()
        await self.db.execute(
            """INSERT INTO consumer_cursors (consumer, source, cursor_seq, session_id, updated_at, expires_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(consumer, source) DO UPDATE SET
                   cursor_seq=excluded.cursor_seq, session_id=COALESCE(excluded.session_id, consumer_cursors.session_id),
                   updated_at=excluded.updated_at, expires_at=excluded.expires_at""",
            (consumer, source, cursor_seq, session_id, now.isoformat(), expires),
        )
        await self.db.commit()

    async def list_consumer_cursors(self, consumer: str | None = None) -> list[dict]:
        """List active (non-expired) consumer cursors with unread counts."""
        now = datetime.now(timezone.utc).isoformat()
        conditions = ["(expires_at IS NULL OR expires_at > ?)"]
        params: list = [now]
        if consumer:
            conditions.append("consumer = ?")
            params.append(consumer)
        where = " AND ".join(conditions)

        async with self.db.execute(
            f"""SELECT cc.consumer, cc.source, cc.cursor_seq, cc.session_id,
                       cc.updated_at, cc.expires_at,
                       (SELECT COUNT(*) FROM source_messages sm
                        WHERE sm.source = cc.source AND sm.rowid > cc.cursor_seq) as unread
                FROM consumer_cursors cc
                WHERE {where}
                ORDER BY cc.consumer, cc.source""",
            tuple(params),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def read_source_messages_by_rowid(
        self,
        source: str,
        after_seq: int,
        limit: int = 50,
    ) -> list[dict]:
        """Read messages from one source with rowid > after_seq.

        Returns dicts with 'rowid' included for cursor advancement.
        Uses processed_content if available, falls back to content.
        """
        async with self.db.execute(
            """SELECT rowid, id, source, record_type, summary,
                      COALESCE(processed_content, content) as content,
                      timestamp, metadata, run_session_id, created_at
               FROM source_messages
               WHERE source = ? AND rowid > ?
               ORDER BY rowid ASC
               LIMIT ?""",
            (source, after_seq, limit),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("metadata"):
                try:
                    row["metadata"] = json.loads(row["metadata"])
                except (json.JSONDecodeError, TypeError):
                    pass
        return rows

    async def browse_source_messages(
        self,
        source: str,
        limit: int = 50,
        before_seq: int | None = None,
        after_seq: int | None = None,
    ) -> list[dict]:
        """Browse historical messages with manual cursor. No consumer state modified.

        before_seq: messages with rowid < X (paginate backwards, newest first)
        after_seq: messages with rowid > X (paginate forwards, oldest first)
        Neither: return most recent messages (newest first)
        """
        conditions = ["source = ?"]
        params: list = [source]

        if before_seq is not None:
            conditions.append("rowid < ?")
            params.append(before_seq)
            order = "DESC"
        elif after_seq is not None:
            conditions.append("rowid > ?")
            params.append(after_seq)
            order = "ASC"
        else:
            order = "DESC"

        where = " AND ".join(conditions)
        params.append(limit)

        async with self.db.execute(
            f"""SELECT rowid, id, source, record_type, summary,
                       COALESCE(processed_content, content) as content,
                       timestamp, metadata, run_session_id, created_at
                FROM source_messages
                WHERE {where}
                ORDER BY rowid {order}
                LIMIT ?""",
            tuple(params),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("metadata"):
                try:
                    row["metadata"] = json.loads(row["metadata"])
                except (json.JSONDecodeError, TypeError):
                    pass
        return rows

    async def cleanup_expired_consumer_cursors(self) -> int:
        """Delete expired consumer cursors. Returns count deleted."""
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            async with self.db.execute(
                "SELECT COUNT(*) FROM consumer_cursors WHERE expires_at IS NOT NULL AND expires_at < ?",
                (now,),
            ) as cursor:
                count = (await cursor.fetchone())[0]
            if count > 0:
                await self.db.execute(
                    "DELETE FROM consumer_cursors WHERE expires_at IS NOT NULL AND expires_at < ?",
                    (now,),
                )
        return count

    # --- memU audit log ---

    async def log_audit(
        self,
        action: str,
        target_type: str,
        target_id: str | None = None,
        source: str | None = None,
        details: dict | None = None,
    ) -> int:
        """Write an entry to the memU audit log."""
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            "INSERT INTO memu_audit_log (timestamp, action, target_type, target_id, source, details) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (now, action, target_type, target_id, source, json.dumps(details) if details else None),
        ) as cursor:
            log_id = cursor.lastrowid
        await self.db.commit()
        return log_id

    async def get_audit_logs(
        self,
        action: str | None = None,
        target_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Retrieve audit log entries, newest first."""
        conditions: list[str] = []
        params: list = []
        if action:
            conditions.append("action = ?")
            params.append(action)
        if target_type:
            conditions.append("target_type = ?")
            params.append(target_type)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        query = f"SELECT * FROM memu_audit_log {where} ORDER BY timestamp DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        async with self.db.execute(query, tuple(params)) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("details"):
                try:
                    row["details"] = json.loads(row["details"])
                except (json.JSONDecodeError, TypeError):
                    pass
        return rows

    # --- Skills ---

    async def upsert_skill(
        self,
        skill_id: str,
        name: str,
        description: str,
        version: str = "1.0.0",
        enabled: bool = True,
        user_invocable: bool = True,
        model_invocable: bool = True,
        allowed_tools: list[str] | None = None,
        metadata: dict | None = None,
    ) -> None:
        """Insert or update a skill in the registry."""
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            """INSERT INTO skills (id, name, description, version, enabled,
               user_invocable, model_invocable, allowed_tools, metadata, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 name=excluded.name, description=excluded.description,
                 version=excluded.version, user_invocable=excluded.user_invocable,
                 model_invocable=excluded.model_invocable,
                 allowed_tools=excluded.allowed_tools, metadata=excluded.metadata,
                 updated_at=excluded.updated_at""",
            (skill_id, name, description, version, enabled,
             user_invocable, model_invocable,
             json.dumps(allowed_tools) if allowed_tools else None,
             json.dumps(metadata or {}), now, now),
        )
        await self.db.commit()

    async def get_skill_row(self, skill_id: str) -> dict | None:
        """Get a single skill record."""
        async with self.db.execute(
            "SELECT * FROM skills WHERE id = ?", (skill_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_skills(self) -> list[dict]:
        """List all skills."""
        async with self.db.execute(
            "SELECT * FROM skills ORDER BY name"
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def delete_skill_row(self, skill_id: str) -> None:
        """Remove a skill and its usage records."""
        await self.db.execute("DELETE FROM skill_usage WHERE skill_id = ?", (skill_id,))
        await self.db.execute("DELETE FROM skills WHERE id = ?", (skill_id,))
        await self.db.commit()

    async def update_skill_enabled(self, skill_id: str, enabled: bool) -> None:
        """Toggle a skill's enabled state."""
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "UPDATE skills SET enabled = ?, updated_at = ? WHERE id = ?",
            (enabled, now, skill_id),
        )
        await self.db.commit()

    async def record_skill_usage(
        self,
        skill_id: str,
        session_id: str | None = None,
        invoked_by: str = "model",
        duration_ms: int | None = None,
        success: bool = True,
        error: str | None = None,
    ) -> None:
        """Log a skill invocation."""
        await self.db.execute(
            """INSERT INTO skill_usage (skill_id, session_id, invoked_by, duration_ms, success, error)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (skill_id, session_id, invoked_by, duration_ms, success, error),
        )
        await self.db.commit()

    async def get_skill_usage(self, skill_id: str, limit: int = 50) -> list[dict]:
        """Get usage history for a skill."""
        async with self.db.execute(
            "SELECT * FROM skill_usage WHERE skill_id = ? ORDER BY created_at DESC LIMIT ?",
            (skill_id, limit),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_skill_stats(self, skill_id: str | None = None) -> list[dict]:
        """Get aggregate usage stats per skill."""
        where = "WHERE skill_id = ?" if skill_id else ""
        params: list = [skill_id] if skill_id else []
        query = f"""
            SELECT
                skill_id,
                COUNT(*) as total_invocations,
                SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count,
                ROUND(AVG(duration_ms), 0) as avg_duration_ms,
                MAX(created_at) as last_used
            FROM skill_usage
            {where}
            GROUP BY skill_id
        """
        async with self.db.execute(query, tuple(params)) as cursor:
            return [dict(row) async for row in cursor]

    async def get_all_skills_with_stats(self) -> list[dict]:
        """List all skills with aggregated usage stats."""
        async with self.db.execute("""
            SELECT s.*,
                   COALESCE(u.total_invocations, 0) as total_invocations,
                   COALESCE(u.success_count, 0) as success_count,
                   u.avg_duration_ms,
                   u.last_used
            FROM skills s
            LEFT JOIN (
                SELECT skill_id,
                       COUNT(*) as total_invocations,
                       SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count,
                       ROUND(AVG(duration_ms), 0) as avg_duration_ms,
                       MAX(created_at) as last_used
                FROM skill_usage
                GROUP BY skill_id
            ) u ON s.id = u.skill_id
            ORDER BY s.name
        """) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("allowed_tools"):
                try:
                    row["allowed_tools"] = json.loads(row["allowed_tools"])
                except (json.JSONDecodeError, TypeError):
                    pass
            if row.get("metadata"):
                try:
                    row["metadata"] = json.loads(row["metadata"])
                except (json.JSONDecodeError, TypeError):
                    pass
        return rows

    # --- MCP server operations ---

    async def upsert_mcp_server(
        self,
        name: str,
        server_type: str,
        enabled: bool = True,
        tool_count: int = 0,
    ) -> None:
        """Register or update an MCP server entry."""
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            """INSERT INTO mcp_servers (name, type, enabled, tool_count, first_seen_at, last_seen_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET
                   type = excluded.type,
                   enabled = excluded.enabled,
                   tool_count = CASE WHEN excluded.tool_count > 0
                                     THEN excluded.tool_count
                                     ELSE mcp_servers.tool_count END,
                   last_seen_at = excluded.last_seen_at""",
            (name, server_type, enabled, tool_count, now, now),
        )
        await self.db.commit()

    async def record_mcp_tool_usage(
        self,
        server_name: str,
        tool_name: str,
        session_id: str | None = None,
        duration_ms: int | None = None,
        success: bool = True,
        error: str | None = None,
    ) -> None:
        """Log an MCP tool invocation."""
        await self.db.execute(
            """INSERT INTO mcp_tool_usage
               (server_name, tool_name, session_id, duration_ms, success, error)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (server_name, tool_name, session_id, duration_ms, success, error),
        )
        await self.db.commit()

    async def get_mcp_server_stats(self) -> list[dict]:
        """List all MCP servers with aggregated usage stats."""
        async with self.db.execute("""
            SELECT s.*,
                   COALESCE(u.total_invocations, 0) as total_invocations,
                   COALESCE(u.success_count, 0) as success_count,
                   u.avg_duration_ms,
                   u.last_used
            FROM mcp_servers s
            LEFT JOIN (
                SELECT server_name,
                       COUNT(*) as total_invocations,
                       SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count,
                       ROUND(AVG(duration_ms), 0) as avg_duration_ms,
                       MAX(created_at) as last_used
                FROM mcp_tool_usage
                GROUP BY server_name
            ) u ON s.name = u.server_name
            ORDER BY s.name
        """) as cursor:
            return [dict(row) async for row in cursor]

    async def get_mcp_tool_breakdown(self, server_name: str) -> list[dict]:
        """Get per-tool usage breakdown for a server."""
        async with self.db.execute("""
            SELECT tool_name,
                   COUNT(*) as invocations,
                   SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count,
                   ROUND(AVG(duration_ms), 0) as avg_duration_ms,
                   MAX(created_at) as last_used
            FROM mcp_tool_usage
            WHERE server_name = ?
            GROUP BY tool_name
            ORDER BY invocations DESC
        """, (server_name,)) as cursor:
            return [dict(row) async for row in cursor]

    async def get_mcp_server_usage(
        self, server_name: str, limit: int = 50,
    ) -> list[dict]:
        """Get recent usage history for a specific MCP server."""
        async with self.db.execute(
            """SELECT * FROM mcp_tool_usage
               WHERE server_name = ?
               ORDER BY created_at DESC LIMIT ?""",
            (server_name, limit),
        ) as cursor:
            return [dict(row) async for row in cursor]


# Global database instance
_db: Database | None = None


async def get_db() -> Database:
    """Get the global database instance."""
    global _db
    if _db is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _db


async def init_db(db_path: Path | None = None) -> Database:
    """Initialize the global database."""
    global _db
    if db_path is None:
        db_path = Path("~/.nerve/nerve.db").expanduser()
    _db = Database(db_path)
    await _db.connect()
    return _db


async def close_db() -> None:
    """Close the global database."""
    global _db
    if _db:
        await _db.close()
        _db = None
