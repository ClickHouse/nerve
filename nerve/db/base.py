"""Core Database class — connection management, write lock, migrations, and FTS health check.

The Database class composes all domain-specific mixin stores via multiple
inheritance.  External code continues to import ``Database`` from ``nerve.db``
(via the package ``__init__.py``), so the public API is unchanged.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import aiosqlite

from nerve.db.audit import AuditStore
from nerve.db.cron import CronStore
from nerve.db.files import FileStore
from nerve.db.mcp import McpStore
from nerve.db.messages import MessageStore
from nerve.db.migrations.runner import discover_migrations, run_migrations
from nerve.db.notifications import NotificationStore
from nerve.db.plans import PlanStore
from nerve.db.sessions import SessionStore
from nerve.db.skills import SkillStore
from nerve.db.sources import SourceStore
from nerve.db.tasks import TaskStore
from nerve.db.usage import UsageStore

logger = logging.getLogger(__name__)

# SCHEMA_VERSION is derived from the highest migration file number.
# This keeps a single source of truth (the migration files themselves).
SCHEMA_VERSION = max(v for v, _ in discover_migrations()) if discover_migrations() else 0


class Database(
    SessionStore,
    MessageStore,
    TaskStore,
    PlanStore,
    NotificationStore,
    SourceStore,
    CronStore,
    SkillStore,
    McpStore,
    AuditStore,
    UsageStore,
    FileStore,
):
    """Async SQLite database wrapper.

    Provides connection management, write serialization, schema migrations,
    and all domain-specific data access methods via mixin inheritance.
    """

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
        await run_migrations(self._db)
        await self._check_fts_integrity()

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

    async def _check_fts_integrity(self) -> None:
        """FTS integrity check — runs every startup.

        If the tasks table and tasks_fts index are out of sync, reseed FTS
        from disk files (the source of truth).
        """
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
