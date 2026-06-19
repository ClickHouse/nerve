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
from nerve.db.maintenance import MaintenanceStore
from nerve.db.mcp import McpStore
from nerve.db.messages import MessageStore
from nerve.db.migrations.runner import discover_migrations, run_migrations
from nerve.db.notifications import NotificationStore
from nerve.db.plans import PlanStore
from nerve.db.sessions import SessionStore
from nerve.db.skills import SkillStore
from nerve.db.sources import SourceStore
from nerve.db.task_statuses import TaskStatusStore
from nerve.db.tasks import TaskStore
from nerve.db.usage import UsageStore
from nerve.db.wakeups import WakeupStore

logger = logging.getLogger(__name__)

# SCHEMA_VERSION is derived from the highest migration file number.
# This keeps a single source of truth (the migration files themselves).
SCHEMA_VERSION = max(v for v, _ in discover_migrations()) if discover_migrations() else 0


# Connection pragmas applied on every ``connect()``. These mirror the tuning
# memU already uses for its own SQLite connections (see
# ``nerve/memory/memu_bridge.py``) — the primary operational DB is where the
# heaviest cron/CLI/backup contention happens, yet it was never tuned.
#
# Why each one matters:
#   journal_mode=WAL   — readers don't block the single writer. This setting is
#                        durable (persists in the DB file), but re-asserting it
#                        on every open is harmless and keeps intent explicit.
#   busy_timeout=10000 — milliseconds to wait+retry on a locked DB instead of
#                        failing instantly with "database is locked". The
#                        gateway, every CLI command (``nerve sync``/``doctor``/
#                        ``db prune``), and the scheduled backup are separate
#                        connections to one file; WAL allows a single writer,
#                        so the others must briefly queue rather than error.
#   synchronous=NORMAL — safe under WAL (no corruption risk; only the most
#                        recent transaction can be lost on an OS/power crash)
#                        and skips the fsync that FULL forces on *every* commit
#                        — the per-commit cost behind write-lock "wait hours".
#   foreign_keys=ON    — enforce FK constraints (per-connection; off by default).
#   temp_store=MEMORY  — keep temp tables/indices in RAM.
#   cache_size=-16000  — ~16 MB page cache (negative value = KiB).
#
# All but ``journal_mode`` are *per-connection* and must be re-applied on every
# open — which is exactly why this is centralized rather than set inline.
_DEFAULT_PRAGMAS: dict[str, object] = {
    "journal_mode": "WAL",
    "busy_timeout": 10000,
    "synchronous": "NORMAL",
    "foreign_keys": "ON",
    "temp_store": "MEMORY",
    "cache_size": -16000,
}


class Database(
    SessionStore,
    MessageStore,
    TaskStore,
    TaskStatusStore,
    PlanStore,
    NotificationStore,
    SourceStore,
    CronStore,
    SkillStore,
    McpStore,
    AuditStore,
    UsageStore,
    FileStore,
    WakeupStore,
    MaintenanceStore,
):
    """Async SQLite database wrapper.

    Provides connection management, write serialization, schema migrations,
    and all domain-specific data access methods via mixin inheritance.
    """

    def __init__(self, db_path: Path, workspace: Path | None = None):
        self.db_path = db_path
        # Workspace root used to resolve task file_path values during FTS
        # reseed. Defaults to the DB's parent dir for backward compatibility,
        # but production passes the configured workspace (task files live in
        # the workspace, NOT next to the DB in ~/.nerve).
        self.workspace = workspace
        self._db: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()
        # Per-connection pragmas (see _DEFAULT_PRAGMAS). Copied per instance so
        # a caller or test can tune them before connect() (e.g. busy_timeout=0).
        self._pragmas: dict[str, object] = dict(_DEFAULT_PRAGMAS)

    async def _apply_pragmas(self) -> None:
        """Apply the connection pragmas (see :data:`_DEFAULT_PRAGMAS`).

        Pragma names and values are module-controlled constants, never user
        input, so interpolating them into the statement is safe.
        """
        for name, value in self._pragmas.items():
            await self.db.execute(f"PRAGMA {name}={value}")

    async def connect(self) -> None:
        """Open the database connection, tune it, and apply migrations."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self.db_path))
        self._db.row_factory = aiosqlite.Row
        # Apply pragmas BEFORE migrations so the migration writes also run under
        # the tuned busy_timeout/synchronous settings and contend politely.
        await self._apply_pragmas()
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
            # Read content from disk files (source of truth) instead of seeding
            # empty. Task file_path values are relative to the workspace root,
            # which is NOT the DB directory (~/.nerve) — fall back to it only
            # when no workspace was provided.
            workspace = (self.workspace or self.db_path.parent).expanduser()
            async with self.db.execute("SELECT id, title, file_path FROM tasks") as cur:
                rows = await cur.fetchall()
            for row in rows:
                content = ""
                try:
                    fp = workspace / row["file_path"]
                    if fp.exists():
                        content = await asyncio.to_thread(
                            fp.read_text, encoding="utf-8",
                        )
                except Exception as e:
                    logger.warning("Failed to read %s for FTS reseed: %s", row["file_path"], e)
                await self.db.execute(
                    "INSERT INTO tasks_fts (task_id, title, content) VALUES (?, ?, ?)",
                    (row["id"], row["title"], content),
                )
            await self.db.commit()
            logger.info("FTS reseeded with %d tasks (content from disk)", task_count)
