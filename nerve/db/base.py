"""Core Database class — connection management, write lock, migrations, and FTS health check.

The Database class composes all domain-specific mixin stores via multiple
inheritance.  External code continues to import ``Database`` from ``nerve.db``
(via the package ``__init__.py``), so the public API is unchanged.
"""

from __future__ import annotations

import asyncio
import logging
import os
import stat
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, NamedTuple

import aiosqlite

from nerve.db.accounts import AccountStore
from nerve.db.audit import AuditStore
from nerve.db.cron import CronStore
from nerve.db.files import FileStore
from nerve.db.maintenance import MaintenanceStore
from nerve.db.mcp import McpStore
from nerve.db.messages import MessageStore
from nerve.db.migrations.runner import discover_migrations, run_migrations
from nerve.db.notifications import NotificationStore
from nerve.db.plans import PlanStore
from nerve.db.review_loops import ReviewLoopStore
from nerve.db.sessions import SessionStore
from nerve.db.skills import SkillStore
from nerve.db.sources import SourceStore
from nerve.db.task_statuses import TaskStatusStore
from nerve.db.tasks import TaskStore
from nerve.db.usage import UsageStore
from nerve.db.wakeups import WakeupStore
from nerve.db.workflow_runs import WorkflowRunStore

logger = logging.getLogger(__name__)

# SCHEMA_VERSION is derived from the highest migration file number.
# This keeps a single source of truth (the migration files themselves).
SCHEMA_VERSION = max(v for v, _ in discover_migrations()) if discover_migrations() else 0


class WriteResult(NamedTuple):
    """Outcome of a single-statement write executed via :meth:`Database._write`.

    Mirrors the two cursor attributes writer call sites actually use. The
    live cursor is closed before the commit, so callers can never fetch from
    a cursor whose transaction has already ended.
    """

    lastrowid: int | None
    rowcount: int


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


# Owner-only modes for the state directory and the database files. nerve.db
# holds the JWT signing secret generated for installs without auth.jwt_secret
# (``instance_secrets``) — a credential that used to live only in a 0600 config
# file — so the files that carry it must be no weaker, and neither may the
# directory that lists them. Re-asserted on every connect rather than only at
# creation, which is what covers installs created under a permissive umask
# before this existed and databases put in place by a restore.
_STATE_DIR_MODE = 0o700
_DB_FILE_MODE = 0o600
# The main file plus every sidecar SQLite may leave beside it.
_DB_FILE_SUFFIXES = ("", "-wal", "-shm", "-journal")


# Any of these bits set on a database file means another user on the box can
# read it — the condition that makes keeping a signing secret in it a hazard.
_GROUP_WORLD_BITS = 0o077


def _mode_of(path: Path) -> int | None:
    """Permission bits of ``path``; None when it does not exist or cannot be
    inspected (which is logged)."""
    try:
        return stat.S_IMODE(path.stat().st_mode)
    except FileNotFoundError:
        return None
    except OSError as e:
        logger.warning("Could not inspect permissions on %s: %s", path, e)
        return None


def _harden_state_permissions(db_path: Path) -> list[tuple[Path, int]]:
    """Make the state directory 0700 and the database files 0600; report what
    stayed readable by other users.

    Idempotent: a mode that is already right is left alone and a file that
    does not exist is skipped. Every change is *verified* with a second
    ``stat``, because on a filesystem without modes (some network and
    FAT-style mounts) a ``chmod`` can succeed and change nothing. Returns the
    database files — the main file and any existing sidecar — that still
    carry group or world bits afterwards; an empty list means secured. The
    directory is tightened on the same terms but only warned about: with the
    files themselves ``0600`` it is not load-bearing.

    Never raises over a permissions problem. Whether an unsecured database is
    tolerable depends on whether a signing secret would have to live in it,
    and that is the bootstrap's decision
    (:func:`nerve.migrate.ensure_jwt_secret`), not this layer's.
    """
    unsecured: list[tuple[Path, int]] = []
    targets = [(db_path.parent, _STATE_DIR_MODE, False)]
    targets.extend(
        (Path(f"{db_path}{suffix}"), _DB_FILE_MODE, True) for suffix in _DB_FILE_SUFFIXES
    )
    for path, mode, is_db_file in targets:
        current = _mode_of(path)
        if current is None:
            continue
        if current != mode:
            try:
                os.chmod(path, mode)
            except OSError as e:
                logger.warning(
                    "Could not restrict permissions on %s to %04o (currently %04o): %s",
                    path, mode, current, e,
                )
            # Trust the filesystem's answer, not chmod's return.
            current = _mode_of(path)
            if current is None:
                continue
        if not current & _GROUP_WORLD_BITS:
            continue
        if is_db_file:
            unsecured.append((path, current))
        else:
            logger.warning(
                "State directory %s is %04o (expected %04o) and could not be tightened; "
                "the database files inside it are what matter, and they are checked "
                "separately.",
                path, current, mode,
            )
    return unsecured


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
    WorkflowRunStore,
    ReviewLoopStore,
    AccountStore,
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
        # Database files still readable by other users after connect() tried to
        # tighten them, as (path, mode). Empty on every ordinary filesystem. The
        # identity bootstrap consults it before it would put a signing secret
        # in the database (see nerve.migrate.ensure_jwt_secret).
        self.unsecured_files: list[tuple[Path, int]] = []

    @property
    def state_secured(self) -> bool:
        """Whether the database files are readable by this user only."""
        return not self.unsecured_files

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
        # Owner-only before anything is written: the directory, and any
        # database files a previous run or a restore left wider than that.
        _harden_state_permissions(self.db_path)
        if not self.db_path.exists():
            # Create the file owner-only *before* SQLite does. SQLite creates a
            # new database at 0644-under-umask and gives the -wal/-shm sidecars
            # the main file's mode, so fixing the mode first covers all three
            # with no window in which the file is wider than intended.
            self.db_path.touch(mode=_DB_FILE_MODE)
        self._db = await aiosqlite.connect(str(self.db_path))
        self._db.row_factory = aiosqlite.Row
        # Apply pragmas BEFORE migrations so the migration writes also run under
        # the tuned busy_timeout/synchronous settings and contend politely.
        await self._apply_pragmas()
        # journal_mode=WAL has now opened the sidecars; a -wal/-shm pair that
        # already existed from an earlier, wider run keeps its old mode until
        # this second pass tightens it. Its verdict is what the bootstrap reads.
        self.unsecured_files = _harden_state_permissions(self.db_path)
        if self.unsecured_files:
            logger.error(
                "Database files readable by other users and not tightenable: %s "
                "(expected %04o). No signing secret will be kept in this database; "
                "startup refuses unless auth.jwt_secret is configured.",
                ", ".join(f"{p} is {m:04o}" for p, m in self.unsecured_files),
                _DB_FILE_MODE,
            )
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

    # -- Write path ---------------------------------------------------------
    #
    # All writes on the single shared connection MUST go through ``_atomic()``
    # (multi-statement transactions) or ``_write()`` (single statements).
    # Both serialize under ``_write_lock`` and guarantee the connection is
    # never left inside an open transaction — on success they COMMIT, on any
    # error (including ``asyncio.CancelledError``) they ROLLBACK.
    #
    # Why this is load-bearing (production outage, 2026-07-06): a write task
    # was abandoned mid-transaction, leaving the shared connection inside an
    # open transaction pinned to a WAL read snapshot. A second process then
    # committed to the same DB file, making that snapshot stale. From that
    # moment every write on the shared connection failed *instantly* with
    # "database is locked" (SQLITE_BUSY_SNAPSHOT — the busy handler is
    # deliberately not invoked for snapshot conflicts, so ``busy_timeout``
    # does not apply), reads silently served the frozen snapshot, and nothing
    # ever called ROLLBACK — wedging the daemon for 10 hours until a restart.
    # ``_heal_leaked_txn()`` is the belt-and-suspenders guard that recovers
    # from that state even if some future code path leaks a transaction.

    @asynccontextmanager
    async def _atomic(self) -> AsyncIterator[None]:
        """Serialize a multi-statement write and make it a real transaction.

        Once a coroutine begins a multi-statement write, no other coroutine
        can interleave writes before the commit. The body's statements are
        committed on success and rolled back on any exception — including
        task cancellation — so a failed body neither half-commits nor leaves
        the shared connection inside an open (poisoned) transaction.

        Statements inside the body must use ``self.db.execute(...)`` directly
        (never ``_write()``, which would deadlock on the non-reentrant lock).
        """
        async with self._write_lock:
            await self._heal_leaked_txn()
            try:
                yield
                # Shield so a cancellation arriving mid-commit cannot abandon
                # a half-finished transaction: the inner task runs to
                # completion on aiosqlite's worker thread regardless.
                await asyncio.shield(self.db.commit())
            except BaseException:
                await self._rollback_quietly()
                raise

    async def _write(self, sql: str, params: tuple | list = ()) -> WriteResult:
        """Execute one write statement and commit, under the write lock.

        The single-statement counterpart of :meth:`_atomic` with the same
        guarantees: serialized against all other writers (so its commit can
        never flush someone else's in-flight transaction) and commit-or-
        rollback semantics (so an error or cancellation can never leave the
        connection mid-transaction).
        """
        async with self._write_lock:
            await self._heal_leaked_txn()
            try:
                cursor = await self.db.execute(sql, params)
                result = WriteResult(cursor.lastrowid, cursor.rowcount)
                await cursor.close()
                await asyncio.shield(self.db.commit())
                return result
            except BaseException:
                await self._rollback_quietly()
                raise

    async def _heal_leaked_txn(self) -> None:
        """Roll back a leaked open transaction on the shared connection.

        Called under ``_write_lock``. Every legitimate transaction commits or
        rolls back before releasing the lock, so ``in_transaction`` being true
        here means some code path abandoned a transaction (see the write-path
        comment above for the outage this causes). Recover loudly.
        """
        if self.db.in_transaction:
            logger.error(
                "Leaked open transaction detected on the shared connection — "
                "rolling back to prevent a wedged write path "
                "(SQLITE_BUSY_SNAPSHOT poisoning)",
            )
            await self._rollback_quietly()

    async def _rollback_quietly(self) -> None:
        """Best-effort ROLLBACK that never raises and survives cancellation."""
        try:
            await asyncio.shield(self.db.rollback())
        except asyncio.CancelledError:
            # The shielded rollback still runs to completion on the worker
            # thread; re-raise so the caller's cancellation proceeds.
            raise
        except Exception:
            logger.exception("Rollback failed on the shared connection")

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
            # Read content from disk files (source of truth) instead of seeding
            # empty. Task file_path values are relative to the workspace root,
            # which is NOT the DB directory (~/.nerve) — fall back to it only
            # when no workspace was provided.
            workspace = (self.workspace or self.db_path.parent).expanduser()
            async with self._atomic():
                await self.db.execute("DELETE FROM tasks_fts")
                async with self.db.execute(
                    "SELECT id, title, file_path FROM tasks",
                ) as cur:
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
            logger.info("FTS reseeded with %d tasks (content from disk)", task_count)
