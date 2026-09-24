"""Core Database class — connection management, write lock, migrations, and FTS health check.

The Database class composes internal domain-specific stores via multiple
inheritance. Production code calls services and workflows; the storage methods
are not a public API.
"""

from __future__ import annotations

import asyncio
import logging
import os
import stat
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
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


# nerve.db can hold the generated JWT signing secret (``instance_secrets``), so
# the state directory and database files are owner-only. connect() applies
# these modes every time, which also covers old installs and restored files.
_STATE_DIR_MODE = 0o700
_DB_FILE_MODE = 0o600
# The main file and every sidecar SQLite can create.
_DB_FILE_SUFFIXES = ("", "-wal", "-shm", "-journal")


# Group/world write on the files or the directory lets another user replace or
# change the database (integrity). Group/world read on the files lets another
# user copy the signing secret (confidentiality). Read on the directory is
# ordinary traversal and is permitted.
_GROUP_WORLD_WRITE = 0o022
_GROUP_WORLD_READ = 0o044

# _mode_of result for a path that does not exist. ``None`` means the path
# exists but its mode cannot be read, which is treated as unsafe.
_ABSENT = object()


class InsecureStateStorage(RuntimeError):
    """The state directory or database files are not safe to use.

    :meth:`Database.connect` raises it before it opens the database when a
    database file, sidecar or the state directory is group/world-writable, or
    its mode cannot be read. Another user may have changed the contents, so the
    operator must check them and fix the modes by hand. The bootstrap also
    raises it when a generated signing secret would go into a readable file.
    """


class IdentityInvariantError(RuntimeError):
    """The migration-guaranteed singleton system actor is corrupt."""


def _mode_of(path: Path):
    """Permission bits of ``path``, ``_ABSENT`` if it does not exist, or
    ``None`` if its mode cannot be read."""
    try:
        return stat.S_IMODE(os.stat(path).st_mode)
    except FileNotFoundError:
        return _ABSENT
    except OSError as e:
        logger.warning("Could not inspect permissions on %s: %s", path, e)
        return None


@dataclass
class StatePermissions:
    """The modes of the state directory and database files, classified.

    ``writable`` and ``uninspectable`` are integrity hazards: connect() refuses
    to open and repairs nothing. ``readable`` (files only) is a confidentiality
    hazard: connect() repairs it, and the bootstrap refuses only if the repair
    failed and the secret would be stored in the database.
    ``exposed_before_repair`` is set when a database file was readable before
    any repair; a stored signing secret may have been copied and is rotated.
    """

    writable: list[tuple[Path, int]] = field(default_factory=list)
    uninspectable: list[Path] = field(default_factory=list)
    readable: list[tuple[Path, int]] = field(default_factory=list)
    exposed_before_repair: bool = False

    @property
    def integrity_hazards(self) -> list[str]:
        return [f"{p} is {m:04o} (group/world-writable)" for p, m in self.writable] + [
            f"{p} has a mode that cannot be read" for p in self.uninspectable
        ]

    @property
    def readable_hazards(self) -> list[str]:
        return [f"{p} is {m:04o}" for p, m in self.readable]

    @property
    def secured(self) -> bool:
        return not (self.writable or self.uninspectable or self.readable)


def _state_targets(db_path: Path) -> list[tuple[Path, int, bool]]:
    """(path, desired mode, is a database file) for the directory and each
    database file."""
    targets = [(db_path.parent, _STATE_DIR_MODE, False)]
    targets.extend(
        (Path(f"{db_path}{suffix}"), _DB_FILE_MODE, True) for suffix in _DB_FILE_SUFFIXES
    )
    return targets


def _inspect_state_permissions(db_path: Path) -> StatePermissions:
    """Classify the current modes without changing anything.

    connect() calls this before it opens the database.
    """
    perms = StatePermissions()
    for path, _desired, is_db_file in _state_targets(db_path):
        mode = _mode_of(path)
        if mode is _ABSENT:
            continue
        if mode is None:
            perms.uninspectable.append(path)
            continue
        if mode & _GROUP_WORLD_WRITE:
            perms.writable.append((path, mode))
        if is_db_file and (mode & _GROUP_WORLD_READ):
            perms.readable.append((path, mode))
            perms.exposed_before_repair = True
    return perms


def _repair_state_permissions(db_path: Path) -> StatePermissions:
    """Set the directory to 0700 and the database files to 0600, then classify
    what is still not secure.

    Each change is verified by reading the mode again: on a filesystem without
    Unix modes, chmod can succeed and change nothing.
    """
    perms = StatePermissions()
    for path, desired, is_db_file in _state_targets(db_path):
        pre = _mode_of(path)
        if pre is _ABSENT:
            continue
        if pre is None:
            perms.uninspectable.append(path)
            continue
        if is_db_file and (pre & _GROUP_WORLD_READ):
            perms.exposed_before_repair = True
        final = pre
        if pre != desired:
            try:
                os.chmod(path, desired)
            except OSError as e:
                logger.warning(
                    "Could not restrict permissions on %s to %04o (currently %04o): %s",
                    path, desired, pre, e,
                )
            final = _mode_of(path)  # trust the filesystem, not chmod's return
            if final is _ABSENT:
                continue
            if final is None:
                perms.uninspectable.append(path)
                continue
        if final & _GROUP_WORLD_WRITE:
            perms.writable.append((path, final))
        elif is_db_file and (final & _GROUP_WORLD_READ):
            perms.readable.append((path, final))
    return perms


def _refusal_message(db_path: Path, perms: StatePermissions) -> str:
    """The actionable text for an :class:`InsecureStateStorage` at open."""
    files = " ".join(
        str(p) for p in (Path(f"{db_path}{s}") for s in _DB_FILE_SUFFIXES) if p.exists()
    ) or str(db_path)
    return (
        f"Refusing to open {db_path}: {'; '.join(perms.integrity_hazards)}. Another "
        f"user could have altered the state (accounts, actors, history), so it is "
        f"not repaired automatically. If you trust the contents, acknowledge by "
        f"fixing the modes yourself — chmod 700 {db_path.parent}; chmod 600 {files} "
        f"— then start again."
    )


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
        self._system_actor_id: str | None = None
        # The state-file modes connect() found after its repair. The identity
        # bootstrap reads this before it stores a signing secret in the
        # database (nerve.migrate._refuse_insecure_secret_storage).
        self.state_permissions: StatePermissions = StatePermissions()

    @property
    def state_secured(self) -> bool:
        """Whether the state directory and database files are owner-only."""
        return self.state_permissions.secured

    async def _apply_pragmas(self) -> None:
        """Apply the connection pragmas (see :data:`_DEFAULT_PRAGMAS`).

        Pragma names and values are module-controlled constants, never user
        input, so interpolating them into the statement is safe.
        """
        for name, value in self._pragmas.items():
            await self.db.execute(f"PRAGMA {name}={value}")

    async def connect(self) -> None:
        """Open the database connection, tune it, and apply migrations.

        Every opener goes through this state-file policy:

        1. If a database file, sidecar or the state directory is
           group/world-writable, or its mode cannot be read, refuse to open.
           Another user may have changed the contents, so nothing is repaired.
        2. Remove group/world read bits and verify the result.
        3. Migrate. If a database file was readable before step 2, delete the
           stored signing secret, which may have been copied.
        """
        # A new directory is created owner-only. An existing one is checked
        # below as it is.
        self.db_path.parent.mkdir(mode=_STATE_DIR_MODE, parents=True, exist_ok=True)
        pre = _inspect_state_permissions(self.db_path)
        if pre.writable or pre.uninspectable:
            raise InsecureStateStorage(_refusal_message(self.db_path, pre))
        if not self.db_path.exists():
            # Create the file owner-only before SQLite creates it with the
            # umask mode. SQLite gives the -wal/-shm sidecars the main file's
            # mode, so this covers them too.
            self.db_path.touch(mode=_DB_FILE_MODE)
        self._db = await aiosqlite.connect(str(self.db_path))
        # aiosqlite runs a non-daemon thread for the connection, and that thread
        # keeps the process alive. Close the connection on any failure below,
        # including cancellation.
        try:
            self._db.row_factory = aiosqlite.Row
            # Apply pragmas BEFORE migrations so the migration writes also run
            # under the tuned busy_timeout/synchronous settings and contend
            # politely.
            await self._apply_pragmas()
            # After journal_mode=WAL, so existing -wal/-shm files are included.
            self.state_permissions = _repair_state_permissions(self.db_path)
            if self.state_permissions.writable or self.state_permissions.uninspectable:
                # A backstop: the check before open covers the known cases.
                raise InsecureStateStorage(
                    _refusal_message(self.db_path, self.state_permissions)
                )
            exposed = (
                pre.exposed_before_repair or self.state_permissions.exposed_before_repair
            )
            if self.state_permissions.readable:
                logger.error(
                    "Database files are readable by other users and could not be "
                    "tightened: %s (expected %04o). No signing secret will be kept in "
                    "this database; the bootstrap refuses unless auth.jwt_secret is "
                    "configured.",
                    "; ".join(self.state_permissions.readable_hazards), _DB_FILE_MODE,
                )
            await run_migrations(self._db)
            await self._cache_system_actor_id()
            # After migrations, so the table exists. A key that other users
            # could read is compromised.
            if exposed:
                await self._rotate_exposed_signing_secret()
            await self._check_fts_integrity()
        except BaseException:
            db, self._db = self._db, None
            self._system_actor_id = None
            try:
                await db.close()
            except Exception as e:  # noqa: BLE001 — never mask the real failure
                logger.warning(
                    "Closing %s after a failed connect raised: %s", self.db_path, e,
                )
            raise

    async def _cache_system_actor_id(self) -> None:
        """Validate and cache the one system actor created by migrations."""
        async with self.db.execute(
            "SELECT id FROM actor_refs WHERE kind = 'system'"
        ) as cursor:
            rows = await cursor.fetchall()
        if len(rows) != 1:
            raise IdentityInvariantError(
                "actor identity is corrupt: expected exactly one "
                f"actor_refs(kind='system') row, found {len(rows)}"
            )
        actor_id = rows[0]["id"]
        if not isinstance(actor_id, str) or not actor_id:
            raise IdentityInvariantError(
                "actor identity is corrupt: the system actor has no invariant id"
            )
        self._system_actor_id = actor_id

    @property
    def system_actor_id(self) -> str:
        """The migration-validated system actor id for this connection."""
        if self._system_actor_id is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._system_actor_id

    async def _rotate_exposed_signing_secret(self) -> None:
        """Delete a stored signing secret that other users could read, and
        unpin it if this process uses it.

        The key may have been copied, so fixing the file mode is not enough.
        Requests fail closed until the bootstrap generates and pins a new key.
        A database with no stored key has nothing to rotate.
        """
        from nerve.db.accounts import JWT_SECRET_NAME

        async with self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='instance_secrets'"
        ) as cur:
            if await cur.fetchone() is None:
                return  # pre-v047 or a non-nerve database
        stored = await self._get_instance_secret(JWT_SECRET_NAME)
        if stored is None:
            return
        await self._delete_instance_secret(JWT_SECRET_NAME)
        logger.warning(
            "%s (or a sidecar) was readable by other users; the stored JWT signing "
            "secret is treated as compromised and has been retired. A fresh one is "
            "generated at bootstrap and existing sessions must re-authenticate. If "
            "this is not the daemon's own process and the daemon is running, restart "
            "it: it may still hold the retired key pinned until then.",
            self.db_path,
        )
        # A pinned copy of the deleted key would still verify tokens signed with
        # it. Imported here to keep the gateway module out of this import path.
        from nerve.gateway.auth import pinned_jwt_secret, unpin_jwt_secret

        if pinned_jwt_secret() == stored:
            unpin_jwt_secret()
            logger.warning(
                "The retired signing secret was the one pinned in this process; it "
                "has been unpinned, so requests fail closed until the bootstrap pins "
                "its replacement.",
            )

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None
            self._system_actor_id = None

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
