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


# Group/world permission bits, split by the hazard each poses to the state DB.
#   write (020/002) — another user can replace nerve.db or rewrite accounts,
#                     actors and history. An integrity hazard, on the files AND
#                     the directory (a writable dir lets a 0600 file be swapped).
#   read  (040/004) — another user can read a signing secret out of the file.
#                     A confidentiality hazard, and only on the files: read on
#                     the directory is ordinary traversal (0755) and harmless.
_GROUP_WORLD_WRITE = 0o022
_GROUP_WORLD_READ = 0o044

# _mode_of sentinel: the path does not exist (which is fine — an absent sidecar,
# a fresh install). Distinct from ``None``, which means the path exists but its
# mode could not be read, and must fail closed (an attacker cannot make a file
# uninspectable to hide a wide mode, but we must not assume secure either).
_ABSENT = object()


class InsecureStateStorage(RuntimeError):
    """The state directory or database files are not owner-only in a way this
    layer will not repair on its own.

    Raised by :meth:`Database.connect` *before* the database is opened or
    migrated when a database file, a sidecar or the state directory is
    group/world-writable, or when its mode cannot be read. Write access by
    another user means the contents — accounts, actors, history — may have
    been altered; chmod'ing that away would erase the evidence and then trust
    the result. The operator acknowledges by fixing the modes by hand. Also
    raised by the bootstrap when a database-held signing secret would have to
    live in a file other users can read (confidentiality).
    """


def _mode_of(path: Path):
    """Permission bits of ``path``; ``_ABSENT`` if it does not exist; ``None``
    if it exists but cannot be inspected (logged, and treated as unsafe)."""
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

    ``writable`` (files or the directory) and ``uninspectable`` are *integrity*
    hazards — another user could replace or rewrite the database — and are fatal
    whatever the signing-secret arrangement is; :meth:`Database.connect` refuses
    to open on them and repairs nothing. ``readable`` (files only) is a
    *confidentiality* hazard: repaired automatically, and fatal at bootstrap
    only when a database-held secret would be used and the repair failed.
    ``exposed_before_repair`` records that a database file carried group/world
    read bits when first observed, before any chmod: the trigger to rotate a
    stored signing key, since it may already have been copied.
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
    """(path, desired mode, is a database file) for the directory and every
    database file SQLite may leave beside the main one."""
    targets = [(db_path.parent, _STATE_DIR_MODE, False)]
    targets.extend(
        (Path(f"{db_path}{suffix}"), _DB_FILE_MODE, True) for suffix in _DB_FILE_SUFFIXES
    )
    return targets


def _inspect_state_permissions(db_path: Path) -> StatePermissions:
    """Classify the current modes without changing anything.

    The pre-open snapshot. Anything ``writable`` or ``uninspectable`` here is
    evidence the contents may have been altered, and is what
    :meth:`Database.connect` refuses on; ``readable``/``exposed_before_repair``
    is what it repairs and rotates for.
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
    """Make the state directory 0700 and the database files 0600, verify, and
    classify whatever could not be secured.

    Only ever reached after :func:`_inspect_state_permissions` found no
    integrity hazard, so what it repairs is read exposure and the directory
    mode. Idempotent: a mode already right is left alone and an absent path
    skipped. Every change is *verified* by re-reading the mode, never trusting
    chmod's return, because on a filesystem without modes a chmod can succeed
    and change nothing. Read exposure on a database file *before* repair is
    remembered so a possibly-copied signing key can be rotated even after the
    mode is fixed.
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
        # What connect() found when it tried to make the state files owner-only.
        # Secured on every ordinary filesystem. The identity bootstrap consults
        # it before it trusts — or stores a signing secret in — the database
        # (see nerve.migrate._refuse_insecure_secret_storage).
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

        The state-file policy is enforced here, so every opener — the gateway,
        each CLI command, the installer, tests — gets the same treatment:

        1. Inspect before touching anything. A group/world-**writable** database
           file, sidecar or state directory, or one whose mode cannot be read,
           means another user may have altered the contents. That is evidence,
           not something to chmod away: refuse to open (no migration, no
           repair) with the manual remedy, so the operator acknowledges it.
        2. Repair what is repairable: read exposure on the files and the
           directory mode, verified after the chmod.
        3. Migrate, then — if a database file was readable before the repair —
           retire the stored signing secret, which may have been copied, and
           drop a matching process pin so nothing accepts it meanwhile.
        """
        # A directory this call creates is created owner-only; one that already
        # exists is judged as found. mkdir's mode is subject to the umask, which
        # can only remove bits, so this never widens anything.
        self.db_path.parent.mkdir(mode=_STATE_DIR_MODE, parents=True, exist_ok=True)
        pre = _inspect_state_permissions(self.db_path)
        if pre.writable or pre.uninspectable:
            raise InsecureStateStorage(_refusal_message(self.db_path, pre))
        if not self.db_path.exists():
            # Create the file owner-only *before* SQLite does. SQLite creates a
            # new database at 0644-under-umask and gives the -wal/-shm sidecars
            # the main file's mode, so fixing the mode first covers all three
            # with no window in which the file is wider than intended.
            self.db_path.touch(mode=_DB_FILE_MODE)
        self._db = await aiosqlite.connect(str(self.db_path))
        # The connection now exists, and aiosqlite keeps a live non-daemon
        # thread behind it, so *every* failure below has to close it: a
        # migration that raised used to leave both in place — the caller sees
        # the exception, the thread goes on holding the process open, and a
        # retry opens a second one. BaseException, so a cancellation partway
        # through cleans up too.
        try:
            self._db.row_factory = aiosqlite.Row
            # Apply pragmas BEFORE migrations so the migration writes also run
            # under the tuned busy_timeout/synchronous settings and contend
            # politely.
            await self._apply_pragmas()
            # journal_mode=WAL has now opened the sidecars; a -wal/-shm pair that
            # already existed from an earlier, wider run keeps its old mode until
            # this pass tightens it. Its verdict is what the bootstrap reads.
            self.state_permissions = _repair_state_permissions(self.db_path)
            if self.state_permissions.writable or self.state_permissions.uninspectable:
                # Not reachable for anything the pre-open inspection saw; a
                # sidecar SQLite just created inherits the main file's 0600.
                # Kept as the backstop for whatever else a filesystem might do.
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
            # After migrations (the table exists) and after repair: a key that was
            # readable by other users is compromised and must not be reused.
            if exposed:
                await self._rotate_exposed_signing_secret()
            await self._check_fts_integrity()
        except BaseException:
            db, self._db = self._db, None
            try:
                await db.close()
            except Exception as e:  # noqa: BLE001 — never mask the real failure
                logger.warning(
                    "Closing %s after a failed connect raised: %s", self.db_path, e,
                )
            raise

    async def _rotate_exposed_signing_secret(self) -> None:
        """Retire a database-held signing secret that was readable by other
        users before connect() repaired the mode — on disk *and* in memory.

        The key may already have been copied, so re-securing the file is not
        enough: the row is deleted, and if this process had that very key
        pinned, the pin is dropped so requests fail closed until the bootstrap
        pins the replacement (:func:`nerve.migrate.ensure_jwt_secret` generates
        a fresh one). Runs in the connect path so it covers every opener
        (gateway, ``nerve migrate``, the installer), not just the gateway
        process. Only an actually-stored key is rotated: a fresh 0644 database
        from old code that never held one is not an exposure.
        """
        from nerve.db.accounts import JWT_SECRET_NAME

        async with self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='instance_secrets'"
        ) as cur:
            if await cur.fetchone() is None:
                return  # pre-v047 or a non-nerve database
        stored = await self.get_instance_secret(JWT_SECRET_NAME)
        if stored is None:
            return
        await self.delete_instance_secret(JWT_SECRET_NAME)
        logger.warning(
            "%s (or a sidecar) was readable by other users; the stored JWT signing "
            "secret is treated as compromised and has been retired. A fresh one is "
            "generated at bootstrap and existing sessions must re-authenticate. If "
            "this is not the daemon's own process and the daemon is running, restart "
            "it: it may still hold the retired key pinned until then.",
            self.db_path,
        )
        # Memory must agree with disk: a verifier still holding the retired key
        # would keep accepting tokens minted with the copy. Imported lazily —
        # the gateway module is heavier than this layer needs at import time.
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
