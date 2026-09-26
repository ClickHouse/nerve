"""Backup and restore for Nerve state.

Produces a single portable bundle — ``nerve-backup-<host>-<ts>.tar.zst``
(or ``.tar.gz`` when zstandard is unavailable) — containing a consistent
snapshot of everything that makes a Nerve instance *this* instance:

- ``nerve.db``   — sessions, messages, tasks index, notifications, plans, usage,
  accounts and actor identity. ``--no-secrets`` empties its two credentials in
  the snapshot: the JWT signing secret in ``instance_secrets`` and each
  account's password hash in ``accounts.credential``
  (see :func:`_scrub_snapshot_secrets`). It also replaces secret values in the
  staged workspace ``config/*.yaml`` with ``${VAR}`` placeholders, because a
  tracked ``auth.password_hash`` is still a verifier
  (see :func:`_sanitised_config`).
- ``memu.sqlite`` — the entire long-term memory
- the memU sidecar dirs (``memu-conversations/``, ``memu-manual/``, ``memu-resources/``)
- secrets (``certs/``, ``mcp-token``, ``telegram_sync.session``, ``config.local.yaml``)
- cron jobs (``cron/``) and the config-dir pointer
- the workspace "BRAIN" (identity markdown, ``memory/``, ``scripts/``, ``skills/``)

The databases run in **WAL mode**, so a naive ``cp`` of the live files can
capture a torn snapshot. We use SQLite's online backup API
(:meth:`sqlite3.Connection.backup`) which produces a transactionally
consistent copy while writers continue — the core correctness win over a
file copy (the June-2026 box migration only worked because the service was
stopped first; an automated backup can't rely on that).

Design:

- **Local-dir targets only** in v1 — an external mount or a synced dir
  covers the off-box requirement. Cloud upload is a follow-up.
- **Verified restore** — checksums vs. manifest, ``PRAGMA integrity_check``
  on both DBs, and a schema-version guard so a newer-schema bundle never
  lands on older code.
- **Loud failures** — the scheduled job notifies on failure (silent
  backups that fail are worse than none).
- **Trash over rm** — ``restore --force`` relocates the old state dir to
  ``~/.nerve.pre-restore-<ts>`` rather than deleting it, and refuses to
  run against a live daemon (no override — stop it first).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import socket
import sqlite3
import stat
import subprocess
import tarfile
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Callable, Iterator

import yaml

logger = logging.getLogger(__name__)

# Bundle format revision — bump if the on-disk layout changes incompatibly.
FORMAT_VERSION = 1

# --- What goes in state/ (everything under ~/.nerve worth keeping) --------- #
# The two databases are snapshotted via the online-backup API (below); the
# rest are plain file/dir copies. Anything not named here is excluded by
# construction — that intentionally drops nerve.log, nerve.pid, the
# *-wal/-shm sidecars (folded into the snapshot), bin/, .crates*, and the
# stale memu.backup*.sqlite copies.
STATE_DB_FILES: tuple[str, ...] = ("nerve.db", "memu.sqlite")
STATE_DIRS: tuple[str, ...] = (
    "memu-conversations",
    "memu-manual",
    "memu-resources",
    "cron",
    "certs",
)
STATE_FILES: tuple[str, ...] = (
    "config_dir",
    "mcp-token",
    "telegram_sync.session",
)

# Secret members (relative to the bundle root) — omitted with --no-secrets
# and re-chmod'd to 0600 on restore. The JWT signing secret and the account
# password hashes are rows inside nerve.db; :func:`_scrub_snapshot_secrets`
# removes them.
SECRET_MEMBERS: frozenset[str] = frozenset({
    "state/certs",
    "state/mcp-token",
    "state/telegram_sync.session",
    "config/config.local.yaml",
})
# Files within the bundle whose mode must be 0600 after restore. nerve.db is
# one of them because it can hold the generated JWT signing secret.
SECRET_FILE_MODE = 0o600
# Owner-only state directory on restore, as in nerve.db.base._STATE_DIR_MODE.
_STATE_DIR_MODE = 0o700
# Group/world write bits. Another user can rename or replace entries in a
# directory with either bit set, so it cannot hold staged secrets (see
# :func:`_stage_parent`).
_GROUP_WORLD_WRITE = 0o022
# The re-chmod loop in restore skips ``nerve.db``, because
# :func:`_secure_install_db` installs it owner-only.
_SECRET_RESTORE_PATHS: tuple[str, ...] = (
    "nerve.db",
    "mcp-token",
    "telegram_sync.session",
)

# --- Workspace BRAIN allowlist --------------------------------------------- #
# The workspace is typically buried in tens of GB of repo/build junk, so we
# take an *allowlist* of the small, irreplaceable "brain": identity markdown
# at the root plus a few known directories. Extra excludes from config are
# applied *within* these.
#
# ``config`` carries the shareable settings and the cron jobs. It is tracked in
# the workspace repo, but so are ``skills`` and ``memory`` — "it is in git" has
# never been the rule here, and a backup taken to survive a lost machine is
# worth little if restoring it brings back no schedule. Nothing in it is secret
# by construction: values that matter are ``${ENV_VAR}`` references, and the
# real secrets live in the machine-local overlay that is already handled
# separately.
WORKSPACE_INCLUDE_DIRS: tuple[str, ...] = ("config", "memory", "scripts", "skills")
WORKSPACE_INCLUDE_FILE_GLOBS: tuple[str, ...] = ("*.md",)

# What restore may write back, which is deliberately *not* what backup collects.
# The asymmetry is the point, and it is one directory wide: `config` is captured
# so a lost machine loses nothing, and never written back, because the authority
# for tracked config is the git remote rather than a tarball. Writing it back
# would replace reviewed config on a locked instance, and on any instance leave
# the checkout dirty enough that the next sync refuses to merge.
#
# These must stay separate constants. Defining the restore side as "whatever
# backup collects" is what silently opened that hole the moment `config` was
# added above — one edit, two opposite meanings.
WORKSPACE_RESTORE_DIRS: tuple[str, ...] = ("memory", "scripts", "skills")

# Directory names always pruned while walking the included workspace dirs.
_PRUNE_DIR_NAMES: frozenset[str] = frozenset({
    ".git", "node_modules", ".venv", "venv", "__pycache__",
    ".mypy_cache", ".pytest_cache", ".ruff_cache",
})

# Warn if the workspace payload exceeds this — a sign the junk exclusion
# missed something (e.g. a build artifact landed under memory/).
WORKSPACE_WARN_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB


def _restorable_workspace_path(rel: Path) -> bool:
    """Whether a workspace path from a bundle may be written back on restore.

    Restore applies :data:`WORKSPACE_RESTORE_DIRS`, not the collect-side
    allowlist. Bundles this module writes *do* carry ``config/`` — deliberately,
    so a lost machine loses no settings or schedule — and a bundle it is handed
    can carry anything at all. Either way, writing ``config/settings.yaml`` or
    ``config/cron/gates/x.py`` back would let a tarball replace the git-tracked
    config subtree: on a locked instance that displaces the reviewed remote
    config the whole mode exists to guarantee, and on any instance it leaves the
    checkout dirty enough that the next sync refuses to merge.

    So the config subtree travels in the bundle and is never written back. It is
    still there to be read out and applied through review if a restore really
    needs it, which the skip warning says.
    """
    import fnmatch

    parts = rel.parts
    if len(parts) == 1:
        return any(fnmatch.fnmatch(parts[0], g) for g in WORKSPACE_INCLUDE_FILE_GLOBS)
    return parts[0] in WORKSPACE_RESTORE_DIRS

# Retention prune only ever touches files matching this exact pattern, so it
# can never delete an unrelated file that happens to share the directory.
# An optional ``-<n>`` disambiguates bundles created within the same second.
BUNDLE_RE = re.compile(r"^nerve-backup-.+-\d{8}-\d{6}(-\d+)?\.tar\.(zst|gz)$")


class BackupError(Exception):
    """Raised when a backup or restore operation cannot proceed safely."""


# --------------------------------------------------------------------------- #
#  Results                                                                     #
# --------------------------------------------------------------------------- #


@dataclass
class BackupResult:
    path: Path
    size: int
    compression: str
    counts: dict
    file_count: int
    include_secrets: bool
    include_workspace: bool
    workspace_bytes: int


@dataclass
class VerifyReport:
    ok: bool
    manifest: dict
    counts: dict
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def summary(self) -> str:
        c = self.counts
        parts = [
            f"sessions={c.get('sessions', '?')}",
            f"messages={c.get('messages', '?')}",
            f"tasks={c.get('tasks', '?')}",
            f"memU items={c.get('memu_items', '?')}",
        ]
        return ", ".join(parts)


# --------------------------------------------------------------------------- #
#  Low-level helpers                                                           #
# --------------------------------------------------------------------------- #


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _hostname() -> str:
    try:
        return socket.gethostname() or "unknown-host"
    except Exception:
        return "unknown-host"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# Busy timeout (seconds) for this module's own sqlite3 connections. The online
# snapshot reads the *live* nerve.db/memu.sqlite while the gateway is writing,
# so without a wait it could fail with "database is locked". The stdlib
# ``timeout=`` arg maps to sqlite3_busy_timeout under the hood; 10s matches the
# gateway's ``PRAGMA busy_timeout=10000`` (see ``nerve/db/base.py``).
_SQLITE_TIMEOUT = 10.0


def _connect(path: Path) -> sqlite3.Connection:
    """Open a sqlite3 connection with the shared busy timeout applied."""
    return sqlite3.connect(str(path), timeout=_SQLITE_TIMEOUT)


def _nerve_version() -> str:
    try:
        from importlib.metadata import version

        return version("nerve")
    except Exception:
        return "unknown"


def _git_sha() -> str:
    """Best-effort git SHA of the installed source checkout."""
    try:
        source_root = Path(__file__).resolve().parent.parent
        if not (source_root / ".git").exists():
            return ""
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=source_root,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if out.returncode == 0:
            return out.stdout.strip()
    except Exception:
        pass
    return ""


def _unique_bundle_path(
    output_dir: Path, host: str, stamp: str, ext: str,
) -> Path:
    """Pick a non-colliding bundle path.

    The second-resolution timestamp can collide when two backups land in the
    same second (rapid manual runs, tests). Append ``-2``, ``-3``, … until the
    name (and its ``.tmp`` sibling) is free.
    """
    base = f"nerve-backup-{host}-{stamp}"
    candidate = output_dir / f"{base}.{ext}"
    n = 2
    while candidate.exists() or (output_dir / (candidate.name + ".tmp")).exists():
        candidate = output_dir / f"{base}-{n}.{ext}"
        n += 1
    return candidate


def _zstd_available() -> bool:
    try:
        import zstandard  # noqa: F401

        return True
    except Exception:
        return False


def _snapshot_db(src: Path, dst: Path) -> None:
    """Copy a (possibly live, WAL-mode) SQLite DB consistently.

    Uses the online-backup API so writers are never blocked and the copy is
    transactionally consistent — including any pages still living in the WAL.
    Verifies the copy with ``PRAGMA integrity_check`` and raises
    :class:`BackupError` on any inconsistency.
    """
    if not src.exists():
        raise BackupError(f"database not found: {src}")

    source = _connect(src)
    try:
        dest = _connect(dst)
        try:
            # pages>0 copies incrementally, retrying pages dirtied by a
            # concurrent writer rather than holding a long read lock.
            source.backup(dest, pages=4096)
        finally:
            dest.close()
    finally:
        source.close()

    check = _connect(dst)
    try:
        row = check.execute("PRAGMA integrity_check").fetchone()
    finally:
        check.close()
    if not row or row[0] != "ok":
        raise BackupError(
            f"integrity_check failed for snapshot of {src.name}: {row!r}"
        )


def _scrub_instance_secrets(snapshot: Path) -> None:
    """Empty ``instance_secrets`` in a snapshot copy of nerve.db.

    The table holds the generated JWT signing secret, which can mint tokens for
    the live instance, so ``--no-secrets`` must not carry it. This edits the
    snapshot before it is checksummed and never touches the live database.
    ``secure_delete`` overwrites the freed pages. A snapshot without the table
    is left as is. The restored instance generates a new secret on first start.
    """
    conn = _connect(snapshot)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='instance_secrets'"
        ).fetchone()
        if not exists:
            return
        conn.execute("PRAGMA secure_delete=ON")
        conn.execute("DELETE FROM instance_secrets")
        conn.commit()
    finally:
        conn.close()


def _scrub_account_credentials(snapshot: Path) -> None:
    """Empty ``accounts.credential`` in a *snapshot* copy of nerve.db.

    ``--no-secrets`` removes every account hash and sets each
    ``credential_source`` to ``none``. Changing the source also handles
    transitional ``config`` rows whose hash lived in the omitted
    ``config.local.yaml``; leaving them on ``config`` would restore an account
    that cannot authenticate but is not considered passwordless.

    A restored single-account database is passwordless. With multiple accounts,
    nobody can log in until credentials are restored from a secrets-bearing
    backup.

    Edits the snapshot after it is taken and before it is checksummed; the live
    database is never touched. ``secure_delete`` makes SQLite overwrite the
    freed pages rather than merely unlink them. A snapshot from before the table
    existed is left alone.
    """
    conn = _connect(snapshot)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='accounts'"
        ).fetchone()
        if not exists:
            return
        conn.execute("PRAGMA secure_delete=ON")
        conn.execute(
            "UPDATE accounts SET credential = NULL, credential_source = 'none'"
        )
        conn.commit()
        left = conn.execute(
            "SELECT COUNT(*) FROM accounts "
            "WHERE credential IS NOT NULL OR credential_source != 'none'"
        ).fetchone()[0]
        if left:  # pragma: no cover - an UPDATE that reported success and did not
            raise BackupError(
                f"could not scrub account credentials from {snapshot}: "
                f"{left} row(s) still carry one, or still read the configured one"
            )
    finally:
        conn.close()


# Configuration files in the bundle that are rewritten rather than copied when
# ``--no-secrets`` is in force. Everything under the workspace's ``config/``:
# ``settings.yaml`` and the cron job files, whose env blocks are as good a place
# for a credential as any.
def _is_sanitisable_config(arcname: str) -> bool:
    head, _, rest = arcname.partition("/")
    return head == "config" and bool(rest) and arcname.endswith((".yaml", ".yml"))


def _sanitised_config(src: Path) -> str | None:
    """``src`` with every secret leaf replaced by a ``${VAR}`` placeholder.

    Returns ``None`` when the file parses and contains no secret values, so the
    caller may copy it unchanged. Raises :class:`BackupError` when the file
    cannot be inspected because a no-secrets bundle cannot safely copy an
    unparseable configuration file.

    Tracked or fleet-managed configuration may still contain
    ``auth.password_hash`` because startup does not rewrite those files. The
    scrubber removes it and every other recognized secret.
    """
    from nerve.migrate import _scrub_secrets

    try:
        raw = yaml.safe_load(src.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as e:
        raise BackupError(
            f"Backup: {src} could not be parsed, so a --no-secrets bundle cannot "
            f"promise it carries no credential ({e}). Fix the file, or take the "
            f"backup with secrets and keep it as private as the instance."
        ) from e
    if not isinstance(raw, dict):
        raise BackupError(
            f"Backup: {src} is not a configuration mapping, so a --no-secrets "
            f"bundle cannot promise it carries no credential. Fix the file, or "
            f"take the backup with secrets and keep it as private as the instance."
        )
    tracked, _secrets, moved = _scrub_secrets(raw)
    if not moved:
        return None
    logger.info(
        "Backup: scrubbed %d credential-shaped value(s) from %s (--no-secrets)",
        len(moved), src.name,
    )
    return (
        "# Nerve backup: taken with --no-secrets. Credential-shaped values were\n"
        "# replaced with ${ENV_VAR} placeholders and are NOT in this archive.\n\n"
        + yaml.safe_dump(tracked, default_flow_style=False, sort_keys=False)
    )


def _stage_config_file(src: Path, dst: Path, *, include_secrets: bool) -> None:
    """Put one configuration file in the stage, sanitised if it has to be.

    A rewritten file is created owner-only and written through the same
    descriptor, so sanitized secret-bearing input is never staged at a wider
    mode.
    """
    text = None if include_secrets else _sanitised_config(src)
    if text is None:
        shutil.copy2(src, dst)
        return
    fd = _secure_create(dst, "Backup")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", closefd=False) as out:
            out.write(text)
            out.flush()
            os.fsync(out.fileno())
        _verify_still_the_created_file(fd, dst, "Backup")
    finally:
        os.close(fd)


def _scrub_snapshot_secrets(snapshot: Path) -> None:
    """Take every credential out of a ``--no-secrets`` snapshot of nerve.db.

    Removes the signing secret and account password hashes. Add any future
    database credential to this function.
    """
    _scrub_instance_secrets(snapshot)
    _scrub_account_credentials(snapshot)


def _mode_is_private(st_mode: int) -> bool:
    """True when no group/world bit is set on a stat mode."""
    return (stat.S_IMODE(st_mode) & 0o077) == 0


def _scrub_db_credentials(db_file: Path) -> None:
    """Take every credential out of a database file that could not be made
    private, and verify. The restore fails either way; this is about what is
    left on disk when it does."""
    _scrub_db_secret(db_file)
    _scrub_account_credentials(db_file)


def _scrub_db_secret(db_file: Path) -> None:
    """Delete the stored JWT signing secret from ``db_file`` and verify it is gone.

    Raises :class:`BackupError` on any failure, so a scrub that did nothing is
    never reported as done. A database without ``instance_secrets`` (an older
    bundle) is detected by a table lookup, so an I/O or lock error is not
    mistaken for it.
    """
    try:
        conn = _connect(db_file)
    except sqlite3.Error as e:
        raise BackupError(f"could not open {db_file} to scrub the signing secret: {e}") from e
    try:
        has_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='instance_secrets'"
        ).fetchone()
        if not has_table:
            return
        conn.execute("PRAGMA secure_delete=ON")
        conn.execute("DELETE FROM instance_secrets WHERE name='jwt_secret'")
        conn.commit()
        left = conn.execute(
            "SELECT COUNT(*) FROM instance_secrets WHERE name='jwt_secret'"
        ).fetchone()[0]
        if left:
            raise BackupError(
                f"could not scrub the signing secret from {db_file}: the row is still present"
            )
    except sqlite3.Error as e:
        raise BackupError(f"could not scrub the signing secret from {db_file}: {e}") from e
    finally:
        conn.close()


def _secure_directory(path: Path) -> None:
    """Make ``path`` 0700 and verify it.

    Raises :class:`BackupError` if a group/world bit remains or the mode cannot
    be read. Nothing secret is written into a directory that fails this check.
    """
    try:
        os.chmod(path, _STATE_DIR_MODE)
    except OSError as e:
        logger.warning("Restore: could not set %s to %04o: %s", path, _STATE_DIR_MODE, e)
    try:
        st_mode = os.stat(path).st_mode
    except OSError as e:
        raise BackupError(f"Restore: cannot inspect the mode of {path}: {e}") from e
    if not _mode_is_private(st_mode):
        raise BackupError(
            f"Restore: {path} is {stat.S_IMODE(st_mode):04o} and could not be made "
            f"{_STATE_DIR_MODE:04o}; refusing to restore secret-bearing state into a "
            f"directory other users can reach. Fix the filesystem or choose another "
            f"state directory."
        )


def _exclusive_create(path: Path, what: str) -> tuple[int, bool]:
    """Create ``path`` as a new file and return ``(fd, private)``.

    ``O_CREAT|O_EXCL|O_NOFOLLOW`` at mode 0600 fails if the name exists, so a
    planted symlink is never followed. ``private`` is the mode read back
    through the descriptor; ``False`` means the filesystem ignored the mode,
    and the caller decides what to do. All writes go through ``fd``. Raises
    :class:`BackupError` if the file cannot be created.
    """
    path.unlink(missing_ok=True)
    try:
        fd = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            SECRET_FILE_MODE,
        )
    except OSError as e:
        raise BackupError(f"{what}: cannot create {path}: {e}") from e
    try:
        return fd, _mode_is_private(os.fstat(fd).st_mode)
    except BaseException:
        os.close(fd)
        path.unlink(missing_ok=True)
        raise


def _secure_create(path: Path, what: str, *, detail: str = "nothing was copied") -> int:
    """Like :func:`_exclusive_create`, but raise unless the file is owner-only.

    The check reads the mode through the descriptor while the file is empty,
    so a filesystem that ignores 0600 is caught before anything is written.
    On failure no file is left behind.
    """
    fd, private = _exclusive_create(path, what)
    if not private:
        os.close(fd)
        path.unlink(missing_ok=True)
        raise BackupError(
            f"{what}: could not create {path} owner-only (the filesystem ignored "
            f"mode {SECRET_FILE_MODE:04o}); {detail}. Fix the filesystem "
            f"or choose a directory that supports Unix modes."
        )
    return fd


def _is_group_world_writable(path: Path) -> bool:
    """True when ``path`` exists and other users may write in it."""
    try:
        return bool(stat.S_IMODE(os.stat(path).st_mode) & _GROUP_WORLD_WRITE)
    except OSError:
        return False


def _unsafe_stage_reason(path: Path) -> str | None:
    """Why ``path`` may not hold a staging directory, or ``None`` if it may.

    Another user who can rename or replace an entry on the path can redirect
    the staged snapshot. So every component of the resolved path is checked:
    it must be a directory owned by this user or by root (an owner can rename
    children whatever the mode), and either not group/world-writable or sticky
    (as ``/tmp`` at 1777 is).
    """
    euid = os.geteuid()
    try:
        resolved = path.resolve(strict=True)
    except OSError as e:
        return f"{path} cannot be resolved ({e})"
    for component in [resolved, *resolved.parents]:
        try:
            st = os.stat(component, follow_symlinks=False)
        except OSError as e:
            return f"{component} cannot be inspected ({e})"
        if not stat.S_ISDIR(st.st_mode):
            return f"{component} is not a directory"
        if st.st_uid not in (euid, 0):
            return (
                f"{component} is owned by uid {st.st_uid}, who can rename anything "
                f"inside it regardless of its mode"
            )
        mode = stat.S_IMODE(st.st_mode)
        if (mode & _GROUP_WORLD_WRITE) and not (st.st_mode & stat.S_ISVTX):
            return f"{component} is {mode:04o}: writable by other users and not sticky"
    return None


def _stage_parent(nerve_dir: Path) -> Path:
    """Return a parent for the staging directory that no other user can rename.

    The output directory is not used: it is often a shared or mounted target,
    and the staged ``nerve.db`` snapshot holds accounts, history and (until
    ``--no-secrets`` scrubs it) the signing secret. The state directory is the
    first choice, because it is owner-only and on the same filesystem as the
    data. Otherwise the system temp directory is used if it passes
    :func:`_unsafe_stage_reason`. The path is always explicit, so ``mkdtemp``
    never reads ``TMPDIR`` again.
    """
    reason = (
        _unsafe_stage_reason(nerve_dir) if nerve_dir.is_dir()
        else f"{nerve_dir} is not a directory"
    )
    if reason is None and os.stat(nerve_dir).st_uid == os.geteuid():
        return nerve_dir
    logger.info("Backup: staging in the system temp directory instead — %s", reason)
    return _vetted_temp_dir(f"{nerve_dir} is not usable")


def _vetted_temp_dir(context: str = "") -> Path:
    """Return the resolved system temp directory if it passes the staging check.

    Staging a snapshot and extracting a bundle both put ``nerve.db`` here, so
    the directory gets the :func:`_unsafe_stage_reason` check. Raises
    :class:`BackupError` if it fails.
    """
    temp_dir = Path(tempfile.gettempdir())
    reason = _unsafe_stage_reason(temp_dir)
    if reason is not None:
        raise BackupError(
            f"Backup: nowhere safe to put the database. "
            + (f"{context}, and " if context else "")
            + f"the temp directory will not do either: {reason}. Another user "
            f"could replace that directory while the database — signing secret "
            f"included — is inside it. Point TMPDIR at a directory you own."
        )
    return temp_dir.resolve()


def _verify_same_file(fd: int, path: Path, what: str) -> None:
    """Raise :class:`BackupError` unless ``path`` still names the file open on ``fd``.

    Files are written through a descriptor but published by name with
    ``os.replace``. In between, a user with write access to the directory can
    replace the temporary with a symlink. Comparing device and inode, without
    following symlinks, detects that.
    """
    st = os.fstat(fd)
    try:
        current = os.stat(path, follow_symlinks=False)
    except OSError as e:
        raise BackupError(f"{what}: cannot inspect {path} before publishing it: {e}") from e
    if (current.st_dev, current.st_ino) != (st.st_dev, st.st_ino):
        raise BackupError(
            f"{what}: {path} was replaced while it was being written; refusing to "
            f"publish it. Nothing was installed."
        )


def _verify_still_the_created_file(fd: int, path: Path, what: str) -> None:
    """:func:`_verify_same_file`, plus a check that the file is still owner-only.

    The mode is read through the descriptor. Use this for credentials; use
    :func:`_verify_same_file` alone where a wide mode was already accepted (a
    ``--no-secrets`` bundle on a filesystem without modes).
    """
    st = os.fstat(fd)
    if not _mode_is_private(st.st_mode):
        raise BackupError(
            f"{what}: {path} is no longer owner-only "
            f"({stat.S_IMODE(st.st_mode):04o}); nothing is published."
        )
    _verify_same_file(fd, path, what)


def _secure_install_file(
    src: Path,
    dst: Path,
    *,
    what: str,
    last_resort: Callable[[Path], None] | None = None,
    exposed_detail: str = "",
) -> None:
    """Install a secret-bearing file at ``dst`` so it is never readable by others.

    1. Create the temporary owner-only and verify it (:func:`_secure_create`).
    2. Copy the bytes through that descriptor, never through the path.
    3. Verify the path is still that file and still private, then rename it
       over ``dst``.
    4. Check ``dst`` again. If it is wide or its mode cannot be read, run
       ``last_resort`` on it and fail anyway.

    A partial temporary is always removed. Failures raise :class:`BackupError`.
    """
    tmp = dst.with_name(dst.name + ".restore-tmp")
    fd = _secure_create(tmp, what)
    try:
        with os.fdopen(fd, "wb", closefd=False) as out, open(src, "rb") as inp:
            shutil.copyfileobj(inp, out)
            out.flush()
            os.fsync(out.fileno())
        _verify_still_the_created_file(fd, tmp, what)
        os.replace(tmp, dst)
    except BaseException:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    finally:
        os.close(fd)
    try:
        final: int | None = os.stat(dst).st_mode
    except OSError as e:
        # A mode that cannot be read counts as exposed.
        logger.warning("%s: cannot inspect the mode of %s: %s", what, dst, e)
        final = None
    if final is None or not _mode_is_private(final):
        if last_resort is not None:
            last_resort(dst)
        found = f"is {stat.S_IMODE(final):04o}" if final is not None else "has an unreadable mode"
        raise BackupError(
            f"{what}: {dst} {found} despite a verified {SECRET_FILE_MODE:04o} "
            f"temporary, so the filesystem is not keeping it private.{exposed_detail} "
            f"The restore is not complete: fix the filesystem or choose another "
            f"directory, then restore again."
        )


def _secure_install_db(src: Path, dst: Path) -> None:
    """Install the restored ``nerve.db`` through :func:`_secure_install_file`.

    If the installed file reads back wide, :func:`_scrub_db_credentials` removes
    the signing secret and the password hashes from it, and the restore still
    fails.
    """
    _secure_install_file(
        src, dst, what="Restore", last_resort=_scrub_db_credentials,
        exposed_detail=(
            " The stored JWT signing secret and the account password hashes were "
            "scrubbed from it so no usable credential is exposed, but the "
            "accounts and history remain readable."
        ),
    )


def _db_schema_version(db_path: Path) -> int:
    """Read the persisted schema version from a nerve.db (0 if absent)."""
    try:
        conn = _connect(db_path)
        try:
            row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
            return int(row[0]) if row and row[0] is not None else 0
        finally:
            conn.close()
    except Exception:
        return 0


def _count(db_path: Path, table: str) -> int | None:
    """COUNT(*) of a table, or None when the DB/table is unavailable."""
    try:
        conn = _connect(db_path)
        try:
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            return int(row[0]) if row else None
        finally:
            conn.close()
    except Exception:
        return None


def _parity_counts(nerve_db: Path, memu_db: Path) -> dict:
    """Row counts that codify the migration parity-diff UX."""
    return {
        "sessions": _count(nerve_db, "sessions"),
        "messages": _count(nerve_db, "messages"),
        "tasks": _count(nerve_db, "tasks"),
        "memu_items": _count(memu_db, "memu_memory_items"),
    }


# --- tar (de)compression context managers ---------------------------------- #


@contextmanager
def _tar_writer(
    path: Path, compression: str, fileobj: "IO[bytes] | None" = None,
) -> Iterator[tarfile.TarFile]:
    """Open a streaming tar for writing, zstd or gzip.

    If ``fileobj`` is given, write to it and do not open ``path``. The
    bundle is written this way so the path is not opened again after it was
    checked, when another user could point it at their own file. The stream is
    closed here; the caller keeps the descriptor under it (``closefd=False``)
    to verify the file before publishing it.
    """
    fh = fileobj if fileobj is not None else open(path, "wb")
    try:
        if compression == "zstd":
            import zstandard

            cctx = zstandard.ZstdCompressor(level=10, threads=-1)
            comp = cctx.stream_writer(fh)
            tar = tarfile.open(mode="w|", fileobj=comp)
            try:
                yield tar
            finally:
                tar.close()
                comp.close()  # flush the zstd frame
        else:
            tar = tarfile.open(mode="w:gz", fileobj=fh)
            try:
                yield tar
            finally:
                tar.close()
    finally:
        fh.close()


@contextmanager
def _tar_reader(path: Path, compression: str) -> Iterator[tarfile.TarFile]:
    """Open a streaming tar for reading, zstd or gzip."""
    if compression == "zstd":
        import zstandard

        dctx = zstandard.ZstdDecompressor()
        fh = open(path, "rb")
        reader = dctx.stream_reader(fh)
        tar = tarfile.open(mode="r|", fileobj=reader)
        try:
            yield tar
        finally:
            tar.close()
            reader.close()
            fh.close()
    else:
        tar = tarfile.open(path, mode="r:gz")
        try:
            yield tar
        finally:
            tar.close()


def _compression_for(path: Path) -> str:
    """Infer the compression of an existing bundle from its name."""
    name = path.name
    if name.endswith(".tar.zst"):
        return "zstd"
    if name.endswith(".tar.gz"):
        return "gzip"
    raise BackupError(
        f"unrecognized bundle extension: {path.name} "
        "(expected .tar.zst or .tar.gz)"
    )


# --- workspace walk -------------------------------------------------------- #


def _norm_excludes(extra: list[str] | None) -> list[str]:
    return [e for e in (extra or []) if e]


def _excluded_by_user(rel_posix: str, name: str, globs: list[str]) -> bool:
    import fnmatch

    for pat in globs:
        if fnmatch.fnmatch(rel_posix, pat) or fnmatch.fnmatch(name, pat):
            return True
    return False


def _collect_workspace_files(
    workspace: Path, extra_excludes: list[str] | None,
) -> list[tuple[Path, str]]:
    """Return ``(abs_path, arcname)`` pairs for the workspace BRAIN.

    ``arcname`` is relative to the bundle's ``workspace/`` root. Root-level
    files matching :data:`WORKSPACE_INCLUDE_FILE_GLOBS` plus the contents of
    :data:`WORKSPACE_INCLUDE_DIRS` are collected; junk dirs and any
    ``extra_excludes`` globs are pruned.
    """
    import fnmatch

    globs = _norm_excludes(extra_excludes)
    out: list[tuple[Path, str]] = []
    if not workspace.exists():
        return out

    # Root-level markdown (identity + work notes).
    for entry in sorted(workspace.iterdir()):
        if not entry.is_file():
            continue
        if any(fnmatch.fnmatch(entry.name, g) for g in WORKSPACE_INCLUDE_FILE_GLOBS):
            if _excluded_by_user(entry.name, entry.name, globs):
                continue
            out.append((entry, entry.name))

    # Known directories, walked with junk pruning.
    for dname in WORKSPACE_INCLUDE_DIRS:
        droot = workspace / dname
        if not droot.is_dir():
            continue
        for root, dirs, files in os.walk(droot):
            root_path = Path(root)
            rel_root = root_path.relative_to(workspace)
            # Prune junk + user-excluded dirs in place.
            kept: list[str] = []
            for d in sorted(dirs):
                if d in _PRUNE_DIR_NAMES:
                    continue
                rel = (rel_root / d).as_posix()
                if _excluded_by_user(rel, d, globs):
                    continue
                kept.append(d)
            dirs[:] = kept
            for f in sorted(files):
                fp = root_path / f
                if fp.is_symlink() or not fp.is_file():
                    continue
                rel = (rel_root / f).as_posix()
                if _excluded_by_user(rel, f, globs):
                    continue
                out.append((fp, rel))
    return out


# --------------------------------------------------------------------------- #
#  Create                                                                      #
# --------------------------------------------------------------------------- #


def create_backup(
    nerve_dir: Path,
    workspace: Path,
    output_dir: Path,
    *,
    config_dir: Path | None = None,
    include_workspace: bool = True,
    include_secrets: bool = True,
    state_only: bool = False,
    workspace_excludes: list[str] | None = None,
    compression: str | None = None,
) -> BackupResult:
    """Create a backup bundle and return a :class:`BackupResult`.

    Synchronous (the scheduled task wraps this in ``asyncio.to_thread``).
    Staging uses a directory only this user can write to (see
    :func:`_stage_parent`), because the staged snapshot holds the accounts and
    the signing secret. Only the finished bundle is written to the output
    directory. It is created owner-only and renamed into place atomically.
    """
    nerve_dir = Path(nerve_dir).expanduser()
    workspace = Path(workspace).expanduser()
    output_dir = Path(output_dir).expanduser()
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
    if not output_dir.is_dir():
        raise BackupError(f"output dir is not a directory: {output_dir}")

    if state_only:
        include_workspace = False

    if compression is None:
        compression = "zstd" if _zstd_available() else "gzip"
    if compression == "zstd" and not _zstd_available():
        logger.warning("zstandard unavailable — falling back to gzip")
        compression = "gzip"

    ext = "tar.zst" if compression == "zstd" else "tar.gz"
    host = _hostname()
    stamp = _now_stamp()
    final_path = _unique_bundle_path(output_dir, host, stamp, ext)
    final_name = final_path.name

    # The staging directory is held open so its identity can be checked again
    # before secrets are written into it and before they are archived.
    stage = Path(tempfile.mkdtemp(
        prefix=".nerve-backup-stage-", dir=_stage_parent(nerve_dir),
    ))
    # O_NOFOLLOW: a symlink at this name is not the directory mkdtemp made.
    stage_fd = os.open(stage, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
    if not _mode_is_private(os.fstat(stage_fd).st_mode):  # mkdtemp promises 0700
        os.close(stage_fd)
        shutil.rmtree(stage, ignore_errors=True)
        raise BackupError(
            f"Backup: the staging directory {stage} is not owner-only; refusing to "
            f"copy the database into it."
        )
    if _is_group_world_writable(output_dir):
        logger.warning(
            "Backup: %s is writable by other users. The bundle itself is created "
            "owner-only and published through a verified descriptor, but a "
            "directory anyone can write to is a poor home for backups.", output_dir,
        )
    workspace_bytes = 0
    # Open until the archive has been read; the finally below closes them.
    snapshot_fds: list[tuple[int, Path]] = []
    try:
        # Created relative to the descriptor, so it lands in the verified
        # directory. The check after it detects a swapped name.
        os.mkdir("state", mode=_STATE_DIR_MODE, dir_fd=stage_fd)
        state_dir = stage / "state"
        _verify_still_the_created_file(stage_fd, stage, "Backup")

        # 1. Consistent DB snapshots. The online-backup API opens the
        # destination by name and would create it at the umask mode. So the
        # file is created 0600 first, and its descriptor stays open through
        # the copy, the scrub and the archive read, to detect a swap of the
        # file at that name. The archived member is 0600 as a result.
        for db_name in STATE_DB_FILES:
            src = nerve_dir / db_name
            if src.exists():
                snapshot = state_dir / db_name
                snapshot_fd = _secure_create(snapshot, "Backup")
                snapshot_fds.append((snapshot_fd, snapshot))
                _snapshot_db(src, snapshot)
                # SQLite opened the destination by name: check that the
                # directory and the file are still ours and still private.
                _verify_still_the_created_file(stage_fd, stage, "Backup")
                _verify_still_the_created_file(snapshot_fd, snapshot, "Backup")
                if db_name == "nerve.db" and not include_secrets:
                    _scrub_snapshot_secrets(snapshot)
                    _verify_still_the_created_file(snapshot_fd, snapshot, "Backup")
            else:
                logger.warning("state DB missing, skipping: %s", src)

        # 2. State directories (memU sidecars, cron, certs).
        for dname in STATE_DIRS:
            src = nerve_dir / dname
            if not src.is_dir():
                continue
            if not include_secrets and f"state/{dname}" in SECRET_MEMBERS:
                continue
            shutil.copytree(src, state_dir / dname, symlinks=True)

        # 3. State files.
        for fname in STATE_FILES:
            src = nerve_dir / fname
            if not src.is_file():
                continue
            if not include_secrets and f"state/{fname}" in SECRET_MEMBERS:
                continue
            shutil.copy2(src, state_dir / fname)

        # 4. config.local.yaml (secret) → config/.
        if include_secrets and config_dir is not None:
            local_cfg = Path(config_dir).expanduser() / "config.local.yaml"
            if local_cfg.is_file():
                os.mkdir("config", mode=_STATE_DIR_MODE, dir_fd=stage_fd)
                cfg_dir = stage / "config"
                _verify_still_the_created_file(stage_fd, stage, "Backup")
                shutil.copy2(local_cfg, cfg_dir / "config.local.yaml")

        # 5. Workspace BRAIN.
        if include_workspace:
            ws_files = _collect_workspace_files(workspace, workspace_excludes)
            ws_root = stage / "workspace"
            for src, arc in ws_files:
                dst = ws_root / arc
                dst.parent.mkdir(parents=True, exist_ok=True)
                if _is_sanitisable_config(arc):
                    _stage_config_file(src, dst, include_secrets=include_secrets)
                else:
                    shutil.copy2(src, dst)
                try:
                    workspace_bytes += src.stat().st_size
                except OSError:
                    pass
            if workspace_bytes > WORKSPACE_WARN_BYTES:
                logger.warning(
                    "workspace payload is %.1f GB — junk exclusion may have "
                    "missed something (check workspace_excludes)",
                    workspace_bytes / (1024 ** 3),
                )

        # 6. Checksums for every staged file (manifest written last). The
        # steps above used names, so first confirm the staging directory and
        # each snapshot are still the ones that were created.
        _verify_still_the_created_file(stage_fd, stage, "Backup")
        for fd, snapshot in snapshot_fds:
            _verify_still_the_created_file(fd, snapshot, "Backup")
        files_meta: dict[str, dict] = {}
        for p in sorted(stage.rglob("*")):
            if not p.is_file():
                continue
            arc = p.relative_to(stage).as_posix()
            files_meta[arc] = {"sha256": _sha256(p), "size": p.stat().st_size}

        # 7. Parity counts from the *snapshot* (not the live DB).
        counts = _parity_counts(
            state_dir / "nerve.db", state_dir / "memu.sqlite",
        )

        manifest = {
            "format_version": FORMAT_VERSION,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "host": host,
            "nerve_version": _nerve_version(),
            "git_sha": _git_sha(),
            "schema_version": _db_schema_version(state_dir / "nerve.db"),
            "code_schema_version": _code_schema_version(),
            "compression": compression,
            "flags": {
                "include_secrets": include_secrets,
                "include_workspace": include_workspace,
                "state_only": state_only,
            },
            "nerve_dir": str(nerve_dir),
            "workspace": str(workspace),
            "config_dir": str(config_dir) if config_dir else "",
            "counts": counts,
            "files": files_meta,
        }
        (stage / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8",
        )

        # 8. Build the tar (manifest first for cheap inspection).
        #
        # The bundle can hold the signing secret and config.local.yaml. It is
        # created exclusively and owner-only, written through its descriptor,
        # and checked by inode before the rename, so another user with write
        # access to the output directory cannot swap in a symlink.
        # ``os.replace`` keeps the mode.
        tmp_bundle = output_dir / (final_name + ".tmp")
        # A mode the filesystem ignores stops a backup with secrets while the
        # file is still empty. A --no-secrets bundle holds no credential, so it
        # continues with a warning.
        fd, private = _exclusive_create(tmp_bundle, "Backup")
        if not private:
            if include_secrets:
                os.close(fd)
                tmp_bundle.unlink(missing_ok=True)
                raise BackupError(
                    f"Backup: could not create {tmp_bundle} owner-only (the "
                    f"filesystem ignored mode {SECRET_FILE_MODE:04o}); nothing was "
                    f"written. Fix the filesystem or choose a directory that "
                    f"supports Unix modes."
                )
            logger.warning(
                "Backup: %s could not be created owner-only; the bundle carries no "
                "secrets (--no-secrets) but is readable by other users.", tmp_bundle,
            )
        try:
            with _tar_writer(
                tmp_bundle, compression, fileobj=os.fdopen(fd, "wb", closefd=False),
            ) as tar:
                tar.add(stage / "manifest.json", arcname="manifest.json")
                for sub in ("config", "state", "workspace"):
                    p = stage / sub
                    if p.exists():
                        tar.add(p, arcname=sub)
            # Always check identity. Check the mode too unless a wide mode was
            # accepted above.
            if private:
                _verify_still_the_created_file(fd, tmp_bundle, "Backup")
            else:
                _verify_same_file(fd, tmp_bundle, "Backup")
        except BaseException:
            tmp_bundle.unlink(missing_ok=True)
            raise
        finally:
            os.close(fd)
        os.replace(tmp_bundle, final_path)

        size = final_path.stat().st_size
        logger.info(
            "Backup created: %s (%.1f MB, %d files, %s)",
            final_path, size / (1024 ** 2), len(files_meta), compression,
        )
        return BackupResult(
            path=final_path,
            size=size,
            compression=compression,
            counts=counts,
            file_count=len(files_meta),
            include_secrets=include_secrets,
            include_workspace=include_workspace,
            workspace_bytes=workspace_bytes,
        )
    finally:
        for fd, _snapshot in snapshot_fds:
            try:
                os.close(fd)
            except OSError:
                pass
        # Remove the staging directory only if the name still points to it. If
        # it was swapped, the name is another user's directory; ours stays 0700
        # wherever it was moved, and the log names it.
        try:
            _verify_same_file(stage_fd, stage, "Backup")
        except BackupError as e:
            logger.error(
                "Backup: leaving %s alone — it is no longer the staging directory "
                "that was created (%s).", stage, e,
            )
        else:
            shutil.rmtree(stage, ignore_errors=True)
        os.close(stage_fd)


def _code_schema_version() -> int:
    """Current migration-head schema version of this codebase."""
    try:
        from nerve.db import SCHEMA_VERSION

        return int(SCHEMA_VERSION)
    except Exception:
        return 0


# --------------------------------------------------------------------------- #
#  Verify                                                                      #
# --------------------------------------------------------------------------- #


def _extract_bundle(path: Path, dest: Path) -> dict:
    """Extract a bundle to ``dest`` and return its manifest dict."""
    compression = _compression_for(path)
    with _tar_reader(path, compression) as tar:
        # ``filter='data'`` (py3.12+) blocks path traversal / absolute paths.
        try:
            tar.extractall(dest, filter="data")
        except TypeError:  # pragma: no cover - older Python without filter
            tar.extractall(dest)
    manifest_path = dest / "manifest.json"
    if not manifest_path.is_file():
        raise BackupError("bundle is missing manifest.json")
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise BackupError(f"corrupt manifest.json: {e}") from e


def verify_bundle(path: Path, extract_to: Path | None = None) -> VerifyReport:
    """Verify a bundle's integrity without installing it.

    Extracts (to ``extract_to`` or a temp dir), checks every file's sha256
    against the manifest, runs ``PRAGMA integrity_check`` on both DBs, and
    guards the schema version. Returns a :class:`VerifyReport`; never raises
    for *verification* failures (only for an unreadable/corrupt bundle).
    """
    path = Path(path).expanduser()
    if not path.is_file():
        raise BackupError(f"bundle not found: {path}")

    own_tmp = extract_to is None
    # Extraction writes nerve.db (and possibly the signing secret) to disk, so
    # the temp parent gets the same check as staging (see _vetted_temp_dir).
    work = Path(extract_to) if extract_to else Path(
        tempfile.mkdtemp(prefix=".nerve-verify-", dir=_vetted_temp_dir())
    )
    errors: list[str] = []
    warnings: list[str] = []
    manifest: dict = {}
    counts: dict = {}
    try:
        manifest = _extract_bundle(path, work)
        counts = manifest.get("counts", {}) or {}

        # Checksums.
        files_meta = manifest.get("files", {}) or {}
        for arc, meta in files_meta.items():
            fp = work / arc
            if not fp.is_file():
                errors.append(f"missing file in bundle: {arc}")
                continue
            actual = _sha256(fp)
            if actual != meta.get("sha256"):
                errors.append(f"checksum mismatch: {arc}")

        # DB integrity.
        for db_name in STATE_DB_FILES:
            fp = work / "state" / db_name
            if not fp.is_file():
                # memu.sqlite may legitimately be absent only on a broken box;
                # treat a missing nerve.db as an error, memu as a warning.
                (errors if db_name == "nerve.db" else warnings).append(
                    f"{db_name} not present in bundle"
                )
                continue
            try:
                conn = _connect(fp)
                try:
                    row = conn.execute("PRAGMA integrity_check").fetchone()
                finally:
                    conn.close()
                if not row or row[0] != "ok":
                    errors.append(f"integrity_check failed: {db_name} ({row!r})")
            except Exception as e:
                errors.append(f"cannot open {db_name}: {e}")

        # Schema guard — restoring a newer-schema bundle onto older code
        # would break at startup; flag it here.
        bundle_schema = int(manifest.get("schema_version", 0) or 0)
        code_schema = _code_schema_version()
        if bundle_schema > code_schema:
            errors.append(
                f"bundle schema v{bundle_schema} is newer than this code "
                f"(v{code_schema}) — upgrade Nerve before restoring"
            )

        ok = not errors
        return VerifyReport(
            ok=ok, manifest=manifest, counts=counts,
            errors=errors, warnings=warnings,
        )
    finally:
        if own_tmp:
            shutil.rmtree(work, ignore_errors=True)


# --------------------------------------------------------------------------- #
#  Restore                                                                     #
# --------------------------------------------------------------------------- #


def _pid_is_alive(pid_file: Path) -> int | None:
    """Return the live PID if the daemon is running, else None."""
    try:
        pid = int(pid_file.read_text().strip())
    except (FileNotFoundError, ValueError, OSError):
        return None
    try:
        os.kill(pid, 0)
        return pid
    except ProcessLookupError:
        return None
    except PermissionError:
        return pid  # exists but not ours to signal


def _dir_nonempty(d: Path) -> bool:
    try:
        return d.is_dir() and any(d.iterdir())
    except OSError:
        return False


def restore_bundle(
    path: Path,
    nerve_dir: Path,
    workspace: Path,
    *,
    config_dir: Path | None = None,
    force: bool = False,
) -> VerifyReport:
    """Restore a bundle into ``nerve_dir`` + ``workspace``.

    Safety rails:

    - **Refuses while the daemon is alive** (no override — stop it first).
      Restoring under a running process would corrupt the live WAL DBs.
    - **Refuses to overwrite a non-empty** ``nerve_dir`` without ``force``.
      With ``force`` the existing dir is *relocated* to
      ``<nerve_dir>.pre-restore-<ts>`` (trash over rm), never deleted.
    - **Verifies before installing** — a bundle that fails verification is
      never unpacked into place.

    Returns the :class:`VerifyReport` produced during the install.
    """
    path = Path(path).expanduser()
    nerve_dir = Path(nerve_dir).expanduser()
    workspace = Path(workspace).expanduser()

    pid_file = nerve_dir / "nerve.pid"
    live = _pid_is_alive(pid_file)
    if live is not None:
        raise BackupError(
            f"Nerve daemon appears to be running (PID {live}). "
            "Stop it first ('nerve stop') — restore will not run against a "
            "live process."
        )

    if _dir_nonempty(nerve_dir) and not force:
        raise BackupError(
            f"{nerve_dir} is not empty. Re-run with --force to relocate the "
            f"existing state to {nerve_dir}.pre-restore-<ts> and restore."
        )

    # Verify into a staging dir we then install from (extract once). It holds
    # nerve.db, so its parent gets the staging check (see _vetted_temp_dir).
    staging = Path(tempfile.mkdtemp(prefix=".nerve-restore-", dir=_vetted_temp_dir()))
    try:
        report = verify_bundle(path, extract_to=staging)
        if not report.ok:
            raise BackupError(
                "bundle failed verification; refusing to restore:\n  - "
                + "\n  - ".join(report.errors)
            )

        # Relocate the old state dir if forcing over a non-empty target.
        if _dir_nonempty(nerve_dir):
            relocated = nerve_dir.with_name(
                nerve_dir.name + f".pre-restore-{_now_stamp()}"
            )
            os.replace(nerve_dir, relocated)
            logger.info("Relocated existing state dir to %s", relocated)
        nerve_dir.mkdir(mode=_STATE_DIR_MODE, parents=True, exist_ok=True)
        # Make the directory owner-only before any secret goes into it; fatal
        # if that fails.
        _secure_directory(nerve_dir)

        # Install state/. nerve.db goes first, through a verified 0600
        # temporary and an atomic rename, so a failure stops the restore before
        # anything else is written.
        staged_state = staging / "state"
        if staged_state.is_dir():
            entries = sorted(staged_state.iterdir(), key=lambda p: (p.name != "nerve.db", p.name))
            for entry in entries:
                dst = nerve_dir / entry.name
                if entry.is_dir():
                    shutil.copytree(entry, dst, symlinks=True, dirs_exist_ok=True)
                elif entry.name == "nerve.db":
                    _secure_install_db(entry, dst)
                else:
                    shutil.copy2(entry, dst)

        # Re-tighten the remaining secret files (nerve.db is already done).
        # Only warn: the daemon enforces the modes on its next start.
        for secret in _SECRET_RESTORE_PATHS:
            if secret == "nerve.db":
                continue
            sp = nerve_dir / secret
            if sp.is_file():
                try:
                    os.chmod(sp, SECRET_FILE_MODE)
                except OSError as e:
                    logger.warning(
                        "Restore: could not set %s to %04o: %s. It may hold a "
                        "credential; tighten it before starting the daemon.",
                        sp, SECRET_FILE_MODE, e,
                    )
        certs_dir = nerve_dir / "certs"
        if certs_dir.is_dir():
            for f in certs_dir.rglob("*"):
                if f.is_file():
                    try:
                        os.chmod(f, SECRET_FILE_MODE)
                    except OSError:
                        pass

        # Install config.local.yaml next to where the pointer says config lives.
        # It holds the password hash and machine-local secrets, so it goes
        # through the same verified 0600 temporary as nerve.db. Failure is
        # fatal: a restore without it leaves the instance with no password.
        staged_cfg = staging / "config" / "config.local.yaml"
        if staged_cfg.is_file():
            dest_cfg_dir = _resolve_restore_config_dir(
                config_dir, nerve_dir, staging,
            )
            try:
                dest_cfg_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                raise BackupError(
                    f"Restore: could not create the config directory {dest_cfg_dir} "
                    f"for config.local.yaml (it holds the password hash and the "
                    f"machine-local secrets): {e}"
                ) from e
            dest = dest_cfg_dir / "config.local.yaml"
            try:
                _secure_install_file(
                    staged_cfg, dest, what="Restore",
                    exposed_detail=(
                        " It holds the password hash and the machine-local secrets, "
                        "which are now readable by other users."
                    ),
                )
            except OSError as e:
                raise BackupError(
                    f"Restore: could not install config.local.yaml to {dest} (it holds "
                    f"the password hash and the machine-local secrets): {e}"
                ) from e
            logger.info("Restored config.local.yaml to %s", dest)

        # Install workspace BRAIN (overlay; never relocates the workspace).
        staged_ws = staging / "workspace"
        if staged_ws.is_dir():
            workspace.mkdir(parents=True, exist_ok=True)
            refused: list[str] = []
            for src in sorted(staged_ws.rglob("*")):
                if not src.is_file():
                    continue
                rel = src.relative_to(staged_ws)
                if not _restorable_workspace_path(rel):
                    refused.append(str(rel))
                    continue
                dst = workspace / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)
            if refused:
                logger.warning(
                    "Restore: skipped %d workspace file(s) not restorable (%s): "
                    "%s. The config subtree is intentionally among these — it "
                    "travels in the bundle but is never written back, because "
                    "tracked config comes from the git remote. Read it out of "
                    "the bundle and apply it through review if you need it.",
                    len(refused), ", ".join(WORKSPACE_RESTORE_DIRS),
                    ", ".join(refused[:10]),
                )
                report.warnings.append(
                    f"skipped {len(refused)} workspace file(s) this bundle "
                    f"carries that restore does not write back (the config "
                    f"subtree is deliberately one of them) — including "
                    f"{refused[0]!r}"
                )

        logger.info("Restore complete: %s → %s", path.name, nerve_dir)
        return report
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _resolve_restore_config_dir(
    config_dir: Path | None, nerve_dir: Path, staging: Path,
) -> Path:
    """Decide where config.local.yaml should land on restore.

    Priority: an explicit ``config_dir`` arg, then the restored
    ``state/config_dir`` pointer (the original install location), then
    ``nerve_dir`` itself as a safe fallback.
    """
    if config_dir is not None:
        return Path(config_dir).expanduser()
    pointer = staging / "state" / "config_dir"
    if pointer.is_file():
        try:
            raw = pointer.read_text(encoding="utf-8").strip()
            if raw:
                return Path(raw).expanduser()
        except OSError:
            pass
    return nerve_dir


# --------------------------------------------------------------------------- #
#  Retention                                                                   #
# --------------------------------------------------------------------------- #


def list_bundles(target_dir: Path) -> list[Path]:
    """Return existing bundles in ``target_dir``, newest first."""
    target_dir = Path(target_dir).expanduser()
    if not target_dir.is_dir():
        return []
    bundles = [
        p for p in target_dir.iterdir()
        if p.is_file() and BUNDLE_RE.match(p.name)
    ]
    bundles.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return bundles


def prune(target_dir: Path, keep_n: int) -> list[Path]:
    """Delete all but the newest ``keep_n`` bundles. Returns deleted paths.

    Only ever touches files whose name matches :data:`BUNDLE_RE`, so an
    unrelated file sharing the directory is never at risk.
    """
    if keep_n < 0:
        return []
    bundles = list_bundles(target_dir)
    to_delete = bundles[keep_n:]
    deleted: list[Path] = []
    for p in to_delete:
        try:
            p.unlink()
            deleted.append(p)
        except OSError as e:
            logger.warning("Could not prune %s: %s", p, e)
    if deleted:
        logger.info("Pruned %d old backup(s) in %s", len(deleted), target_dir)
    return deleted


def latest_bundle_age_seconds(target_dir: Path) -> float | None:
    """Age (seconds) of the newest bundle, or None if there are none."""
    bundles = list_bundles(target_dir)
    if not bundles:
        return None
    import time

    return time.time() - bundles[0].stat().st_mtime
