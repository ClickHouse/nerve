"""Backup and restore for Nerve state.

Produces a single portable bundle — ``nerve-backup-<host>-<ts>.tar.zst``
(or ``.tar.gz`` when zstandard is unavailable) — containing a consistent
snapshot of everything that makes a Nerve instance *this* instance:

- ``nerve.db``   — sessions, messages, tasks index, notifications, plans, usage
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
import subprocess
import tarfile
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

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
# and re-chmod'd to 0600 on restore.
SECRET_MEMBERS: frozenset[str] = frozenset({
    "state/certs",
    "state/mcp-token",
    "state/telegram_sync.session",
    "config/config.local.yaml",
})
# Files within the bundle whose mode must be 0600 after restore.
SECRET_FILE_MODE = 0o600
_SECRET_RESTORE_PATHS: tuple[str, ...] = (
    "mcp-token",
    "telegram_sync.session",
)

# --- Workspace BRAIN allowlist --------------------------------------------- #
# The workspace is typically buried in tens of GB of repo/build junk, so we
# take an *allowlist* of the small, irreplaceable "brain": identity markdown
# at the root plus a few known directories. Extra excludes from config are
# applied *within* these.
WORKSPACE_INCLUDE_DIRS: tuple[str, ...] = ("memory", "scripts", "skills")
WORKSPACE_INCLUDE_FILE_GLOBS: tuple[str, ...] = ("*.md",)

# Directory names always pruned while walking the included workspace dirs.
_PRUNE_DIR_NAMES: frozenset[str] = frozenset({
    ".git", "node_modules", ".venv", "venv", "__pycache__",
    ".mypy_cache", ".pytest_cache", ".ruff_cache",
})

# Warn if the workspace payload exceeds this — a sign the junk exclusion
# missed something (e.g. a build artifact landed under memory/).
WORKSPACE_WARN_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB

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
def _tar_writer(path: Path, compression: str) -> Iterator[tarfile.TarFile]:
    """Open a streaming tar for writing, zstd or gzip."""
    if compression == "zstd":
        import zstandard

        cctx = zstandard.ZstdCompressor(level=10, threads=-1)
        fh = open(path, "wb")
        comp = cctx.stream_writer(fh)
        tar = tarfile.open(mode="w|", fileobj=comp)
        try:
            yield tar
        finally:
            tar.close()
            comp.close()  # flush the zstd frame
            fh.close()
    else:
        tar = tarfile.open(path, mode="w:gz")
        try:
            yield tar
        finally:
            tar.close()


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
    Staging happens on the *output* filesystem so disk-space failures surface
    on the target, and the final bundle is renamed into place atomically.
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

    stage = Path(tempfile.mkdtemp(prefix=".nerve-backup-stage-", dir=output_dir))
    workspace_bytes = 0
    try:
        state_dir = stage / "state"
        state_dir.mkdir(parents=True, exist_ok=True)

        # 1. Consistent DB snapshots.
        for db_name in STATE_DB_FILES:
            src = nerve_dir / db_name
            if src.exists():
                _snapshot_db(src, state_dir / db_name)
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
                cfg_dir = stage / "config"
                cfg_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(local_cfg, cfg_dir / "config.local.yaml")

        # 5. Workspace BRAIN.
        if include_workspace:
            ws_files = _collect_workspace_files(workspace, workspace_excludes)
            ws_root = stage / "workspace"
            for src, arc in ws_files:
                dst = ws_root / arc
                dst.parent.mkdir(parents=True, exist_ok=True)
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

        # 6. Checksums for every staged file (manifest written last).
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
        tmp_bundle = output_dir / (final_name + ".tmp")
        if tmp_bundle.exists():
            tmp_bundle.unlink()
        with _tar_writer(tmp_bundle, compression) as tar:
            tar.add(stage / "manifest.json", arcname="manifest.json")
            for sub in ("config", "state", "workspace"):
                p = stage / sub
                if p.exists():
                    tar.add(p, arcname=sub)
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
        shutil.rmtree(stage, ignore_errors=True)


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
    work = Path(extract_to) if extract_to else Path(
        tempfile.mkdtemp(prefix=".nerve-verify-")
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

    # Verify into a staging dir we then install from (extract once).
    staging = Path(tempfile.mkdtemp(prefix=".nerve-restore-"))
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
        nerve_dir.mkdir(parents=True, exist_ok=True)

        # Install state/.
        staged_state = staging / "state"
        if staged_state.is_dir():
            for entry in sorted(staged_state.iterdir()):
                dst = nerve_dir / entry.name
                if entry.is_dir():
                    shutil.copytree(entry, dst, symlinks=True, dirs_exist_ok=True)
                else:
                    shutil.copy2(entry, dst)

        # Re-tighten secret file modes.
        for secret in _SECRET_RESTORE_PATHS:
            sp = nerve_dir / secret
            if sp.is_file():
                try:
                    os.chmod(sp, SECRET_FILE_MODE)
                except OSError:
                    pass
        certs_dir = nerve_dir / "certs"
        if certs_dir.is_dir():
            for f in certs_dir.rglob("*"):
                if f.is_file():
                    try:
                        os.chmod(f, SECRET_FILE_MODE)
                    except OSError:
                        pass

        # Install config.local.yaml next to where the pointer says config lives.
        staged_cfg = staging / "config" / "config.local.yaml"
        if staged_cfg.is_file():
            dest_cfg_dir = _resolve_restore_config_dir(
                config_dir, nerve_dir, staging,
            )
            try:
                dest_cfg_dir.mkdir(parents=True, exist_ok=True)
                dest = dest_cfg_dir / "config.local.yaml"
                shutil.copy2(staged_cfg, dest)
                os.chmod(dest, SECRET_FILE_MODE)
                logger.info("Restored config.local.yaml to %s", dest)
            except OSError as e:
                logger.warning("Could not restore config.local.yaml: %s", e)

        # Install workspace BRAIN (overlay; never relocates the workspace).
        staged_ws = staging / "workspace"
        if staged_ws.is_dir():
            workspace.mkdir(parents=True, exist_ok=True)
            for src in sorted(staged_ws.rglob("*")):
                if not src.is_file():
                    continue
                rel = src.relative_to(staged_ws)
                dst = workspace / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)

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
