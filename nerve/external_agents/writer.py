"""Atomic file writer for external-agent config and memory files.

Why a dedicated writer:

- **Allowlist enforcement.** Only paths under ``~/.codex``, ``~/.claude``,
  or ``~/.cursor`` are writable. The wizard and the sync cron run with
  full user privileges; a bug that pointed at ``/etc/passwd`` should
  fail loudly, not destroy the host.
- **Conflict policy.** ``backup`` saves a timestamped copy of the
  existing file before overwriting; ``skip`` leaves the original in
  place and returns ``None``; ``merge`` deep-merges JSON dicts (Claude
  Code's ``settings.json`` is the canonical use case — we want to add
  an ``mcpServers`` block without nuking the user's model/plugin
  preferences).
- **Atomic write.** Writes go via temp file + ``rename`` so a SIGTERM
  mid-write doesn't leave half-baked config on disk.
- **Idempotency.** Hash sidecar (``<output>.nerve-hash``) records the
  hash we wrote so the sync cron can skip no-op writes.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class SecurityError(Exception):
    """Raised when a write would touch a path outside the allowlist.

    Distinct from ``PermissionError`` because the OS may well permit
    the write — this is a Nerve-side policy refusal.
    """


def _default_allowlist() -> list[Path]:
    """Return the resolved set of directories the writer may touch.

    Centralised so tests can monkeypatch and future agents (Cursor,
    Continue, ...) can be added without scattering the policy.
    """
    return [
        Path("~/.codex").expanduser().resolve(),
        Path("~/.claude").expanduser().resolve(),
        Path("~/.cursor").expanduser().resolve(),
    ]


def _hash_text(text: str) -> str:
    """Stable hash for sidecar comparison — sha256 hex, short prefix."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursive dict merge — overlay wins on conflicts.

    Only dict values are merged. Lists, primitives, ``None`` get
    replaced wholesale. Mirrors the merge semantics of
    ``deep_merge`` in ``nerve.config`` so behaviour is predictable for
    users who already know how config.local.yaml overlays config.yaml.
    """
    result = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


class ConfigWriter:
    """Atomic, allowlist-bound writer for external-agent config files.

    Used by both the bootstrap wizard's apply step and the periodic
    sync service — sharing one code path means the conflict policy is
    enforced uniformly and there's only one place to audit.
    """

    def __init__(
        self,
        *,
        conflict_policy: str = "backup",
        allowlist: list[Path] | None = None,
    ) -> None:
        if conflict_policy not in {"backup", "skip", "merge"}:
            raise ValueError(
                f"Invalid conflict_policy: {conflict_policy!r}. "
                "Expected one of: backup, skip, merge."
            )
        self.policy = conflict_policy
        self._allowlist = allowlist if allowlist is not None else _default_allowlist()

    # ---- Public API -------------------------------------------------

    def write(self, path: Path, content: str) -> Path | None:
        """Write ``content`` to ``path`` atomically.

        Returns the backup path created (if any), or ``None`` if the
        write was skipped or no backup was needed.

        Raises :class:`SecurityError` if ``path`` is outside the
        allowlist.
        """
        self._assert_in_allowlist(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        backup_path: Path | None = None
        if path.exists():
            if self.policy == "skip":
                logger.info("Skipping existing file %s (policy=skip)", path)
                return None
            if self.policy == "backup":
                backup_path = self._make_backup(path)

        self._atomic_write_text(path, content)
        self._write_sidecar_hash(path, content)
        return backup_path

    def merge_json(self, path: Path, partial: dict[str, Any]) -> Path | None:
        """Deep-merge ``partial`` into the JSON at ``path``.

        Creates the file with just ``partial`` if it doesn't exist.
        For ``policy=skip`` the existing file is left alone and
        ``None`` is returned; for ``policy=backup`` a backup is made
        before the merged write; for ``policy=merge`` the existing
        keys win where they conflict — i.e. user customisations are
        preserved and Nerve only ever *adds* keys.

        Returns the backup path (if any), or ``None``.
        """
        self._assert_in_allowlist(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        existing: dict[str, Any] = {}
        backup_path: Path | None = None
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8") or "{}")
                if not isinstance(existing, dict):
                    logger.warning(
                        "Expected JSON object at %s, got %s — overwriting.",
                        path,
                        type(existing).__name__,
                    )
                    existing = {}
            except json.JSONDecodeError as e:
                logger.warning("Invalid JSON at %s (%s) — overwriting.", path, e)
                existing = {}

            if self.policy == "skip":
                logger.info("Skipping existing JSON file %s (policy=skip)", path)
                return None
            if self.policy == "backup":
                backup_path = self._make_backup(path)
                merged = _deep_merge(existing, partial)
            elif self.policy == "merge":
                # User keys win — flip the merge direction.
                merged = _deep_merge(partial, existing)
            else:  # pragma: no cover - validated in __init__
                merged = _deep_merge(existing, partial)
        else:
            merged = dict(partial)

        content = json.dumps(merged, indent=2, sort_keys=False) + "\n"
        self._atomic_write_text(path, content)
        self._write_sidecar_hash(path, content)
        return backup_path

    def is_up_to_date(self, path: Path, content: str) -> bool:
        """Return True if the hash sidecar matches ``content``.

        Lets the sync service skip writes when nothing changed,
        avoiding noisy logs and pointless ``mtime`` bumps that would
        confuse downstream filesystem watchers.
        """
        sidecar = self._sidecar_path(path)
        if not sidecar.exists() or not path.exists():
            return False
        try:
            stored = sidecar.read_text(encoding="utf-8").strip()
        except OSError:
            return False
        return stored == _hash_text(content)

    # ---- Internals --------------------------------------------------

    def _assert_in_allowlist(self, path: Path) -> None:
        resolved = path.expanduser().resolve()
        for allowed in self._allowlist:
            try:
                resolved.relative_to(allowed)
                return
            except ValueError:
                continue
        raise SecurityError(
            f"Refusing to write outside agent config allowlist: {resolved}. "
            f"Allowed roots: {', '.join(str(p) for p in self._allowlist)}"
        )

    def _make_backup(self, path: Path) -> Path:
        ts = int(time.time())
        backup_path = path.with_suffix(path.suffix + f".nerve-backup-{ts}")
        # If we managed to collide (sub-second double-write), append a
        # short random suffix rather than overwriting an earlier backup.
        if backup_path.exists():
            backup_path = path.with_suffix(
                path.suffix + f".nerve-backup-{ts}-{os.urandom(2).hex()}"
            )
        shutil.copy2(path, backup_path)
        logger.info("Backed up %s -> %s", path, backup_path)
        return backup_path

    def _atomic_write_text(self, path: Path, content: str) -> None:
        tmp = path.with_suffix(path.suffix + ".nerve-tmp")
        tmp.write_text(content, encoding="utf-8")
        # Preserve mode if the destination existed previously. Honors
        # any chmod the user applied to ``~/.codex/config.toml`` etc.
        if path.exists():
            try:
                mode = path.stat().st_mode
                os.chmod(tmp, mode)
            except OSError as e:
                logger.debug("chmod on %s failed: %s", tmp, e)
        os.replace(tmp, path)

    def _sidecar_path(self, path: Path) -> Path:
        return path.with_suffix(path.suffix + ".nerve-hash")

    def _write_sidecar_hash(self, path: Path, content: str) -> None:
        sidecar = self._sidecar_path(path)
        try:
            sidecar.write_text(_hash_text(content) + "\n", encoding="utf-8")
        except OSError as e:
            logger.debug("Could not write hash sidecar at %s: %s", sidecar, e)
