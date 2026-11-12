"""File-based memory operations — read/write MEMORY.md, daily logs, etc.

The workspace memory directory is the source of truth.
memU indexes it for semantic search but doesn't replace it.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


def memory_file_path(workspace: Path, relative_path: str) -> Path | None:
    """The memory file that ``relative_path`` names, or None if it is not one.

    A memory file is a markdown file at the workspace root or under
    ``memory/``. These are the files that the memory listings show. The path
    is resolved first, so ``..`` segments, an absolute path and symlinks are
    judged by the file that they name. The file does not have to exist.

    The callers get the path from a request. The workspace also holds files
    that the daemon runs: git runs ``core.fsmonitor`` from ``.git/config``,
    and the notification dispatcher runs ``scripts/``. A memory editor must
    not write to them.
    """
    root = Path(workspace).resolve()
    try:
        candidate = (root / relative_path).resolve()
    except (OSError, RuntimeError, ValueError):
        # A name that is too long, a symlink loop, a NUL byte: not a file.
        return None
    if not candidate.is_relative_to(root) or candidate.suffix != ".md":
        return None
    parts = candidate.relative_to(root).parts
    if len(parts) == 1 or parts[0] == "memory":
        return candidate
    return None


class MemoryManager:
    """Manages file-based memory in the workspace."""

    def __init__(self, workspace: Path, timezone_name: str = "America/New_York"):
        self.workspace = workspace
        self.memory_dir = workspace / "memory"
        self.timezone_name = timezone_name
        self.memory_dir.mkdir(parents=True, exist_ok=True)

    def read_memory(self) -> str:
        """Read the main MEMORY.md file."""
        path = self.workspace / "MEMORY.md"
        if path.exists():
            return path.read_text(encoding="utf-8")
        return ""

    def write_memory(self, content: str) -> None:
        """Write the main MEMORY.md file."""
        path = self.workspace / "MEMORY.md"
        path.write_text(content, encoding="utf-8")

    def get_today_log_path(self) -> Path:
        """Get the path for today's daily log."""
        try:
            tz = ZoneInfo(self.timezone_name)
            today = datetime.now(tz).strftime("%Y-%m-%d")
        except Exception:
            today = datetime.now().strftime("%Y-%m-%d")
        return self.memory_dir / f"{today}.md"

    def read_daily_log(self, date: str | None = None) -> str:
        """Read a daily log. If date is None, reads today's log."""
        if date:
            path = self.memory_dir / f"{date}.md"
        else:
            path = self.get_today_log_path()

        if path.exists():
            return path.read_text(encoding="utf-8")
        return ""

    def append_daily_log(self, content: str, date: str | None = None) -> None:
        """Append content to a daily log."""
        if date:
            path = self.memory_dir / f"{date}.md"
        else:
            path = self.get_today_log_path()

        if not path.exists():
            path.write_text(f"# {path.stem}\n\n", encoding="utf-8")

        with open(path, "a", encoding="utf-8") as f:
            f.write(content + "\n")

    def ensure_daily_log(self) -> Path:
        """Ensure today's daily log file exists. Returns the path."""
        path = self.get_today_log_path()
        if not path.exists():
            path.write_text(f"# {path.stem}\n\n", encoding="utf-8")
        return path

    def list_memory_files(self) -> list[dict]:
        """List all memory files in the workspace."""
        files = []

        # Root-level md files
        for f in sorted(self.workspace.glob("*.md")):
            files.append({
                "path": f.name,
                "name": f.name,
                "size": f.stat().st_size,
                "type": "identity" if f.name in ("SOUL.md", "IDENTITY.md", "USER.md") else "memory",
            })

        # Memory subdirectory
        if self.memory_dir.exists():
            for f in sorted(self.memory_dir.rglob("*.md")):
                rel = f.relative_to(self.workspace)
                files.append({
                    "path": str(rel),
                    "name": f.name,
                    "size": f.stat().st_size,
                    "type": "daily" if f.stem.count("-") == 2 else "reference",
                })

        return files

    def read_file(self, relative_path: str) -> str | None:
        """Read a memory file (see :func:`memory_file_path`) by relative path."""
        path = memory_file_path(self.workspace, relative_path)
        if path is None:
            logger.warning("Refused a path that is not a memory file: %s", relative_path)
            return None

        if path.is_file():
            return path.read_text(encoding="utf-8")
        return None

    def write_file(self, relative_path: str, content: str) -> bool:
        """Write a memory file (see :func:`memory_file_path`) by relative path.

        Raises :class:`~nerve.config.LockdownError` for a path inside the tracked
        config subtree on a locked instance — the caller supplies the path, so
        whether this is a config edit is a property of the argument.
        """
        from nerve.config import ensure_path_not_tracked_config

        ensure_path_not_tracked_config(self.workspace / relative_path, "write")
        path = memory_file_path(self.workspace, relative_path)
        if path is None:
            logger.warning("Refused a path that is not a memory file: %s", relative_path)
            return False

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return True
