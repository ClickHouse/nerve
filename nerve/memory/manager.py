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
        """Read any file in the workspace by relative path."""
        full_path = self.workspace / relative_path
        # Security: ensure it's within workspace
        try:
            full_path.resolve().relative_to(self.workspace.resolve())
        except ValueError:
            logger.warning("Attempted path traversal: %s", relative_path)
            return None

        if full_path.exists() and full_path.is_file():
            return full_path.read_text(encoding="utf-8")
        return None

    def write_file(self, relative_path: str, content: str) -> bool:
        """Write a file in the workspace by relative path.

        Raises :class:`~nerve.config.LockdownError` for a path inside the tracked
        config subtree on a locked instance — the caller supplies the path, so
        whether this is a config edit is a property of the argument.
        """
        from nerve.config import ensure_path_not_tracked_config

        full_path = self.workspace / relative_path
        try:
            full_path.resolve().relative_to(self.workspace.resolve())
        except ValueError:
            logger.warning("Attempted path traversal: %s", relative_path)
            return False
        ensure_path_not_tracked_config(full_path, "write")

        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(content, encoding="utf-8")
        return True
