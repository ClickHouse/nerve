"""Task manager — CRUD operations for markdown task files + SQLite index."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

from nerve.config import ensure_path_not_tracked_config
from nerve.db import Database
from nerve.tasks.models import (
    Task,
    TaskStatus,
    parse_tags_string,
    parse_task_frontmatter,
    parse_task_title,
    tags_to_string,
)

logger = logging.getLogger(__name__)


class TaskManager:
    """Manages tasks stored as markdown files with SQLite indexing."""

    def __init__(self, workspace: Path, db: Database):
        self.workspace = workspace
        self.db = db
        self.active_dir = workspace / "memory" / "tasks" / "active"
        self.done_dir = workspace / "memory" / "tasks" / "done"
        self.active_dir.mkdir(parents=True, exist_ok=True)
        self.done_dir.mkdir(parents=True, exist_ok=True)

    async def reindex(self) -> int:
        """Scan task files and rebuild the SQLite + FTS index.

        ``upsert_task`` replaces the whole row, so every column this loop does
        not supply is written back as its signature default. Values that live
        only in the DB (status) are carried from the stored row. For values the
        file may carry, the rule is per column, each matching the save path that
        already writes it: ``tags`` takes the file whenever the field is
        present, so a present-but-empty field is an explicit clear;
        ``source_url`` and ``deadline`` take any non-empty file value and
        otherwise keep the stored row.
        """
        await self.db.rebuild_fts()
        count = 0

        for directory, default_status in [
            (self.active_dir, "pending"),
            (self.done_dir, "done"),
        ]:
            md_files = await asyncio.to_thread(lambda d=directory: sorted(d.glob("*.md")))
            for md_file in md_files:
                try:
                    content = await asyncio.to_thread(md_file.read_text, encoding="utf-8")
                    title = parse_task_title(content)
                    fields = parse_task_frontmatter(content)
                    task_id = md_file.stem
                    rel_path = str(md_file.relative_to(self.workspace))
                    stored = await self.db.get_task(task_id) or {}

                    if default_status == "done":
                        # A file under done/ is terminal by definition, so the
                        # directory wins: a nonterminal row here is an orphan.
                        status = "done"
                    else:
                        # No writer emits a **Status:** line, so the stored row
                        # is the only source. A stored "done" on an active/ file
                        # is the orphan state docs/tasks.md forbids, so reset it.
                        prev = stored.get("status")
                        status = prev if prev and prev != "done" else default_status

                    await self.db.upsert_task(
                        task_id=task_id,
                        file_path=rel_path,
                        title=title,
                        status=status,
                        # **Source:** carries the URL (handlers/tasks.py writes
                        # source_url there); `source` is a DB-only vocabulary.
                        source=stored.get("source"),
                        source_url=fields.get("source") or stored.get("source_url"),
                        deadline=fields.get("deadline") or stored.get("deadline"),
                        # Presence, not truthiness: a present-but-empty field is
                        # an explicit clear, an absent one is "no information".
                        tags=(
                            tags_to_string(parse_tags_string(fields["tags"]))
                            if "tags" in fields
                            else (stored.get("tags") or "")
                        ),
                        content=content,
                    )
                    count += 1
                except Exception as e:
                    logger.warning("Failed to index task %s: %s", md_file.name, e)

        logger.info("Reindexed %d tasks", count)
        return count

    async def list_tasks(self, status: str | None = None) -> list[Task]:
        """List tasks from SQLite index."""
        rows = await self.db.list_tasks(status=status)
        return [Task.from_db_row(row) for row in rows]

    async def get_task(self, task_id: str) -> Task | None:
        """Get a task by ID with its file content."""
        row = await self.db.get_task(task_id)
        if not row:
            return None

        task = Task.from_db_row(row)
        file_path = self.workspace / row["file_path"]
        if file_path.exists():
            task.content = await asyncio.to_thread(
                file_path.read_text, encoding="utf-8",
            )
        return task

    async def mark_done(self, task_id: str) -> bool:
        """Mark a task as done and move its file.

        Raises :class:`~nerve.config.LockdownError` on a locked instance when the
        stored ``file_path`` lands inside the tracked config subtree.
        """
        row = await self.db.get_task(task_id)
        if not row:
            return False

        src = self.workspace / row["file_path"]
        # Done is a write like any other — it copies the file into done/ and
        # unlinks the source, so a stored ``file_path`` inside the tracked config
        # subtree would delete config. Refuse before any of it runs, or the
        # refusal still leaves that config file mirrored into done/.
        ensure_path_not_tracked_config(src, "move")
        if src.exists():
            dst = self.done_dir / src.name
            content = await asyncio.to_thread(src.read_text, encoding="utf-8")
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            content += f"\n- {today}: DONE"

            def _move_to_done() -> None:
                dst.write_text(content, encoding="utf-8")
                src.unlink()

            await asyncio.to_thread(_move_to_done)

            # Update DB
            rel_path = str(dst.relative_to(self.workspace))
            await self.db.upsert_task(
                task_id=task_id,
                file_path=rel_path,
                title=row["title"],
                status="done",
                source=row.get("source"),
                source_url=row.get("source_url"),
                deadline=row.get("deadline"),
                tags=row.get("tags") or "",
                content=content,
            )

        return True

    async def get_overdue_tasks(self) -> list[Task]:
        """Get tasks that are past their deadline."""
        tasks = await self.list_tasks(status="pending")
        overdue = []
        now = datetime.now(timezone.utc)
        for task in tasks:
            if task.deadline:
                try:
                    deadline = datetime.fromisoformat(task.deadline)
                    if deadline < now:
                        overdue.append(task)
                except ValueError:
                    pass
        return overdue
