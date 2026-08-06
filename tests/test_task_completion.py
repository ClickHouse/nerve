"""Regression tests for completing tasks."""

from __future__ import annotations

import pytest

from nerve.agent.tools.handlers.tasks import task_done_handler
from nerve.agent.tools.registry import ToolContext
from nerve.db import Database
from nerve.tasks.manager import TaskManager


TASK_METADATA = {
    "source": "github",
    "source_url": "https://github.com/example/project/issues/123",
    "deadline": "2026-08-02",
    "tags": "bug,github,urgent",
}


async def _create_task(db: Database, tmp_path, task_id: str) -> None:
    active_dir = tmp_path / "memory" / "tasks" / "active"
    active_dir.mkdir(parents=True, exist_ok=True)
    task_file = active_dir / f"{task_id}.md"
    content = "# Preserve metadata\n\nInvestigate source synchronization.\n"
    task_file.write_text(content, encoding="utf-8")
    await db.upsert_task(
        task_id=task_id,
        file_path=str(task_file.relative_to(tmp_path)),
        title="Preserve metadata",
        status="pending",
        content=content,
        **TASK_METADATA,
    )


async def _assert_metadata_preserved(db: Database, task_id: str) -> None:
    task = await db.get_task(task_id)
    assert task is not None
    assert task["status"] == "done"
    assert {field: task[field] for field in TASK_METADATA} == TASK_METADATA
    assert task["file_path"].startswith("memory/tasks/done/")

    matches = await db.search_tasks(
        query="source synchronization", status="all", tag="urgent",
    )
    assert [match["id"] for match in matches] == [task_id]


@pytest.mark.asyncio
async def test_task_done_handler_preserves_metadata(db: Database, tmp_path):
    task_id = "2026-07-30-handler-metadata"
    await _create_task(db, tmp_path, task_id)

    ctx = ToolContext(session_id="test", db=db, workspace=tmp_path)
    await task_done_handler(ctx, {"task_id": task_id})

    await _assert_metadata_preserved(db, task_id)


@pytest.mark.asyncio
async def test_task_manager_mark_done_preserves_metadata(db: Database, tmp_path):
    task_id = "2026-07-30-manager-metadata"
    await _create_task(db, tmp_path, task_id)

    manager = TaskManager(tmp_path, db)
    assert await manager.mark_done(task_id)

    await _assert_metadata_preserved(db, task_id)


class TestCompletingATaskTwice:
    """The second completion reads the ``file_path`` the first one stored, so
    its source and its destination are one path under done/. While the move
    was a copy followed by an unlink, that sequence wrote the file and then
    deleted it: the task kept its row and lost every word of its markdown,
    including the update history the file had collected.

    Two callers reach it without anything unusual happening. An agent that
    retries ``task_done`` after a timeout sends the call twice, and the board
    can move a card out of Done and back in.
    """

    @pytest.mark.asyncio
    async def test_task_done_twice_keeps_the_file(self, db: Database, tmp_path):
        task_id = "2026-07-30-done-twice"
        await _create_task(db, tmp_path, task_id)
        ctx = ToolContext(session_id="test", db=db, workspace=tmp_path)
        done_file = tmp_path / "memory" / "tasks" / "done" / f"{task_id}.md"

        await task_done_handler(ctx, {"task_id": task_id, "note": "first"})
        await task_done_handler(ctx, {"task_id": task_id, "note": "second"})

        assert done_file.exists()
        body = done_file.read_text(encoding="utf-8")
        assert "Investigate source synchronization." in body
        # Both completions recorded, and neither cost the file its history.
        assert "DONE — first" in body
        assert "DONE — second" in body
        await _assert_metadata_preserved(db, task_id)

    @pytest.mark.asyncio
    async def test_the_row_still_points_at_the_file(self, db: Database, tmp_path):
        """A row pointing at a path that no longer exists is the quiet half of
        the bug: ``task_read`` and the detail page 404 while the task still
        lists."""
        task_id = "2026-07-30-done-twice-row"
        await _create_task(db, tmp_path, task_id)
        ctx = ToolContext(session_id="test", db=db, workspace=tmp_path)

        await task_done_handler(ctx, {"task_id": task_id})
        await task_done_handler(ctx, {"task_id": task_id})

        row = await db.get_task(task_id)
        assert (tmp_path / row["file_path"]).exists()

    @pytest.mark.asyncio
    async def test_mark_done_twice_keeps_the_file(self, db: Database, tmp_path):
        task_id = "2026-07-30-manager-done-twice"
        await _create_task(db, tmp_path, task_id)
        manager = TaskManager(tmp_path, db)

        assert await manager.mark_done(task_id)
        assert await manager.mark_done(task_id)

        done_file = tmp_path / "memory" / "tasks" / "done" / f"{task_id}.md"
        assert done_file.exists()
        assert "Investigate source synchronization." in done_file.read_text(
            encoding="utf-8",
        )
