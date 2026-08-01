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
