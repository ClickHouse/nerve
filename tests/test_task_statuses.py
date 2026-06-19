"""Tests for configurable task statuses: the TaskStatusStore and the
status validation / management tool handlers."""

from __future__ import annotations

import pytest

from nerve.agent.tools.registry import ToolContext
from nerve.agent.tools.handlers.tasks import (
    task_create_handler,
    task_status_create_handler,
    task_status_list_handler,
    task_update_handler,
)
from nerve.db import Database
from nerve.db.task_statuses import normalize_color


def _text(result) -> str:
    return result.content[0]["text"]


@pytest.mark.asyncio
class TestTaskStatusStore:
    async def test_seed_defaults(self, db: Database):
        statuses = await db.list_task_statuses()
        names = [s["name"] for s in statuses]
        assert names == ["pending", "in_progress", "done", "deferred"]
        by_name = {s["name"]: s for s in statuses}
        # pending + done are protected; the others are not.
        assert by_name["pending"]["is_system"] == 1
        assert by_name["done"]["is_system"] == 1
        assert by_name["in_progress"]["is_system"] == 0
        assert by_name["deferred"]["is_system"] == 0
        # Colors are stored as hex.
        assert by_name["pending"]["color"].startswith("#")

    async def test_create_appends_with_next_sort_order(self, db: Database):
        created = await db.create_task_status(
            name="blocked", label="Blocked", color="#ff0000",
            description="Waiting on something external.",
        )
        assert created["name"] == "blocked"
        assert created["sort_order"] == 4  # after the 4 seeded (0..3)
        assert "blocked" in await db.task_status_names()

    async def test_create_normalizes_color(self, db: Database):
        created = await db.create_task_status(name="x", label="X", color="abc")
        assert created["color"] == "#aabbcc"  # shorthand expanded + #-prefixed

    async def test_update_def(self, db: Database):
        await db.update_task_status_def(
            "in_progress", label="Doing", color="#123456", description="new desc",
        )
        s = await db.get_task_status_def("in_progress")
        assert s["label"] == "Doing"
        assert s["color"] == "#123456"
        assert s["description"] == "new desc"

    async def test_count_and_delete(self, db: Database):
        await db.create_task_status(name="review", label="Review")
        await db.upsert_task(
            task_id="t1", file_path="t1.md", title="T1", status="review",
        )
        assert await db.count_tasks_with_status("review") == 1
        await db.delete_task_status_def("review")
        assert "review" not in await db.task_status_names()


class TestColorNormalization:
    def test_shorthand_and_prefix(self):
        assert normalize_color("#abc") == "#aabbcc"
        assert normalize_color("abcdef") == "#abcdef"
        assert normalize_color("#ABCDEF") == "#abcdef"

    def test_invalid_falls_back_to_palette(self):
        c = normalize_color("not-a-color")
        assert c.startswith("#") and len(c) == 7

    def test_empty_returns_random(self):
        c = normalize_color("")
        assert c.startswith("#") and len(c) == 7


@pytest.mark.asyncio
class TestStatusToolHandlers:
    def _ctx(self, db, tmp_path) -> ToolContext:
        return ToolContext(session_id="test", db=db, workspace=tmp_path)

    async def test_create_rejects_invalid_status(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        result = await task_create_handler(
            ctx, {"title": "Demo", "content": "x", "status": "bogus"},
        )
        text = _text(result)
        assert "Invalid task status: 'bogus'" in text
        # Reminder lists valid statuses with descriptions.
        assert "pending:" in text
        assert "in_progress:" in text
        # No task should have been created.
        assert await db.list_tasks(status="all") == []

    async def test_create_defaults_to_pending(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        result = await task_create_handler(ctx, {"title": "Demo task", "content": "x"})
        assert "status: pending" in _text(result)
        tasks = await db.list_tasks(status="all")
        assert len(tasks) == 1 and tasks[0]["status"] == "pending"

    async def test_create_accepts_valid_custom_status(self, db: Database, tmp_path):
        await db.create_task_status(name="blocked", label="Blocked")
        ctx = self._ctx(db, tmp_path)
        result = await task_create_handler(
            ctx, {"title": "Blocked task", "content": "x", "status": "blocked"},
        )
        assert "status: blocked" in _text(result)
        tasks = await db.list_tasks(status="blocked")
        assert len(tasks) == 1

    async def test_create_terminal_status_moves_to_done(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        await task_create_handler(
            ctx, {"title": "Already done", "content": "x", "status": "done"},
        )
        tasks = await db.list_tasks(status="done")
        assert len(tasks) == 1
        # Routed through task_done → file lives under done/.
        assert "/done/" in tasks[0]["file_path"] or tasks[0]["file_path"].startswith("done/") \
            or "done" in tasks[0]["file_path"]

    async def test_update_rejects_invalid_status(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        await task_create_handler(ctx, {"title": "Some task", "content": "x"})
        task_id = (await db.list_tasks(status="all"))[0]["id"]
        result = await task_update_handler(ctx, {"task_id": task_id, "status": "nope"})
        assert "Invalid task status: 'nope'" in _text(result)
        # Status unchanged.
        assert (await db.get_task(task_id))["status"] == "pending"

    async def test_update_accepts_valid_status(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        await task_create_handler(ctx, {"title": "Another task", "content": "x"})
        task_id = (await db.list_tasks(status="all"))[0]["id"]
        await task_update_handler(ctx, {"task_id": task_id, "status": "in_progress"})
        assert (await db.get_task(task_id))["status"] == "in_progress"

    async def test_status_create_handler(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        result = await task_status_create_handler(
            ctx, {"name": "in_review", "description": "Awaiting review"},
        )
        assert "Created task status 'in_review'" in _text(result)
        assert "in_review" in await db.task_status_names()

    async def test_status_create_rejects_duplicate(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        result = await task_status_create_handler(ctx, {"name": "pending"})
        assert "already exists" in _text(result)

    async def test_status_create_rejects_invalid_name(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        result = await task_status_create_handler(ctx, {"name": "In Review!"})
        assert "Invalid status name" in _text(result)

    async def test_status_list_handler(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        text = _text(await task_status_list_handler(ctx, {}))
        assert "pending" in text and "[protected]" in text
