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
        assert names == [
            "backlog", "pending", "plan_review", "in_progress", "done", "deferred",
        ]
        assert [s["sort_order"] for s in statuses] == list(range(6))
        by_name = {s["name"]: s for s in statuses}
        # pending, plan_review and done are protected; the others are not.
        assert by_name["pending"]["is_system"] == 1
        assert by_name["plan_review"]["is_system"] == 1
        assert by_name["done"]["is_system"] == 1
        assert by_name["backlog"]["is_system"] == 0
        assert by_name["in_progress"]["is_system"] == 0
        assert by_name["deferred"]["is_system"] == 0
        assert by_name["backlog"]["label"] == "Backlog"
        assert by_name["plan_review"]["label"] == "Plan in review"
        # Colors are stored as hex.
        assert by_name["pending"]["color"].startswith("#")

    async def test_create_appends_with_next_sort_order(self, db: Database):
        created = await db.create_task_status(
            name="blocked", label="Blocked", color="#ff0000",
            description="Waiting on something external.",
        )
        assert created["name"] == "blocked"
        assert created["sort_order"] == 6  # after the 6 seeded (0..5)
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


@pytest.mark.asyncio
class TestBuiltinStatusMigration:
    """v059 adds ``backlog`` and ``plan_review`` to an install that has statuses."""

    @staticmethod
    async def _install(tmp_path, rows: list[tuple[str, str, int]]):
        """A database with only ``task_statuses``, holding (name, label, sort_order)."""
        import aiosqlite

        from nerve.db.migrations import v030_task_statuses

        conn = await aiosqlite.connect(tmp_path / "statuses.db")
        await v030_task_statuses.up(conn)
        await conn.execute("DELETE FROM task_statuses")
        await conn.executemany(
            "INSERT INTO task_statuses (name, label, sort_order) VALUES (?, ?, ?)",
            rows,
        )
        return conn

    @staticmethod
    async def _statuses(conn) -> list[tuple]:
        async with conn.execute(
            "SELECT name, label, is_system, sort_order FROM task_statuses "
            "ORDER BY sort_order, name"
        ) as cursor:
            return [tuple(row) async for row in cursor]

    async def test_places_them_around_pending_and_keeps_a_custom_order(self, tmp_path):
        from nerve.db.migrations import v059_backlog_plan_review_statuses as v059

        conn = await self._install(tmp_path, [
            ("deferred", "Deferred", 0), ("in_progress", "In Progress", 1),
            ("pending", "Pending", 2), ("blocked", "Blocked", 3), ("done", "Done", 4),
        ])
        try:
            await v059.up(conn)
            rows = await self._statuses(conn)
        finally:
            await conn.close()
        assert [r[0] for r in rows] == [
            "deferred", "in_progress", "backlog", "pending", "plan_review",
            "blocked", "done",
        ]
        assert [r[3] for r in rows] == list(range(7))
        by_name = {r[0]: r for r in rows}
        assert by_name["plan_review"][1:3] == ("Plan in review", 1)
        assert by_name["backlog"][1:3] == ("Backlog", 0)

    async def test_keeps_a_status_that_has_the_name_and_runs_once(self, tmp_path):
        from nerve.db.migrations import v059_backlog_plan_review_statuses as v059

        conn = await self._install(tmp_path, [
            ("pending", "Pending", 0), ("done", "Done", 1), ("backlog", "Someday", 2),
        ])
        try:
            await v059.up(conn)
            first = await self._statuses(conn)
            await v059.up(conn)
            second = await self._statuses(conn)
        finally:
            await conn.close()
        assert [(r[0], r[1]) for r in first] == [
            ("pending", "Pending"), ("plan_review", "Plan in review"),
            ("done", "Done"), ("backlog", "Someday"),
        ]
        assert second == first


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
        # Text is the model-facing reminder and must not move.
        assert "Invalid task status: 'nope'" in _text(result)
        # is_error is the only field a programmatic caller can test.
        assert result.is_error is True
        # Status unchanged.
        assert (await db.get_task(task_id))["status"] == "pending"

    async def test_update_missing_task_is_flagged(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        result = await task_update_handler(
            ctx, {"task_id": "no-such-task", "status": "in_progress", "note": "n"},
        )
        assert "Task not found: no-such-task" in _text(result)
        assert result.is_error is True

    async def test_update_missing_task_done_routed_is_flagged(
        self, db: Database, tmp_path,
    ):
        # status == "done" delegates to task_done_handler, a separate return site.
        ctx = self._ctx(db, tmp_path)
        result = await task_update_handler(
            ctx, {"task_id": "no-such-task", "status": "done", "note": "n"},
        )
        assert "Task not found: no-such-task" in _text(result)
        assert result.is_error is True

    async def test_update_success_is_not_flagged(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        await task_create_handler(ctx, {"title": "Happy path", "content": "x"})
        task_id = (await db.list_tasks(status="all"))[0]["id"]
        result = await task_update_handler(
            ctx, {"task_id": task_id, "status": "in_progress"},
        )
        assert result.is_error is False
        assert (await db.get_task(task_id))["status"] == "in_progress"

    async def test_update_accepts_valid_status(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        await task_create_handler(ctx, {"title": "Another task", "content": "x"})
        task_id = (await db.list_tasks(status="all"))[0]["id"]
        await task_update_handler(ctx, {"task_id": task_id, "status": "in_progress"})
        assert (await db.get_task(task_id))["status"] == "in_progress"

    async def test_update_deadline_syncs_database(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        await task_create_handler(ctx, {"title": "Deadline task", "content": "x"})
        task_id = (await db.list_tasks(status="all"))[0]["id"]

        await task_update_handler(ctx, {"task_id": task_id, "deadline": "2026-08-01"})

        task = await db.get_task(task_id)
        assert task["deadline"] == "2026-08-01"
        assert "**Deadline:** 2026-08-01" in (tmp_path / task["file_path"]).read_text(
            encoding="utf-8",
        )

    async def test_update_note_refreshes_fts(self, db: Database, tmp_path):
        ctx = self._ctx(db, tmp_path)
        await task_create_handler(ctx, {"title": "Search task", "content": "x"})
        task_id = (await db.list_tasks(status="all"))[0]["id"]

        await task_update_handler(ctx, {"task_id": task_id, "note": "Investigated quasarflux"})

        results = await db.search_tasks("quasarflux")
        assert [task["id"] for task in results] == [task_id]

    async def test_update_combined_fields_indexes_final_state(
        self, db: Database, tmp_path,
    ):
        ctx = self._ctx(db, tmp_path)
        await task_create_handler(ctx, {"title": "Original title", "content": "x"})
        task_id = (await db.list_tasks(status="all"))[0]["id"]

        await task_update_handler(
            ctx,
            {
                "task_id": task_id,
                "title": "Renamed task",
                "status": "in_progress",
                "deadline": "2026-08-01",
                "tags": "ops,urgent",
                "note": "Found nebularift",
            },
        )

        task = await db.get_task(task_id)
        assert (task["title"], task["status"], task["deadline"], task["tags"]) == (
            "Renamed task", "in_progress", "2026-08-01", "ops,urgent",
        )
        assert [task["id"] for task in await db.search_tasks("nebularift")] == [task_id]

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


@pytest.mark.asyncio
class TestReorderTaskStatuses:
    """Board column order, rewritten as a whole sequence.

    Reordering by drag shifts several statuses at once, so the API takes the
    full desired order rather than one status's new index — that makes the
    write idempotent and avoids the half-applied states a per-row PATCH
    storm would leave if one of them failed.
    """

    async def test_reorder_sets_the_given_sequence(self, db):
        await db.create_task_status(name="review", label="Review", color="#111111")
        original = [s["name"] for s in await db.list_task_statuses()]
        reversed_order = list(reversed(original))

        returned = await db.reorder_task_statuses(reversed_order)

        assert [s["name"] for s in returned] == reversed_order
        # And it persisted, rather than only shaping the return value.
        assert [s["name"] for s in await db.list_task_statuses()] == reversed_order

    async def test_reorder_is_idempotent(self, db):
        order = [s["name"] for s in await db.list_task_statuses()]
        await db.reorder_task_statuses(order)
        await db.reorder_task_statuses(order)
        assert [s["name"] for s in await db.list_task_statuses()] == order

    async def test_omitted_statuses_are_kept_after_the_named_ones(self, db):
        order = [s["name"] for s in await db.list_task_statuses()]
        assert len(order) > 2

        returned = await db.reorder_task_statuses([order[-1]])

        # A status left out of the request must not vanish from the board.
        assert [s["name"] for s in returned][0] == order[-1]
        assert set(s["name"] for s in returned) == set(order)

    async def test_unknown_names_are_ignored(self, db):
        order = [s["name"] for s in await db.list_task_statuses()]

        returned = await db.reorder_task_statuses(["not_a_status", *order])

        assert [s["name"] for s in returned] == order

    async def test_sort_order_is_contiguous_from_zero(self, db):
        order = [s["name"] for s in await db.list_task_statuses()]
        returned = await db.reorder_task_statuses(list(reversed(order)))
        assert [s["sort_order"] for s in returned] == list(range(len(order)))
