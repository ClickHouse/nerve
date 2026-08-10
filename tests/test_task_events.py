"""Status-transition history (``task_events``).

Before v044 a task's status was overwritten in place and nothing recorded
that it had changed. The markdown ``## Updates`` list only gains a line
when a note is passed or on done/reopen, so a plain status flip left no
trace at all.

Two properties matter and pull against each other: every *real*
transition must be recorded exactly once, and routine rewrites must
record nothing. The second is the fragile one — ``TaskManager.reindex()``
rewrites every row on startup and through a full-row upsert, which is one
of the paths that can change status. Nothing but the no-op check stops it
from doubling the table every time it runs.
"""

from __future__ import annotations

import pytest

from nerve.db import Database
from nerve.tasks.manager import TaskManager


async def _add(db: Database, task_id: str, status: str = "pending", **row) -> None:
    row.setdefault("title", f"Task {task_id}")
    row.setdefault("file_path", f"memory/tasks/active/{task_id}.md")
    await db.upsert_task(task_id=task_id, status=status, **row)


async def _transitions(db: Database, task_id: str) -> list[tuple]:
    events = await db.list_task_events(task_id)
    return [(e["from_status"], e["to_status"]) for e in events]


@pytest.mark.asyncio
class TestRecording:
    async def test_creation_records_an_origin_event(self, db: Database):
        await _add(db, "t1")
        # from_status NULL distinguishes "created here" from "moved here",
        # which is what lets an aging calculation tell the two apart.
        assert await _transitions(db, "t1") == [(None, "pending")]

    async def test_status_change_is_recorded(self, db: Database):
        await _add(db, "t1")
        await db.update_task_status("t1", "in_progress")

        assert await _transitions(db, "t1") == [
            (None, "pending"), ("pending", "in_progress"),
        ]

    async def test_move_across_lanes_is_recorded(self, db: Database):
        await _add(db, "t1")
        await db.move_task("t1", status="done")

        assert await _transitions(db, "t1") == [
            (None, "pending"), ("pending", "done"),
        ]

    async def test_upsert_that_changes_status_is_recorded(self, db: Database):
        """A full-row upsert is a status-change path too.

        ``task_update`` with a note and the PATCH route saving content both
        flip status this way, never touching ``update_task_status``.
        """
        await _add(db, "t1")
        await _add(db, "t1", status="deferred")

        assert await _transitions(db, "t1") == [
            (None, "pending"), ("pending", "deferred"),
        ]

    async def test_actor_is_recorded(self, db: Database):
        await _add(db, "t1")
        await db.update_task_status("t1", "in_progress", actor="sess-abc")

        events = await db.list_task_events("t1")
        assert events[-1]["actor"] == "sess-abc"

    async def test_actor_defaults_rather_than_nulls(self, db: Database):
        await _add(db, "t1")
        assert (await db.list_task_events("t1"))[0]["actor"] == "system"

    async def test_a_real_creation_is_told_apart_by_actor_not_from_status(
        self, db: Database,
    ):
        """The one thing separating a real origin from a seeded one.

        v044 backfills an origin row per pre-existing task with
        ``actor='backfill'``, and both that row and a genuine creation carry
        a NULL ``from_status`` — so anything reading NULL as "synthesized"
        is wrong. docs/tasks.md documents ``backfill`` as the marker; this
        pins the half of that contract the code owns.
        """
        await _add(db, "t1")

        origin = (await db.list_task_events("t1"))[0]
        assert origin["from_status"] is None
        assert origin["actor"] == "system"

    async def test_history_is_ordered_oldest_first(self, db: Database):
        await _add(db, "t1")
        for status in ("in_progress", "deferred", "in_progress", "done"):
            await db.update_task_status("t1", status)

        assert await _transitions(db, "t1") == [
            (None, "pending"),
            ("pending", "in_progress"),
            ("in_progress", "deferred"),
            ("deferred", "in_progress"),
            ("in_progress", "done"),
        ]


@pytest.mark.asyncio
class TestNoOpsAreNotRecorded:
    """The half that keeps the table meaningful rather than merely large."""

    async def test_same_status_update_records_nothing(self, db: Database):
        await _add(db, "t1")
        await db.update_task_status("t1", "pending")

        assert len(await db.list_task_events("t1")) == 1

    async def test_reorder_within_a_lane_records_nothing(self, db: Database):
        await _add(db, "a")
        await _add(db, "b")
        await db.move_task("b", before_id="a")

        # Dragging within a lane is the most frequent board interaction;
        # recording it would bury the actual transitions.
        assert len(await db.list_task_events("b")) == 1

    async def test_upsert_with_unchanged_status_records_nothing(self, db: Database):
        await _add(db, "t1")
        await _add(db, "t1", title="edited")

        assert len(await db.list_task_events("t1")) == 1

    async def test_reindex_does_not_duplicate_history(self, db: Database, tmp_path):
        """The flooding case: reindex rewrites every row, repeatedly."""
        directory = tmp_path / "memory" / "tasks" / "active"
        directory.mkdir(parents=True)
        (tmp_path / "memory" / "tasks" / "done").mkdir(parents=True)
        (directory / "t1.md").write_text("# t1\n", encoding="utf-8")
        await _add(db, "t1", status="in_progress")

        before = len(await db.list_task_events("t1"))
        manager = TaskManager(tmp_path, db)
        await manager.reindex()
        await manager.reindex()

        assert len(await db.list_task_events("t1")) == before

    async def test_reindex_records_an_orphan_correction(self, db: Database, tmp_path):
        """...but a reindex that *does* change status has changed something."""
        active = tmp_path / "memory" / "tasks" / "active"
        done = tmp_path / "memory" / "tasks" / "done"
        active.mkdir(parents=True)
        done.mkdir(parents=True)
        # File under done/ but the row claims otherwise — the orphan state
        # reindex exists to correct.
        (done / "t1.md").write_text("# t1\n", encoding="utf-8")
        await _add(
            db, "t1", status="in_progress",
            file_path="memory/tasks/done/t1.md",
        )

        await TaskManager(tmp_path, db).reindex()

        assert await _transitions(db, "t1") == [
            (None, "in_progress"), ("in_progress", "done"),
        ]


@pytest.mark.asyncio
class TestStatusEntryTimes:
    async def test_reports_when_the_current_status_was_entered(self, db: Database):
        await _add(db, "t1")
        await db.update_task_status("t1", "in_progress")

        entered = await db.get_status_entry_times(["t1"])
        events = await db.list_task_events("t1")
        assert entered["t1"] == events[-1]["created_at"]

    async def test_omits_tasks_with_no_history(self, db: Database):
        # A row whose status last changed by some path predating this table
        # has no truthful entry time. Silence beats a fabricated age.
        await _add(db, "t1")
        await db._write("DELETE FROM task_events WHERE task_id = 't1'")

        assert await db.get_status_entry_times(["t1"]) == {}

    async def test_omits_tasks_whose_history_disagrees_with_the_row(
        self, db: Database,
    ):
        await _add(db, "t1")
        # Status changed behind the table's back (direct DB edit).
        await db._write("UPDATE tasks SET status = 'deferred' WHERE id = 't1'")

        assert await db.get_status_entry_times(["t1"]) == {}

    async def test_handles_an_empty_request(self, db: Database):
        assert await db.get_status_entry_times([]) == {}

    async def test_covers_many_tasks_in_one_call(self, db: Database):
        for tid in ("a", "b", "c"):
            await _add(db, tid)
        await db.update_task_status("b", "done")

        entered = await db.get_status_entry_times(["a", "b", "c"])
        assert set(entered) == {"a", "b", "c"}
