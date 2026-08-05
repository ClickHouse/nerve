"""Leaving the terminal status: the inverse of ``task_done``.

``task_done`` is a one-way door — it writes the markdown into ``done/`` and
unlinks the source. Nothing put it back, which was fine only while nothing
could take a task *out* of ``done``. The board can, so the file has to
travel with the status.

The failure this guards against is quiet rather than loud. A row left
pointing into ``done/`` while claiming an active status is exactly what
``TaskManager.reindex()`` calls an orphan (manager.py: "a file under done/
is terminal by definition, so the directory wins"), and it force-resets the
status back to ``done``. So a half-done reopen doesn't error — it just
silently undoes itself the next time anything reindexes.
"""

from __future__ import annotations

import pytest

from nerve.agent.tools.handlers.tasks import (
    task_create_handler,
    task_done_handler,
    task_reopen_handler,
    task_update_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.db import Database
from nerve.tasks.manager import TaskManager


@pytest.fixture
def workspace(tmp_path):
    (tmp_path / "memory" / "tasks" / "active").mkdir(parents=True)
    (tmp_path / "memory" / "tasks" / "done").mkdir(parents=True)
    return tmp_path


@pytest.fixture
def ctx(workspace, db: Database) -> ToolContext:
    return ToolContext(session_id="test", workspace=workspace, db=db)


async def _completed_task(ctx: ToolContext, title: str = "Ship the thing") -> str:
    """Create a task, finish it, and return its id."""
    result = await task_create_handler(
        ctx, {"title": title, "content": "body", "confirm_duplicate": True},
    )
    task_id = result.structured["task_id"]
    await task_done_handler(ctx, {"task_id": task_id, "note": ""})
    return task_id


def _active(workspace) -> list[str]:
    return sorted(p.name for p in (workspace / "memory" / "tasks" / "active").glob("*.md"))


def _done(workspace) -> list[str]:
    return sorted(p.name for p in (workspace / "memory" / "tasks" / "done").glob("*.md"))


@pytest.mark.asyncio
class TestReopen:
    async def test_reopen_moves_the_file_back_to_active(self, ctx, workspace, db):
        task_id = await _completed_task(ctx)
        assert _done(workspace) == [f"{task_id}.md"]

        result = await task_reopen_handler(
            ctx, {"task_id": task_id, "status": "in_progress"},
        )

        assert not result.is_error
        assert _active(workspace) == [f"{task_id}.md"]
        assert _done(workspace) == []

    async def test_reopen_updates_the_row_to_match(self, ctx, db):
        task_id = await _completed_task(ctx)

        await task_reopen_handler(ctx, {"task_id": task_id, "status": "in_progress"})

        row = await db.get_task(task_id)
        assert row["status"] == "in_progress"
        # The stored path has to follow the file, or the next read 404s and
        # the next reindex calls the row an orphan.
        assert row["file_path"] == f"memory/tasks/active/{task_id}.md"

    async def test_reindex_leaves_a_reopened_task_alone(self, ctx, workspace, db):
        """The actual regression: reopen must survive a reindex."""
        task_id = await _completed_task(ctx)
        await task_reopen_handler(ctx, {"task_id": task_id, "status": "in_progress"})

        await TaskManager(workspace, db).reindex()

        row = await db.get_task(task_id)
        assert row["status"] == "in_progress", (
            "reindex reset a reopened task to done — its file is still in done/"
        )

    async def test_reopen_appends_an_audit_line(self, ctx, workspace):
        task_id = await _completed_task(ctx)

        await task_reopen_handler(
            ctx,
            {"task_id": task_id, "status": "pending", "note": "shipped too early"},
        )

        content = (workspace / "memory" / "tasks" / "active" / f"{task_id}.md").read_text()
        assert "REOPENED (pending)" in content
        assert "shipped too early" in content
        # The DONE line stays: the history is the point of the file.
        assert "DONE" in content

    async def test_reopen_defaults_to_the_default_status(self, ctx, db):
        task_id = await _completed_task(ctx)

        await task_reopen_handler(ctx, {"task_id": task_id})

        assert (await db.get_task(task_id))["status"] == "pending"

    async def test_reopen_rejects_the_terminal_status(self, ctx, db):
        task_id = await _completed_task(ctx)

        result = await task_reopen_handler(ctx, {"task_id": task_id, "status": "done"})

        assert result.is_error
        assert (await db.get_task(task_id))["status"] == "done"

    async def test_reopen_rejects_an_unknown_status(self, ctx, db):
        task_id = await _completed_task(ctx)

        result = await task_reopen_handler(
            ctx, {"task_id": task_id, "status": "not_a_status"},
        )

        assert result.is_error
        assert (await db.get_task(task_id))["status"] == "done"

    async def test_reopen_rejects_a_task_that_is_not_done(self, ctx, db):
        result = await task_create_handler(
            ctx, {"title": "Still going", "content": "b", "confirm_duplicate": True},
        )
        task_id = result.structured["task_id"]

        outcome = await task_reopen_handler(
            ctx, {"task_id": task_id, "status": "in_progress"},
        )

        assert outcome.is_error
        assert "not done" in outcome.text_content

    async def test_reopen_rejects_a_missing_task(self, ctx):
        result = await task_reopen_handler(ctx, {"task_id": "nope"})
        assert result.is_error


@pytest.mark.asyncio
class TestReopenGuardOrdering:
    async def test_tracked_config_guard_fires_before_the_status_flip(
        self, ctx, db, monkeypatch,
    ):
        """A refused move must not leave a task claiming to be active.

        Same ordering rationale as ``task_done``: check first, flip second.
        Reversed, a rejected reopen leaves the row active while its file
        never left ``done/`` — the orphan state this module exists to
        prevent, arrived at from the other direction.
        """
        import nerve.agent.tools.handlers.tasks as handlers

        task_id = await _completed_task(ctx)

        def _refuse(path, operation):
            raise PermissionError(f"Cannot {operation} tracked config: {path}")

        monkeypatch.setattr(handlers, "ensure_path_not_tracked_config", _refuse)

        with pytest.raises(PermissionError):
            await task_reopen_handler(
                ctx, {"task_id": task_id, "status": "in_progress"},
            )

        assert (await db.get_task(task_id))["status"] == "done"

    async def test_a_missing_file_is_refused_before_the_status_flip(
        self, ctx, workspace, db,
    ):
        """No file to move means there is no move to make.

        Flipping anyway strands the row in exactly the shape this module
        exists to prevent — an active status whose ``file_path`` still
        points into ``done/`` — and this is the one way of reaching it that
        ``reindex()`` cannot repair, because it only walks files that exist.
        """
        task_id = await _completed_task(ctx)
        (workspace / "memory" / "tasks" / "done" / f"{task_id}.md").unlink()

        result = await task_reopen_handler(
            ctx, {"task_id": task_id, "status": "in_progress"},
        )

        assert result.is_error
        row = await db.get_task(task_id)
        assert row["status"] == "done"
        assert row["file_path"] == f"memory/tasks/done/{task_id}.md"


@pytest.mark.asyncio
class TestTaskUpdateRoutesToReopen:
    """``task_update`` is the single entry point; it dispatches both ways."""

    async def test_status_change_out_of_done_reopens(self, ctx, workspace, db):
        task_id = await _completed_task(ctx)

        await task_update_handler(
            ctx, {"task_id": task_id, "status": "in_progress"},
        )

        assert _active(workspace) == [f"{task_id}.md"]
        assert (await db.get_task(task_id))["status"] == "in_progress"

    async def test_reopen_applies_other_edits_in_the_same_call(
        self, ctx, workspace, db,
    ):
        # The reopen moves the file first; the remaining edits then have to
        # land on the new path, not the stale done/ one.
        task_id = await _completed_task(ctx)

        await task_update_handler(
            ctx,
            {"task_id": task_id, "status": "pending", "tags": "urgent,backend"},
        )

        row = await db.get_task(task_id)
        assert row["status"] == "pending"
        assert row["tags"] == "backend,urgent"
        content = (workspace / "memory" / "tasks" / "active" / f"{task_id}.md").read_text()
        assert "**Tags:** backend, urgent" in content

    async def test_a_normal_status_change_does_not_touch_files(
        self, ctx, workspace, db,
    ):
        result = await task_create_handler(
            ctx, {"title": "Ordinary", "content": "b", "confirm_duplicate": True},
        )
        task_id = result.structured["task_id"]

        await task_update_handler(ctx, {"task_id": task_id, "status": "in_progress"})

        assert _active(workspace) == [f"{task_id}.md"]
        assert _done(workspace) == []
