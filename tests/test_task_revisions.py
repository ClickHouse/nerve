"""The task revision token and the writes it fences.

Every write to a ``tasks`` row advances ``revision``. A caller that passes the
value it read as ``expect_revision`` has its write refused, with nothing
written, once the row has moved on. Omitting it keeps last-write-wins.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import nerve.agent.tools.handlers.tasks as handlers
import nerve.tasks.manager as manager_module
from nerve.agent.tools.handlers.tasks import (
    task_create_handler,
    task_done_handler,
    task_read_handler,
    task_reopen_handler,
    task_update_handler,
    task_write_handler,
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


async def _create(ctx: ToolContext) -> str:
    result = await task_create_handler(
        ctx, {"title": "Fence me", "content": "body", "confirm_duplicate": True},
    )
    return result.structured["task_id"]


async def _rev(db: Database, task_id: str) -> int:
    return (await db.get_task(task_id))["revision"]


async def _stale_token(db: Database, task_id: str) -> int:
    """A token read before another writer changed the row."""
    token = await _rev(db, task_id)
    await db.update_task_tags(task_id, "moved-on")
    return token


def _external(db: Database, sql: str) -> None:
    """Commit from a second, independent connection (another process)."""
    ext = sqlite3.connect(str(db.db_path), timeout=5)
    try:
        ext.execute(sql)
        ext.commit()
    finally:
        ext.close()


def _file(workspace: Path, task_id: str, lane: str = "active") -> Path:
    return workspace / "memory" / "tasks" / lane / f"{task_id}.md"


@pytest.mark.asyncio
class TestEveryWriteAdvancesIt:
    async def test_rows_start_at_one(self, db):
        await db.upsert_task("a", "a.md", "A")
        _external(db, "INSERT INTO tasks (id, file_path, title) VALUES ('b', 'b.md', 'B')")
        assert (await _rev(db, "a"), await _rev(db, "b")) == (1, 1)

    @pytest.mark.parametrize("write", [
        pytest.param(lambda db: db.upsert_task("t", "t.md", "Renamed"), id="upsert_task"),
        pytest.param(lambda db: db.update_task_status("t", "pending"), id="status-same-lane"),
        pytest.param(lambda db: db.update_task_status("t", "in_progress"), id="status-new-lane"),
        pytest.param(lambda db: db.update_task_tags("t", "x"), id="update_task_tags"),
        pytest.param(lambda db: db.move_task("t", status="in_progress"), id="move_task"),
        pytest.param(lambda db: db.update_task_escalation("t", 1), id="update_task_escalation"),
    ])
    async def test_by_exactly_one(self, db, write):
        await db.upsert_task("t", "t.md", "T")
        await write(db)
        assert await _rev(db, "t") == 2

    async def test_a_lane_respace_advances_every_row_in_it(self, db):
        for task_id in ("a", "b", "c"):
            await db.upsert_task(task_id, f"{task_id}.md", task_id)
        async with db._atomic():
            await db._renormalize_lane("pending")
        assert [await _rev(db, t) for t in ("a", "b", "c")] == [2, 2, 2]


@pytest.mark.asyncio
class TestConditionalUpsert:
    async def test_current_token_applies(self, db):
        await db.upsert_task("t", "t.md", "T")
        assert await db.upsert_task("t", "t.md", "T2", expect_revision=1) is True
        row = await db.get_task("t")
        assert (row["title"], row["revision"]) == ("T2", 2)

    async def test_stale_token_writes_nothing(self, db):
        await db.upsert_task("t", "t.md", "T", content="original words")
        await db.update_task_status("t", "in_progress")
        row, events = await db.get_task("t"), await db.list_task_events("t")

        assert await db.upsert_task(
            "t", "moved.md", "T2", status="done", content="replacement",
            expect_revision=1,
        ) is False
        assert await db.get_task("t") == row
        assert await db.list_task_events("t") == events
        assert [r["id"] for r in await db.search_tasks("original", status="all")] == ["t"]
        assert await db.search_tasks("replacement", status="all") == []

    async def test_an_absent_row_is_not_created(self, db):
        assert await db.upsert_task("ghost", "g.md", "G", expect_revision=1) is False
        assert await db.get_task("ghost") is None

    async def test_a_row_deleted_since_the_read_stays_deleted(self, db):
        await db.upsert_task("t", "t.md", "T")
        _external(db, "DELETE FROM tasks WHERE id = 't'")
        assert await db.upsert_task("t", "t.md", "T", expect_revision=1) is False
        assert await db.get_task("t") is None


@pytest.mark.asyncio
class TestTransitionTask:
    async def test_writes_only_when_every_expectation_holds(self, db):
        await db.upsert_task("t", "t.md", "T")
        assert await db.transition_task("t", "in_progress", expect=("done",)) is False
        assert await db.transition_task("t", "in_progress", expect_revision=7) is False
        assert (await db.get_task("t"))["status"] == "pending"

        assert await db.transition_task(
            "t", "in_progress", expect=("pending",), expect_revision=1,
        ) is True
        row = await db.get_task("t")
        assert (row["status"], row["revision"]) == ("in_progress", 2)
        assert await db.transition_task("ghost", "done") is False

    async def test_same_lane_keeps_rank_and_records_nothing(self, db):
        await db.upsert_task("t", "t.md", "T")
        position, events = (await db.get_task("t"))["position"], await db.list_task_events("t")
        assert await db.transition_task("t", "pending") is True
        assert (await db.get_task("t"))["position"] == position
        assert await db.list_task_events("t") == events

    async def test_new_lane_ranks_on_top_and_records_one_event(self, db):
        await db.upsert_task("a", "a.md", "A", status="in_progress")
        await db.upsert_task("b", "b.md", "B")
        before = await db.list_task_events("b")
        assert await db.transition_task("b", "in_progress", actor="s1") is True
        assert (await db.get_task("b"))["position"] < (await db.get_task("a"))["position"]
        events = await db.list_task_events("b")
        assert len(events) == len(before) + 1
        assert (events[-1]["from_status"], events[-1]["to_status"], events[-1]["actor"]) == (
            "pending", "in_progress", "s1",
        )

    async def test_content_resyncs_search_in_the_same_write(self, db):
        await db.upsert_task("t", "t.md", "T", content="before")
        assert await db.transition_task("t", "done", content="before zebracorn") is True
        assert [r["id"] for r in await db.search_tasks("zebracorn", status="all")] == ["t"]

    async def test_a_write_from_another_process_is_seen(self, db):
        await db.upsert_task("t", "t.md", "T")
        _external(db, "UPDATE tasks SET revision = revision + 1 WHERE id = 't'")
        assert await db.upsert_task("t", "t.md", "T2", expect_revision=1) is False
        assert await db.transition_task("t", "done", expect_revision=1) is False
        row = await db.get_task("t")
        assert (row["title"], row["status"]) == ("T", "pending")

    async def test_a_second_reopen_racing_the_first_is_refused(self, db, monkeypatch):
        """Both reopens pass the "is it done?" check; only the first flip lands."""
        await db.upsert_task("t", "t.md", "T", status="done")
        ctx = ToolContext(session_id="test", db=db)  # no workspace, so no file moves
        real = db.transition_task
        raced = False

        async def racing(*args, **kwargs):
            nonlocal raced
            if not raced:
                raced = True
                await task_reopen_handler(ctx, {"task_id": "t", "status": "in_progress"})
            return await real(*args, **kwargs)

        monkeypatch.setattr(db, "transition_task", racing)
        outer = await task_reopen_handler(ctx, {"task_id": "t", "status": "pending"})
        assert outer.is_error and "no longer done" in outer.text_content
        assert (await db.get_task("t"))["status"] == "in_progress"

    async def test_a_reopen_that_loses_the_race_moves_no_file(
        self, ctx, db, workspace, monkeypatch,
    ):
        task_id = await _create(ctx)
        await task_done_handler(ctx, {"task_id": task_id})
        real = db.transition_task
        raced = False

        async def racing(*args, **kwargs):
            nonlocal raced
            if not raced:
                raced = True
                await task_reopen_handler(ctx, {"task_id": task_id, "status": "in_progress"})
            return await real(*args, **kwargs)

        moves = 0
        real_move = handlers.move_task_file

        def counting_move(*args, **kwargs):
            nonlocal moves
            moves += 1
            return real_move(*args, **kwargs)

        monkeypatch.setattr(db, "transition_task", racing)
        monkeypatch.setattr(handlers, "move_task_file", counting_move)
        outer = await task_reopen_handler(ctx, {"task_id": task_id, "status": "pending"})
        assert outer.is_error
        assert moves == 1
        active = _file(workspace, task_id).read_text()
        assert "REOPENED (in_progress)" in active and "REOPENED (pending)" not in active
        row = await db.get_task(task_id)
        assert (row["status"], row["file_path"]) == (
            "in_progress", f"memory/tasks/active/{task_id}.md",
        )
        assert not _file(workspace, task_id, "done").exists()


@pytest.mark.asyncio
class TestCompletion:
    @pytest.mark.parametrize("via", ["task_done", "mark_done"])
    async def test_search_sees_the_done_line(self, ctx, db, workspace, via):
        task_id = await _create(ctx)
        if via == "task_done":
            await task_done_handler(ctx, {"task_id": task_id, "note": "zebracorn"})
            word = "zebracorn"
        else:
            assert await TaskManager(workspace, db).mark_done(task_id) is True
            word = "done"
        assert [r["id"] for r in await db.search_tasks(word, status="all")] == [task_id]

    async def test_mark_done_with_the_file_gone_still_completes_the_row(self, ctx, db, workspace):
        task_id = await _create(ctx)
        _file(workspace, task_id).unlink()
        assert await TaskManager(workspace, db).mark_done(task_id) is True
        assert (await db.get_task(task_id))["status"] == "done"

    async def test_mark_done_with_a_stale_token_changes_nothing(self, ctx, db, workspace):
        task_id = await _create(ctx)
        stale = await _stale_token(db, task_id)
        assert await TaskManager(workspace, db).mark_done(task_id, expect_revision=stale) is False
        assert (await db.get_task(task_id))["status"] == "pending"
        assert _file(workspace, task_id).exists()
        assert not _file(workspace, task_id, "done").exists()


@pytest.mark.asyncio
class TestFencedTools:
    async def test_task_done(self, ctx, db, workspace):
        task_id = await _create(ctx)
        await db.create_plan("p1", task_id, "plan")
        await db.update_plan("p1", status="implementing")
        stale = await _stale_token(db, task_id)

        result = await task_done_handler(ctx, {"task_id": task_id, "expect_revision": stale})
        assert result.is_error
        assert (await db.get_task(task_id))["status"] == "pending"
        assert _file(workspace, task_id).exists()
        assert (await db.get_plan("p1"))["status"] == "implementing"

    @pytest.mark.parametrize("edit", [{"note": "late"}, {"deadline": "2030-01-01"}])
    async def test_task_update_leaves_the_file_alone(self, ctx, db, workspace, edit):
        task_id = await _create(ctx)
        stale = await _stale_token(db, task_id)
        before = _file(workspace, task_id).read_bytes()

        result = await task_update_handler(
            ctx, {"task_id": task_id, "expect_revision": stale, **edit},
        )
        assert result.is_error
        assert _file(workspace, task_id).read_bytes() == before
        assert (await db.get_task(task_id))["deadline"] is None

    @pytest.mark.parametrize("edit", [
        {"status": "in_progress"}, {"tags": "x"},
        {"note": "late"}, {"deadline": "2030-01-01"}, {"title": "Renamed"},
    ])
    async def test_task_update_without_a_task_file(self, ctx, db, workspace, edit):
        task_id = await _create(ctx)
        _file(workspace, task_id).unlink()
        stale = await _stale_token(db, task_id)
        row = await db.get_task(task_id)

        result = await task_update_handler(
            ctx, {"task_id": task_id, "expect_revision": stale, **edit},
        )
        assert result.is_error
        assert await db.get_task(task_id) == row

    async def test_a_current_token_cannot_edit_a_missing_file(self, ctx, db, workspace):
        task_id = await _create(ctx)
        _file(workspace, task_id).unlink()
        result = await task_update_handler(ctx, {
            "task_id": task_id, "deadline": "2030-01-01",
            "expect_revision": await _rev(db, task_id),
        })
        assert result.is_error
        assert (await db.get_task(task_id))["deadline"] is None

    async def test_task_update_to_done_forwards_it(self, ctx, db):
        task_id = await _create(ctx)
        stale = await _stale_token(db, task_id)
        result = await task_update_handler(
            ctx, {"task_id": task_id, "status": "done", "expect_revision": stale},
        )
        assert result.is_error
        assert (await db.get_task(task_id))["status"] == "pending"

    async def test_task_update_out_of_done_forwards_it(self, ctx, db):
        task_id = await _create(ctx)
        await task_done_handler(ctx, {"task_id": task_id})
        stale = await _stale_token(db, task_id)
        result = await task_update_handler(
            ctx, {"task_id": task_id, "status": "pending", "expect_revision": stale},
        )
        assert result.is_error
        assert (await db.get_task(task_id))["status"] == "done"

    async def test_edits_after_a_reopen_are_fenced_on_the_reopened_row(self, ctx, db):
        task_id = await _create(ctx)
        await task_done_handler(ctx, {"task_id": task_id})
        result = await task_update_handler(ctx, {
            "task_id": task_id, "status": "pending", "tags": "x",
            "expect_revision": await _rev(db, task_id),
        })
        assert not result.is_error
        row = await db.get_task(task_id)
        assert (row["status"], row["tags"]) == ("pending", "x")

    async def test_a_completion_between_a_reopen_and_its_edits_is_not_undone(
        self, ctx, db, workspace, monkeypatch,
    ):
        task_id = await _create(ctx)
        await task_done_handler(ctx, {"task_id": task_id})
        token = await _rev(db, task_id)
        real_reopen = handlers.task_reopen_handler

        async def reopen_then_complete(*args, **kwargs):
            reopened = await real_reopen(*args, **kwargs)
            done = await task_done_handler(
                ctx, {"task_id": task_id, "expect_revision": await _rev(db, task_id)},
            )
            assert not done.is_error
            return reopened

        monkeypatch.setattr(handlers, "task_reopen_handler", reopen_then_complete)
        result = await task_update_handler(ctx, {
            "task_id": task_id, "status": "pending", "tags": "x", "expect_revision": token,
        })
        assert result.is_error and "reopened" in result.text_content
        row = await db.get_task(task_id)
        assert (row["status"], row["file_path"]) == ("done", f"memory/tasks/done/{task_id}.md")
        assert "x" not in (row["tags"] or "")
        assert _file(workspace, task_id, "done").exists()
        assert not _file(workspace, task_id).exists()


@pytest.mark.asyncio
async def test_without_a_token_writes_stay_unconditional(ctx, db, workspace):
    task_id = await _create(ctx)
    await db.update_task_tags(task_id, "moved-on")
    assert not (await task_update_handler(ctx, {"task_id": task_id, "note": "n"})).is_error
    assert not (await task_done_handler(ctx, {"task_id": task_id})).is_error
    assert not (await task_reopen_handler(ctx, {"task_id": task_id, "status": "pending"})).is_error
    row = await db.get_task(task_id)
    assert (row["status"], row["file_path"]) == ("pending", f"memory/tasks/active/{task_id}.md")


@pytest.mark.asyncio
async def test_task_read_reports_it_and_task_write_strips_it(ctx, db, workspace):
    task_id = await _create(ctx)
    read = await task_read_handler(ctx, {"task_id": task_id})
    trailer = "<!-- nerve: revision=1 (index metadata, not file content) -->"
    assert read.structured == {"revision": 1}
    assert read.text_content.endswith(f"\n{trailer}")

    await task_write_handler(
        ctx, {"task_id": task_id, "content": read.text_content.replace("body", "new body")},
    )
    written = _file(workspace, task_id).read_text()
    assert "new body" in written and "nerve: revision" not in written

    read = await task_read_handler(ctx, {"task_id": task_id})
    await task_write_handler(
        ctx, {"task_id": task_id, "content": read.text_content + "\n- appended below the trailer\n"},
    )
    written = _file(workspace, task_id).read_text()
    assert "appended below the trailer" in written and "nerve: revision" not in written
    assert (await task_write_handler(ctx, {"task_id": task_id, "content": trailer})).text_content == (
        "Cannot write empty content."
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fenced", [False, True], ids=["no-token", "token"])
@pytest.mark.parametrize("op", ["task_update", "task_done", "mark_done", "task_reopen"])
async def test_write_order(ctx, db, workspace, monkeypatch, op, fenced):
    """Without a token task_update, task_done and mark_done write the file first.
    With a token, and for every reopen, the row is written first, so a refused
    caller touches no file."""
    task_id = await _create(ctx)
    if op == "task_reopen":
        await task_done_handler(ctx, {"task_id": task_id})
    order: list[str] = []

    def spy(real, mark):
        def wrapper(*args, **kwargs):
            order.append(mark)
            return real(*args, **kwargs)
        return wrapper

    if op == "task_update":
        monkeypatch.setattr(Path, "write_text", spy(Path.write_text, "file"))
    else:
        for module in (handlers, manager_module):
            monkeypatch.setattr(module, "move_task_file", spy(module.move_task_file, "file"))
    for method in ("upsert_task", "transition_task"):
        monkeypatch.setattr(db, method, spy(getattr(db, method), "row"))

    token = {"expect_revision": await _rev(db, task_id)} if fenced else {}
    if op == "task_update":
        await task_update_handler(ctx, {"task_id": task_id, "note": "n", **token})
    elif op == "task_done":
        await task_done_handler(ctx, {"task_id": task_id, **token})
    elif op == "mark_done":
        await TaskManager(workspace, db).mark_done(task_id, **token)
    else:
        await task_reopen_handler(ctx, {"task_id": task_id, "status": "pending", **token})
    assert order == (["row", "file"] if fenced or op == "task_reopen" else ["file", "row"])
