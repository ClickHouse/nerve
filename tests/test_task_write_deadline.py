"""``task_write`` must derive the ``deadline`` column from the content it wrote.

``task_write`` overwrites a task's markdown file and then re-syncs the DB row.
The ``deadline`` column is a pure projection of the file's ``**Deadline:**``
line: it has exactly one SQL writer (the ``upsert_task`` INSERT) and no
row-only setter, unlike ``status`` and ``tags`` which each have a dedicated
``update_task_*`` method. So there is no row state to preserve for it -- only a
file to agree with.

The handler used to fall back to a snapshot taken *before* the file was
replaced, which made the row assert a deadline the file it had just written
denied: a concurrent writer's value was dropped, and a deliberately cleared
deadline was resurrected. ``escalation.py`` reads that column, so a stale row
escalates on a date nothing asks for.

The rival writer here commits strictly *inside* the handler's snapshot ->
upsert window, via a one-shot wrapper on ``db.get_task``. Each test asserts the
hook fired and the rival's write was committed before asserting anything about
the outcome -- a rival that lands before the snapshot is read cannot expose the
bug at all, so without those assertions the tests would pass vacuously.
"""

from __future__ import annotations

import pytest

from nerve.agent.tools.handlers import tasks as task_handlers
from nerve.agent.tools.registry import ToolContext
from nerve.db import Database
from nerve.tasks.manager import TaskManager
from nerve.tasks.models import parse_task_frontmatter

TASK_ID = "t1"
REL_PATH = f"memory/tasks/active/{TASK_ID}.md"

WITH_DEADLINE = "# T1\n\n**Tags:** urgent\n**Deadline:** 2030-01-01\n\nbody\n"
NO_DEADLINE_LINE = "# T1\n\n**Tags:** urgent\n\nbody edited\n"
STATES_DEADLINE = "# T1\n\n**Tags:** urgent\n**Deadline:** 2030-01-01\n\nbody edited\n"

RIVAL_DEADLINE = "2030-06-30"
RIVAL_CONTENT = WITH_DEADLINE.replace("2030-01-01", RIVAL_DEADLINE)
CLEARED_CONTENT = WITH_DEADLINE.replace("**Deadline:** 2030-01-01\n", "")


async def _seed(tmp_path, db, monkeypatch):
    """Workspace + row holding ``2030-01-01``, ready for ``task_write``."""
    workspace = tmp_path / "ws"
    (workspace / "memory" / "tasks" / "active").mkdir(parents=True)
    (workspace / "memory" / "tasks" / "done").mkdir(parents=True)
    (workspace / REL_PATH).write_text(WITH_DEADLINE, encoding="utf-8")
    await db.upsert_task(
        task_id=TASK_ID, file_path=REL_PATH, title="T1", status="in_progress",
        deadline="2030-01-01", tags="urgent", content=WITH_DEADLINE,
    )
    monkeypatch.setattr(task_handlers, "_tasks_read", {TASK_ID})
    return workspace, ToolContext(session_id="test", db=db, workspace=workspace)


def _arm_rival(db, workspace, *, clear: bool):
    """Fire one rival write right after the handler reads its snapshot.

    Returns a dict the caller must check: ``fired`` proves the hook ran (so the
    rival really is inside the window) and ``committed`` proves the rival's
    write reached the DB.
    """
    state: dict[str, object] = {"fired": False, "committed": "<never>"}
    original_get_task = db.get_task

    async def hooked(task_id):
        row = await original_get_task(task_id)
        if not state["fired"]:
            state["fired"] = True
            content = CLEARED_CONTENT if clear else RIVAL_CONTENT
            (workspace / REL_PATH).write_text(content, encoding="utf-8")
            await db.upsert_task(
                task_id=TASK_ID, file_path=REL_PATH, title="T1",
                status="in_progress", tags="urgent",
                deadline=None if clear else RIVAL_DEADLINE, content=content,
            )
            state["committed"] = (await original_get_task(TASK_ID))["deadline"]
        return row

    db.get_task = hooked
    return state


def _assert_rival_landed(state, *, clear: bool):
    assert state["fired"], "harness: the get_task hook never fired"
    expected = None if clear else RIVAL_DEADLINE
    assert state["committed"] == expected, (
        f"harness: rival did not commit ({state['committed']!r} != {expected!r})"
    )


async def _reindex_oracle(tmp_path, db_path_name, workspace):
    """What the indexer derives from the file, i.e. the authoritative answer."""
    oracle_db = Database(tmp_path / db_path_name, workspace=workspace)
    await oracle_db.connect()
    try:
        await TaskManager(workspace, oracle_db).reindex()
        row = await oracle_db.get_task(TASK_ID)
        return row["deadline"] if row else None
    finally:
        await oracle_db.close()


@pytest.mark.asyncio
async def test_rival_moves_the_deadline_and_content_omits_the_line(tmp_path, db, monkeypatch):
    """Content is silent about the deadline, so the row must be silent too."""
    workspace, ctx = await _seed(tmp_path, db, monkeypatch)
    state = _arm_rival(db, workspace, clear=False)

    await task_handlers.task_write_handler(
        ctx, {"task_id": TASK_ID, "content": NO_DEADLINE_LINE},
    )
    _assert_rival_landed(state, clear=False)

    written = (workspace / REL_PATH).read_text(encoding="utf-8")
    assert written == NO_DEADLINE_LINE
    assert parse_task_frontmatter(written).get("deadline") is None
    row = await db.get_task(TASK_ID)
    # The stale snapshot value must not survive as the row's answer.
    assert row["deadline"] is None
    assert row["deadline"] == await _reindex_oracle(tmp_path, "oracle1.db", workspace)


@pytest.mark.asyncio
async def test_rival_clears_the_deadline_and_content_omits_the_line(tmp_path, db, monkeypatch):
    """A deliberately cleared deadline must not be resurrected."""
    workspace, ctx = await _seed(tmp_path, db, monkeypatch)
    state = _arm_rival(db, workspace, clear=True)

    await task_handlers.task_write_handler(
        ctx, {"task_id": TASK_ID, "content": NO_DEADLINE_LINE},
    )
    _assert_rival_landed(state, clear=True)

    row = await db.get_task(TASK_ID)
    assert row["deadline"] is None
    assert row["deadline"] == await _reindex_oracle(tmp_path, "oracle2.db", workspace)


@pytest.mark.asyncio
async def test_content_that_states_a_deadline_still_wins(tmp_path, db, monkeypatch):
    """Unchanged behaviour: a stated deadline is written even against a rival.

    This case already passed before the fix -- it pins that the change did not
    turn the content-derived path into a clear.
    """
    workspace, ctx = await _seed(tmp_path, db, monkeypatch)
    state = _arm_rival(db, workspace, clear=False)

    await task_handlers.task_write_handler(
        ctx, {"task_id": TASK_ID, "content": STATES_DEADLINE},
    )
    _assert_rival_landed(state, clear=False)

    row = await db.get_task(TASK_ID)
    assert row["deadline"] == "2030-01-01"
    assert row["deadline"] == await _reindex_oracle(tmp_path, "oracle3.db", workspace)


@pytest.mark.asyncio
async def test_dropping_the_deadline_line_clears_the_column(tmp_path, db, monkeypatch):
    """Single-threaded observable change: no line in the content -> no deadline.

    Previously the column kept its old value even though the file no longer
    stated one, leaving the row disagreeing with the file. The value is not
    lost destructively: the file is authoritative and a reindex reproduces
    whatever the file says, which is what the oracle below asserts.
    """
    workspace, ctx = await _seed(tmp_path, db, monkeypatch)

    await task_handlers.task_write_handler(
        ctx, {"task_id": TASK_ID, "content": NO_DEADLINE_LINE},
    )

    row = await db.get_task(TASK_ID)
    assert row["deadline"] is None
    assert row["deadline"] == await _reindex_oracle(tmp_path, "oracle4.db", workspace)
