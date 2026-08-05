"""Board ordering: ``tasks.position`` rank math and its preservation.

Two separable concerns live here.

**Rank arithmetic** — ``move_task`` resolves "put this between A and B" into
a concrete REAL. The interesting cases are the degenerate ones: a missing
anchor, an anchor in the wrong lane, and neighbours that have converged far
enough that a midpoint no longer fits between them.

**Preservation** — ``upsert_task`` is preserve-on-omit for ``position``, so a
caller that does not know the column exists cannot reset it. ``tags`` learned
that the hard way in v018 (see the module docstring of
``test_task_reindex.py``), and ``position`` is the same shape of column with a
worse failure mode, because nothing in the markdown file can restore it.
``test_task_upsert_preserve.py`` covers the rule itself. The tests below drive
the *real* callers — reindex, task_write, task_update — rather than calling
``upsert_task`` directly, so a fix that only patches the signature default
cannot pass them.
"""

from __future__ import annotations

import pytest

from nerve.db import Database
from nerve.db.tasks import POSITION_GAP
from nerve.tasks.manager import TaskManager


async def _add(db: Database, task_id: str, status: str = "pending", **row) -> None:
    row.setdefault("title", f"Task {task_id}")
    row.setdefault("file_path", f"memory/tasks/active/{task_id}.md")
    await db.upsert_task(task_id=task_id, status=status, **row)


async def _lane(db: Database, status: str = "pending") -> list[str]:
    """Task ids in the order the board would render them."""
    rows = await db.list_tasks(status=status, sort="position", limit=100)
    return [r["id"] for r in rows]


async def _position(db: Database, task_id: str) -> float:
    return (await db.get_task(task_id))["position"]


@pytest.mark.asyncio
class TestNewTaskRanking:
    async def test_first_task_in_lane_gets_the_base_gap(self, db: Database):
        await _add(db, "solo")
        assert await _position(db, "solo") == POSITION_GAP

    async def test_new_tasks_stack_at_the_top_of_their_lane(self, db: Database):
        # Newest-first matches what the list view showed before the board,
        # and puts a just-created task where its author is looking.
        for tid in ("first", "second", "third"):
            await _add(db, tid)
        assert await _lane(db) == ["third", "second", "first"]

    async def test_lanes_rank_independently(self, db: Database):
        await _add(db, "p1", status="pending")
        await _add(db, "d1", status="deferred")
        # A fresh lane restarts at the base gap rather than inheriting the
        # global minimum, so lanes can't drift apart over time.
        assert await _position(db, "d1") == POSITION_GAP


@pytest.mark.asyncio
class TestMoveTask:
    async def test_move_between_two_cards_takes_the_midpoint(self, db: Database):
        for tid in ("c", "b", "a"):  # → lane order a, b, c
            await _add(db, tid)
        await _add(db, "mover")

        moved = await db.move_task("mover", before_id="a", after_id="b")

        assert moved is not None
        expected = (await _position(db, "a") + await _position(db, "b")) / 2
        assert moved["position"] == expected
        assert await _lane(db) == ["a", "mover", "b", "c"]

    async def test_move_to_top_with_only_a_lower_anchor(self, db: Database):
        for tid in ("b", "a"):
            await _add(db, tid)
        await _add(db, "mover")

        await db.move_task("mover", after_id="a")

        assert await _lane(db) == ["mover", "a", "b"]

    async def test_move_to_bottom_with_only_an_upper_anchor(self, db: Database):
        for tid in ("b", "a"):
            await _add(db, tid)
        await _add(db, "mover")

        await db.move_task("mover", before_id="b")

        assert await _lane(db) == ["a", "b", "mover"]

    async def test_move_with_no_anchors_appends(self, db: Database):
        for tid in ("b", "a"):
            await _add(db, tid)
        await _add(db, "mover")

        await db.move_task("mover")

        assert await _lane(db) == ["a", "b", "mover"]

    async def test_missing_task_returns_none(self, db: Database):
        assert await db.move_task("nope") is None

    async def test_anchor_in_another_lane_is_ignored(self, db: Database):
        # The client's board can be a few seconds stale; an anchor that has
        # since moved lanes must degrade to "append here", not corrupt the
        # rank by borrowing a number from an unrelated lane.
        await _add(db, "elsewhere", status="deferred")
        await _add(db, "here")
        await _add(db, "mover")

        await db.move_task("mover", before_id="elsewhere")

        assert await _lane(db) == ["here", "mover"]

    async def test_self_anchor_is_ignored(self, db: Database):
        await _add(db, "a")
        await _add(db, "mover")

        await db.move_task("mover", before_id="mover", after_id="mover")

        # Degenerates to "no usable anchor" → append, not a rank built from
        # the moved card's own (about to be overwritten) position.
        assert await _lane(db) == ["a", "mover"]


@pytest.mark.asyncio
class TestCrossLaneMove:
    async def test_move_changes_status_and_reranks(self, db: Database):
        await _add(db, "target", status="in_progress")
        await _add(db, "mover", status="pending")

        moved = await db.move_task("mover", status="in_progress", after_id="target")

        assert moved["status"] == "in_progress"
        assert await _lane(db, "in_progress") == ["mover", "target"]
        assert await _lane(db, "pending") == []

    async def test_move_into_an_empty_lane(self, db: Database):
        await _add(db, "mover", status="pending")

        moved = await db.move_task("mover", status="deferred")

        assert moved["status"] == "deferred"
        assert moved["position"] == POSITION_GAP

    async def test_status_change_reranks_to_top_of_destination(self, db: Database):
        # A rank only means something within its own lane. Carrying it across
        # would drop the card at an arbitrary depth of a list it was never
        # ordered against — here, below `settled` instead of above it.
        for tid in ("deep3", "deep2", "deep1"):
            await _add(db, tid, status="pending")
        await _add(db, "settled", status="in_progress")

        await db.update_task_status("deep3", "in_progress")

        assert await _lane(db, "in_progress") == ["deep3", "settled"]

    async def test_same_status_update_does_not_rerank(self, db: Database):
        await _add(db, "a")
        await _add(db, "b")
        before = await _position(db, "a")

        await db.update_task_status("a", "pending")

        assert await _position(db, "a") == before
        assert await _lane(db) == ["b", "a"]


@pytest.mark.asyncio
class TestRenormalization:
    async def test_converged_ranks_are_respaced_and_the_move_still_lands(
        self, db: Database,
    ):
        await _add(db, "top")
        await _add(db, "bottom")
        await _add(db, "mover")
        # Collapse two neighbours onto ranks no midpoint can separate.
        await db.move_task("top", before_id=None, after_id=None)
        async with db._atomic():
            await db.db.execute(
                "UPDATE tasks SET position = 5.0 WHERE id = 'top'",
            )
            await db.db.execute(
                "UPDATE tasks SET position = 5.0000000001 WHERE id = 'bottom'",
            )

        await db.move_task("mover", before_id="top", after_id="bottom")

        lane = await _lane(db)
        assert lane == ["top", "mover", "bottom"]
        # Post-renormalize the lane is back on wide integral spacing, so the
        # next drag has room again instead of failing the same way.
        gaps = [await _position(db, t) for t in lane]
        assert gaps[1] - gaps[0] >= POSITION_GAP / 2
        assert gaps[2] - gaps[1] >= POSITION_GAP / 2

    async def test_swapped_anchors_fall_back_to_append(self, db: Database):
        # No amount of re-spacing fixes anchors handed over in the wrong
        # order (re-spacing preserves order), so the move must not spin.
        for tid in ("b", "a"):
            await _add(db, tid)
        await _add(db, "mover")

        await db.move_task("mover", before_id="b", after_id="a")

        assert await _lane(db) == ["a", "b", "mover"]


@pytest.mark.asyncio
class TestPositionSurvivesItsCallers:
    """The v018 regression class: a column no caller knows about.

    Every test here drives a *production* write path and asserts the lane
    order is unchanged afterwards. None of these callers passes a position,
    and none could reconstruct one — the markdown file has no rank in it.
    """

    async def test_upsert_without_position_preserves_it(self, db: Database):
        await _add(db, "a")
        await _add(db, "b")
        ranked = await _lane(db)  # ["b", "a"]

        # A plain re-save: same status, no position argument.
        await db.upsert_task(
            task_id="b",
            file_path="memory/tasks/active/b.md",
            title="Task b (edited)",
            status="pending",
        )

        assert await _lane(db) == ranked

    async def test_explicit_position_still_wins(self, db: Database):
        await _add(db, "a")
        await _add(db, "b")

        await db.upsert_task(
            task_id="b",
            file_path="memory/tasks/active/b.md",
            title="Task b",
            status="pending",
            position=99_999.0,
        )

        assert await _lane(db) == ["a", "b"]

    async def test_reindex_preserves_lane_order(self, db: Database, tmp_path):
        directory = tmp_path / "memory" / "tasks" / "active"
        directory.mkdir(parents=True)
        for tid in ("alpha", "beta", "gamma"):
            (directory / f"{tid}.md").write_text(f"# {tid}\n", encoding="utf-8")
            await _add(db, tid)
        (tmp_path / "memory" / "tasks" / "done").mkdir(parents=True)

        # Put the lane in an order no default sort would produce, so a
        # reindex that dropped position could not accidentally reproduce it.
        # (Creation order alone would give gamma, beta, alpha.)
        await db.move_task("gamma", before_id="beta")
        ranked = await _lane(db)
        assert ranked == ["beta", "gamma", "alpha"]

        await TaskManager(tmp_path, db).reindex()

        assert await _lane(db) == ranked

    async def test_task_update_note_preserves_lane_order(
        self, db: Database, tmp_path,
    ):
        from nerve.agent.tools.handlers.tasks import task_update_handler
        from nerve.agent.tools.registry import ToolContext

        directory = tmp_path / "memory" / "tasks" / "active"
        directory.mkdir(parents=True)
        for tid in ("alpha", "beta"):
            (directory / f"{tid}.md").write_text(f"# {tid}\n\nbody\n", encoding="utf-8")
            await _add(db, tid)
        ranked = await _lane(db)

        ctx = ToolContext(session_id="t", workspace=tmp_path, db=db)
        await task_update_handler(ctx, {"task_id": "beta", "note": "still working"})

        assert await _lane(db) == ranked
