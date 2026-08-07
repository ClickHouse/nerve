"""``task_create`` must never write over an existing task.

A task ID is ``<date>-<slug>`` where the slug is the title truncated to 40
characters. Two titles that differ only past that cut therefore produce the
same ID on the same day -- and the handler used to write it out regardless:
``write_text`` truncated the older task's markdown to the new body, and
``upsert_task`` rewrote that same row's title, status and content. Both copies
of the older task were gone, and the tool still answered ``Task created``.

The duplicate check does not cover this. It reports *similar* tasks and offers
``confirm_duplicate=true``, which was the one flag that skipped straight past
the report into the overwrite. Confirming a duplicate means "create a second
task", so a taken ID now moves the new task to the next free suffix.

Each collision test asserts up front that the two titles really do share a base
ID. Without that a change to the slug rule would make them pass while testing
nothing.
"""

from __future__ import annotations

import asyncio

import pytest

from nerve.agent.tools.handlers import tasks as task_handlers
from nerve.agent.tools.handlers.tasks import (
    task_create_handler,
    task_done_handler,
    task_read_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.db import Database

# Same first 40 characters, different tails.
PREFIX = "the importer rebuilds nested records from the wire format"
TITLE_A = f"{PREFIX} lossily"
TITLE_B = f"{PREFIX} without a branch for one case"
TITLE_C = f"{PREFIX} for a third distinct reason"


def _text(result) -> str:
    return result.content[0]["text"]


def _ctx(db: Database | None, workspace) -> ToolContext:
    return ToolContext(session_id="test", db=db, workspace=workspace)


def _active_dir(workspace):
    return workspace / "memory" / "tasks" / "active"


def _done_dir(workspace):
    return workspace / "memory" / "tasks" / "done"


def _ids(workspace, subdir: str) -> list[str]:
    return sorted(p.stem for p in (workspace / "memory" / "tasks" / subdir).glob("*.md"))


async def _create(ctx, title: str, content: str) -> str:
    """Create a task past the similarity report, returning the response text."""
    result = await task_create_handler(
        ctx, {"title": title, "content": content, "confirm_duplicate": True},
    )
    assert result.is_error is False, _text(result)
    return _text(result)


def _assert_bases_collide(ctx, *titles: str) -> str:
    """Guard against a vacuous test: the titles must share one base ID."""
    bases = {task_handlers._make_task_id(t, ctx) for t in titles}
    assert len(bases) == 1, f"harness: titles no longer collide ({bases})"
    return bases.pop()


@pytest.mark.asyncio
class TestSlugCollision:
    async def test_second_create_keeps_the_first_task(self, db: Database, tmp_path):
        """The older task keeps its ID, its file and its row; the new one moves."""
        ctx = _ctx(db, tmp_path)
        base = _assert_bases_collide(ctx, TITLE_A, TITLE_B)

        await _create(ctx, TITLE_A, "first body")
        second = await _create(ctx, TITLE_B, "second body")

        assert _ids(tmp_path, "active") == [base, f"{base}-2"]
        # The response has to name the ID that was actually claimed, or the
        # caller updates someone else's task next.
        assert f"Task created: {base}-2" in second

        rows = {r["id"]: r for r in await db.list_tasks(status="all")}
        assert set(rows) == {base, f"{base}-2"}
        assert rows[base]["title"] == TITLE_A
        assert rows[f"{base}-2"]["title"] == TITLE_B

        first_file = (_active_dir(tmp_path) / f"{base}.md").read_text(encoding="utf-8")
        assert f"# {TITLE_A}" in first_file
        assert "first body" in first_file
        assert "second body" not in first_file
        # The row's indexed content follows the file, so search still finds it.
        assert [r["id"] for r in await db.search_tasks("first body")] == [base]

    async def test_suffixes_keep_walking(self, db: Database, tmp_path):
        """A third collision takes ``-3`` rather than reusing ``-2``."""
        ctx = _ctx(db, tmp_path)
        base = _assert_bases_collide(ctx, TITLE_A, TITLE_B, TITLE_C)

        await _create(ctx, TITLE_A, "first body")
        await _create(ctx, TITLE_B, "second body")
        await _create(ctx, TITLE_C, "third body")

        assert _ids(tmp_path, "active") == [base, f"{base}-2", f"{base}-3"]
        assert len(await db.list_tasks(status="all")) == 3

    async def test_completed_task_ids_stay_reserved(self, db: Database, tmp_path):
        """A finished task must not be resurrected as a new pending one.

        Its file has moved to done/ but its row still holds the ID, so writing
        the ID again flipped that row back to pending and repointed it at the
        new active file -- taking the completed task out of done/ in the index
        while its markdown sat there untouched.
        """
        ctx = _ctx(db, tmp_path)
        base = _assert_bases_collide(ctx, TITLE_A, TITLE_B)

        await _create(ctx, TITLE_A, "first body")
        await task_done_handler(ctx, {"task_id": base, "note": "shipped"})
        await _create(ctx, TITLE_B, "second body")

        assert _ids(tmp_path, "done") == [base]
        assert _ids(tmp_path, "active") == [f"{base}-2"]

        finished = await db.get_task(base)
        assert (finished["status"], finished["title"]) == ("done", TITLE_A)
        assert finished["file_path"] == f"memory/tasks/done/{base}.md"
        assert "shipped" in (tmp_path / finished["file_path"]).read_text(encoding="utf-8")

        fresh = await db.get_task(f"{base}-2")
        assert (fresh["status"], fresh["title"]) == ("pending", TITLE_B)

    async def test_untracked_file_is_not_overwritten(self, db: Database, tmp_path):
        """The file is checked too, not just the row.

        An active/ file with no row is what a lost or not-yet-rebuilt index
        looks like. Trusting the row alone would overwrite it, and reindex
        would then have nothing left to recover.
        """
        ctx = _ctx(db, tmp_path)
        base = _assert_bases_collide(ctx, TITLE_A, TITLE_B)

        _active_dir(tmp_path).mkdir(parents=True, exist_ok=True)
        orphan = _active_dir(tmp_path) / f"{base}.md"
        orphan.write_text(f"# {TITLE_A}\n\nunindexed body\n", encoding="utf-8")

        await _create(ctx, TITLE_B, "second body")

        assert orphan.read_text(encoding="utf-8") == f"# {TITLE_A}\n\nunindexed body\n"
        assert _ids(tmp_path, "active") == [base, f"{base}-2"]

    async def test_row_without_a_file_still_holds_its_id(self, db: Database, tmp_path):
        """The row check is the only guard once the tree has lost the file.

        A row outlives its markdown -- deleted by hand, or written under a
        path this workspace no longer has. It is then the last record of that
        task, and ``upsert_task`` would rewrite it in place: its status,
        source and tags replaced by the new task's, with nothing on disk left
        to reindex from.
        """
        ctx = _ctx(db, tmp_path)
        base = _assert_bases_collide(ctx, TITLE_A, TITLE_B)
        await db.upsert_task(
            task_id=base,
            file_path=f"memory/tasks/active/{base}.md",
            title=TITLE_A,
            status="in_progress",
            source="github",
            source_url="https://example.invalid/issues/7",
            tags="ops",
            content="first body",
        )
        assert not (_active_dir(tmp_path) / f"{base}.md").exists(), (
            "harness: the row is supposed to have no file"
        )

        await _create(ctx, TITLE_B, "second body")

        survivor = await db.get_task(base)
        assert (survivor["title"], survivor["status"]) == (TITLE_A, "in_progress")
        assert survivor["source_url"] == "https://example.invalid/issues/7"
        assert survivor["tags"] == "ops"
        assert _ids(tmp_path, "active") == [f"{base}-2"]

    async def test_done_file_without_a_row_is_not_shadowed(self, db: Database, tmp_path):
        """Same rule for done/: an unindexed completed task keeps its ID.

        Reusing it would put two files with one ID in the tree, and a reindex
        would then have to pick a winner.
        """
        ctx = _ctx(db, tmp_path)
        base = _assert_bases_collide(ctx, TITLE_A, TITLE_B)

        _done_dir(tmp_path).mkdir(parents=True, exist_ok=True)
        (_done_dir(tmp_path) / f"{base}.md").write_text(
            f"# {TITLE_A}\n\nfinished body\n", encoding="utf-8",
        )

        await _create(ctx, TITLE_B, "second body")

        assert _ids(tmp_path, "active") == [f"{base}-2"]
        assert _ids(tmp_path, "done") == [base]

    async def test_a_rival_inside_the_check_window_is_not_overwritten(
        self, db: Database, tmp_path, monkeypatch,
    ):
        """The write itself is the gate, not a check that precedes it.

        The rival file appears *after* the free-ID check has already passed,
        which is the window two concurrent creators share: the row is only
        written once the file is, so neither can see the other coming. Only
        an exclusive create refuses at that point -- a preceding
        ``path.exists()`` looked, found nothing, and then overwrote.

        The hook fires inside the real call, so it stands in for the rival
        without needing two live sessions.
        """
        ctx = _ctx(db, tmp_path)
        base = _assert_bases_collide(ctx, TITLE_A, TITLE_B)
        state = {"fired": False}
        real_write = task_handlers._write_new_file

        def rival_then_write(path, content):
            if not state["fired"]:
                state["fired"] = True
                path.write_text("rival body\n", encoding="utf-8")
            real_write(path, content)

        monkeypatch.setattr(task_handlers, "_write_new_file", rival_then_write)

        await _create(ctx, TITLE_A, "first body")

        assert state["fired"], "harness: the rival hook never ran"
        assert (_active_dir(tmp_path) / f"{base}.md").read_text(
            encoding="utf-8",
        ) == "rival body\n"
        assert _ids(tmp_path, "active") == [base, f"{base}-2"]
        assert (await db.get_task(f"{base}-2"))["title"] == TITLE_A

    async def test_concurrent_creates_do_not_share_an_id(self, db: Database, tmp_path):
        """The end-to-end shape of that race: two creates, two tasks.

        Which of the two takes the base ID is up to the scheduler, so this
        pins only the invariant. The window itself is pinned above.
        """
        ctx = _ctx(db, tmp_path)
        base = _assert_bases_collide(ctx, TITLE_A, TITLE_B)

        await asyncio.gather(
            _create(ctx, TITLE_A, "first body"),
            _create(ctx, TITLE_B, "second body"),
        )

        assert _ids(tmp_path, "active") == [base, f"{base}-2"]
        titles = {r["title"] for r in await db.list_tasks(status="all")}
        assert titles == {TITLE_A, TITLE_B}

    async def test_a_failed_create_gives_its_id_back(
        self, db: Database, tmp_path, monkeypatch,
    ):
        """A create that cannot index its task must not keep the ID.

        Claiming is now sticky, so a file left behind by a failed create
        would push every retry of the same title onto a fresh suffix and
        strand the orphan for a later reindex to adopt as a second task.
        """
        ctx = _ctx(db, tmp_path)
        base = _assert_bases_collide(ctx, TITLE_A, TITLE_B)
        boom = RuntimeError("index unavailable")

        async def failing_upsert(**kwargs):
            raise boom

        monkeypatch.setattr(db, "upsert_task", failing_upsert)
        with pytest.raises(RuntimeError):
            await task_create_handler(
                ctx, {"title": TITLE_A, "content": "first body", "confirm_duplicate": True},
            )

        assert _ids(tmp_path, "active") == []
        monkeypatch.undo()

        # The retry gets the ID the failed attempt was reaching for.
        await _create(ctx, TITLE_A, "first body")
        assert _ids(tmp_path, "active") == [base]

    async def test_exhausted_suffixes_refuse_instead_of_overwriting(
        self, db: Database, tmp_path, monkeypatch,
    ):
        """With every candidate taken, create fails loudly and touches nothing."""
        ctx = _ctx(db, tmp_path)
        base = _assert_bases_collide(ctx, TITLE_A, TITLE_B, TITLE_C)
        monkeypatch.setattr(task_handlers, "_MAX_TASK_ID_SUFFIX", 2)

        await _create(ctx, TITLE_A, "first body")
        await _create(ctx, TITLE_B, "second body")

        result = await task_create_handler(
            ctx, {"title": TITLE_C, "content": "third body", "confirm_duplicate": True},
        )

        assert result.is_error is True
        assert base in _text(result)
        assert _ids(tmp_path, "active") == [base, f"{base}-2"]
        assert len(await db.list_tasks(status="all")) == 2
        assert "first body" in (_active_dir(tmp_path) / f"{base}.md").read_text(
            encoding="utf-8",
        )

    async def test_claimed_id_is_readable(self, db: Database, tmp_path):
        """The suffixed ID is a real task: the row and the file agree on it."""
        ctx = _ctx(db, tmp_path)
        base = _assert_bases_collide(ctx, TITLE_A, TITLE_B)

        await _create(ctx, TITLE_A, "first body")
        await _create(ctx, TITLE_B, "second body")

        content = _text(await task_read_handler(ctx, {"task_id": f"{base}-2"}))
        assert f"# {TITLE_B}" in content
        assert "second body" in content

    async def test_creating_two_done_tasks_keeps_both(self, db: Database, tmp_path):
        """The terminal-status shortcut moves each claimed file to done/ separately.

        Creating straight into ``done`` routes through ``task_done``, so the
        claim has to survive a move out of active/ that happens before the
        next create looks at it.
        """
        ctx = _ctx(db, tmp_path)
        base = _assert_bases_collide(ctx, TITLE_A, TITLE_B)

        for title, body in ((TITLE_A, "first body"), (TITLE_B, "second body")):
            result = await task_create_handler(
                ctx,
                {
                    "title": title, "content": body,
                    "status": "done", "confirm_duplicate": True,
                },
            )
            assert result.is_error is False, _text(result)

        assert _ids(tmp_path, "done") == [base, f"{base}-2"]
        assert _ids(tmp_path, "active") == []
        rows = {r["id"]: r["title"] for r in await db.list_tasks(status="done")}
        assert rows == {base: TITLE_A, f"{base}-2": TITLE_B}

    async def test_a_title_landing_on_a_suffixed_id_moves_again(
        self, db: Database, tmp_path,
    ):
        """The suffix namespace overlaps the base one, and that is handled.

        Short titles are not truncated, so creating "rerun sync" twice takes
        both ``…-rerun-sync`` and ``…-rerun-sync-2`` -- and ``…-rerun-sync-2``
        is exactly the ID a task titled "rerun sync 2" derives for itself.
        """
        ctx = _ctx(db, tmp_path)
        base = task_handlers._make_task_id("rerun sync", ctx)
        assert task_handlers._make_task_id("rerun sync 2", ctx) == f"{base}-2", (
            "harness: the third title no longer lands on the second task's ID"
        )

        await _create(ctx, "rerun sync", "first body")
        await _create(ctx, "rerun sync", "second body")
        await _create(ctx, "rerun sync 2", "third body")

        assert _ids(tmp_path, "active") == sorted(
            [base, f"{base}-2", f"{base}-2-2"],
        )
        rows = {r["id"]: r["title"] for r in await db.list_tasks(status="all")}
        assert rows[f"{base}-2"] == "rerun sync"
        assert rows[f"{base}-2-2"] == "rerun sync 2"

    async def test_titles_with_nothing_to_slugify_still_split(
        self, db: Database, tmp_path,
    ):
        """The degenerate case: every such title shares one base every day.

        A title of pure punctuation slugifies to nothing, so this is where
        collisions were densest. The IDs it produces are ugly, but no task
        may be lost to one.
        """
        ctx = _ctx(db, tmp_path)
        base = _assert_bases_collide(ctx, "!!!", "???")

        await _create(ctx, "!!!", "first body")
        await _create(ctx, "???", "second body")

        assert _ids(tmp_path, "active") == [base, f"{base}-2"]
        rows = {r["id"]: r["title"] for r in await db.list_tasks(status="all")}
        assert rows == {base: "!!!", f"{base}-2": "???"}

    async def test_collision_is_resolved_without_an_index(self, tmp_path):
        """No DB, so the files alone have to carry the rule.

        ``ctx.db`` is optional, and the row check is the half that goes
        missing there -- the exclusive create is what still holds.
        """
        ctx = _ctx(None, tmp_path)
        base = _assert_bases_collide(ctx, TITLE_A, TITLE_B)

        await _create(ctx, TITLE_A, "first body")
        await _create(ctx, TITLE_B, "second body")

        assert _ids(tmp_path, "active") == [base, f"{base}-2"]
        assert "first body" in (_active_dir(tmp_path) / f"{base}.md").read_text(
            encoding="utf-8",
        )

    async def test_distinct_titles_keep_unsuffixed_ids(self, db: Database, tmp_path):
        """Unchanged behaviour: no collision, no suffix."""
        ctx = _ctx(db, tmp_path)
        first = await _create(ctx, "Rotate the staging credentials", "x")
        second = await _create(ctx, "Backfill the audit table", "y")

        assert "-2" not in first.splitlines()[0]
        assert "-2" not in second.splitlines()[0]
        assert len(_ids(tmp_path, "active")) == 2
