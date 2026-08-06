"""The preserve-on-omit contract of ``TaskStore.upsert_task``.

Four columns hold state that a markdown file does not always carry:
``source``, ``source_url``, ``deadline`` and ``tags``. A caller that rewrites
a row for another reason — a file move, an appended note — omits them, and
the stored values have to survive. Under the full-replace semantics this
replaced, every such caller had to read the row and pass all four back, and
two shipped bugs came from a caller that forgot: ``reindex`` dropped ``tags``
after v018 added the column, and ``task_done`` nulled all four when it moved
a file into done/.

The tests here drive ``upsert_task`` directly, because the rule itself is the
subject. The caller-level regressions live with their callers
(``test_task_completion.py``, ``test_task_reindex.py``).
"""

from __future__ import annotations

import pytest

from nerve.db import Database


METADATA = {
    "source": "github",
    "source_url": "https://github.com/example/project/issues/7",
    "deadline": "2026-09-01",
    "tags": "backend,p1",
}

CONTENT = "Body text about queue draining."


async def _seed(db: Database, task_id: str = "seeded", **overrides) -> str:
    """A pending row carrying every metadata column."""
    await db.upsert_task(
        task_id=task_id,
        file_path=f"memory/tasks/active/{task_id}.md",
        title="Seeded",
        status="pending",
        content=CONTENT,
        **{**METADATA, **overrides},
    )
    return task_id


async def _rewrite(db: Database, task_id: str, **columns) -> None:
    """The shape every non-metadata caller uses: move the file, keep the rest."""
    await db.upsert_task(
        task_id=task_id,
        file_path=f"memory/tasks/done/{task_id}.md",
        title="Seeded",
        status="done",
        content=CONTENT,
        **columns,
    )


@pytest.mark.asyncio
class TestOmit:
    async def test_omitting_every_column_keeps_all_four(self, db: Database):
        task_id = await _seed(db)

        await _rewrite(db, task_id)

        row = await db.get_task(task_id)
        assert {field: row[field] for field in METADATA} == METADATA
        # The columns the caller did supply still replace.
        assert row["status"] == "done"
        assert row["file_path"] == f"memory/tasks/done/{task_id}.md"

    @pytest.mark.parametrize("column", sorted(METADATA))
    async def test_omitting_one_column_leaves_the_others_alone(
        self, db: Database, column: str,
    ):
        """One supplied column must not turn the rest into a full replace."""
        task_id = await _seed(db)
        replacement = "" if column == "tags" else "changed"

        await _rewrite(db, task_id, **{column: replacement})

        row = await db.get_task(task_id)
        assert row[column] == replacement
        assert {f: row[f] for f in METADATA if f != column} == {
            f: v for f, v in METADATA.items() if f != column
        }

    async def test_a_new_row_falls_back_to_the_column_defaults(self, db: Database):
        """Nothing is stored yet, so an omitted column has nothing to keep."""
        await db.upsert_task(
            task_id="fresh",
            file_path="memory/tasks/active/fresh.md",
            title="Fresh",
            content=CONTENT,
        )

        row = await db.get_task("fresh")
        assert row["source"] is None
        assert row["source_url"] is None
        assert row["deadline"] is None
        assert row["tags"] == ""


@pytest.mark.asyncio
class TestExplicitClear:
    """Clearing has to stay expressible, which is why ``None`` is not the
    'omitted' marker: the detail modal can drop a deadline or the last tag."""

    @pytest.mark.parametrize("column", ["source", "source_url", "deadline"])
    async def test_none_clears_a_nullable_column(self, db: Database, column: str):
        task_id = await _seed(db)

        await _rewrite(db, task_id, **{column: None})

        assert (await db.get_task(task_id))[column] is None

    async def test_an_empty_string_clears_tags(self, db: Database):
        task_id = await _seed(db)

        await _rewrite(db, task_id, tags="")

        assert (await db.get_task(task_id))["tags"] == ""


@pytest.mark.asyncio
class TestSearchIndex:
    async def test_preserved_tags_stay_searchable(self, db: Database):
        """The FTS text is rebuilt from scratch on every upsert, so a
        preserved column has to reach the index and not just the row."""
        task_id = await _seed(db)

        await _rewrite(db, task_id)

        matches = await db.search_tasks("queue draining", status="all", tag="p1")
        assert [match["id"] for match in matches] == [task_id]

    async def test_cleared_tags_leave_the_index(self, db: Database):
        task_id = await _seed(db)

        await _rewrite(db, task_id, tags="")

        assert await db.search_tasks("queue draining", status="all", tag="p1") == []


@pytest.mark.asyncio
async def test_a_stored_null_tags_column_resolves_to_empty(db: Database):
    """``tags`` gained a ``DEFAULT ''`` in v018, but a row written before it
    can hold NULL. Preserving that value must not reach ``str.replace`` in
    the FTS text build."""
    task_id = await _seed(db)
    await db.db.execute("UPDATE tasks SET tags = NULL WHERE id = ?", (task_id,))
    await db.db.commit()

    await _rewrite(db, task_id)

    assert (await db.get_task(task_id))["tags"] == ""
