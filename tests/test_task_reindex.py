"""Regression tests for TaskManager.reindex() column preservation.

``upsert_task`` replaces the whole row (its ``ON CONFLICT(id) DO UPDATE SET``
assigns every column unconditionally), so any argument ``reindex`` omits or
mis-keys is written back as that argument's signature default. These tests pin
one behaviour per affected column so a partial fix cannot pass.

Discriminator notes (measured against the unfixed tree, not assumed):

* ``**Status:**`` has no production writer, so the stored row is the only
  source for status. The directory decides terminality instead.
* ``search_tasks(query=<tag token>)`` is **not** a valid probe of the
  ``fts_content`` tag mirror: the raw ``**Tags:**`` line is part of ``content``,
  so the token matches through the body even when the ``tags`` column is empty.
  ``test_reindex_repopulates_fts_tag_mirror`` therefore uses a token that exists
  *only* in the stored ``tags`` column.
* A present-but-falsy ``tags`` field is reachable at any position in the file
  (the frontmatter value is line-bounded), which is what makes the
  ``"tags" in fields`` presence test observably different from a
  ``fields.get("tags")`` truthiness test. ``task_update(tags="-onlytag")``
  writes exactly that shape mid-file.
* Every preservation test also pins a file-derived value, because ``reindex``
  swallows per-file exceptions: see ``_assert_row_was_processed``.
"""

from __future__ import annotations

import pytest

from nerve.db import Database
from nerve.tasks.manager import TaskManager


SOURCE_URL = "https://github.com/ClickHouse/nerve/pull/1"
STALE_SOURCE_URL = "https://github.com/ClickHouse/nerve/pull/0"


def _write(tmp_path, task_id: str, content: str, *, done: bool = False) -> str:
    """Write a task file and return its workspace-relative path."""
    sub = "done" if done else "active"
    directory = tmp_path / "memory" / "tasks" / sub
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{task_id}.md"
    path.write_text(content, encoding="utf-8")
    return f"memory/tasks/{sub}/{task_id}.md"


async def _seed(db: Database, task_id: str, rel_path: str, content: str, **row) -> None:
    """Insert the pre-existing DB row a reindex will later overwrite."""
    row.setdefault("title", "Stored title")
    row.setdefault("status", "pending")
    await db.upsert_task(task_id=task_id, file_path=rel_path, content=content, **row)


def _assert_row_was_processed(row: dict, expected_title: str) -> None:
    """Prove the row was actually upserted, not skipped.

    ``reindex`` swallows every per-file exception (manager.py logs and
    continues), so a skipped file leaves the seeded row exactly as seeded --
    which is what a preservation assertion asserts. Every preservation test
    therefore also pins a file-derived value the seed did not already hold: the
    title from the H1, seeded as "Stale title". Paired with the ``reindex() ==
    1`` return (incremented only after a successful upsert) this separates
    "preserved" from "never touched": the count alone would be satisfied by an
    upsert that wrote defaults, the title alone by an upsert that never read the
    preserved column.
    """
    assert row["title"] == expected_title, "row was not resynced from the file"


# --- D1: status ---


@pytest.mark.asyncio
async def test_reindex_preserves_nonterminal_status_for_active_file(db: Database, tmp_path):
    """An active/ file must keep the stored nonterminal status, not reset to pending.

    This is the whole-fleet case: nothing writes a **Status:** line, so the
    directory default would silently overwrite every blocked task's status.
    """
    task_id = "2026-08-04-reindex-status-active"
    content = "# Keep status\n\n**Tags:** ci\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, status="on_review", tags="ci",
                title="Stale title")

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["status"] == "on_review"
    _assert_row_was_processed(row, "Keep status")


@pytest.mark.asyncio
async def test_reindex_forces_done_status_for_file_in_done_dir(db: Database, tmp_path):
    """A done/ file is terminal by definition, so a nonterminal row is repaired.

    The directory, not the row, is authoritative for terminality: ``task_done``
    moves the file into done/, so a done/ file carrying a nonterminal status is
    an orphan whose destination path already equals its source path.

    The fixture carries a **Status:** line so the outcome is decided by the
    directory rule under test rather than by the directory *default*, which
    already yields "done" here.
    """
    task_id = "2026-08-04-reindex-status-done"
    content = "# Terminal\n\n**Tags:** ci\n**Status:** monitoring_external\n\nBody.\n"
    rel = _write(tmp_path, task_id, content, done=True)
    await _seed(db, task_id, rel, content, status="monitoring_external", tags="ci")

    await TaskManager(tmp_path, db).reindex()

    row = await db.get_task(task_id)
    assert row["status"] == "done"


@pytest.mark.asyncio
async def test_reindex_repairs_orphan_done_status_on_active_file(db: Database, tmp_path):
    """The mirror case: status=done in the DB while the file is still in active/.

    docs/tasks.md calls this the orphan state; reindex resets it to the
    directory default rather than propagating it.

    As above, the **Status:** line makes the assertion depend on the rule under
    test instead of on the directory default, which already yields "pending".
    """
    task_id = "2026-08-04-reindex-status-orphan"
    content = "# Orphan\n\n**Tags:** ci\n**Status:** done\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, status="done", tags="ci")

    await TaskManager(tmp_path, db).reindex()

    row = await db.get_task(task_id)
    assert row["status"] == "pending"


@pytest.mark.asyncio
async def test_reindex_ignores_a_status_line_in_the_file(db: Database, tmp_path):
    """A **Status:** line in the body must never reach the status column.

    No writer emits that line, so the four live files that contain one carry
    prose. Reading it stored the prose verbatim as the task's status, which is
    not a value any status filter or the configurable-status table knows. This
    is also the only shape where the two status tests below are not decided by
    the directory default alone.
    """
    task_id = "2026-08-04-reindex-status-prose"
    content = "# Prose status\n\n**Tags:** ci\n**Status:** NEW master regression\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, status="on_review", tags="ci",
                title="Stale title")

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["status"] == "on_review"
    _assert_row_was_processed(row, "Prose status")


@pytest.mark.asyncio
async def test_reindex_seeds_new_rows_with_directory_default(db: Database, tmp_path):
    """Control: reindex still performs its primary job of indexing unseen files."""
    active_id = "2026-08-04-reindex-new-active"
    done_id = "2026-08-04-reindex-new-done"
    _write(tmp_path, active_id, "# New active\n\n**Tags:** a\n\nBody.\n")
    _write(tmp_path, done_id, "# New done\n\n**Tags:** b\n\nBody.\n", done=True)

    assert await TaskManager(tmp_path, db).reindex() == 2

    assert (await db.get_task(active_id))["status"] == "pending"
    assert (await db.get_task(done_id))["status"] == "done"


# --- D2: tags ---


@pytest.mark.asyncio
async def test_reindex_stores_tags_canonicalized(db: Database, tmp_path):
    """The on-disk form is display text; every reader's predicate needs the
    lowercase comma-joined form."""
    task_id = "2026-08-04-reindex-tags-canon"
    content = "# Canon\n\n**Tags:** Alpha, beta\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content)

    await TaskManager(tmp_path, db).reindex()

    assert (await db.get_task(task_id))["tags"] == "alpha,beta"


@pytest.mark.asyncio
async def test_reindex_keeps_row_retrievable_by_each_tag(db: Database, tmp_path):
    """Per key, not membership in a set: a single-key assertion can pass on a
    partial fix that stores only the first tag."""
    task_id = "2026-08-04-reindex-tags-filter"
    content = "# Filter\n\n**Tags:** Alpha, beta\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content)

    await TaskManager(tmp_path, db).reindex()

    for tag in ("alpha", "beta"):
        rows = await db.list_tasks(status="all", tag=tag)
        assert [r["id"] for r in rows] == [task_id], tag
        assert await db.count_tasks(status="all", tag=tag) == 1, tag


@pytest.mark.asyncio
async def test_reindex_preserves_tags_when_file_has_no_tags_line(db: Database, tmp_path):
    """An absent field is "no information", not "empty".

    Both full-content save paths (handlers/tasks.py, routes/tasks.py) preserve
    on absence; reindex matches them.
    """
    task_id = "2026-08-04-reindex-tags-absent"
    content = "# No tags line\n\nBody only.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, tags="a,b", title="Stale title")

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["tags"] == "a,b"
    _assert_row_was_processed(row, "No tags line")


@pytest.mark.asyncio
async def test_reindex_leaves_tags_empty_for_new_row_without_tags_line(db: Database, tmp_path):
    """The discriminator between "no information" and "explicitly empty":
    with no stored row there is nothing to preserve."""
    task_id = "2026-08-04-reindex-tags-absent-new"
    _write(tmp_path, task_id, "# No tags line\n\nBody only.\n")

    await TaskManager(tmp_path, db).reindex()

    assert (await db.get_task(task_id))["tags"] == ""


@pytest.mark.asyncio
async def test_reindex_clears_tags_for_present_but_empty_field(db: Database, tmp_path):
    """Presence, not truthiness.

    A whitespace-only value registers the key with an empty value, here at end
    of file. That is the shape where ``"tags" in fields`` and
    ``bool(fields.get("tags"))`` disagree, so it pins the presence test: the
    field is there and explicitly empty, so the stored value is cleared rather
    than preserved. The mid-file position is covered separately, since only that
    one distinguishes a line-bounded parse from a crossing one.
    """
    task_id = "2026-08-04-reindex-tags-empty-field"
    content = "# Empty field\n\nBody.\n\n**Tags:**   \n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, tags="a,b")

    from nerve.tasks.models import parse_task_frontmatter

    fields = parse_task_frontmatter(content)
    assert "tags" in fields and not fields["tags"], "fixture must be present-but-falsy"

    await TaskManager(tmp_path, db).reindex()

    assert (await db.get_task(task_id))["tags"] == ""


@pytest.mark.asyncio
async def test_reindex_clears_tags_for_empty_json_array(db: Database, tmp_path):
    """An explicit empty JSON array is present and truthy, but parses to no
    tags, so it is also an explicit clear."""
    task_id = "2026-08-04-reindex-tags-empty-json"
    content = "# Empty json\n\n**Tags:** []\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, tags="a,b")

    await TaskManager(tmp_path, db).reindex()

    assert (await db.get_task(task_id))["tags"] == ""


@pytest.mark.asyncio
async def test_reindex_is_idempotent_for_tags(db: Database, tmp_path):
    """A second reindex must not drift or accumulate."""
    task_id = "2026-08-04-reindex-tags-idempotent"
    content = "# Idempotent\n\n**Tags:** Alpha, beta\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content)

    manager = TaskManager(tmp_path, db)
    await manager.reindex()
    first = (await db.get_task(task_id))["tags"]
    await manager.reindex()

    assert (await db.get_task(task_id))["tags"] == first == "alpha,beta"


# --- D3: source vs source_url ---


@pytest.mark.asyncio
async def test_reindex_does_not_write_the_url_into_the_source_column(db: Database, tmp_path):
    """**Source:** on disk holds the URL (handlers/tasks.py writes source_url
    there), while ``source`` is a DB-only vocabulary column."""
    task_id = "2026-08-04-reindex-source-column"
    content = f"# Source column\n\n**Tags:** ci\n**Source:** {SOURCE_URL}\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, tags="ci", source="github",
                source_url=SOURCE_URL, title="Stale title")

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["source"] == "github"
    assert row["source_url"] == SOURCE_URL
    _assert_row_was_processed(row, "Source column")


@pytest.mark.asyncio
async def test_reindex_preserves_source_columns_when_file_has_no_source_line(
    db: Database, tmp_path,
):
    """Neither column may be nulled just because the file omits the line."""
    task_id = "2026-08-04-reindex-source-absent"
    content = "# No source line\n\n**Tags:** ci\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, tags="ci", source="github",
                source_url=SOURCE_URL, title="Stale title")

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["source"] == "github"
    assert row["source_url"] == SOURCE_URL
    _assert_row_was_processed(row, "No source line")


@pytest.mark.asyncio
async def test_reindex_takes_source_url_from_the_file_when_present(db: Database, tmp_path):
    """The file wins for ``source_url``, so the two candidates must differ.

    The two tests above seed the same URL they write into the file, so neither
    observes the file-wins half of ``fields.get("source") or
    stored.get("source_url")`` choosing anything: a mutant reducing it to
    ``stored.get("source_url")`` passes them both.
    """
    task_id = "2026-08-04-reindex-source-url-file-wins"
    content = f"# Source url from file\n\n**Tags:** ci\n**Source:** {SOURCE_URL}\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    assert STALE_SOURCE_URL != SOURCE_URL, "the candidates must be distinct"
    await _seed(db, task_id, rel, content, tags="ci", source="github",
                source_url=STALE_SOURCE_URL, title="Stale title")

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["source_url"] == SOURCE_URL
    assert row["source"] == "github"
    _assert_row_was_processed(row, "Source url from file")


# --- Blank frontmatter fields (line-bounded parse) ---


@pytest.mark.asyncio
async def test_reindex_clears_tags_for_blank_field_followed_by_body(db: Database, tmp_path):
    """The mid-file blank field, which is the shape production writes.

    ``task_update(tags="-onlytag")`` removes the last tag and rewrites the line
    as ``**Tags:**`` with nothing after it. A frontmatter value that could cross
    the newline would capture the next non-empty line, storing body text as the
    task's tags -- the opposite of the explicit clear this represents.
    """
    task_id = "2026-08-04-reindex-tags-blank-then-body"
    content = "# Blank then body\n\n**Tags:** \n\nBody text here.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, tags="a,b", title="Stale title")

    from nerve.tasks.models import parse_task_frontmatter

    fields = parse_task_frontmatter(content)
    assert "tags" in fields and not fields["tags"], "fixture must be present-but-falsy"

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["tags"] == ""
    _assert_row_was_processed(row, "Blank then body")


@pytest.mark.asyncio
async def test_reindex_clears_tags_for_blank_field_without_a_trailing_space(
    db: Database, tmp_path,
):
    """The blank field must clear whether or not the writer left a trailing space.

    A parse that is line-bounded but still demands a character leaves the key
    unregistered here, which is indistinguishable from an absent field, so the
    stored value would be preserved instead of cleared.
    """
    task_id = "2026-08-04-reindex-tags-blank-nospace"
    content = "# Blank no space\n\n**Tags:**\n\nBody text here.\n"
    rel = _write(tmp_path, task_id, content)
    assert "**Tags:**\n" in content, "fixture must have no trailing space"
    await _seed(db, task_id, rel, content, tags="a,b", title="Stale title")

    from nerve.tasks.models import parse_task_frontmatter

    fields = parse_task_frontmatter(content)
    assert "tags" in fields and not fields["tags"], "fixture must be present-but-falsy"

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["tags"] == ""
    _assert_row_was_processed(row, "Blank no space")


@pytest.mark.asyncio
async def test_reindex_clears_tags_for_blank_field_followed_by_another_metadata_line(
    db: Database, tmp_path,
):
    """A blank field must not consume the next metadata line either."""
    task_id = "2026-08-04-reindex-tags-blank-then-meta"
    content = "# Blank then meta\n\n**Tags:** \n**Deadline:** 2026-09-01\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, tags="a,b", deadline="2026-12-31",
                title="Stale title")

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["tags"] == ""
    assert row["deadline"] == "2026-09-01"
    _assert_row_was_processed(row, "Blank then meta")


@pytest.mark.asyncio
async def test_reindex_keeps_deadline_for_blank_field_followed_by_body(db: Database, tmp_path):
    """A blank ``**Deadline:**`` must not store body text as a date.

    ``deadline`` is truthiness-based, so a blank field keeps the stored row --
    but only because the parse yields ``''``. A crossing capture would store
    "Body text here." in a date column.
    """
    task_id = "2026-08-04-reindex-deadline-blank-then-body"
    content = "# Deadline blank\n\n**Deadline:** \n\nBody text here.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, deadline="2026-12-31", title="Stale title")

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["deadline"] == "2026-12-31"
    _assert_row_was_processed(row, "Deadline blank")


@pytest.mark.asyncio
async def test_reindex_keeps_deadline_for_blank_field_followed_by_another_metadata_line(
    db: Database, tmp_path,
):
    """The blank deadline must not consume the following metadata line."""
    task_id = "2026-08-04-reindex-deadline-blank-then-meta"
    content = "# Deadline blank meta\n\n**Deadline:** \n**Tags:** ci\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, deadline="2026-12-31", tags="old",
                title="Stale title")

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["deadline"] == "2026-12-31"
    assert row["tags"] == "ci"
    _assert_row_was_processed(row, "Deadline blank meta")


@pytest.mark.asyncio
async def test_reindex_keeps_source_url_for_blank_field_followed_by_body(db: Database, tmp_path):
    """A blank ``**Source:**`` must not store body text as the source URL."""
    task_id = "2026-08-04-reindex-source-blank-then-body"
    content = "# Source blank\n\n**Source:** \n\nBody text here.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, source="github", source_url=SOURCE_URL,
                title="Stale title")

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["source_url"] == SOURCE_URL
    assert row["source"] == "github"
    _assert_row_was_processed(row, "Source blank")


@pytest.mark.asyncio
async def test_reindex_keeps_source_url_for_blank_field_followed_by_another_metadata_line(
    db: Database, tmp_path,
):
    """The blank source must not consume the following metadata line."""
    task_id = "2026-08-04-reindex-source-blank-then-meta"
    content = "# Source blank meta\n\n**Source:** \n**Tags:** ci\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, source="github", source_url=SOURCE_URL,
                tags="old", title="Stale title")

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["source_url"] == SOURCE_URL
    assert row["tags"] == "ci"
    _assert_row_was_processed(row, "Source blank meta")


# --- Adjacent columns and controls ---


@pytest.mark.asyncio
async def test_reindex_preserves_deadline_when_file_has_no_deadline_line(db: Database, tmp_path):
    """Same omission class as tags: an absent **Deadline:** line must not null
    the stored deadline."""
    task_id = "2026-08-04-reindex-deadline-absent"
    content = "# No deadline line\n\n**Tags:** ci\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, tags="ci", deadline="2026-12-31",
                title="Stale title")

    assert await TaskManager(tmp_path, db).reindex() == 1

    row = await db.get_task(task_id)
    assert row["deadline"] == "2026-12-31"
    _assert_row_was_processed(row, "No deadline line")


@pytest.mark.asyncio
async def test_reindex_takes_deadline_from_the_file_when_present(db: Database, tmp_path):
    """Control: the file still wins for fields it actually carries."""
    task_id = "2026-08-04-reindex-deadline-present"
    content = "# Deadline present\n\n**Tags:** ci\n**Deadline:** 2026-09-01\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, tags="ci", deadline="2026-12-31")

    await TaskManager(tmp_path, db).reindex()

    assert (await db.get_task(task_id))["deadline"] == "2026-09-01"


@pytest.mark.asyncio
async def test_reindex_still_resyncs_title_and_keeps_created_at(db: Database, tmp_path):
    """Control: reindex keeps doing its job (title from the H1) and must not
    disturb created_at, which ON CONFLICT never assigns."""
    task_id = "2026-08-04-reindex-title-control"
    content = "# Fresh title from H1\n\n**Tags:** ci\n\nBody.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, tags="ci", title="Stale title")
    created_at = (await db.get_task(task_id))["created_at"]

    await TaskManager(tmp_path, db).reindex()

    row = await db.get_task(task_id)
    assert row["title"] == "Fresh title from H1"
    assert row["created_at"] == created_at


@pytest.mark.asyncio
async def test_reindex_repopulates_fts_tag_mirror(db: Database, tmp_path):
    """``fts_content`` is derived from the ``tags`` argument, so passing tags
    also restores tag-token full-text search.

    The token deliberately appears **only** in the stored tags column, never in
    the body, id or title: a token drawn from a **Tags:** line would still match
    through ``content`` even with the tags column empty, making the assertion
    vacuous.
    """
    task_id = "2026-08-04-reindex-fts-mirror"
    content = "# Fts mirror\n\nOrdinary body with no tags line.\n"
    rel = _write(tmp_path, task_id, content)
    await _seed(db, task_id, rel, content, tags="zqxtag")
    assert "zqxtag" not in content and "zqxtag" not in task_id

    await TaskManager(tmp_path, db).reindex()

    matches = await db.search_tasks(query="zqxtag", status="all")
    assert [m["id"] for m in matches] == [task_id]
