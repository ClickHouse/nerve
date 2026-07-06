"""Re-surfacing of updated mutable-source messages to consumer cursors.

Mutable sources (notably GitHub) reuse one stable notification id per thread,
so a new comment arrives as an *update* to an existing ``source_messages`` row.
Consumers poll ``rowid > cursor_seq`` and advance their cursor to the max rowid
they have seen, so an updated row must land at a strictly-higher rowid to be
re-delivered.

Regression guard for the SQLite rowid-reuse bug: ``source_messages`` has
``PRIMARY KEY (source, id)`` and no ``AUTOINCREMENT``, so a naive
delete-then-reinsert lands at ``MAX(rowid)+1``, which *reuses* the old rowid
when the replaced row was itself the table max, leaving the update at or below
a cursor already parked there (silently never re-delivered).
"""

from __future__ import annotations

import pytest

from nerve.sources.models import SourceRecord


def _rec(rid: str, content: str, reason: str = "mention") -> SourceRecord:
    """A GitHub-ish notification record with a stable per-thread id."""
    return SourceRecord(
        id=rid,
        source="github",
        record_type="github_notification",
        summary=f"[owner/repo] thread {rid} ({reason})",
        content=content,
        timestamp="2026-01-01T00:00:00Z",
        metadata={"reason": reason, "repo_name": "owner/repo"},
    )


async def _rowid_of(db, source: str, rid: str) -> int:
    async with db.db.execute(
        "SELECT rowid FROM source_messages WHERE source = ? AND id = ?",
        (source, rid),
    ) as cur:
        return (await cur.fetchone())[0]


@pytest.mark.asyncio
async def test_update_to_max_rowid_row_resurfaces_to_consumer(db):
    """The bug's exact shape: the updated thread is the current MAX rowid.

    A consumer reads up to that rowid, then the thread gets a new comment.
    The re-inserted row must appear above the consumer's cursor.
    """
    # Establish the consumer cursor while the inbox is empty (it initializes to
    # the current max rowid = 0), mirroring an inbox cron that has run before.
    seq = await db.get_consumer_cursor("inbox", "github")
    assert seq == 0

    # Two earlier threads, then the thread under test; it is now the max rowid.
    await db.insert_source_messages([_rec("t1", "a")], source="github")
    await db.insert_source_messages([_rec("t2", "b")], source="github")
    await db.insert_source_messages([_rec("hot", "comment-1")], source="github")

    # Consumer drains the inbox and parks its cursor at the latest rowid (the
    # "hot" thread). This mirrors an inbox cron run that processed the thread.
    rows = await db.read_source_messages_by_rowid("github", after_seq=seq, limit=50)
    assert {r["id"] for r in rows} == {"t1", "t2", "hot"}
    max_seq = max(r["rowid"] for r in rows)
    await db.set_consumer_cursor("inbox", "github", max_seq)
    parked = await db.get_consumer_cursor("inbox", "github")

    rowid_before = await _rowid_of(db, "github", "hot")
    assert rowid_before == parked  # the updated thread IS the parked max rowid

    # New comment lands on the same thread -> content changes -> re-insert.
    n = await db.insert_source_messages([_rec("hot", "comment-2")], source="github")
    assert n == 1  # the changed row was (re-)inserted

    rowid_after = await _rowid_of(db, "github", "hot")
    assert rowid_after > parked, (
        f"re-inserted row rowid {rowid_after} must exceed parked cursor {parked} "
        "(rowid-reuse regression)"
    )

    # The consumer's next poll re-delivers exactly the updated thread.
    fresh = await db.read_source_messages_by_rowid("github", after_seq=parked, limit=50)
    assert [r["id"] for r in fresh] == ["hot"]
    assert fresh[0]["content"] == "comment-2"


@pytest.mark.asyncio
async def test_update_to_non_max_row_also_resurfaces(db):
    """The general path: updating a non-max row still re-surfaces above the cursor."""
    base = await db.get_consumer_cursor("inbox", "github")  # 0 on empty inbox
    await db.insert_source_messages([_rec("old", "x")], source="github")
    await db.insert_source_messages([_rec("new", "y")], source="github")

    # Consumer parks at the latest rowid ("new").
    rows = await db.read_source_messages_by_rowid("github", after_seq=base, limit=50)
    parked = max(r["rowid"] for r in rows)
    await db.set_consumer_cursor("inbox", "github", parked)

    # The OLDER thread (below the cursor) gets new activity.
    await db.insert_source_messages([_rec("old", "x-updated")], source="github")

    fresh = await db.read_source_messages_by_rowid("github", after_seq=parked, limit=50)
    assert [r["id"] for r in fresh] == ["old"]
    assert fresh[0]["content"] == "x-updated"


@pytest.mark.asyncio
async def test_unchanged_record_does_not_resurface(db):
    """Idempotency preserved: re-ingesting an identical record is a no-op."""
    await db.insert_source_messages([_rec("t", "same")], source="github")
    rowid_before = await _rowid_of(db, "github", "t")

    n = await db.insert_source_messages([_rec("t", "same")], source="github")
    assert n == 0  # nothing changed -> skipped silently

    rowid_after = await _rowid_of(db, "github", "t")
    assert rowid_after == rowid_before  # no churn, no spurious re-surface


@pytest.mark.asyncio
async def test_repeated_updates_keep_climbing(db):
    """Several successive comments on the same thread each re-surface in turn."""
    await db.insert_source_messages([_rec("hot", "c1")], source="github")
    last = 0
    for i in range(2, 6):
        await db.insert_source_messages([_rec("hot", f"c{i}")], source="github")
        rid = await _rowid_of(db, "github", "hot")
        assert rid > last, f"rowid must strictly increase on each update (got {rid} <= {last})"
        last = rid
        # Exactly one live row for the thread (re-insert replaces, not duplicates).
        async with db.db.execute(
            "SELECT COUNT(*) FROM source_messages WHERE source='github' AND id='hot'"
        ) as cur:
            assert (await cur.fetchone())[0] == 1
