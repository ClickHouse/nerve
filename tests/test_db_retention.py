"""Tests for nerve.db.maintenance — opt-in retention (compaction + pruning).

Safety contract under test (see notes/repo-conventions/nerve/db-retention.md):
compaction only drops blocks/thinking from messages that are old AND already
past their session's memorize watermark AND in a non-starred, non-active
session; it never touches content, never deletes rows, and is idempotent.
Telemetry/snapshot pruning deletes only the targeted append-only tables.
"""

from datetime import datetime, timedelta, timezone

import pytest

from nerve.db import Database


def _days_ago(n: float) -> str:
    """A ``YYYY-MM-DD HH:MM:SS`` UTC timestamp ``n`` days in the past."""
    return (datetime.now(timezone.utc) - timedelta(days=n)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


async def _make_session(
    db: Database,
    sid: str,
    *,
    status: str = "idle",
    starred: int = 0,
    last_memorized_at: str | None = None,
) -> None:
    await db.create_session(sid, status=status)
    fields: dict = {"starred": starred}
    if last_memorized_at is not None:
        fields["last_memorized_at"] = last_memorized_at
    await db.update_session_fields(sid, fields)


async def _add_msg(
    db: Database,
    sid: str,
    *,
    created_at: str,
    content: str = "hello",
) -> int:
    return await db.add_message(
        sid,
        role="assistant",
        content=content,
        thinking="internal reasoning",
        blocks=[{"type": "text", "text": content}],
        created_at=created_at,
    )


async def _blocks_thinking(db: Database, msg_id: int) -> tuple:
    async with db.db.execute(
        "SELECT blocks, thinking, content FROM messages WHERE id = ?", (msg_id,)
    ) as cur:
        row = await cur.fetchone()
    return row["blocks"], row["thinking"], row["content"]


@pytest.mark.asyncio
class TestCompaction:
    async def test_compacts_old_memorized_nonstarred_inactive(self, db: Database):
        """The eligible case: old + memorized + not starred + not active."""
        await _make_session(db, "s1", last_memorized_at=_days_ago(1))
        mid = await _add_msg(db, "s1", created_at=_days_ago(100))

        report = await db.compact_messages(full_days=30)

        assert report["messages_compacted"] == 1
        assert report["bytes_reclaimed"] > 0
        blocks, thinking, content = await _blocks_thinking(db, mid)
        assert blocks is None
        assert thinking is None
        assert content == "hello"  # content is always kept

    async def test_recent_message_untouched(self, db: Database):
        """A message inside the full-retention window keeps its blocks."""
        await _make_session(db, "s1", last_memorized_at=_days_ago(1))
        mid = await _add_msg(db, "s1", created_at=_days_ago(5))

        report = await db.compact_messages(full_days=30)

        assert report["messages_compacted"] == 0
        blocks, thinking, _ = await _blocks_thinking(db, mid)
        assert blocks is not None
        assert thinking is not None

    async def test_starred_session_untouched(self, db: Database):
        await _make_session(db, "s1", starred=1, last_memorized_at=_days_ago(1))
        mid = await _add_msg(db, "s1", created_at=_days_ago(100))

        report = await db.compact_messages(full_days=30)

        assert report["messages_compacted"] == 0
        blocks, _, _ = await _blocks_thinking(db, mid)
        assert blocks is not None

    async def test_active_session_untouched(self, db: Database):
        await _make_session(
            db, "s1", status="active", last_memorized_at=_days_ago(1)
        )
        mid = await _add_msg(db, "s1", created_at=_days_ago(100))

        report = await db.compact_messages(full_days=30)

        assert report["messages_compacted"] == 0
        blocks, _, _ = await _blocks_thinking(db, mid)
        assert blocks is not None

    async def test_unmemorized_message_untouched(self, db: Database):
        """Watermark guard: a session never memorized (NULL watermark) is safe."""
        await _make_session(db, "s1", last_memorized_at=None)
        mid = await _add_msg(db, "s1", created_at=_days_ago(100))

        report = await db.compact_messages(full_days=30)

        assert report["messages_compacted"] == 0
        blocks, _, _ = await _blocks_thinking(db, mid)
        assert blocks is not None

    async def test_message_newer_than_watermark_untouched(self, db: Database):
        """A message after the watermark is not yet memorized; keep its blocks."""
        # Watermark older than the message: msg(50d) is newer than wm(120d).
        await _make_session(db, "s1", last_memorized_at=_days_ago(120))
        mid = await _add_msg(db, "s1", created_at=_days_ago(50))

        report = await db.compact_messages(full_days=30)

        assert report["messages_compacted"] == 0
        blocks, _, _ = await _blocks_thinking(db, mid)
        assert blocks is not None

    async def test_idempotent(self, db: Database):
        await _make_session(db, "s1", last_memorized_at=_days_ago(1))
        await _add_msg(db, "s1", created_at=_days_ago(100))

        first = await db.compact_messages(full_days=30)
        second = await db.compact_messages(full_days=30)

        assert first["messages_compacted"] == 1
        assert second["messages_compacted"] == 0  # nothing left to compact

    async def test_dry_run_reports_without_mutating(self, db: Database):
        await _make_session(db, "s1", last_memorized_at=_days_ago(1))
        mid = await _add_msg(db, "s1", created_at=_days_ago(100))

        report = await db.compact_messages(full_days=30, dry_run=True)

        assert report["messages_compacted"] == 1
        assert report["bytes_reclaimed"] > 0
        blocks, thinking, _ = await _blocks_thinking(db, mid)
        assert blocks is not None  # not mutated
        assert thinking is not None

    async def test_row_count_unchanged(self, db: Database):
        """Compaction nulls columns but never deletes message rows."""
        await _make_session(db, "s1", last_memorized_at=_days_ago(1))
        await _add_msg(db, "s1", created_at=_days_ago(100))
        await _add_msg(db, "s1", created_at=_days_ago(5))

        async with db.db.execute("SELECT COUNT(*) FROM messages") as cur:
            before = (await cur.fetchone())[0]
        await db.compact_messages(full_days=30)
        async with db.db.execute("SELECT COUNT(*) FROM messages") as cur:
            after = (await cur.fetchone())[0]

        assert before == after == 2


async def _seed_telemetry(db: Database, ts: str) -> None:
    """Insert one row at timestamp ``ts`` into each telemetry table."""
    await db.create_session("tel", status="idle")
    await db.db.execute(
        "INSERT INTO session_events (session_id, event_type, created_at) "
        "VALUES (?, ?, ?)",
        ("tel", "test", ts),
    )
    await db.db.execute(
        "INSERT INTO mcp_tool_usage (server_name, tool_name, created_at) "
        "VALUES (?, ?, ?)",
        ("srv", "tool", ts),
    )
    await db.db.execute(
        "INSERT INTO session_usage (session_id, created_at) VALUES (?, ?)",
        ("tel", ts),
    )
    await db.db.execute(
        "INSERT INTO memu_audit_log (action, target_type, timestamp) "
        "VALUES (?, ?, ?)",
        ("memorize", "session", ts),
    )
    await db.db.commit()


async def _count(db: Database, table: str) -> int:
    async with db.db.execute(f"SELECT COUNT(*) FROM {table}") as cur:
        return (await cur.fetchone())[0]


@pytest.mark.asyncio
class TestTelemetryPrune:
    async def test_deletes_old_keeps_new(self, db: Database):
        await _seed_telemetry(db, _days_ago(200))  # old
        await _seed_telemetry(db, _days_ago(1))     # new

        report = await db.prune_telemetry(days=90)

        assert report["telemetry_deleted"] == 4  # one old row per table
        for table in (
            "session_events",
            "mcp_tool_usage",
            "session_usage",
            "memu_audit_log",
        ):
            assert report["by_table"][table] == 1
            assert await _count(db, table) == 1  # the new row survives

    async def test_dry_run_mutates_nothing(self, db: Database):
        await _seed_telemetry(db, _days_ago(200))

        report = await db.prune_telemetry(days=90, dry_run=True)

        assert report["telemetry_deleted"] == 4
        assert await _count(db, "session_events") == 1  # still there

    async def test_preserves_core_tables(self, db: Database):
        await _make_session(db, "core", last_memorized_at=_days_ago(1))
        await _add_msg(db, "core", created_at=_days_ago(200))
        await _seed_telemetry(db, _days_ago(200))

        before = {
            t: await _count(db, t)
            for t in ("messages", "sessions", "tasks", "plans", "notifications")
        }
        await db.prune_telemetry(days=90)
        after = {
            t: await _count(db, t)
            for t in ("messages", "sessions", "tasks", "plans", "notifications")
        }

        assert before == after


@pytest.mark.asyncio
class TestFileSnapshotPrune:
    async def test_deletes_old_keeps_new(self, db: Database):
        await db.create_session("s1", status="idle")
        await db.db.execute(
            "INSERT INTO session_file_snapshots "
            "(session_id, file_path, original_content, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("s1", "/old.py", "x" * 100, _days_ago(200)),
        )
        await db.db.execute(
            "INSERT INTO session_file_snapshots "
            "(session_id, file_path, original_content, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("s1", "/new.py", "y" * 100, _days_ago(1)),
        )
        await db.db.commit()

        report = await db.prune_file_snapshots(days=90)

        assert report["snapshots_deleted"] == 1
        assert report["bytes_reclaimed"] > 0
        assert await _count(db, "session_file_snapshots") == 1


@pytest.mark.asyncio
class TestRunRetention:
    async def test_combined_report(self, db: Database):
        await _make_session(db, "s1", last_memorized_at=_days_ago(1))
        await _add_msg(db, "s1", created_at=_days_ago(100))
        await _seed_telemetry(db, _days_ago(200))

        report = await db.run_retention(
            retention_days=90, retention_full_days=30
        )

        assert report["dry_run"] is False
        assert report["messages_compacted"] == 1
        assert report["message_bytes_reclaimed"] > 0
        assert report["telemetry_deleted"] == 4
        assert "by_table" in report
        assert report["snapshots_deleted"] == 0

    async def test_dry_run_mutates_nothing(self, db: Database):
        await _make_session(db, "s1", last_memorized_at=_days_ago(1))
        mid = await _add_msg(db, "s1", created_at=_days_ago(100))
        await _seed_telemetry(db, _days_ago(200))

        report = await db.run_retention(
            retention_days=90, retention_full_days=30, dry_run=True
        )

        assert report["dry_run"] is True
        assert report["messages_compacted"] == 1
        assert report["telemetry_deleted"] == 4
        # nothing actually changed
        blocks, _, _ = await _blocks_thinking(db, mid)
        assert blocks is not None
        assert await _count(db, "session_events") == 1
