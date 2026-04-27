"""Tests for nerve.db — Schema V5 migration, session operations, lifecycle, FTS5 search."""

import json

import pytest
import pytest_asyncio

from nerve.db import Database


@pytest.mark.asyncio
class TestSchemaMigration:
    """Test that migrations run cleanly on a fresh DB."""

    async def test_schema_version_is_current(self, db: Database):
        from nerve.db import SCHEMA_VERSION
        async with db.db.execute("SELECT MAX(version) FROM schema_version") as cur:
            row = await cur.fetchone()
        assert row[0] == SCHEMA_VERSION

    async def test_source_run_log_table_exists(self, db: Database):
        async with db.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='source_run_log'"
        ) as cur:
            row = await cur.fetchone()
        assert row is not None

    async def test_sessions_table_has_v3_columns(self, db: Database):
        """All V3 columns should exist on the sessions table."""
        async with db.db.execute("PRAGMA table_info(sessions)") as cur:
            columns = {row[1] async for row in cur}
        expected = {
            "id", "title", "created_at", "updated_at", "source", "metadata",
            "status", "sdk_session_id", "parent_session_id",
            "forked_from_message", "connected_at", "last_activity_at",
            "archived_at", "message_count", "total_cost_usd",
        }
        assert expected.issubset(columns), f"Missing columns: {expected - columns}"

    async def test_channel_sessions_table_exists(self, db: Database):
        async with db.db.execute("PRAGMA table_info(channel_sessions)") as cur:
            columns = {row[1] async for row in cur}
        assert "channel_key" in columns
        assert "session_id" in columns

    async def test_session_events_table_exists(self, db: Database):
        async with db.db.execute("PRAGMA table_info(session_events)") as cur:
            columns = {row[1] async for row in cur}
        assert "session_id" in columns
        assert "event_type" in columns
        assert "details" in columns


@pytest.mark.asyncio
class TestSessionCRUD:
    """Test session create/read/update/delete operations."""

    async def test_create_session(self, db: Database):
        session = await db.create_session("test-1", title="Test", source="web")
        assert session["id"] == "test-1"
        assert session["title"] == "Test"
        assert session["status"] == "created"

    async def test_create_session_defaults(self, db: Database):
        session = await db.create_session("test-defaults")
        assert session["title"] == "test-defaults"  # title defaults to ID
        assert session["status"] == "created"

    async def test_create_session_with_parent(self, db: Database):
        await db.create_session("parent-1")
        session = await db.create_session(
            "fork-1", parent_session_id="parent-1",
            forked_from_message="msg-xyz",
        )
        assert session["parent_session_id"] == "parent-1"

    async def test_get_session(self, db: Database):
        await db.create_session("get-test", title="Get Test")
        session = await db.get_session("get-test")
        assert session is not None
        assert session["title"] == "Get Test"
        assert session["status"] == "created"

    async def test_get_session_not_found(self, db: Database):
        session = await db.get_session("nonexistent")
        assert session is None

    async def test_create_session_idempotent(self, db: Database):
        """INSERT OR IGNORE should not overwrite existing session."""
        await db.create_session("idem-1", title="First")
        await db.create_session("idem-1", title="Second")
        session = await db.get_session("idem-1")
        assert session["title"] == "First"

    async def test_delete_session(self, db: Database):
        await db.create_session("del-1")
        await db.add_message("del-1", "user", "hello")
        await db.log_session_event("del-1", "created", {})
        await db.delete_session("del-1")
        assert await db.get_session("del-1") is None

    async def test_delete_session_cleans_up_related(self, db: Database):
        """Delete should remove messages, events, and channel mappings."""
        await db.create_session("del-related")
        await db.add_message("del-related", "user", "test")
        await db.log_session_event("del-related", "created", {})
        await db.set_channel_session("tg:123", "del-related")

        await db.delete_session("del-related")

        msgs = await db.get_messages("del-related")
        assert len(msgs) == 0
        events = await db.get_session_events("del-related")
        assert len(events) == 0
        ch = await db.get_channel_session("tg:123")
        assert ch is None


@pytest.mark.asyncio
class TestUpdateSessionFields:
    """Test the partial field update method."""

    async def test_update_single_field(self, db: Database):
        await db.create_session("upd-1")
        await db.update_session_fields("upd-1", {"status": "active"})
        session = await db.get_session("upd-1")
        assert session["status"] == "active"

    async def test_update_multiple_fields(self, db: Database):
        await db.create_session("upd-2")
        await db.update_session_fields("upd-2", {
            "status": "active",
            "sdk_session_id": "sdk-abc",
            "connected_at": "2024-01-01T00:00:00",
        })
        session = await db.get_session("upd-2")
        assert session["status"] == "active"
        assert session["sdk_session_id"] == "sdk-abc"
        assert session["connected_at"] == "2024-01-01T00:00:00"

    async def test_update_ignores_unknown_fields(self, db: Database):
        await db.create_session("upd-3")
        # Should not raise, just ignore unknown fields
        await db.update_session_fields("upd-3", {
            "status": "active",
            "bogus_field": "should_be_ignored",
        })
        session = await db.get_session("upd-3")
        assert session["status"] == "active"

    async def test_update_sets_none(self, db: Database):
        """Setting a field to None should clear it."""
        await db.create_session("upd-4")
        await db.update_session_fields("upd-4", {"sdk_session_id": "abc"})
        await db.update_session_fields("upd-4", {"sdk_session_id": None})
        session = await db.get_session("upd-4")
        assert session["sdk_session_id"] is None

    async def test_update_touches_updated_at(self, db: Database):
        await db.create_session("upd-5")
        before = (await db.get_session("upd-5"))["updated_at"]
        import asyncio
        await asyncio.sleep(0.01)
        await db.update_session_fields("upd-5", {"status": "active"})
        after = (await db.get_session("upd-5"))["updated_at"]
        assert after >= before


@pytest.mark.asyncio
class TestListSessions:
    """Test listing with archived filter."""

    async def test_list_excludes_archived(self, db: Database):
        await db.create_session("list-active", status="active")
        await db.create_session("list-archived", status="archived")
        sessions = await db.list_sessions(include_archived=False)
        ids = [s["id"] for s in sessions]
        assert "list-active" in ids
        assert "list-archived" not in ids

    async def test_list_includes_archived(self, db: Database):
        await db.create_session("list-a2", status="active")
        await db.create_session("list-arch2", status="archived")
        sessions = await db.list_sessions(include_archived=True)
        ids = [s["id"] for s in sessions]
        assert "list-a2" in ids
        assert "list-arch2" in ids


@pytest.mark.asyncio
class TestSessionEvents:
    """Test lifecycle event logging."""

    async def test_log_and_get_events(self, db: Database):
        await db.create_session("ev-1")
        await db.log_session_event("ev-1", "created", {"source": "web"})
        await db.log_session_event("ev-1", "started", {"sdk": "abc"})
        events = await db.get_session_events("ev-1")
        assert len(events) == 2
        # Newest first
        assert events[0]["event_type"] == "started"
        assert events[1]["event_type"] == "created"
        # Details are parsed from JSON
        assert events[0]["details"]["sdk"] == "abc"


@pytest.mark.asyncio
class TestChannelSessions:
    """Test persistent channel-to-session mapping."""

    async def test_set_and_get(self, db: Database):
        await db.create_session("ch-sess-1")
        await db.set_channel_session("telegram:123", "ch-sess-1")
        row = await db.get_channel_session("telegram:123")
        assert row["session_id"] == "ch-sess-1"

    async def test_get_nonexistent(self, db: Database):
        row = await db.get_channel_session("nonexistent:99")
        assert row is None

    async def test_overwrite(self, db: Database):
        await db.create_session("ch-a")
        await db.create_session("ch-b")
        await db.set_channel_session("tg:1", "ch-a")
        await db.set_channel_session("tg:1", "ch-b")
        row = await db.get_channel_session("tg:1")
        assert row["session_id"] == "ch-b"


@pytest.mark.asyncio
class TestMessages:
    """Test message operations and counter."""

    async def test_add_message_increments_count(self, db: Database):
        await db.create_session("msg-1")
        await db.add_message("msg-1", "user", "hello")
        await db.add_message("msg-1", "assistant", "hi")
        session = await db.get_session("msg-1")
        assert session["message_count"] == 2

    async def test_get_messages_ordered(self, db: Database):
        await db.create_session("msg-2")
        await db.add_message("msg-2", "user", "first")
        await db.add_message("msg-2", "assistant", "second")
        msgs = await db.get_messages("msg-2")
        assert len(msgs) == 2
        assert msgs[0]["content"] == "first"
        assert msgs[1]["content"] == "second"


@pytest.mark.asyncio
class TestCleanupQueries:
    """Test stale session detection and cleanup queries."""

    async def test_get_stale_sessions(self, db: Database):
        await db.create_session("stale-1", status="idle")
        await db.update_session_fields("stale-1", {
            "status": "idle",
        })
        # Force old updated_at
        await db.db.execute(
            "UPDATE sessions SET updated_at = '2020-01-01T00:00:00' WHERE id = 'stale-1'"
        )
        await db.db.commit()

        stale = await db.get_stale_sessions("2024-01-01T00:00:00")
        ids = [s["id"] for s in stale]
        assert "stale-1" in ids

    async def test_get_stale_excludes_active(self, db: Database):
        await db.create_session("active-not-stale", status="active")
        await db.db.execute(
            "UPDATE sessions SET updated_at = '2020-01-01T00:00:00' WHERE id = 'active-not-stale'"
        )
        await db.db.commit()

        stale = await db.get_stale_sessions("2024-01-01T00:00:00")
        ids = [s["id"] for s in stale]
        assert "active-not-stale" not in ids

    async def test_get_stale_excludes_by_id(self, db: Database):
        await db.create_session("stale-excl", status="idle")
        await db.db.execute(
            "UPDATE sessions SET updated_at = '2020-01-01T00:00:00' WHERE id = 'stale-excl'"
        )
        await db.db.commit()

        stale = await db.get_stale_sessions(
            "2024-01-01T00:00:00", exclude_ids=["stale-excl"],
        )
        ids = [s["id"] for s in stale]
        assert "stale-excl" not in ids

    async def test_count_active_sessions(self, db: Database):
        await db.create_session("count-1", status="active")
        await db.create_session("count-2", status="idle")
        await db.create_session("count-3", status="archived")
        count = await db.count_active_sessions()
        # count-1 and count-2 are not archived
        assert count >= 2

    async def test_get_sessions_by_status(self, db: Database):
        await db.create_session("stat-1", status="active")
        await db.create_session("stat-2", status="error")
        await db.create_session("stat-3", status="idle")
        active = await db.get_sessions_by_status(["active"])
        ids = [s["id"] for s in active]
        assert "stat-1" in ids
        assert "stat-2" not in ids


@pytest.mark.asyncio
class TestMemorizationQuery:
    """Test get_sessions_needing_memorization."""

    async def test_never_memorized_session_returned(self, db: Database):
        await db.create_session("memo-never", status="active")
        await db.add_message("memo-never", "user", "hello")
        sessions = await db.get_sessions_needing_memorization()
        ids = [s["id"] for s in sessions]
        assert "memo-never" in ids

    async def test_fully_memorized_session_excluded(self, db: Database):
        await db.create_session("memo-done", status="active")
        await db.add_message("memo-done", "user", "hello")
        # Set watermark to the future so all messages are "memorized"
        await db.update_session_fields("memo-done", {
            "last_memorized_at": "2099-01-01T00:00:00",
        })
        sessions = await db.get_sessions_needing_memorization()
        ids = [s["id"] for s in sessions]
        assert "memo-done" not in ids

    async def test_partially_memorized_session_returned(self, db: Database):
        await db.create_session("memo-partial", status="active")
        await db.add_message("memo-partial", "user", "old message")
        # Set watermark to past, then add a newer message
        await db.update_session_fields("memo-partial", {
            "last_memorized_at": "2020-01-01T00:00:00",
        })
        await db.add_message("memo-partial", "user", "new message")
        sessions = await db.get_sessions_needing_memorization()
        ids = [s["id"] for s in sessions]
        assert "memo-partial" in ids

    async def test_archived_session_excluded(self, db: Database):
        await db.create_session("memo-archived", status="archived")
        await db.add_message("memo-archived", "user", "hello")
        sessions = await db.get_sessions_needing_memorization()
        ids = [s["id"] for s in sessions]
        assert "memo-archived" not in ids

    async def test_empty_session_excluded(self, db: Database):
        await db.create_session("memo-empty", status="active")
        sessions = await db.get_sessions_needing_memorization()
        ids = [s["id"] for s in sessions]
        assert "memo-empty" not in ids


@pytest.mark.asyncio
class TestMetadataBackwardCompat:
    """Test that update_session_metadata still syncs to dedicated columns."""

    async def test_metadata_syncs_sdk_session_id(self, db: Database):
        await db.create_session("meta-sync")
        await db.update_session_metadata("meta-sync", {
            "sdk_session_id": "sdk-xyz",
            "connected_at": "2024-06-01T12:00:00",
        })
        session = await db.get_session("meta-sync")
        assert session["sdk_session_id"] == "sdk-xyz"
        assert session["connected_at"] == "2024-06-01T12:00:00"


@pytest.mark.asyncio
class TestTaskSearch:
    """Test FTS5 full-text search for tasks."""

    async def _create_task(self, db: Database, task_id: str, title: str, content: str = "", status: str = "pending"):
        await db.upsert_task(
            task_id=task_id, file_path=f"memory/tasks/active/{task_id}.md",
            title=title, status=status, content=content,
        )

    async def test_fts_table_exists(self, db: Database):
        async with db.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='tasks_fts'"
        ) as cur:
            row = await cur.fetchone()
        assert row is not None

    async def test_search_single_word(self, db: Database):
        await self._create_task(db, "t1", "Fix billing issue")
        results = await db.search_tasks("billing")
        assert len(results) == 1
        assert results[0]["id"] == "t1"

    async def test_search_multi_word_tokenized(self, db: Database):
        """Multi-word query should match tasks containing all words, not the exact phrase."""
        await self._create_task(db, "t1", "Fix database connection timeout on staging server")
        # "database" and "connection" are both in the title — should match
        results = await db.search_tasks("database connection")
        assert len(results) == 1
        assert results[0]["id"] == "t1"

    async def test_search_matches_content_not_just_title(self, db: Database):
        """Words in the body content should be searchable, not just the title."""
        await self._create_task(
            db, "t1",
            title="Fix database connection timeout on staging",
            content="Connection pool exhausted after 100 concurrent requests. Increase pool size.",
        )
        # "pool" is NOT in the title, but IS in the content
        results = await db.search_tasks("pool")
        assert len(results) == 1
        assert results[0]["id"] == "t1"

    async def test_search_cross_title_and_content(self, db: Database):
        """Query with words split across title and content should match."""
        await self._create_task(
            db, "t1",
            title="Fix database connection timeout on staging",
            content="Connection pool is still exhausted intermittently.",
        )
        # "database" is in title, "pool" is in content
        results = await db.search_tasks("database pool")
        assert len(results) == 1

    async def test_search_status_filter(self, db: Database):
        await self._create_task(db, "t1", "Fix billing", status="pending")
        await self._create_task(db, "t2", "Old billing task", status="done")
        # Default: exclude done
        results = await db.search_tasks("billing")
        assert len(results) == 1
        assert results[0]["id"] == "t1"
        # all: include done
        results = await db.search_tasks("billing", status="all")
        assert len(results) == 2

    async def test_search_no_results(self, db: Database):
        await self._create_task(db, "t1", "Fix billing issue")
        results = await db.search_tasks("xyznonexistent")
        assert len(results) == 0

    async def test_fts_updated_on_upsert(self, db: Database):
        await self._create_task(db, "t1", "Old title", content="old content")
        results = await db.search_tasks("old")
        assert len(results) == 1
        # Re-upsert with new title and content
        await self._create_task(db, "t1", "New title", content="new content")
        results = await db.search_tasks("old")
        assert len(results) == 0
        results = await db.search_tasks("new")
        assert len(results) == 1

    async def test_rebuild_fts(self, db: Database):
        await self._create_task(db, "t1", "Some task", content="secret keyword xyzzy")
        # Content-only word is findable via FTS
        results = await db.search_tasks("xyzzy")
        assert len(results) == 1
        # Rebuild clears the FTS index
        await db.rebuild_fts()
        # Content-only word no longer found (not in title or slug)
        results = await db.search_tasks("xyzzy")
        assert len(results) == 0
        # But title LIKE fallback still works
        results = await db.search_tasks("task")
        assert len(results) == 1

    async def test_build_fts_query_sanitizes_special_chars(self, db: Database):
        """FTS5 special characters should be stripped, not cause errors."""
        await self._create_task(db, "t1", "Test task with special chars")
        # These should not raise
        results = await db.search_tasks('"test"')
        assert len(results) == 1
        results = await db.search_tasks("test*")
        assert len(results) == 1
        results = await db.search_tasks("test (special)")
        assert len(results) == 1

    async def test_empty_query_returns_empty(self, db: Database):
        await self._create_task(db, "t1", "Some task")
        results = await db.search_tasks("")
        assert len(results) == 0
        results = await db.search_tasks("   ")
        assert len(results) == 0

    async def test_search_prefix_matching(self, db: Database):
        """Partial word should match via FTS5 prefix syntax."""
        await self._create_task(db, "t1", "Distribution documentation update")
        results = await db.search_tasks("distrib")
        assert len(results) == 1
        assert results[0]["id"] == "t1"

    async def test_search_by_exact_task_id(self, db: Database):
        """Exact task ID should be found."""
        await self._create_task(db, "2026-03-10-distribution-docs", "Distribution documentation")
        results = await db.search_tasks("2026-03-10-distribution-docs")
        assert len(results) == 1
        assert results[0]["id"] == "2026-03-10-distribution-docs"

    async def test_search_by_slug_substring(self, db: Database):
        """Partial slug should find the task."""
        await self._create_task(db, "2026-03-10-cache-invalidation", "Redis cache TTL expiry fix")
        results = await db.search_tasks("cache-invalidation")
        assert len(results) == 1
        assert results[0]["id"] == "2026-03-10-cache-invalidation"

    async def test_search_slug_words_via_fts(self, db: Database):
        """Words from the slug should be searchable via FTS."""
        await self._create_task(db, "2026-04-01-flaky-tests", "CI pipeline reliability issue")
        # "flaky" is only in the slug, not the title
        results = await db.search_tasks("flaky")
        assert len(results) == 1

    async def test_search_deduplicates_across_strategies(self, db: Database):
        """A task matching multiple strategies should appear only once."""
        await self._create_task(db, "billing-fix", "Fix billing issue", content="billing problem")
        results = await db.search_tasks("billing")
        assert len(results) == 1

    async def test_search_relevance_ordering(self, db: Database):
        """FTS matches should rank higher than LIKE-only matches."""
        # Task with exact FTS match on title
        await self._create_task(db, "t1", "Optimize database queries")
        # Task where "optim" only matches via slug LIKE
        await self._create_task(db, "2026-01-01-optimize-cache", "Cache layer improvements")
        results = await db.search_tasks("optimize", status="all")
        assert len(results) == 2
        # FTS match (t1 has "Optimize" in title) should come first
        assert results[0]["id"] == "t1"

    async def test_search_by_tag(self, db: Database):
        """Tags should be searchable via FTS."""
        await db.upsert_task(
            task_id="tagged-task", file_path="memory/tasks/active/tagged-task.md",
            title="Some CI issue", status="pending", tags="p0,fuzzer,trunk-bug",
            content="test content",
        )
        results = await db.search_tasks("fuzzer")
        assert len(results) == 1
        assert results[0]["id"] == "tagged-task"

    async def test_list_tasks_tag_filter(self, db: Database):
        """Tag filter should match exact tags in comma-separated list."""
        await db.upsert_task(
            task_id="t-p0", file_path="f1.md", title="P0 task",
            status="pending", tags="p0,ci",
        )
        await db.upsert_task(
            task_id="t-p2", file_path="f2.md", title="P2 task",
            status="pending", tags="p2,ci",
        )
        results = await db.list_tasks(tag="p0")
        assert len(results) == 1
        assert results[0]["id"] == "t-p0"

        # Both have "ci" tag
        results = await db.list_tasks(tag="ci")
        assert len(results) == 2


class TestTagParsing:
    """Test tag string parsing handles various agent input formats."""

    def test_normal_csv(self):
        from nerve.tasks.models import parse_tags_string
        assert parse_tags_string("ci,fuzzer,p0") == ["ci", "fuzzer", "p0"]

    def test_json_array(self):
        from nerve.tasks.models import parse_tags_string
        assert parse_tags_string('["ci","fuzzer","p0"]') == ["ci", "fuzzer", "p0"]

    def test_empty_json_array(self):
        from nerve.tasks.models import parse_tags_string
        assert parse_tags_string("[]") == []

    def test_malformed_json_fragments(self):
        from nerve.tasks.models import parse_tags_string
        result = parse_tags_string('"fuzzer","p0","trunk-bug"],["ci"')
        assert "ci" in result
        assert "fuzzer" in result
        assert "p0" in result
        assert "trunk-bug" in result
        # No brackets or quotes in any tag
        for tag in result:
            assert "[" not in tag and "]" not in tag and '"' not in tag

    def test_quoted_csv(self):
        from nerve.tasks.models import parse_tags_string
        assert parse_tags_string('"ci","p0"') == ["ci", "p0"]

    def test_empty_string(self):
        from nerve.tasks.models import parse_tags_string
        assert parse_tags_string("") == []

    def test_tags_to_string_accepts_string(self):
        from nerve.tasks.models import tags_to_string
        assert tags_to_string('["ci","p0"]') == "ci,p0"

    def test_tags_to_string_accepts_list(self):
        from nerve.tasks.models import tags_to_string
        assert tags_to_string(["P0", "ci", "Fuzzer"]) == "ci,fuzzer,p0"

    def test_roundtrip(self):
        from nerve.tasks.models import parse_tags_string, tags_to_string
        original = "ci,fuzzer,p0,trunk-bug"
        assert tags_to_string(parse_tags_string(original)) == original


# --- Diagnostics helpers ---

@pytest.mark.asyncio
class TestDiagnosticsHelpers:
    """Test the count + retention helpers introduced for the diagnostics
    endpoint. These exist to keep /api/diagnostics off full-table scans."""

    async def test_count_sessions(self, db: Database):
        await db.create_session("d-1", title="One", source="web")
        await db.create_session("d-2", title="Two", source="web")
        await db.create_session("d-3", title="Three", source="web")
        # Archive d-3 directly via the status field.
        await db.update_session_fields("d-3", {"status": "archived"})

        assert await db.count_sessions() == 2
        assert await db.count_sessions(include_archived=True) == 3

    async def test_cleanup_old_cron_logs(self, db: Database):
        # Three logs across different ages.
        log_old = await db.log_cron_start("job-a")
        log_mid = await db.log_cron_start("job-b")
        log_new = await db.log_cron_start("job-c")

        # Backdate two of them via direct UPDATE — the helper isn't going
        # to time-travel for us.
        await db.db.execute(
            "UPDATE cron_logs SET started_at = datetime('now', '-30 days') WHERE id = ?",
            (log_old,),
        )
        await db.db.execute(
            "UPDATE cron_logs SET started_at = datetime('now', '-10 days') WHERE id = ?",
            (log_mid,),
        )
        await db.db.commit()

        deleted = await db.cleanup_old_cron_logs(days=14)
        assert deleted == 1  # only the 30-day-old one

        async with db.db.execute(
            "SELECT id FROM cron_logs ORDER BY id"
        ) as cur:
            remaining = {row[0] async for row in cur}
        assert remaining == {log_mid, log_new}

    async def test_cleanup_old_cron_logs_noop_when_empty(self, db: Database):
        # Nothing to delete is not an error — must return 0.
        deleted = await db.cleanup_old_cron_logs(days=14)
        assert deleted == 0


# --- Schema state after V26 ---

@pytest.mark.asyncio
class TestToolCallsColumnDropped:
    """V26 dropped the redundant ``messages.tool_calls`` column. After the
    fixture (which runs all migrations) the column should be gone and
    ``add_message`` must work without it."""

    async def test_tool_calls_column_is_gone(self, db: Database):
        async with db.db.execute("PRAGMA table_info(messages)") as cur:
            cols = {row[1] async for row in cur}
        assert "tool_calls" not in cols
        assert "blocks" in cols  # ensure we didn't accidentally drop the wrong one

    async def test_add_message_no_longer_accepts_tool_calls_kwarg(self, db: Database):
        await db.create_session("s-1", title="T", source="web")
        # Should succeed without tool_calls.
        msg_id = await db.add_message(
            "s-1", "assistant", "ok",
            blocks=[{"type": "text", "content": "ok"}],
        )
        assert msg_id is not None
        # Tool_calls is no longer a valid parameter.
        import inspect
        sig = inspect.signature(db.add_message)
        assert "tool_calls" not in sig.parameters

    async def test_get_messages_returns_blocks_not_tool_calls(self, db: Database):
        await db.create_session("s-2", title="T", source="web")
        await db.add_message(
            "s-2", "assistant", "hi",
            blocks=[
                {"type": "thinking", "content": "thinking..."},
                {"type": "tool_call", "tool": "Bash", "input": {}, "tool_use_id": "x"},
                {"type": "text", "content": "done"},
            ],
        )
        msgs = await db.get_messages("s-2")
        assert len(msgs) == 1
        assert "tool_calls" not in msgs[0]
        assert isinstance(msgs[0]["blocks"], list)
        assert len(msgs[0]["blocks"]) == 3


# --- V26 migration backfill ---

@pytest.mark.asyncio
async def test_v26_backfills_blocks_from_legacy_tool_calls(tmp_path):
    """Reconstruct a pre-V26 schema (with tool_calls, no blocks) and
    verify the migration both populates blocks and drops the column."""
    import json
    import aiosqlite

    db_path = tmp_path / "legacy.db"
    async with aiosqlite.connect(db_path) as db:
        # Minimal schema mirroring v001 + the relevant subset of v003+
        await db.executescript("""
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT,
                thinking TEXT,
                tool_calls JSON,
                blocks JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                channel TEXT
            );
        """)

        # Legacy row: only tool_calls populated.
        legacy_tc = [
            {"tool": "Read", "input": {"file_path": "/foo"},
             "tool_use_id": "tu_1", "result": "contents", "is_error": False}
        ]
        await db.execute(
            "INSERT INTO messages (session_id, role, content, thinking, tool_calls) "
            "VALUES (?, ?, ?, ?, ?)",
            ("s-legacy", "assistant", "final text", "thinking text",
             json.dumps(legacy_tc)),
        )

        # Already-blocked row that we must not touch.
        already = [{"type": "text", "content": "already migrated"}]
        await db.execute(
            "INSERT INTO messages (session_id, role, content, blocks) VALUES (?, ?, ?, ?)",
            ("s-keep", "assistant", "x", json.dumps(already)),
        )
        await db.commit()

        # Run the migration directly.
        from nerve.db.migrations.v026_drop_legacy_tool_calls import up
        await up(db)

        # tool_calls column should be gone now.
        async with db.execute("PRAGMA table_info(messages)") as cur:
            cols = {row[1] async for row in cur}
        assert "tool_calls" not in cols
        assert "blocks" in cols

        # Legacy row's blocks were reconstructed in the documented order.
        async with db.execute(
            "SELECT blocks FROM messages WHERE session_id = 's-legacy'"
        ) as cur:
            (raw,) = await cur.fetchone()
        blocks = json.loads(raw)
        assert [b["type"] for b in blocks] == ["thinking", "tool_call", "text"]
        assert blocks[0]["content"] == "thinking text"
        assert blocks[1]["tool"] == "Read"
        assert blocks[1]["tool_use_id"] == "tu_1"
        assert blocks[2]["content"] == "final text"

        # Already-migrated row preserved.
        async with db.execute(
            "SELECT blocks FROM messages WHERE session_id = 's-keep'"
        ) as cur:
            (raw,) = await cur.fetchone()
        assert json.loads(raw) == already


@pytest.mark.asyncio
async def test_v26_is_idempotent_when_column_already_dropped(tmp_path):
    """Running V26 twice (or on a fresh DB built by later schemas) must
    not fail."""
    import aiosqlite

    db_path = tmp_path / "post.db"
    async with aiosqlite.connect(db_path) as db:
        await db.executescript("""
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT,
                thinking TEXT,
                blocks JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                channel TEXT
            );
        """)
        await db.commit()

        from nerve.db.migrations.v026_drop_legacy_tool_calls import up
        await up(db)  # must not raise


# --- Consumer Cursors ---

@pytest.mark.asyncio
class TestConsumerCursors:
    """Test consumer cursor operations for Kafka-like source consumption."""

    async def _insert_messages(self, db: Database, source: str, count: int) -> list[int]:
        """Insert test source messages and return their rowids."""
        from nerve.sources.models import SourceRecord
        records = []
        for i in range(count):
            records.append(SourceRecord(
                id=f"msg-{source}-{i}",
                source=source,
                record_type="test",
                summary=f"Test message {i} from {source}",
                content=f"Content of message {i}",
                timestamp=f"2026-03-06T{10+i:02d}:00:00Z",
            ))
        await db.insert_source_messages(records, source=source, ttl_days=7)
        # Retrieve rowids
        rowids = []
        async with db.db.execute(
            "SELECT rowid FROM source_messages WHERE source = ? ORDER BY rowid ASC",
            (source,),
        ) as cursor:
            async for row in cursor:
                rowids.append(row[0])
        return rowids

    async def test_consumer_cursor_init_to_latest(self, db: Database):
        """New consumer cursor should initialize to MAX(rowid) so first poll returns nothing."""
        rowids = await self._insert_messages(db, "github", 3)
        max_rowid = max(rowids)

        cursor = await db.get_consumer_cursor("test-consumer", "github")
        assert cursor == max_rowid

        # Poll should return nothing (cursor is at latest)
        messages = await db.read_source_messages_by_rowid("github", after_seq=cursor, limit=50)
        assert len(messages) == 0

    async def test_consumer_cursor_read_advance(self, db: Database):
        """Consumer cursor should advance and subsequent reads return only new messages."""
        rowids = await self._insert_messages(db, "github", 3)

        # Set cursor to first message
        await db.set_consumer_cursor("reader", "github", rowids[0])

        # Read should return messages after cursor
        messages = await db.read_source_messages_by_rowid("github", after_seq=rowids[0], limit=50)
        assert len(messages) == 2
        assert messages[0]["rowid"] == rowids[1]
        assert messages[1]["rowid"] == rowids[2]

        # Advance cursor to last message
        await db.set_consumer_cursor("reader", "github", rowids[2])

        # Read should return nothing
        messages = await db.read_source_messages_by_rowid("github", after_seq=rowids[2], limit=50)
        assert len(messages) == 0

    async def test_consumer_cursor_per_source(self, db: Database):
        """Independent cursors per (consumer, source) pair."""
        gh_rowids = await self._insert_messages(db, "github", 2)
        gm_rowids = await self._insert_messages(db, "gmail:test@example.com", 2)

        # Set cursor for github only
        await db.set_consumer_cursor("inbox", "github", gh_rowids[1])

        # Github should show nothing new
        gh_msgs = await db.read_source_messages_by_rowid("github", after_seq=gh_rowids[1], limit=50)
        assert len(gh_msgs) == 0

        # Gmail cursor initializes to latest (new consumer for this source)
        gm_cursor = await db.get_consumer_cursor("inbox", "gmail:test@example.com")
        assert gm_cursor == max(gm_rowids)

        # Gmail also shows nothing (cursor at latest)
        gm_msgs = await db.read_source_messages_by_rowid("gmail:test@example.com", after_seq=gm_cursor, limit=50)
        assert len(gm_msgs) == 0

    async def test_consumer_cursor_expiry(self, db: Database):
        """Expired cursor should re-initialize to latest."""
        rowids = await self._insert_messages(db, "github", 3)

        # Insert an already-expired cursor directly
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        expired = (now - timedelta(hours=1)).isoformat()
        await db.db.execute(
            """INSERT OR REPLACE INTO consumer_cursors
               (consumer, source, cursor_seq, updated_at, expires_at)
               VALUES (?, ?, ?, ?, ?)""",
            ("expiring", "github", rowids[0], now.isoformat(), expired),
        )
        await db.db.commit()

        # get_consumer_cursor should detect expiry and re-init to latest
        cursor = await db.get_consumer_cursor("expiring", "github")
        assert cursor == max(rowids)

    async def test_consumer_cursor_ttl_refresh(self, db: Database):
        """Each set_consumer_cursor should extend the TTL."""
        rowids = await self._insert_messages(db, "github", 1)

        await db.set_consumer_cursor("reader", "github", rowids[0], ttl_days=2)

        async with db.db.execute(
            "SELECT expires_at FROM consumer_cursors WHERE consumer = ? AND source = ?",
            ("reader", "github"),
        ) as cursor:
            row = await cursor.fetchone()
            first_expires = row[0]

        # Update again with longer TTL
        await db.set_consumer_cursor("reader", "github", rowids[0], ttl_days=5)

        async with db.db.execute(
            "SELECT expires_at FROM consumer_cursors WHERE consumer = ? AND source = ?",
            ("reader", "github"),
        ) as cursor:
            row = await cursor.fetchone()
            second_expires = row[0]

        # Second TTL should be later
        assert second_expires > first_expires

    async def test_list_consumer_cursors_with_unread(self, db: Database):
        """list_consumer_cursors should include accurate unread counts."""
        rowids = await self._insert_messages(db, "github", 5)

        # Set cursor to 3rd message (2 unread)
        await db.set_consumer_cursor("inbox", "github", rowids[2])

        cursors = await db.list_consumer_cursors(consumer="inbox")
        assert len(cursors) == 1
        assert cursors[0]["consumer"] == "inbox"
        assert cursors[0]["source"] == "github"
        assert cursors[0]["cursor_seq"] == rowids[2]
        assert cursors[0]["unread"] == 2

    async def test_browse_source_messages(self, db: Database):
        """Browse historical messages with manual cursors."""
        rowids = await self._insert_messages(db, "github", 5)

        # Browse latest (no cursor)
        messages = await db.browse_source_messages("github", limit=3)
        assert len(messages) == 3
        # Newest first (DESC)
        assert messages[0]["rowid"] == rowids[4]

        # Browse before a seq
        messages = await db.browse_source_messages("github", limit=2, before_seq=rowids[3])
        assert len(messages) == 2
        assert all(m["rowid"] < rowids[3] for m in messages)

        # Browse after a seq
        messages = await db.browse_source_messages("github", limit=2, after_seq=rowids[1])
        assert len(messages) == 2
        assert all(m["rowid"] > rowids[1] for m in messages)

    async def test_read_source_messages_uses_processed_content(self, db: Database):
        """read_source_messages_by_rowid should prefer processed_content over content."""
        rowids = await self._insert_messages(db, "github", 1)

        # Update processed_content
        await db.db.execute(
            "UPDATE source_messages SET processed_content = ? WHERE source = ? AND rowid = ?",
            ("condensed version", "github", rowids[0]),
        )
        await db.db.commit()

        messages = await db.read_source_messages_by_rowid("github", after_seq=0, limit=50)
        assert len(messages) == 1
        assert messages[0]["content"] == "condensed version"

    async def test_cleanup_expired_consumer_cursors(self, db: Database):
        """Cleanup should remove only expired cursors."""
        await self._insert_messages(db, "github", 1)

        await db.set_consumer_cursor("active", "github", 1, ttl_days=7)

        # Insert an already-expired cursor
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        expired = (now - timedelta(hours=1)).isoformat()
        await db.db.execute(
            """INSERT INTO consumer_cursors (consumer, source, cursor_seq, updated_at, expires_at)
               VALUES (?, ?, ?, ?, ?)""",
            ("expired", "github", 1, now.isoformat(), expired),
        )
        await db.db.commit()

        count = await db.cleanup_expired_consumer_cursors()
        assert count == 1

        # Active cursor should still exist
        cursors = await db.list_consumer_cursors()
        assert len(cursors) == 1
        assert cursors[0]["consumer"] == "active"
