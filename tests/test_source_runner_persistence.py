import sqlite3

import pytest

from nerve.sources.base import Source
from nerve.sources.models import FetchResult, SourceRecord
from nerve.sources.runner import SourceRunner


SOURCE_NAME = "test-source-persistence"


def _record(record_id: str) -> SourceRecord:
    return SourceRecord(
        id=record_id,
        source=SOURCE_NAME,
        record_type="test",
        summary=f"Record {record_id}",
        content=f"Content {record_id}",
        timestamp="2026-08-25T00:00:00Z",
    )


class _FixedSource(Source):
    source_name = SOURCE_NAME

    def __init__(self, records: list[SourceRecord], next_cursor: str = "new"):
        self.records = records
        self.next_cursor = next_cursor

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        return FetchResult(records=list(self.records), next_cursor=self.next_cursor)


@pytest.mark.asyncio
async def test_insert_source_messages_rolls_back_the_batch_on_insert_failure(db):
    records = [_record("a"), _record("b"), _record("c")]
    records[1].summary = None

    with pytest.raises(sqlite3.IntegrityError):
        await db.insert_source_messages(records, source=SOURCE_NAME)

    rows, _ = await db.list_source_messages(source=SOURCE_NAME, limit=10)
    assert rows == []
    assert not db.db.in_transaction


@pytest.mark.asyncio
async def test_runner_reports_persistence_failure_without_advancing_cursor(
    db, monkeypatch,
):
    await db.set_sync_cursor(SOURCE_NAME, "old")
    runner = SourceRunner(_FixedSource([_record("a")]), db)

    async def fail(records, source, ttl_days):
        raise RuntimeError("inbox unavailable")

    monkeypatch.setattr(db, "insert_source_messages", fail)

    result = await runner.run()

    assert result.records_ingested == 0
    assert result.error == "inbox unavailable"
    assert await db.get_sync_cursor(SOURCE_NAME) == "old"


@pytest.mark.asyncio
async def test_runner_rolls_back_real_insert_failure_and_preserves_cursor(
    db, caplog,
):
    await db.set_sync_cursor(SOURCE_NAME, "old")
    records = [_record("good"), _record("bad")]
    records[1].summary = None
    runner = SourceRunner(_FixedSource(records), db)

    result = await runner.run()

    assert result.records_ingested == 0
    assert result.error is not None
    assert await db.get_sync_cursor(SOURCE_NAME) == "old"
    rows, _ = await db.list_source_messages(source=SOURCE_NAME, limit=10)
    assert rows == []
    assert not db.db.in_transaction
    assert f"{SOURCE_NAME}/bad" in caplog.text


@pytest.mark.asyncio
async def test_runner_rolls_back_existing_update_when_later_insert_fails(db):
    existing = _record("existing")
    await db.insert_source_messages([existing], source=SOURCE_NAME)

    async with db.db.execute(
        "SELECT rowid FROM source_messages WHERE source = ? AND id = ?",
        (SOURCE_NAME, existing.id),
    ) as cursor:
        old_rowid = (await cursor.fetchone())[0]

    await db.set_sync_cursor(SOURCE_NAME, "old")
    updated = _record(existing.id)
    updated.content = "Updated content"
    bad = _record("bad")
    bad.summary = None

    result = await SourceRunner(_FixedSource([updated, bad]), db).run()

    assert result.records_ingested == 0
    assert result.error is not None
    assert await db.get_sync_cursor(SOURCE_NAME) == "old"
    async with db.db.execute(
        "SELECT rowid, id, summary, content FROM source_messages "
        "WHERE source = ? ORDER BY rowid",
        (SOURCE_NAME,),
    ) as cursor:
        rows = [dict(row) async for row in cursor]
    assert rows == [{
        "rowid": old_rowid,
        "id": existing.id,
        "summary": existing.summary,
        "content": existing.content,
    }]
    assert not db.db.in_transaction


@pytest.mark.asyncio
async def test_runner_retries_the_same_batch_after_persistence_recovers(
    db, monkeypatch,
):
    records = [_record("a"), _record("b")]
    runner = SourceRunner(_FixedSource(records), db)
    insert_source_messages = db.insert_source_messages
    attempts = 0

    async def fail_once(records, source, ttl_days):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary inbox failure")
        return await insert_source_messages(records, source=source, ttl_days=ttl_days)

    monkeypatch.setattr(db, "insert_source_messages", fail_once)

    first = await runner.run()
    assert first.error == "temporary inbox failure"
    assert await db.get_sync_cursor(SOURCE_NAME) is None

    runner.health.backoff_until = None
    second = await runner.run()

    assert second.error is None
    assert second.records_ingested == 2
    assert await db.get_sync_cursor(SOURCE_NAME) == "new"
    rows, _ = await db.list_source_messages(source=SOURCE_NAME, limit=10)
    assert {row["id"] for row in rows} == {"a", "b"}
