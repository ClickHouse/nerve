"""Regression tests for the Gmail source."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from nerve.sources.gmail import GmailSource


def _message(message_id: str, epoch: int) -> dict:
    return {
        "id": message_id,
        "threadId": f"thread-{message_id}",
        "subject": f"Message {message_id}",
        "from": "sender@example.com",
        "date": f"1970-01-01T00:{epoch // 60:02d}:{epoch % 60:02d}Z",
        "labels": ["INBOX"],
    }


@pytest.mark.asyncio
async def test_failed_body_fetch_is_retried_until_message_recovers():
    source = GmailSource("me@example.com", {})
    messages = [_message("failed", 101), _message("healthy", 102)]
    failed_body_recovers = False
    queries: list[str] = []

    async def search(query, limit, env):
        queries.append(query)
        return messages

    async def fetch_body(message_id, env, sem):
        if message_id == "failed" and not failed_body_recovers:
            return None
        epoch = 101 if message_id == "failed" else 102
        return f"body for {message_id}", None, epoch

    source._search_messages = search
    source._fetch_message_body = fetch_body

    first = await source.fetch(cursor="100")

    assert [record.id for record in first.records] == ["healthy"]
    first_cursor = json.loads(first.next_cursor)
    assert first_cursor["epoch"] == 102
    assert first_cursor["pending"]["failed"]["status"] == "failed"
    assert first_cursor["pending"]["failed"]["attempts"] == 1
    assert "body for healthy" in first.records[0].content

    failed_body_recovers = True
    second = await source.fetch(cursor=first.next_cursor)

    assert [record.id for record in second.records] == ["failed"]
    assert "body for failed" in second.records[0].content
    assert json.loads(second.next_cursor) == {"epoch": 102, "pending": {}}
    assert queries == [
        "after:101 -in:spam -in:trash",
        "after:103 -in:spam -in:trash",
    ]


@pytest.mark.asyncio
async def test_body_fetch_exception_does_not_create_header_only_record():
    source = GmailSource("me@example.com", {})

    async def search(query, limit, env):
        return [_message("failed", 101)]

    async def fetch_body(message_id, env, sem):
        raise TimeoutError("temporary failure")

    source._search_messages = search
    source._fetch_message_body = fetch_body

    result = await source.fetch(cursor=None)

    assert result.records == []
    state = json.loads(result.next_cursor)
    assert state["epoch"] is None
    assert state["pending"]["failed"]["status"] == "failed"
    assert state["pending"]["failed"]["attempts"] == 1

    retry = await source.fetch(cursor=result.next_cursor)
    retry_state = json.loads(retry.next_cursor)
    assert retry_state["pending"]["failed"]["attempts"] == 2


@pytest.mark.asyncio
async def test_partial_first_sync_persists_failed_message_for_recovery():
    source = GmailSource("me@example.com", {})
    messages = [_message("failed", 101), _message("healthy", 102)]
    failed_body_recovers = False
    queries: list[str] = []

    async def search(query, limit, env):
        queries.append(query)
        assert query in {
            "newer_than:1d -in:spam -in:trash",
            "after:103 -in:spam -in:trash",
        }
        return messages

    async def fetch_body(message_id, env, sem):
        if message_id == "failed" and not failed_body_recovers:
            return None
        epoch = 101 if message_id == "failed" else 102
        return f"body for {message_id}", None, epoch

    source._search_messages = search
    source._fetch_message_body = fetch_body

    first = await source.fetch(cursor=None)

    assert [record.id for record in first.records] == ["healthy"]
    first_cursor = json.loads(first.next_cursor)
    assert first_cursor["epoch"] == 102
    assert "failed" in first_cursor["pending"]

    failed_body_recovers = True
    second = await source.fetch(cursor=first.next_cursor)

    assert [record.id for record in second.records] == ["failed"]
    assert json.loads(second.next_cursor) == {"epoch": 102, "pending": {}}
    assert queries == [
        "newer_than:1d -in:spam -in:trash",
        "after:103 -in:spam -in:trash",
    ]


@pytest.mark.asyncio
async def test_pending_body_fetch_is_retried_after_message_leaves_search_window():
    source = GmailSource("me@example.com", {})
    failed = _message("failed", 101)
    search_results = [[failed], []]
    failed_body_recovers = False
    queries: list[str] = []

    async def search(query, limit, env):
        queries.append(query)
        return search_results.pop(0)

    async def fetch_body(message_id, env, sem):
        if not failed_body_recovers:
            return None
        return "recovered body", None, 101

    source._search_messages = search
    source._fetch_message_body = fetch_body

    first = await source.fetch(cursor="100")
    assert first.records == []
    assert json.loads(first.next_cursor)["pending"]["failed"]["attempts"] == 1

    failed_body_recovers = True
    second = await source.fetch(cursor=first.next_cursor)

    assert [record.id for record in second.records] == ["failed"]
    assert "recovered body" in second.records[0].content
    assert json.loads(second.next_cursor) == {"epoch": 101, "pending": {}}
    assert queries == [
        "after:101 -in:spam -in:trash",
        "after:101 -in:spam -in:trash",
    ]


@pytest.mark.asyncio
async def test_nonzero_body_command_is_reported_as_failure():
    source = GmailSource("me@example.com", {})
    process = AsyncMock()
    process.returncode = 1
    process.communicate.return_value = (b"", b"temporary API failure")

    with patch(
        "nerve.sources.gmail.asyncio.create_subprocess_exec",
        AsyncMock(return_value=process),
    ):
        result = await source._fetch_message_body(
            "message-id", {}, asyncio.Semaphore(1),
        )

    assert result is None
