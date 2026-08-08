"""Regression tests for the Gmail source."""

from __future__ import annotations

import asyncio
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
async def test_failed_body_fetch_keeps_cursor_until_message_recovers():
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
    assert first.next_cursor == "100"
    assert "body for healthy" in first.records[0].content

    failed_body_recovers = True
    second = await source.fetch(cursor=first.next_cursor)

    assert [record.id for record in second.records] == ["failed", "healthy"]
    assert "body for failed" in second.records[0].content
    assert second.next_cursor == "102"
    assert queries == ["after:101 -in:spam -in:trash"] * 2


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
    assert result.next_cursor is None


@pytest.mark.asyncio
async def test_partial_first_sync_waits_to_establish_cursor_until_recovery():
    source = GmailSource("me@example.com", {})
    messages = [_message("failed", 101), _message("healthy", 102)]
    failed_body_recovers = False

    async def search(query, limit, env):
        assert query == "newer_than:1d -in:spam -in:trash"
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
    assert first.next_cursor is None

    failed_body_recovers = True
    second = await source.fetch(cursor=first.next_cursor)

    assert [record.id for record in second.records] == ["failed", "healthy"]
    assert second.next_cursor == "102"


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
