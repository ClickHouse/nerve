"""Tests for the external_tool_call audit writer.

Verifies that :func:`build_audit_writer` produces a coroutine that
serializes a tool call into a ``session_events`` row with the expected
shape, truncates oversized payloads, and never propagates DB errors
(swallowed so a logging hiccup can't break the conversation).
"""

from __future__ import annotations

import json

import pytest

from nerve.agent.tools import ToolResult
from nerve.mcp_server.audit import build_audit_writer


@pytest.mark.asyncio
async def test_audit_writer_records_tool_call(db):
    await db.create_session(
        session_id="external:codex:t-audit",
        source="external",
    )
    write = build_audit_writer(db)
    await write(
        session_id="external:codex:t-audit",
        tool_name="task_create",
        args={"title": "test", "content": "x"},
        result=ToolResult.text("Created task t-12345"),
        duration_ms=42.7,
        is_error=False,
    )

    async with db.db.execute(
        "SELECT event_type, details FROM session_events WHERE session_id = ?",
        ("external:codex:t-audit",),
    ) as cursor:
        rows = [dict(r) async for r in cursor]

    assert len(rows) == 1
    assert rows[0]["event_type"] == "external_tool_call"
    details = json.loads(rows[0]["details"])
    assert details["tool"] == "task_create"
    assert details["is_error"] is False
    assert details["duration_ms"] == 42.7
    assert "Created task" in details["result"]
    assert "task_create" not in details["args"]  # tool name not in args dict
    assert "test" in details["args"]


@pytest.mark.asyncio
async def test_audit_writer_truncates_large_payload(db):
    await db.create_session(
        session_id="external:codex:t-big",
        source="external",
    )
    write = build_audit_writer(db)
    big_text = "X" * 10_000
    await write(
        session_id="external:codex:t-big",
        tool_name="memory_recall",
        args={"query": big_text},
        result=ToolResult.text(big_text),
        duration_ms=10.0,
        is_error=False,
    )

    async with db.db.execute(
        "SELECT details FROM session_events WHERE session_id = ?",
        ("external:codex:t-big",),
    ) as cursor:
        row = await cursor.fetchone()
    details = json.loads(row["details"])
    assert "truncated" in details["args"]
    assert "truncated" in details["result"]
    assert len(details["args"]) < 6000
    assert len(details["result"]) < 6000


@pytest.mark.asyncio
async def test_audit_writer_swallows_db_errors(db):
    """A broken DB layer must not propagate — audit failures are best-
    effort and must never break a successful tool call."""
    # No session row → log_session_event would normally violate the FK,
    # but SQLite without explicit foreign_keys=ON ignores the constraint.
    # Force the failure by closing the DB before the write.
    await db.close()
    write = build_audit_writer(db)
    # Should not raise even though the DB connection is dead.
    await write(
        session_id="nonexistent",
        tool_name="task_create",
        args={},
        result=ToolResult.text("ok"),
        duration_ms=1.0,
        is_error=False,
    )
