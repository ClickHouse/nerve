"""Tests for the ``session_context`` MCP tool.

Verifies the tool stitches recalled memories + active skills + session
metadata into one ToolResult. Uses stub bridge/manager objects rather
than spinning up the real memU bridge — the integration is already
covered by ``test_memu_bridge.py``.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.tools.handlers.memory import session_context_handler
from nerve.agent.tools.registry import ToolContext


def _ctx(*, memory_bridge=None, skill_manager=None, db=None, workspace=None):
    return ToolContext(
        session_id="test-session-123",
        workspace=workspace or Path("/tmp/ws"),
        db=db,
        memory_bridge=memory_bridge,
        skill_manager=skill_manager,
        config=None,
    )


@pytest.mark.asyncio
async def test_returns_error_when_topic_missing() -> None:
    result = await session_context_handler(_ctx(), {"topic": ""})
    assert result.is_error
    assert "topic" in result.content[0]["text"]


@pytest.mark.asyncio
async def test_bundles_recall_and_skills() -> None:
    bridge = MagicMock()
    bridge.available = True
    bridge.recall = AsyncMock(return_value=[
        {"id": "mem-1", "type": "knowledge", "summary": "Alice is in NYC"},
        {"id": "mem-2", "type": "profile", "summary": "Prefers concise responses"},
    ])

    skill_manager = MagicMock()
    skill_manager.get_enabled_summaries = AsyncMock(return_value=[
        {"id": "db-query", "name": "db-query", "description": "Query the database"},
    ])

    db = MagicMock()
    db.get_session = AsyncMock(return_value={"source": "codex"})

    ctx = _ctx(memory_bridge=bridge, skill_manager=skill_manager, db=db)
    result = await session_context_handler(ctx, {
        "topic": "fix the auth endpoint",
        "memory_limit": 5,
    })

    text = result.content[0]["text"]
    assert "fix the auth endpoint" in text
    assert "Alice is in NYC" in text
    assert "db-query" in text
    assert "test-session-123" in text
    assert "codex" in text  # source from session record

    # Recall was biased by topic
    args, kwargs = bridge.recall.call_args
    assert "fix the auth endpoint" in args[0]
    assert kwargs["limit"] == 5


@pytest.mark.asyncio
async def test_handles_missing_memory_bridge_gracefully() -> None:
    """No bridge => the tool still returns metadata + skills + a note."""
    skill_manager = MagicMock()
    skill_manager.get_enabled_summaries = AsyncMock(return_value=[])
    ctx = _ctx(memory_bridge=None, skill_manager=skill_manager)

    result = await session_context_handler(ctx, {"topic": "anything"})
    text = result.content[0]["text"]
    assert "memory bridge not available" in text
    assert "Session Context" in text


@pytest.mark.asyncio
async def test_include_skills_false_omits_skills() -> None:
    skill_manager = MagicMock()
    skill_manager.get_enabled_summaries = AsyncMock(return_value=[
        {"id": "x", "name": "x", "description": "y"},
    ])
    ctx = _ctx(skill_manager=skill_manager)

    result = await session_context_handler(ctx, {
        "topic": "anything",
        "include_skills": False,
    })
    text = result.content[0]["text"]
    assert "Active Skills" not in text
