"""Tests for the external-session guard in :class:`NotificationService`.

External (satellite) sessions are MCP-driven by an outside agent
(Codex, Claude Code, ...). When a user answers a notification posed by
such a session, Nerve must NOT call ``engine.run()`` — the satellite
session has no SDK process to run, and starting one would spin up a
stray native turn the external agent never sees.

These tests pin that contract by mocking both the engine and the
broadcaster.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nerve.config import NerveConfig
from nerve.notifications.service import NotificationService


def _make_service(db, engine) -> NotificationService:
    return NotificationService(config=NerveConfig(), db=db, engine=engine)


@pytest.mark.asyncio
async def test_external_session_answer_does_not_call_engine_run(db):
    """Answering a notification posted by an external session must NOT
    trigger ``engine.run()``."""
    await db.create_session(
        session_id="external:codex:t1",
        source="external",
        metadata={"client_name": "codex"},
    )
    await db.create_notification(
        notification_id="ask-ext-1",
        session_id="external:codex:t1",
        type="question",
        title="Should I proceed?",
    )

    engine = MagicMock()
    engine.sessions = MagicMock()
    engine.sessions.is_running = MagicMock(return_value=False)
    engine.run = AsyncMock()

    service = _make_service(db, engine)
    with patch("nerve.agent.streaming.broadcaster.broadcast", new=AsyncMock()):
        ok = await service.handle_answer("ask-ext-1", "yes", "telegram")

    assert ok is True
    engine.run.assert_not_called()


@pytest.mark.asyncio
async def test_native_session_answer_still_injects(db):
    """Answering a notification on a native session keeps the existing
    behaviour: spawn an ``engine.run()`` task to inject the answer."""
    await db.create_session(
        session_id="native-session-1",
        source="web",
    )
    await db.create_notification(
        notification_id="ask-native-1",
        session_id="native-session-1",
        type="question",
        title="Continue?",
    )

    engine = MagicMock()
    engine.sessions = MagicMock()
    engine.sessions.is_running = MagicMock(return_value=False)
    engine.run = AsyncMock()

    service = _make_service(db, engine)
    with patch("nerve.agent.streaming.broadcaster.broadcast", new=AsyncMock()):
        ok = await service.handle_answer("ask-native-1", "go", "web")

    assert ok is True
    # engine.run is wrapped in asyncio.create_task — give it a tick to run.
    import asyncio
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    engine.run.assert_called_once()
    _args, kwargs = engine.run.call_args
    assert kwargs["session_id"] == "native-session-1"


@pytest.mark.asyncio
async def test_external_session_answer_broadcasts_to_global(db):
    """Even though we skip engine.run() for external sessions, we still
    broadcast ``notification_answered`` to the UI's global channel so
    the notifications page reflects the answered state immediately."""
    await db.create_session(
        session_id="external:codex:t2",
        source="external",
        metadata={"client_name": "codex"},
    )
    await db.create_notification(
        notification_id="ask-ext-2",
        session_id="external:codex:t2",
        type="question",
        title="Approve?",
    )

    engine = AsyncMock()
    service = _make_service(db, engine)
    broadcast_calls: list[tuple] = []

    async def _fake_broadcast(channel, message):
        broadcast_calls.append((channel, message["type"]))

    with patch("nerve.agent.streaming.broadcaster.broadcast", new=_fake_broadcast):
        ok = await service.handle_answer("ask-ext-2", "yes", "telegram")

    assert ok is True
    assert ("__global__", "notification_answered") in broadcast_calls
