"""Tests for the interactive tool handler: timeout, waiting-input indicator."""

from __future__ import annotations

import asyncio

import pytest
from claude_agent_sdk.types import PermissionResultAllow, PermissionResultDeny

from nerve.agent.interactive import (
    INTERACTION_TIMEOUT,
    InteractiveToolHandler,
    _humanize_seconds,
    get_awaiting_ids,
    register_handler,
    unregister_handler,
)


def test_interaction_timeout_is_one_hour():
    """AskUserQuestion / plan-mode waits give the user a full hour to respond."""
    assert INTERACTION_TIMEOUT == 3600


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (3600, "1 hour"),
        (7200, "2 hours"),
        (300, "5 minutes"),
        (60, "1 minute"),
        (90, "1 minute"),  # floor division -> 1
        (5400, "90 minutes"),  # not a whole number of hours
    ],
)
def test_humanize_seconds(seconds, expected):
    assert _humanize_seconds(seconds) == expected


async def _wait_until_pending(handler: InteractiveToolHandler) -> None:
    """Spin briefly until the handler registers its pending interaction."""
    for _ in range(200):
        if handler.has_pending:
            return
        await asyncio.sleep(0.005)
    raise AssertionError("handler never became pending")


@pytest.mark.asyncio
async def test_awaiting_input_broadcast_and_registry():
    """An interactive wait sets the registry flag and broadcasts awaiting=true,
    then clears both once the user answers."""
    messages: list[tuple[str, dict]] = []

    async def broadcast(channel: str, msg: dict) -> None:
        messages.append((channel, msg))

    handler = InteractiveToolHandler("sess-await", broadcast, interactive_capable=True)
    register_handler("sess-await", handler)
    try:
        task = asyncio.create_task(
            handler._handle_interactive("AskUserQuestion", {"questions": []})
        )
        await _wait_until_pending(handler)

        # Registry reports the session as waiting for input.
        assert "sess-await" in get_awaiting_ids()

        # A global awaiting=true broadcast was emitted.
        awaiting = [m for (_c, m) in messages if m["type"] == "session_awaiting_input"]
        assert awaiting and awaiting[-1]["awaiting"] is True
        assert any(c == "__global__" for (c, m) in messages if m["type"] == "session_awaiting_input")

        # Resolve with an answer.
        interaction = next(m for (_c, m) in messages if m["type"] == "interaction")
        assert handler.resolve(interaction["interaction_id"], {"q": "a"})
        result = await task

        assert isinstance(result, PermissionResultAllow)
        assert result.updated_input == {"questions": [], "answers": {"q": "a"}}

        # Waiting state cleared in registry and via a final awaiting=false broadcast.
        assert not handler.has_pending
        assert "sess-await" not in get_awaiting_ids()
        awaiting = [m for (_c, m) in messages if m["type"] == "session_awaiting_input"]
        assert awaiting[-1]["awaiting"] is False
    finally:
        unregister_handler("sess-await")


@pytest.mark.asyncio
async def test_resolve_broadcasts_interaction_resolved():
    """Resolving emits a session-scoped interaction_resolved event so parallel
    clients clear their pending poll/plan prompt instead of re-prompting."""
    messages: list[tuple[str, dict]] = []

    async def broadcast(channel: str, msg: dict) -> None:
        messages.append((channel, msg))

    handler = InteractiveToolHandler("sess-resolved", broadcast, interactive_capable=True)
    register_handler("sess-resolved", handler)
    try:
        task = asyncio.create_task(
            handler._handle_interactive("AskUserQuestion", {"questions": []})
        )
        await _wait_until_pending(handler)
        interaction = next(m for (_c, m) in messages if m["type"] == "interaction")

        assert handler.resolve(interaction["interaction_id"], {"q": "a"})
        await task

        resolved = [(c, m) for (c, m) in messages if m["type"] == "interaction_resolved"]
        assert resolved, "expected an interaction_resolved broadcast"
        channel, msg = resolved[-1]
        assert channel == "sess-resolved"
        assert msg["session_id"] == "sess-resolved"
        assert msg["interaction_id"] == interaction["interaction_id"]
    finally:
        unregister_handler("sess-resolved")


@pytest.mark.asyncio
async def test_cancel_all_clears_awaiting():
    """Stopping a session mid-wait denies the interaction and clears the flag."""
    messages: list[tuple[str, dict]] = []

    async def broadcast(channel: str, msg: dict) -> None:
        messages.append((channel, msg))

    handler = InteractiveToolHandler("sess-cancel", broadcast, interactive_capable=True)
    register_handler("sess-cancel", handler)
    try:
        task = asyncio.create_task(handler._handle_interactive("EnterPlanMode", {}))
        await _wait_until_pending(handler)
        assert "sess-cancel" in get_awaiting_ids()

        handler.cancel_all()
        result = await task

        assert isinstance(result, PermissionResultDeny)
        assert not handler.has_pending
        assert "sess-cancel" not in get_awaiting_ids()
        awaiting = [m for (_c, m) in messages if m["type"] == "session_awaiting_input"]
        assert awaiting[-1]["awaiting"] is False
    finally:
        unregister_handler("sess-cancel")
