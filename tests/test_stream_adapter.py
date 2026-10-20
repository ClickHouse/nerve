"""Channel replies show the orchestrator's answer only.

The session channel no longer carries sub-agent frames, but a channel reply
must not pick up a sub-agent's text or tool labels, nor agent and queue
state, even from a mixed deploy.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.channels.base import ChannelCapability, ChannelConstraints
from nerve.channels.stream_adapter import StreamAdapter


def _adapter(*, streaming: bool = False) -> tuple[StreamAdapter, MagicMock]:
    channel = MagicMock()
    channel.capabilities = ChannelCapability.SEND_TEXT
    if streaming:
        channel.capabilities |= ChannelCapability.STREAMING
    channel.constraints = ChannelConstraints(
        max_message_length=4000, supports_message_edit=streaming,
    )
    channel.format_response = lambda text: text
    channel.send = AsyncMock()
    channel.send_placeholder = AsyncMock(return_value=None)
    channel.edit_message = AsyncMock()
    channel.delete_message = AsyncMock()
    return StreamAdapter(channel, "C1", "s1"), channel


@pytest.mark.asyncio
async def test_parent_tagged_events_are_ignored():
    adapter, channel = _adapter()
    await adapter.on_event("s1", {"type": "token", "content": "Answer. "})
    await adapter.on_event("s1", {
        "type": "token", "content": "sub-agent text", "parent_tool_use_id": "toolu_A",
    })
    await adapter.on_event("s1", {
        "type": "tool_use", "tool": "Bash", "parent_tool_use_id": "toolu_A",
    })
    await adapter.on_event("s1", {"type": "token", "content": "Done."})
    await adapter.on_event("s1", {"type": "done"})
    [call] = channel.send.await_args_list
    assert call.args[0].text == "Answer. Done."


@pytest.mark.asyncio
@pytest.mark.parametrize("message", [
    {"type": "agent_update", "session_id": "s1", "agents": [], "summary": {}},
    {"type": "agent_event", "session_id": "s1", "agent_id": "ag_1", "item": {"kind": "text"}},
    {"type": "queue_update", "session_id": "s1", "queue": []},
    {"type": "session_agents", "session_id": "s1", "background_count": 1,
     "has_background_tasks": True},
    # A parent-tagged terminal frame must not end the reply either.
    {"type": "done", "parent_tool_use_id": "toolu_A"},
    {"type": "error", "error": "boom", "parent_tool_use_id": "toolu_A"},
])
async def test_agent_and_queue_state_is_ignored(message):
    adapter, channel = _adapter()
    await adapter.on_event("s1", {"type": "token", "content": "Answer."})
    await adapter.on_event("s1", message)
    channel.send.assert_not_awaited()
    await adapter.on_event("s1", {"type": "done"})
    [call] = channel.send.await_args_list
    assert call.args[0].text == "Answer."


@pytest.mark.asyncio
async def test_a_streamed_reply_ignores_sub_agent_tools():
    adapter, channel = _adapter(streaming=True)
    adapter._placeholder_id = "1.1"
    await adapter.on_event("s1", {"type": "tool_use", "tool": "Read"})
    await adapter.on_event("s1", {
        "type": "tool_use", "tool": "Bash", "parent_tool_use_id": "toolu_A",
    })
    assert "Bash" not in adapter._buffer
    assert "[Read]" in adapter._buffer
