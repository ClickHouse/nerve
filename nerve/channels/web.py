"""Web UI channel — WebSocket relay.

The web channel doesn't have its own transport; it relies on the FastAPI
WebSocket endpoint in gateway/server.py. This module provides the channel
interface for capability declaration, formatting, and cron output delivery.
"""

from __future__ import annotations

from typing import Any

from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    ChannelConstraints,
    OutboundMessage,
)


class WebChannel(BaseChannel):
    """Web UI channel (passive — uses gateway WebSocket).

    The WebSocket handler in gateway/server.py manages its own broadcaster
    registration for the lifetime of each connection. This channel primarily
    serves as the capability declaration and delivery target for cron output.
    """

    @property
    def name(self) -> str:
        return "web"

    @property
    def capabilities(self) -> ChannelCapability:
        return (
            ChannelCapability.SEND_TEXT
            | ChannelCapability.STREAMING
            | ChannelCapability.MARKDOWN
            | ChannelCapability.INTERACTIVE
            | ChannelCapability.SEND_FILES
        )

    @property
    def constraints(self) -> ChannelConstraints:
        return ChannelConstraints(
            max_message_length=0,           # No limit
            supports_message_edit=False,    # WebSocket uses event stream, not edits
        )

    async def start(self) -> None:
        pass  # WebSocket is managed by FastAPI

    async def stop(self) -> None:
        pass

    async def send(self, message: OutboundMessage) -> None:
        """Send a complete message to web clients via the broadcaster.

        Used for cron delivery and non-streaming responses.
        """
        from nerve.agent.streaming import broadcaster
        await broadcaster.broadcast(message.session_id, {
            "type": "message",
            "session_id": message.session_id,
            "content": message.text,
        })

    def format_response(self, text: str) -> str:
        # Web UI renders markdown directly
        return text

    async def send_interaction(
        self,
        target: str,
        session_id: str,
        interaction_id: str,
        interaction_type: str,
        tool_name: str,
        tool_input: dict[str, Any],
    ) -> None:
        """Broadcast interaction to web clients via the broadcaster."""
        from nerve.agent.streaming import broadcaster
        await broadcaster.broadcast_interaction(
            session_id, interaction_type, interaction_id,
            tool_name, tool_input,
        )

    async def send_file(self, target: str, file_path: str) -> bool:
        """Web file delivery is handled by the persisted tool_call block.

        The frontend renders a SendFileBlock card from the stored
        ``send_file`` tool call (input + result). Returning True signals
        that no fallback message is needed — the file is reachable via
        the inline download card in the web UI.
        """
        return True
