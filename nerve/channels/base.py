"""Base channel interface and data models.

Channels are the interfaces through which users interact with Nerve:
Telegram, Web UI, Discord, etc. Each channel implements the transport
layer and declares its capabilities; the ChannelRouter handles session
management and message dispatch.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from enum import Flag, auto
from typing import Any


class ChannelCapability(Flag):
    """Capabilities a channel can declare.

    The router checks these before calling optional methods — a channel
    that does not declare STREAMING will never have send_placeholder()
    or edit_message() called.
    """

    SEND_TEXT = auto()          # Can send plain text messages
    STREAMING = auto()          # Supports incremental updates (edit-in-place or chunked)
    MARKDOWN = auto()           # Renders markdown natively
    INTERACTIVE = auto()        # Can route interactive tool responses back (AskUserQuestion, etc.)
    TYPING_INDICATOR = auto()   # Can show "typing…" status
    REACTIONS = auto()          # Can set emoji reactions on messages


@dataclass(frozen=True)
class ChannelConstraints:
    """Concrete constraints for a channel.

    Channels override these to declare platform-specific limits.
    """

    max_message_length: int = 0         # 0 = unlimited
    min_edit_interval: float = 0.0      # Minimum seconds between edits (rate limit)
    supports_message_edit: bool = False  # Can edit previously sent messages


@dataclass
class InboundMessage:
    """A message received from a channel, normalized for the router."""

    channel_name: str                               # "telegram", "web", "discord"
    channel_key: str                                # "telegram:12345", "web:default"
    sender_id: str                                  # User identifier within the channel
    text: str                                       # The message content
    session_id: str | None = None                   # Explicit session override (web sends this)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class OutboundMessage:
    """A message to send to a channel target."""

    target: str                                     # Channel-specific target (chat_id, client_id)
    text: str
    session_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseChannel(abc.ABC):
    """Abstract base for all communication channels.

    Channels own their transport (Telegram bot, Discord gateway, WebSocket, etc.)
    and translate between transport-native messages and InboundMessage/OutboundMessage.

    Session management is NOT the channel's responsibility — that is handled by
    ChannelRouter. Channels should never call engine.run() or engine.sessions.*
    directly.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Channel identifier (e.g., 'telegram', 'web', 'discord')."""
        ...

    @property
    @abc.abstractmethod
    def capabilities(self) -> ChannelCapability:
        """Declare what this channel supports."""
        ...

    @property
    def constraints(self) -> ChannelConstraints:
        """Channel-specific constraints. Override for non-default values."""
        return ChannelConstraints()

    @abc.abstractmethod
    async def start(self) -> None:
        """Start the channel transport (connect, listen, poll, etc.)."""
        ...

    @abc.abstractmethod
    async def stop(self) -> None:
        """Stop the channel gracefully."""
        ...

    @abc.abstractmethod
    async def send(self, message: OutboundMessage) -> None:
        """Send a complete message to a target."""
        ...

    def format_response(self, text: str) -> str:
        """Format agent response for this channel. Override for channel-specific formatting."""
        return text

    # ------------------------------------------------------------------ #
    #  Optional: streaming support                                         #
    #  Only called if channel declares ChannelCapability.STREAMING.        #
    # ------------------------------------------------------------------ #

    async def send_placeholder(self, target: str, session_id: str) -> str | None:
        """Send a placeholder message for streaming updates.

        Returns a message_id that can be passed to edit_message(), or None.
        Only called if channel declares STREAMING capability.
        """
        return None

    async def edit_message(self, target: str, message_id: str, text: str) -> None:
        """Edit a previously sent message (for streaming).

        Only called if channel declares STREAMING capability and
        constraints.supports_message_edit is True.
        """

    # ------------------------------------------------------------------ #
    #  Optional: typing indicator                                          #
    #  Only called if channel declares ChannelCapability.TYPING_INDICATOR. #
    # ------------------------------------------------------------------ #

    async def send_typing(self, target: str) -> None:
        """Show typing indicator.

        Only called if channel declares TYPING_INDICATOR capability.
        """

    # ------------------------------------------------------------------ #
    #  Optional: reactions support                                          #
    #  Only called if channel declares ChannelCapability.REACTIONS.         #
    # ------------------------------------------------------------------ #

    async def set_reaction(self, target: str, message_id: int, emoji: str) -> None:
        """Set an emoji reaction on a message.

        Only called if channel declares REACTIONS capability.
        """

    # ------------------------------------------------------------------ #
    #  Optional: interactive tool support                                   #
    #  Only called if channel declares ChannelCapability.INTERACTIVE.       #
    # ------------------------------------------------------------------ #

    async def send_interaction(
        self,
        target: str,
        session_id: str,
        interaction_id: str,
        interaction_type: str,
        tool_name: str,
        tool_input: dict[str, Any],
    ) -> None:
        """Send an interactive tool prompt to the user.

        Only called if channel declares INTERACTIVE capability.
        For Telegram, this could render as inline keyboard buttons.
        For Web, this is a JSON event over WebSocket.
        """
