"""Channel router — centralized session management and message dispatch.

Sits between channels and the agent engine. Channels send InboundMessages
to the router; the router resolves sessions, sets up streaming, runs the
agent, and tears down after completion.

Replaces the duplicated session management logic that was previously
spread across TelegramChannel and the WebSocket handler in gateway/server.py.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, TYPE_CHECKING

from nerve.agent.interactive import get_handler
from nerve.agent.streaming import broadcaster
from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    InboundMessage,
    OutboundMessage,
)
from nerve.channels.stream_adapter import StreamAdapter

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine

logger = logging.getLogger(__name__)


class ChannelRouter:
    """Central message router between channels and the agent engine.

    Responsibilities:
    - Channel registry (replaces engine._channels)
    - Session resolution for inbound messages
    - Broadcaster listener management per channel/target
    - StreamAdapter creation per channel capability
    - Interactive tool answer routing
    - Cron output delivery
    """

    def __init__(self, engine: AgentEngine):
        self.engine = engine
        self._channels: dict[str, BaseChannel] = {}
        # Active stream adapters: (channel_name, target) -> StreamAdapter
        self._adapters: dict[tuple[str, str], StreamAdapter] = {}

    # ------------------------------------------------------------------ #
    #  Channel registry                                                    #
    # ------------------------------------------------------------------ #

    def register(self, channel: BaseChannel) -> None:
        """Register a channel."""
        self._channels[channel.name] = channel
        logger.info(
            "Registered channel: %s (capabilities: %s)",
            channel.name, channel.capabilities,
        )

    def get_channel(self, name: str) -> BaseChannel | None:
        """Get a registered channel by name."""
        return self._channels.get(name)

    @property
    def channels(self) -> dict[str, BaseChannel]:
        """All registered channels (read-only view)."""
        return dict(self._channels)

    # ------------------------------------------------------------------ #
    #  Inbound: channel → engine                                           #
    # ------------------------------------------------------------------ #

    async def handle_message(self, msg: InboundMessage) -> str:
        """Process an inbound user message.

        1. Resolve session (from explicit session_id or channel mapping)
        2. Show typing indicator if supported
        3. Set up streaming adapter for the response
        4. Run the agent
        5. Tear down streaming adapter
        6. Return the final response text

        Called by channel implementations when they receive a user message.
        """
        channel = self._channels.get(msg.channel_name)
        if not channel:
            raise ValueError(f"Unknown channel: {msg.channel_name}")

        # Resolve session
        if msg.session_id:
            session_id = msg.session_id
            # Ensure mapping is persisted
            await self.engine.sessions.set_active_session(
                msg.channel_key, session_id,
            )
        else:
            session_id = await self.engine.sessions.get_active_session(
                msg.channel_key, source=msg.channel_name,
            )

        # Show typing indicator if supported
        if ChannelCapability.TYPING_INDICATOR in channel.capabilities:
            try:
                await channel.send_typing(msg.sender_id)
            except Exception as e:
                logger.debug("Typing indicator failed for %s: %s", msg.channel_name, e)

        # Set up streaming adapter
        adapter = await self._setup_streaming(
            channel, msg.sender_id, session_id,
        )

        # Extract images from metadata (e.g. Telegram photos)
        images = msg.metadata.get("images") if msg.metadata else None

        # Wrap in a Task so stop_session() can cancel it (otherwise
        # channels that ``await engine.run()`` directly — like Telegram —
        # have no cancellable task and /stop only sends an SDK interrupt
        # which may hang indefinitely).
        task = asyncio.create_task(
            self.engine.run(
                session_id=session_id,
                user_message=msg.text,
                source=msg.channel_name,
                channel=msg.channel_name,
                images=images,
            )
        )
        self.engine.register_task(session_id, task)
        try:
            response = await task
            return response
        except asyncio.CancelledError:
            # /stop cancelled the task — _run_inner already handled
            # cleanup (persisted sdk_session_id, marked stopped, etc.).
            # Return whatever partial response was captured.
            if task.done() and not task.cancelled():
                return task.result()
            return ""
        finally:
            await self._teardown_streaming(
                channel.name, msg.sender_id, session_id,
            )

    # ------------------------------------------------------------------ #
    #  Interactive tool response routing                                    #
    # ------------------------------------------------------------------ #

    async def handle_interaction_response(
        self,
        session_id: str,
        interaction_id: str,
        result: dict[str, Any] | None = None,
        denied: bool = False,
        deny_message: str = "",
    ) -> bool:
        """Route an interactive tool response to the correct handler.

        Returns True if the response was delivered, False if no handler found.
        """
        handler = get_handler(session_id)
        if not handler:
            logger.warning("No interactive handler for session %s", session_id)
            return False

        if denied:
            handler.deny(interaction_id, deny_message)
        else:
            handler.resolve(interaction_id, result)
        return True

    # ------------------------------------------------------------------ #
    #  Session management helpers                                          #
    #  Thin wrappers around engine.sessions — channels use these           #
    #  instead of touching engine.sessions directly.                       #
    # ------------------------------------------------------------------ #

    async def get_active_session(
        self, channel_key: str, source: str,
    ) -> str:
        """Get or create the active session for a channel."""
        return await self.engine.sessions.get_active_session(
            channel_key, source=source,
        )

    async def get_last_session(self, channel_key: str) -> str | None:
        """Get the last used session for a channel without auto-creating."""
        return await self.engine.sessions.get_last_session(channel_key)

    async def switch_session(
        self, channel_key: str, session_id: str,
    ) -> None:
        """Switch the active session for a channel."""
        await self.engine.sessions.set_active_session(
            channel_key, session_id,
        )

    async def create_session(
        self,
        channel_key: str,
        title: str | None = None,
        source: str = "web",
    ) -> str:
        """Create a new session and map it to a channel."""
        session_id = str(uuid.uuid4())[:8]
        await self.engine.sessions.get_or_create(
            session_id, title=title, source=source,
        )
        await self.engine.sessions.set_active_session(
            channel_key, session_id,
        )
        return session_id

    async def list_sessions(self, limit: int = 20) -> list[dict[str, Any]]:
        """List sessions, most recently updated first."""
        return await self.engine.sessions.list_sessions(limit=limit)

    # ------------------------------------------------------------------ #
    #  Outbound: engine → channel (cron delivery, etc.)                    #
    # ------------------------------------------------------------------ #

    async def deliver(
        self,
        channel_name: str,
        target: str,
        message: str,
        session_id: str | None = None,
    ) -> None:
        """Deliver a complete message to a channel target.

        Used by cron jobs and other non-interactive output delivery.
        """
        channel = self._channels.get(channel_name)
        if not channel:
            logger.warning("Cannot deliver to unknown channel: %s", channel_name)
            return

        formatted = channel.format_response(message)
        await channel.send(OutboundMessage(
            target=target,
            text=formatted,
            session_id=session_id or "",
        ))

    # ------------------------------------------------------------------ #
    #  Streaming adapter lifecycle                                         #
    # ------------------------------------------------------------------ #

    async def _setup_streaming(
        self,
        channel: BaseChannel,
        target: str,
        session_id: str,
    ) -> StreamAdapter:
        """Create and register a streaming adapter for a channel response."""
        adapter = StreamAdapter(channel, target, session_id)
        await adapter.initialize()

        listener_id = f"{channel.name}:{target}"
        await broadcaster.register(session_id, listener_id, adapter.on_event)

        self._adapters[(channel.name, target)] = adapter
        return adapter

    async def _teardown_streaming(
        self,
        channel_name: str,
        target: str,
        session_id: str,
    ) -> None:
        """Unregister a streaming adapter after the agent run completes."""
        listener_id = f"{channel_name}:{target}"
        await broadcaster.unregister(session_id, listener_id)
        self._adapters.pop((channel_name, target), None)
