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
        # Per-session inbound message context (for reaction support)
        # Maps session_id -> {channel_name, target, message_id}
        self._message_context: dict[str, dict[str, Any]] = {}
        # Per-session locks and pending queues for message batching.
        # Messages arriving while a session is busy are queued and
        # processed as a single combined turn once the current run ends.
        self._session_locks: dict[str, asyncio.Lock] = {}
        self._pending_batches: dict[
            str, list[tuple[InboundMessage, asyncio.Future[str]]]
        ] = {}

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

        Messages for the same session are serialized via a per-session lock.
        If a session is already busy, arriving messages are queued and
        processed together as a single batched turn once the current run
        finishes.  This avoids "Session already running" errors and lets
        rapid-fire or forwarded messages be handled as a group.

        Called by channel implementations when they receive a user message.
        """
        channel = self._channels.get(msg.channel_name)
        if not channel:
            raise ValueError(f"Unknown channel: {msg.channel_name}")

        # Resolve session
        if msg.session_id:
            session_id = msg.session_id
            await self.engine.sessions.set_active_session(
                msg.channel_key, session_id,
            )
        else:
            session_id = await self.engine.sessions.get_active_session(
                msg.channel_key, source=msg.channel_name,
            )

        # Store message context for reaction support
        msg_id = msg.metadata.get("message_id") if msg.metadata else None
        if msg_id is not None:
            self._message_context[session_id] = {
                "channel_name": msg.channel_name,
                "target": msg.sender_id,
                "message_id": msg_id,
            }

        # If the session is busy, queue for batch processing instead of
        # setting up a streaming adapter that would never be used.
        lock = self._session_locks.setdefault(session_id, asyncio.Lock())
        if lock.locked():
            future: asyncio.Future[str] = asyncio.get_running_loop().create_future()
            self._pending_batches.setdefault(session_id, []).append(
                (msg, future),
            )
            return await future

        async with lock:
            try:
                response = await self._run_single(channel, msg, session_id)
            except (asyncio.CancelledError, Exception):
                # Current run failed/cancelled — cancel all pending futures
                self._cancel_pending(session_id)
                raise

            # Drain messages that arrived while we were busy
            while pending := self._pending_batches.pop(session_id, None):
                try:
                    batch_response = await self._run_batch(
                        channel, pending, session_id,
                    )
                except asyncio.CancelledError:
                    for _, fut in pending:
                        if not fut.done():
                            fut.cancel()
                    self._cancel_pending(session_id)
                    raise
                except Exception as exc:
                    for _, fut in pending:
                        if not fut.done():
                            fut.set_exception(exc)
                else:
                    for _, fut in pending:
                        if not fut.done():
                            fut.set_result(batch_response)

            return response

    # ------------------------------------------------------------------ #
    #  Internal run helpers                                                #
    # ------------------------------------------------------------------ #

    async def _run_single(
        self,
        channel: BaseChannel,
        msg: InboundMessage,
        session_id: str,
    ) -> str:
        """Run the engine for a single message with streaming."""
        if ChannelCapability.TYPING_INDICATOR in channel.capabilities:
            try:
                await channel.send_typing(msg.sender_id)
            except Exception as e:
                logger.debug(
                    "Typing indicator failed for %s: %s", msg.channel_name, e,
                )

        adapter = await self._setup_streaming(
            channel, msg.sender_id, session_id,
        )
        images = msg.metadata.get("images") if msg.metadata else None

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
            return await task
        except asyncio.CancelledError:
            if task.done() and not task.cancelled():
                return task.result()
            return ""
        finally:
            await self._teardown_streaming(
                channel.name, msg.sender_id, session_id,
            )

    async def _run_batch(
        self,
        channel: BaseChannel,
        pending: list[tuple[InboundMessage, asyncio.Future[str]]],
        session_id: str,
    ) -> str:
        """Combine pending messages into one turn and run."""
        last_msg = pending[-1][0]
        sender_id = last_msg.sender_id

        # Combine texts (each message on its own line)
        combined_text = "\n\n".join(m.text for m, _ in pending)

        # Combine images from all messages
        all_images: list[dict[str, Any]] = []
        for m, _ in pending:
            imgs = m.metadata.get("images") if m.metadata else None
            if imgs:
                all_images.extend(imgs)

        # Update reaction context to the last message in the batch
        msg_id = last_msg.metadata.get("message_id") if last_msg.metadata else None
        if msg_id is not None:
            self._message_context[session_id] = {
                "channel_name": last_msg.channel_name,
                "target": sender_id,
                "message_id": msg_id,
            }

        if ChannelCapability.TYPING_INDICATOR in channel.capabilities:
            try:
                await channel.send_typing(sender_id)
            except Exception as e:
                logger.debug(
                    "Typing indicator failed for %s: %s",
                    last_msg.channel_name, e,
                )

        adapter = await self._setup_streaming(channel, sender_id, session_id)

        task = asyncio.create_task(
            self.engine.run(
                session_id=session_id,
                user_message=combined_text,
                source=last_msg.channel_name,
                channel=last_msg.channel_name,
                images=all_images or None,
            )
        )
        self.engine.register_task(session_id, task)
        try:
            return await task
        except asyncio.CancelledError:
            if task.done() and not task.cancelled():
                return task.result()
            return ""
        finally:
            await self._teardown_streaming(
                channel.name, sender_id, session_id,
            )

    def _cancel_pending(self, session_id: str) -> None:
        """Cancel all pending futures for a session."""
        for _, fut in self._pending_batches.pop(session_id, []):
            if not fut.done():
                fut.cancel()

    # ------------------------------------------------------------------ #
    #  Reactions                                                            #
    # ------------------------------------------------------------------ #

    async def set_reaction(self, session_id: str, emoji: str) -> bool:
        """Set a reaction on the last inbound message for a session.

        Returns True if the reaction was set, False if no context or
        the channel does not support reactions.
        """
        ctx = self._message_context.get(session_id)
        if not ctx:
            return False

        channel = self._channels.get(ctx["channel_name"])
        if not channel or ChannelCapability.REACTIONS not in channel.capabilities:
            return False

        await channel.set_reaction(ctx["target"], ctx["message_id"], emoji)
        return True

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
