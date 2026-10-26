"""Channel router — centralized session management and message dispatch.

Sits between channels and the agent engine. Channels send InboundMessages
to the router; the router resolves sessions, sets up streaming, runs the
agent, and tears down after completion.

Replaces the duplicated session management logic that was previously
spread across TelegramChannel and the WebSocket handler in gateway/server.py.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import sqlite3
import uuid
from dataclasses import asdict
from typing import Any, TYPE_CHECKING

from nerve.agent.interactive import get_handler
from nerve.agent.streaming import broadcaster
from nerve.channels.access import Decision
from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    InboundMessage,
    ObservedMessage,
    OutboundMessage,
    OutboundRefused,
)
from nerve.channels.stream_adapter import StreamAdapter
from nerve.db.observations import DEFAULT_MAX_ROWS
from nerve.gateway.auth import is_external_mode
from nerve.identity import ACTOR_KIND_HUMAN, UNATTRIBUTED, Actor, actor_id_of

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine

logger = logging.getLogger(__name__)


def turn_stop_note(result: dict[str, Any], session_id: str) -> str | None:
    """What a channel answers when ``/stop`` could not stop the turn, or
    ``None`` when it stopped one (``stopped`` / ``killed``) or nothing ran.

    ``/stop`` stops the turn only. A runtime that cannot spare background
    agents is never told to stop (``needs_confirmation``): the answer says
    so instead of "nothing running". A turn that did not end within the
    graceful wait while agents run is left running (``not_stopped``).
    """
    outcome = result.get("turn")
    if outcome == "needs_confirmation":
        n = len(result.get("would_end") or [])
        agents = "1 background agent" if n == 1 else f"{n} background agents"
        return (
            f"Couldn't stop `{session_id}` without stopping its {agents}. "
            f"Stop {'it' if n == 1 else 'them'} from the web UI."
        )
    if outcome == "not_stopped":
        return (
            f"`{session_id}` didn't stop within 5 s. Force stop it from the "
            "web UI; that also ends its background agents."
        )
    return None


def _turns_by_actor(
    pending: list[tuple[InboundMessage, asyncio.Future[str]]],
) -> list[list[tuple[InboundMessage, asyncio.Future[str]]]]:
    """Split pending messages into consecutive runs from one actor, in order.

    Messages without an actor count as one actor.
    """
    runs = itertools.groupby(pending, key=lambda item: actor_id_of(item[0].actor))
    return [list(run) for _, run in runs]


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

    def unregister(self, channel: BaseChannel) -> bool:
        """Remove *channel* only if it is still the registered instance."""
        if self._channels.get(channel.name) is not channel:
            return False
        del self._channels[channel.name]
        logger.info("Unregistered channel: %s", channel.name)
        return True

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

    @property
    def accepting_turns(self) -> bool:
        """False while the engine drains. handle_message then refuses the turn."""
        return self.engine.accepting_turns

    async def actor_for_principal(
        self, principal_id: uuid.UUID, display_name: str | None = None,
    ) -> Actor | None:
        """The person that a channel gateway resolved for an event, or ``None``.

        Only external mode has gateway principals: in local mode, the ID names
        no local person. Adds the ``actor_refs`` row when it is absent, before
        the turn writes anything that refers to it, and keeps the display name
        that a web request stored. *display_name*, the name that the event
        gives the person, is written only when the row has no name. The nil
        UUID, the system actor's ID, and a failed write give ``None``: the
        event then has no actor.
        """
        if not is_external_mode():
            return None
        if principal_id.int == 0:
            logger.warning("A channel event names no person, so it has no actor")
            return None
        actor_id = str(principal_id)
        try:
            stored_name = await self.engine.db.ensure_external_actor(actor_id, display_name)
        except sqlite3.IntegrityError:
            # A schema trigger refuses the system actor's ID.
            logger.warning(
                "A channel event names the system actor as its person, so it has no actor",
            )
            return None
        except Exception as e:
            logger.warning(
                "The person of a channel event was not resolved, so it has no actor: %s", e,
            )
            return None
        return Actor(
            actor_id=actor_id,
            kind=ACTOR_KIND_HUMAN,
            account_id=None,
            display_name=stored_name,
        )

    # Debounce window (seconds) for collecting simultaneous messages
    # (e.g. forwarded messages, rapid-fire sends) into a single batch.
    BATCH_DEBOUNCE = 0.60

    async def handle_message(self, msg: InboundMessage) -> str:
        """Process an inbound user message.

        Messages for the same session are always queued first.  A short
        debounce window collects simultaneous messages (e.g. forwarded
        messages or rapid-fire sends) so they are processed as a single
        batched turn.  Messages that arrive while the engine is busy are
        queued for the next batch.

        A turn runs as one actor. When a batch holds messages from more than
        one actor, each consecutive run of messages from one actor is a
        separate turn, in arrival order.

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
                msg.channel_key, source=msg.channel_name, actor=msg.actor,
            )

        # Store message context for reaction support
        self._note_message_context(session_id, msg)

        # Queue the message; a Future carries the result back to the caller.
        future: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        self._pending_batches.setdefault(session_id, []).append(
            (msg, future),
        )

        # If the session is already busy, the message is queued —
        # the driver coroutine will include it in the next batch.
        lock = self._session_locks.setdefault(session_id, asyncio.Lock())
        if lock.locked():
            return await future

        # We're the driver — acquire the lock and process batches.
        async with lock:
            # Short debounce to collect simultaneous messages.
            await asyncio.sleep(self.BATCH_DEBOUNCE)

            while pending := self._pending_batches.pop(session_id, None):
                for turn in _turns_by_actor(pending):
                    try:
                        if len(turn) == 1:
                            response = await self._run_single(
                                channel, turn[0][0], session_id,
                            )
                        else:
                            response = await self._run_batch(
                                channel, turn, session_id,
                            )
                    except asyncio.CancelledError:
                        for _, fut in pending:
                            if not fut.done():
                                fut.cancel()
                        self._cancel_pending(session_id)
                        raise
                    except Exception as exc:
                        for _, fut in turn:
                            if not fut.done():
                                fut.set_exception(exc)
                    else:
                        for _, fut in turn:
                            if not fut.done():
                                fut.set_result(response)

        return await future

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
        self._note_message_context(session_id, msg)

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
                # A self-hosted channel has no mapping from a provider user
                # to a person, so its messages have no actor. A hosted
                # channel sets the person that the channel gateway resolved.
                actor=msg.actor,
                origin=msg.channel_name,
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
        """Combine pending messages into one turn and run.

        Every message in *pending* has the same actor.
        """
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
        self._note_message_context(session_id, last_msg)

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
                actor=last_msg.actor,
                origin=last_msg.channel_name,
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

    def _note_message_context(self, session_id: str, msg: InboundMessage) -> None:
        """Make *msg* the message that the session's reactions go to."""
        msg_id = msg.metadata.get("message_id") if msg.metadata else None
        if msg_id is not None:
            self._message_context[session_id] = {
                "channel_name": msg.channel_name,
                "target": msg.sender_id,
                "message_id": msg_id,
            }

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
    #  Stickers                                                             #
    # ------------------------------------------------------------------ #

    async def send_sticker(self, session_id: str, sticker: str) -> bool:
        """Send a sticker to the chat associated with a session.

        Returns True if the sticker was sent, False if no context or
        the channel does not support stickers.
        """
        ctx = self._message_context.get(session_id)
        if not ctx:
            return False

        channel = self._channels.get(ctx["channel_name"])
        if not channel or not hasattr(channel, "send_sticker"):
            return False

        await channel.send_sticker(ctx["target"], sticker)
        return True

    # ------------------------------------------------------------------ #
    #  File delivery                                                        #
    # ------------------------------------------------------------------ #

    async def send_file(
        self,
        session_id: str,
        file_path: str,
        channel: str | None = None,
    ) -> bool:
        """Deliver a file via the channel passed by the caller.

        Refuses delivery if ``channel`` is None — falling back to
        ``_message_context`` would leak files from cron/planner runs
        to whatever chat last touched the session (e.g. a stale
        Telegram inbound from hours earlier).

        ``_message_context`` is consulted only to look up the target
        identifier when its cached channel matches the requested
        ``channel``; otherwise the target is empty. Channels needing
        a real target (Telegram) fail safely inside their own
        ``send_file``; broadcast channels (web) succeed regardless.

        Returns True on successful delivery, False otherwise.
        """
        if channel is None:
            return False

        chan_obj = self._channels.get(channel)
        if not chan_obj or ChannelCapability.SEND_FILES not in chan_obj.capabilities:
            return False

        ctx = self._message_context.get(session_id)
        target = ctx["target"] if ctx and ctx.get("channel_name") == channel else ""
        return await chan_obj.send_file(target, file_path)

    # ------------------------------------------------------------------ #
    #  Observation buffer                                                    #
    # ------------------------------------------------------------------ #

    async def observe(
        self,
        msg: ObservedMessage,
        ttl_days: int = 7,
        max_stored_messages: int = DEFAULT_MAX_ROWS,
    ) -> bool:
        """Buffer a message a channel collected for the source inbox.

        Channels reach the database through the router, never through the
        engine directly, so this is the seam. Returns True if the record was
        buffered.

        A failure is swallowed and logged. This sits on a channel's dispatch
        path, ahead of any live handling, so a database hiccup must not take
        down message handling for the sake of a buffered copy.
        """
        db = getattr(self.engine, "db", None)
        if db is None:
            return False
        try:
            await db.insert_channel_observation(
                channel=msg.channel_name,
                channel_key=msg.channel_key,
                payload=asdict(msg),
                ttl_days=ttl_days,
                max_rows=max_stored_messages,
            )
            return True
        except Exception as e:
            logger.warning(
                "Failed to buffer an observation from %s: %s", msg.channel_name, e,
            )
            return False

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
        """Get or create the active session for an unidentified channel user."""
        return await self.engine.sessions.get_active_session(
            channel_key, source=source, actor=None,
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
            actor=None,
        )
        await self.engine.sessions.set_active_session(
            channel_key, session_id,
        )
        return session_id

    async def list_sessions(self, limit: int = 20) -> list[dict[str, Any]]:
        """List sessions, most recently updated first."""
        return await self.engine.sessions.list_sessions(limit=limit)

    async def list_interactive_sessions(
        self, limit: int, offset: int = 0, current_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Switchable sessions for the /sessions keyboard: interactive sources
        only, current → starred → recent, paginated. See
        :meth:`nerve.agent.sessions.SessionManager.list_interactive_sessions`.
        """
        return await self.engine.sessions.list_interactive_sessions(
            limit=limit, offset=offset, current_id=current_id,
        )

    async def list_conversation_sessions(
        self, channel_key: str, limit: int = 20,
    ) -> list[dict[str, Any]]:
        """List live mappings for one exact conversation and its children."""
        return await self.engine.db.list_channel_sessions_for_conversation(
            channel_key,
            limit=limit,
            exclude_statuses=("archived", "stopped"),
        )

    # Channel commands come from a channel user that names no person, so
    # the session changes that they make are unattributed.

    async def stop_session(self, session_id: str) -> bool:
        """Stop a session through the engine lifecycle boundary."""
        return await self.engine.stop_session(session_id, attribution=UNATTRIBUTED)

    async def stop_turn(self, session_id: str) -> dict[str, Any]:
        """Stop the session's turn (background agents keep running) and
        return the engine's ``StopTurnResult``, so ``/stop`` can say why a
        turn was left running (see :func:`turn_stop_note`)."""
        return await self.engine.stop_turn(session_id, attribution=UNATTRIBUTED)

    async def set_session_starred(self, session_id: str, starred: bool) -> bool:
        """Star/unstar a session. Starred sessions are never auto-archived."""
        return await self.engine.sessions.set_starred(
            session_id, starred, attribution=UNATTRIBUTED,
        )

    async def toggle_session_starred(self, session_id: str) -> bool:
        """Toggle a session's starred state."""
        return await self.engine.sessions.toggle_starred(
            session_id, attribution=UNATTRIBUTED,
        )

    async def get_session(self, session_id: str) -> dict[str, Any] | None:
        """Fetch a session row (title/status/…), or None if it is gone."""
        return await self.engine.db.get_session(session_id)

    async def get_session_messages(
        self, session_id: str, limit: int,
    ) -> list[dict[str, Any]]:
        """Recent messages for a session, oldest→newest (native chat order)."""
        return await self.engine.db.get_messages(session_id, limit=limit)

    async def count_session_messages(self, session_id: str) -> int:
        """Total message count for a session (to know if more history exists)."""
        return await self.engine.db.count_messages(session_id)

    async def list_plans(
        self, status: str | None = None, limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List plans (optionally filtered by status), newest first."""
        return await self.engine.db.list_plans(status=status, limit=limit)

    async def get_plan(self, plan_id: str) -> dict[str, Any] | None:
        """Fetch a plan row (joined with its task title), or None if gone."""
        return await self.engine.db.get_plan(plan_id)

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

        Used by cron jobs and other non-interactive output delivery, which
        have no return value to read — see :meth:`deliver_addressed` for the
        same send with the refusal reason attached.
        """
        await self.deliver_addressed(channel_name, target, message, session_id)

    async def deliver_addressed(
        self,
        channel_name: str,
        target: str,
        message: str,
        session_id: str | None = None,
    ) -> Decision:
        """Deliver to a target the caller names, reporting why if refused.

        The target is always supplied by the caller and never inferred from
        ``_message_context``, so a cron run cannot spill into whatever chat
        last touched the session — the same reasoning as :meth:`send_file`.

        The channel authorizes the destination first, because only it knows
        what a target means; see :meth:`BaseChannel.authorize_outbound`.
        Returning the :class:`~nerve.channels.access.Decision` lets a caller
        say why nothing was sent instead of only that nothing was.

        A transport failure propagates rather than becoming a refusal: "the
        policy said no" and "Slack was down" are different answers. A channel
        whose policy is checked at delivery raises
        :class:`~nerve.channels.base.OutboundRefused`, which becomes a refusal.
        """
        channel = self._channels.get(channel_name)
        if not channel:
            logger.warning("Cannot deliver to unknown channel: %s", channel_name)
            return Decision(False, f"unknown channel {channel_name!r}")

        verdict = await channel.authorize_outbound(target)
        if not verdict.allowed:
            logger.info(
                "Refused addressed delivery to %s: %s", channel_name, verdict.reason,
            )
            return verdict

        formatted = channel.format_response(message)
        try:
            await channel.send(OutboundMessage(
                target=target,
                text=formatted,
                session_id=session_id or "",
            ))
        except OutboundRefused as refusal:
            logger.info(
                "Refused addressed delivery to %s at delivery: %s", channel_name, refusal.reason,
            )
            return Decision(False, refusal.reason)
        return verdict

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
