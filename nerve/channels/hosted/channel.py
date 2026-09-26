"""The hosted channel: contract events into the router and the source inbox.

A :class:`HostedChannel` is registered under its provider's name, such as
``slack``, so the router, notifications, and tools see the same channel name
as with the self-hosted transport. It consumes normalized contract events; it
does not rebuild provider payloads.

Invoke events start agent turns with the self-hosted session keys:
``<provider>:<conversation>`` for a direct conversation, and
``<provider>:<conversation>:<thread>`` elsewhere, where a top-level message
roots its own thread. The gateway decides admission, and this channel keeps
its own addressed-message check as a second layer: outside a direct
conversation, a message must mention the agent or continue a thread that
already has a session.

Observe events go to the source inbox through ``router.observe`` under the
provider's local source grant (for Slack, ``slack.source``). The display
names in events are untrusted, so they can match only a deny rule.

Outbound operations are not implemented here yet. :meth:`send` drops the
message with a warning, and :attr:`is_available` is false, so notifications
and addressed delivery skip this channel.
"""

from __future__ import annotations

import asyncio
import collections
import logging
import uuid
from datetime import timezone
from typing import TYPE_CHECKING, Any, Callable

from nerve.channels.access import Identity, PatternGate, needs_name_resolution
from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    InboundMessage,
    ObservedMessage,
    OutboundMessage,
)
from nerve.channels.hosted.contract import Capabilities, ContentPart, Event
from nerve.channels.hosted.intake import Disposition
from nerve.channels.observation import ObservationPolicy

if TYPE_CHECKING:
    from nerve.channels.router import ChannelRouter
    from nerve.config import ChannelSourceConfig, NerveConfig

logger = logging.getLogger(__name__)

# Dispatch tasks that may run at once. The inbox reader stops reading at this
# limit instead of reading rows it cannot take.
MAX_INFLIGHT = 100
_MESSAGE_CACHE_MAX = 200
_HANDLED_CACHE_MAX = 500
_CONNECTION_CACHE_MAX = 1000
_SNIPPET_CHARS = 200


def render_content(parts: tuple[ContentPart, ...], self_id: str | None = None) -> str:
    """Turn contract content into prompt text.

    A mention of the agent itself is left out, as the self-hosted channel
    strips its own mention. Other mentions become ``@name`` or ``#name``, and
    a link with a label becomes ``label (url)``. Action elements are not text.
    """
    out: list[str] = []
    for part in parts:
        if part.text is not None:
            out.append(part.text.body)
        elif part.reference is not None:
            reference = part.reference
            if reference.kind == "mention":
                if reference.mention_kind == "user" and self_id and reference.id == self_id:
                    continue
                prefix = "#" if reference.mention_kind == "conversation" else "@"
                out.append(prefix + (reference.label or reference.id).lstrip("@#"))
            elif reference.kind == "link":
                if reference.label and reference.label != reference.url:
                    out.append(f"{reference.label} ({reference.url})")
                else:
                    out.append(reference.url)
            else:
                out.append(reference.label)
    return "".join(out).strip()


def describe_attachments(event: Event) -> str:
    """Name an event's attachments without reading them."""
    return " ".join(
        f"[File: {attachment.name or 'unnamed'}]" for attachment in event.attachments
    )


def mentions(event: Event, author_id: str) -> bool:
    return any(
        part.reference is not None
        and part.reference.kind == "mention"
        and part.reference.mention_kind == "user"
        and part.reference.id == author_id
        for part in event.content
    )


class HostedChannel(BaseChannel):
    """One provider's traffic through the gateway, under the provider's name.

    The router holds one channel for each name, so one channel takes the
    events of every connection of its provider. It records the connection
    that each conversation arrived on, which is where a reply must go.
    """

    def __init__(
        self,
        provider: str,
        router: ChannelRouter,
        config: Callable[[], NerveConfig],
        *,
        max_inflight: int = MAX_INFLIGHT,
        is_id: Callable[[str], bool] | None = None,
    ) -> None:
        self._provider = provider
        self.router = router
        self._config = config
        self._max_inflight = max_inflight
        self._is_id = is_id
        self._running = False
        # conversation -> the connection it last arrived on.
        self._connections: collections.OrderedDict[str, uuid.UUID] = collections.OrderedDict()
        # connection -> the agent's own author ID, from capabilities.
        self._self_ids: dict[uuid.UUID, str] = {}
        self._inflight: set[asyncio.Task] = set()
        # (conversation, message) -> (target, text snippet), for reactions.
        self._message_cache: collections.OrderedDict[tuple[str, str], tuple[str, str]] = (
            collections.OrderedDict()
        )
        # Message identities accepted for a turn, so the observe copy of the
        # same message can be left out of the source inbox.
        self._handled: collections.OrderedDict[tuple[uuid.UUID, str, str], None] = (
            collections.OrderedDict()
        )

    # ------------------------------------------------------------------ #
    #  BaseChannel                                                         #
    # ------------------------------------------------------------------ #

    @property
    def name(self) -> str:
        return self._provider

    @property
    def capabilities(self) -> ChannelCapability:
        return ChannelCapability.SEND_TEXT | ChannelCapability.MARKDOWN

    @property
    def is_available(self) -> bool:
        """Whether external delivery may use this channel."""
        return False

    async def start(self) -> None:
        self._running = True

    async def stop(self, *, drain: bool = False) -> None:
        """Stop dispatching, cancelling turns that are still starting."""
        self._running = False
        inflight = list(self._inflight)
        if drain and inflight:
            await asyncio.wait(inflight, timeout=30.0)
        for task in inflight:
            task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)

    async def send(self, message: OutboundMessage) -> None:
        logger.warning(
            "Hosted %s channel cannot send yet; a reply to %s was not delivered",
            self._provider, message.target,
        )

    # ------------------------------------------------------------------ #
    #  Connection knowledge                                                #
    # ------------------------------------------------------------------ #

    def note_capabilities(self, connection_id: uuid.UUID, capabilities: Capabilities) -> None:
        """Remember the agent's own identity on *connection_id*.

        Capabilities expire with their stream for operations, but the
        provider identity of the agent does not change, so it is kept.
        """
        self._self_ids[connection_id] = capabilities.self.id

    def self_id(self, connection_id: uuid.UUID) -> str | None:
        return self._self_ids.get(connection_id)

    def connection_for(self, conversation_id: str) -> uuid.UUID | None:
        """The connection that the conversation's latest accepted event came on."""
        return self._connections.get(conversation_id)

    def remember_message(self, conversation_id: str, message_id: str, target: str, text: str) -> None:
        """Keep a message's target and a text snippet, for reactions to it."""
        snippet = (text or "")[:_SNIPPET_CHARS]
        if not snippet:
            return
        key = (conversation_id, message_id)
        self._message_cache[key] = (target, snippet)
        self._message_cache.move_to_end(key)
        while len(self._message_cache) > _MESSAGE_CACHE_MAX:
            self._message_cache.popitem(last=False)

    # ------------------------------------------------------------------ #
    #  Intake                                                              #
    # ------------------------------------------------------------------ #

    def can_accept(self) -> bool:
        """Whether the channel runs, is still switched on, and has room for a turn.

        The provider section is read per call, so turning the provider off
        by a reload stops intake until it is turned on again.
        """
        section = getattr(self._config(), self._provider, None)
        switched_on = bool(
            section is not None and section.enabled and getattr(section, "mode", "") == "hosted",
        )
        return self._running and switched_on and len(self._inflight) < self._max_inflight

    async def deliver(self, event: Event) -> Disposition:
        """Dispatch one admitted event and say what happened to it."""
        purpose = event.admission.purpose
        if purpose == "invoke" and event.kind == "message":
            disposition = await self._invoke_message(event)
        elif purpose == "invoke" and event.kind == "reaction_added":
            disposition = await self._invoke_reaction(event)
        elif purpose == "invoke":
            return Disposition.rejected("admission_rejected", f"{event.kind} does not start a turn")
        elif event.kind == "message":
            disposition = await self._observe_message(event)
        else:
            return Disposition.rejected("admission_rejected", f"{event.kind} is not collected")
        if disposition.outcome == "accepted":
            self._note_connection(event)
        return disposition

    def _note_connection(self, event: Event) -> None:
        key = event.conversation.id
        self._connections[key] = event.connection_id
        self._connections.move_to_end(key)
        while len(self._connections) > _CONNECTION_CACHE_MAX:
            self._connections.popitem(last=False)

    def _target(self, event: Event) -> str:
        conversation = event.conversation
        if conversation.kind == "direct":
            return conversation.id
        thread_id = event.thread.id if event.thread is not None else event.message.id
        return f"{conversation.id}:{thread_id}"

    async def _addressed(self, event: Event, channel_key: str) -> Disposition | None:
        """``None`` when the message is addressed to the agent, else a disposition.

        A direct message always is. Elsewhere the message must mention the
        agent, or continue a thread that already has a session. Without the
        agent's identity for the connection, a mention cannot be ruled out,
        so the row waits for capabilities.
        """
        if event.conversation.kind == "direct":
            return None
        self_id = self.self_id(event.connection_id)
        if self_id is not None and mentions(event, self_id):
            return None
        reply = event.thread is not None and event.thread.id != event.message.id
        if reply and await self.router.get_last_session(channel_key):
            return None
        if self_id is None:
            return Disposition.deferred(
                f"no {self._provider} capabilities for connection {event.connection_id}",
                until_capabilities=True,
            )
        return Disposition.rejected(
            "admission_rejected", "the message neither mentions the agent nor continues its thread",
        )

    async def _invoke_message(self, event: Event) -> Disposition:
        target = self._target(event)
        channel_key = f"{self._provider}:{target}"
        refusal = await self._addressed(event, channel_key)
        if refusal is not None:
            return refusal

        text = render_content(event.content, self.self_id(event.connection_id))
        attachments = describe_attachments(event)
        if attachments:
            text = f"{attachments}\n\n{text}" if text else attachments
        if not text:
            return Disposition.rejected("admission_rejected", "the message carries no text")

        self.remember_message(event.conversation.id, event.message.id, target, text)
        self._note_handled(event)
        metadata: dict[str, Any] = {
            "message_id": event.message.id,
            "author_id": event.author.id,
            "connection_id": str(event.connection_id),
            "principal_id": str(event.admission.invoke.resolved_principal_id),
        }
        logger.info(
            "Hosted %s message in %s: %s", self._provider, target,
            text[:80] + ("..." if len(text) > 80 else ""),
        )
        return self._dispatch(InboundMessage(
            channel_name=self._provider,
            channel_key=channel_key,
            sender_id=target,
            text=text,
            metadata=metadata,
        ))

    async def _invoke_reaction(self, event: Event) -> Disposition:
        cached = self._message_cache.get((event.conversation.id, event.message.id))
        if cached is None:
            return Disposition.rejected("not_found", "the reaction is on a message without context")
        target, snippet = cached
        if event.conversation.kind != "direct" and ":" not in target:
            return Disposition.rejected("admission_rejected", "the reacted message has no thread")
        name = event.reaction.name
        label = f":{name.removeprefix('custom:')}:" if name.startswith("custom:") else name
        return self._dispatch(InboundMessage(
            channel_name=self._provider,
            channel_key=f"{self._provider}:{target}",
            sender_id=target,
            text=f'[Reaction: {label} on message: "{snippet}"]',
            metadata={},
        ))

    def _dispatch(self, message: InboundMessage) -> Disposition:
        """Start the turn as a task. Acceptance does not wait for the turn."""
        if not self.can_accept():
            return Disposition.deferred(f"{self._provider} has {len(self._inflight)} turns starting")
        task = asyncio.create_task(self._run_turn(message))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)
        return Disposition.accepted()

    async def _run_turn(self, message: InboundMessage) -> None:
        try:
            await self.router.handle_message(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - one turn must not stop intake
            logger.error(
                "Agent error for hosted %s target %s: %s",
                self._provider, message.sender_id, error, exc_info=True,
            )

    def _note_handled(self, event: Event) -> None:
        key = (event.connection_id, event.conversation.id, event.message.id)
        self._handled[key] = None
        self._handled.move_to_end(key)
        while len(self._handled) > _HANDLED_CACHE_MAX:
            self._handled.popitem(last=False)

    # ------------------------------------------------------------------ #
    #  Observation                                                         #
    # ------------------------------------------------------------------ #

    def _source(self) -> ChannelSourceConfig | None:
        section = getattr(self._config(), self._provider, None)
        return getattr(section, "source", None)

    def _identity(self, id_: str, display_name: str, gate: PatternGate) -> Identity:
        """An identity whose display name can deny but never grant.

        A name-based rule can only be checked against a display name, so an
        identity without one cannot clear a deny list that names people or
        rooms by name.
        """
        names_needed = needs_name_resolution(
            PatternGate(gate.label, deny=gate.deny), is_id=self._is_id,
        )
        return Identity(
            id=id_,
            self_set_names=(display_name,) if display_name else (),
            complete=bool(display_name) or not names_needed,
        )

    async def _observe_message(self, event: Event) -> Disposition:
        source = self._source()
        if source is None:
            return Disposition.rejected("policy_denied", "the channel has no source settings")
        if event.conversation.kind != "channel":
            return Disposition.rejected("policy_denied", "private conversations are never collected")
        handled = (event.connection_id, event.conversation.id, event.message.id) in self._handled
        if handled and not source.include_handled_messages:
            return Disposition.rejected("policy_denied", "the message started a turn")
        policy = ObservationPolicy(
            enabled=source.enabled,
            conversations=PatternGate(
                "conversation",
                allow=list(source.allow_conversations),
                deny=list(source.deny_conversations),
            ),
            senders=PatternGate(
                "sender",
                allow=list(source.allow_senders),
                deny=list(source.deny_senders),
            ),
        )
        author = event.author
        verdict = policy.check(
            self._identity(
                event.conversation.id, event.conversation.display_name, policy.conversations,
            ),
            self._identity(
                author.id if author else "", author.display_name if author else "", policy.senders,
            ),
        )
        if not verdict.allowed:
            return Disposition.rejected("policy_denied", verdict.reason)

        text = render_content(event.content, self.self_id(event.connection_id))
        attachments = describe_attachments(event)
        if attachments:
            text = f"{attachments}\n\n{text}" if text else attachments
        if not text:
            return Disposition.rejected("admission_rejected", "the message carries no text")

        thread_id = event.thread.id if event.thread is not None else ""
        observed = ObservedMessage(
            channel_name=self._provider,
            channel_key=f"{self._provider}:{self._target(event)}",
            conversation_id=event.conversation.id,
            sender_id=author.id if author else "",
            text=text,
            message_id=event.message.id,
            timestamp=event.occurred_at.astimezone(timezone.utc).isoformat(),
            conversation_title=event.conversation.display_name,
            sender_name=author.display_name if author else "",
            metadata={
                "thread_ts": thread_id,
                "connection_id": str(event.connection_id),
            },
        )
        config = self._config()
        stored = await self.router.observe(
            observed,
            ttl_days=config.sync.message_ttl_days,
            max_stored_messages=source.max_stored_messages,
        )
        if not stored:
            return Disposition.deferred("the observation could not be stored")
        return Disposition.accepted()


__all__ = ["HostedChannel", "describe_attachments", "mentions", "render_content"]
