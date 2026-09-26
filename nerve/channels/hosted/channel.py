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

Outbound messages are contract operations on the connection that the
conversation's latest accepted event came on, or on the only connection that
supports the operation. :meth:`send` posts CommonMark text in parts of at
most the connection's advisory ``text_characters``. Streaming uses a
placeholder message, edits paced by ``edit_interval_millis``, and a delete.
The typing indicator is an ``eyes`` reaction on the message that started the
turn. The gateway decides where the agent may post: a ``forbidden`` result of
a send is a refusal, not a transport failure.

Files go out through ``file_send``. The attachments of a message that starts
a turn are read through ``file_read`` before the turn and use the
self-hosted extraction rules. A notification is a message with one button
for each answer. A button press arrives as an invoke ``interaction`` event:
it answers the notification when the card was delivered in that
conversation, and the ``interaction`` operation then replaces the card
without its buttons.
"""

from __future__ import annotations

import asyncio
import collections
import functools
import logging
import mimetypes
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Mapping

from nerve.channels.access import Decision, Identity, PatternGate, needs_name_resolution
from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    ChannelConstraints,
    InboundMessage,
    ObservedMessage,
    OutboundMessage,
    OutboundRefused,
)
from nerve.channels.hosted.attachments import extract_attachments
from nerve.channels.hosted.contract import Capabilities, ContentPart, Event
from nerve.channels.hosted.contract.model import (
    MAX_TRANSFER_BYTES,
    AttachmentReference,
    AttachmentTarget,
    ConnectionLimits,
    ContentReference,
    ConversationReference,
    DeleteOperation,
    EditOperation,
    FileReadOperation,
    FileSendOperation,
    FileUpload,
    InteractionOperation,
    InteractionTarget,
    MessageTarget,
    Reaction,
    ReactionOperation,
    SendOperation,
    TextContent,
)
from nerve.channels.hosted.intake import Disposition
from nerve.channels.hosted.operations import OperationFailed, OperationRunner
from nerve.channels.hosted.outbound import (
    container_for,
    file_name,
    message_target,
    notification_actions,
    notification_answer,
    parse_target,
    reaction_name,
    split_text,
    truncate_text,
    valid_identifier,
)
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
_TARGET_CACHE_MAX = 1000
_SNIPPET_CHARS = 200
# Used until a connection advertises its own limits.
DEFAULT_TEXT_CHARACTERS = 3900
DEFAULT_EDIT_INTERVAL_MILLIS = 1200
PLACEHOLDER_TEXT = "⏳"
TYPING_REACTION = "\U0001f440"
# What the agent learns when the gateway refuses a message. The gateway's
# reason is telemetry, so it is not passed on.
REFUSAL_REASON = "the channel gateway does not allow a message to this destination"
# Room kept in a settled card for the answer line.
_STATUS_ROOM = 256


def _remember(cache: collections.OrderedDict, key: Any, value: Any, maximum: int) -> None:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > maximum:
        cache.popitem(last=False)


def _markdown(text: str) -> tuple[ContentPart, ...]:
    return (ContentPart(kind="text", text=TextContent(format="markdown", body=text)),)


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


def _why(error: Exception) -> str:
    """The log text of an error, with a failed operation's reason and detail."""
    return getattr(error, "detailed", None) or str(error)


def _origin(event: Event) -> MessageTarget:
    """The full target of an event's message."""
    return MessageTarget(
        conversation=ConversationReference(id=event.conversation.id),
        thread=event.thread,
        topic=event.topic,
        message=event.message,
    )


def _snoozed_until(notification: dict[str, Any]) -> str | None:
    """The next delivery time of a snoozed notification, as local time text."""
    try:
        if notification.get("status") != "pending" or not notification.get("redeliver_at"):
            return None
        when = datetime.fromisoformat(notification["redeliver_at"])
        return when.astimezone().strftime("%Y-%m-%d %H:%M %Z")
    except Exception:  # noqa: BLE001 - only presentation depends on it
        return None


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
        operations: OperationRunner | None = None,
        reaction_names: Mapping[str, str] | None = None,
        notifications: Any | None = None,
        notification_target: Callable[[NerveConfig], str | None] | None = None,
        button_style: Callable[[str], str] | None = None,
    ) -> None:
        self._provider = provider
        self.router = router
        self._config = config
        self._max_inflight = max_inflight
        self._is_id = is_id
        self._operations = operations
        self._reaction_names = dict(reaction_names or {})
        # The notification service, the choice of the notification
        # conversation, and the style of an answer button.
        self._notifications = notifications
        self._notification_target = notification_target
        self._button_style = button_style or (lambda value: "")
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
        # (conversation, message) -> its connection and full target, for the
        # messages that started turns and the agent's own posts.
        self._targets: collections.OrderedDict[tuple[str, str], tuple[uuid.UUID, MessageTarget]] = (
            collections.OrderedDict()
        )
        # reply target -> (conversation, message) of its latest turn message.
        self._last_inbound: collections.OrderedDict[str, tuple[str, str]] = collections.OrderedDict()
        # Turn messages that already carry the typing reaction.
        self._typing_marked: collections.OrderedDict[tuple[str, str], None] = collections.OrderedDict()
        # (conversation, message) -> start time of its latest edit.
        self._last_edit: collections.OrderedDict[tuple[str, str], float] = collections.OrderedDict()
        self._editing: set[tuple[str, str]] = set()
        self._background: set[asyncio.Task] = set()
        # (conversation, message) -> the text of a notification card.
        self._notification_texts: collections.OrderedDict[tuple[str, str], str] = (
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
        capabilities = (
            ChannelCapability.SEND_TEXT
            | ChannelCapability.MARKDOWN
            | ChannelCapability.REACTIONS
            | ChannelCapability.TYPING_INDICATOR
            | ChannelCapability.SEND_FILES
        )
        section = getattr(self._config(), self._provider, None)
        if getattr(section, "stream_mode", "partial") == "partial":
            capabilities |= ChannelCapability.STREAMING
        return capabilities

    @property
    def constraints(self) -> ChannelConstraints:
        """The strictest limits of the connections that can send now.

        Messages count as editable unless a connection that can send lacks
        ``edit`` or ``delete``, so a streamed reply never leaves a
        placeholder that it cannot replace.
        """
        limits = self._send_limits()
        editable = True
        if self._operations is not None:
            streams = self._operations.streams
            senders = streams.connections("send")
            editable = senders <= streams.connections("edit") & streams.connections("delete")
        return ChannelConstraints(
            max_message_length=min(
                (item.text_characters for item in limits), default=DEFAULT_TEXT_CHARACTERS,
            ),
            min_edit_interval=max(
                (item.edit_interval_millis for item in limits), default=DEFAULT_EDIT_INTERVAL_MILLIS,
            ) / 1000,
            supports_message_edit=editable,
        )

    @property
    def is_available(self) -> bool:
        """Whether a stream advertises ``send`` for some connection now."""
        return self._operations is not None and bool(self._operations.streams.connections("send"))

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
        background = list(self._background)
        for task in background:
            task.cancel()
        if background:
            await asyncio.gather(*background, return_exceptions=True)

    async def send(self, message: OutboundMessage) -> None:
        """Post *message* in parts that fit the connection's text limit.

        Raises :class:`OutboundRefused` when the gateway forbids the first
        part, and :class:`OperationFailed` for other failures. A failed part
        stops the message; parts before it stay posted. A part whose frame
        is too large for the stream is split again, as nothing was sent.
        """
        conversation, thread = parse_target(message.target)
        connection = await self._connection(conversation, "send")
        parts = split_text(message.text, self._limits(connection).text_characters)
        posted = 0
        while parts:
            part = parts.pop(0)
            try:
                await self._post(
                    connection, conversation, thread, message.target, part, refusal=posted == 0,
                )
            except OperationFailed as error:
                if error.local and error.reason_code == "content_too_large" and len(part) > 1:
                    parts[:0] = split_text(part, len(part) // 2)
                    continue
                raise
            posted += 1

    # ------------------------------------------------------------------ #
    #  Outbound                                                            #
    # ------------------------------------------------------------------ #

    async def _connection(self, conversation_id: str, operation: str) -> uuid.UUID:
        """The connection for outbound work in *conversation_id*.

        That is the connection of the conversation's latest accepted event,
        else the only connection that advertises *operation*, which may take
        a short wait for a stream. Raises :class:`OperationFailed` when
        neither exists.
        """
        if not conversation_id:
            raise OperationFailed(operation, "unavailable", "invalid_target", local=True,
                                  detail="the target names no conversation")
        connection = self._connections.get(conversation_id)
        if connection is not None:
            return connection
        candidates = await self._operations.connections(operation) if self._operations else set()
        if len(candidates) == 1:
            return next(iter(candidates))
        raise OperationFailed(
            operation, "unavailable", "invalid_target" if candidates else "provider_unavailable",
            local=True,
            detail=f"no single {self._provider} connection serves conversation {conversation_id}",
        )

    def _limits(self, connection_id: uuid.UUID) -> ConnectionLimits:
        capabilities = (
            self._operations.streams.capabilities_for(connection_id) if self._operations else None
        )
        if capabilities is not None:
            return capabilities.limits
        return ConnectionLimits(
            text_characters=DEFAULT_TEXT_CHARACTERS,
            edit_interval_millis=DEFAULT_EDIT_INTERVAL_MILLIS,
        )

    def _send_limits(self) -> list[ConnectionLimits]:
        if self._operations is None:
            return []
        streams = self._operations.streams
        found = (streams.capabilities_for(connection) for connection in streams.connections("send"))
        return [capabilities.limits for capabilities in found if capabilities is not None]

    async def _perform(self, connection_id: uuid.UUID, kind: str, payload: Any, **options: Any):
        if self._operations is None:
            raise OperationFailed(kind, "unavailable", "provider_unavailable", local=True,
                                  detail="no gateway streams")
        return await self._operations.perform(connection_id, kind, payload, **options)

    async def _fitting(
        self, kind: str, body: str, attempt: Callable[[str], Awaitable[Any]],
    ) -> tuple[Any, str]:
        """Run *attempt* with *body*, and halve the text while its frame is too large.

        A frame that is too large is refused before it is sent, so a shorter
        attempt cannot repeat a side effect. The gateway refuses empty text,
        so an empty *body* fails before the send. Returns the result and the
        text that was sent.
        """
        if not body:
            raise OperationFailed(kind, "unavailable", "content_too_large", local=True, detail="the text is empty")
        while True:
            try:
                return await attempt(body), body
            except OperationFailed as error:
                if not (error.local and error.reason_code == "content_too_large" and len(body) > 1):
                    raise
                body = truncate_text(body, len(body) // 2)

    async def _post(
        self,
        connection_id: uuid.UUID,
        conversation_id: str,
        thread_id: str | None,
        reply_target: str,
        text: str,
        *,
        refusal: bool = True,
        **options: Any,
    ) -> MessageTarget:
        """Send one message and remember it.

        With *refusal*, a ``forbidden`` result raises :class:`OutboundRefused`.
        """
        try:
            result = await self._perform(connection_id, "send", SendOperation(
                destination=container_for(conversation_id, thread_id), content=_markdown(text),
            ), **options)
        except OperationFailed as error:
            if error.outcome == "forbidden" and refusal:
                logger.info(
                    "The gateway refused a %s message to %s (%s)",
                    self._provider, reply_target, error.reason_code or "no reason",
                )
                raise OutboundRefused(REFUSAL_REASON) from error
            raise
        target = result.target
        _remember(
            self._targets, (conversation_id, target.message.id), (connection_id, target),
            _TARGET_CACHE_MAX,
        )
        self.remember_message(conversation_id, target.message.id, reply_target, text)
        return target

    async def _locate(
        self, target: str, message_id: str, operation: str,
    ) -> tuple[uuid.UUID, MessageTarget]:
        """The connection and full target of a message in *target*."""
        conversation, thread = parse_target(target)
        known = self._targets.get((conversation, message_id))
        if known is not None:
            return known
        if not valid_identifier(message_id):
            raise OperationFailed(operation, "unavailable", "invalid_target", local=True,
                                  detail="the message ID is not valid")
        connection = await self._connection(conversation, operation)
        return connection, message_target(conversation, thread, message_id)

    async def send_placeholder(self, target: str, session_id: str) -> str | None:
        """Post the streaming placeholder and return its message ID, or ``None``."""
        try:
            conversation, thread = parse_target(target)
            connection = await self._connection(conversation, "send")
            posted = await self._post(
                connection, conversation, thread, target, PLACEHOLDER_TEXT,
                rate_limit_retries=0, stream_wait=0,
            )
        except (OperationFailed, OutboundRefused) as error:
            logger.warning("Hosted %s placeholder in %s failed: %s", self._provider, target, _why(error))
            return None
        return posted.message.id

    async def edit_message(
        self, target: str, message_id: str, text: str, *, throttle: bool = False,
    ) -> None:
        """Replace a message's text, paced by the connection's edit interval.

        A throttled edit is dropped while an edit of the message is in flight
        or its interval has not passed, when no stream has room for it now,
        and after ``rate_limited``. Any other edit waits for the interval.
        """
        try:
            connection, located = await self._locate(target, message_id, "edit")
        except OperationFailed as error:
            logger.warning("Hosted %s edit in %s skipped: %s", self._provider, target, _why(error))
            return
        limits = self._limits(connection)
        key = (located.conversation.id, message_id)
        interval = limits.edit_interval_millis / 1000
        loop = asyncio.get_running_loop()
        last = self._last_edit.get(key)
        if throttle and (key in self._editing or (last is not None and loop.time() - last < interval)):
            return
        if last is not None and loop.time() - last < interval:
            await asyncio.sleep(interval - (loop.time() - last))
        body = truncate_text(text, limits.text_characters)
        if not body.strip():
            return
        _remember(self._last_edit, key, loop.time(), _TARGET_CACHE_MAX)
        self._editing.add(key)
        options = {"rate_limit_retries": 0, "stream_wait": 0} if throttle else {}
        try:
            _, body = await self._fitting("edit", body, lambda text: self._perform(
                connection, "edit", EditOperation(target=located, content=_markdown(text)), **options,
            ))
        except OperationFailed as error:
            log = logger.debug if throttle else logger.warning
            log("Hosted %s edit in %s failed: %s", self._provider, target, _why(error))
            return
        finally:
            self._editing.discard(key)
        self.remember_message(located.conversation.id, message_id, target, body)

    async def delete_message(self, target: str, message_id: str) -> None:
        try:
            connection, located = await self._locate(target, message_id, "delete")
            await self._perform(connection, "delete", DeleteOperation(target=located))
        except OperationFailed as error:
            logger.warning("Hosted %s delete in %s failed: %s", self._provider, target, _why(error))
            return
        self._targets.pop((located.conversation.id, message_id), None)

    async def set_reaction(self, target: str, message_id: Any, emoji: str) -> None:
        """Add a reaction. A short name, such as ``eyes``, is mapped to its emoji."""
        name = reaction_name(emoji, self._reaction_names)
        if name is None:
            logger.info("Reaction %r is not an emoji or a short name; skipped", emoji)
            return
        await self._react(target, str(message_id), name)

    async def send_typing(self, target: str) -> None:
        """Mark the message that started the turn with an ``eyes`` reaction, once.

        The reaction is sent in the background, so the turn does not wait
        for the gateway.
        """
        latest = self._last_inbound.get(target)
        if latest is None or latest in self._typing_marked:
            return
        _remember(self._typing_marked, latest, None, _TARGET_CACHE_MAX)
        task = asyncio.create_task(
            self._react(target, latest[1], TYPING_REACTION, rate_limit_retries=0, stream_wait=0),
        )
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _react(self, target: str, message_id: str, name: str, **options: Any) -> None:
        try:
            connection, located = await self._locate(target, message_id, "reaction")
            await self._perform(connection, "reaction", ReactionOperation(
                target=located, reaction=Reaction(action="add", name=name),
            ), **options)
        except OperationFailed as error:
            logger.warning("Hosted %s reaction in %s failed: %s", self._provider, target, _why(error))

    async def send_file(self, target: str, file_path: str) -> bool:
        """Upload a file into *target* through ``file_send``.

        Returns ``False`` without a send when the file is missing, empty, or
        larger than the connection's ``file_bytes``, and ``False`` when the
        operation fails. A failed upload is not sent again. While no stream
        serves the connection, the stream that takes the upload checks its
        limit.
        """
        path = Path(file_path)
        conversation, thread = parse_target(target)
        try:
            connection = await self._connection(conversation, "file_send")
            capabilities = (
                self._operations.streams.capabilities_for(connection) if self._operations else None
            )
            limit = MAX_TRANSFER_BYTES
            if capabilities is not None:
                limit = min(capabilities.limits.file_bytes, limit)
            size = path.stat().st_size if path.is_file() else 0
            if not 0 < size <= limit:
                logger.info(
                    "Hosted %s file %s not sent: %d bytes, limit %d",
                    self._provider, path.name, size, limit,
                )
                return False
            data = await asyncio.to_thread(path.read_bytes)
            if not 0 < len(data) <= limit:
                return False
            await self._perform(connection, "file_send", FileSendOperation(
                destination=container_for(conversation, thread),
                file=FileUpload(
                    # The runner gives each request its own transfer ID.
                    transfer_id="upload",
                    name=file_name(path.name),
                    media_type=mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                    total_bytes=len(data),
                ),
            ), upload=data)
        except (OperationFailed, OSError) as error:
            logger.warning("Hosted %s file upload to %s failed: %s", self._provider, target, _why(error))
            return False
        return True

    async def post_notification(
        self, notification_id: str, text: str, options: list[tuple[str, str]] | None = None,
    ) -> tuple[str, str] | None:
        """Post a notification card: its text and one button for each option.

        The card goes to the same conversation as with the self-hosted
        channel. Returns ``(target, message_id)``, or ``None`` when there is
        no such conversation or the send fails.
        """
        config = self._config()
        target = self._notification_target(config) if self._notification_target else None
        if not target or not self.is_available:
            return None
        actions = notification_actions(notification_id, options, self._button_style)
        try:
            connection = await self._connection(target, "send")
            result, body = await self._fitting(
                "send", truncate_text(text, self._limits(connection).text_characters),
                lambda body: self._perform(connection, "send", SendOperation(
                    destination=container_for(target, None), content=_markdown(body) + actions,
                )),
            )
        except OperationFailed as error:
            logger.warning(
                "Hosted %s notification %s was not posted: %s", self._provider, notification_id, _why(error),
            )
            return None
        located = result.target
        message_id = located.message.id
        _remember(self._targets, (target, message_id), (connection, located), _TARGET_CACHE_MAX)
        _remember(self._notification_texts, (target, message_id), body, _TARGET_CACHE_MAX)
        self.remember_message(target, message_id, target, body)
        return target, message_id

    async def expire_notification(self, target: str, message_id: str, text: str) -> None:
        """Replace a notification card with *text* and no buttons."""
        try:
            connection, located = await self._locate(target, message_id, "edit")
            await self._fitting(
                "edit", truncate_text(text, self._limits(connection).text_characters),
                lambda body: self._perform(
                    connection, "edit", EditOperation(target=located, content=_markdown(body)),
                ),
            )
        except OperationFailed as error:
            logger.warning(
                "Hosted %s notification card %s was not expired: %s", self._provider, message_id, _why(error),
            )
        self._notification_texts.pop((parse_target(target)[0], message_id), None)

    async def authorize_outbound(self, target: str) -> Decision:
        """Check the local settings and the target's form; the gateway decides the rest.

        When ``allow_channels`` or ``deny_channels`` is set, the conversation
        ID must also pass them. Hosted events carry no trusted conversation
        names, so a name rule cannot grant here. The gateway enforces where
        the agent may post when it gets the message, so a destination that
        passes here can still be refused by :meth:`send`.
        """
        section = getattr(self._config(), self._provider, None)
        if not getattr(section, "allow_outbound", False):
            return Decision(
                False,
                f"{self._provider}.allow_outbound is not enabled, so the agent may not post "
                "to a conversation it names",
            )
        conversation, _ = parse_target(target)
        if not conversation:
            return Decision(False, f"no {self._provider} conversation ID in the target")
        if self._is_id is not None and not self._is_id(conversation):
            return Decision(False, f"target must be a {self._provider} conversation ID, not a name")
        allow = list(getattr(section, "allow_channels", None) or [])
        deny = list(getattr(section, "deny_channels", None) or [])
        if allow or deny:
            gate = PatternGate("conversation", allow=allow, deny=deny)
            verdict = gate.check(self._identity(conversation, "", gate))
            if not verdict.allowed:
                # The detail names the matching pattern, so it goes to the log only.
                logger.info("Hosted %s refused addressed delivery: %s", self._provider, verdict.reason)
                return Decision(
                    False, f"the destination is not approved by the {self._provider} channel policy",
                )
        if self._connections.get(conversation) is None and self._operations is not None:
            candidates = self._operations.streams.connections("send")
            if len(candidates) > 1:
                return Decision(
                    False,
                    f"several {self._provider} connections serve this agent, and none has "
                    "delivered a message from this conversation",
                )
        return Decision(True, "the channel gateway decides at delivery")

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
        elif purpose == "invoke" and event.kind == "interaction":
            disposition = await self._invoke_interaction(event)
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

        body = render_content(event.content, self.self_id(event.connection_id))
        attachments = describe_attachments(event)
        text = f"{attachments}\n\n{body}" if attachments and body else attachments or body
        if not text:
            return Disposition.rejected("admission_rejected", "the message carries no text")

        self.remember_message(event.conversation.id, event.message.id, target, text)
        self._note_turn_message(event, target)
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
        # The turn reads the attachments before it starts and puts them
        # before the text.
        return self._dispatch(InboundMessage(
            channel_name=self._provider,
            channel_key=channel_key,
            sender_id=target,
            text=body if event.attachments else text,
            metadata=metadata,
        ), event if event.attachments else None)

    async def _add_attachments(self, message: InboundMessage, event: Event) -> None:
        """Read the event's attachments into the message text and images."""
        context, blocks = await extract_attachments(
            event.attachments, functools.partial(self._read_attachment, event),
        )
        if context:
            message.text = f"{context}\n\n{message.text}" if message.text else context
        if blocks:
            message.metadata["images"] = blocks

    async def _read_attachment(
        self, event: Event, attachment: AttachmentReference, length: int,
    ) -> bytes | None:
        """Read up to *length* bytes of an attachment, or ``None`` on failure."""
        if self._operations is None:
            return None
        try:
            return await self._operations.read(event.connection_id, "file_read", FileReadOperation(
                target=AttachmentTarget(origin=_origin(event), attachment=attachment),
                offset_bytes=0,
                length_bytes=length,
            ))
        except OperationFailed as error:
            logger.warning(
                "Hosted %s attachment %s was not read: %s",
                self._provider, attachment.name or attachment.id, _why(error),
            )
            return None

    async def _invoke_interaction(self, event: Event) -> Disposition:
        """Answer a notification from a button press, then settle its card.

        The answer counts only in the conversation where the card was
        delivered. An answered card is replaced in the background, without
        buttons. A press that answers nothing gets a short notice, and the
        card stays as it is.
        """
        if not self.can_accept():
            return Disposition.deferred(f"{self._provider} takes no events now")
        interaction = event.interaction
        parsed = notification_answer(interaction.action_id, interaction.selections)
        if parsed is None:
            return Disposition.rejected("admission_rejected", "the interaction answers no notification")
        notification_id, answer = parsed
        actor = event.author.id if event.author is not None else ""
        key = (event.conversation.id, event.message.id)
        result: dict[str, Any] | None = None
        if self._notifications is not None:
            try:
                result = await self._notifications.answer_delivered_notification(
                    notification_id, answer,
                    channel=self._provider, target=event.conversation.id, actor=actor,
                )
            except Exception as error:  # noqa: BLE001 - the row is read again later
                logger.warning(
                    "Hosted %s answer to notification %s was not recorded: %s",
                    self._provider, notification_id, error,
                )
                return Disposition.deferred("the answer could not be recorded")
        if result:
            content = self._answered_card(event, key, answer, actor, result)
            self._notification_texts.pop(key, None)
            settle = self._settle_card(event, content)
        else:
            notice = "Already answered or expired." if self._notifications else "Service unavailable."
            settle = self._settle_card(event, _markdown(notice), response="acknowledge")
        task = asyncio.create_task(settle)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return Disposition.accepted()

    def _answered_card(
        self, event: Event, key: tuple[str, str], answer: str, actor: str, result: dict[str, Any],
    ) -> tuple[ContentPart, ...]:
        """The card text with its answer line, and who answered."""
        original = self._notification_texts.get(key) or render_content(event.content)
        snoozed = _snoozed_until(result)
        status = f"💤 Snoozed until {snoozed}, will resurface" if snoozed else f"✅ Answered: {answer}"
        limit = self._limits(event.connection_id).text_characters
        if original:
            original = truncate_text(original, max(1, limit - _STATUS_ROOM))
            status = f"{original}\n\n{status}"
        status = truncate_text(status, max(1, limit - _STATUS_ROOM // 2))
        if not actor:
            return _markdown(status)
        return _markdown(f"{status} (by ") + (
            ContentPart(kind="reference", reference=ContentReference(
                kind="mention", mention_kind="user", id=actor,
            )),
            ContentPart(kind="text", text=TextContent(format="markdown", body=")")),
        )

    async def _settle_card(
        self, event: Event, content: tuple[ContentPart, ...], response: str = "update",
    ) -> None:
        """Answer the press through the ``interaction`` operation.

        ``update`` replaces the card; ``acknowledge`` shows *content* as a
        short notice. When the provider no longer takes an update, the card
        is edited instead. An ambiguous update is not followed by an edit.
        """
        origin = _origin(event)
        try:
            await self._perform(event.connection_id, "interaction", InteractionOperation(
                target=InteractionTarget(origin=origin, interaction_id=event.interaction.id),
                response=response,
                content=content,
            ))
            return
        except OperationFailed as error:
            if error.outcome == "ambiguous" or response != "update":
                logger.warning("Hosted %s card %s failed: %s", self._provider, response, _why(error))
                return
            logger.info("Hosted %s card update failed (%s); editing the card", self._provider, _why(error))
        try:
            await self._perform(event.connection_id, "edit", EditOperation(target=origin, content=content))
        except OperationFailed as error:
            logger.warning("Hosted %s card edit failed: %s", self._provider, _why(error))

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

    def _dispatch(self, message: InboundMessage, attachments: Event | None = None) -> Disposition:
        """Start the turn as a task. Acceptance does not wait for the turn.

        With *attachments*, the task reads that event's attachments first.
        """
        if not self.can_accept():
            return Disposition.deferred(f"{self._provider} has {len(self._inflight)} turns starting")
        task = asyncio.create_task(self._run_turn(message, attachments))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)
        return Disposition.accepted()

    async def _run_turn(self, message: InboundMessage, attachments: Event | None = None) -> None:
        try:
            if attachments is not None:
                await self._add_attachments(message, attachments)
            await self.router.handle_message(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - one turn must not stop intake
            logger.error(
                "Agent error for hosted %s target %s: %s",
                self._provider, message.sender_id, error, exc_info=True,
            )

    def _note_turn_message(self, event: Event, target: str) -> None:
        """Keep where a turn's message is, for its reactions and typing mark."""
        key = (event.conversation.id, event.message.id)
        _remember(self._targets, key, (event.connection_id, _origin(event)), _TARGET_CACHE_MAX)
        _remember(self._last_inbound, target, key, _TARGET_CACHE_MAX)

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
