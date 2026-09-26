"""The records of version 1 of the channel contract that Nerve uses.

The gateway admits events, keeps them in the tenant and agent scope of the
stream, and checks every frame that Nerve sends. Nerve checks only what its
own code relies on: the frame kinds that it handles, the members that it
reads, the values that it branches on, and the size bounds that protect it.
A frame that fails a check closes its stream.
"""

from __future__ import annotations

import dataclasses
import uuid
from datetime import datetime, timezone
from typing import Any

from nerve.channels.hosted.contract.wire import (
    RejectionReason,
    decode_record,
    dumps,
    malformed,
    omit_empty,
    parse_json,
    reject,
    struct,
)

_record = dataclasses.dataclass(frozen=True)

PROTOCOL_VERSION = "1"
DELIVERY_MODE_PULL = "pull"
MAX_FRAME_BYTES = 256 * 1024
MIN_FRAME_BYTES = 16 * 1024
MAX_TRANSFER_BYTES = 16 * 1024 * 1024

NIL_UUID = uuid.UUID(int=0)
ZERO_TIME = datetime(1, 1, 1, tzinfo=timezone.utc)

# The frame kinds that the gateway sends and Nerve handles.
GATEWAY_FRAME_KINDS = frozenset({
    "nudge", "inbox_read_result", "inbox_ack_result", "connection_status",
    "capabilities", "heartbeat", "drain", "negotiation",
})
# The frame kinds whose envelope names a logical connection.
CONNECTION_SCOPED_KINDS = frozenset({"connection_status", "capabilities"})


def byte_length(value: str) -> int:
    """UTF-8 length of *value*; a string with a lone surrogate counts as too long."""
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError:
        return MAX_FRAME_BYTES + 1


def _one_of(name: str, value: str, allowed: tuple[str, ...]) -> None:
    if value not in allowed:
        reject(RejectionReason.KIND_UNSUPPORTED, f"unknown {name} {value!r}")


# ---------------------------------------------------------------------- #
#  References and content                                                 #
# ---------------------------------------------------------------------- #


@_record
class ConversationReference:
    id: str = ""
    kind: str = omit_empty()
    display_name: str = omit_empty()


@_record
class ThreadReference:
    id: str = ""


@_record
class MessageReference:
    id: str = ""


@_record
class AuthorReference:
    """Provider authorship. It grants no authority."""

    id: str = ""
    kind: str = ""
    display_name: str = omit_empty()


@_record
class AttachmentReference:
    name: str = omit_empty()


@_record
class TextContent:
    format: str = ""
    body: str = ""


@_record
class ContentReference:
    """A mention, a link that a person wrote, or unsupported provider content."""

    kind: str = ""
    mention_kind: str = omit_empty()
    id: str = omit_empty()
    url: str = omit_empty()
    label: str = omit_empty()


@_record
class ContentPart:
    """One part of a message. ``kind`` names the member that is set."""

    kind: str = ""
    text: TextContent | None = omit_empty(None)
    reference: ContentReference | None = omit_empty(None)


@_record
class Reaction:
    action: str = ""
    name: str = ""


# ---------------------------------------------------------------------- #
#  Events                                                                 #
# ---------------------------------------------------------------------- #


@_record
class InvokeAdmission:
    resolved_principal_id: uuid.UUID = NIL_UUID


@_record
class EventAdmission:
    """Why the gateway admitted an event: ``invoke`` or ``observe``."""

    purpose: str = ""
    invoke: InvokeAdmission | None = omit_empty(None)


@_record
class EventDelivery:
    """The inbox row that carries an event."""

    inbox_id: str = ""


@_record
class Event:
    """One normalized provider event.

    ``event_id`` is the logical event and the duplicate key.
    """

    event_id: str = ""
    connection_id: uuid.UUID = NIL_UUID
    provider: str = ""
    kind: str = ""
    conversation: ConversationReference = struct(ConversationReference)
    thread: ThreadReference | None = omit_empty(None)
    message: MessageReference | None = omit_empty(None)
    author: AuthorReference | None = omit_empty(None)
    admission: EventAdmission | None = omit_empty(None)
    delivery: EventDelivery | None = omit_empty(None)
    occurred_at: datetime = ZERO_TIME
    content: tuple[ContentPart, ...] = omit_empty(())
    attachments: tuple[AttachmentReference, ...] = omit_empty(())
    reaction: Reaction | None = omit_empty(None)

    def validate(self) -> None:
        """Check the members that intake and the hosted channel read."""
        if not self.event_id or self.connection_id == NIL_UUID or not self.conversation.id:
            malformed("event has no event ID, connection, or conversation")
        if self.delivery is None or not self.delivery.inbox_id:
            malformed("inbox event has no inbox ID")
        if self.admission is None:
            malformed("inbox event has no admission")
        _one_of("admission purpose", self.admission.purpose, ("invoke", "observe"))
        if self.admission.purpose == "invoke" and self.admission.invoke is None:
            malformed("invoke admission has no invoke member")
        if self.kind in ("message", "reaction_added") and (self.message is None or self.author is None):
            malformed(f"{self.kind} event has no message or author")
        if self.kind == "reaction_added" and self.reaction is None:
            malformed("reaction event has no reaction")


# ---------------------------------------------------------------------- #
#  Inbox                                                                  #
# ---------------------------------------------------------------------- #


@_record
class Nudge:
    """The inbox may have new rows. It carries no content and can be lost."""

    purpose: str = omit_empty()


@_record
class InboxRead:
    maximum_events: int = 0
    maximum_bytes: int = 0


@_record
class InboxReadResult:
    """One page of inbox rows in inbox order. An empty page has no events."""

    outcome: str = ""
    events: tuple[Event, ...] = omit_empty(())
    reason_code: str = omit_empty()

    def validate(self) -> None:
        _one_of("inbox outcome", self.outcome, ("succeeded", "unavailable"))
        for event in self.events:
            event.validate()


def validate_inbox_read_result(read: InboxRead, result: InboxReadResult, frame_bytes: int) -> None:
    """Check that a page fits the read it answers.

    ``frame_bytes`` is the wire length of the frame. A page of one event may
    exceed the read's byte limit, because the gateway cannot split an event.
    """
    if len(result.events) > read.maximum_events:
        reject(RejectionReason.LIMIT_EXCEEDED, "inbox page exceeds the read event limit")
    if len(result.events) > 1 and frame_bytes > read.maximum_bytes:
        reject(RejectionReason.LIMIT_EXCEEDED, "inbox page of several events exceeds the read byte limit")


@_record
class InboxAckItem:
    inbox_id: str = ""
    outcome: str = ""
    reason_code: str = omit_empty()


@_record
class InboxAck:
    items: tuple[InboxAckItem, ...] = ()


@_record
class InboxAckResult:
    outcome: str = ""
    reason_code: str = omit_empty()

    def validate(self) -> None:
        _one_of("inbox outcome", self.outcome, ("succeeded", "unavailable"))


# ---------------------------------------------------------------------- #
#  Stream management                                                      #
# ---------------------------------------------------------------------- #


@_record
class StreamLimits:
    """What one peer accepts from the other for the whole stream epoch."""

    frame_bytes: int = 0
    transfer_bytes: int = 0
    memory_bytes: int = 0
    in_flight_requests: int = 0


@_record
class ConnectionStatus:
    state: str = ""
    reason_code: str = omit_empty()


@_record
class Capabilities:
    """A connection's own identity."""

    self: AuthorReference = struct(AuthorReference)


@_record
class Heartbeat:
    """Stream liveness only. It has no activity or parking meaning."""

    sequence: int = 0
    sent_at: datetime = ZERO_TIME


@_record
class Drain:
    reason: str = ""
    initiated_at: datetime = ZERO_TIME
    deadline: datetime = ZERO_TIME


@_record
class Negotiation:
    """Versions, delivery modes, and receive limits of one peer."""

    preferred_version: str = ""
    supported_versions: tuple[str, ...] = ()
    delivery_modes: tuple[str, ...] = ()
    receive_limits: StreamLimits = struct(StreamLimits)

    def validate(self) -> None:
        if PROTOCOL_VERSION not in self.supported_versions:
            reject(RejectionReason.VERSION_UNSUPPORTED, "negotiation does not offer version 1")
        if DELIVERY_MODE_PULL not in self.delivery_modes:
            reject(RejectionReason.VERSION_UNSUPPORTED, "negotiation does not offer pull delivery")
        # Nerve sizes its frames and requests by these limits.
        limits = self.receive_limits
        if limits.frame_bytes < MIN_FRAME_BYTES or limits.in_flight_requests < 1:
            reject(RejectionReason.LIMIT_EXCEEDED, "negotiation has no usable frame or request limit")


# ---------------------------------------------------------------------- #
#  Envelope                                                               #
# ---------------------------------------------------------------------- #


@_record
class Payload:
    """The frame body: the member named like the frame kind."""

    nudge: Nudge | None = omit_empty(None)
    inbox_read: InboxRead | None = omit_empty(None)
    inbox_read_result: InboxReadResult | None = omit_empty(None)
    inbox_ack: InboxAck | None = omit_empty(None)
    inbox_ack_result: InboxAckResult | None = omit_empty(None)
    connection_status: ConnectionStatus | None = omit_empty(None)
    capabilities: Capabilities | None = omit_empty(None)
    heartbeat: Heartbeat | None = omit_empty(None)
    drain: Drain | None = omit_empty(None)
    negotiation: Negotiation | None = omit_empty(None)


@_record
class Envelope:
    """One versioned stream frame.

    ``request_id`` names a new request frame, one-way frames included.
    ``correlation_id`` names the request that a response answers.
    """

    version: str = ""
    kind: str = ""
    request_id: str = omit_empty()
    correlation_id: str = omit_empty()
    connection_id: uuid.UUID | None = omit_empty(None)
    payload: Payload = struct(Payload)

    @property
    def body(self) -> Any:
        """The payload member that ``kind`` selects."""
        return getattr(self.payload, self.kind, None)

    def validate(self) -> None:
        """Check a frame from the gateway before Nerve handles it."""
        if self.version != PROTOCOL_VERSION:
            reject(RejectionReason.VERSION_UNSUPPORTED, f"unsupported channel protocol version {self.version!r}")
        if self.kind not in GATEWAY_FRAME_KINDS:
            reject(RejectionReason.KIND_UNSUPPORTED, f"frame kind {self.kind!r} is not handled from the gateway")
        body = self.body
        if body is None:
            malformed(f"{self.kind} frame has no {self.kind} payload")
        if self.kind in CONNECTION_SCOPED_KINDS and self.connection_id in (None, NIL_UUID):
            reject(RejectionReason.SCOPE_MISMATCH, f"{self.kind} frame names no logical connection")
        check = getattr(body, "validate", None)
        if check is not None:
            check()


def decode_envelope(text: str, maximum_bytes: int = MAX_FRAME_BYTES) -> Envelope:
    """Decode and check one frame from the gateway.

    ``maximum_bytes`` is the ``frame_bytes`` value that Nerve advertised. It
    applies to the wire bytes and is checked before parsing.
    """
    if byte_length(text) > maximum_bytes:
        reject(RejectionReason.FRAME_TOO_LARGE, "encoded frame exceeds its byte limit")
    envelope = decode_record(Envelope, parse_json(text))
    envelope.validate()
    return envelope


def encode_envelope(envelope: Envelope, maximum_bytes: int = MAX_FRAME_BYTES) -> str:
    """The text to send for a frame.

    ``maximum_bytes`` is the ``frame_bytes`` value that the gateway
    advertised. The check counts the exact bytes that this returns.
    """
    text = dumps(envelope)
    if byte_length(text) > maximum_bytes:
        reject(RejectionReason.FRAME_TOO_LARGE, "encoded frame exceeds its byte limit")
    return text
