"""One gateway stream: negotiation, frame checks, requests, heartbeat, drain.

Each stream negotiates on its own, and its request IDs, limits, capabilities,
in-flight requests, and transfers belong to it alone. A response arrives on
the stream of its request. A frame that fails decoding, direction, limit, or
stream-state checks closes the stream with status ``1002`` and the contract's
rejection reason.

A request that passes its local deadline is abandoned: the caller may send it
again on any stream, and a late response is discarded. It still counts
against the gateway's ``in_flight_requests`` until the response arrives or
the stream closes.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Protocol

from starlette.websockets import WebSocket

from nerve.channels.hosted.contract import (
    DELIVERY_MODE_PULL,
    PROTOCOL_VERSION,
    Capabilities,
    ConnectionStatus,
    Drain,
    Envelope,
    Heartbeat,
    Negotiation,
    Payload,
    Rejected,
    RejectionReason,
    StreamLimits,
    decode_envelope,
    encode_envelope,
    validate_inbox_read_result,
)
from nerve.channels.hosted.contract.model import byte_length

logger = logging.getLogger(__name__)

CLOSE_NORMAL = 1000
CLOSE_GOING_AWAY = 1001
CLOSE_PROTOCOL_ERROR = 1002

# The kind of the response that answers each request kind.
RESPONSE_KIND = {
    "inbox_read": "inbox_read_result",
    "inbox_ack": "inbox_ack_result",
}
_BEFORE_NEGOTIATION = frozenset({"negotiation", "heartbeat", "drain"})
# A drain is a courtesy; a peer that does not take it quickly is closed anyway.
_DRAIN_SEND_SECONDS = 2.0


class StreamClosed(Exception):
    """The stream closed, or is closing, before the request finished."""


class StreamBusy(Exception):
    """The stream cannot take another request now."""


class RequestTimedOut(Exception):
    """No response arrived before the local deadline. The request is abandoned."""


@dataclass(frozen=True)
class StreamTiming:
    """Timers of one stream. Tests shorten them.

    Heartbeat frames tell the gateway that Nerve is alive. Nerve does not
    close a stream that sends none: WebSocket pings already detect a dead
    transport, and the contract sets no heartbeat interval.
    """

    heartbeat_interval: float = 20.0
    negotiation_timeout: float = 10.0


@dataclass(frozen=True)
class Response:
    """A response frame and its wire length."""

    envelope: Envelope
    frame_bytes: int

    @property
    def body(self) -> Any:
        return self.envelope.body


@dataclass
class _Pending:
    kind: str
    body: Any
    connection_id: uuid.UUID | None
    future: asyncio.Future[Response]


class StreamListener(Protocol):
    """What a stream reports to its manager."""

    def stream_ready(self, stream: ChannelStream) -> None: ...
    def stream_nudged(self, stream: ChannelStream, purpose: str) -> None: ...
    def stream_capabilities(
        self, stream: ChannelStream, connection_id: uuid.UUID, capabilities: Capabilities,
    ) -> None: ...
    def stream_changed(self, stream: ChannelStream) -> None: ...


class ChannelStream:
    """One authenticated WebSocket from one gateway replica.

    ``receive_limits`` is what Nerve accepts on this stream. Nerve advertises
    the same limits on every stream, so a row that one stream cannot carry
    cannot be carried by another either.
    """

    def __init__(
        self,
        websocket: WebSocket,
        *,
        stream_id: int,
        receive_limits: StreamLimits,
        listener: StreamListener,
        timing: StreamTiming = StreamTiming(),
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.id = stream_id
        self._websocket = websocket
        self._receive_limits = receive_limits
        self._listener = listener
        self._timing = timing
        self._clock = clock
        self.opened_at = clock()
        self.state = "negotiating"
        # The gateway sent drain: new requests go to another stream.
        self.draining = False
        # Nerve sent drain: the gateway sends no new nudges here.
        self.drain_sent = False
        self.peer_limits: StreamLimits | None = None
        self.capabilities: dict[uuid.UUID, Capabilities] = {}
        self.connection_status: dict[uuid.UUID, ConnectionStatus] = {}
        self._pending: dict[str, _Pending] = {}
        self._outstanding: set[str] = set()
        self._next_request = 0
        self._next_one_way = 0
        self._heartbeat_sequence = 0
        self._send_lock = asyncio.Lock()
        self.last_received = self.opened_at
        self._last_heartbeat_sent = self.opened_at
        self.close_reason = ""

    def __repr__(self) -> str:
        return f"<ChannelStream {self.id} {self.state}{' draining' if self.draining else ''}>"

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    @property
    def ready(self) -> bool:
        return self.state == "ready"

    def has_capacity(self) -> bool:
        """Whether one more request fits the gateway's in-flight limit."""
        return (
            self.ready
            and self.peer_limits is not None
            and len(self._outstanding) < self.peer_limits.in_flight_requests
        )

    async def run(self) -> None:
        """Serve the accepted WebSocket until it closes."""
        watchdog = asyncio.create_task(self._watchdog(), name=f"channel-stream-{self.id}-watchdog")
        try:
            await self._send_one_way("negotiation", Negotiation(
                preferred_version=PROTOCOL_VERSION,
                supported_versions=(PROTOCOL_VERSION,),
                delivery_modes=(DELIVERY_MODE_PULL,),
                receive_limits=self._receive_limits,
            ))
            await self._receive_loop()
        except StreamClosed:
            pass
        finally:
            watchdog.cancel()
            try:
                await watchdog
            except (asyncio.CancelledError, Exception):
                pass
            self._mark_closed()

    async def close(self, code: int = CLOSE_NORMAL, reason: str = "") -> None:
        """Close the WebSocket. Every request of this stream fails."""
        if self.state == "closed":
            return
        self.close_reason = reason
        self._mark_closed()
        try:
            await self._websocket.close(code=code, reason=reason)
        except Exception:  # noqa: BLE001 - the peer may already be gone
            pass

    async def drain(self, reason: str = "shutdown", seconds: float = 10.0) -> None:
        """Tell the gateway that Nerve stops using this stream."""
        if not self.ready or self.drain_sent:
            return
        initiated = datetime.now(timezone.utc)
        deadline = initiated + timedelta(seconds=seconds)
        self.drain_sent = True
        try:
            async with asyncio.timeout(_DRAIN_SEND_SECONDS):
                await self._send_one_way("drain", Drain(
                    reason=reason, initiated_at=initiated, deadline=deadline,
                ))
        except (StreamClosed, TimeoutError):
            pass

    def _mark_closed(self) -> None:
        if self.state == "closed":
            return
        self.state = "closed"
        for pending in self._pending.values():
            if not pending.future.done():
                pending.future.set_exception(StreamClosed())
        self._pending.clear()
        self._outstanding.clear()
        self._listener.stream_changed(self)

    async def _watchdog(self) -> None:
        """Send heartbeats, and close a stream that does not negotiate in time."""
        tick = max(0.01, min(self._timing.heartbeat_interval, self._timing.negotiation_timeout) / 4)
        while self.state != "closed":
            await asyncio.sleep(tick)
            now = self._clock()
            if self.state == "negotiating" and now - self.opened_at > self._timing.negotiation_timeout:
                logger.info("Channel stream %d did not negotiate in time", self.id)
                await self.close(CLOSE_GOING_AWAY)
                return
            if now - self._last_heartbeat_sent >= self._timing.heartbeat_interval:
                self._last_heartbeat_sent = now
                self._heartbeat_sequence += 1
                try:
                    await self._send_one_way("heartbeat", Heartbeat(
                        sequence=self._heartbeat_sequence, sent_at=datetime.now(timezone.utc),
                    ))
                except StreamClosed:
                    return

    # ------------------------------------------------------------------ #
    #  Receiving                                                           #
    # ------------------------------------------------------------------ #

    async def _receive_loop(self) -> None:
        while self.state != "closed":
            try:
                message = await self._websocket.receive()
            except Exception:  # noqa: BLE001 - a receive after close raises
                return
            if message["type"] == "websocket.disconnect":
                return
            text = message.get("text")
            try:
                if text is None:
                    raise Rejected(RejectionReason.MALFORMED_FRAME, "binary WebSocket message")
                self.last_received = self._clock()
                envelope = decode_envelope(text, self._receive_limits.frame_bytes)
                self._handle(envelope, byte_length(text))
            except Rejected as error:
                logger.warning(
                    "Channel stream %d closed on a rejected frame: %s", self.id, error,
                )
                await self.close(CLOSE_PROTOCOL_ERROR, error.reason)
                return

    def _handle(self, envelope: Envelope, frame_bytes: int) -> None:
        kind = envelope.kind
        if self.state == "negotiating" and kind not in _BEFORE_NEGOTIATION:
            raise Rejected(RejectionReason.MALFORMED_FRAME, f"{kind} frame before negotiation")
        body = envelope.body
        if kind == "negotiation":
            if self.peer_limits is not None:
                raise Rejected(RejectionReason.MALFORMED_FRAME, "negotiation repeated in one epoch")
            self.peer_limits = body.receive_limits
            self.state = "ready"
            logger.info("Channel stream %d negotiated version %s", self.id, PROTOCOL_VERSION)
            self._listener.stream_ready(self)
        elif kind == "heartbeat":
            pass
        elif kind == "drain":
            if not self.draining:
                self.draining = True
                logger.info("Channel stream %d is draining (%s)", self.id, body.reason)
                self._listener.stream_changed(self)
        elif kind == "nudge":
            self._listener.stream_nudged(self, body.purpose)
        elif kind == "capabilities":
            self.capabilities[envelope.connection_id] = body
            self._listener.stream_capabilities(self, envelope.connection_id, body)
        elif kind == "connection_status":
            self.connection_status[envelope.connection_id] = body
        else:
            self._resolve(envelope, frame_bytes)

    def _resolve(self, envelope: Envelope, frame_bytes: int) -> None:
        """Deliver a response to its request, or discard a late one."""
        correlation_id = envelope.correlation_id
        pending = self._pending.get(correlation_id)
        if pending is None:
            if self._sent_in_this_epoch(correlation_id):
                # Answered, abandoned, or failed: a late response is not invalid.
                self._outstanding.discard(correlation_id)
                self._listener.stream_changed(self)
                return
            raise Rejected(RejectionReason.MALFORMED_FRAME, "response names no request of this stream")
        if RESPONSE_KIND.get(pending.kind) != envelope.kind:
            raise Rejected(RejectionReason.MALFORMED_FRAME, "response kind does not match its request")
        if envelope.connection_id != pending.connection_id and envelope.kind == "operation_result":
            raise Rejected(RejectionReason.SCOPE_MISMATCH, "result names another connection")
        if envelope.kind == "inbox_read_result":
            validate_inbox_read_result(pending.body, envelope.body, frame_bytes)
        del self._pending[correlation_id]
        self._outstanding.discard(correlation_id)
        if not pending.future.done():
            pending.future.set_result(Response(envelope, frame_bytes))
        self._listener.stream_changed(self)

    def _sent_in_this_epoch(self, request_id: str) -> bool:
        prefix, _, number = request_id.partition("-")
        return prefix == "n" and number.isdigit() and 0 < int(number) <= self._next_request

    # ------------------------------------------------------------------ #
    #  Sending                                                             #
    # ------------------------------------------------------------------ #

    def _new_request_id(self, *, one_way: bool) -> str:
        """A new request ID. Requests that expect a response use ``n-``.

        One-way frames use ``o-``, so a response that names one of them is
        told apart from a late response and closes the stream.
        """
        if one_way:
            self._next_one_way += 1
            return f"o-{self._next_one_way}"
        self._next_request += 1
        return f"n-{self._next_request}"

    def _encode(
        self, kind: str, body: Any, connection_id: uuid.UUID | None, *, one_way: bool,
    ) -> tuple[str, str]:
        request_id = self._new_request_id(one_way=one_way)
        envelope = Envelope(
            version=PROTOCOL_VERSION,
            kind=kind,
            request_id=request_id,
            connection_id=connection_id,
            payload=Payload(**{kind: body}),
        )
        limit = self.peer_limits.frame_bytes if self.peer_limits is not None else 16 * 1024
        return request_id, encode_envelope(envelope, limit)

    async def _send_text(self, text: str) -> None:
        async with self._send_lock:
            await self._send_locked(text)

    async def _send_locked(self, text: str) -> None:
        """Send one frame. The caller holds the send lock."""
        if self.state == "closed":
            raise StreamClosed()
        try:
            await self._websocket.send_text(text)
        except Exception as error:
            self._mark_closed()
            raise StreamClosed() from error

    async def _send_one_way(
        self, kind: str, body: Any, connection_id: uuid.UUID | None = None,
    ) -> None:
        _, text = self._encode(kind, body, connection_id, one_way=True)
        await self._send_text(text)

    async def request(
        self,
        kind: str,
        body: Any,
        *,
        timeout: float,
        connection_id: uuid.UUID | None = None,
    ) -> Response:
        """Send one request on this stream and wait for its response.

        Raises :class:`StreamBusy` when the stream is not ready or is at the
        gateway's in-flight limit, :class:`StreamClosed` when it closes, and
        :class:`RequestTimedOut` after *timeout* seconds.
        """
        if kind not in RESPONSE_KIND:
            raise ValueError(f"{kind} is not a request that has a response")
        if not self.has_capacity():
            raise StreamClosed() if self.state == "closed" else StreamBusy()
        request_id, text = self._encode(kind, body, connection_id, one_way=False)
        future: asyncio.Future[Response] = asyncio.get_running_loop().create_future()
        try:
            async with asyncio.timeout(timeout):
                async with self._send_lock:
                    # Registered just before the send, so a deadline that
                    # passes while the lock is taken leaves nothing behind.
                    self._pending[request_id] = _Pending(kind, body, connection_id, future)
                    self._outstanding.add(request_id)
                    await self._send_locked(text)
                return await asyncio.shield(future)
        except TimeoutError:
            raise RequestTimedOut() from None
        finally:
            if future.done():
                if not future.cancelled():
                    future.exception()  # retrieved, also when the send failed first
            else:
                # Abandoned: it counts against the limit until a late
                # response arrives or the stream closes.
                self._pending.pop(request_id, None)
                future.cancel()


__all__ = [
    "CLOSE_GOING_AWAY",
    "CLOSE_NORMAL",
    "CLOSE_PROTOCOL_ERROR",
    "ChannelStream",
    "RequestTimedOut",
    "Response",
    "StreamBusy",
    "StreamClosed",
    "StreamListener",
    "StreamTiming",
]
