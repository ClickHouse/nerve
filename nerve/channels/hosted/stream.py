"""One gateway stream: negotiation, frame checks, requests, heartbeat, drain.

Each stream negotiates on its own, and its request IDs, limits, capabilities,
in-flight requests, and transfers belong to it alone. A response arrives on
the stream of its request. A frame that fails decoding, direction, limit, or
stream-state checks closes the stream with status ``1002`` and the contract's
rejection reason.

A request that passes its local deadline is abandoned. An inbox request or a
retry-safe operation may be sent again on any stream; any other operation is
not sent again. The abandoned request still counts against the gateway's
``in_flight_requests``, and an operation against its connection's
``in_flight_operations``, until the response arrives or the stream closes. A
late response gets the same checks and is then discarded.

Transfers belong to the stream and the operation that owns them. A
``file_read`` ends at the final chunk of the transfer that its result
declares. Nerve sends a read only while the requested length,
together with the reads already in flight, fits its own ``memory_bytes``, so
the gateway never has to exceed it. An upload (``file_send``) sends its
chunks after the operation frame, sized to the gateway's ``frame_bytes``, and
stops when the result arrives first. Nerve starts an upload only while its
size, together with the uploads already in flight, fits the gateway's
``memory_bytes``.
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
    validate_operation_result,
)
from nerve.channels.hosted.contract.model import MAX_TRANSFER_CHUNK_BYTES, TransferChunk, byte_length

logger = logging.getLogger(__name__)

CLOSE_NORMAL = 1000
CLOSE_GOING_AWAY = 1001
CLOSE_PROTOCOL_ERROR = 1002

# The kind of the response that answers each request kind.
RESPONSE_KIND = {
    "inbox_read": "inbox_read_result",
    "inbox_ack": "inbox_ack_result",
    "operation": "operation_result",
}
_BEFORE_NEGOTIATION = frozenset({"negotiation", "heartbeat", "drain"})
# A drain is a courtesy; a peer that does not take it quickly is closed anyway.
_DRAIN_SEND_SECONDS = 2.0
# Room left in an upload chunk frame for a longer offset or final marker.
_CHUNK_FRAME_MARGIN = 32
# How long a cancelled sender still lets its frame finish.
_WRITE_FINISH_SECONDS = 5.0


class StreamClosed(Exception):
    """The stream closed, or is closing, before the request finished.

    A request may have reached the gateway before the close.
    """


class StreamBusy(Exception):
    """The stream cannot take another request now. Nothing was sent."""


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
    """A response frame and its wire length.

    ``data`` holds the bytes of a read's transfer.
    """

    envelope: Envelope
    frame_bytes: int
    data: bytes = b""

    @property
    def body(self) -> Any:
        return self.envelope.body


@dataclass
class _Pending:
    kind: str
    body: Any
    connection_id: uuid.UUID | None
    future: asyncio.Future[Response]
    # A read's successful result, kept until its final chunk.
    result: Response | None = None
    received: int = 0
    data: bytearray | None = None


def read_bytes(operation: str, payload: Any) -> int:
    """The largest transfer that a read operation can bring, else 0.

    *payload* is the operation's own member, such as a ``FileReadOperation``.
    """
    return payload.length_bytes if operation == "file_read" else 0


def _request_read_bytes(kind: str, body: Any) -> int:
    if kind != "operation":
        return 0
    return read_bytes(body.kind, getattr(body, body.kind))


def _request_upload_bytes(kind: str, body: Any) -> int:
    if kind != "operation" or body.kind != "file_send":
        return 0
    return body.file_send.file.total_bytes


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
        # Requests sent and not answered, abandoned ones included.
        self._pending: dict[str, _Pending] = {}
        self._next_request = 0
        self._next_one_way = 0
        self._heartbeat_sequence = 0
        self._send_lock = asyncio.Lock()
        self.last_received = self.opened_at
        self._last_heartbeat_sent = self.opened_at
        self.close_reason = ""
        self._close_task: asyncio.Task | None = None

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
            and len(self._pending) < self.peer_limits.in_flight_requests
        )

    def supports(self, connection_id: uuid.UUID, operation: str) -> bool:
        """Whether this stream advertised *operation* for *connection_id*."""
        capabilities = self.capabilities.get(connection_id)
        return capabilities is not None and operation in capabilities.operations

    def operations_in_flight(self, connection_id: uuid.UUID) -> int:
        return sum(
            1 for pending in self._pending.values()
            if pending.kind == "operation" and pending.connection_id == connection_id
        )

    def has_operation_capacity(
        self, connection_id: uuid.UUID, read_bytes: int = 0, upload_bytes: int = 0,
    ) -> bool:
        """Whether one more operation fits the stream and the connection limits.

        A read of up to *read_bytes* must also fit Nerve's ``memory_bytes``
        with the reads already in flight and one frame. An upload of
        *upload_bytes* must fit the gateway's ``memory_bytes`` in the same way.
        An upload above the gateway's limits counts as fitting, so that the
        request refuses it at once.
        """
        capabilities = self.capabilities.get(connection_id)
        return (
            capabilities is not None
            and self.has_capacity()
            and self.operations_in_flight(connection_id) < capabilities.limits.in_flight_operations
            and (read_bytes == 0 or self._read_room() >= read_bytes)
            and (
                upload_bytes == 0
                or upload_bytes > self._upload_limit(connection_id)
                or self._upload_room() >= upload_bytes
            )
        )

    def _upload_limit(self, connection_id: uuid.UUID) -> int:
        """The largest upload that the gateway takes for *connection_id*."""
        capabilities = self.capabilities.get(connection_id)
        if self.peer_limits is None or capabilities is None:
            return 0
        return min(self.peer_limits.transfer_bytes, capabilities.limits.file_bytes)

    def _upload_room(self) -> int:
        """The bytes that one more upload may send to the gateway."""
        limits = self.peer_limits
        if limits is None:
            return 0
        reserved = sum(
            _request_upload_bytes(pending.kind, pending.body) for pending in self._pending.values()
        )
        return limits.memory_bytes - limits.frame_bytes - reserved

    def _read_room(self) -> int:
        """The bytes that one more read transfer may bring."""
        limits = self._receive_limits
        reserved = sum(_request_read_bytes(pending.kind, pending.body) for pending in self._pending.values())
        return min(limits.transfer_bytes, limits.memory_bytes - limits.frame_bytes - reserved)

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
        await self._close_websocket(code, reason)

    async def _close_websocket(self, code: int, reason: str = "") -> None:
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
        elif kind == "transfer":
            self._receive_chunk(envelope)
        else:
            self._resolve(envelope, frame_bytes)

    def _resolve(self, envelope: Envelope, frame_bytes: int) -> None:
        """Deliver a response to its request, or discard a late one."""
        correlation_id = envelope.correlation_id
        pending = self._pending.get(correlation_id)
        if pending is None:
            if self._sent_in_this_epoch(correlation_id):
                # Already answered: a repeated response is discarded.
                return
            raise Rejected(RejectionReason.MALFORMED_FRAME, "response names no request of this stream")
        if RESPONSE_KIND.get(pending.kind) != envelope.kind or pending.result is not None:
            raise Rejected(RejectionReason.MALFORMED_FRAME, "response kind does not match its request")
        if envelope.connection_id != pending.connection_id and envelope.kind == "operation_result":
            raise Rejected(RejectionReason.SCOPE_MISMATCH, "result names another connection")
        if envelope.kind == "inbox_read_result":
            validate_inbox_read_result(pending.body, envelope.body, frame_bytes)
        elif envelope.kind == "operation_result":
            validate_operation_result(pending.body, envelope.body)
            transfer = envelope.body.transfer
            if transfer is not None:
                if transfer.total_bytes > _request_read_bytes(pending.kind, pending.body):
                    raise Rejected(RejectionReason.LIMIT_EXCEEDED, "transfer exceeds the read")
                # A successful read ends at the final chunk of its transfer.
                pending.result = Response(envelope, frame_bytes)
                pending.data = None if pending.future.done() else bytearray()
                return
        del self._pending[correlation_id]
        if not pending.future.done():
            pending.future.set_result(Response(envelope, frame_bytes))
        self._listener.stream_changed(self)

    def _receive_chunk(self, envelope: Envelope) -> None:
        """Add one chunk to a read's transfer, and finish the read at the final chunk.

        The chunk must continue the transfer that the read's result declared,
        on the same connection. An abandoned read keeps counting its offset
        but drops the data.
        """
        pending = self._pending.get(envelope.correlation_id)
        if pending is None or pending.result is None:
            raise Rejected(RejectionReason.MALFORMED_FRAME, "transfer chunk names no read of this stream")
        if envelope.connection_id != pending.connection_id:
            raise Rejected(RejectionReason.SCOPE_MISMATCH, "transfer chunk names another connection")
        chunk: TransferChunk = envelope.body
        declared = pending.result.body.transfer
        if chunk.transfer_id != declared.transfer_id or chunk.total_bytes != declared.total_bytes:
            raise Rejected(RejectionReason.MALFORMED_FRAME, "transfer chunk does not match its result")
        if chunk.offset != pending.received:
            raise Rejected(RejectionReason.MALFORMED_FRAME, "transfer chunk is not contiguous")
        pending.received += len(chunk.data)
        if pending.future.done():
            pending.data = None
        elif pending.data is not None:
            pending.data += chunk.data
        if not chunk.final:
            return
        del self._pending[envelope.correlation_id]
        if not pending.future.done() and pending.data is not None:
            result = pending.result
            pending.future.set_result(Response(result.envelope, result.frame_bytes, bytes(pending.data)))
        self._listener.stream_changed(self)

    def _sent_in_this_epoch(self, request_id: str) -> bool:
        prefix, _, number = request_id.partition("-")
        return prefix == "n" and number.isdigit() and 0 < int(number) <= self._next_request

    # ------------------------------------------------------------------ #
    #  Sending                                                             #
    # ------------------------------------------------------------------ #

    def _encode(
        self,
        kind: str,
        body: Any,
        connection_id: uuid.UUID | None,
        request_id: str,
        correlation_id: str = "",
    ) -> str:
        """Encode a request frame, or an upload chunk with *correlation_id*.

        Requests that expect a response use ``n-<k>`` IDs, and every such ID
        is sent. One-way frames use ``o-<k>``, so a response that names one of
        them is told apart from a late response and closes the stream.
        """
        envelope = Envelope(
            version=PROTOCOL_VERSION,
            kind=kind,
            request_id=request_id,
            correlation_id=correlation_id,
            connection_id=connection_id,
            payload=Payload(**{kind: body}),
        )
        limit = self.peer_limits.frame_bytes if self.peer_limits is not None else 16 * 1024
        return encode_envelope(envelope, limit)

    async def _send_text(self, text: str) -> None:
        async with self._send_lock:
            await self._send_locked(text)

    async def _send_locked(self, text: str) -> None:
        """Send one frame. The caller holds the send lock.

        When the caller is cancelled, the frame still gets a short time to
        finish, so the other requests of the stream are not lost.
        """
        if self.state == "closed":
            raise StreamClosed()
        write = asyncio.ensure_future(self._websocket.send_text(text))
        try:
            await asyncio.shield(write)
        except asyncio.CancelledError:
            try:
                await asyncio.wait({write}, timeout=_WRITE_FINISH_SECONDS)
            except asyncio.CancelledError:
                pass
            if not write.done() or write.cancelled() or write.exception() is not None:
                # The frame may be written only in part, so the stream
                # cannot carry another one.
                write.cancel()
                self._mark_closed()
                self._close_task = asyncio.get_running_loop().create_task(
                    self._close_websocket(CLOSE_GOING_AWAY),
                )
                if not asyncio.current_task().cancelling():
                    # The write was cancelled, not the caller.
                    raise StreamClosed() from None
            raise
        except Exception as error:
            self._mark_closed()
            raise StreamClosed() from error

    async def _send_one_way(
        self, kind: str, body: Any, connection_id: uuid.UUID | None = None,
    ) -> None:
        self._next_one_way += 1
        text = self._encode(kind, body, connection_id, f"o-{self._next_one_way}")
        await self._send_text(text)

    def _check_room(
        self, kind: str, body: Any, connection_id: uuid.UUID | None, upload: bytes | None,
    ) -> None:
        if self.state == "closed":
            raise StreamBusy()
        if kind == "operation":
            if connection_id is None or not self.supports(connection_id, body.kind):
                raise StreamBusy()
            if upload is not None:
                if len(upload) > self._upload_limit(connection_id):
                    raise Rejected(
                        RejectionReason.LIMIT_EXCEEDED,
                        "upload exceeds the gateway's transfer or file limit",
                    )
            if not self.has_operation_capacity(
                connection_id, _request_read_bytes(kind, body), _request_upload_bytes(kind, body),
            ):
                raise StreamBusy()
        elif not self.has_capacity():
            raise StreamBusy()

    async def request(
        self,
        kind: str,
        body: Any,
        *,
        timeout: float,
        connection_id: uuid.UUID | None = None,
        upload: bytes | None = None,
    ) -> Response:
        """Send one request on this stream and wait for its response.

        Raises :class:`StreamBusy` before the send when the stream is not
        ready, is closed, is at the gateway's in-flight limit, or does not
        free its send lock before *timeout*,
        :class:`StreamClosed` when it closes after the send starts, and
        :class:`RequestTimedOut` after *timeout* seconds. An operation also
        needs the connection's capabilities on this stream and room under its
        ``in_flight_operations``; otherwise it raises :class:`StreamBusy`.
        A ``file_send`` operation sends *upload* as its transfer, within
        *timeout*; an upload above the gateway's ``transfer_bytes`` or the
        connection's ``file_bytes`` raises :class:`Rejected` before the send.
        """
        if kind not in RESPONSE_KIND:
            raise ValueError(f"{kind} is not a request that has a response")
        if (upload is not None) != (kind == "operation" and body.kind == "file_send"):
            raise ValueError("a file_send operation, and only that, carries an upload")
        if upload is not None and len(upload) != body.file_send.file.total_bytes:
            raise ValueError("upload size does not match its declared total")
        self._check_room(kind, body, connection_id, upload)
        future: asyncio.Future[Response] = asyncio.get_running_loop().create_future()
        registered = False
        try:
            async with asyncio.timeout(timeout):
                async with self._send_lock:
                    # Checked, numbered, and registered just before the send,
                    # so concurrent requests stay inside the limits, and a
                    # deadline or a refused frame before the send leaves
                    # nothing behind.
                    self._check_room(kind, body, connection_id, upload)
                    request_id = f"n-{self._next_request + 1}"
                    text = self._encode(kind, body, connection_id, request_id)
                    self._next_request += 1
                    self._pending[request_id] = _Pending(kind, body, connection_id, future)
                    registered = True
                    await self._send_locked(text)
                if upload is not None:
                    await self._send_upload(request_id, connection_id, body.file_send.file, upload, future)
                return await asyncio.shield(future)
        except TimeoutError:
            if not registered:
                raise StreamBusy() from None
            raise RequestTimedOut() from None
        finally:
            if future.done():
                if not future.cancelled():
                    future.exception()  # retrieved, also when the send failed first
            else:
                # Abandoned: the entry stays until a late response or the
                # close, so it still counts and a late response is checked.
                future.cancel()


    async def _send_upload(
        self,
        request_id: str,
        connection_id: uuid.UUID | None,
        file: Any,
        data: bytes,
        future: asyncio.Future[Response],
    ) -> None:
        """Send the chunks of an upload after its operation frame.

        Each chunk takes the send lock alone, so other frames can go between
        chunks. Sending stops when the result arrives first.
        """
        size = self._chunk_bytes(request_id, connection_id, file.transfer_id, len(data))
        offset = 0
        while offset < len(data) and not future.done():
            piece = data[offset:offset + size]
            chunk = TransferChunk(
                transfer_id=file.transfer_id, offset=offset, total_bytes=len(data),
                data=piece, final=offset + len(piece) == len(data),
            )
            text = self._encode("transfer", chunk, connection_id, "", request_id)
            async with self._send_lock:
                await self._send_locked(text)
            offset += len(piece)
            # Let the receive loop take a result that arrived.
            await asyncio.sleep(0)

    def _chunk_bytes(
        self, request_id: str, connection_id: uuid.UUID | None, transfer_id: str, total: int,
    ) -> int:
        """The largest chunk whose frame fits the gateway's ``frame_bytes``."""
        probe = TransferChunk(
            transfer_id=transfer_id, offset=total - 1, total_bytes=total, data=b"\0", final=True,
        )
        # One byte encodes as four base64 characters.
        overhead = byte_length(self._encode("transfer", probe, connection_id, "", request_id)) - 4
        room = self.peer_limits.frame_bytes - overhead - _CHUNK_FRAME_MARGIN
        return max(1, min(MAX_TRANSFER_CHUNK_BYTES, room // 4 * 3))


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
    "read_bytes",
]
