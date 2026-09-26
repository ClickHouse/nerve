"""The stream endpoint, and the choice of stream for each request.

Every gateway replica opens its own stream, so Nerve holds several equal
streams to one agent. Nerve prefers the oldest ready stream that is not
draining and has room for another request. When the preferred stream closes
or drains, the next one takes its place. A draining stream is used only when
no other stream is open. An operation goes only to a stream that advertised
the operation for its connection, with the same preference.

The upgrade is refused before the WebSocket opens: ``401`` without detail for
any authentication failure, ``503`` above the stream limit, and ``404`` when
hosted channels are off.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import uuid
from typing import Callable

from starlette.responses import Response as HTTPResponse
from starlette.websockets import WebSocket

from nerve.channels.hosted.auth import GatewayTokenVerifier, TokenRejected
from nerve.channels.hosted.contract import Capabilities, StreamLimits
from nerve.channels.hosted.stream import (
    CLOSE_GOING_AWAY,
    ChannelStream,
    StreamTiming,
)

logger = logging.getLogger(__name__)

STREAM_PATH = "/_internal/channel/v1/stream"


async def refuse_upgrade(websocket: WebSocket, status: int) -> None:
    """Answer an upgrade with an HTTP status and no body or detail."""
    try:
        await websocket.send_denial_response(HTTPResponse(status_code=status))
    except RuntimeError:
        # A server without the denial response extension answers 403.
        await websocket.close(code=1008)


def bearer_token(websocket: WebSocket) -> str | None:
    """The token of the one ``Authorization: Bearer`` header, or ``None``.

    The token is read from this header only, never from a query parameter or
    a cookie.
    """
    values = websocket.headers.getlist("authorization")
    if len(values) != 1:
        return None
    scheme, _, token = values[0].partition(" ")
    if scheme.lower() != "bearer" or not token or " " in token:
        return None
    return token


class StreamManager:
    """Accept authenticated gateway streams and choose among them.

    ``on_ready``, ``on_nudge``, and ``on_capabilities`` report stream events to
    the inbox reader and the hosted channels.
    """

    def __init__(
        self,
        *,
        verifier: GatewayTokenVerifier,
        receive_limits: StreamLimits,
        max_streams: int,
        timing: StreamTiming = StreamTiming(),
        on_ready: Callable[[ChannelStream], None] = lambda stream: None,
        on_nudge: Callable[[ChannelStream, str], None] = lambda stream, purpose: None,
        on_capabilities: Callable[[ChannelStream, uuid.UUID, Capabilities], None] = (
            lambda stream, connection_id, capabilities: None
        ),
    ) -> None:
        self._verifier = verifier
        self._receive_limits = receive_limits
        self._max_streams = max_streams
        self._timing = timing
        self._on_ready = on_ready
        self._on_nudge = on_nudge
        self._on_capabilities = on_capabilities
        self._streams: dict[int, ChannelStream] = {}
        self._ids = itertools.count(1)
        self._changed = asyncio.Event()
        self._closing = False
        self.refused: dict[int, int] = {}

    @property
    def streams(self) -> list[ChannelStream]:
        """Open streams, oldest first."""
        return [self._streams[key] for key in sorted(self._streams)]

    # ------------------------------------------------------------------ #
    #  Endpoint                                                            #
    # ------------------------------------------------------------------ #

    async def serve(self, websocket: WebSocket) -> None:
        """Authenticate one upgrade, then serve the stream until it closes."""
        status = await self._authorize(websocket)
        if status is None and (self._closing or len(self._streams) >= self._max_streams):
            status = 503
        if status is not None:
            self.refused[status] = self.refused.get(status, 0) + 1
            await refuse_upgrade(websocket, status)
            return

        stream = ChannelStream(
            websocket,
            stream_id=next(self._ids),
            receive_limits=self._receive_limits,
            listener=self,
            timing=self._timing,
        )
        # Reserved before the first await, so concurrent upgrades cannot
        # both pass the stream limit.
        self._streams[stream.id] = stream
        try:
            await websocket.accept()
            logger.info("Channel stream %d opened (%d open)", stream.id, len(self._streams))
            await stream.run()
        finally:
            self._streams.pop(stream.id, None)
            logger.info(
                "Channel stream %d closed%s (%d open)",
                stream.id,
                f": {stream.close_reason}" if stream.close_reason else "",
                len(self._streams),
            )
            self._notify()

    async def _authorize(self, websocket: WebSocket) -> int | None:
        """``401`` for any failed check, otherwise ``None``.

        Every browser sends ``Origin`` on a WebSocket upgrade, and the gateway
        never does, so a request with one is refused before the token is read.
        """
        if "origin" in websocket.headers:
            logger.info("Channel stream upgrade refused: Origin header present")
            return 401
        token = bearer_token(websocket)
        if token is None:
            logger.info("Channel stream upgrade refused: no single bearer token")
            return 401
        try:
            await self._verifier.verify(token)
        except TokenRejected as error:
            logger.info("Channel stream upgrade refused: %s", error)
            return 401
        return None

    # ------------------------------------------------------------------ #
    #  Stream choice                                                       #
    # ------------------------------------------------------------------ #

    def preferred(self) -> ChannelStream | None:
        """The stream for the next request, or ``None`` when none can take one."""
        ready = [stream for stream in self.streams if stream.ready]
        steady = [stream for stream in ready if not stream.draining]
        for stream in steady:
            if stream.has_capacity():
                return stream
        if steady:
            return None
        for stream in ready:
            if stream.has_capacity():
                return stream
        return None

    async def wait_for_stream(self) -> ChannelStream:
        """Wait until :meth:`preferred` has a stream, and return it."""
        while True:
            stream = self.preferred()
            if stream is not None:
                return stream
            changed = self._changed
            await changed.wait()

    def operation_stream(
        self, connection_id: uuid.UUID, operation: str, read_bytes: int = 0, upload_bytes: int = 0,
    ) -> ChannelStream | None:
        """The stream for the next *operation* on *connection_id*, or ``None``.

        ``None`` also when streams advertise the operation but none has room
        for it now, including room for a read of up to *read_bytes* or an
        upload of *upload_bytes*.
        """
        able = [
            stream for stream in self.streams
            if stream.ready and stream.supports(connection_id, operation)
        ]
        steady = [stream for stream in able if not stream.draining]
        for stream in steady or able:
            if stream.has_operation_capacity(connection_id, read_bytes, upload_bytes):
                return stream
        return None

    def connections(self, operation: str) -> set[uuid.UUID]:
        """The connections that some ready stream advertised *operation* for."""
        return {
            connection_id
            for stream in self.streams if stream.ready
            for connection_id in stream.capabilities
            if stream.supports(connection_id, operation)
        }

    async def wait_for_change(self, timeout: float) -> None:
        """Wait up to *timeout* seconds for a stream, request, or capability change."""
        changed = self._changed
        try:
            await asyncio.wait_for(changed.wait(), timeout)
        except TimeoutError:
            pass

    def capabilities_for(self, connection_id: uuid.UUID) -> Capabilities | None:
        """The capabilities of *connection_id* on the preferred stream that has them."""
        for stream in self.streams:
            if stream.ready and not stream.draining and connection_id in stream.capabilities:
                return stream.capabilities[connection_id]
        for stream in self.streams:
            if stream.ready and connection_id in stream.capabilities:
                return stream.capabilities[connection_id]
        return None

    def _notify(self) -> None:
        changed, self._changed = self._changed, asyncio.Event()
        changed.set()

    # ------------------------------------------------------------------ #
    #  Stream events                                                       #
    # ------------------------------------------------------------------ #

    def stream_ready(self, stream: ChannelStream) -> None:
        self._notify()
        self._on_ready(stream)

    def stream_nudged(self, stream: ChannelStream, purpose: str) -> None:
        self._on_nudge(stream, purpose)

    def stream_capabilities(
        self, stream: ChannelStream, connection_id: uuid.UUID, capabilities: Capabilities,
    ) -> None:
        self._notify()
        self._on_capabilities(stream, connection_id, capabilities)

    def stream_changed(self, stream: ChannelStream) -> None:
        self._notify()

    # ------------------------------------------------------------------ #
    #  Shutdown                                                            #
    # ------------------------------------------------------------------ #

    async def drain(self, seconds: float = 10.0) -> None:
        """Refuse new streams, and send ``drain`` on every open stream."""
        self._closing = True
        await asyncio.gather(
            *(stream.drain("shutdown", seconds) for stream in self.streams),
            return_exceptions=True,
        )

    async def close(self) -> None:
        """Close every stream."""
        self._closing = True
        await asyncio.gather(
            *(stream.close(CLOSE_GOING_AWAY) for stream in self.streams),
            return_exceptions=True,
        )


__all__ = ["STREAM_PATH", "StreamManager", "bearer_token", "refuse_upgrade"]
