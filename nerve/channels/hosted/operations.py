"""Provider operations over gateway streams: stream choice, deadlines, outcomes.

An operation goes to a stream that advertised it for its connection, as
:meth:`StreamManager.operation_stream` chooses. The gateway gets the
connection's ``operation_deadline_millis`` as its budget, and Nerve waits a
short time longer for the result.

When the stream closes or the local deadline passes before the result, the
outcome depends on the operation. A retry-safe operation, such as ``typing``
or a read, is sent again as a new request on any stream. Any other operation
is ``ambiguous``: its side effect may have reached the provider, so it is
never sent again. A ``rate_limited`` result is sent again after the advised
delay, a limited number of times, when the delay is not too long. Every other
failure goes to the caller as :class:`OperationFailed`.

A read returns the bytes of its transfer. A ``file_send`` sends its bytes as
a transfer after the operation frame, with a new transfer ID for each request.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import uuid
from dataclasses import dataclass
from typing import Any

from nerve.channels.hosted.contract import (
    Operation,
    OperationResult,
    Rejected,
    RejectionReason,
)
from nerve.channels.hosted.contract.model import RETRY_SAFE_OPERATIONS
from nerve.channels.hosted.manager import StreamManager
from nerve.channels.hosted.stream import (
    ChannelStream,
    RequestTimedOut,
    Response,
    StreamBusy,
    StreamClosed,
    read_bytes,
)

logger = logging.getLogger(__name__)

# How many times a lost retry-safe operation is sent again.
LOST_RESENDS = 2
# How many times a rate-limited operation is sent again by default.
RATE_LIMIT_RETRIES = 2


@dataclass(frozen=True)
class OperationTiming:
    """Timers of the operation runner. Tests shorten them.

    ``result_grace`` is the time after the gateway's budget that Nerve still
    waits for a result. ``stream_wait`` is how long an operation waits for a
    stream that can take it, for example while a gateway replica reconnects.
    ``retry_delay`` applies to a ``rate_limited`` result without a delay, and
    a result that advises more than ``max_retry_delay`` is not retried.
    """

    result_grace: float = 2.0
    stream_wait: float = 10.0
    retry_delay: float = 1.0
    max_retry_delay: float = 30.0


class OperationFailed(Exception):
    """An operation did not succeed.

    ``outcome`` is a contract outcome other than ``succeeded``. ``local`` is
    true when Nerve decided the outcome without a result from the gateway.
    The text names only the kind and the outcome, because it can reach the
    agent. The gateway's reason code and the local detail are telemetry:
    they are in the attributes and in :attr:`detailed`, for logs.
    """

    def __init__(
        self,
        kind: str,
        outcome: str,
        reason_code: str = "",
        *,
        retry_after: float = 0.0,
        local: bool = False,
        detail: str = "",
    ) -> None:
        self.kind = kind
        self.outcome = outcome
        self.reason_code = reason_code
        self.retry_after = retry_after
        self.local = local
        self.detail = detail
        text = f"{kind} operation {outcome}"
        if outcome == "ambiguous":
            text += ": it may already have taken effect"
        super().__init__(text)

    @property
    def detailed(self) -> str:
        """The text with the reason code and the local detail, for logs."""
        text = f"{self.kind} operation {self.outcome}"
        if self.reason_code:
            text += f" ({self.reason_code})"
        if self.detail:
            text += f": {self.detail}"
        return text

    @classmethod
    def from_result(cls, result: OperationResult) -> OperationFailed:
        return cls(
            result.kind, result.outcome, result.reason_code,
            retry_after=result.retry_after_millis / 1000,
        )


class OperationRunner:
    """Send operations on the streams of one agent and return their results."""

    def __init__(self, streams: StreamManager, timing: OperationTiming = OperationTiming()) -> None:
        self._streams = streams
        self._timing = timing

    @property
    def streams(self) -> StreamManager:
        return self._streams

    async def perform(
        self,
        connection_id: uuid.UUID,
        kind: str,
        payload: Any,
        *,
        rate_limit_retries: int = RATE_LIMIT_RETRIES,
        stream_wait: float | None = None,
        upload: bytes | None = None,
    ) -> OperationResult:
        """Run one operation and return its successful result.

        *stream_wait* replaces the configured wait for a stream that can take
        the operation; ``0`` fails at once. A ``file_send`` needs its bytes
        in *upload*. Raises :class:`OperationFailed` for every other outcome.
        """
        response = await self._run(
            connection_id, kind, payload,
            rate_limit_retries=rate_limit_retries, stream_wait=stream_wait, upload=upload,
        )
        return response.body

    async def read(
        self, connection_id: uuid.UUID, kind: str, payload: Any, *, stream_wait: float | None = None,
    ) -> bytes:
        """Run a ``file_read`` and return its transferred bytes."""
        response = await self._run(connection_id, kind, payload, stream_wait=stream_wait)
        return response.data

    async def _run(
        self,
        connection_id: uuid.UUID,
        kind: str,
        payload: Any,
        *,
        rate_limit_retries: int = RATE_LIMIT_RETRIES,
        stream_wait: float | None = None,
        upload: bytes | None = None,
    ) -> Response:
        loop = asyncio.get_running_loop()
        reserve = read_bytes(kind, payload)
        wait = self._timing.stream_wait if stream_wait is None else stream_wait
        lost = 0
        limited = 0
        # Each request gets its own wait for a stream; a lost race for room
        # does not start a new one.
        choose_by = loop.time() + wait
        while True:
            stream = await self._stream_for(
                connection_id, kind, choose_by, reserve, len(upload) if upload is not None else 0,
            )
            limits = stream.capabilities[connection_id].limits
            if upload is not None:
                payload = dataclasses.replace(
                    payload, file=dataclasses.replace(payload.file, transfer_id=f"u-{uuid.uuid4()}"),
                )
            operation = Operation(
                kind=kind, deadline_millis=limits.operation_deadline_millis, **{kind: payload},
            )
            try:
                response = await stream.request(
                    "operation", operation,
                    connection_id=connection_id,
                    timeout=limits.operation_deadline_millis / 1000 + self._timing.result_grace,
                    upload=upload,
                )
            except StreamBusy:
                # Another task took the last place first, or the stream's
                # send lock stayed taken; choose again.
                if loop.time() >= choose_by:
                    raise OperationFailed(
                        kind, "unavailable", "overloaded", local=True,
                        detail="no stream took the operation in time",
                    ) from None
                await asyncio.sleep(0)
                continue
            except Rejected as error:
                too_large = error.reason in (
                    RejectionReason.FRAME_TOO_LARGE, RejectionReason.LIMIT_EXCEEDED,
                )
                raise OperationFailed(
                    kind, "unavailable", "content_too_large" if too_large else "invalid_target",
                    local=True, detail=error.detail,
                ) from None
            except (StreamClosed, RequestTimedOut) as error:
                how = "stream closed" if isinstance(error, StreamClosed) else "no result in time"
                if kind not in RETRY_SAFE_OPERATIONS:
                    logger.warning(
                        "Hosted %s operation on connection %s is ambiguous (%s); it is not sent again",
                        kind, connection_id, how,
                    )
                    raise OperationFailed(kind, "ambiguous", local=True, detail=how) from None
                if lost >= LOST_RESENDS:
                    raise OperationFailed(
                        kind, "unavailable", "deadline_exceeded", local=True, detail=how,
                    ) from None
                lost += 1
                logger.info("Hosted %s operation lost (%s); sending it again", kind, how)
                choose_by = loop.time() + wait
                continue

            result: OperationResult = response.body
            if result.outcome == "succeeded":
                return response
            delay = result.retry_after_millis / 1000 or self._timing.retry_delay
            if (
                result.outcome == "rate_limited" and limited < rate_limit_retries
                and delay <= self._timing.max_retry_delay
            ):
                limited += 1
                logger.info(
                    "Hosted %s operation rate limited; sending it again in %.1f s", kind, delay,
                )
                await asyncio.sleep(delay)
                choose_by = loop.time() + wait
                continue
            raise OperationFailed.from_result(result)

    async def connections(self, kind: str) -> set[uuid.UUID]:
        """The connections that advertise *kind*, after up to ``stream_wait`` for one."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timing.stream_wait
        while True:
            found = self._streams.connections(kind)
            remaining = deadline - loop.time()
            if found or remaining <= 0:
                return found
            await self._streams.wait_for_change(remaining)

    async def _stream_for(
        self,
        connection_id: uuid.UUID,
        kind: str,
        deadline: float,
        read_bytes: int = 0,
        upload_bytes: int = 0,
    ) -> ChannelStream:
        """Wait until *deadline* for a stream that can take the operation now.

        A connection whose streams all leave the operation out gets
        ``unsupported`` at once. No stream for the connection, or no room on
        the streams that have it, is ``unavailable`` at the deadline.
        """
        loop = asyncio.get_running_loop()
        while True:
            stream = self._streams.operation_stream(connection_id, kind, read_bytes, upload_bytes)
            if stream is not None:
                return stream
            supported = connection_id in self._streams.connections(kind)
            if not supported and self._streams.capabilities_for(connection_id) is not None:
                raise OperationFailed(
                    kind, "unsupported", local=True,
                    detail=f"connection {connection_id} does not advertise it",
                )
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise OperationFailed(
                    kind, "unavailable", "overloaded" if supported else "provider_unavailable",
                    local=True,
                    detail="no stream had room in time" if supported
                    else f"no stream serves connection {connection_id}",
                )
            await self._streams.wait_for_change(remaining)


__all__ = ["LOST_RESENDS", "OperationFailed", "OperationRunner", "OperationTiming"]
