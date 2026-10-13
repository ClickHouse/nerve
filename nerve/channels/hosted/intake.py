"""The inbox reader: one reader of the agent's inbox across all streams.

The gateway keeps admitted events in an inbox until Nerve acknowledges them.
Nerve reads a page, processes each event, acknowledges every processed row,
and reads again until a page is empty. It never has more than one read or
unacknowledged page at a time, on any stream.

Nerve reads after each stream negotiates, at once after an ``invoke`` nudge
or a nudge without a purpose, at most once every five seconds for ``observe``
nudges, and every 30 seconds while it runs, because a nudge can be lost. A
nudge that arrives while a read or a page is outstanding causes one more read
after that page is acknowledged.

A request that passes its local deadline is sent again on any stream. After
``unavailable`` the reader waits with backoff and jitter. A row that cannot
be processed now (for example when the local database fails) stops the page:
the rows before it are acknowledged, and the rest come back on a later read.
Until that read, nudges do not start reads, because each read of the row
counts toward the gateway's limit for rows that are never acknowledged.
"""

from __future__ import annotations

import asyncio
import collections
import logging
import random
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Protocol

from nerve.channels.hosted.contract import (
    Event,
    InboxAck,
    InboxAckItem,
    InboxRead,
)
from nerve.channels.hosted.stream import (
    ChannelStream,
    RequestTimedOut,
    StreamBusy,
    StreamClosed,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Disposition:
    """What Nerve did with one inbox row.

    ``accepted``, ``duplicate``, and ``rejected`` are acknowledgement
    outcomes. ``deferred`` is local only: the row is not acknowledged and
    comes back on a later read.
    """

    outcome: str
    reason_code: str = ""
    detail: str = ""

    @classmethod
    def accepted(cls) -> Disposition:
        return cls("accepted")

    @classmethod
    def rejected(cls, reason_code: str, detail: str) -> Disposition:
        return cls("rejected", reason_code, detail)

    @classmethod
    def deferred(cls, detail: str) -> Disposition:
        return cls("deferred", detail=detail)


class EventConsumer(Protocol):
    """A hosted channel as the reader sees it."""

    @property
    def name(self) -> str: ...
    def can_accept(self) -> bool: ...
    async def deliver(self, event: Event) -> Disposition: ...


class StreamSource(Protocol):
    def preferred(self) -> ChannelStream | None: ...
    async def wait_for_stream(self) -> ChannelStream: ...


@dataclass(frozen=True)
class ReaderSettings:
    """Page limits and timers of the reader. Tests shorten the timers."""

    maximum_events: int = 25
    maximum_bytes: int = 128 * 1024
    request_timeout: float = 15.0
    poll_interval: float = 30.0
    observe_interval: float = 5.0
    backoff_initial: float = 0.5
    backoff_maximum: float = 30.0
    capacity_poll: float = 0.1
    stop_grace: float = 5.0
    duplicate_capacity: int = 10_000


class DuplicateCache:
    """Accepted events by ``(connection, event_id)``.

    It lasts for the process and is bounded, so eviction can make an old
    event look new, and a restart starts empty.
    """

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._entries: collections.OrderedDict[tuple[uuid.UUID, str], None] = collections.OrderedDict()

    def __contains__(self, key: tuple[uuid.UUID, str]) -> bool:
        if key not in self._entries:
            return False
        self._entries.move_to_end(key)
        return True

    def add(self, key: tuple[uuid.UUID, str]) -> None:
        self._entries[key] = None
        self._entries.move_to_end(key)
        while len(self._entries) > self._capacity:
            self._entries.popitem(last=False)

    def __len__(self) -> int:
        return len(self._entries)


class Backoff:
    """Exponential backoff with jitter: half fixed, half random."""

    def __init__(self, initial: float, maximum: float, rng: Callable[[], float]) -> None:
        self._initial = initial
        self._maximum = maximum
        self._rng = rng
        self._delay = min(maximum, initial)

    def next(self) -> float:
        # The delay doubles up to the maximum and then stays there, so any
        # number of failures in a row gives a finite value.
        delay = self._delay
        self._delay = min(self._maximum, delay * 2)
        return delay / 2 + self._rng() * delay / 2

    def reset(self) -> None:
        self._delay = min(self._maximum, self._initial)


class InboxReader:
    """Read, process, and acknowledge pages over whichever stream is preferred."""

    def __init__(
        self,
        streams: StreamSource,
        consumers: dict[str, EventConsumer],
        *,
        settings: ReaderSettings = ReaderSettings(),
        clock: Callable[[], float] = time.monotonic,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self._streams = streams
        self._consumers = consumers
        self._settings = settings
        self._clock = clock
        self._rng = rng
        self._duplicates = DuplicateCache(settings.duplicate_capacity)
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._stopping = False
        self._deferred = False
        self._immediate = False
        self._again = False
        self._busy = False
        self._observe_due: float | None = None
        self._next_poll: float | None = None
        self._retry_at: float | None = None
        self._last_read_start = float("-inf")
        self._deferral_backoff = Backoff(settings.backoff_initial, settings.backoff_maximum, rng)
        self.outcomes: collections.Counter[str] = collections.Counter()

    # ------------------------------------------------------------------ #
    #  Triggers                                                            #
    # ------------------------------------------------------------------ #

    def read_now(self) -> None:
        """Read as soon as possible: a stream negotiated, or an invoke arrived.

        While a deferred row waits, a read would only return that row again
        and add to its read count at the gateway, so the deferral timer
        decides when the next read starts.
        """
        if self._deferred:
            return
        if self._busy:
            self._again = True
        else:
            self._immediate = True
        self._wake.set()

    def nudge(self, purpose: str) -> None:
        """React to a nudge. Observe nudges are combined into one read."""
        if purpose != "observe":
            self.read_now()
            return
        if self._deferred:
            return
        due = max(self._clock(), self._last_read_start + self._settings.observe_interval)
        if self._observe_due is None or due < self._observe_due:
            self._observe_due = due
        self._wake.set()

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="channel-inbox-reader")

    async def stop(self) -> None:
        """Stop reading. A page in progress may finish its acknowledgement first.

        An acknowledgement that does not reach the gateway returns its rows
        after a restart, when the duplicate cache is empty again. While a
        stream is still open, the reader gets ``stop_grace`` seconds to finish
        the page it holds. Under uvicorn the streams are closed before the
        application shuts down, so there is then no stream and no wait.
        """
        task, self._task = self._task, None
        if task is None:
            return
        self._stopping = True
        self._wake.set()
        if self._busy and self._streams.preferred() is not None:
            try:
                await asyncio.wait_for(asyncio.shield(task), self._settings.stop_grace)
            except TimeoutError:
                pass
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _run(self) -> None:
        while not self._stopping:
            await self._wait_for_turn()
            if self._stopping:
                return
            try:
                await self._cycle()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Channel inbox reader failed; retrying")
                self._deferred = True
                self._retry_at = self._clock() + self._deferral_backoff.next()

    async def _wait_for_turn(self) -> None:
        while True:
            if self._immediate or self._stopping:
                return
            now = self._clock()
            timers = (self._retry_at,) if self._deferred else (
                self._observe_due, self._next_poll, self._retry_at,
            )
            deadlines = [moment for moment in timers if moment is not None]
            if deadlines and min(deadlines) <= now:
                return
            self._wake.clear()
            timeout = min(deadlines) - now if deadlines else None
            try:
                await asyncio.wait_for(self._wake.wait(), timeout)
            except TimeoutError:
                pass

    # ------------------------------------------------------------------ #
    #  One cycle: read and acknowledge until a page is empty              #
    # ------------------------------------------------------------------ #

    async def _cycle(self) -> None:
        self._immediate = False
        self._deferred = False
        self._retry_at = None
        self._next_poll = None
        self._busy = True
        try:
            while not self._stopping:
                await self._wait_for_capacity()
                if self._stopping:
                    return
                self._again = False
                self._observe_due = None
                self._last_read_start = self._clock()
                events = await self._read()
                if self._stopping:
                    # Rows read but not processed come back after a restart.
                    return
                if events:
                    items, deferral = await self._process(events)
                    if items:
                        await self._acknowledge(items)
                    if deferral is not None:
                        self._defer(deferral)
                        return
                    self._deferral_backoff.reset()
                    continue
                if self._again or (self._observe_due is not None and self._observe_due <= self._clock()):
                    continue
                return
        finally:
            self._busy = False
            self._next_poll = self._clock() + self._settings.poll_interval

    async def _wait_for_capacity(self) -> None:
        """Stop reading while a channel cannot take more events."""
        while not self._stopping and not all(
            consumer.can_accept() for consumer in self._consumers.values()
        ):
            await asyncio.sleep(self._settings.capacity_poll)

    def _defer(self, deferral: Disposition) -> None:
        logger.info("Channel inbox read paused: %s", deferral.detail)
        self._deferred = True
        self._again = False
        self._observe_due = None
        self._retry_at = self._clock() + self._deferral_backoff.next()

    async def _read(self) -> list[Event]:
        read = InboxRead(
            maximum_events=self._settings.maximum_events,
            maximum_bytes=self._settings.maximum_bytes,
        )
        backoff = Backoff(self._settings.backoff_initial, self._settings.backoff_maximum, self._rng)
        while True:
            response = await self._send("inbox_read", read)
            result = response.body
            if result.outcome == "succeeded":
                return list(result.events)
            logger.info("Channel inbox read unavailable: %s", result.reason_code)
            await asyncio.sleep(backoff.next())

    async def _acknowledge(self, items: list[InboxAckItem]) -> None:
        """Acknowledge until the gateway stores the items, on any stream."""
        acknowledgement = InboxAck(items=tuple(items))
        backoff = Backoff(self._settings.backoff_initial, self._settings.backoff_maximum, self._rng)
        while True:
            response = await self._send("inbox_ack", acknowledgement)
            if response.body.outcome == "succeeded":
                return
            logger.info("Channel inbox acknowledgement unavailable: %s", response.body.reason_code)
            await asyncio.sleep(backoff.next())

    async def _send(self, kind: str, body: object):
        """Send one inbox request, again on any stream after a close or deadline."""
        while True:
            stream = await self._streams.wait_for_stream()
            try:
                return await stream.request(kind, body, timeout=self._settings.request_timeout)
            except RequestTimedOut:
                logger.info("Channel %s on stream %d passed its deadline; sending again", kind, stream.id)
            except (StreamClosed, StreamBusy):
                await asyncio.sleep(0.01)

    # ------------------------------------------------------------------ #
    #  Processing                                                          #
    # ------------------------------------------------------------------ #

    async def _process(self, events: list[Event]) -> tuple[list[InboxAckItem], Disposition | None]:
        items: list[InboxAckItem] = []
        for event in events:
            disposition = await self._dispatch(event)
            if disposition.outcome == "deferred":
                return items, disposition
            self.outcomes[disposition.outcome] += 1
            if disposition.outcome == "rejected":
                logger.info(
                    "Channel event %s (%s, %s) rejected: %s: %s",
                    event.event_id, event.kind, event.admission.purpose,
                    disposition.reason_code, disposition.detail,
                )
            items.append(InboxAckItem(
                inbox_id=event.delivery.inbox_id,
                outcome=disposition.outcome,
                reason_code=disposition.reason_code,
            ))
        return items, None

    async def _dispatch(self, event: Event) -> Disposition:
        key = (event.connection_id, event.event_id)
        if key in self._duplicates:
            return Disposition("duplicate")
        consumer = self._consumers.get(event.provider)
        if consumer is None:
            return Disposition.rejected(
                "configuration_invalid", f"no hosted channel for provider {event.provider!r}",
            )
        try:
            disposition = await consumer.deliver(event)
        except Exception as error:  # noqa: BLE001 - a local failure; the row comes back
            logger.exception("Hosted channel %s failed on event %s", consumer.name, event.event_id)
            return Disposition.deferred(f"{consumer.name} failed: {type(error).__name__}")
        if disposition.outcome == "accepted":
            self._duplicates.add(key)
        return disposition


__all__ = [
    "Backoff",
    "Disposition",
    "DuplicateCache",
    "EventConsumer",
    "InboxReader",
    "ReaderSettings",
]
