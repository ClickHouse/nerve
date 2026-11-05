"""Broadcast pump for agent state.

Ingestion runs on the hot path of every sub-agent event, often while the
session's run lock is held, so it never awaits a WebSocket send. It tells the
emitter what changed (all methods are synchronous) and one consumer task does
the sends:

* :meth:`AgentEmitter.upsert` — a record changed. Pending ids are coalesced
  per session and sent as one ``agent_update`` built from the *current*
  records: 50 ms after an immediate change (creation, status, attention),
  1 s after a progress change (2 s when the session has more than 20 live
  agents).
* :meth:`AgentEmitter.event` — a transcript item, sent as ``agent_event`` on
  the per-agent channel ``agent:<session>:<agent>``, only when a socket
  subscribed to it.
* :meth:`AgentEmitter.counts` — the live set may have changed; debounced 1 s,
  the global ``session_agents`` event goes out when the count changed.

Nothing here is buffered for turn replay (``buffer=False``): a reconnecting
client gets the roster from ``session_status`` instead. When more than 10,000
item messages wait, the oldest are dropped (clients see the ``seq`` gap and
refetch) with a WARNING at most once a minute.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

IMMEDIATE_DELAY = 0.05
PROGRESS_DELAY = 1.0
BUSY_PROGRESS_DELAY = 2.0
BUSY_LIVE_AGENTS = 20
COUNTS_DELAY = 1.0
MAX_QUEUED = 10_000
DROP_WARNING_INTERVAL = 60.0

Sink = Callable[[str, dict], Awaitable[None]]


def agent_channel(session_id: str, agent_id: str) -> str:
    """The broadcast channel of one agent's live transcript."""
    return f"agent:{session_id}:{agent_id}"


async def _broadcast_sink(channel: str, message: dict) -> None:
    from nerve.agent.streaming import broadcaster

    await broadcaster.broadcast(channel, message, buffer=False)


def _broadcast_has_listeners(channel: str) -> bool:
    from nerve.agent.streaming import broadcaster

    return broadcaster.has_listeners(channel)


class AgentEmitter:
    """Single-consumer broadcast pump with coalescing."""

    def __init__(
        self,
        *,
        sink: Sink | None = None,
        has_listeners: Callable[[str], bool] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._sink = sink or _broadcast_sink
        self._has_listeners = has_listeners or _broadcast_has_listeners
        self._clock = clock
        self._registry: Any = None
        # FIFO of (channel, message): per-agent transcript items.
        self._queue: deque[tuple[str, str, dict]] = deque()  # (session, channel, msg)
        self._pending: dict[str, set[str]] = {}
        self._due: dict[str, float] = {}
        self._counts_due: dict[str, float] = {}
        self._last_counts: dict[str, tuple[int, bool]] = {}
        self._wake = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._dropped = 0
        self._drop_warned_at: float | None = None

    def bind(self, registry: Any) -> None:
        """The registry that builds views, summaries and counts at send time."""
        self._registry = registry

    # ------------------------------------------------------------------ #
    #  Producer side (sync)                                               #
    # ------------------------------------------------------------------ #

    def upsert(self, session_id: str, agent_id: str, immediate: bool) -> None:
        self._pending.setdefault(session_id, set()).add(agent_id)
        if immediate:
            delay = IMMEDIATE_DELAY
        elif self._live_count(session_id) > BUSY_LIVE_AGENTS:
            delay = BUSY_PROGRESS_DELAY
        else:
            delay = PROGRESS_DELAY
        due = self._clock() + delay
        current = self._due.get(session_id)
        if current is None or due < current:
            self._due[session_id] = due
        self._wake.set()

    def event(self, session_id: str, agent_id: str, item: dict) -> None:
        channel = agent_channel(session_id, agent_id)
        try:
            listening = self._has_listeners(channel)
        except Exception:  # noqa: BLE001
            listening = False
        if not listening:
            return
        self._queue.append((session_id, channel, {
            "type": "agent_event",
            "session_id": session_id,
            "agent_id": agent_id,
            "item": item,
        }))
        if len(self._queue) > MAX_QUEUED:
            while len(self._queue) > MAX_QUEUED:
                self._queue.popleft()
                self._dropped += 1
            now = self._clock()
            if (
                self._drop_warned_at is None
                or now - self._drop_warned_at >= DROP_WARNING_INTERVAL
            ):
                self._drop_warned_at = now
                logger.warning(
                    "agent emitter overloaded: dropped %d transcript events "
                    "(subscribers refetch the gap)", self._dropped,
                )
                self._dropped = 0
        self._wake.set()

    def counts(self, session_id: str) -> None:
        if session_id not in self._counts_due:
            self._counts_due[session_id] = self._clock() + COUNTS_DELAY
            self._wake.set()

    def forget(self, session_id: str) -> None:
        """Drop everything pending for a deleted session."""
        self._pending.pop(session_id, None)
        self._due.pop(session_id, None)
        self._counts_due.pop(session_id, None)
        self._last_counts.pop(session_id, None)
        if self._queue:
            self._queue = deque(e for e in self._queue if e[0] != session_id)

    # ------------------------------------------------------------------ #
    #  Consumer side                                                      #
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="agent-emitter")

    async def close(self) -> None:
        task, self._task = self._task, None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                logger.exception("agent emitter task failed")
        await self.flush()

    async def flush(self) -> None:
        """Send everything pending now (tests and shutdown)."""
        await self._deliver(force=True)

    async def _run(self) -> None:
        while True:
            try:
                await self._deliver(force=False)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — the pump must survive
                logger.exception("agent emitter delivery failed")
            if self._queue:
                continue
            self._wake.clear()
            delay = self._next_delay()
            if self._queue or (delay is not None and delay <= 0):
                continue
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    def _next_delay(self) -> float | None:
        dues = list(self._due.values()) + list(self._counts_due.values())
        if not dues:
            return None
        return max(0.0, min(dues) - self._clock())

    async def _deliver(self, *, force: bool) -> None:
        async with self._lock:
            while self._queue:
                _sid, channel, message = self._queue.popleft()
                await self._send(channel, message)
            now = self._clock()
            for sid in [s for s, d in self._due.items() if force or d <= now]:
                self._due.pop(sid, None)
                ids = self._pending.pop(sid, set())
                if not ids or self._registry is None:
                    continue
                message = self._registry.agent_update_message(sid, ids)
                if message is not None:
                    await self._send(sid, message)
            for sid in [s for s, d in self._counts_due.items() if force or d <= now]:
                self._counts_due.pop(sid, None)
                if self._registry is None:
                    continue
                live = self._registry.live_count(sid)
                state = (live, live > 0)
                if self._last_counts.get(sid) == state:
                    continue
                self._last_counts[sid] = state
                await self._send("__global__", {
                    "type": "session_agents",
                    "session_id": sid,
                    "background_count": live,
                    "has_background_tasks": live > 0,
                })

    async def _send(self, channel: str, message: dict) -> None:
        try:
            await self._sink(channel, message)
        except Exception as e:  # noqa: BLE001 — a dead socket never stops the pump
            logger.debug("agent broadcast to %s failed: %s", channel, e)

    def _live_count(self, session_id: str) -> int:
        if self._registry is None:
            return 0
        try:
            return int(self._registry.live_count(session_id))
        except Exception:  # noqa: BLE001
            return 0


class RecordingEmitter(AgentEmitter):
    """An emitter that records ``(channel, message)`` pairs in ``emitted``
    instead of broadcasting (tests). Every agent channel counts as
    subscribed unless ``has_listeners`` says otherwise."""

    def __init__(self, *, has_listeners: Callable[[str], bool] | None = None,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.emitted: list[tuple[str, dict]] = []

        async def _record(channel: str, message: dict) -> None:
            self.emitted.append((channel, message))

        super().__init__(
            sink=_record,
            has_listeners=has_listeners or (lambda _channel: True),
            clock=clock,
        )
