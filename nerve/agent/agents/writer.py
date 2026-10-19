"""Write-behind persistence for agent records and transcript items.

Ingestion runs on the hot path of every sub-agent event, often while the
session's run lock is held, so it never awaits the database. It queues:
``mark_dirty(rec)`` for a changed record, ``append(rec, item)`` for a new
transcript item. One background task flushes:

* when 1.0 s has passed since the oldest queued change,
* when 200 items are queued,
* immediately after ``kick()`` (every status transition kicks).

A flush writes one transaction per session: rows of records the registry
folded into another one are deleted first (their task id moves to the
surviving record, and a task id is unique per session), then that session's
dirty records (the items reference them), then its items. An
``IntegrityError`` on a foreign key means the session was deleted meanwhile:
its batch is dropped (DEBUG). Any other ``IntegrityError`` is retried one
record at a time, so one bad row costs only its own record and items
(WARNING). Any other error is logged at WARNING and the batch retried on the
next flush, at most three times, then dropped with an ERROR. A flush cut
short by cancellation (shutdown) puts its records back, so the final flush
writes them. Items stay visible to :meth:`pending_items` until their
transaction commits, so a transcript read never misses an item that is
neither flushed nor pending.

Persistence failure never breaks ingestion or a turn.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from typing import Any, Callable

from nerve.agent.agents.model import AgentRecord, iso_z, to_row

logger = logging.getLogger(__name__)

FLUSH_INTERVAL_SECONDS = 1.0
FLUSH_MAX_ITEMS = 200
MAX_RETRIES = 3


class AgentWriter:
    """Queues record and item writes; one task flushes them in batches."""

    def __init__(
        self,
        db: Any,
        *,
        interval: float = FLUSH_INTERVAL_SECONDS,
        max_items: int = FLUSH_MAX_ITEMS,
        max_retries: int = MAX_RETRIES,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self._db = db
        self._interval = interval
        self._max_items = max_items
        self._max_retries = max_retries
        self._clock = clock
        self._wall_clock = wall_clock
        # session_id -> agent_id -> record (rows are built at flush time
        # from the record's current state).
        self._dirty: dict[str, dict[str, AgentRecord]] = {}
        # session_id -> [(agent_id, item)], in append order.
        self._items: dict[str, list[tuple[str, dict]]] = {}
        # session_id -> ids of folded records whose rows and items go.
        self._deletes: dict[str, set[str]] = {}
        self._n_items = 0
        self._oldest: float | None = None
        self._kicked = False
        self._retries: dict[str, int] = {}
        self._wake = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._closed = False

    # ------------------------------------------------------------------ #
    #  Queueing (sync, never blocks)                                      #
    # ------------------------------------------------------------------ #

    def mark_dirty(self, rec: AgentRecord) -> None:
        """Queue ``rec`` for an upsert."""
        if rec.id in self._deletes.get(rec.session_id, ()):
            return  # folded away: its row goes
        self._dirty.setdefault(rec.session_id, {})[rec.id] = rec
        self._note_change()

    def forget_record(self, rec: AgentRecord) -> None:
        """A record folded into another one: drop its queued writes and
        delete its stored row and items (before the next upserts)."""
        self._deletes.setdefault(rec.session_id, set()).add(rec.id)
        dirty = self._dirty.get(rec.session_id)
        if dirty is not None:
            dirty.pop(rec.id, None)
        self._kicked = True
        self._note_change()
        self._wake.set()

    def append(self, rec: AgentRecord, item: dict) -> None:
        """Queue a transcript item of ``rec`` (``item`` carries its ``seq``,
        ``at`` and ``kind``)."""
        self._items.setdefault(rec.session_id, []).append((rec.id, item))
        self._n_items += 1
        self._note_change()
        if self._n_items >= self._max_items:
            self._wake.set()

    def kick(self) -> None:
        """Flush as soon as possible."""
        self._kicked = True
        self._wake.set()

    def pending_items(self, agent_id: str) -> list[dict]:
        """Copies of the queued, not yet committed items of one agent, in
        ``seq`` order."""
        out = [
            dict(item)
            for items in self._items.values()
            for aid, item in items
            if aid == agent_id
        ]
        out.sort(key=lambda i: i.get("seq", 0))
        return out

    def forget_session(self, session_id: str) -> None:
        """Drop every queued write of a deleted session."""
        self._dirty.pop(session_id, None)
        self._deletes.pop(session_id, None)
        dropped = self._items.pop(session_id, None)
        if dropped:
            self._n_items -= len(dropped)
        self._retries.pop(session_id, None)

    @property
    def queued_items(self) -> int:
        return self._n_items

    def has_pending(self, session_id: str) -> bool:
        """Whether anything of the session is still queued."""
        return bool(
            self._dirty.get(session_id) or self._items.get(session_id)
            or self._deletes.get(session_id)
        )

    def _note_change(self) -> None:
        if self._oldest is None:
            self._oldest = self._clock()
            self._wake.set()  # arm the timer

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                          #
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Start the flush task (idempotent; needs a running loop). Without a
        database there is nothing to flush, so no task runs."""
        if self._db is None:
            return
        if self._task is None or self._task.done():
            self._closed = False
            self._task = asyncio.create_task(self._run(), name="agent-writer")

    async def close(self) -> None:
        """Flush everything and stop the task."""
        self._closed = True
        task, self._task = self._task, None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                logger.exception("agent writer task failed")
        await self.flush()

    async def _run(self) -> None:
        while not self._closed:
            due = self._due_in()
            if due is None or due > 0:
                self._wake.clear()
                due = self._due_in()  # re-check: a change may have raced the clear
                if due is None or due > 0:
                    try:
                        await asyncio.wait_for(self._wake.wait(), timeout=due)
                    except asyncio.TimeoutError:
                        pass
                    continue
            try:
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — the loop must survive
                logger.exception("agent writer flush failed")
                await asyncio.sleep(self._interval)

    def _due_in(self) -> float | None:
        """Seconds until the next flush is due (0 = now, None = nothing
        queued)."""
        if not self._dirty and not self._n_items and not self._deletes:
            self._kicked = False
            return None
        if self._kicked or self._n_items >= self._max_items:
            return 0.0
        if self._oldest is None:
            self._oldest = self._clock()
        return max(0.0, self._oldest + self._interval - self._clock())

    # ------------------------------------------------------------------ #
    #  Flushing                                                           #
    # ------------------------------------------------------------------ #

    async def flush(self) -> None:
        """Write everything queued now."""
        async with self._lock:
            self._kicked = False
            self._oldest = None
            for session_id in list(set(self._dirty) | set(self._items) | set(self._deletes)):
                await self._flush_session(session_id)
            if (self._dirty or self._n_items or self._deletes) and self._oldest is None:
                self._oldest = self._clock()

    def _requeue(self, session_id: str, records: dict[str, AgentRecord],
                 deletes: set[str]) -> None:
        """Put a flush's records and deletes back, unless a newer change
        already queued them."""
        if deletes:
            self._deletes.setdefault(session_id, set()).update(deletes)
        gone = self._deletes.get(session_id, set())
        dirty = self._dirty.setdefault(session_id, {})
        for rec in records.values():
            if rec.id not in gone:
                dirty.setdefault(rec.id, rec)
        if not dirty:
            self._dirty.pop(session_id, None)

    async def _flush_session(self, session_id: str) -> None:
        records = self._dirty.pop(session_id, {})
        deletes = self._deletes.pop(session_id, set())
        items = list(self._items.get(session_id, ()))
        if not records and not items and not deletes:
            return
        if self._db is None:
            # No database (tests): records are not stored and items stay
            # pending, where transcript reads find them.
            return
        updated_at = iso_z(self._wall_clock())
        rows = [
            to_row(rec, updated_at=updated_at)
            for rec in records.values() if rec.id not in deletes
        ]
        events = [
            (
                agent_id, session_id, int(item["seq"]), str(item["kind"]),
                json.dumps(item, ensure_ascii=False, default=str),
                str(item.get("at") or updated_at),
            )
            for agent_id, item in items
            if agent_id not in deletes
        ]
        try:
            await self._write(rows, events, deletes)
        except asyncio.CancelledError:
            # Shutdown cancelled the flush mid-write (the transaction rolled
            # back): the final flush writes these records and items.
            self._requeue(session_id, records, deletes)
            raise
        except sqlite3.IntegrityError as e:
            if "FOREIGN KEY" in str(e).upper():
                logger.debug(
                    "agents[%s] write dropped: the session is gone (%s)",
                    session_id[:8], e,
                )
                self._settle(session_id, len(items))
                return
            # One bad row (a task id already stored for another record):
            # write the batch one record at a time so only it is lost.
            try:
                await self._write_one_by_one(session_id, rows, events, deletes, e)
            except asyncio.CancelledError:
                self._requeue(session_id, records, deletes)
                raise
            self._settle(session_id, len(items))
            return
        except Exception as e:  # noqa: BLE001 — persistence never breaks ingestion
            tries = self._retries.get(session_id, 0) + 1
            if tries > self._max_retries:
                logger.error(
                    "agents[%s] write dropped after %d attempts: %s "
                    "(%d records, %d items)",
                    session_id[:8], tries - 1, e, len(rows), len(events),
                )
                self._settle(session_id, len(items))
                return
            self._retries[session_id] = tries
            logger.warning(
                "agents[%s] write failed (attempt %d), retrying: %s",
                session_id[:8], tries, e,
            )
            self._requeue(session_id, records, deletes)
            return
        self._settle(session_id, len(items))

    async def _write(self, rows: list[dict], events: list[tuple], deletes: set[str]) -> None:
        if deletes:
            await self._db.write_agent_batch(rows, events, delete_ids=sorted(deletes))
        else:
            await self._db.write_agent_batch(rows, events)

    async def _write_one_by_one(
        self, session_id: str, rows: list[dict], events: list[tuple],
        deletes: set[str], first_error: Exception,
    ) -> None:
        """After a non-FK integrity error: deletes alone, then each record
        with its items in its own transaction; a failing one is dropped."""
        try:
            if deletes:
                await self._write([], [], deletes)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.warning("agents[%s] delete of folded records failed: %s", session_id[:8], e)
        by_agent: dict[str, list[tuple]] = {}
        for event in events:
            by_agent.setdefault(event[0], []).append(event)
        lost: list[str] = []
        for row in rows:
            try:
                await self._write([row], by_agent.pop(row["id"], []), set())
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                lost.append(row["id"])
        # Items of records that were not in this batch (already stored).
        for agent_id, agent_events in by_agent.items():
            try:
                await self._write([], agent_events, set())
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                lost.append(agent_id)
        logger.warning(
            "agents[%s] write of %s dropped: %s",
            session_id[:8], ", ".join(lost) or "nothing", first_error,
        )

    def _settle(self, session_id: str, n_items: int) -> None:
        """Drop the first ``n_items`` items of a session (written or given
        up); items appended during the write stay queued."""
        self._retries.pop(session_id, None)
        queued = self._items.get(session_id)
        if queued is None:
            return  # forgotten meanwhile
        del queued[:n_items]
        self._n_items -= n_items
        if not queued:
            self._items.pop(session_id, None)
