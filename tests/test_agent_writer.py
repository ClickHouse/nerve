"""Write-behind persistence of agent records and transcript items."""

from __future__ import annotations

import asyncio
import sqlite3

import pytest
import pytest_asyncio

from nerve.agent.agents.model import AgentRecord, iso_z, to_row
from nerve.agent.agents.writer import AgentWriter
from nerve.db import Database

T0 = 1_772_150_400.0


def _rec(agent_id: str, session_id: str = "s1", **kw) -> AgentRecord:
    return AgentRecord(id=agent_id, session_id=session_id, created_at=iso_z(T0), **kw)


def _item(seq: int, kind: str = "text", **kw) -> dict:
    return {"seq": seq, "at": iso_z(T0 + seq), "kind": kind, **kw}


@pytest_asyncio.fixture
async def seeded(db: Database):
    await db.create_session("s1", source="web", actor=None)
    await db.create_session("s2", source="web", actor=None)
    return db


async def _wait_for(predicate, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not await predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.01)


class _RecordingDB:
    """A database stand-in that records write_agent_batch calls and can be
    told to block or fail."""

    def __init__(self):
        self.calls: list[tuple[list, list]] = []
        self.gate: asyncio.Event | None = None
        self.fail: list[BaseException] = []

    async def write_agent_batch(self, rows, events):
        if self.gate is not None:
            await self.gate.wait()
        if self.fail:
            raise self.fail.pop(0)
        self.calls.append((rows, events))


@pytest.mark.asyncio
async def test_flushes_after_the_interval(seeded: Database):
    writer = AgentWriter(seeded, interval=0.5)  # wide margin for a loaded runner
    writer.start()
    try:
        rec = _rec("ag_time")
        writer.mark_dirty(rec)
        writer.append(rec, _item(1, text="hello"))
        await asyncio.sleep(0.03)
        assert await seeded.get_session_agent("ag_time") is None  # not yet

        async def _written():
            return await seeded.get_session_agent("ag_time") is not None

        await _wait_for(_written)
        assert [i["text"] for i in await seeded.list_agent_events("ag_time")] == ["hello"]
        assert writer.pending_items("ag_time") == []
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_flushes_when_200_items_are_queued(seeded: Database):
    writer = AgentWriter(seeded, interval=60.0)  # the timer would never fire
    writer.start()
    try:
        rec = _rec("ag_count")
        writer.mark_dirty(rec)
        for seq in range(1, 200):
            writer.append(rec, _item(seq))
        await asyncio.sleep(0.05)
        assert await seeded.list_agent_events("ag_count") == []
        writer.append(rec, _item(200))  # the 200th item triggers the flush

        async def _all_written():
            return len(await seeded.list_agent_events("ag_count", limit=500)) == 200

        await _wait_for(_all_written)
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_kick_flushes_immediately(seeded: Database):
    writer = AgentWriter(seeded, interval=60.0)
    writer.start()
    try:
        rec = _rec("ag_kick", status="completed")
        writer.mark_dirty(rec)
        writer.kick()

        async def _written():
            return await seeded.get_session_agent("ag_kick") is not None

        await _wait_for(_written, timeout=1.0)
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_one_transaction_per_session_records_before_items():
    db = _RecordingDB()
    writer = AgentWriter(db)
    a, b = _rec("ag_a", "s1"), _rec("ag_b", "s2")
    writer.mark_dirty(a)
    writer.append(a, _item(1))
    writer.append(a, _item(2))
    writer.mark_dirty(b)
    writer.append(b, _item(1))
    await writer.flush()
    assert len(db.calls) == 2  # one batch per session
    by_session = {rows[0]["session_id"]: (rows, events) for rows, events in db.calls}
    rows, events = by_session["s1"]
    assert [r["id"] for r in rows] == ["ag_a"]
    assert [(e[0], e[2]) for e in events] == [("ag_a", 1), ("ag_a", 2)]
    assert {r["id"] for r in by_session["s2"][0]} == {"ag_b"}


@pytest.mark.asyncio
async def test_integrity_error_drops_only_that_session(seeded: Database):
    """A session deleted meanwhile fails its foreign key: its batch is dropped,
    every other session still flushes."""
    writer = AgentWriter(seeded)
    gone, kept = _rec("ag_gone", "deleted-session"), _rec("ag_kept", "s2")
    for rec in (gone, kept):
        writer.mark_dirty(rec)
        writer.append(rec, _item(1))
    await writer.flush()
    assert await seeded.get_session_agent("ag_gone") is None
    assert await seeded.get_session_agent("ag_kept") is not None
    assert writer.pending_items("ag_gone") == []  # dropped, not retried
    assert writer.queued_items == 0


@pytest.mark.asyncio
async def test_pending_items_are_copies_until_commit():
    db = _RecordingDB()
    db.gate = asyncio.Event()
    writer = AgentWriter(db)
    rec = _rec("ag_pending")
    writer.mark_dirty(rec)
    writer.append(rec, _item(1, text="one"))
    flushing = asyncio.create_task(writer.flush())
    await asyncio.sleep(0.01)  # the write is in flight
    pending = writer.pending_items("ag_pending")
    assert [i["seq"] for i in pending] == [1]
    pending[0]["text"] = "mutated"  # a copy: the queue is unaffected
    writer.append(rec, _item(2, text="two"))  # arrives during the write
    db.gate.set()
    await flushing
    assert db.calls[0][1][0][4].count("one") == 1
    # Only the committed item left the queue; the late one waits its turn.
    assert [i["seq"] for i in writer.pending_items("ag_pending")] == [2]


@pytest.mark.asyncio
async def test_other_errors_retry_three_times_then_drop():
    db = _RecordingDB()
    writer = AgentWriter(db)
    rec = _rec("ag_retry")
    writer.mark_dirty(rec)
    writer.append(rec, _item(1))
    db.fail = [RuntimeError("disk busy")] * 4
    for _ in range(3):
        await writer.flush()
        assert [i["seq"] for i in writer.pending_items("ag_retry")] == [1]
    await writer.flush()  # the fourth failure gives up
    assert writer.pending_items("ag_retry") == []
    assert db.calls == []
    # A transient failure followed by success writes everything once.
    writer.mark_dirty(rec)
    writer.append(rec, _item(2))
    db.fail = [RuntimeError("once")]
    await writer.flush()
    await writer.flush()
    assert [[e[2] for e in events] for _, events in db.calls] == [[2]]


@pytest.mark.asyncio
async def test_close_flushes_everything(seeded: Database):
    writer = AgentWriter(seeded, interval=60.0)
    writer.start()
    rec = _rec("ag_close")
    writer.mark_dirty(rec)
    writer.append(rec, _item(1))
    await writer.close()
    assert await seeded.get_session_agent("ag_close") is not None
    assert len(await seeded.list_agent_events("ag_close")) == 1


@pytest.mark.asyncio
async def test_forget_session_drops_queued_writes():
    db = _RecordingDB()
    writer = AgentWriter(db)
    rec = _rec("ag_forget")
    writer.mark_dirty(rec)
    writer.append(rec, _item(1))
    writer.forget_session("s1")
    await writer.flush()
    assert db.calls == [] and writer.queued_items == 0


@pytest.mark.asyncio
async def test_without_a_database_items_stay_pending():
    writer = AgentWriter(None)
    writer.start()  # no task without a database
    rec = _rec("ag_mem")
    writer.mark_dirty(rec)
    writer.append(rec, _item(1))
    await writer.flush()
    assert [i["seq"] for i in writer.pending_items("ag_mem")] == [1]
    await writer.close()


@pytest.mark.asyncio
async def test_records_rewritten_after_a_failed_write_keep_latest_state(seeded: Database):
    writer = AgentWriter(seeded)
    rec = _rec("ag_state", status="running")
    writer.mark_dirty(rec)
    real = seeded.write_agent_batch
    calls = 0

    async def _flaky(rows, events):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise sqlite3.OperationalError("database is locked")
        await real(rows, events)

    seeded.write_agent_batch = _flaky  # type: ignore[method-assign]
    try:
        await writer.flush()  # fails, record re-queued
        rec.status = "completed"  # changed before the retry
        await writer.flush()
    finally:
        seeded.write_agent_batch = real  # type: ignore[method-assign]
    assert (await seeded.get_session_agent("ag_state"))["status"] == "completed"


@pytest.mark.asyncio
async def test_close_during_a_flush_keeps_its_records(seeded: Database):
    """Shutdown cancels a flush mid-write: the records it held go back to the
    queue and the final flush writes them with their items."""
    writer = AgentWriter(seeded, interval=60.0)
    real = seeded.write_agent_batch
    entered = asyncio.Event()
    calls = 0

    async def _first_blocks(rows, events, **kw):
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await asyncio.Event().wait()  # never returns: cancelled by close()
        await real(rows, events, **kw)

    seeded.write_agent_batch = _first_blocks  # type: ignore[method-assign]
    try:
        writer.start()
        rec = _rec("ag_inflight", status="completed")
        writer.mark_dirty(rec)
        writer.append(rec, _item(1, text="last words"))
        writer.kick()
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        await writer.close()
    finally:
        seeded.write_agent_batch = real  # type: ignore[method-assign]
    row = await seeded.get_session_agent("ag_inflight")
    assert row is not None and row["status"] == "completed"
    assert [i["text"] for i in await seeded.list_agent_events("ag_inflight")] == ["last words"]


@pytest.mark.asyncio
async def test_unique_task_conflict_costs_only_its_record(seeded: Database):
    """A task id already stored for another record fails the unique index:
    only that record is lost, the rest of the session's batch is written."""
    stored = _rec("ag_stored", task_id="t-dup")
    await seeded.write_agent_batch([to_row(stored, updated_at=iso_z(T0))], [])
    writer = AgentWriter(seeded)
    bad, good = _rec("ag_bad", task_id="t-dup"), _rec("ag_good", task_id="t-ok")
    for rec in (bad, good):
        writer.mark_dirty(rec)
        writer.append(rec, _item(1, text=rec.id))
    await writer.flush()
    assert await seeded.get_session_agent("ag_bad") is None
    assert (await seeded.get_session_agent("ag_good"))["task_id"] == "t-ok"
    assert [i["text"] for i in await seeded.list_agent_events("ag_good")] == ["ag_good"]
    assert writer.queued_items == 0


@pytest.mark.asyncio
async def test_forgotten_record_row_goes_before_its_task_moves(seeded: Database):
    """A folded record's row and items are deleted in the same transaction
    that stores its task id on the surviving record."""
    twin = _rec("ag_twin", task_id="t-1")
    writer = AgentWriter(seeded)
    writer.mark_dirty(twin)
    writer.append(twin, _item(1, kind="lifecycle", event="started"))
    await writer.flush()
    spawn = _rec("ag_spawn", task_id="t-1")
    twin.task_id = None
    writer.forget_record(twin)
    writer.mark_dirty(twin)  # ignored: its row goes
    writer.mark_dirty(spawn)
    await writer.flush()
    assert await seeded.get_session_agent("ag_twin") is None
    assert await seeded.list_agent_events("ag_twin") == []
    assert (await seeded.get_session_agent("ag_spawn"))["task_id"] == "t-1"
    assert not writer.has_pending("s1")
