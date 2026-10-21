"""Agent records and transcript items in SQLite (v058, ``AgentStore``)."""

from __future__ import annotations

import json
import sqlite3
import time

import pytest
import pytest_asyncio

from nerve.agent.agents.model import AgentRecord, from_row, iso_z, to_row
from nerve.db import Database
from nerve.db.actor_columns import ACTOR_COLUMNS
from nerve.db.migrations.v058_session_agents import up as v058_up
from nerve.identity import Attribution

from tests.actor_rows import ensure_actor_row
from tests.conftest import TEST_ACTOR

T0 = 1_772_150_400.0  # a fixed instant (placeholder date in 2026)


def _rec(agent_id: str, session_id: str = "s1", **kw) -> AgentRecord:
    rec = AgentRecord(id=agent_id, session_id=session_id, **kw)
    rec.created_at = rec.created_at or iso_z(T0)
    return rec


def _row(rec: AgentRecord) -> dict:
    return to_row(rec, updated_at=iso_z(T0 + 1))


def _item(seq: int, kind: str = "text", **kw) -> dict:
    return {"seq": seq, "at": iso_z(T0 + seq), "kind": kind, **kw}


def _event_rows(agent_id: str, session_id: str, items: list[dict]) -> list[tuple]:
    return [
        (agent_id, session_id, it["seq"], it["kind"], json.dumps(it), it["at"])
        for it in items
    ]


@pytest_asyncio.fixture
async def seeded(db: Database):
    await db.create_session("s1", source="web", actor=None)
    await db.create_session("s2", source="web", actor=None)
    return db


async def _tables(db: Database) -> set[str]:
    async with db.db.execute("SELECT name FROM sqlite_master WHERE type='table'") as cur:
        return {r[0] async for r in cur}


@pytest.mark.asyncio
async def test_v058_creates_tables_and_is_idempotent(db: Database):
    assert {"session_agents", "session_agent_events"} <= await _tables(db)
    await v058_up(db.db)  # a second run does nothing
    await v058_up(db.db)
    await db.db.commit()
    assert {"session_agents", "session_agent_events"} <= await _tables(db)


@pytest.mark.asyncio
async def test_actor_columns_registry_lists_the_stop_attribution():
    assert ACTOR_COLUMNS[-2:] == (
        ("session_agents", "stopped_by_actor_id"),
        ("session_agents", "stopped_caused_by_actor_id"),
    )


@pytest.mark.asyncio
async def test_upsert_round_trips_every_column(seeded: Database):
    rec = _rec(
        "ag_000000000001", kind="agent", task_id="a1b2c3d4e5f6a7b8c",
        task_type="local_agent", spawn_tool_use_id="toolu_01AGENT",
        route_ids={"toolu_01AGENT", "toolu_02RESUME"}, depth=2,
        name="build-api", description="Build the API", agent_type="general-purpose",
        prompt="Build it", requested_model="opus", models=["model-a", "model-b"],
        model_changed_at=iso_z(T0 + 5), background=False, status="running",
        activity="Bash: pytest -q", summary="Running tests", tokens=42,
        tool_uses=3, usage={"input_tokens": 1, "output_tokens": 2}, cost_usd=0.25,
        runs=2, workflow={"name": "wf"}, rev=17,
    )
    await seeded.upsert_session_agents([_row(rec)])
    row = await seeded.get_session_agent("ag_000000000001")
    assert row["route_ids"] == ["toolu_01AGENT", "toolu_02RESUME"]
    assert row["models"] == ["model-a", "model-b"]
    assert row["usage"] == {"input_tokens": 1, "output_tokens": 2}
    assert row["workflow"] == {"name": "wf"}
    assert row["background"] is False and row["ambient"] is False
    back = from_row(row)
    for key in (
        "id", "session_id", "kind", "task_id", "task_type", "spawn_tool_use_id",
        "route_ids", "depth", "name", "description", "agent_type", "prompt",
        "requested_model", "models", "model_changed_at", "background", "status",
        "activity", "summary", "tokens", "tool_uses", "usage", "cost_usd", "runs",
        "workflow", "rev", "created_at",
    ):
        assert getattr(back, key) == getattr(rec, key), key
    # Persisted usage counts as settled history for later frames.
    assert back.usage_folded == rec.usage and back.cost_folded == rec.cost_usd


@pytest.mark.asyncio
async def test_upsert_keeps_identity_and_birth(seeded: Database):
    rec = _rec("ag_000000000002", status="starting")
    await seeded.upsert_session_agents([_row(rec)])
    changed = _row(rec)
    changed.update(status="completed", created_at=iso_z(T0 + 999), tokens=5)
    await seeded.upsert_session_agents([changed])
    row = await seeded.get_session_agent("ag_000000000002")
    assert row["status"] == "completed" and row["tokens"] == 5
    assert row["created_at"] == iso_z(T0)  # never rewritten


@pytest.mark.asyncio
async def test_foreign_keys_and_unique_task(seeded: Database):
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
        await seeded.upsert_session_agents([_row(_rec("ag_x", session_id="gone"))])
    await seeded.upsert_session_agents([_row(_rec("ag_a", task_id="t1"))])
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        await seeded.upsert_session_agents([_row(_rec("ag_b", task_id="t1"))])
    # The same task id in another session is fine.
    await seeded.upsert_session_agents([_row(_rec("ag_c", session_id="s2", task_id="t1"))])


@pytest.mark.asyncio
async def test_stop_attribution_must_name_an_actor(seeded: Database):
    rec = _rec("ag_stop", status="stopping", stopped_by_actor_id="no-such-actor")
    with pytest.raises(sqlite3.IntegrityError):
        await seeded.upsert_session_agents([_row(rec)])
    await ensure_actor_row(seeded, TEST_ACTOR)
    rec.stopped_by_actor_id = TEST_ACTOR.actor_id
    await seeded.upsert_session_agents([_row(rec)])
    row = await seeded.get_session_agent("ag_stop")
    assert row["stopped_by_actor_id"] == TEST_ACTOR.actor_id


@pytest.mark.asyncio
async def test_events_cursors_and_idempotent_insert(seeded: Database):
    await seeded.upsert_session_agents([_row(_rec("ag_ev"))])
    items = [_item(i, text=f"t{i}") for i in range(1, 11)]
    rows = _event_rows("ag_ev", "s1", items)
    await seeded.insert_agent_events(rows)
    await seeded.insert_agent_events(rows[:3])  # a retry inserts nothing twice

    tail = await seeded.list_agent_events("ag_ev", limit=4)
    assert [i["seq"] for i in tail] == [7, 8, 9, 10]
    assert tail[0] == items[6]
    before = await seeded.list_agent_events("ag_ev", before_seq=7, limit=3)
    assert [i["seq"] for i in before] == [4, 5, 6]
    after = await seeded.list_agent_events("ag_ev", after_seq=8, limit=5)
    assert [i["seq"] for i in after] == [9, 10]
    assert await seeded.max_agent_event_seqs(["ag_ev", "ag_none"]) == {"ag_ev": 10}


@pytest.mark.asyncio
async def test_write_agent_batch_is_one_transaction(seeded: Database):
    """Records first, then items, atomically: a bad item rolls the records
    back too, so a retry starts from a clean state."""
    rec = _rec("ag_batch")
    bad = [("ag_batch", "s1", 1, "text", "{}", iso_z(T0)), ("ag_unknown", "s1", 1, "text", "{}", iso_z(T0))]
    with pytest.raises(sqlite3.IntegrityError):
        await seeded.write_agent_batch([_row(rec)], bad)
    assert await seeded.get_session_agent("ag_batch") is None
    await seeded.write_agent_batch([_row(rec)], bad[:1])
    assert await seeded.get_session_agent("ag_batch") is not None
    assert [i["seq"] for i in await seeded.list_agent_events("ag_batch")] == [1]


@pytest.mark.asyncio
async def test_list_session_agents_keeps_live_and_newest(seeded: Database):
    rows = []
    for i in range(6):
        status = "running" if i == 0 else "completed"
        rows.append(_row(_rec(f"ag_{i:012d}", status=status, created_at=iso_z(T0 + i))))
    await seeded.upsert_session_agents(rows)
    listed = await seeded.list_session_agents("s1", limit=2)
    # The oldest record is live, so it stays; plus the two newest; ascending.
    assert [r["id"] for r in listed] == [
        "ag_000000000000", "ag_000000000004", "ag_000000000005",
    ]
    assert await seeded.list_session_agents("s2") == []


@pytest.mark.asyncio
async def test_delete_session_removes_records_and_items(seeded: Database):
    await seeded.upsert_session_agents([_row(_rec("ag_del")), _row(_rec("ag_keep", session_id="s2"))])
    await seeded.insert_agent_events(
        _event_rows("ag_del", "s1", [_item(1)]) + _event_rows("ag_keep", "s2", [_item(1)]),
    )
    assert await seeded.delete_session("s1", attribution=Attribution(None, None)) is True
    assert await seeded.get_session_agent("ag_del") is None
    assert await seeded.list_agent_events("ag_del") == []
    # Other sessions are untouched.
    assert await seeded.get_session_agent("ag_keep") is not None
    assert len(await seeded.list_agent_events("ag_keep")) == 1


@pytest.mark.asyncio
async def test_mark_live_agents_interrupted(seeded: Database):
    await seeded.upsert_session_agents([
        _row(_rec("ag_live1", status="running", rev=5)),
        _row(_rec("ag_live2", status="stopping")),
        _row(_rec("ag_done", status="completed", end_reason="completed")),
        _row(_rec("ag_other", session_id="s2", status="starting")),
    ])
    await seeded.insert_agent_events(_event_rows("ag_live1", "s1", [_item(1), _item(2)]))
    now = iso_z(T0 + 100)
    n = await seeded.mark_live_agents_interrupted(
        reason="gateway_restart", now=now, rev=1_000, session_id="s1",
    )
    assert n == 2
    live1 = await seeded.get_session_agent("ag_live1")
    assert (live1["status"], live1["end_reason"], live1["ended_at"]) == (
        "interrupted", "gateway_restart", now,
    )
    assert live1["rev"] == 1_000
    assert (await seeded.get_session_agent("ag_done"))["status"] == "completed"
    assert (await seeded.get_session_agent("ag_other"))["status"] == "starting"
    # Each settled record got a lifecycle item after its last one.
    (lifecycle,) = await seeded.list_agent_events("ag_live1", after_seq=2)
    assert lifecycle["kind"] == "lifecycle"
    assert lifecycle["event"] == "interrupted" and lifecycle["reason"] == "gateway_restart"
    assert lifecycle["seq"] == 3
    # Without a session filter every remaining live row is settled.
    assert await seeded.mark_live_agents_interrupted(
        reason="gateway_restart", now=now, rev=2_000,
    ) == 1
    assert (await seeded.get_session_agent("ag_other"))["status"] == "interrupted"


@pytest.mark.asyncio
async def test_mark_agents_notified(seeded: Database):
    await seeded.upsert_session_agents([_row(_rec("ag_n1")), _row(_rec("ag_n2"))])
    await seeded.mark_agents_notified(["ag_n1"], iso_z(T0 + 3))
    assert (await seeded.get_session_agent("ag_n1"))["orchestrator_notified_at"] == iso_z(T0 + 3)
    assert (await seeded.get_session_agent("ag_n2"))["orchestrator_notified_at"] is None


@pytest.mark.asyncio
async def test_telemetry_retention_prunes_old_transcript_items(seeded: Database):
    await seeded.upsert_session_agents([_row(_rec("ag_old"))])
    old = {"seq": 1, "at": "2020-01-01T00:00:00.000Z", "kind": "text", "text": "old"}
    new = {**_item(2, text="new"), "at": iso_z(time.time())}  # inside the retention window
    await seeded.insert_agent_events(_event_rows("ag_old", "s1", [old, new]))
    report = await seeded.prune_telemetry(days=90)
    assert report["by_table"]["session_agent_events"] == 1
    assert [i["seq"] for i in await seeded.list_agent_events("ag_old")] == [2]
    # The record itself is kept for the session's life.
    assert await seeded.get_session_agent("ag_old") is not None
