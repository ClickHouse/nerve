"""The agent broadcast pump: coalescing, debounced counts, overflow."""

from __future__ import annotations

import asyncio
import logging

import pytest

from nerve.agent.agents import emitter as em
from nerve.agent.agents.emitter import AgentEmitter, agent_channel


class _Clock:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t


class _Registry:
    def __init__(self):
        self.live: dict[str, int] = {}

    def live_count(self, sid):
        return self.live.get(sid, 0)

    def agent_update_message(self, sid, ids):
        return {"type": "agent_update", "session_id": sid,
                "agents": [{"id": i} for i in sorted(ids)], "summary": {}}


def _emitter(clock=None, listening=True):
    sent: list[tuple[str, dict]] = []

    async def _sink(channel, msg):
        sent.append((channel, msg))

    emitter = AgentEmitter(sink=_sink, has_listeners=lambda ch: listening, clock=clock or _Clock())
    registry = _Registry()
    emitter.bind(registry)
    return emitter, registry, sent


@pytest.mark.asyncio
async def test_immediate_and_progress_delays():
    clock = _Clock()
    emitter, _reg, sent = _emitter(clock)
    emitter.upsert("s1", "ag_a", immediate=False)
    await emitter._deliver(force=False)
    assert sent == []
    clock.t += 0.5
    emitter.upsert("s1", "ag_b", immediate=False)  # coalesces, keeps the earlier due
    clock.t += 0.49
    await emitter._deliver(force=False)
    assert sent == []
    clock.t += 0.02  # 1 s after the first change
    await emitter._deliver(force=False)
    assert sent == [("s1", {"type": "agent_update", "session_id": "s1",
                            "agents": [{"id": "ag_a"}, {"id": "ag_b"}], "summary": {}})]
    sent.clear()
    emitter.upsert("s1", "ag_c", immediate=True)
    clock.t += 0.04
    await emitter._deliver(force=False)
    assert sent == []
    clock.t += 0.02
    await emitter._deliver(force=False)
    assert [m["agents"] for _, m in sent] == [[{"id": "ag_c"}]]


@pytest.mark.asyncio
async def test_an_immediate_change_pulls_a_progress_flush_forward():
    clock = _Clock()
    emitter, _reg, sent = _emitter(clock)
    emitter.upsert("s1", "ag_a", immediate=False)
    emitter.upsert("s1", "ag_b", immediate=True)
    clock.t += 0.06
    await emitter._deliver(force=False)
    assert [m["agents"] for _, m in sent] == [[{"id": "ag_a"}, {"id": "ag_b"}]]


@pytest.mark.asyncio
async def test_busy_sessions_coalesce_progress_over_two_seconds():
    clock = _Clock()
    emitter, reg, sent = _emitter(clock)
    reg.live["s1"] = 21
    emitter.upsert("s1", "ag_a", immediate=False)
    clock.t += 1.5
    await emitter._deliver(force=False)
    assert sent == []
    clock.t += 0.6
    await emitter._deliver(force=False)
    assert len(sent) == 1


@pytest.mark.asyncio
async def test_counts_debounced_and_sent_only_on_change():
    clock = _Clock()
    emitter, reg, sent = _emitter(clock)
    reg.live["s1"] = 2
    emitter.counts("s1")
    emitter.counts("s1")
    await emitter._deliver(force=False)
    assert sent == []
    clock.t += 1.0
    await emitter._deliver(force=False)
    assert sent == [("__global__", {"type": "session_agents", "session_id": "s1",
                                    "background_count": 2, "has_background_tasks": True})]
    sent.clear()
    emitter.counts("s1")
    await emitter.flush()
    assert sent == []  # unchanged
    reg.live["s1"] = 0
    emitter.counts("s1")
    await emitter.flush()
    assert [m["has_background_tasks"] for _, m in sent] == [False]


@pytest.mark.asyncio
async def test_events_only_for_listened_channels():
    emitter, _reg, sent = _emitter(listening=False)
    emitter.event("s1", "ag_a", {"seq": 1})
    await emitter.flush()
    assert sent == []
    emitter, _reg, sent = _emitter(listening=True)
    emitter.event("s1", "ag_a", {"seq": 1})
    await emitter.flush()
    assert sent == [(agent_channel("s1", "ag_a"), {
        "type": "agent_event", "session_id": "s1", "agent_id": "ag_a", "item": {"seq": 1},
    })]


@pytest.mark.asyncio
async def test_overflow_drops_oldest_events_and_warns(monkeypatch, caplog):
    monkeypatch.setattr(em, "MAX_QUEUED", 5)
    emitter, _reg, sent = _emitter()
    with caplog.at_level(logging.WARNING, logger="nerve.agent.agents.emitter"):
        for seq in range(1, 9):
            emitter.event("s1", "ag_a", {"seq": seq})
    await emitter.flush()
    assert [m["item"]["seq"] for _, m in sent] == [4, 5, 6, 7, 8]
    warnings = [r for r in caplog.records if "dropped" in r.getMessage()]
    assert len(warnings) == 1  # at most once a minute


@pytest.mark.asyncio
async def test_forget_drops_pending_for_the_session():
    emitter, _reg, sent = _emitter()
    emitter.upsert("s1", "ag_a", immediate=True)
    emitter.event("s1", "ag_a", {"seq": 1})
    emitter.counts("s1")
    emitter.upsert("s2", "ag_b", immediate=True)
    emitter.forget("s1")
    await emitter.flush()
    assert [ch for ch, _ in sent] == ["s2"]


@pytest.mark.asyncio
async def test_the_pump_delivers_and_close_flushes():
    emitter, _reg, sent = _emitter(clock=None)
    emitter._clock = asyncio.get_running_loop().time  # real time for the pump
    emitter.start()
    try:
        emitter.upsert("s1", "ag_a", immediate=True)
        for _ in range(100):
            if sent:
                break
            await asyncio.sleep(0.01)
        assert [m["type"] for _, m in sent] == ["agent_update"]
        emitter.upsert("s1", "ag_b", immediate=False)  # due in 1 s
    finally:
        await emitter.close()  # flushes what is still pending
    assert [m["agents"] for _, m in sent][-1] == [{"id": "ag_b"}]


@pytest.mark.asyncio
async def test_a_failing_sink_never_stops_the_pump():
    calls = []

    async def _sink(channel, msg):
        calls.append(channel)
        raise RuntimeError("socket gone")

    emitter = AgentEmitter(sink=_sink, has_listeners=lambda ch: True)
    emitter.bind(_Registry())
    emitter.event("s1", "ag_a", {"seq": 1})
    emitter.event("s1", "ag_a", {"seq": 2})
    await emitter.flush()
    assert len(calls) == 2
