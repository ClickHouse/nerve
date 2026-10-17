"""LiveWork: the work in a Codex app-server that an end of the process stops."""

from __future__ import annotations

from nerve.agent.backends.codex.live_work import IDLE, LiveWork

MAIN = "th-main"


def _item(method, item, thread=MAIN, turn="turn-1"):
    return method, {"threadId": thread, "turnId": turn, "item": item}


def _blockers(work):
    return sorted(work.blockers(MAIN))


def test_an_open_command_outlives_its_turn():
    work = LiveWork()
    work.observe("turn/started", {"threadId": MAIN, "turn": {"id": "turn-1"}})
    assert _blockers(work) == ["active_turn"]
    work.observe(*_item("item/started", {"id": "c1", "type": "commandExecution"}))
    work.observe(*_item("item/started", {"id": "m1", "type": "mcpToolCall"}))
    work.observe("turn/completed", {"threadId": MAIN, "turn": {"id": "turn-1"}})
    assert _blockers(work) == ["command"]
    work.observe(*_item("item/completed", {"id": "c1", "type": "commandExecution"}))
    assert _blockers(work) == []


def test_child_agents_follow_the_last_evidence():
    work = LiveWork()
    spawn = {
        "id": "a1", "type": "collabAgentToolCall", "tool": "spawnAgent",
        "receiverThreadIds": ["child"], "agentsStates": {},
    }
    work.observe(*_item("item/started", spawn))
    work.observe(*_item("item/completed", spawn))
    # Spawned, no state reported: it may run.
    assert _blockers(work) == ["state_unknown"]
    work.observe(*_item("item/completed", {
        **spawn, "id": "a2", "tool": "wait",
        "agentsStates": {"child": {"status": "running"}},
    }))
    assert _blockers(work) == ["child_agent"]
    work.observe("turn/completed", {"threadId": "child", "turn": {"id": "x"}})
    assert _blockers(work) == []
    work.observe("thread/status/changed", {
        "threadId": "child", "status": {"type": "active", "activeFlags": []},
    })
    assert _blockers(work) == ["child_agent"]
    work.observe(*_item("item/completed", {
        **spawn, "id": "a3", "tool": "closeAgent",
        "agentsStates": {"child": {"status": "shutdown"}},
    }))
    assert _blockers(work) == []


def test_an_unknown_status_is_live_work():
    work = LiveWork()
    work.observe("thread/status/changed", {"threadId": "child", "status": {"type": "later"}})
    assert _blockers(work) == ["state_unknown"]


def test_the_app_server_report_settles_threads():
    work = LiveWork()
    work.observe(*_item("item/completed", {
        "id": "a1", "type": "collabAgentToolCall", "receiverThreadIds": ["gone", "busy"],
    }))
    work.observe(*_item("item/started", {"id": "c1", "type": "commandExecution"}))
    work.observe(*_item("item/started", {"id": "x1", "type": "webSearch"}, thread="gone"))
    work.take_status({MAIN: IDLE, "busy": "active"})
    # "gone" is not loaded: nothing runs in it. The command stays open.
    assert _blockers(work) == ["child_agent", "command"]
    work.take_status({MAIN: IDLE, "busy": IDLE})
    assert _blockers(work) == ["command"]
