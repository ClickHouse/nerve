"""The agent record model: the AgentView contract, caps, kinds, activity."""

from __future__ import annotations

import json
import re
import time

import pytest

from nerve.agent.agents import model as m
from nerve.agent.agents.model import AgentRecord

# Key for key, in order, as documented in docs/api.md (AgentView).
AGENT_VIEW_KEYS = [
    "id", "session_id", "backend", "kind", "task_id", "task_type",
    "spawn_tool_use_id", "parent_agent_id", "depth", "name", "label",
    "description", "agent_type", "command", "background", "ambient", "status",
    "end_reason", "error", "attention", "activity", "summary", "result_preview",
    "requested_model", "model", "model_changed", "tokens", "tool_uses",
    "cost_usd", "runs", "created_at", "started_at", "run_started_at",
    "last_activity_at", "ended_at", "stop_requested_at", "stopped_by_actor_id",
    "resumable", "controls", "workflow", "rev",
]


def _rec(**kw) -> AgentRecord:
    return AgentRecord(id="ag_3f9a1c2b7d4e", session_id="s-1234", **kw)


def test_view_has_exactly_the_contract_keys():
    view = m.to_view(_rec())
    assert list(view) == AGENT_VIEW_KEYS
    assert list(view["controls"]) == ["stop", "transcript", "output", "message"]
    assert list(m.brief(_rec())) == ["id", "label", "kind", "status", "started_at", "tokens"]
    assert list(m.trigger_of(_rec())) == ["agent_id", "label", "kind", "status"]
    json.dumps(view)  # serializable as is


def test_new_agent_id_shape():
    assert re.fullmatch(r"ag_[0-9a-f]{12}", m.new_agent_id())
    assert m.new_agent_id() != m.new_agent_id()


def test_iso_z_is_utc_fixed_width_and_parses_back():
    stamp = m.iso_z(1_772_150_400.25)
    assert stamp.endswith("Z") and len(stamp) == len("2026-02-27T02:01:00.000Z")
    assert m.parse_iso(stamp) == pytest.approx(1_772_150_400.25, abs=0.001)
    assert m.parse_iso(None) is None and m.parse_iso("not a time") is None


def test_rev_strictly_increases_and_tracks_wall_time():
    before = time.time_ns() // 1000
    rev = m.next_rev(0)
    assert rev >= before
    assert m.next_rev(rev) > rev
    far = 10**18
    assert m.next_rev(far) == far + 1


@pytest.mark.parametrize("kw, label", [
    ({"name": "build-api", "description": "Build the API"}, "build-api"),
    ({"description": "Build the API"}, "Build the API"),
    ({"command": "x" * 80}, "x" * 60),
    ({"task_type": "local_bash"}, "local_bash"),
    ({"kind": "monitor"}, "monitor"),
])
def test_label_precedence(kw, label):
    assert m.to_view(_rec(**kw))["label"] == label


@pytest.mark.parametrize("kw, attention", [
    ({"status": "running", "pending_interaction": True}, "needs_input"),
    ({"status": "failed"}, "failed"),
    ({"status": "interrupted"}, "interrupted"),
    ({"status": "lost"}, "lost"),
    ({"status": "stopping", "stop_unconfirmed": True}, "stop_unconfirmed"),
    ({"status": "stopping"}, None),
    ({"status": "completed"}, None),
    ({"status": "stopped"}, None),
])
def test_attention(kw, attention):
    assert m.to_view(_rec(**kw))["attention"] == attention


def test_model_and_model_changed():
    view = m.to_view(_rec(models=["model-a"]))
    assert view["model"] == "model-a" and view["model_changed"] is None
    view = m.to_view(_rec(models=["model-a", "model-b"], model_changed_at="2026-02-27T02:49:00.000Z"))
    assert view["model"] == "model-b"
    assert view["model_changed"] == {
        "from": "model-a", "to": "model-b", "at": "2026-02-27T02:49:00.000Z",
    }


def test_result_preview_first_line_of_result_then_summary():
    assert m.to_view(_rec(result_text="\n\n4 files changed\nmore"))["result_preview"] == "4 files changed"
    assert m.to_view(_rec(summary="Running the suite"))["result_preview"] == "Running the suite"
    assert len(m.to_view(_rec(result_text="y" * 500))["result_preview"]) == 200


def test_controls_and_resumable():
    live = _rec(status="running", kind="agent")
    view = m.to_view(live, stop_supported=True, agent_teams=True, live_client_gen=3)
    assert view["controls"] == {"stop": True, "transcript": True, "output": False, "message": True}
    assert m.to_view(live, stop_supported=False)["controls"]["stop"] is False
    assert m.to_view(_rec(status="running", ambient=True), stop_supported=True)["controls"]["stop"] is False

    done = _rec(status="completed", end_reason="completed", client_gen=3)
    assert m.to_view(done, agent_teams=True, live_client_gen=3)["resumable"] is True
    assert m.to_view(done, agent_teams=True, live_client_gen=3)["controls"]["message"] is True
    assert m.to_view(done, agent_teams=True, live_client_gen=4)["resumable"] is False
    stopped = _rec(status="failed", end_reason="user", client_gen=3)
    assert m.to_view(stopped, agent_teams=True, live_client_gen=3)["resumable"] is False
    assert m.to_view(stopped, agent_teams=True, live_client_gen=3)["controls"]["message"] is False

    shell = _rec(kind="shell", status="running", output_file="/tmp/x/tasks/b1.output")
    assert m.to_view(shell)["controls"] == {
        "stop": False, "transcript": False, "output": True, "message": False,
    }
    codex = _rec(backend="codex", kind="agent", status="running")
    assert m.to_view(codex, stop_supported=False, agent_teams=True)["controls"] == {
        "stop": False, "transcript": False, "output": False, "message": False,
    }


def test_workflow_snapshot_only_for_workflow_kind():
    snap = {"name": "wf", "status": "running"}
    assert m.to_view(_rec(kind="workflow", workflow=snap))["workflow"] == snap
    assert m.to_view(_rec(kind="agent", workflow=snap))["workflow"] is None


@pytest.mark.parametrize("task_type, spawn_tool, expected", [
    ("local_agent", None, ("agent", False)),
    ("local_workflow", None, ("workflow", False)),
    ("in_process_teammate", None, ("teammate", False)),
    ("remote_agent", None, ("remote_agent", False)),
    ("mcp_task", None, ("mcp_task", False)),
    ("local_bash", None, ("shell", False)),
    ("local_bash", "Monitor", ("monitor", False)),
    ("monitor_mcp", None, ("monitor", False)),
    ("monitor_ws", None, ("monitor", False)),
    ("dream", None, ("other", True)),
    ("auto_mode_scan", None, ("other", True)),
    ("something_new", None, ("other", False)),
    (None, None, ("other", False)),
])
def test_kind_for_task_type(task_type, spawn_tool, expected):
    assert m.kind_for_task_type(task_type, spawn_tool) == expected


@pytest.mark.parametrize("tool, tool_input, kind", [
    ("Agent", {}, "agent"),
    ("Task", {}, "agent"),
    ("Workflow", {}, "workflow"),
    ("Monitor", {}, "monitor"),
    ("Bash", {"run_in_background": True}, "shell"),
    ("Bash", {"run_in_background": "true"}, None),
    ("Bash", {}, None),
    ("Read", {}, None),
])
def test_kind_for_spawn(tool, tool_input, kind):
    assert m.kind_for_spawn(tool, tool_input) == kind


def test_cap_text_cuts_on_a_character_boundary():
    text, truncated, size = m.cap_text("é" * 10, 5)  # 2 bytes each
    assert (text, truncated, size) == ("éé", True, 20)
    assert m.cap_text("short", 100) == ("short", False, 5)
    assert m.cap_text(None, 10) == ("", False, 0)
    # Orphaned surrogates never reach a UTF-8 encoder.
    assert m.cap_text("a\ud800b", 10)[0] == "a�b"


def test_cap_tool_input_keeps_structure_and_reports_size():
    small = {"command": "pytest -q"}
    assert m.cap_tool_input(small, 16_384) == (small, False, len(json.dumps(small)))
    big = {"file_path": "/tmp/a.py", "old_string": "x" * 40_000, "new_string": "y" * 40_000}
    capped, truncated, size = m.cap_tool_input(big, 16_384)
    assert truncated is True and size > 80_000
    assert set(capped) == set(big) and capped["file_path"] == "/tmp/a.py"
    assert len(json.dumps(capped).encode()) <= 16_384
    # Pathological shapes still fit, as a preview.
    deep = {"k": [{"x": "z" * 100} for _ in range(5000)]}
    capped, truncated, _ = m.cap_tool_input(deep, 1024)
    assert truncated and len(json.dumps(capped).encode()) <= 1024


@pytest.mark.parametrize("tool, tool_input, line", [
    ("Bash", {"command": "pytest -q\necho done"}, "Bash: pytest -q"),
    ("Read", {"file_path": "/repo/src/app.py"}, "Read: app.py"),
    ("NotebookEdit", {"notebook_path": "/repo/n.ipynb"}, "NotebookEdit: n.ipynb"),
    ("Grep", {"pattern": "TODO"}, "Grep: TODO"),
    ("Glob", {"pattern": "**/*.py"}, "Glob: **/*.py"),
    ("WebFetch", {"url": "https://docs.example.com/page"}, "WebFetch: docs.example.com"),
    ("WebSearch", {"query": "release notes"}, "WebSearch: release notes"),
    ("Agent", {"description": "review-api"}, "Agent: review-api"),
    ("mcp__nerve__memory_recall", {}, "nerve/memory_recall"),
    ("TodoWrite", {}, "TodoWrite"),
    ("Bash", {}, "Bash"),
])
def test_summarize_activity(tool, tool_input, line):
    assert m.summarize_activity(tool, tool_input) == line


def test_summarize_activity_is_capped():
    assert len(m.summarize_activity("Bash", {"command": "x" * 500})) == 160


def test_row_round_trip_through_json_columns():
    rec = _rec(route_ids={"b", "a"}, models=["m1"], usage={"input_tokens": 3}, workflow=None)
    row = m.to_row(rec, updated_at="2026-02-27T02:01:00.000Z")
    assert list(row) == list(m.ROW_COLUMNS)
    assert row["route_ids"] == ["a", "b"] and row["background"] == 1
    encoded = {k: json.dumps(v) if k in m.JSON_COLUMNS else v for k, v in row.items()}
    back = m.from_row(encoded)
    assert back.route_ids == {"a", "b"} and back.models == ["m1"]
    assert back.usage == {"input_tokens": 3} and back.workflow is None
