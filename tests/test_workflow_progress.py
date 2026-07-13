"""Tests for dynamic-workflow (Claude Code Workflow tool) progress support:
snapshot parsing, status mapping, name derivation, the broadcast envelope,
and out-of-band persistence onto the Workflow tool_call block."""

import pytest

from nerve.agent.engine import AgentEngine
from nerve.agent.streaming import StreamBroadcaster
from nerve.db import Database


# A representative ``workflow_progress`` tree as emitted by the CLI on a
# task_progress message (flat list of phase + agent entries).
WP_TREE = [
    {"type": "workflow_phase", "index": 1, "title": "Scope"},
    {"type": "workflow_phase", "index": 2, "title": "Search"},
    {
        "type": "workflow_agent", "label": "scope", "phaseIndex": 1,
        "phaseTitle": "Scope", "state": "done", "model": "claude-opus-4-8",
        "tokens": 27172, "toolCalls": 1, "lastToolName": "StructuredOutput",
        "lastToolSummary": "x" * 400, "durationMs": 26648,
    },
    {
        "type": "workflow_agent", "label": "search:a", "phaseIndex": 2,
        "phaseTitle": "Search", "state": "running", "model": "claude-opus-4-8",
        "tokens": 5000, "toolCalls": 3, "lastToolName": "WebSearch",
    },
]


class TestBuildSnapshot:
    def test_parses_phases_agents_and_totals(self):
        snap = AgentEngine._build_workflow_snapshot(WP_TREE)
        assert [p["title"] for p in snap["phases"]] == ["Scope", "Search"]
        assert snap["agentCount"] == 2
        assert snap["totalTokens"] == 27172 + 5000
        assert snap["totalToolCalls"] == 1 + 3
        # Agent fields preserved
        a0 = snap["agents"][0]
        assert a0["label"] == "scope"
        assert a0["state"] == "done"
        assert a0["phaseIndex"] == 1
        assert a0["lastToolName"] == "StructuredOutput"

    def test_truncates_long_tool_summary(self):
        snap = AgentEngine._build_workflow_snapshot(WP_TREE)
        assert len(snap["agents"][0]["lastToolSummary"]) == 200

    def test_ignores_malformed_entries(self):
        snap = AgentEngine._build_workflow_snapshot(
            [None, "bad", {"type": "unknown"}, {"type": "workflow_phase", "index": 1, "title": "P"}]
        )
        assert snap["agentCount"] == 0
        assert len(snap["phases"]) == 1

    def test_handles_missing_token_fields(self):
        snap = AgentEngine._build_workflow_snapshot(
            [{"type": "workflow_agent", "label": "x", "state": "queued"}]
        )
        assert snap["totalTokens"] == 0
        assert snap["totalToolCalls"] == 0


class TestWorkflowStatus:
    def test_started_and_progress_are_running(self):
        assert AgentEngine._workflow_status("task_started", {}) == "running"
        assert AgentEngine._workflow_status("task_progress", {}) == "running"

    def test_notification_status_passes_through(self):
        assert AgentEngine._workflow_status(
            "task_notification", {"status": "completed"}
        ) == "completed"
        assert AgentEngine._workflow_status(
            "task_notification", {"status": "failed"}
        ) == "failed"

    def test_updated_killed_maps_to_stopped(self):
        assert AgentEngine._workflow_status(
            "task_updated", {"patch": {"status": "killed"}}
        ) == "stopped"

    def test_updated_without_status_stays_running(self):
        assert AgentEngine._workflow_status(
            "task_updated", {"patch": {"end_time": 1}}
        ) == "running"


class TestDeriveName:
    def test_named_workflow(self):
        assert AgentEngine._derive_workflow_name({"name": "deep-research"}) == "deep-research"

    def test_inline_script_meta_name(self):
        script = "export const meta = {\n  name: 'find-flaky-tests',\n  description: '...'\n}"
        assert AgentEngine._derive_workflow_name({"script": script}) == "find-flaky-tests"

    def test_fallback(self):
        assert AgentEngine._derive_workflow_name({}) == "Workflow"
        assert AgentEngine._derive_workflow_name(None) == "Workflow"


class TestFoldSnapshots:
    def test_folds_cached_snapshot_onto_workflow_block(self):
        blocks = [
            {"type": "text", "content": "hi"},
            {"type": "tool_call", "tool": "Workflow", "tool_use_id": "wf-1", "input": {}},
            {"type": "tool_call", "tool": "Bash", "tool_use_id": "b-1", "input": {}},
        ]
        wf_reg = {"wf-1": {"name": "x", "snapshot": {"status": "completed", "agents": []}}}
        AgentEngine._fold_workflow_snapshots(blocks, wf_reg)
        assert blocks[1]["workflow"] == {"status": "completed", "agents": []}
        assert "workflow" not in blocks[2]  # non-matching block untouched

    def test_noop_without_registry_or_blocks(self):
        # Should not raise on empty/None inputs.
        AgentEngine._fold_workflow_snapshots(None, {"wf-1": {"snapshot": {}}})
        AgentEngine._fold_workflow_snapshots([], None)
        blocks = [{"type": "tool_call", "tool": "Workflow", "tool_use_id": "wf-1"}]
        AgentEngine._fold_workflow_snapshots(blocks, {"wf-1": {"snapshot": None}})
        assert "workflow" not in blocks[0]  # cached snapshot is None → skip


@pytest.mark.asyncio
class TestBroadcastEnvelope:
    async def test_workflow_progress_envelope(self):
        bc = StreamBroadcaster()
        received = []

        async def handler(sid, msg):
            received.append(msg)

        await bc.register("s1", "l1", handler)
        snap = {"name": "wf", "status": "running", "phases": [], "agents": []}
        await bc.broadcast_workflow_progress("s1", "wf-tool-1", snap)

        assert len(received) == 1
        msg = received[0]
        assert msg["type"] == "workflow_progress"
        assert msg["tool_use_id"] == "wf-tool-1"
        assert msg["workflow"] is snap


@pytest.mark.asyncio
class TestMergeWorkflowIntoCall:
    async def test_merges_snapshot_onto_matching_block(self, db: Database):
        await db.create_session("wf-sess", title="wf", source="web")
        await db.add_message(
            "wf-sess", "assistant", "",
            blocks=[
                {"type": "text", "content": "running a workflow"},
                {"type": "tool_call", "tool": "Workflow", "tool_use_id": "wf-1", "input": {}},
            ],
        )
        snap = {"name": "deep-research", "status": "completed", "phases": [], "agents": []}
        msg_id = await db.merge_workflow_into_call("wf-sess", "wf-1", snap)
        assert msg_id is not None

        messages = await db.get_messages("wf-sess")
        tool_block = next(b for b in messages[0]["blocks"] if b["type"] == "tool_call")
        assert tool_block["workflow"] == snap

    async def test_returns_none_when_no_matching_block(self, db: Database):
        await db.create_session("wf-sess2", title="wf2", source="web")
        await db.add_message(
            "wf-sess2", "assistant", "",
            blocks=[{"type": "tool_call", "tool": "Bash", "tool_use_id": "other", "input": {}}],
        )
        assert await db.merge_workflow_into_call("wf-sess2", "missing-id", {"x": 1}) is None
