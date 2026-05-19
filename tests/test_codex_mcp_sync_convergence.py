"""End-to-end convergence test.

Asserts that the external MCP server (creates satellites for Codex
tool calls) and the rollout sync (creates satellites for transcripts)
end up using the SAME session id for the same Codex thread —
``codex:<thread_id>``.

Without this, downstream systems see two satellite sessions for one
conversation and dedup/memory recall both degrade.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from nerve.mcp_server.session import SatelliteSessionResolver
from nerve.sources.codex_threads.base import ThreadEvent, WorkspaceFilter
from nerve.sources.codex_threads.ingester import (
    CodexIngester,
    codex_session_id,
)


class _NullBroadcaster:
    async def broadcast(self, session_id, payload):
        return


@pytest.mark.asyncio
async def test_mcp_server_with_codex_thread_id_uses_canonical_session_id(db):
    """When Codex's MCP client passes its thread UUID, the satellite
    session id is the canonical ``codex:<thread_id>`` — matching what
    the rollout sync produces."""
    resolver = SatelliteSessionResolver(db)
    thread_id = "11111111-2222-3333-4444-555555555555"
    sid = await resolver.resolve(
        client_name="codex",
        mcp_session_id="transport-xyz",
        client_session_id=thread_id,
    )
    assert sid == f"codex:{thread_id}"


@pytest.mark.asyncio
async def test_mcp_server_without_thread_id_keeps_legacy_format(db):
    """When the client doesn't supply a UUID-shaped id, we fall back to
    the legacy ``external:<client>:<id>`` format."""
    resolver = SatelliteSessionResolver(db)
    sid = await resolver.resolve(
        client_name="codex",
        mcp_session_id="abcd",
        client_session_id="not-a-uuid",
    )
    assert sid.startswith("external:codex:")


@pytest.mark.asyncio
async def test_sync_source_finds_mcp_created_session(db, tmp_path):
    """If the MCP server gets there first (Codex sent a tool call
    before its rollout flushed), the sync source must NOT create a
    second session — it must merge into the existing one."""
    resolver = SatelliteSessionResolver(db)
    thread_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    # MCP server creates the satellite first via a Codex tool call
    mcp_sid = await resolver.resolve(
        client_name="codex",
        mcp_session_id="transport-1",
        client_session_id=thread_id,
    )

    # Rollout sync sees the same thread_id arrive
    ing = CodexIngester(
        db, origin_id="local-pi",
        workspace_filter=WorkspaceFilter(
            mode="nerve_workspace",
            nerve_workspace_path=tmp_path,
        ),
        broadcaster=_NullBroadcaster(),
    )
    await ing.ingest(ThreadEvent(
        type="thread_in_scope",
        thread_id=thread_id,
        sequence=1,
        timestamp=datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc),
        payload={
            "id": thread_id,
            "cwd": str(tmp_path),
            "originator": "codex_tui",
            "source": "tui",
            "cli_version": "0.130.0",
            "model_provider": "openai",
            "base_instructions": {"text": ""},
        },
    ))
    sync_sid = codex_session_id(thread_id)
    assert sync_sid == mcp_sid

    # And only ONE row exists.
    sessions = await db.list_sessions(limit=50)
    matches = [s for s in sessions if s["id"] == mcp_sid]
    assert len(matches) == 1

    # Metadata records both origins.
    meta = json.loads(matches[0]["metadata"])
    assert "local-pi" in meta["origin_ids"]
    assert "nerve-mcp-detected" in meta["origin_ids"]


@pytest.mark.asyncio
async def test_tool_call_via_mcp_dedups_with_synced_tool_call(db, tmp_path):
    """The unique index on (session_id, external_id) drops duplicates
    whether the tool call is recorded via MCP or via the rollout."""
    thread_id = "aaaaaaaa-1234-5678-9abc-def012345678"
    sid = codex_session_id(thread_id)

    # MCP server creates the session by routing a tool call.
    resolver = SatelliteSessionResolver(db)
    await resolver.resolve(
        client_name="codex",
        mcp_session_id="t-1",
        client_session_id=thread_id,
    )

    # Manually mirror what the external_tool_call audit writer does:
    # add a message with external_id=tool_call:<call_id>.
    call_id = "call_TEST_42"
    msg_id = await db.add_message_idempotent(
        session_id=sid,
        role="assistant",
        content="task_list",
        external_id=f"tool_call:{call_id}",
        blocks=[{
            "type": "tool_call",
            "tool": "mcp__nerve__task_list",
            "input": {"limit": 3},
            "tool_use_id": call_id,
        }],
    )
    assert msg_id is not None

    # Rollout sync sees the same call_id later and tries to write it.
    ing = CodexIngester(
        db, origin_id="local-pi",
        workspace_filter=WorkspaceFilter(
            mode="nerve_workspace",
            nerve_workspace_path=tmp_path,
        ),
        broadcaster=_NullBroadcaster(),
    )
    ing.mark_in_scope(thread_id)
    await ing.ingest(ThreadEvent(
        type="tool_call",
        thread_id=thread_id,
        sequence=2,
        timestamp=datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc),
        payload={
            "name": "task_list", "namespace": "mcp__nerve__",
            "arguments": '{"limit": 3}', "call_id": call_id,
        },
    ))
    # Exactly one message for that call_id remains.
    msgs = await db.get_messages(sid)
    matching = [m for m in msgs if m.get("external_id") == f"tool_call:{call_id}"]
    assert len(matching) == 1
