"""CodexIngester — idempotent message inserts + satellite session lifecycle.

The ingester is the seam between origin events and the DB. The tests
exercise:

  * a fresh thread → satellite session is created with the
    ``codex:<thread_id>`` id and ``source="external"``
  * a duplicate ``external_id`` → second insert is a no-op
  * a ``thread_archived`` event → session status flips to archived
  * an out-of-scope thread → no session created, no messages stored
  * an existing satellite session (e.g. MCP server created it first) →
    metadata is merged, no duplicate session row
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from nerve.sources.codex_threads.base import (
    ThreadEvent,
    WorkspaceFilter,
)
from nerve.sources.codex_threads.ingester import CodexIngester, codex_session_id

# ``db`` fixture is supplied by tests/conftest.py


class _NullBroadcaster:
    async def broadcast(self, session_id, payload):
        return


def _filter(workspace: Path) -> WorkspaceFilter:
    return WorkspaceFilter(
        mode="nerve_workspace",
        nerve_workspace_path=workspace,
    )


def _evt(type_: str, *, thread_id: str, payload: dict, seq: int = 1) -> ThreadEvent:
    return ThreadEvent(
        type=type_,                       # type: ignore[arg-type]
        thread_id=thread_id,
        sequence=seq,
        timestamp=datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc),
        payload=payload,
    )


def _session_meta_payload(thread_id: str, cwd: str | None) -> dict:
    return {
        "id": thread_id,
        "cwd": cwd,
        "originator": "codex_exec",
        "cli_version": "0.130.0",
        "source": "exec",
        "model_provider": "openai",
        "base_instructions": {"text": ""},
    }


@pytest.mark.asyncio
async def test_in_scope_thread_creates_satellite_session(db, tmp_path):
    ing = CodexIngester(
        db, origin_id="o1",
        workspace_filter=_filter(tmp_path),
        broadcaster=_NullBroadcaster(),
    )
    tid = "abcd1234"
    await ing.ingest(_evt(
        "thread_in_scope",
        thread_id=tid,
        payload=_session_meta_payload(tid, str(tmp_path)),
    ))
    session = await db.get_session(codex_session_id(tid))
    assert session is not None
    assert session["source"] == "external"
    meta = json.loads(session["metadata"])
    assert meta["client_name"] == "codex"
    assert meta["runtime"] == "codex-external"
    assert meta["codex_thread_id"] == tid
    assert meta["origin_ids"] == ["o1"]


@pytest.mark.asyncio
async def test_out_of_scope_thread_does_not_create_session(db, tmp_path):
    ing = CodexIngester(
        db, origin_id="o1",
        workspace_filter=_filter(tmp_path),
        broadcaster=_NullBroadcaster(),
    )
    tid = "outofscope"
    await ing.ingest(_evt(
        "thread_in_scope",   # session_meta seen but filter rejects
        thread_id=tid,
        payload=_session_meta_payload(tid, "/some/other/path"),
    ))
    assert await db.get_session(codex_session_id(tid)) is None


@pytest.mark.asyncio
async def test_user_message_persisted(db, tmp_path):
    ing = CodexIngester(
        db, origin_id="o1",
        workspace_filter=_filter(tmp_path),
        broadcaster=_NullBroadcaster(),
    )
    tid = "t1"
    await ing.ingest(_evt(
        "thread_in_scope",
        thread_id=tid,
        payload=_session_meta_payload(tid, str(tmp_path)),
    ))
    await ing.ingest(_evt(
        "user_message",
        thread_id=tid,
        payload={"message": "hello", "event_id": "e1"},
        seq=2,
    ))
    msgs = await db.get_messages(codex_session_id(tid))
    assert len(msgs) == 1
    assert msgs[0]["role"] == "user"
    assert msgs[0]["external_id"] == "msg:e1"
    assert msgs[0]["content"] == "hello"


@pytest.mark.asyncio
async def test_duplicate_external_id_is_dedup_no_op(db, tmp_path):
    ing = CodexIngester(
        db, origin_id="o1",
        workspace_filter=_filter(tmp_path),
        broadcaster=_NullBroadcaster(),
    )
    tid = "t1"
    await ing.ingest(_evt(
        "thread_in_scope",
        thread_id=tid,
        payload=_session_meta_payload(tid, str(tmp_path)),
    ))
    # Same call_id, two ingest passes
    payload = {
        "name": "ls", "arguments": "{}", "call_id": "c1",
    }
    await ing.ingest(_evt("tool_call", thread_id=tid, payload=payload, seq=2))
    await ing.ingest(_evt("tool_call", thread_id=tid, payload=payload, seq=3))
    msgs = await db.get_messages(codex_session_id(tid))
    assert len(msgs) == 1
    assert ing.stats["messages_skipped_duplicate"] == 1


@pytest.mark.asyncio
async def test_thread_archived_marks_session(db, tmp_path):
    ing = CodexIngester(
        db, origin_id="o1",
        workspace_filter=_filter(tmp_path),
        broadcaster=_NullBroadcaster(),
    )
    tid = "t1"
    await ing.ingest(_evt(
        "thread_in_scope",
        thread_id=tid,
        payload=_session_meta_payload(tid, str(tmp_path)),
    ))
    await ing.ingest(_evt(
        "thread_archived", thread_id=tid, payload={},
    ))
    session = await db.get_session(codex_session_id(tid))
    assert session is not None
    assert session["status"] == "archived"
    assert ing.stats["threads_archived"] == 1


@pytest.mark.asyncio
async def test_message_before_session_meta_is_dropped(db, tmp_path):
    ing = CodexIngester(
        db, origin_id="o1",
        workspace_filter=_filter(tmp_path),
        broadcaster=_NullBroadcaster(),
    )
    tid = "ghost"
    # User message arrives before we've seen session_meta — dropped.
    await ing.ingest(_evt(
        "user_message",
        thread_id=tid,
        payload={"message": "should not appear"},
    ))
    assert await db.get_session(codex_session_id(tid)) is None


@pytest.mark.asyncio
async def test_existing_satellite_session_is_merged_not_duplicated(
    db, tmp_path,
):
    # MCP server created the satellite first under the same id.
    tid = "11111111-2222-3333-4444-555555555555"
    sid = codex_session_id(tid)
    await db.create_session(
        session_id=sid,
        title="Codex/mcp (11111111)",
        source="external",
        metadata={
            "client_name": "codex",
            "runtime": "codex-external",
            "origin_ids": ["nerve-mcp-detected"],
        },
        status="active",
    )

    ing = CodexIngester(
        db, origin_id="local-pi",
        workspace_filter=_filter(tmp_path),
        broadcaster=_NullBroadcaster(),
    )
    await ing.ingest(_evt(
        "thread_in_scope",
        thread_id=tid,
        payload=_session_meta_payload(tid, str(tmp_path)),
    ))
    session = await db.get_session(sid)
    assert session is not None
    meta = json.loads(session["metadata"])
    # Both origins now recorded on the same row.
    assert "nerve-mcp-detected" in meta["origin_ids"]
    assert "local-pi" in meta["origin_ids"]
    # Sync source backfilled the codex_* fields the MCP server didn't have.
    assert meta["codex_thread_id"] == tid
    assert meta["codex_cwd"] == str(tmp_path)


@pytest.mark.asyncio
async def test_turn_events_do_not_create_messages(db, tmp_path):
    ing = CodexIngester(
        db, origin_id="o",
        workspace_filter=_filter(tmp_path),
        broadcaster=_NullBroadcaster(),
    )
    tid = "t"
    await ing.ingest(_evt(
        "thread_in_scope",
        thread_id=tid,
        payload=_session_meta_payload(tid, str(tmp_path)),
    ))
    await ing.ingest(_evt(
        "turn_started", thread_id=tid, payload={"turn_id": "x"},
    ))
    await ing.ingest(_evt(
        "turn_completed", thread_id=tid, payload={"turn_id": "x"},
    ))
    msgs = await db.get_messages(codex_session_id(tid))
    assert msgs == []
