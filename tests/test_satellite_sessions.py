"""Tests for the satellite session resolver and external session lifecycle.

Verifies that:
  * a new MCP connection produces exactly one ``sessions`` row with
    ``source="external"`` and the expected metadata
  * re-resolving the same connection is idempotent
  * distinct ``client_name``s map to distinct session ids
  * unsafe characters in ``client_name`` are sanitized so the resulting
    session id stays URL/path-safe
"""

from __future__ import annotations

import json

import pytest

from nerve.mcp_server.session import SatelliteSessionResolver

# ``db`` fixture is supplied by ``tests/conftest.py``.


@pytest.mark.asyncio
async def test_resolve_creates_session(db):
    resolver = SatelliteSessionResolver(db)
    sid = await resolver.resolve(
        client_name="codex",
        mcp_session_id="abc12345",
    )
    assert sid == "external:codex:abc12345"

    session = await db.get_session(sid)
    assert session is not None
    assert session["source"] == "external"
    meta = json.loads(session["metadata"] or "{}")
    assert meta["client_name"] == "codex"
    assert meta["mcp_session_id"] == "abc12345"
    assert meta["runtime"] == "codex-external"


@pytest.mark.asyncio
async def test_resolve_is_idempotent(db):
    resolver = SatelliteSessionResolver(db)
    sid1 = await resolver.resolve(
        client_name="codex", mcp_session_id="dupe",
    )
    sid2 = await resolver.resolve(
        client_name="codex", mcp_session_id="dupe",
    )
    assert sid1 == sid2

    sessions = await db.list_sessions()
    matching = [s for s in sessions if s["id"] == sid1]
    assert len(matching) == 1


@pytest.mark.asyncio
async def test_distinct_clients_produce_distinct_sessions(db):
    resolver = SatelliteSessionResolver(db)
    sid_codex = await resolver.resolve(
        client_name="codex", mcp_session_id="x",
    )
    sid_claude = await resolver.resolve(
        client_name="claude-code", mcp_session_id="x",
    )
    assert sid_codex != sid_claude
    assert sid_codex.startswith("external:codex:")
    assert sid_claude.startswith("external:claude-code:")


@pytest.mark.asyncio
async def test_missing_client_name_falls_back_to_external(db):
    resolver = SatelliteSessionResolver(db)
    sid = await resolver.resolve(
        client_name=None, mcp_session_id="anon",
    )
    assert sid == "external:external:anon"


@pytest.mark.asyncio
async def test_unsafe_client_name_is_sanitized(db):
    resolver = SatelliteSessionResolver(db)
    sid = await resolver.resolve(
        client_name="evil/../client name",
        mcp_session_id="boundary",
    )
    # forward slashes, dots, and spaces must not appear in the slug
    parts = sid.split(":")
    assert len(parts) == 3
    assert parts[0] == "external"
    safe_slug = parts[1]
    assert "/" not in safe_slug
    assert " " not in safe_slug
    # underscores are the canonical replacement
    assert "_" in safe_slug


@pytest.mark.asyncio
async def test_client_session_id_overrides_mcp_session_id(db):
    """When the client supplies a stable id, the satellite session is
    keyed off that — so reconnects with the same client_session_id
    reuse the row."""
    resolver = SatelliteSessionResolver(db)
    sid1 = await resolver.resolve(
        client_name="codex",
        mcp_session_id="transport-1",
        client_session_id="thread-42",
    )
    sid2 = await resolver.resolve(
        client_name="codex",
        mcp_session_id="transport-2",        # new transport
        client_session_id="thread-42",       # same thread
    )
    assert sid1 == sid2 == "external:codex:thread-42"


def test_build_session_id_static():
    """Pure helper — no DB. Useful as a sanity check across processes."""
    assert SatelliteSessionResolver.build_session_id("codex", "x") == "external:codex:x"
    assert SatelliteSessionResolver.build_session_id("a/b", "x") == "external:a_b:x"
