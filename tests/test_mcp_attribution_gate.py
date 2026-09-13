"""An MCP tool call that cannot be attributed does not run.

The satellite session id a call resolves to is two things at once: the session
a handler executes under, and the key its audit row is written against. So an
id handed back without a row behind it is not a cosmetic gap — a tool with real
side effects (write a file, call out to a network) would run with no session in
the list and no audit event, and the audit writer's own failure is swallowed by
design, so nothing downstream would notice.

This drives the whole path the way an external client does: the real satellite
resolver, the real ``ToolContext`` builder, and the real ``call_tool``
dispatcher — with the one thing broken that a tool call cannot recover from,
an unresolvable system principal.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio

from mcp.types import CallToolRequestParams, CallToolResult

from nerve.agent.engine import AgentEngine
from nerve.agent.tools import ToolContext, ToolRegistry, ToolResult, ToolSpec
from nerve.config import NerveConfig
from nerve.mcp_server.http import build_ctx_resolver
from nerve.mcp_server.server import build_mcp_server
from nerve.mcp_server.session import SatelliteSessionResolver
from tests.actor_rows import ensure_system_principal

_FAKE_RCTX = SimpleNamespace(request=None, session=None)


class _Harness:
    def __init__(self, db, engine, server, invoked: list[dict]):
        self.db = db
        self.engine = engine
        self.server = server
        self.invoked = invoked

    async def call(self, name: str = "touch") -> CallToolResult:
        entry = self.server.get_request_handler("tools/call")
        return await entry.handler(
            _FAKE_RCTX, CallToolRequestParams(name=name, arguments={}),
        )

    def break_the_principal(self, monkeypatch) -> None:
        async def _gone():
            return None

        monkeypatch.setattr(self.db, "get_system_principal", _gone)


@pytest_asyncio.fixture
async def mcp(db, tmp_path) -> _Harness:
    """The external MCP surface, wired as the HTTP mount wires it."""
    await ensure_system_principal(db)
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
    })
    engine = AgentEngine(config, db)

    invoked: list[dict] = []

    async def _handler(ctx: ToolContext, args: dict) -> ToolResult:
        # A stand-in for a tool with side effects: if this list grows, the
        # call reached the handler.
        invoked.append({"session_id": ctx.session_id, "args": args})
        return ToolResult.text("done")

    registry = ToolRegistry()
    registry.register(ToolSpec(
        name="touch",
        description="a tool with side effects",
        input_schema={"type": "object", "properties": {}, "required": []},
        handler=_handler,
    ))

    resolver = SatelliteSessionResolver(db)
    server = build_mcp_server(
        registry, ctx_resolver=build_ctx_resolver(engine, resolver),
    )
    return _Harness(db, engine, server, invoked)


@pytest.mark.asyncio
class TestAToolCallNeedsAnAttributableSession:
    async def test_the_handler_runs_when_attribution_works(self, mcp):
        """The control: this is what the gate is stopping, so it has to be a
        thing that otherwise happens."""
        result: Any = await mcp.call()

        assert result.is_error is False
        assert len(mcp.invoked) == 1
        session_id = mcp.invoked[0]["session_id"]
        session = await mcp.db.get_session(session_id)
        assert session is not None, "the handler ran without a session row"
        identity = await mcp.db.get_local_identity()
        assert session["created_by_actor_id"] == identity.system_actor_id

    async def test_the_handler_never_runs_when_it_cannot(self, mcp, monkeypatch):
        mcp.break_the_principal(monkeypatch)

        result: Any = await mcp.call()

        assert result.is_error is True
        assert "context error" in result.content[0].text.lower()
        assert mcp.invoked == [], "a tool ran with no attributable session"
        assert await mcp.db.list_sessions(limit=50) == []

    async def test_and_the_next_call_works(self, mcp, monkeypatch):
        """Refusing is a transient failure, not a broken mount."""
        mcp.break_the_principal(monkeypatch)
        assert (await mcp.call()).is_error is True

        monkeypatch.undo()
        result: Any = await mcp.call()

        assert result.is_error is False
        assert len(mcp.invoked) == 1


@pytest.mark.asyncio
class TestOnlyALostRaceIsSurvivable:
    async def test_a_concurrent_create_is_absorbed(self, mcp):
        """The one failure the resolver may swallow: another request created
        the row between the lookup and the insert. The row the caller needs
        exists, so the call proceeds."""
        resolver = SatelliteSessionResolver(mcp.db)
        sid = resolver.build_session_id("claude-code", "race-1")
        identity = await mcp.db.get_local_identity()

        real_create = mcp.db.create_session

        async def _lose_the_race(*args, **kwargs):
            # Someone else got there first...
            await real_create(
                kwargs["session_id"], source="external",
                actor=await _system(mcp.db),
            )
            raise RuntimeError("UNIQUE constraint failed: sessions.id")

        mcp.db.create_session = _lose_the_race
        try:
            assert await resolver.resolve(
                client_name="claude-code", mcp_session_id="race-1",
            ) == sid
        finally:
            mcp.db.create_session = real_create

        session = await mcp.db.get_session(sid)
        assert session is not None
        assert session["created_by_actor_id"] == identity.system_actor_id

    async def test_anything_else_refuses(self, mcp):
        """A create that leaves no row is not a race, and must not be
        mistaken for one."""
        resolver = SatelliteSessionResolver(mcp.db)

        async def _fail(*args, **kwargs):
            raise RuntimeError("disk is on fire")

        mcp.db.create_session = _fail
        try:
            with pytest.raises(RuntimeError, match="disk is on fire"):
                await resolver.resolve(
                    client_name="claude-code", mcp_session_id="doomed",
                )
        finally:
            del mcp.db.create_session

        assert await mcp.db.list_sessions(limit=50) == []


async def _system(db):
    from nerve.identity import system_actor

    return await system_actor(db)
