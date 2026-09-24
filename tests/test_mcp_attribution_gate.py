"""An MCP tool call that cannot be attributed does not run.

The satellite session id a call resolves to is two things at once: the session
a handler executes under, and the key its audit row is written against. So an
id handed back without a row behind it is not a cosmetic gap — a tool with real
side effects (write a file, call out to a network) would run with no session in
the list and no audit event, and the audit writer's own failure is swallowed by
design, so nothing downstream would notice.

This drives the real satellite resolver, ``ToolContext`` builder, and tool
dispatcher across a database create failure.
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
        assert session["created_by_actor_id"] == mcp.db.system_actor_id

    async def test_database_create_failure_refuses_the_tool(self, mcp):
        await mcp.db.db.execute(
            """CREATE TRIGGER fail_satellite_create
               BEFORE INSERT ON sessions
               WHEN NEW.source = 'external'
               BEGIN SELECT RAISE(ABORT, 'injected satellite failure'); END"""
        )
        await mcp.db.db.commit()

        result: Any = await mcp.call()

        assert result.is_error is True
        assert "context error" in result.content[0].text.lower()
        assert mcp.invoked == [], "a tool ran without a durable session"
        assert await mcp.db.list_sessions(limit=50) == []

        # The failed context must not poison this resolver/server connection.
        await mcp.db.db.execute("DROP TRIGGER fail_satellite_create")
        await mcp.db.db.commit()
        retry: Any = await mcp.call()
        assert retry.is_error is False
        assert len(mcp.invoked) == 1
        session = await mcp.db.get_session(mcp.invoked[0]["session_id"])
        assert session["created_by_actor_id"] == mcp.db.system_actor_id


@pytest.mark.asyncio
class TestOnlyALostRaceIsSurvivable:
    async def test_a_concurrent_create_is_absorbed(self, mcp):
        """The one failure the resolver may swallow: another request created
        the row between the lookup and the insert. The row the caller needs
        exists, so the call proceeds."""
        resolver = SatelliteSessionResolver(mcp.db)
        sid = resolver.build_session_id("claude-code", "race-1")

        real_create = mcp.db.create_session

        async def _lose_the_race(*args, **kwargs):
            # Someone else got there first...
            await real_create(
                kwargs["session_id"], source="external",
                actor=mcp.db.system_actor,
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
        assert session["created_by_actor_id"] == mcp.db.system_actor_id
