"""Pinned-client gate: a running client follows the MCP gateway catalog.

Opt-in, and not part of ordinary CI: it starts the real pinned clients (the
Claude CLI bundled with the ``claude-agent-sdk`` version in ``uv.lock``, and
the Codex CLI version in ``tests/fixtures/codex_schema_meta.json``) and makes
real model calls with the least expensive model of each provider. Run it
with::

    NERVE_PINNED_CLIENT_GATE=1 pytest tests/test_managed_mcp_pinned_clients.py

Credentials come from the environment: ``ANTHROPIC_API_KEY`` for Claude;
``OPENAI_API_KEY``, or ``NERVE_GATE_CODEX_AUTH_FILE`` (a Codex ``auth.json``
in API-key mode, copied into the test's own Codex home), for Codex. Nothing
prints a credential. Each test uses its own HOME and Codex home.

Each test drives Nerve's engine in external mode against the fake MCP
gateway and puts a recording proxy in front of the model API, so it checks
the model requests themselves:

- An added server makes Nerve replace the client. The next model request
  carries the new tools and the earlier conversation, and the session keeps
  the same native ID.
- Claude: a removed server, a changed tool policy and a returning server
  change the client in place (turn off, reconnect, turn on), and the next
  model request shows it. Codex: the same changes replace the client on the
  same thread.
- A ``tools/call`` whose connection drops is not sent again by the client.
- A ``-32002`` reset of the gateway reaches the model unchanged, and the
  engine keeps its client.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

import pytest

from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig
from nerve.gateway.auth import AUTH_MODE_ENV
from tests.fake_mcp_gateway import (
    BEHAVIOR_DROP,
    BEHAVIOR_RESET,
    RESET_MESSAGE,
    FakeMcpGateway,
    catalog_payload,
)
from tests.model_api_recorder import ModelApiRecorder

GATE = os.environ.get("NERVE_PINNED_CLIENT_GATE") == "1"
# A file for a summary of the model requests (tool names, model, sizes),
# without request text or credentials.
_REPORT = os.environ.get("NERVE_PINNED_CLIENT_GATE_REPORT", "")
# Read at import: an autouse fixture removes ANTHROPIC_API_KEY for each test.
_ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
_OPENAI_KEY = os.environ.get("OPENAI_API_KEY", "")
_CODEX_AUTH_FILE = os.environ.get("NERVE_GATE_CODEX_AUTH_FILE", "")

CLAUDE_MODEL = "claude-haiku-5-5"
CODEX_MODEL = "gpt-5.6-luna"
CODE = "PURPLE-HARBOR-42"

pytestmark = [
    pytest.mark.skipif(not GATE, reason="set NERVE_PINNED_CLIENT_GATE=1 to run the gate"),
    pytest.mark.asyncio,
]

SESSION = "gate-1"


def _names(tools: Any) -> set[str]:
    """Every tool name of a request's tool list, also inside namespaces."""
    found: set[str] = set()
    if isinstance(tools, list):
        for tool in tools:
            found |= _names(tool)
    elif isinstance(tools, dict):
        if isinstance(tools.get("name"), str):
            found.add(tools["name"])
        for value in tools.values():
            if isinstance(value, (list, dict)):
                found |= _names(value)
    return found


def _conversation(body: dict) -> Any:
    return body.get("messages", body.get("input"))


def _model_calls(body: dict, tool: str) -> int:
    """The calls of ``tool`` that the model made, as the request repeats them."""
    count = 0

    def walk(value: Any) -> None:
        nonlocal count
        if isinstance(value, list):
            for item in value:
                walk(item)
        elif isinstance(value, dict):
            if (
                value.get("type") in ("tool_use", "function_call")
                and str(value.get("name", "")).endswith(tool)
            ):
                count += 1
            for item in value.values():
                if isinstance(item, (list, dict)):
                    walk(item)

    walk(_conversation(body))
    return count


def _has_text(body: dict, text: str) -> bool:
    return text in json.dumps(_conversation(body))


class _Gate:
    """One engine, the fake gateway and the model API recorder."""

    def __init__(self, engine, gateway, recorder, db, path, inventory):
        self.engine = engine
        self.gateway = gateway
        self.recorder = recorder
        self.db = db
        self.path = path
        self._inventory = inventory
        self.turns: list[dict] = []
        # The model requests of the last turn.
        self.last: list[dict] = []

    async def inventory(self) -> set[str]:
        """The MCP tool names that the session's client offers the model."""
        return await self._inventory(self)

    async def issued(self, tool: str) -> int:
        """The calls of ``tool`` in the last assistant message, as Nerve
        stored them from the client's tool-use events."""
        messages = await self.db.get_messages(SESSION)
        last = [m for m in messages if m["role"] == "assistant"][-1]
        blocks = last.get("blocks") or []
        if isinstance(blocks, str):
            blocks = json.loads(blocks)
        return sum(
            1 for block in blocks
            if block.get("type") == "tool_call"
            and str(block.get("tool", "")).endswith(tool)
        )

    def requests(self) -> list[dict]:
        """The model requests of the conversation: they carry tools (Claude:
        ``tools``; Codex: an ``additional_tools`` input item) and the first
        prompt."""
        return [
            body for body in self.recorder.bodies(self.path)
            if isinstance(body, dict)
            and (body.get("tools") or _has_text(body, '"additional_tools"'))
            and _has_text(body, "lookup_code")
        ]

    async def turn(self, text: str) -> tuple[str, list[dict]]:
        before = len(self.requests())
        reply = await self.engine.run(
            SESSION, text, source="web", actor=self.db.system_actor,
        )
        requests = self.requests()[before:]
        self.last = requests
        self.turns.append({
            "native_id": await self.native_id(),
            "client": id(self.engine.sessions.get_client(SESSION)),
            "requests": [
                {
                    "model": body.get("model"),
                    "mcp_tools": sorted(
                        name for name in _names(body.get("tools"))
                        if name.startswith("mcp__")
                    ),
                    "conversation_items": len(_conversation(body) or []),
                    "metadata": body.get("metadata"),
                }
                for body in requests
            ],
        })
        return reply, requests

    async def report(self, name: str) -> None:
        if not _REPORT:
            return
        path = Path(_REPORT)
        data = json.loads(path.read_text()) if path.exists() else {}
        session = await self.db.get_session(SESSION) or {}
        data[name] = {
            "cost_usd": session.get("total_cost_usd"),
            "usage": await self.db.get_session_usage_totals(SESSION),
            "turns": self.turns,
            "gateway_http": [list(entry) for entry in self.gateway.http_log],
            "model_api": [
                [path, sorted(body) if isinstance(body, dict) else type(body).__name__]
                for path, body in self.recorder.requests
            ],
        }
        path.write_text(json.dumps(data, indent=1))
        if os.environ.get("NERVE_PINNED_CLIENT_GATE_BODIES"):
            Path(os.environ["NERVE_PINNED_CLIENT_GATE_BODIES"]).write_text(
                json.dumps(self.recorder.requests, indent=1),
            )

    async def native_id(self) -> str | None:
        return (await self.db.get_session(SESSION) or {}).get("sdk_session_id")

    async def catalog(self, generation: int, servers: dict) -> None:
        self.gateway.catalog = catalog_payload(generation, servers)
        await self.engine._mcp_gateway.refresh()

    def session(self) -> dict:
        return {
            entry["session_id"]: entry
            for entry in self.engine.mcp_gateway_status()["sessions"]
        }[SESSION]


def _config(tmp_path: Path, gateway: FakeMcpGateway, **extra) -> NerveConfig:
    (tmp_path / "ws").mkdir(exist_ok=True)
    return NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "mcp_gateway_url": gateway.url,
        "mcp_gateway_refresh_seconds": 0,
        **extra,
    })


async def _claude_inventory(gate: "_Gate") -> set[str]:
    """The tools of the first model request of the last turn."""
    return _names(gate.last[0].get("tools")) if gate.last else set()


async def _codex_inventory(gate: "_Gate") -> set[str]:
    """The MCP tools that the app-server offers the thread.

    Codex sends no static tool list: the model finds MCP tools at run time
    through its code tool, which offers what the app-server lists here.
    """
    client = gate.engine.sessions.get_client(SESSION)
    response = await client._transport.request(
        "mcpServerStatus/list", {"threadId": client.native_session_id},
    )
    return {
        f"mcp__{server['name']}__{tool}"
        for server in response.get("data") or []
        for tool in server.get("tools") or {}
    }


async def _scenario(gate: _Gate, *, in_place: bool) -> None:
    """The gate's steps; ``in_place`` tells whether removals and policy
    changes change the client in place (Claude) or replace it (Codex)."""
    gateway = gate.gateway

    # 1. A first turn uses a gateway tool; its result is the context to keep.
    await gate.turn(
        "Call the tool lookup_code of the docs server once, then reply with "
        "the code that it returned."
    )
    assert gateway.tool_calls("lookup_code") == 1
    native = await gate.native_id()
    assert native
    first_client = gate.engine.sessions.get_client(SESSION)

    # 2. An added server: the client is replaced and resumes the session.
    await gate.catalog(8, {"docs": ["lookup_code"], "jira": ["create_ticket"]})
    reply, requests = await gate.turn(
        "Call the tool create_ticket of the jira server once, with no "
        "arguments. Then reply with the code that the lookup_code tool "
        "returned earlier in this conversation."
    )
    assert requests, "no model request in the turn"
    tools = await gate.inventory()
    assert "mcp__jira__create_ticket" in tools, sorted(tools)
    assert "mcp__docs__lookup_code" in tools, sorted(tools)
    assert gateway.tool_calls("create_ticket") >= 1
    assert _has_text(requests[0], CODE), "the earlier conversation is not in the request"
    assert CODE in reply
    assert await gate.native_id() == native
    assert gate.engine.sessions.get_client(SESSION) is not first_client
    entry = gate.session()
    assert entry["last_update"]["result"] == "replaced"
    assert entry["applied_revision"] == gateway.catalog["desiredRevision"]
    assert entry["pending"] is None

    # 3. A removed server and a changed tool policy.
    second_client = gate.engine.sessions.get_client(SESSION)
    await gate.catalog(9, {"docs": ["lookup_code", "lookup_more", "flaky", "stateful"]})
    reply, requests = await gate.turn(
        "Call the tool lookup_more of the docs server once, with no "
        "arguments, then reply with the word DONE."
    )
    tools = await gate.inventory()
    assert not any(name.startswith("mcp__jira__") for name in tools), sorted(tools)
    assert "mcp__docs__lookup_more" in tools, sorted(tools)
    assert gateway.tool_calls("lookup_more") >= 1
    assert _has_text(requests[0], CODE)
    assert await gate.native_id() == native
    entry = gate.session()
    assert entry["pending"] is None
    assert entry["applied_revision"] == gateway.catalog["desiredRevision"]
    if in_place:
        assert gate.engine.sessions.get_client(SESSION) is second_client
        assert entry["last_update"]["method"] == "in_place"
    else:
        assert gate.engine.sessions.get_client(SESSION) is not second_client
        assert entry["last_update"]["result"] == "replaced"

    # 4. A removed server comes back.
    third_client = gate.engine.sessions.get_client(SESSION)
    await gate.catalog(10, {
        "docs": ["lookup_code", "lookup_more", "flaky", "stateful"],
        "jira": ["create_ticket"],
    })
    calls = gateway.tool_calls("create_ticket")
    await gate.turn(
        "Call the tool create_ticket of the jira server once, with no "
        "arguments, then reply with the word DONE."
    )
    assert "mcp__jira__create_ticket" in await gate.inventory()
    assert gateway.tool_calls("create_ticket") > calls
    assert await gate.native_id() == native
    assert gate.session()["pending"] is None
    if in_place:
        assert gate.engine.sessions.get_client(SESSION) is third_client
    else:
        assert gate.engine.sessions.get_client(SESSION) is not third_client

    # 5. A dropped tools/call is not sent again by the client.
    gateway.tool_behavior["flaky"] = BEHAVIOR_DROP
    client = gate.engine.sessions.get_client(SESSION)
    await gate.turn(
        "Call the tool flaky of the docs server exactly once, with no "
        "arguments. Whatever happens, do not call it again; then reply with "
        "one word: FINISHED."
    )
    issued = await gate.issued("flaky")
    assert issued >= 1
    assert gateway.tool_calls("flaky") == issued

    # 6. A -32002 reset reaches the model unchanged; the client stays.
    gateway.tool_behavior["stateful"] = BEHAVIOR_RESET
    catalog_reads = gateway.catalog_requests
    reply, requests = await gate.turn(
        "Call the tool stateful of the docs server exactly once, with no "
        "arguments, then reply with the error text that it returned."
    )
    assert any(_has_text(body, RESET_MESSAGE) for body in requests)
    issued = await gate.issued("stateful")
    assert issued >= 1
    assert gateway.tool_calls("stateful") == issued
    assert gate.engine.sessions.get_client(SESSION) is client
    assert gateway.catalog_requests == catalog_reads
    assert gate.session()["pending"] is None
    assert await gate.native_id() == native


@pytest.mark.skipif(not _ANTHROPIC_KEY, reason="ANTHROPIC_API_KEY is not set")
async def test_claude_follows_the_catalog_in_the_same_session(tmp_path, db, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv(AUTH_MODE_ENV, "external")
    gateway = FakeMcpGateway(catalog_payload(7, {"docs": ["lookup_code"]})).start()
    gateway.tool_results["lookup_code"] = f"The code is {CODE}."
    recorder = ModelApiRecorder("https://api.anthropic.com").start()
    config = _config(
        tmp_path, gateway,
        agent={"model": CLAUDE_MODEL, "effort": "low"},
        anthropic_api_key=_ANTHROPIC_KEY,
        anthropic_base_url=recorder.url,
    )
    engine = AgentEngine(config, db)
    await engine._mcp_gateway.start()
    gate = _Gate(engine, gateway, recorder, db, "/v1/messages", _claude_inventory)
    try:
        await _scenario(gate, in_place=True)
    finally:
        await gate.report("claude")
        await engine.shutdown()
        await engine._mcp_gateway.close()
        recorder.stop()
        gateway.stop()


@pytest.mark.skipif(
    not (_OPENAI_KEY or _CODEX_AUTH_FILE),
    reason="OPENAI_API_KEY or NERVE_GATE_CODEX_AUTH_FILE is not set",
)
async def test_codex_follows_the_catalog_on_the_same_thread(tmp_path, db, monkeypatch):
    home = tmp_path / "home"
    codex_home = tmp_path / "codex-home"
    codex_home.mkdir(parents=True)
    if _CODEX_AUTH_FILE:
        shutil.copy(_CODEX_AUTH_FILE, codex_home / "auth.json")
        os.chmod(codex_home / "auth.json", 0o600)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv(AUTH_MODE_ENV, "external")
    gateway = FakeMcpGateway(catalog_payload(7, {"docs": ["lookup_code"]})).start()
    gateway.tool_results["lookup_code"] = f"The code is {CODE}."
    recorder = ModelApiRecorder("https://api.openai.com").start()
    codex = {
        "bin_path": shutil.which("codex") or "codex",
        "home_dir": str(codex_home),
        "model": CODEX_MODEL,
        "auth": "api_key",
        "models": [CODEX_MODEL],
        # The model requests go through the recorder.
        "extra_config": {"openai_base_url": recorder.url + "/v1"},
    }
    if _OPENAI_KEY:
        codex["api_key"] = _OPENAI_KEY
    config = _config(
        tmp_path, gateway, codex=codex, agent={"backend": "codex", "effort": "low"},
    )
    engine = AgentEngine(config, db)
    await engine._mcp_gateway.start()
    await db.create_session(SESSION, source="web", backend="codex", actor=None)
    gate = _Gate(engine, gateway, recorder, db, "/v1/responses", _codex_inventory)
    try:
        await _scenario(gate, in_place=False)
    finally:
        await gate.report("codex")
        await engine.shutdown()
        await engine._mcp_gateway.close()
        recorder.stop()
        gateway.stop()
        # pytest keeps the temporary directories of recent runs.
        (codex_home / "auth.json").unlink(missing_ok=True)
