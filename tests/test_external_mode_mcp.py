"""MCP servers from the MCP gateway in external mode.

In external mode Nerve takes its MCP servers only from the gateway's catalog
and gives them to Claude and Codex. Local mode does not change. The gateway
is an httpx transport (tests/test_mcp_gateway.py).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

import pytest
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

from nerve.agent.backends import SessionSpec
from nerve.agent.backends import events as ev
from nerve.agent.backends.claude import translate_message
from nerve.agent.engine import AgentEngine
from nerve.config import ConfigError, NerveConfig
from nerve.gateway.auth import AUTH_MODE_ENV
from tests.test_mcp_gateway import BASE, FakeGateway, _document


@pytest.fixture
def external(monkeypatch):
    monkeypatch.setenv(AUTH_MODE_ENV, "external")


def _config(tmp_path: Path, **extra) -> NerveConfig:
    (tmp_path / "ws").mkdir(exist_ok=True)
    return NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
        # Servers that local mode uses and external mode does not.
        "mcp_servers": {
            "local-files": {"type": "stdio", "command": "/usr/bin/true"},
            "remote-api": {
                "type": "http", "url": "https://mcp.example.com/v1",
                "headers": {"Authorization": "Bearer placeholder"},
            },
        },
        **extra,
    })


def _engine(tmp_path: Path, db, gateway: FakeGateway | None = None) -> AgentEngine:
    """An engine whose MCP gateway is ``gateway`` (no gateway URL without it)."""
    extra = {"mcp_gateway_url": BASE} if gateway is not None else {}
    engine = AgentEngine(_config(tmp_path, **extra), db)
    if gateway is not None and engine._mcp_gateway is not None:
        engine._mcp_gateway = gateway.reader()
    return engine


def _spec(cfg: NerveConfig, session_id: str = "s1") -> SessionSpec:
    return SessionSpec(
        session_id=session_id, source="web", model=None, effort="high",
        system_prompt="You are Nerve.", cwd=str(cfg.workspace),
    )


def _claude_options(engine: AgentEngine, spec: SessionSpec | None = None):
    claude = engine._backends["claude"]
    with patch.object(claude, "_build_hooks", return_value={}):
        return claude._build_options(spec or _spec(engine.config))


class _StubClient:
    model = "stub-model"

    def __init__(self):
        self.buffered = 0
        self.disconnected = False

    def is_alive(self):
        return True

    def buffer_used(self):
        return self.buffered

    async def disconnect(self):
        self.disconnected = True


def _capture_claude_clients(engine: AgentEngine) -> list:
    """Make ``create_client`` record the Claude options that it would use."""
    built: list = []

    async def create_client(spec):
        built.append(_claude_options(engine, spec))
        return _StubClient()

    engine._backends["claude"].create_client = create_client
    return built


class _NoMemory:
    available = False

    def __init__(self, *args, **kwargs):
        pass

    async def initialize(self):
        return False

    async def aclose(self):
        pass


async def _initialize(engine: AgentEngine):
    with patch("nerve.memory.memu_bridge.MemUBridge", _NoMemory), \
         patch("nerve.memory.xmemory_bridge.XmemoryBridge", _NoMemory), \
         patch("nerve.config.load_claude_code_plugins") as plugins:
        await engine.initialize()
    return plugins


class TestClients:
    @pytest.mark.asyncio
    async def test_claude_gets_the_gateway_servers_only(self, tmp_path, db, external):
        engine = _engine(tmp_path, db, FakeGateway(_document("docs", "issues")))
        await engine._refresh_mcp_catalog()
        options = _claude_options(engine)
        await engine.shutdown()

        assert set(options.mcp_servers) == {"nerve", "docs", "issues"}
        # No header: the VM holds no upstream credential.
        assert options.mcp_servers["docs"] == {
            "type": "http", "url": f"{BASE}/s/docs/mcp",
        }
        assert options.strict_mcp_config is True
        assert options.plugins == []

    @pytest.mark.asyncio
    async def test_codex_gets_the_gateway_servers_only(self, tmp_path, db, external):
        engine = _engine(tmp_path, db, FakeGateway(_document("docs")))
        await engine._refresh_mcp_catalog()
        codex = engine._backends["codex"]
        spec = _spec(engine.config)
        overrides = codex.build_config_overrides(spec)
        env = codex.build_env(spec)
        await engine.shutdown()

        servers = [o for o in overrides
                   if o.startswith("mcp_servers.") and not o.startswith("mcp_servers.nerve.")]
        assert servers == [
            f'mcp_servers.docs.url="{BASE}/s/docs/mcp"',
            'mcp_servers.docs.default_tools_approval_mode="approve"',
        ]
        assert not any(key.startswith("NERVE_CODEX_MCP_EXTERNAL_") for key in env)
        assert "placeholder" not in "\n".join([*overrides, *env.values()])

    @pytest.mark.asyncio
    async def test_no_catalog_means_nerve_only(self, tmp_path, db, external):
        gateway = FakeGateway()
        gateway.status = 503
        engine = _engine(tmp_path, db, gateway)
        await engine._refresh_mcp_catalog()
        options = _claude_options(engine)
        await engine.shutdown()
        assert set(options.mcp_servers) == {"nerve"}
        assert options.strict_mcp_config is True
        assert engine._mcp_catalog_revision is None

    def test_without_a_gateway_url(self, tmp_path, db, external):
        engine = _engine(tmp_path, db)
        assert engine._mcp_gateway is None
        options = _claude_options(engine)
        assert set(options.mcp_servers) == {"nerve"}
        assert options.strict_mcp_config is True

    @pytest.mark.asyncio
    async def test_a_catalog_server_named_nerve_is_not_used(self, tmp_path, db, external):
        engine = _engine(tmp_path, db, FakeGateway(_document("docs", "nerve")))
        await engine._refresh_mcp_catalog()
        options = _claude_options(engine)
        rows = {row["name"]: row for row in await db.get_mcp_server_stats()}
        await engine.shutdown()
        assert [s.name for s in engine._mcp_servers_cache] == ["docs"]
        assert options.mcp_servers["nerve"] != {
            "type": "http", "url": f"{BASE}/s/nerve/mcp",
        }
        assert rows["nerve"]["type"] == "sdk"


class TestNewSessions:
    @pytest.mark.asyncio
    async def test_a_new_session_reads_the_catalog(self, tmp_path, db, external):
        gateway = FakeGateway(_document("docs", "issues", revision="sha256:one"))
        engine = _engine(tmp_path, db, gateway)
        built = _capture_claude_clients(engine)
        try:
            await db.create_session("s-1", source="web", actor=None)
            first = await engine._get_or_create_client("s-1", "web", None)
            assert engine._mcp_catalog_revision == "sha256:one"

            gateway.document = _document("docs", "wiki", revision="sha256:two")
            # A turn of a running session does not read the catalog.
            assert await engine._get_or_create_client("s-1", "web", None) is first
            assert len(built) == 1

            await db.create_session("s-2", source="web", actor=None)
            await engine._get_or_create_client("s-2", "web", None)
        finally:
            await engine.shutdown()

        assert set(built[0].mcp_servers) == {"nerve", "docs", "issues"}
        assert set(built[1].mcp_servers) == {"nerve", "docs", "wiki"}
        assert engine._mcp_catalog_revision == "sha256:two"

    @pytest.mark.asyncio
    async def test_outage_at_startup_then_recovery(self, tmp_path, db, external):
        gateway = FakeGateway()
        gateway.status = 503
        engine = _engine(tmp_path, db, gateway)
        built = _capture_claude_clients(engine)
        await _initialize(engine)
        try:
            await db.create_session("s-1", source="web", actor=None)
            await engine._get_or_create_client("s-1", "web", None)
            assert set(built[0].mcp_servers) == {"nerve"}

            gateway.status = 200
            await db.create_session("s-2", source="web", actor=None)
            await engine._get_or_create_client("s-2", "web", None)
            assert set(built[1].mcp_servers) == {"nerve", "docs"}
        finally:
            await engine.shutdown()

    @pytest.mark.asyncio
    async def test_initialize_reads_the_catalog_and_no_plugins(self, tmp_path, db, external):
        engine = _engine(tmp_path, db, FakeGateway(_document("docs", "issues")))
        plugins = await _initialize(engine)
        try:
            plugins.assert_not_called()
            assert engine._mcp_catalog_revision == "sha256:catalog"
            rows = {row["name"]: row for row in await db.get_mcp_server_stats()}
            assert set(rows) == {"nerve", "docs", "issues"}
            assert rows["docs"]["type"] == "http"
        finally:
            await engine.shutdown()

    @pytest.mark.asyncio
    async def test_reload_reads_the_catalog_not_the_files(self, tmp_path, db, external):
        gateway = FakeGateway(_document("docs"))
        engine = _engine(tmp_path, db, gateway)
        await engine._refresh_mcp_catalog()
        gateway.document = _document("wiki", revision="sha256:two")
        with patch("nerve.config.load_mcp_servers") as yaml_servers:
            servers = await engine.reload_mcp_config()
        await engine.shutdown()
        yaml_servers.assert_not_called()
        assert [(s.name, s.type, s.url, s.headers) for s in servers] == [
            ("wiki", "http", f"{BASE}/s/wiki/mcp", {}),
        ]


async def _until(condition, timeout: float = 2.0) -> None:
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.005)


class TestPeriodicRead:
    @pytest.mark.asyncio
    async def test_reads_the_catalog_at_a_fixed_interval(self, tmp_path, db, external):
        gateway = FakeGateway()
        gateway.status = 503
        engine = _engine(tmp_path, db, gateway)
        engine._MCP_CATALOG_REFRESH_SECONDS = 0.01
        await _initialize(engine)
        try:
            # Startup found no catalog; a later periodic read gets it.
            assert engine._mcp_catalog_revision is None
            gateway.status = 200
            await _until(lambda: engine._mcp_catalog_revision == "sha256:catalog")

            gateway.document = _document("wiki", revision="sha256:two")
            await _until(lambda: engine._mcp_catalog_revision == "sha256:two")
            assert [s.name for s in engine._mcp_servers_cache] == ["wiki"]
        finally:
            await engine.shutdown()

        # Shutdown stops the reads.
        assert engine._mcp_catalog_task is None
        reads = len(gateway.requests)
        await asyncio.sleep(0.05)
        assert len(gateway.requests) == reads

    @pytest.mark.asyncio
    async def test_a_failed_read_does_not_stop_the_reads(self, tmp_path, db, external):
        gateway = FakeGateway()
        engine = _engine(tmp_path, db, gateway)
        engine._MCP_CATALOG_REFRESH_SECONDS = 0.01
        await _initialize(engine)
        sync = engine._sync_mcp_servers_to_db
        failures = []

        async def failing_once():
            if not failures:
                failures.append(True)
                raise RuntimeError("database is locked")
            await sync()

        engine._sync_mcp_servers_to_db = failing_once
        try:
            gateway.document = _document("wiki", revision="sha256:two")
            await _until(lambda: failures)
            gateway.document = _document("docs", "wiki", revision="sha256:three")
            await _until(lambda: engine._mcp_catalog_revision == "sha256:three")
        finally:
            await engine.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_stops_a_read_in_progress(self, tmp_path, db, external):
        gateway = FakeGateway()
        engine = _engine(tmp_path, db, gateway)
        engine._MCP_CATALOG_REFRESH_SECONDS = 0.01
        await _initialize(engine)
        gateway.delay = 3.0
        await _until(lambda: len(gateway.requests) == 2)
        async with asyncio.timeout(1):
            await engine.shutdown()
        assert engine._mcp_catalog_task is None

    @pytest.mark.asyncio
    async def test_no_periodic_read_without_a_gateway_url(self, tmp_path, db, external):
        engine = _engine(tmp_path, db)
        await _initialize(engine)
        assert engine._mcp_catalog_task is None
        await engine.shutdown()

    @pytest.mark.asyncio
    async def test_no_periodic_read_in_local_mode(self, tmp_path, db):
        engine = _engine(tmp_path, db, FakeGateway())
        await _initialize(engine)
        assert engine._mcp_catalog_task is None
        await engine.shutdown()


@dataclass
class _Build:
    spec: SessionSpec
    servers: set[str]
    client: _StubClient


def _capture_builds(engine: AgentEngine, backend: str = "claude") -> list[_Build]:
    """Make ``create_client`` of ``backend`` record each build: the spec, the
    gateway servers that the client would get, and the client."""
    builds: list[_Build] = []

    async def create_client(spec):
        if backend == "claude":
            servers = set(_claude_options(engine, spec).mcp_servers) - {"nerve"}
        else:
            overrides = engine._backends["codex"].build_config_overrides(spec)
            servers = {o.split(".")[1] for o in overrides
                       if o.startswith("mcp_servers.")} - {"nerve"}
        client = _StubClient()
        if backend == "codex":
            client.model = engine.config.codex.model
        builds.append(_Build(spec, servers, client))
        return client

    engine._backends[backend].create_client = create_client
    return builds


async def _running_session(engine, db, gateway, backend: str = "claude"):
    """A session whose client has the catalog ``sha256:one`` and a stored
    conversation, and a gateway that has the catalog ``sha256:two`` now."""
    builds = _capture_builds(engine, backend)
    await db.create_session("s-1", source="web", backend=backend, actor=None)
    await engine._get_or_create_client("s-1", "web", None)
    await db.update_session_fields("s-1", {"sdk_session_id": "native-1"})
    gateway.document = _document("docs", "wiki", revision="sha256:two")
    return builds


class TestRunningSessions:
    @pytest.fixture
    def gateway(self):
        return FakeGateway(_document("docs", "issues", revision="sha256:one"))

    @pytest.fixture
    def engine(self, tmp_path, db, external, gateway):
        engine = _engine(tmp_path, db, gateway)
        claude = engine._backends["claude"]
        with patch.object(claude, "validate_resume_target", return_value=True):
            yield engine

    @pytest.mark.asyncio
    @pytest.mark.parametrize("backend", ["claude", "codex"])
    async def test_the_next_turn_gets_a_client_with_the_new_catalog(
        self, engine, db, gateway, backend,
    ):
        builds = await _running_session(engine, db, gateway, backend)
        await engine._refresh_mcp_catalog()
        try:
            client = await engine._get_or_create_client("s-1", "web", None)
            # The next turn after that keeps the new client.
            assert await engine._get_or_create_client("s-1", "web", None) is client
        finally:
            await engine.shutdown()

        first, second = builds
        assert first.servers == {"docs", "issues"}
        assert first.client.disconnected
        assert second.client is client
        assert second.servers == {"docs", "wiki"}
        # The new client continues the same conversation.
        assert second.spec.resume_native_id == "native-1"
        assert not second.spec.fork
        session = await db.get_session("s-1")
        assert session["sdk_session_id"] == "native-1"

    @pytest.mark.asyncio
    async def test_no_new_client_without_a_new_catalog(self, engine, db, gateway):
        builds = await _running_session(engine, db, gateway)
        first = builds[0].client
        try:
            # Not read yet.
            assert await engine._get_or_create_client("s-1", "web", None) is first
            # A failed read keeps the last catalog.
            gateway.status = 503
            await engine._refresh_mcp_catalog()
            assert await engine._get_or_create_client("s-1", "web", None) is first
        finally:
            await engine.shutdown()
        assert len(builds) == 1

    @pytest.mark.asyncio
    async def test_a_session_without_a_catalog_gets_the_first_one(
        self, engine, db, gateway,
    ):
        gateway.status = 503
        builds = await _running_session(engine, db, gateway)
        gateway.status = 200
        await engine._refresh_mcp_catalog()
        try:
            await engine._get_or_create_client("s-1", "web", None)
        finally:
            await engine.shutdown()
        assert [b.servers for b in builds] == [set(), {"docs", "wiki"}]

    @pytest.mark.asyncio
    async def test_waits_while_a_background_task_runs(self, engine, db, gateway):
        builds = await _running_session(engine, db, gateway)
        first = builds[0].client
        await engine._refresh_mcp_catalog()
        await engine.agents.on_system_event("s-1", ev.SystemEvent("task_started", {
            "task_id": "t-1", "task_type": "local_bash", "description": "t-1",
        }))
        try:
            assert await engine._get_or_create_client("s-1", "web", None) is first
            assert not first.disconnected
            await engine.agents.on_system_event("s-1", ev.SystemEvent(
                "task_notification", {"task_id": "t-1", "status": "completed"},
            ))
            assert await engine._get_or_create_client("s-1", "web", None) is not first
        finally:
            await engine.shutdown()
        assert first.disconnected

    @pytest.mark.asyncio
    async def test_waits_while_output_is_buffered(self, engine, db, gateway):
        builds = await _running_session(engine, db, gateway)
        first = builds[0].client
        await engine._refresh_mcp_catalog()
        first.buffered = 512
        try:
            assert await engine._get_or_create_client("s-1", "web", None) is first
            first.buffered = 0
            assert await engine._get_or_create_client("s-1", "web", None) is not first
        finally:
            await engine.shutdown()

    @pytest.mark.asyncio
    async def test_keeps_the_client_when_the_conversation_cannot_resume(
        self, engine, db, gateway,
    ):
        builds = await _running_session(engine, db, gateway)
        first = builds[0].client
        await engine._refresh_mcp_catalog()
        claude = engine._backends["claude"]
        try:
            with patch.object(claude, "validate_resume_target", return_value=False):
                assert await engine._get_or_create_client("s-1", "web", None) is first
        finally:
            await engine.shutdown()
        assert len(builds) == 1
        session = await db.get_session("s-1")
        assert session["sdk_session_id"] == "native-1"


class _TurnClient:
    """A client that answers each turn with one text and a result."""

    model = "claude-test"

    def __init__(self, native_id: str):
        self.native_session_id = native_id
        self.turns: list[str] = []
        self.disconnected = False

    async def start_turn(self, turn):
        self.turns.append(turn.text)

    async def receive_turn(self):
        messages = [
            AssistantMessage(content=[TextBlock(text="ok")], model="claude-test"),
            ResultMessage(
                subtype="success", duration_ms=1, duration_api_ms=1,
                is_error=False, num_turns=1, session_id=self.native_session_id,
                total_cost_usd=0.0, usage={"input_tokens": 1},
            ),
        ]
        for message in messages:
            for event in translate_message(message):
                yield event

    def is_alive(self):
        return True

    def buffer_used(self):
        return 0

    def try_receive_idle_events(self):
        return None

    async def disconnect(self):
        self.disconnected = True


@pytest.mark.asyncio
async def test_a_turn_of_a_running_session_starts_on_the_new_catalog(
    tmp_path, db, external,
):
    """Through ``run()``: the turn after a catalog read goes to a new client
    that resumes the conversation and has the new servers."""
    gateway = FakeGateway(_document("docs", revision="sha256:one"))
    engine = _engine(tmp_path, db, gateway)
    claude = engine._backends["claude"]
    clients: list[_TurnClient] = []
    servers: list[list[str]] = []
    specs: list[SessionSpec] = []

    async def create_client(spec):
        specs.append(spec)
        servers.append(sorted(s.name for s in engine._mcp_servers_cache))
        clients.append(_TurnClient(spec.resume_native_id or "native-1"))
        return clients[-1]

    try:
        with patch.object(claude, "create_client", create_client), \
             patch.object(claude, "validate_resume_target", return_value=True):
            await engine.run("s-1", "first", source="web", channel="web", actor=None)
            gateway.document = _document("docs", "wiki", revision="sha256:two")
            await engine._refresh_mcp_catalog()
            await engine.run("s-1", "second", source="web", channel="web", actor=None)
    finally:
        await engine.shutdown()

    assert servers == [["docs"], ["docs", "wiki"]]
    assert specs[1].resume_native_id == "native-1"
    assert clients[0].disconnected
    [first_turn] = clients[0].turns
    [second_turn] = clients[1].turns
    assert "first" in first_turn and "second" in second_turn
    assert (await db.get_session("s-1"))["sdk_session_id"] == "native-1"


class TestLocalModeUnchanged:
    def test_clients_use_the_configured_servers_and_plugins(self, tmp_path, db):
        engine = _engine(tmp_path, db, FakeGateway())
        assert engine._mcp_gateway is None
        assert engine._mcp_catalog_revision is None
        plugin = {"type": "local", "path": str(tmp_path)}
        engine._claude_code_plugins = [plugin]
        options = _claude_options(engine)
        assert set(options.mcp_servers) == {"nerve", "local-files", "remote-api"}
        assert options.strict_mcp_config is False
        assert options.plugins == [plugin]

        overrides = engine._backends["codex"].build_config_overrides(_spec(engine.config))
        assert 'mcp_servers.remote-api.url="https://mcp.example.com/v1"' in overrides
        assert not any("default_tools_approval_mode" in o and "remote-api" in o
                       for o in overrides)

    @pytest.mark.asyncio
    async def test_reload_reads_the_files(self, tmp_path, db):
        engine = _engine(tmp_path, db)
        with patch("nerve.config.load_mcp_servers", return_value=[]) as yaml_servers, \
             patch("nerve.config.load_claude_code_plugins", return_value=[]):
            await engine.reload_mcp_config()
        yaml_servers.assert_called_once()

    @pytest.mark.asyncio
    async def test_a_running_session_keeps_its_client_after_a_reload(self, tmp_path, db):
        engine = _engine(tmp_path, db)
        _capture_claude_clients(engine)
        await db.create_session("s-1", source="web", actor=None)
        first = await engine._get_or_create_client("s-1", "web", None)
        with patch("nerve.config.load_mcp_servers", return_value=[]), \
             patch("nerve.config.load_claude_code_plugins", return_value=[]):
            await engine.reload_mcp_config()
        assert await engine._get_or_create_client("s-1", "web", None) is first
        await engine.shutdown()


class TestGatewayUrlSetting:
    @pytest.mark.parametrize(("value", "expected"), [
        (None, ""),
        ("  ", ""),
        ("http://192.0.2.1:8080/", "http://192.0.2.1:8080"),
        ("https://gw.example/base//", "https://gw.example/base"),
        ("http://[2001:db8::1]:3130", "http://[2001:db8::1]:3130"),
    ])
    def test_accepted(self, tmp_path, value, expected):
        assert _config(tmp_path, mcp_gateway_url=value).mcp_gateway_url == expected

    @pytest.mark.parametrize("value", [
        "192.0.2.1:8080",
        "ftp://192.0.2.1",
        "http://",
        "http://user:secret@192.0.2.1",
        "http://192.0.2.1/?agent=1",
        "http://192.0.2.1/#part",
        "http://192.0.2.1/?",
        "http://192.0.2.1:99999",
        "http://192.0.2.1:0",
        "http://192.0.2.1/a b",
        "http://[2001:db8::1",
    ])
    def test_refused(self, tmp_path, value):
        with pytest.raises(ConfigError):
            _config(tmp_path, mcp_gateway_url=value)
