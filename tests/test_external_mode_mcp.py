"""MCP servers from the MCP gateway in external mode.

In external mode Nerve takes its MCP servers only from the gateway's catalog
and gives them to Claude and Codex. Local mode does not change. The gateway
is an httpx transport (tests/test_mcp_gateway.py).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest

from nerve.agent.backends import SessionSpec
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

    def is_alive(self):
        return True

    async def disconnect(self):
        pass


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
    async def test_a_new_catalog_reaches_new_sessions_only(self, tmp_path, db, external):
        gateway = FakeGateway(_document("docs", "issues", revision="sha256:one"))
        engine = _engine(tmp_path, db, gateway)
        built = _capture_claude_clients(engine)
        try:
            await db.create_session("s-1", source="web", actor=None)
            first = await engine._get_or_create_client("s-1", "web", None)
            assert engine._mcp_catalog_revision == "sha256:one"

            gateway.document = _document("docs", "wiki", revision="sha256:two")
            # The running session keeps its client and its servers.
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
