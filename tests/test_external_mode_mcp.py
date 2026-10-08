"""MCP servers from the MCP gateway in external mode.

In external mode Nerve takes its MCP servers only from the gateway's catalog
and writes the client configuration for Claude and Codex from it. Local mode
does not change. The gateway is a fake HTTP server on loopback.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import pytest_asyncio

from nerve.agent.backends import SessionSpec
from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig
from nerve.gateway.auth import AUTH_MODE_ENV
from tests.fake_mcp_gateway import FakeMcpGateway, catalog_payload

LOCKDOWN_FEATURES = [
    "features.apps=false",
    "features.plugins=false",
    "features.skill_mcp_dependency_install=false",
]


@pytest.fixture
def gateway():
    fake = FakeMcpGateway().start()
    yield fake
    fake.stop()


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


def _spec(cfg: NerveConfig, session_id: str = "s1", source: str = "web") -> SessionSpec:
    return SessionSpec(
        session_id=session_id, source=source, model=None, effort="high",
        system_prompt="You are Nerve.", cwd=str(cfg.workspace),
    )


def _claude_options(engine: AgentEngine, spec: SessionSpec):
    claude = engine._backends["claude"]
    with patch.object(claude, "_build_hooks", return_value={}):
        return claude._build_options(spec)


class _StubClient:
    model = "stub-model"

    def is_alive(self):
        return True

    async def disconnect(self):
        pass


def _capture_clients(engine: AgentEngine, name: str) -> list:
    """Make ``create_client`` record the client configuration it would use."""
    backend = engine._backends[name]
    built: list = []

    async def create_client(spec):
        if name == "claude":
            built.append(_claude_options(engine, spec))
        else:
            built.append(backend.build_config_overrides(spec))
        return _StubClient()

    backend.create_client = create_client
    return built


def _gateway_overrides(overrides: list[str]) -> list[str]:
    return [o for o in overrides if o.startswith(("mcp_servers.", "features."))
            and not o.startswith("mcp_servers.nerve.")]


class TestClaudeConfiguration:
    @pytest.mark.asyncio
    async def test_gateway_servers_only(self, tmp_path, db, gateway, external):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        engine._claude_code_plugins = [{"type": "local", "path": str(tmp_path)}]
        await engine._mcp_gateway.start()
        try:
            options = _claude_options(engine, _spec(engine.config))
        finally:
            await engine._mcp_gateway.close()

        assert set(options.mcp_servers) == {"nerve", "docs", "github"}
        assert options.mcp_servers["docs"] == {
            "type": "http", "url": f"{gateway.url}/s/docs/mcp",
        }
        assert options.mcp_servers["github"] == {
            "type": "http", "url": f"{gateway.url}/s/github/mcp",
        }
        assert options.strict_mcp_config is True
        assert options.allowed_tools == ["mcp__docs", "mcp__github"]
        assert options.plugins == []

    @pytest.mark.asyncio
    async def test_no_catalog_yet_means_nerve_only(self, tmp_path, db, gateway, external):
        gateway.stop()
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        try:
            options = _claude_options(engine, _spec(engine.config))
        finally:
            await engine._mcp_gateway.close()
        assert set(options.mcp_servers) == {"nerve"}
        assert options.strict_mcp_config is True
        assert options.allowed_tools == []
        assert options.plugins == []

    def test_external_mode_without_gateway_url(self, tmp_path, db, external):
        engine = AgentEngine(_config(tmp_path), db)
        assert engine._mcp_gateway is None
        assert engine.managed_mcp_servers() == ()
        options = _claude_options(engine, _spec(engine.config))
        assert set(options.mcp_servers) == {"nerve"}
        assert options.strict_mcp_config is True
        assert engine.mcp_gateway_status()["error"] == "mcp_gateway_url is not set"

    @pytest.mark.asyncio
    async def test_a_catalog_server_named_nerve_is_not_used(
        self, tmp_path, db, gateway, external,
    ):
        gateway.catalog = catalog_payload(3, {"nerve": ["x"], "docs": ["search"]})
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        try:
            options = _claude_options(engine, _spec(engine.config))
        finally:
            await engine._mcp_gateway.close()
        assert [server.id for server in engine.managed_mcp_servers()] == ["docs"]
        assert options.mcp_servers["nerve"] != {
            "type": "http", "url": f"{gateway.url}/s/nerve/mcp",
        }
        assert options.allowed_tools == ["mcp__docs"]


class TestCodexConfiguration:
    @pytest.mark.asyncio
    async def test_gateway_servers_approve_their_tools(
        self, tmp_path, db, gateway, external,
    ):
        cfg = _config(tmp_path, mcp_gateway_url=gateway.url, codex={
            "home_dir": str(tmp_path / "codex-home"),
            "extra_config": {
                "mcp_servers.extra.url": "https://mcp.example.com/extra",
                "model_verbosity": "low",
            },
        })
        engine = AgentEngine(cfg, db)
        await engine._mcp_gateway.start()
        try:
            codex = engine._backends["codex"]
            overrides = codex.build_config_overrides(_spec(cfg))
            env = codex.build_env(_spec(cfg))
        finally:
            await engine._mcp_gateway.close()

        assert _gateway_overrides(overrides) == [
            f'mcp_servers.docs.url="{gateway.url}/s/docs/mcp"',
            'mcp_servers.docs.default_tools_approval_mode="approve"',
            f'mcp_servers.github.url="{gateway.url}/s/github/mcp"',
            'mcp_servers.github.default_tools_approval_mode="approve"',
            *LOCKDOWN_FEATURES,
        ]
        # The lockdown comes last, so no earlier override can change it.
        assert overrides[-3:] == LOCKDOWN_FEATURES
        assert 'model_verbosity="low"' in overrides
        assert not any("local-files" in o or "remote-api" in o for o in overrides)
        assert not any(key.startswith("NERVE_CODEX_MCP_EXTERNAL_") for key in env)

    @pytest.mark.asyncio
    async def test_servers_in_codex_files_are_turned_off(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        home = tmp_path / "codex-home"
        home.mkdir()
        (home / "config.toml").write_text(
            '[mcp_servers.personal]\nurl = "https://mcp.example.com/p"\n'
            '[mcp_servers."has.dot"]\ncommand = "/usr/bin/true"\n',
            encoding="utf-8",
        )
        system = tmp_path / "etc-codex-config.toml"
        system.write_text(
            '[mcp_servers.fleet]\ncommand = "/usr/bin/true"\n', encoding="utf-8",
        )
        monkeypatch.setattr(
            "nerve.agent.backends.codex.backend._CODEX_SYSTEM_CONFIG", system,
        )
        cfg = _config(tmp_path, mcp_gateway_url=gateway.url)
        engine = AgentEngine(cfg, db)
        await engine._mcp_gateway.start()
        try:
            overrides = engine._backends["codex"].build_config_overrides(_spec(cfg))
        finally:
            await engine._mcp_gateway.close()

        assert "mcp_servers.personal.enabled=false" in overrides
        assert "mcp_servers.fleet.enabled=false" in overrides
        assert f'mcp_servers.docs.url="{gateway.url}/s/docs/mcp"' in overrides
        assert not any("has.dot" in o for o in overrides)

    @pytest.mark.asyncio
    async def test_a_colliding_user_server_is_removed_before_codex_starts(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """A stdio server named like a gateway server must not stop Codex.

        Codex merges the file's table with the overrides, so the session would
        fail with "url is not supported for stdio". The entry is removed from
        the user file first; a copy of the file stays next to it.
        """
        home = tmp_path / "codex-home"
        home.mkdir()
        original = (
            '# operator note\n'
            '[mcp_servers.docs]\ncommand = "/usr/bin/true"\n'
            '[mcp_servers.personal]\nurl = "https://mcp.example.com/p"\n'
        )
        (home / "config.toml").write_text(original, encoding="utf-8")
        cfg = _config(tmp_path, mcp_gateway_url=gateway.url)
        engine = AgentEngine(cfg, db)
        backend = engine._backends["codex"]
        written: list = []

        async def write(path, edits):
            written.append((path, edits))
            # What Codex's writer does with these edits.
            path.write_text(
                '# operator note\n'
                '[mcp_servers.personal]\nurl = "https://mcp.example.com/p"\n',
                encoding="utf-8",
            )

        async def version_ok():
            return "codex-cli 0.155.1"

        class StubClient:
            def __init__(self, backend, spec):
                self.overrides = backend.build_config_overrides(spec)

            async def connect(self):
                pass

            async def disconnect(self):
                pass

        monkeypatch.setattr(backend, "_write_codex_config", write)
        monkeypatch.setattr(backend, "_check_cli_version", version_ok)
        monkeypatch.setattr(
            "nerve.agent.backends.codex.backend.CodexClient", StubClient,
        )
        await engine._mcp_gateway.start()
        try:
            client = await backend.create_client(_spec(cfg))
            # A second session finds nothing more to remove.
            await backend.create_client(_spec(cfg, session_id="s2"))
        finally:
            await engine._mcp_gateway.close()

        assert written == [(home / "config.toml", [
            {"keyPath": "mcp_servers.docs", "value": None, "mergeStrategy": "replace"},
        ])]
        backups = list(home.glob("config.toml.nerve-mcp-backup-*"))
        assert len(backups) == 1
        assert backups[0].read_text(encoding="utf-8") == original
        assert backups[0].stat().st_mode & 0o777 == 0o600
        assert f'mcp_servers.docs.url="{gateway.url}/s/docs/mcp"' in client.overrides
        assert "mcp_servers.personal.enabled=false" in client.overrides

    @pytest.mark.asyncio
    async def test_a_failed_removal_stops_the_session_with_the_reason(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        from nerve.agent.backends.base import BackendError

        home = tmp_path / "codex-home"
        home.mkdir()
        (home / "config.toml").write_text(
            '[mcp_servers.github]\ncommand = "/usr/bin/true"\n', encoding="utf-8",
        )
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        backend = engine._backends["codex"]

        async def write(path, edits):
            raise RuntimeError("config file is read-only")

        monkeypatch.setattr(backend, "_write_codex_config", write)
        await engine._mcp_gateway.start()
        try:
            with pytest.raises(BackendError) as refused:
                await backend._remove_colliding_user_mcp_servers(
                    engine.managed_mcp_servers(),
                )
        finally:
            await engine._mcp_gateway.close()
        assert "github" in str(refused.value)
        assert str(home / "config.toml") in str(refused.value)
        assert "config file is read-only" in str(refused.value)

    @pytest.mark.asyncio
    async def test_local_mode_leaves_the_user_file_alone(
        self, tmp_path, db, monkeypatch,
    ):
        home = tmp_path / "codex-home"
        home.mkdir()
        (home / "config.toml").write_text(
            '[mcp_servers.docs]\ncommand = "/usr/bin/true"\n', encoding="utf-8",
        )
        engine = AgentEngine(_config(tmp_path), db)
        backend = engine._backends["codex"]
        calls = []

        async def write(path, edits):
            calls.append(edits)

        async def version_ok():
            return "codex-cli 0.155.1"

        class StubClient:
            def __init__(self, backend, spec):
                pass

            async def connect(self):
                pass

        monkeypatch.setattr(backend, "_write_codex_config", write)
        monkeypatch.setattr(backend, "_check_cli_version", version_ok)
        monkeypatch.setattr(
            "nerve.agent.backends.codex.backend.CodexClient", StubClient,
        )
        await backend.create_client(_spec(engine.config))
        assert calls == []
        assert not list(home.glob("config.toml.nerve-mcp-backup-*"))

    @pytest.mark.asyncio
    async def test_a_system_file_collision_is_left_out_and_reported(
        self, tmp_path, db, gateway, external, monkeypatch, caplog,
    ):
        """Nerve does not change /etc/codex/config.toml, and Codex cannot merge
        a stdio server with a gateway server of the same name.

        The session starts without that gateway server; diagnostics and the
        MCP server API name it, and the error is logged once per generation.
        """
        import logging

        from nerve.gateway.routes import _deps
        from nerve.gateway.routes.mcp_servers import (
            get_mcp_server_detail,
            list_mcp_servers,
        )

        system = tmp_path / "etc-codex-config.toml"
        system.write_text('[mcp_servers.docs]\ncommand = "/usr/bin/true"\n')
        monkeypatch.setattr(
            "nerve.agent.backends.codex.backend._CODEX_SYSTEM_CONFIG", system,
        )
        reason = f"name used by the system configuration ({system})"
        cfg = _config(tmp_path, mcp_gateway_url=gateway.url)
        engine = AgentEngine(cfg, db)
        codex = engine._backends["codex"]
        previous = _deps._deps
        caplog.set_level(logging.ERROR, logger="nerve.agent.engine")
        try:
            await engine._mcp_gateway.start()
            # Three sessions of one generation: one error.
            built = [codex.build_config_overrides(_spec(cfg, session_id=f"s{i}"))
                     for i in range(3)]
            await engine._mcp_gateway.refresh()
            errors = [r for r in caplog.records if "not applied to codex" in r.getMessage()]
            assert len(errors) == 1
            assert "'docs' (catalog generation 7)" in errors[0].getMessage()
            assert reason in errors[0].getMessage()

            status = engine.mcp_gateway_status()
            assert status["not_applied"] == {"docs": {"codex": reason}}
            _deps.init_deps(engine, db)
            rows = {row["name"]: row for row in (await list_mcp_servers())["servers"]}
            assert rows["docs"]["not_applied"] == {"codex": reason}
            assert rows["github"]["not_applied"] == {}
            assert (await get_mcp_server_detail("docs"))["not_applied"] == {"codex": reason}

            # A new generation logs it again.
            gateway.catalog = catalog_payload(8, {"docs": ["search"]})
            await engine._mcp_gateway.refresh()
            errors = [r for r in caplog.records if "not applied to codex" in r.getMessage()]
            assert len(errors) == 2
            assert "(catalog generation 8)" in errors[1].getMessage()

            # The system file is fixed: the report follows at once.
            system.write_text("")
            assert engine.mcp_gateway_status()["not_applied"] == {}
        finally:
            _deps._deps = previous
            await engine._mcp_gateway.close()

        for overrides in built:
            assert not any(o.startswith("mcp_servers.docs.url=") for o in overrides)
            assert "mcp_servers.docs.enabled=false" in overrides
            assert f'mcp_servers.github.url="{gateway.url}/s/github/mcp"' in overrides

    def test_ultracode_is_off_in_external_mode(self, tmp_path, db, external):
        cfg = _config(tmp_path, codex={
            "home_dir": str(tmp_path / "codex-home"),
            "ultracode": {"enabled": True},
        })
        engine = AgentEngine(cfg, db)
        codex = engine._backends["codex"]
        assert codex._ultracode_enabled is False
        env = codex.build_env(_spec(cfg))
        assert "CODEX_CLI_PATH" not in env
        assert "ULTRACODE_UI" not in env

    @pytest.mark.asyncio
    async def test_the_pinned_codex_accepts_every_key(
        self, tmp_path, db, gateway, external,
    ):
        """Codex refuses an unknown key with ``--strict-config``.

        Runs the Codex binary on PATH when its version is in the range that
        Nerve supports, and checks the effective configuration. The user file
        has a stdio server with a gateway server's name, which Codex's own
        writer removes before the session.
        """
        cfg = _config(tmp_path, mcp_gateway_url=gateway.url)
        codex = shutil.which(cfg.codex.bin_path)
        if codex is None:
            pytest.skip("no Codex binary on PATH")
        home = tmp_path / "codex-home"
        home.mkdir()
        (home / "config.toml").write_text(
            '# operator note\nmodel_verbosity = "low"\n\n'
            '[mcp_servers.docs]\ncommand = "/usr/bin/true"\n\n'
            '[mcp_servers.personal]\nurl = "https://mcp.example.com/p"\n',
            encoding="utf-8",
        )
        engine = AgentEngine(cfg, db)
        backend = engine._backends["codex"]
        try:
            await backend._check_cli_version()
        except Exception as e:  # noqa: BLE001
            pytest.skip(f"Codex on PATH is not a supported version: {e}")
        await engine._mcp_gateway.start()
        try:
            await backend._remove_colliding_user_mcp_servers(engine.managed_mcp_servers())
            overrides = backend.build_config_overrides(_spec(cfg))
        finally:
            await engine._mcp_gateway.close()

        remaining = (home / "config.toml").read_text(encoding="utf-8")
        assert "[mcp_servers.docs]" not in remaining
        assert "[mcp_servers.personal]" in remaining
        assert '# operator note\nmodel_verbosity = "low"' in remaining
        effective = _codex_effective_config(codex, overrides, str(home))
        servers = effective["mcp_servers"]
        assert servers["docs"]["url"] == f"{gateway.url}/s/docs/mcp"
        assert "command" not in servers["docs"]
        assert servers["docs"]["default_tools_approval_mode"] == "approve"
        assert servers["github"]["default_tools_approval_mode"] == "approve"
        assert servers["personal"]["enabled"] is False
        features = effective["features"]
        assert features["apps"] is False
        assert features["plugins"] is False
        assert features["skill_mcp_dependency_install"] is False


def _codex_effective_config(codex: str, overrides: list[str], home: str) -> dict:
    """Start ``codex app-server --strict-config`` and read its configuration."""
    Path(home).mkdir(parents=True, exist_ok=True)
    args = [codex]
    for kv in overrides:
        args += ["--config", kv]
    args += ["app-server", "--strict-config", "--listen", "stdio://"]
    requests = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "clientInfo": {"name": "nerve-test", "title": "Nerve test", "version": "0"},
            "capabilities": {"experimentalApi": True},
        }},
        {"jsonrpc": "2.0", "method": "initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "config/read", "params": {}},
    ]
    proc = subprocess.Popen(
        args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, env={**os.environ, "CODEX_HOME": home}, cwd=home,
    )
    try:
        for request in requests:
            proc.stdin.write(json.dumps(request) + "\n")
            proc.stdin.flush()
        while True:
            line = proc.stdout.readline()
            if not line:
                raise AssertionError(f"Codex refused the configuration: {proc.stderr.read()}")
            message = json.loads(line)
            if message.get("id") == 2:
                assert "error" not in message, message
                return message["result"]["config"]
    finally:
        proc.kill()
        proc.wait(5)


class TestNewSessions:
    @pytest.mark.asyncio
    async def test_a_new_generation_reaches_new_sessions_only(
        self, tmp_path, db, gateway, external,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        built = _capture_clients(engine, "claude")
        await engine._mcp_gateway.start()
        try:
            await db.create_session("s-1", source="web", actor=None)
            first = await engine._get_or_create_client("s-1", "web", None)

            gateway.catalog = catalog_payload(8, {"docs": ["search"], "jira": ["create"]})
            # The running session keeps its client and its servers.
            assert await engine._get_or_create_client("s-1", "web", None) is first
            assert len(built) == 1

            await db.create_session("s-2", source="web", actor=None)
            await engine._get_or_create_client("s-2", "web", None)
        finally:
            await engine._mcp_gateway.close()

        assert set(built[0].mcp_servers) == {"nerve", "docs", "github"}
        assert set(built[1].mcp_servers) == {"nerve", "docs", "jira"}
        assert built[1].allowed_tools == ["mcp__docs", "mcp__jira"]
        assert engine.mcp_gateway_status()["generation"] == 8

    @pytest.mark.asyncio
    async def test_codex_sessions_follow_the_catalog(self, tmp_path, db, gateway, external):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        built = _capture_clients(engine, "codex")
        await engine._mcp_gateway.start()
        try:
            await db.create_session("c-1", source="web", backend="codex", actor=None)
            await engine._get_or_create_client("c-1", "web", None)
            gateway.catalog = catalog_payload(9, {"jira": ["create"]})
            await db.create_session("c-2", source="web", backend="codex", actor=None)
            await engine._get_or_create_client("c-2", "web", None)
        finally:
            await engine._mcp_gateway.close()
        assert any(o.startswith("mcp_servers.github.url=") for o in built[0])
        assert not any(o.startswith("mcp_servers.github.") for o in built[1])
        assert f'mcp_servers.jira.url="{gateway.url}/s/jira/mcp"' in built[1]

    @pytest.mark.asyncio
    async def test_outage_at_startup_then_recovery(self, tmp_path, db, gateway, external):
        gateway.stop()
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        built = _capture_clients(engine, "claude")
        with patch("nerve.memory.memu_bridge.MemUBridge", _NoMemory), \
             patch("nerve.memory.xmemory_bridge.XmemoryBridge", _NoMemory):
            await engine.initialize()
        try:
            status = engine.mcp_gateway_status()
            assert status["generation"] is None
            assert status["retrying"] is True
            assert "cannot reach the MCP gateway" in status["error"]

            await db.create_session("s-1", source="web", actor=None)
            await engine._get_or_create_client("s-1", "web", None)
            assert set(built[0].mcp_servers) == {"nerve"}

            # The gateway is back: the next new session reads the catalog.
            gateway.start()
            await db.create_session("s-2", source="web", actor=None)
            await engine._get_or_create_client("s-2", "web", None)
            assert set(built[1].mcp_servers) == {"nerve", "docs", "github"}
            assert engine.mcp_gateway_status()["error"] is None
            assert engine.mcp_gateway_status()["retrying"] is False
        finally:
            await engine.shutdown()

    @pytest.mark.asyncio
    async def test_initialize_reads_the_catalog_and_no_plugins(
        self, tmp_path, db, gateway, external,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        with patch("nerve.memory.memu_bridge.MemUBridge", _NoMemory), \
             patch("nerve.memory.xmemory_bridge.XmemoryBridge", _NoMemory), \
             patch("nerve.config.load_claude_code_plugins") as plugins:
            await engine.initialize()
        try:
            plugins.assert_not_called()
            assert engine.mcp_gateway_status()["generation"] == 7
            rows = {row["name"]: row for row in await db.get_mcp_server_stats()}
            assert set(rows) == {"nerve", "docs", "github"}
            assert rows["docs"]["type"] == "http"
            assert rows["docs"]["tool_count"] == 2
        finally:
            await engine.shutdown()
        assert engine._mcp_gateway.retrying is False

    @pytest.mark.asyncio
    async def test_reload_reads_the_catalog_not_the_files(
        self, tmp_path, db, gateway, external,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        gateway.catalog = catalog_payload(8, {"jira": ["create"]})
        with patch("nerve.config.load_mcp_servers") as yaml_servers:
            servers = await engine.reload_mcp_config()
        await engine._mcp_gateway.close()
        yaml_servers.assert_not_called()
        assert [(s.name, s.type, s.url) for s in servers] == [
            ("jira", "http", f"{gateway.url}/s/jira/mcp"),
        ]
        assert engine._mcp_servers_cache == []

    @pytest.mark.asyncio
    async def test_a_denied_call_keeps_the_server_and_the_message(
        self, tmp_path, db, gateway, external,
    ):
        from nerve.agent.backends.events import ToolResult

        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        await engine._sync_mcp_servers_to_db()
        await engine._mcp_gateway.close()
        message = "tool not allowed by tenant policy"
        blocks = [{"type": "tool_call", "tool_use_id": "t1"}]
        await engine._process_tool_result(
            ToolResult(tool_use_id="t1", content=message, is_error=True),
            "s1", {}, blocks,
            [{"tool_use_id": "t1", "tool": "mcp__docs__search"}], {},
        )
        assert blocks[0]["result"] == message
        rows = {row["name"]: row for row in await db.get_mcp_server_stats()}
        # The usage record does not turn the gateway server into a plugin.
        assert rows["docs"]["type"] == "http"
        usage = await db.get_mcp_server_usage("docs")
        assert [(u["tool_name"], u["error"]) for u in usage] == [("search", message)]


class TestDenialMessage:
    """A denial from the gateway reaches the model and the UI unchanged.

    The clients talk to the gateway URL directly, with no wrapper of Nerve in
    between, so the JSON-RPC error message is the gateway's own. Nerve's
    event translation keeps it.
    """

    @pytest.mark.asyncio
    async def test_codex_tool_result_keeps_the_message(self):
        from nerve.agent.backends.codex.backend import CodexClient

        message = "Mcp error: -32001: tool not allowed by tenant policy"
        client = CodexClient.__new__(CodexClient)
        client._items = {}
        item = {
            "type": "mcpToolCall", "id": "call-1", "server": "docs",
            "tool": "search", "status": "failed", "error": {"message": message},
        }
        results = await client._map_item_completed(item)
        assert len(results) == 1
        assert json.loads(results[0].content) == {"message": message}
        assert results[0].is_error is True

    @pytest.mark.asyncio
    async def test_gateway_urls_are_the_denial_source(self, tmp_path, db, gateway, external):
        """A tools/call to the configured URL gets the gateway's denial."""
        import httpx

        gateway.denied_tools.add("search")
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        try:
            options = _claude_options(engine, _spec(engine.config))
            url = options.mcp_servers["docs"]["url"]
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json={
                    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": {"name": "search", "arguments": {}},
                })
        finally:
            await engine._mcp_gateway.close()
        assert response.json()["error"] == {
            "code": -32001, "message": "tool not allowed by tenant policy",
        }
        assert gateway.mcp_requests[-1][0] == "docs"

    def test_claude_tool_result_keeps_the_message(self):
        from claude_agent_sdk import ToolResultBlock

        from nerve.agent.backends.claude import _translate_tool_result

        message = "tool not allowed by tenant policy"
        event = _translate_tool_result(
            ToolResultBlock(tool_use_id="t1", content=message, is_error=True), None,
        )
        assert event.content == message
        assert event.is_error is True


class _NoMemory:
    available = False

    def __init__(self, *args, **kwargs):
        pass

    async def initialize(self):
        return False

    async def aclose(self):
        pass


class TestLocalModeUnchanged:
    """Without external mode, the configuration is what it was."""

    def test_claude_uses_the_configured_servers_and_plugins(self, tmp_path, db, gateway):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        assert engine.managed_mcp is False
        assert engine._mcp_gateway is None
        assert engine.managed_mcp_servers() is None
        assert engine.mcp_gateway_status() is None
        plugin = {"type": "local", "path": str(tmp_path)}
        engine._claude_code_plugins = [plugin]
        options = _claude_options(engine, _spec(engine.config))
        assert set(options.mcp_servers) == {"nerve", "local-files", "remote-api"}
        assert options.mcp_servers["remote-api"] == {
            "type": "http", "url": "https://mcp.example.com/v1",
            "headers": {"Authorization": "Bearer placeholder"},
        }
        assert options.strict_mcp_config is False
        assert options.allowed_tools == []
        assert options.plugins == [plugin]
        assert gateway.catalog_requests == 0

    def test_claude_options_match_a_backend_without_the_new_dependency(self, tmp_path, db):
        """The options equal those of a backend that never heard of the gateway."""
        from nerve.agent.backends.claude import ClaudeBackend

        engine = AgentEngine(_config(tmp_path), db)
        claude = engine._backends["claude"]
        legacy = ClaudeBackend(SimpleNamespace(**{
            key: value for key, value in vars(claude._deps).items()
            if key != "managed_mcp_servers"
        }))
        spec = _spec(engine.config)
        with patch.object(legacy, "_build_hooks", return_value={}):
            expected = legacy._build_options(spec)
        actual = _claude_options(engine, spec)
        for name in ("strict_mcp_config", "allowed_tools", "plugins", "disallowed_tools"):
            assert getattr(actual, name) == getattr(expected, name)
        assert set(actual.mcp_servers) == set(expected.mcp_servers)

    def test_codex_uses_the_configured_servers(self, tmp_path, db, gateway):
        cfg = _config(tmp_path, mcp_gateway_url=gateway.url)
        engine = AgentEngine(cfg, db)
        codex = engine._backends["codex"]
        overrides = codex.build_config_overrides(_spec(cfg))
        assert any(o.startswith("mcp_servers.local-files.command=") for o in overrides)
        assert 'mcp_servers.remote-api.url="https://mcp.example.com/v1"' in overrides
        assert not any(o.startswith("features.") for o in overrides)
        assert not any("default_tools_approval_mode" in o and "remote-api" in o
                       for o in overrides)
        env = codex.build_env(_spec(cfg))
        assert any(key.startswith("NERVE_CODEX_MCP_EXTERNAL_") for key in env)

    def test_codex_overrides_match_a_backend_without_the_new_dependency(
        self, tmp_path, db,
    ):
        from nerve.agent.backends.codex import CodexBackend

        cfg = _config(tmp_path, codex={
            "home_dir": str(tmp_path / "codex-home"),
            "extra_config": {"mcp_servers.extra.url": "https://mcp.example.com/e"},
        })
        engine = AgentEngine(cfg, db)
        codex = engine._backends["codex"]
        legacy = CodexBackend(SimpleNamespace(**{
            key: value for key, value in vars(codex._deps).items()
            if key != "managed_mcp_servers"
        }))
        spec = _spec(cfg)
        assert codex.build_config_overrides(spec) == legacy.build_config_overrides(spec)
        assert codex.build_env(spec) == legacy.build_env(spec)

    @pytest.mark.asyncio
    async def test_reload_reads_the_files(self, tmp_path, db):
        engine = AgentEngine(_config(tmp_path), db)
        with patch("nerve.config.load_mcp_servers", return_value=[]) as yaml_servers, \
             patch("nerve.config.load_claude_code_plugins", return_value=[]):
            await engine.reload_mcp_config()
        yaml_servers.assert_called_once()


class TestReadOnlyInExternalMode:
    """Catalog servers are read-only and managed by the organization."""

    @pytest_asyncio.fixture
    async def routes(self, tmp_path, db, gateway, external):
        from nerve.gateway.routes import _deps

        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        await engine._sync_mcp_servers_to_db()
        # A server from an earlier local mode stays in the database.
        await db.upsert_mcp_server(name="local-files", server_type="stdio")
        previous = _deps._deps
        _deps.init_deps(engine, db)
        yield engine
        _deps._deps = previous
        await engine._mcp_gateway.close()

    @pytest.mark.asyncio
    async def test_list_marks_the_catalog_servers(self, routes):
        from nerve.gateway.routes.mcp_servers import list_mcp_servers

        listed = await list_mcp_servers()
        assert listed["managed_by"] == "organization"
        servers = {row["name"]: row for row in listed["servers"]}
        assert set(servers) == {"nerve", "docs", "github"}
        assert servers["docs"]["managed_by"] == "organization"
        assert servers["docs"]["display_name"] == "Docs"
        assert servers["docs"]["description"] == "The docs server."
        assert servers["nerve"]["managed_by"] is None

    @pytest.mark.asyncio
    async def test_detail_of_a_catalog_server(self, routes):
        from fastapi import HTTPException

        from nerve.gateway.routes.mcp_servers import get_mcp_server_detail

        detail = await get_mcp_server_detail("docs")
        assert detail["managed_by"] == "organization"
        assert detail["tools"] == []
        with pytest.raises(HTTPException) as refused:
            await get_mcp_server_detail("local-files")
        assert refused.value.status_code == 404

    @pytest.mark.asyncio
    async def test_reload_is_refused(self, routes):
        from fastapi import HTTPException

        from nerve.gateway.routes.mcp_servers import reload_mcp_servers

        with patch("nerve.config.load_mcp_servers") as yaml_servers:
            with pytest.raises(HTTPException) as refused:
                await reload_mcp_servers()
        assert refused.value.status_code == 409
        assert "organization manages the MCP servers" in refused.value.detail
        yaml_servers.assert_not_called()

    @pytest.mark.asyncio
    async def test_usage_of_a_hidden_server_is_refused(self, routes, db):
        from fastapi import HTTPException

        from nerve.gateway.routes.mcp_servers import get_mcp_server_usage

        await db.record_mcp_tool_usage(server_name="local-files", tool_name="read")
        await db.record_mcp_tool_usage(server_name="docs", tool_name="search")
        with pytest.raises(HTTPException) as refused:
            await get_mcp_server_usage("local-files")
        assert refused.value.status_code == 404
        usage = await get_mcp_server_usage("docs")
        assert [row["tool_name"] for row in usage["usage"]] == ["search"]
        assert (await get_mcp_server_usage("nerve"))["usage"] == []

    @pytest.mark.asyncio
    async def test_a_catalog_server_without_a_row_is_still_listed(self, routes, db):
        """The list follows the applied catalog also when a row write failed."""
        from nerve.gateway.routes.mcp_servers import (
            get_mcp_server_detail,
            list_mcp_servers,
        )

        await db._write("DELETE FROM mcp_servers WHERE name = ?", ("github",))
        servers = {row["name"]: row for row in (await list_mcp_servers())["servers"]}
        assert list(servers) == ["docs", "github", "nerve"]
        github = servers["github"]
        assert github["managed_by"] == "organization"
        assert github["type"] == "http"
        assert github["tool_count"] == 1
        assert github["total_invocations"] == 0
        assert github["first_seen_at"] == routes.mcp_gateway_status()["applied_at"]
        detail = await get_mcp_server_detail("github")
        assert detail["display_name"] == "Github"
        assert detail["recent_usage"] == []

    @pytest.mark.asyncio
    async def test_mcp_reload_tool_refuses(self, routes):
        from nerve.agent.tools.handlers.mcp_admin import mcp_reload_handler

        with patch("nerve.config.load_mcp_servers") as yaml_servers:
            result = await mcp_reload_handler(SimpleNamespace(engine=routes), {})
        yaml_servers.assert_not_called()
        assert result.is_error is True
        assert "mcp_reload is not available" in result.content[0]["text"]

    @pytest.mark.asyncio
    async def test_failed_row_writes_are_repaired_on_the_next_request(
        self, tmp_path, db, gateway, external,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        upsert = db.upsert_mcp_server
        failures = []

        async def flaky_upsert(name, *args, **kwargs):
            if name == "github" and not failures:
                failures.append(name)
                raise RuntimeError("database is locked")
            return await upsert(name, *args, **kwargs)

        with patch.object(db, "upsert_mcp_server", flaky_upsert):
            await engine._mcp_gateway.start()
            rows = {row["name"] for row in await db.get_mcp_server_stats()}
            assert "github" not in rows
            # The next catalog request, the same generation: the rows follow.
            await engine._mcp_gateway.refresh()
        await engine._mcp_gateway.close()
        rows = {row["name"] for row in await db.get_mcp_server_stats()}
        assert {"docs", "github", "nerve"} <= rows
        assert failures == ["github"]

    def test_mcp_reload_is_not_offered(self, tmp_path, db, external):
        from nerve.agent.backends.base import config_excluded_tools

        engine = AgentEngine(_config(tmp_path), db)
        assert "mcp_reload" in config_excluded_tools(engine.config)
        assert "mcp_reload" in engine._backends["claude"].excluded_tools()
        assert "mcp_reload" in engine._backends["codex"].excluded_tools()


class TestApiInLocalMode:
    @pytest.mark.asyncio
    async def test_routes_answer_as_before(self, tmp_path, db):
        from nerve.agent.backends.base import config_excluded_tools
        from nerve.agent.tools.handlers.mcp_admin import mcp_reload_handler
        from nerve.gateway.routes import _deps
        from nerve.gateway.routes.mcp_servers import (
            get_mcp_server_detail,
            get_mcp_server_usage,
            list_mcp_servers,
            reload_mcp_servers,
        )

        engine = AgentEngine(_config(tmp_path), db)
        await engine._sync_mcp_servers_to_db()
        previous = _deps._deps
        _deps.init_deps(engine, db)
        try:
            await db.record_mcp_tool_usage(server_name="local-files", tool_name="read")
            assert len((await get_mcp_server_usage("local-files"))["usage"]) == 1
            assert (await get_mcp_server_usage("never-seen"))["usage"] == []
            listed = await list_mcp_servers()
            assert set(listed) == {"servers"}
            assert all("managed_by" not in row for row in listed["servers"])
            assert "managed_by" not in await get_mcp_server_detail("local-files")
            with patch("nerve.config.load_mcp_servers", return_value=[]) as yaml_servers, \
                 patch("nerve.config.load_claude_code_plugins", return_value=[]):
                reloaded = await reload_mcp_servers()
                result = await mcp_reload_handler(SimpleNamespace(engine=engine), {})
            assert yaml_servers.call_count == 2
            assert reloaded["reloaded"] == 0
            assert result.is_error is False
        finally:
            _deps._deps = previous
        assert "mcp_reload" not in config_excluded_tools(engine.config)


class TestDiagnostics:
    @pytest_asyncio.fixture
    async def diagnostics_of(self, db):
        import nerve.config as cfg_mod
        from nerve.gateway.routes import _deps
        from nerve.gateway.routes.diagnostics import diagnostics

        previous = (_deps._deps, cfg_mod._config)

        async def run(engine):
            _deps.init_deps(engine, db)
            cfg_mod._config = engine.config
            return await diagnostics()

        yield run
        _deps._deps, cfg_mod._config = previous

    @pytest.mark.asyncio
    async def test_reports_the_applied_generation(
        self, tmp_path, db, gateway, external, diagnostics_of,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        try:
            report = await diagnostics_of(engine)
            gateway.catalog = catalog_payload(8, {"docs": ["search"]})
            await engine._mcp_gateway.refresh()
            later = await diagnostics_of(engine)
        finally:
            await engine._mcp_gateway.close()
        block = report["mcp_gateway"]
        assert block["url"] == gateway.url
        assert block["generation"] == 7
        assert block["digest"] == catalog_payload(7, {})["digest"]
        assert block["servers"] == ["docs", "github"]
        assert block["error"] is None
        assert later["mcp_gateway"]["generation"] == 8
        assert later["mcp_gateway"]["servers"] == ["docs"]

    @pytest.mark.asyncio
    async def test_reports_an_unreachable_gateway(
        self, tmp_path, db, gateway, external, diagnostics_of,
    ):
        gateway.stop()
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        try:
            block = (await diagnostics_of(engine))["mcp_gateway"]
        finally:
            await engine._mcp_gateway.close()
        assert block["generation"] is None
        assert block["retrying"] is True
        assert "cannot reach the MCP gateway" in block["error"]

    @pytest.mark.asyncio
    async def test_local_mode_has_no_gateway_block(self, tmp_path, db, diagnostics_of):
        engine = AgentEngine(_config(tmp_path), db)
        assert "mcp_gateway" not in await diagnostics_of(engine)


class TestDoctor:
    def _report(self, tmp_path, **extra):
        from nerve.cli import doctor_report

        return doctor_report(_config(tmp_path, **extra))

    def test_external_mode_names_the_gateway(self, tmp_path, external):
        report = self._report(tmp_path, mcp_gateway_url="http://192.0.2.1:8080")
        assert "[OK] MCP gateway: http://192.0.2.1:8080" in report
        assert "mcp_servers: 2 server(s) in the configuration, not used" in report

    def test_external_mode_without_a_gateway_warns(self, tmp_path, external):
        report = self._report(tmp_path)
        assert "[WARN] MCP gateway: mcp_gateway_url is not set" in report

    def test_external_mode_warns_about_ultracode(self, tmp_path, external):
        report = self._report(tmp_path, codex={
            "home_dir": str(tmp_path / "codex-home"), "ultracode": {"enabled": True},
        })
        assert "[WARN] codex.ultracode has no effect in external mode" in report

    def test_local_mode_warns_about_an_unused_gateway(self, tmp_path):
        report = self._report(tmp_path, mcp_gateway_url="http://192.0.2.1:8080")
        assert "[WARN] mcp_gateway_url has no effect in local mode" in report
        assert "[OK] MCP gateway" not in report

    def test_local_mode_without_a_gateway_says_nothing(self, tmp_path):
        assert "MCP gateway" not in self._report(tmp_path)
