"""MCP servers from the MCP gateway in external mode.

In external mode Nerve takes its MCP servers only from the gateway's catalog
and writes the client configuration for Claude and Codex from it. Local mode
does not change. The gateway is a fake HTTP server on loopback.
"""

from __future__ import annotations

import asyncio
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
from nerve.agent.backends import events as ev
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


def _record_codex_launches(monkeypatch, backend) -> list[dict]:
    """Record what each Codex client would launch with, without launching it.

    The real ``CodexClient`` builds the configuration; only its app-server
    transport and the version check are replaced.
    """
    import nerve.agent.backends.codex.backend as codex_module

    launches: list[dict] = []

    class Transport:
        def __init__(self, **kwargs):
            launches.append(kwargs)

    async def no_connect(self):
        pass

    async def version_ok():
        return "codex-cli 0.155.1"

    monkeypatch.setattr(codex_module, "CodexAppServerClient", Transport)
    monkeypatch.setattr(codex_module.CodexClient, "connect", no_connect)
    monkeypatch.setattr(backend, "_check_cli_version", version_ok)
    return launches


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


class TestClaudeSnapshot:
    def test_options_come_from_one_catalog_read(self, tmp_path, db, external, monkeypatch):
        """Servers and permissions of one Claude client match one catalog."""
        from nerve.mcp_gateway import GatewayServer

        engine = AgentEngine(_config(tmp_path), db)
        claude = engine._backends["claude"]
        snapshots = iter([
            (GatewayServer(id="docs", url="http://192.0.2.1:8080/s/docs/mcp", tools=("search",)),),
            (GatewayServer(id="jira", url="http://192.0.2.1:8080/s/jira/mcp", tools=("create",)),),
        ])
        reads = []

        def catalog():
            reads.append(1)
            return next(snapshots)

        monkeypatch.setattr(claude._deps, "managed_mcp_servers", catalog)
        options = _claude_options(engine, _spec(engine.config))
        assert len(reads) == 1
        assert set(options.mcp_servers) == {"nerve", "docs"}
        assert options.allowed_tools == ["mcp__docs"]

    @pytest.mark.asyncio
    async def test_the_client_keeps_the_snapshot_of_its_options(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """A client names the catalog generation that its options come from."""
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        _claude_without_cli(monkeypatch, engine)
        await engine._mcp_gateway.start()
        try:
            claude = engine._backends["claude"]
            with patch.object(claude, "_build_hooks", return_value={}):
                client = await claude.create_client(_spec(engine.config))
        finally:
            await engine._mcp_gateway.close()
        snapshot = client.managed_mcp_servers
        assert (snapshot.generation, snapshot.digest) == (7, catalog_payload(7, {})["digest"])
        assert [server.id for server in snapshot] == ["docs", "github"]
        assert set(client._options.mcp_servers) == {"nerve", "docs", "github"}


FIXTURES = Path(__file__).parent / "fixtures" / "mcp_gateway"


class TestServersWithoutObservedTools:
    """Every desired server is configured, whatever the gateway observed.

    The clients list the tools through MCP when they connect, and the gateway
    checks every call, so an unknown or empty inventory, the tool mode
    ``all`` (no tool names), an unknown tool mode or a tool change behavior
    does not hold a server back. Each client allows the whole server, never a
    list of tool names. The gateway serves the contract's fixtures byte for
    byte.
    """

    @pytest.mark.parametrize("fixture, servers", [
        ("catalog-all-tools.json", ["docs", "wiki"]),
        ("catalog-review-changes.json", ["docs", "github"]),
        ("catalog-unknown-inventory.json", ["docs", "github"]),
        ("catalog-empty-inventory.json", ["docs", "github"]),
        ("catalog-no-tools-yet.json", ["docs", "tracker"]),
        ("catalog-stale-after-failure.json", ["docs", "github"]),
    ])
    @pytest.mark.asyncio
    async def test_claude_and_codex_get_every_desired_server(
        self, tmp_path, db, gateway, external, fixture, servers,
    ):
        gateway.catalog_body = (FIXTURES / fixture).read_bytes()
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        try:
            assert engine.mcp_gateway_status()["error"] is None
            options = _claude_options(engine, _spec(engine.config))
            overrides = engine._backends["codex"].build_config_overrides(
                _spec(engine.config),
            )
        finally:
            await engine._mcp_gateway.close()
        assert set(options.mcp_servers) == {"nerve", *servers}
        assert options.allowed_tools == [f"mcp__{server}" for server in servers]
        for server in servers:
            assert options.mcp_servers[server] == {
                "type": "http", "url": f"{gateway.url}/s/{server}/mcp",
            }
            assert f'mcp_servers.{server}.url="{gateway.url}/s/{server}/mcp"' in overrides
        assert not any("enabled_tools" in o or "disabled_tools" in o for o in overrides)

    @pytest.mark.asyncio
    async def test_a_server_with_an_unknown_tool_mode_is_configured(
        self, tmp_path, db, gateway, external,
    ):
        vectors = json.loads((FIXTURES / "catalog-vectors.json").read_bytes())
        vector = next(
            v for v in vectors["accept"] if v["name"] == "unknown tool mode without tools"
        )
        gateway.catalog_body = json.dumps(vector["catalog"]).encode()
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        try:
            options = _claude_options(engine, _spec(engine.config))
        finally:
            await engine._mcp_gateway.close()
        ids = [server["id"] for server in vector["catalog"]["servers"]]
        assert ids and set(options.mcp_servers) == {"nerve", *ids}


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

        monkeypatch.setattr(backend, "_write_codex_config", write)
        launches = _record_codex_launches(monkeypatch, backend)
        await engine._mcp_gateway.start()
        try:
            await backend.create_client(_spec(cfg))
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
        overrides = launches[0]["config_overrides"]
        assert f'mcp_servers.docs.url="{gateway.url}/s/docs/mcp"' in overrides
        assert "mcp_servers.personal.enabled=false" in overrides

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

        monkeypatch.setattr(backend, "_write_codex_config", write)
        launches = _record_codex_launches(monkeypatch, backend)
        await backend.create_client(_spec(engine.config))
        assert calls == []
        assert len(launches) == 1
        assert not list(home.glob("config.toml.nerve-mcp-backup-*"))

    @pytest.mark.asyncio
    async def test_a_system_file_collision_is_left_out_and_reported(
        self, tmp_path, db, gateway, external, monkeypatch, caplog,
    ):
        """Nerve does not change /etc/codex/config.toml, and Codex cannot merge
        a stdio server with a gateway server of the same name.

        The session starts without that gateway server; diagnostics and the
        MCP server API name it, and the error is logged once per catalog
        change.
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
            # Three sessions of one catalog: one error.
            built = [codex.build_config_overrides(_spec(cfg, session_id=f"s{i}"))
                     for i in range(3)]
            await engine._mcp_gateway.refresh()
            errors = [r for r in caplog.records if "not applied to codex" in r.getMessage()]
            assert len(errors) == 1
            first = gateway.catalog["desiredRevision"]
            assert f"'docs' (desired revision {first})" in errors[0].getMessage()
            assert reason in errors[0].getMessage()

            status = engine.mcp_gateway_status()
            assert status["not_applied"] == {"docs": {"codex": reason}}
            _deps.init_deps(engine, db)
            rows = {row["name"]: row for row in (await list_mcp_servers())["servers"]}
            assert rows["docs"]["not_applied"] == {"codex": reason}
            assert rows["github"]["not_applied"] == {}
            assert (await get_mcp_server_detail("docs"))["not_applied"] == {"codex": reason}

            # Another tenant generation with the same desired servers does
            # not log it again; a new desired revision does.
            gateway.catalog = catalog_payload(
                8, {"docs": ["fetch_page", "search"], "github": ["get_issue"]},
            )
            await engine._mcp_gateway.refresh()
            gateway.catalog = catalog_payload(8, {"docs": ["search"]})
            await engine._mcp_gateway.refresh()
            errors = [r for r in caplog.records if "not applied to codex" in r.getMessage()]
            assert len(errors) == 2
            second = gateway.catalog["desiredRevision"]
            assert f"(desired revision {second})" in errors[1].getMessage()

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

    @pytest.mark.asyncio
    async def test_backups_never_replace_each_other(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """Two removals in the same second keep two copies of the file."""
        import nerve.agent.backends.codex.backend as codex_module

        home = tmp_path / "codex-home"
        home.mkdir()
        first = (
            '[mcp_servers.docs]\ncommand = "/usr/bin/true"\n'
            '[mcp_servers.jira]\ncommand = "/usr/bin/true"\n'
        )
        (home / "config.toml").write_text(first, encoding="utf-8")
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        backend = engine._backends["codex"]

        async def write(path, edits):
            # Codex's writer: drop the named tables.
            names = {edit["keyPath"].split(".", 1)[1] for edit in edits}
            kept = [
                block for block in path.read_text().split("[mcp_servers.")[1:]
                if block.split("]", 1)[0] not in names
            ]
            path.write_text("".join("[mcp_servers." + block for block in kept))

        monkeypatch.setattr(backend, "_write_codex_config", write)
        monkeypatch.setattr(codex_module.time, "strftime", lambda *a: "20261008T120000Z")
        await engine._mcp_gateway.start()
        try:
            await backend._remove_colliding_user_mcp_servers(engine.managed_mcp_servers())
            second = (home / "config.toml").read_text()
            gateway.catalog = catalog_payload(8, {"jira": ["create"]})
            await engine._mcp_gateway.refresh()
            await backend._remove_colliding_user_mcp_servers(engine.managed_mcp_servers())
        finally:
            await engine._mcp_gateway.close()

        backups = sorted(home.glob("config.toml.nerve-mcp-backup-20261008T120000Z-*"))
        assert len(backups) == 2
        assert sorted(b.read_text() for b in backups) == sorted([first, second])
        assert "[mcp_servers.jira]" in second
        assert (home / "config.toml").read_text() == ""

    def test_a_backup_does_not_replace_an_existing_file(self, tmp_path, monkeypatch):
        import uuid

        import nerve.agent.backends.codex.backend as codex_module
        from nerve.agent.backends.codex import CodexBackend

        config = tmp_path / "config.toml"
        config.write_text("[mcp_servers.docs]\n", encoding="utf-8")
        monkeypatch.setattr(codex_module.time, "strftime", lambda *a: "20261008T120000Z")
        same = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
        other = uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
        # The second copy draws the first name again, then a new one.
        values = iter([same, same, other])
        monkeypatch.setattr(codex_module.uuid, "uuid4", lambda: next(values))
        taken = tmp_path / "config.toml.nerve-mcp-backup-20261008T120000Z-aaaaaaaaaaaa"

        assert CodexBackend._write_backup(config) == taken
        assert taken.read_text() == "[mcp_servers.docs]\n"
        assert taken.stat().st_mode & 0o777 == 0o600
        config.write_text("[mcp_servers.jira]\n", encoding="utf-8")
        second = CodexBackend._write_backup(config)
        assert second.name == "config.toml.nerve-mcp-backup-20261008T120000Z-bbbbbbbbbbbb"
        assert taken.read_text() == "[mcp_servers.docs]\n"
        assert second.read_text() == "[mcp_servers.jira]\n"

    @pytest.mark.asyncio
    async def test_a_catalog_change_during_cleanup_does_not_reach_the_launch(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """The launch uses the catalog snapshot that the cleanup used.

        The config writer is held while the catalog changes from generation 7
        (docs, github) to generation 8 (docs, jira). The user file has a stdio
        ``jira`` too. The session must launch with generation 7 only: no
        ``jira`` URL over the file's stdio entry, and that entry turned off.
        """
        import asyncio

        home = tmp_path / "codex-home"
        home.mkdir()
        (home / "config.toml").write_text(
            '[mcp_servers.docs]\ncommand = "/usr/bin/true"\n'
            '[mcp_servers.jira]\ncommand = "/usr/bin/true"\n',
            encoding="utf-8",
        )
        cfg = _config(tmp_path, mcp_gateway_url=gateway.url)
        engine = AgentEngine(cfg, db)
        backend = engine._backends["codex"]
        writing = asyncio.Event()
        release = asyncio.Event()
        written = []

        async def held_write(path, edits):
            written.append([edit["keyPath"] for edit in edits])
            writing.set()
            await release.wait()
            names = {edit["keyPath"].split(".", 1)[1] for edit in edits}
            path.write_text("".join(
                f'[mcp_servers.{name}]\ncommand = "/usr/bin/true"\n'
                for name in ("docs", "jira") if name not in names
            ), encoding="utf-8")

        monkeypatch.setattr(backend, "_write_codex_config", held_write)
        launches = _record_codex_launches(monkeypatch, backend)
        await engine._mcp_gateway.start()
        try:
            build = asyncio.ensure_future(backend.create_client(_spec(cfg)))
            await asyncio.wait_for(writing.wait(), 5)
            gateway.catalog = catalog_payload(8, {"docs": ["search"], "jira": ["create"]})
            await engine._mcp_gateway.refresh()
            assert engine.mcp_gateway_status()["generation"] == 8
            release.set()
            await asyncio.wait_for(build, 5)
        finally:
            await engine._mcp_gateway.close()

        assert written == [["mcp_servers.docs"]]
        overrides = launches[0]["config_overrides"]
        assert f'mcp_servers.docs.url="{gateway.url}/s/docs/mcp"' in overrides
        assert f'mcp_servers.github.url="{gateway.url}/s/github/mcp"' in overrides
        assert not any(o.startswith("mcp_servers.jira.url=") for o in overrides)
        assert "mcp_servers.jira.enabled=false" in overrides

    def test_one_build_reads_the_catalog_once(
        self, tmp_path, db, external, monkeypatch,
    ):
        """Overrides and environment of one launch come from one catalog read."""
        from nerve.agent.backends.codex.backend import CodexClient
        from nerve.mcp_gateway import GatewayServer

        engine = AgentEngine(_config(tmp_path), db)
        backend = engine._backends["codex"]
        snapshots = iter([
            (GatewayServer(id="docs", url="http://192.0.2.1:8080/s/docs/mcp", tools=("search",)),),
            (GatewayServer(id="jira", url="http://192.0.2.1:8080/s/jira/mcp", tools=("create",)),),
        ])
        reads = []

        def catalog():
            reads.append(1)
            return next(snapshots)

        monkeypatch.setattr(backend._deps, "managed_mcp_servers", catalog)
        launches = _record_codex_launches(monkeypatch, backend)
        CodexClient(backend, _spec(engine.config))
        assert len(reads) == 1
        overrides = launches[0]["config_overrides"]
        assert any(o.startswith("mcp_servers.docs.url=") for o in overrides)
        assert not any(o.startswith("mcp_servers.jira.") for o in overrides)

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


def _claude_without_cli(monkeypatch, engine: AgentEngine, fail: dict | None = None) -> list[tuple]:
    """Real Claude clients that start no CLI; their MCP server calls are recorded.

    ``fail`` maps a server name to the number of calls that raise first.
    """
    from claude_agent_sdk import ClaudeSDKClient

    from nerve.agent.backends import claude as claude_module

    calls: list[tuple] = []
    failures = dict(fail or {})

    async def connect(self, prompt=None):
        pass

    def maybe_fail(name):
        if failures.get(name):
            failures[name] -= 1
            raise RuntimeError("MCP server change refused")

    async def toggle(self, name, enabled):
        calls.append(("toggle", name, enabled))
        maybe_fail(name)

    async def reconnect(self, name):
        calls.append(("reconnect", name))
        maybe_fail(name)

    monkeypatch.setattr(ClaudeSDKClient, "connect", connect)
    monkeypatch.setattr(ClaudeSDKClient, "toggle_mcp_server", toggle)
    monkeypatch.setattr(ClaudeSDKClient, "reconnect_mcp_server", reconnect)
    monkeypatch.setattr(claude_module.ClaudeClient, "is_alive", lambda self: True)
    # The idle watcher reads the CLI's stream, which these clients do not have.
    monkeypatch.setattr(engine, "_start_idle_watcher", lambda *args: None)
    return calls


def _sessions_of(engine: AgentEngine) -> dict[str, dict]:
    return {
        entry["session_id"]: entry
        for entry in engine.mcp_gateway_status()["sessions"]
    }


class _ScriptedClient:
    """An agent client with a scripted idle stream and recorded MCP changes.

    ``idle`` holds event batches that are buffered between turns; ``later``
    holds batches that arrive while the drain waits. Every step goes to the
    shared ``log`` in order. ``hang`` maps a server name to the seconds that
    a change of it takes; ``refuse`` names servers whose change fails.
    ``blockers`` is what ``replacement_blockers`` reports (live work of the
    runtime, as a Codex app-server reports it). ``turn_events`` replaces the
    events of the next turns; ``turn_gate``, when set, holds a turn open
    until it is set.
    """

    def __init__(
        self, number, managed, log, *, native_id, mcp_changes=True, hang=None,
        refuse=(), model="claude-sonnet-4-5",
    ):
        self.number = number
        self.managed_mcp_servers = managed
        self.log = log
        self.model = model
        self.native_session_id = native_id
        self.idle: list[list] = []
        self.later: asyncio.Queue = asyncio.Queue()
        self.arriving = False
        self.hang = dict(hang or {})
        self.refuse = set(refuse)
        self.blockers: tuple[str, ...] = ()
        self.stopped = False
        self.turn_events: list | None = None
        self.turn_gate: asyncio.Event | None = None
        if mcp_changes:
            self.set_mcp_server_enabled = self._set_enabled
            self.reconnect_mcp_server = self._reconnect

    async def connect(self):
        pass

    def is_alive(self):
        return not self.stopped

    async def disconnect(self):
        self.stopped = True
        self.log.append(("disconnect", self.number))

    def runtime_stopped(self):
        return self.stopped

    async def interrupt(self):
        self.log.append(("interrupt", self.number))

    async def replacement_blockers(self):
        return self.blockers

    def buffer_used(self):
        return len(self.idle) + (1 if self.arriving else 0)

    def try_receive_idle_events(self):
        if not self.idle:
            return None
        batch = self.idle.pop(0)
        self.log.append(("drained", self.number, [type(e).__name__ for e in batch]))
        return batch

    async def receive_idle_events(self, timeout):
        batch = await asyncio.wait_for(self.later.get(), timeout)
        self.log.append(("drained", self.number, [type(e).__name__ for e in batch]))
        return batch

    async def start_turn(self, turn):
        # The user's text, without the session preamble and the time note.
        text = turn.text.rsplit("</session-context>", 1)[-1].strip()
        self.log.append(("start_turn", self.number, text.split("\n")[0]))

    async def receive_turn(self):
        if self.turn_gate is not None:
            await self.turn_gate.wait()
        for event in self.turn_events or [ev.TextDelta(text="done")]:
            yield event
        yield ev.TurnCompleted(native_session_id=self.native_session_id)

    async def _change(self, *entry):
        name = entry[1]
        self.log.append(("begin", self.number, *entry))
        await asyncio.sleep(self.hang.get(name, 0))
        if name in self.refuse:
            self.log.append(("refused", self.number, *entry))
            raise RuntimeError("MCP server change refused")
        self.log.append(("end", self.number, *entry))

    async def _set_enabled(self, name, enabled):
        await self._change("toggle", name, enabled)

    async def _reconnect(self, name):
        await self._change("reconnect", name)


class _Scripted:
    """Makes a backend build scripted clients, with the catalog snapshot
    that the real backend reads.

    A client built with a resume target continues that native conversation,
    as the real backends do; a new conversation gets ``native-<number>``.
    ``resume_failures`` makes the next builds that must resume fail.
    ``transcripts`` is None (every resume target exists) or the set of
    native IDs whose conversation exists on this host.
    """

    def __init__(self, engine, monkeypatch, backend="claude", **client_options):
        from nerve.agent.backends.base import ResumeRequiredError

        self.engine = engine
        self.log: list[tuple] = []
        self.clients: list[_ScriptedClient] = []
        self.specs: list[SessionSpec] = []
        self.client_options = dict(client_options)
        self.resume_failures = 0
        self.transcripts: set[str] | None = None
        if backend == "codex":
            self.client_options["mcp_changes"] = False
            self.client_options["model"] = engine.config.codex.model

        async def create_client(spec):
            number = len(self.clients) + 1
            self.specs.append(spec)
            if spec.resume_required and self.resume_failures:
                self.resume_failures -= 1
                self.log.append(("resume_failed", number))
                raise ResumeRequiredError("the scripted runtime did not resume")
            client = _ScriptedClient(
                number, engine.managed_mcp_servers(), self.log,
                native_id=spec.resume_native_id or f"native-{number}",
                **self.client_options,
            )
            self.clients.append(client)
            self.log.append(("built", number, spec.resume_native_id, spec.resume_required))
            return client

        def validate_resume_target(native_id, cwd):
            return self.transcripts is None or native_id in self.transcripts

        monkeypatch.setattr(engine._backends[backend], "create_client", create_client)
        monkeypatch.setattr(
            engine._backends[backend], "validate_resume_target", validate_resume_target,
        )
        # The idle watcher would drain the buffered batches before a turn;
        # these tests leave them to the turn's own drain.
        monkeypatch.setattr(engine, "_start_idle_watcher", lambda *args: None)

    @property
    def client(self) -> _ScriptedClient:
        return self.clients[-1]

    def changes(self) -> list[tuple]:
        return [entry[2:] for entry in self.log if entry[0] == "end"]

    def begun(self) -> list[tuple]:
        return [entry[2:] for entry in self.log if entry[0] == "begin"]

    def builds(self) -> list[tuple]:
        return [entry[1:] for entry in self.log if entry[0] == "built"]

    def turns(self, number: int | None = None) -> list[tuple]:
        return [
            entry for entry in self.log
            if entry[0] == "start_turn" and number in (None, entry[1])
        ]

    def index(self, entry: tuple) -> int:
        return self.log.index(entry)


async def _turn(
    engine: AgentEngine, db, session_id: str = "s-1", text: str = "hello",
) -> str:
    return await engine.run(session_id, text, source="web", actor=db.system_actor)


async def _set_catalog(
    engine: AgentEngine, gateway, generation: int, servers: dict, **options,
) -> None:
    gateway.catalog = catalog_payload(generation, servers, **options)
    await engine._mcp_gateway.refresh()


def _revision(gateway) -> str:
    return gateway.catalog["desiredRevision"]


async def _native_id(db, session_id: str = "s-1") -> str | None:
    return (await db.get_session(session_id) or {}).get("sdk_session_id")


async def _user_messages(db, session_id: str = "s-1") -> list[str]:
    return [m["content"] for m in await db.get_messages(session_id) if m["role"] == "user"]


class TestRunningSessions:
    """A newer catalog reaches a running session before its next user turn."""

    @pytest.fixture
    def budget(self, monkeypatch):
        import nerve.agent.managed_mcp as managed_mcp

        monkeypatch.setattr(managed_mcp, "UPDATE_BUDGET_SECONDS", 0.3)
        return 0.3

    @pytest.mark.asyncio
    async def test_a_claude_session_changes_its_servers_in_place(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db, text="first")
            built = _revision(gateway)
            assert _sessions_of(engine)["s-1"] == {
                "session_id": "s-1", "backend": "claude",
                "built_revision": built, "applied_revision": built,
                "inventory": {
                    "docs": gateway.catalog["servers"][0]["inventory"]["revision"],
                    "github": gateway.catalog["servers"][1]["inventory"]["revision"],
                },
                "last_update": None, "pending": None,
            }
            # docs loses a tool, github goes away.
            await _set_catalog(engine, gateway, 8, {"docs": ["search"]})
            assert scripted.changes() == []  # nothing happens before the next turn
            waiting = _sessions_of(engine)["s-1"]["pending"]
            await _turn(engine, db, text="second")
            entry = _sessions_of(engine)["s-1"]
        finally:
            await engine._mcp_gateway.close()
        assert waiting == {
            "reason": "waiting_for_turn", "target_revision": _revision(gateway),
            "servers": ["docs", "github"], "blocked_by": [], "error": None,
        }
        assert len(scripted.clients) == 1
        assert scripted.changes() == [("toggle", "github", False), ("reconnect", "docs")]
        first, second = scripted.turns(1)
        assert (
            scripted.index(first)
            < scripted.index(("begin", 1, "toggle", "github", False))
            < scripted.index(("end", 1, "reconnect", "docs"))
            < scripted.index(second)
        )
        assert entry["built_revision"] == built
        assert entry["applied_revision"] == _revision(gateway)
        assert entry["pending"] is None
        assert entry["last_update"]["result"] == "applied"
        assert entry["last_update"]["method"] == "in_place"

    @pytest.mark.asyncio
    async def test_a_returning_server_is_turned_on_again(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            await _set_catalog(engine, gateway, 8, {"docs": ["fetch_page", "search"]})
            await _turn(engine, db)
            await _set_catalog(
                engine, gateway, 9, {"docs": ["fetch_page", "search"], "github": ["get_issue"]},
            )
            await _turn(engine, db)
            entry = _sessions_of(engine)["s-1"]
        finally:
            await engine._mcp_gateway.close()
        assert len(scripted.clients) == 1
        assert scripted.changes() == [("toggle", "github", False), ("toggle", "github", True)]
        assert entry["applied_revision"] == _revision(gateway)
        assert entry["pending"] is None

    @pytest.mark.asyncio
    async def test_an_added_server_replaces_the_claude_client_on_the_same_conversation(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db, text="first")
            await _set_catalog(
                engine, gateway, 8,
                {"docs": ["fetch_page", "search"], "github": ["get_issue"], "jira": ["create"]},
            )
            await _turn(engine, db, text="second")
            entry = _sessions_of(engine)["s-1"]
            native = await _native_id(db)
            users = await _user_messages(db)
        finally:
            await engine._mcp_gateway.close()
        assert scripted.builds() == [(1, None, False), (2, "native-1", True)]
        assert scripted.client.native_session_id == "native-1" == native
        # The new client names the session's cause on its model requests.
        assert [spec.inference_principal for spec in scripted.specs] == [
            db.system_actor.actor_id, db.system_actor.actor_id,
        ]
        log = scripted.log
        assert log.index(("disconnect", 1)) < log.index(("built", 2, "native-1", True))
        assert scripted.turns() == [("start_turn", 1, "first"), ("start_turn", 2, "second")]
        assert users == ["first", "second"]
        assert [s.id for s in scripted.client.managed_mcp_servers] == ["docs", "github", "jira"]
        assert scripted.changes() == []
        assert entry["built_revision"] == entry["applied_revision"] == _revision(gateway)
        assert entry["pending"] is None
        assert entry["last_update"]["result"] == "replaced"
        assert entry["last_update"]["method"] == "replacement"

    @pytest.mark.asyncio
    async def test_a_definition_change_at_unchanged_policy_lists_the_tools_again(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """Another served definition of docs (a schema or a description) has
        another inventory revision; the desired revision is the same."""
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        servers = {"docs": ["fetch_page", "search"], "github": ["get_issue"]}
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            before = _revision(gateway)
            await _set_catalog(
                engine, gateway, 7, servers,
                inventory={"docs": ["fetch_page", "search", "search-v2"]},
            )
            assert _revision(gateway) == before
            await _turn(engine, db)
            entry = _sessions_of(engine)["s-1"]
        finally:
            await engine._mcp_gateway.close()
        assert len(scripted.clients) == 1
        assert scripted.changes() == [("reconnect", "docs")]
        assert entry["inventory"]["docs"] == gateway.catalog["servers"][0]["inventory"]["revision"]
        assert entry["pending"] is None

    @pytest.mark.asyncio
    async def test_a_client_built_with_an_unknown_inventory_adopts_the_first_revision(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """The client listed the tools when it connected, so the first
        observed revision needs no new list; a later change does."""
        servers = {"docs": ["fetch_page", "search"], "github": ["get_issue"]}
        gateway.catalog = catalog_payload(7, servers, inventory={"docs": None})
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            assert "docs" not in _sessions_of(engine)["s-1"]["inventory"]
            await _set_catalog(engine, gateway, 7, servers)
            assert _sessions_of(engine)["s-1"]["pending"] is None
            await _turn(engine, db)
            adopted = _sessions_of(engine)["s-1"]
            await _set_catalog(engine, gateway, 7, servers, inventory={"docs": ["search"]})
            await _turn(engine, db)
        finally:
            await engine._mcp_gateway.close()
        assert adopted["inventory"]["docs"] == catalog_payload(7, servers)["servers"][0]["inventory"]["revision"]
        assert scripted.changes() == [("reconnect", "docs")]
        assert len(scripted.clients) == 1

    @pytest.mark.asyncio
    async def test_a_change_during_a_foreground_turn_waits_for_the_next_turn(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            scripted.client.turn_gate = gate = asyncio.Event()
            turn = asyncio.ensure_future(_turn(engine, db, text="long"))
            for _ in range(100):
                if scripted.turns(1)[-1][2] == "long":
                    break
                await asyncio.sleep(0.01)
            await _set_catalog(engine, gateway, 8, {"docs": ["search"], "jira": ["create"]})
            await asyncio.sleep(0.05)
            during = (list(scripted.log), _sessions_of(engine)["s-1"]["pending"])
            gate.set()
            await turn
            scripted.client.turn_gate = None
            await _turn(engine, db, text="next")
        finally:
            await engine._mcp_gateway.close()
        log, pending = during
        assert ("disconnect", 1) not in log and ("interrupt", 1) not in log
        assert [e for e in log if e[0] in ("begin", "built")] == [("built", 1, None, False)]
        assert pending["reason"] == "waiting_for_turn"
        assert scripted.builds()[-1] == (2, "native-1", True)
        assert scripted.turns(2) == [("start_turn", 2, "next")]

    @pytest.mark.asyncio
    async def test_a_background_task_defers_the_change_without_stopping_anything(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            engine._bg_task_registry["s-1"] = {"task-1": {"status": "running"}}
            await _set_catalog(engine, gateway, 8, {"docs": ["search"], "jira": ["create"]})
            await _turn(engine, db)
            deferred = _sessions_of(engine)["s-1"]
            deferred_log = list(scripted.log)
            engine._bg_task_registry["s-1"]["task-1"]["status"] = "done"
            await _turn(engine, db)
        finally:
            await engine._mcp_gateway.close()
        assert not [e for e in deferred_log if e[0] in ("begin", "disconnect", "interrupt")]
        assert len([e for e in deferred_log if e[0] == "built"]) == 1
        assert deferred["applied_revision"] == deferred["built_revision"]
        assert deferred["pending"] == {
            "reason": "waiting_for_safe_boundary", "target_revision": _revision(gateway),
            "servers": ["docs", "github", "jira"], "blocked_by": ["background_task"],
            "error": None,
        }
        assert scripted.builds()[-1] == (2, "native-1", True)
        assert _sessions_of(engine)["s-1"]["pending"] is None

    @pytest.mark.asyncio
    async def test_a_background_task_that_settles_during_a_change_defers_the_rest(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """A background task that settles makes the CLI start a turn of its
        own at any time, also after the drain found it quiet. While one
        runs, the client gets no change."""
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            client = scripted.client
            engine._bg_task_registry["s-1"] = {"task-1": {"status": "running"}}

            async def settles_during_the_change(name, enabled):
                client.idle.append([ev.SystemEvent(subtype="init")])
                await client._set_enabled(name, enabled)

            client.set_mcp_server_enabled = settles_during_the_change
            await _set_catalog(engine, gateway, 8, {"docs": ["fetch_page", "search"]})
            await _turn(engine, db)
            assert scripted.begun() == []
            behind = _sessions_of(engine)["s-1"]

            # The task ended and its output is drained: the next turn changes.
            engine._bg_task_registry["s-1"]["task-1"]["status"] = "completed"
            client.idle.clear()
            client.set_mcp_server_enabled = client._set_enabled
            await _turn(engine, db)
        finally:
            await engine._mcp_gateway.close()
        assert behind["pending"]["reason"] == "waiting_for_safe_boundary"
        assert behind["pending"]["servers"] == ["github"]
        assert scripted.changes() == [("toggle", "github", False)]
        assert _sessions_of(engine)["s-1"]["applied_revision"] == _revision(gateway)

    @pytest.mark.asyncio
    async def test_codex_child_work_defers_the_replacement(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """A running command or child agent of the app-server ends with its
        process, so the client is not replaced while one runs."""
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch, backend="codex")
        await engine._mcp_gateway.start()
        try:
            await db.create_session("c-1", source="web", backend="codex", actor=None)
            await _turn(engine, db, "c-1")
            scripted.client.blockers = ("child_agent", "command")
            await _set_catalog(engine, gateway, 8, {"docs": ["search"]})
            await _turn(engine, db, "c-1")
            deferred = _sessions_of(engine)["c-1"]["pending"]
            deferred_log = list(scripted.log)
            scripted.client.blockers = ()
            await _turn(engine, db, "c-1")
            native = await _native_id(db, "c-1")
        finally:
            await engine._mcp_gateway.close()
        assert not [e for e in deferred_log if e[0] in ("disconnect", "interrupt")]
        assert deferred["reason"] == "waiting_for_safe_boundary"
        assert deferred["blocked_by"] == ["child_agent", "command"]
        assert scripted.builds() == [(1, None, False), (2, "native-1", True)]
        assert native == "native-1"
        assert scripted.turns() == [
            ("start_turn", 1, "hello"), ("start_turn", 1, "hello"), ("start_turn", 2, "hello"),
        ]

    @pytest.mark.asyncio
    async def test_a_codex_session_is_replaced_on_the_same_thread(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """Codex has no in-place change: every catalog change that the client
        does not have (a removal, a new definition, an added server) builds
        a new app-server that resumes the same thread."""
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch, backend="codex")
        await engine._mcp_gateway.start()
        try:
            await db.create_session("c-1", source="web", backend="codex", actor=None)
            await _turn(engine, db, "c-1")
            await _set_catalog(engine, gateway, 8, {"docs": ["search"], "jira": ["create"]})
            await _turn(engine, db, "c-1")
            # Another tenant generation with the same servers: no change.
            await _set_catalog(engine, gateway, 9, {"docs": ["search"], "jira": ["create"]})
            await _turn(engine, db, "c-1")
            entry = _sessions_of(engine)["c-1"]
        finally:
            await engine._mcp_gateway.close()
        assert scripted.builds() == [(1, None, False), (2, "native-1", True)]
        assert all(spec.cwd == scripted.specs[0].cwd for spec in scripted.specs)
        assert scripted.specs[1].model == scripted.specs[0].model
        assert scripted.specs[1].system_prompt == scripted.specs[0].system_prompt
        assert [s.id for s in scripted.client.managed_mcp_servers] == ["docs", "jira"]
        assert entry["applied_revision"] == _revision(gateway)
        assert entry["pending"] is None

    @pytest.mark.asyncio
    async def test_rapid_successive_edits_are_coalesced(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            engine._bg_task_registry["s-1"] = {"task-1": {"status": "running"}}
            await _set_catalog(engine, gateway, 8, {"docs": ["search"]})
            await _turn(engine, db)
            await _set_catalog(engine, gateway, 9, {"docs": ["search"], "jira": ["create"]})
            await _set_catalog(engine, gateway, 10, {"docs": ["fetch_page"], "linear": ["read"]})
            await _turn(engine, db)
            engine._bg_task_registry["s-1"]["task-1"]["status"] = "done"
            await _turn(engine, db)
            entry = _sessions_of(engine)["s-1"]
        finally:
            await engine._mcp_gateway.close()
        assert scripted.begun() == []
        assert scripted.builds() == [(1, None, False), (2, "native-1", True)]
        assert [s.id for s in scripted.client.managed_mcp_servers] == ["docs", "linear"]
        assert entry["applied_revision"] == _revision(gateway)

    @pytest.mark.asyncio
    async def test_a_failed_resume_check_keeps_the_running_client(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """The resume target is checked while the old client runs; when it is
        missing, the turn runs on the old client and the session shows an
        apply error."""
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db, text="first")
            scripted.transcripts = set()
            await _set_catalog(engine, gateway, 8, {"docs": ["search"], "jira": ["create"]})
            await _turn(engine, db, text="second")
            entry = _sessions_of(engine)["s-1"]
            native = await _native_id(db)
        finally:
            await engine._mcp_gateway.close()
        assert len(scripted.clients) == 1
        assert ("disconnect", 1) not in scripted.log
        assert scripted.turns() == [("start_turn", 1, "first"), ("start_turn", 1, "second")]
        assert native == "native-1"
        assert entry["applied_revision"] == entry["built_revision"]
        assert entry["pending"]["reason"] == "apply_error"
        assert "missing" in entry["pending"]["error"]
        assert entry["last_update"]["result"] == "failed"

    @pytest.mark.asyncio
    async def test_a_failed_replacement_keeps_the_conversation_and_sends_nothing(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """The old client is stopped, and the new one cannot resume: the turn
        ends with an error, the message reaches no model, the native ID
        stays, and the next turn resumes the same conversation."""
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db, text="first")
            scripted.resume_failures = 1
            await _set_catalog(engine, gateway, 8, {"docs": ["search"], "jira": ["create"]})
            reply = await _turn(engine, db, text="second")
            failed = _sessions_of(engine)["s-1"]
            native_after_failure = await _native_id(db)
            await _turn(engine, db, text="third")
            entry = _sessions_of(engine)["s-1"]
            native = await _native_id(db)
            users = await _user_messages(db)
        finally:
            await engine._mcp_gateway.close()
        assert "could not continue this conversation" in reply
        assert native_after_failure == native == "native-1"
        assert failed["pending"]["reason"] == "apply_error"
        assert failed["last_update"]["result"] == "failed"
        assert [e for e in scripted.log if e[0] in ("built", "resume_failed", "disconnect")] == [
            ("built", 1, None, False),
            ("disconnect", 1),
            ("resume_failed", 2),
            ("built", 2, "native-1", True),
        ]
        # "second" was stored once and sent to no model; "third" went once.
        assert scripted.turns() == [("start_turn", 1, "first"), ("start_turn", 2, "third")]
        assert users == ["first", "second", "third"]
        assert entry["pending"] is None
        assert entry["built_revision"] == _revision(gateway)
        assert entry["last_update"]["result"] == "replaced"

    @pytest.mark.asyncio
    async def test_a_change_without_a_result_replaces_the_client_before_the_turn(
        self, tmp_path, db, gateway, external, monkeypatch, budget,
    ):
        """A disable that outlives the budget may still land in the old CLI,
        so the turn runs on a new client that resumes the conversation."""
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch, hang={"github": 5.0})
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            await _set_catalog(engine, gateway, 8, {"docs": ["fetch_page", "search"]})
            await _turn(engine, db)
            entry = _sessions_of(engine)["s-1"]
        finally:
            await engine._mcp_gateway.close()
        log = scripted.log
        assert len(scripted.clients) == 2
        assert log.index(("begin", 1, "toggle", "github", False)) < log.index(("disconnect", 1))
        assert log.index(("disconnect", 1)) < log.index(("built", 2, "native-1", True))
        assert scripted.turns(2) == [("start_turn", 2, "hello")]
        assert ("end", 1, "toggle", "github", False) not in log
        assert [s.id for s in scripted.client.managed_mcp_servers] == ["docs"]
        assert entry["applied_revision"] == entry["built_revision"] == _revision(gateway)
        assert entry["pending"] is None

    @pytest.mark.asyncio
    async def test_hanging_changes_share_one_budget(
        self, tmp_path, db, gateway, external, monkeypatch, budget,
    ):
        servers = {name: ["read"] for name in ("alpha", "beta", "gamma", "delta")}
        gateway.catalog = catalog_payload(7, servers)
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch, hang={n: 5.0 for n in ("alpha", "beta", "gamma")})
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            await _set_catalog(engine, gateway, 8, {"delta": ["read"]})
            started = asyncio.get_running_loop().time()
            await _turn(engine, db)
            elapsed = asyncio.get_running_loop().time() - started
        finally:
            await engine._mcp_gateway.close()
        assert sorted(e[1] for e in scripted.begun()) == ["alpha", "beta", "gamma"]
        # The three disables ran at the same time, inside one budget.
        assert elapsed < budget * 3
        assert scripted.log[-1] == ("start_turn", 2, "hello")

    @pytest.mark.asyncio
    async def test_removed_servers_are_turned_off_first(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch, hang={"github": 0.05})
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            await _set_catalog(engine, gateway, 8, {"docs": ["search"]})
            await _turn(engine, db)
        finally:
            await engine._mcp_gateway.close()
        log = scripted.log
        assert log.index(("end", 1, "toggle", "github", False)) < log.index(("begin", 1, "reconnect", "docs"))

    @pytest.mark.asyncio
    async def test_a_refused_change_is_pending_and_tried_again(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch, refuse={"github"})
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            built = _revision(gateway)
            await _set_catalog(engine, gateway, 8, {"docs": ["search"]})
            await _turn(engine, db)
            refused = _sessions_of(engine)["s-1"]
            scripted.client.refuse.clear()
            await _turn(engine, db)
            entry = _sessions_of(engine)["s-1"]
        finally:
            await engine._mcp_gateway.close()
        # The turn goes on with the same client; the session is not current.
        assert len(scripted.clients) == 1
        assert refused["applied_revision"] == built
        assert refused["pending"]["reason"] == "apply_error"
        assert refused["pending"]["servers"] == ["github"]
        assert "github" in refused["pending"]["error"]
        assert entry["applied_revision"] == _revision(gateway)
        assert entry["pending"] is None
        assert [e for e in scripted.begun() if e[0] == "toggle"] == [
            ("toggle", "github", False), ("toggle", "github", False),
        ]

    @pytest.mark.asyncio
    async def test_the_periodic_read_reaches_a_running_session(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        engine._mcp_gateway._refresh_interval = 0.05  # the setting's minimum is 1 s
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            gateway.catalog = catalog_payload(8, {"docs": ["fetch_page", "search"]})
            for _ in range(100):
                if engine.mcp_gateway_status()["generation"] == 8:
                    break
                await asyncio.sleep(0.02)
            await _turn(engine, db)
        finally:
            await engine._mcp_gateway.close()
        assert scripted.changes() == [("toggle", "github", False)]
        assert _sessions_of(engine)["s-1"]["applied_revision"] == _revision(gateway)

    @pytest.mark.asyncio
    async def test_generation_and_health_changes_need_no_call(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        servers = {"docs": ["fetch_page", "search"], "github": ["get_issue"]}
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            # Another agent's change: a new generation, the same servers.
            await _set_catalog(engine, gateway, 8, servers)
            await _turn(engine, db)
            # A failed check of github: health only.
            payload = catalog_payload(8, servers)
            payload["servers"][1]["health"] = {
                "inventoryState": "stale", "checkedAt": "2026-10-17T12:05:00Z",
                "failure": "upstream_timeout",
            }
            gateway.catalog = payload
            await engine._mcp_gateway.refresh()
            await _turn(engine, db)
            entry = _sessions_of(engine)["s-1"]
        finally:
            await engine._mcp_gateway.close()
        assert scripted.begun() == []
        assert scripted.builds() == [(1, None, False)]
        assert entry["pending"] is None and entry["last_update"] is None

    @pytest.mark.asyncio
    async def test_an_autonomous_turn_in_progress_defers_the_change(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """The CLI started a turn of its own and gave no content within the
        drain's wait: it is not between turns, so this turn changes nothing."""
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            scripted.client.idle = [[ev.SystemEvent(subtype="init")]]
            await _set_catalog(engine, gateway, 8, {"docs": ["fetch_page", "search"]})
            await _turn(engine, db)
            assert scripted.changes() == []
            assert scripted.log[-1] == ("start_turn", 1, "hello")
            behind = _sessions_of(engine)["s-1"]
            # The next turn finds the client between turns.
            await _turn(engine, db)
        finally:
            await engine._mcp_gateway.close()
        assert behind["pending"]["reason"] == "waiting_for_safe_boundary"
        assert behind["pending"]["blocked_by"] == ["autonomous_turn"]
        assert behind["pending"]["servers"] == ["github"]
        assert scripted.changes() == [("toggle", "github", False)]
        assert _sessions_of(engine)["s-1"]["pending"] is None

    @pytest.mark.asyncio
    async def test_a_buffered_autonomous_turn_is_drained_before_any_change(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            # A background task settled: the CLI runs a whole turn on its own.
            # Its start is buffered; its content comes while the drain waits.
            scripted.client.idle = [[ev.SystemEvent(subtype="init")]]
            scripted.client.later.put_nowait([
                ev.TextDelta(text="background result"),
                ev.TurnCompleted(native_session_id="native-1"),
            ])
            await _set_catalog(engine, gateway, 8, {"docs": ["fetch_page", "search"]})
            await _turn(engine, db)
            messages = await db.get_messages("s-1")
        finally:
            await engine._mcp_gateway.close()
        drained = scripted.index(("drained", 1, ["TextDelta", "TurnCompleted"]))
        changed = scripted.index(("begin", 1, "toggle", "github", False))
        assert drained < changed < scripted.log.index(("start_turn", 1, "hello"), changed)
        assert any("background result" in (m.get("content") or "") for m in messages)
        assert _sessions_of(engine)["s-1"]["pending"] is None

    @pytest.mark.asyncio
    async def test_output_that_arrives_after_the_drain_defers_the_change(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            scripted.client.arriving = True  # buffered, not yet readable
            await _set_catalog(engine, gateway, 8, {"docs": ["fetch_page", "search"]})
            await _turn(engine, db)
        finally:
            await engine._mcp_gateway.close()
        assert scripted.changes() == []
        pending = _sessions_of(engine)["s-1"]["pending"]
        assert pending["servers"] == ["github"]
        assert pending["blocked_by"] == ["buffered_output"]

    @pytest.mark.asyncio
    async def test_a_gateway_session_reset_reaches_the_model_and_changes_nothing(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """A -32002 error of the gateway (the upstream session was reset) is
        a tool result like any other: no rebuild, no reload, no pending
        state, and no new attempt of the call."""
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        message = (
            "MCP error -32002: MCP server session was reset; state from earlier "
            "calls is gone; this call was not sent."
        )
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            requests = gateway.catalog_requests
            scripted.client.turn_events = [
                ev.ToolUse(tool_use_id="t1", name="mcp__docs__search", input={}),
                ev.ToolResult(tool_use_id="t1", content=message, is_error=True),
                ev.TextDelta(text="the session was reset"),
            ]
            await _turn(engine, db)
            scripted.client.turn_events = None
            await _turn(engine, db)
            entry = _sessions_of(engine)["s-1"]
            messages = await db.get_messages("s-1")
        finally:
            await engine._mcp_gateway.close()
        assert scripted.builds() == [(1, None, False)]
        assert scripted.begun() == []
        assert len(scripted.turns()) == 3
        assert gateway.catalog_requests == requests
        assert entry["pending"] is None and entry["last_update"] is None
        stored = json.dumps([m.get("blocks") for m in messages])
        assert json.dumps(message)[1:-1] in stored

    @pytest.mark.asyncio
    async def test_a_turn_that_called_a_tool_is_never_tried_again(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        """The engine retries a turn only before any content, so a retry can
        never send a tool call again."""
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        scripted = _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            await _turn(engine, db)
            client = scripted.client

            async def lost_after_a_tool_call():
                yield ev.ToolUse(tool_use_id="t1", name="mcp__docs__search", input={})
                raise RuntimeError("connection lost")

            client.receive_turn = lost_after_a_tool_call
            reply = await _turn(engine, db, text="again")
        finally:
            await engine._mcp_gateway.close()
        assert "connection lost" in reply
        assert scripted.turns() == [("start_turn", 1, "hello"), ("start_turn", 1, "again")]
        assert scripted.builds() == [(1, None, False)]

    @pytest.mark.asyncio
    async def test_diagnostics_list_running_clients_only(
        self, tmp_path, db, gateway, external, monkeypatch,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        _Scripted(engine, monkeypatch)
        await engine._mcp_gateway.start()
        try:
            for sid in ("s-1", "s-2"):
                await _turn(engine, db, sid)
            assert set(_sessions_of(engine)) == {"s-1", "s-2"}
            engine.sessions.remove_client("s-1")
            assert set(_sessions_of(engine)) == {"s-2"}
            # A rebuilt client starts at the revision of the last catalog read.
            await _set_catalog(engine, gateway, 8, {"jira": ["create"]})
            await _turn(engine, db, "s-1")
        finally:
            await engine._mcp_gateway.close()
        entry = _sessions_of(engine)["s-1"]
        assert entry["built_revision"] == entry["applied_revision"] == _revision(gateway)
        assert entry["pending"] is None

    @pytest.mark.asyncio
    async def test_local_mode_tracks_no_session(self, tmp_path, db, monkeypatch):
        engine = AgentEngine(_config(tmp_path), db)
        _claude_without_cli(monkeypatch, engine)
        await db.create_session("s-1", source="web", actor=None)
        client = await engine._get_or_create_client("s-1", "web", None)
        assert client.managed_mcp_servers is None
        assert engine.mcp_gateway_status() is None
        assert engine._session_catalogs.status(engine.sessions.get_client, None) == []


def _snapshot(generation, *ids, url="http://192.0.2.1:8080", inventory=None):
    from nerve.mcp_gateway import CatalogServers, GatewayInventory, GatewayServer, desired_revision

    servers = [
        GatewayServer(
            id=i, url=f"{url}/s/{i}/mcp", tools=("read",),
            desired_revision=f"sha256:{i}",
            inventory=None if inventory is None else GatewayInventory(
                revision=inventory, tool_count=1,
                observed_at="2026-10-17T12:00:00Z", desired_revision=f"sha256:{i}",
            ),
        )
        for i in ids
    ]
    return CatalogServers(
        servers, generation=generation, digest=f"sha256:{generation}",
        desired_revision=desired_revision((s.id, s.desired_revision) for s in servers),
    )


async def _none() -> tuple:
    return ()


class TestSessionCatalogUpdates:
    """The update step on its own."""

    @pytest.mark.asyncio
    async def test_a_cancelled_update_replaces_the_client_next_time(self):
        from nerve.agent.managed_mcp import SessionCatalogs, UpdateOutcome

        log: list[tuple] = []
        client = _ScriptedClient(1, _snapshot(7, "docs", "github"), log, native_id="n", hang={"github": 5.0})
        catalogs = SessionCatalogs()
        catalogs.record("s-1", "claude", client)
        task = asyncio.ensure_future(catalogs.update(
            "s-1", client, _snapshot(8, "docs"), in_place=True, blockers=_none,
        ))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        outcome = await catalogs.update(
            "s-1", client, _snapshot(8, "docs"), in_place=True, blockers=_none,
        )
        assert outcome is UpdateOutcome.REPLACE
        entry = catalogs.status(lambda sid: client, _snapshot(8, "docs"))[0]
        assert entry["applied_revision"] == _snapshot(7, "docs", "github").desired_revision
        assert entry["pending"]["servers"] == ["github"]

    @pytest.mark.asyncio
    async def test_a_changed_url_needs_a_new_client(self):
        from nerve.agent.managed_mcp import SessionCatalogs, UpdateOutcome

        client = _ScriptedClient(1, _snapshot(7, "docs"), [], native_id="n")
        catalogs = SessionCatalogs()
        catalogs.record("s-1", "claude", client)
        moved = _snapshot(7, "docs", url="http://192.0.2.2:8080")
        assert catalogs.status(lambda sid: client, moved)[0]["pending"]["servers"] == ["docs"]
        outcome = await catalogs.update("s-1", client, moved, in_place=True, blockers=_none)
        assert outcome is UpdateOutcome.REPLACE

    @pytest.mark.asyncio
    async def test_live_work_defers_and_unknown_state_is_live_work(self):
        from nerve.agent.managed_mcp import SessionCatalogs, UpdateOutcome

        client = _ScriptedClient(1, _snapshot(7, "docs"), [], native_id="n", mcp_changes=False)
        catalogs = SessionCatalogs()
        catalogs.record("s-1", "codex", client)

        async def unknown():
            return ("state_unknown",)

        outcome = await catalogs.update(
            "s-1", client, _snapshot(8, "docs", "jira"), in_place=False, blockers=unknown,
        )
        assert outcome is UpdateOutcome.DEFERRED
        pending = catalogs.status(lambda sid: client, _snapshot(8, "docs", "jira"))[0]["pending"]
        assert pending["reason"] == "waiting_for_safe_boundary"
        assert pending["blocked_by"] == ["state_unknown"]

    def test_a_client_built_from_an_older_catalog_keeps_its_revision(self):
        """A newer catalog read during the build does not mark the client."""
        from nerve.agent.managed_mcp import SessionCatalogs

        older, newer = _snapshot(7, "docs"), _snapshot(8, "docs", "jira")
        client = _ScriptedClient(1, older, [], native_id="n")
        catalogs = SessionCatalogs()
        catalogs.record("s-1", "claude", client)
        entry = catalogs.status(lambda sid: client, newer)[0]
        assert entry["built_revision"] == entry["applied_revision"] == older.desired_revision
        assert entry["pending"]["reason"] == "waiting_for_turn"
        assert entry["pending"]["target_revision"] == newer.desired_revision

    @pytest.mark.asyncio
    async def test_the_claude_client_uses_the_documented_sdk_calls(self, tmp_path, db, monkeypatch):
        engine = AgentEngine(_config(tmp_path), db)
        calls = _claude_without_cli(monkeypatch, engine)
        claude = engine._backends["claude"]
        with patch.object(claude, "_build_hooks", return_value={}):
            client = await claude.create_client(_spec(engine.config))
        await client.set_mcp_server_enabled("docs", False)
        await client.reconnect_mcp_server("docs")
        assert calls == [("toggle", "docs", False), ("reconnect", "docs")]
        assert await client.replacement_blockers() == ()

    @pytest.mark.asyncio
    async def test_a_claude_client_that_must_resume_needs_its_transcript(self, tmp_path, db, monkeypatch):
        from nerve.agent.backends.base import ResumeRequiredError

        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        engine = AgentEngine(_config(tmp_path), db)
        _claude_without_cli(monkeypatch, engine)
        claude = engine._backends["claude"]
        spec = _spec(engine.config)
        spec.resume_native_id = "11111111-2222-3333-4444-555555555555"
        spec.resume_required = True
        with patch.object(claude, "_build_hooks", return_value={}):
            with pytest.raises(ResumeRequiredError):
                await claude.create_client(spec)
            projects = Path(os.path.expanduser("~/.claude/projects"))
            folder = projects / os.path.realpath(spec.cwd).replace("/", "-")
            folder.mkdir(parents=True)
            (folder / f"{spec.resume_native_id}.jsonl").write_text("{}\n")
            client = await claude.create_client(spec)
        assert client._options.resume == spec.resume_native_id
        assert client.native_session_id == spec.resume_native_id


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
        """The list follows the last catalog read also when a row write failed."""
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
        assert github["first_seen_at"] == routes.mcp_gateway_status()["fetched_at"]
        detail = await get_mcp_server_detail("github")
        assert detail["display_name"] == "Github"
        assert detail["recent_usage"] == []

    @pytest.mark.asyncio
    async def test_the_server_api_counts_only_observed_tools(
        self, tmp_path, db, gateway, external,
    ):
        from nerve.gateway.routes import _deps
        from nerve.gateway.routes.mcp_servers import list_mcp_servers

        gateway.catalog = catalog_payload(
            7, {"docs": ["search"], "jira": ["create"], "wiki": ["read"]},
            inventory={"jira": None, "wiki": []},
        )
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        await engine._sync_mcp_servers_to_db()
        previous = _deps._deps
        _deps.init_deps(engine, db)
        try:
            rows = {row["name"]: row for row in (await list_mcp_servers())["servers"]}
        finally:
            _deps._deps = previous
            await engine._mcp_gateway.close()
        assert rows["docs"]["tool_count"] == 1
        # Unknown is not zero: the policy names a tool the gateway did not list.
        assert rows["jira"]["tool_count"] is None
        assert rows["wiki"]["tool_count"] == 0

    @pytest.mark.asyncio
    async def test_the_server_api_names_the_tool_mode(
        self, tmp_path, db, gateway, external,
    ):
        from nerve.gateway.routes import _deps
        from nerve.gateway.routes.mcp_servers import get_mcp_server_detail, list_mcp_servers

        gateway.catalog_body = (FIXTURES / "catalog-all-tools.json").read_bytes()
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        await engine._sync_mcp_servers_to_db()
        previous = _deps._deps
        _deps.init_deps(engine, db)
        try:
            rows = {row["name"]: row for row in (await list_mcp_servers())["servers"]}
            wiki = await get_mcp_server_detail("wiki")
        finally:
            _deps._deps = previous
            await engine._mcp_gateway.close()
        assert (rows["docs"]["tool_mode"], rows["docs"]["tool_count"]) == ("selected", 2)
        # With the mode all, the count is the size of the last list, not a limit.
        assert (rows["wiki"]["tool_mode"], rows["wiki"]["tool_count"]) == ("all", 2)
        assert (wiki["tool_mode"], wiki["tool_count"]) == ("all", 2)
        assert "tool_mode" not in rows["nerve"]

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
    async def test_reports_the_catalog_read(
        self, tmp_path, db, gateway, external, diagnostics_of,
    ):
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        try:
            report = await diagnostics_of(engine)
            first = gateway.catalog
            gateway.catalog = catalog_payload(
                8, {"docs": ["search"], "jira": ["create"]}, inventory={"jira": None},
            )
            await engine._mcp_gateway.refresh()
            later = await diagnostics_of(engine)
        finally:
            await engine._mcp_gateway.close()
        block = report["mcp_gateway"]
        assert block["url"] == gateway.url
        assert block["generation"] == 7
        assert block["digest"] == catalog_payload(7, {})["digest"]
        assert block["desired_revision"] == first["desiredRevision"]
        assert [server["id"] for server in block["servers"]] == ["docs", "github"]
        assert block["error"] is None
        assert block["fetched_at"] is not None
        # A catalog read is not a running client's state.
        assert not any("applied" in key for key in block if key != "not_applied")

        block = later["mcp_gateway"]
        assert block["generation"] == 8
        assert block["desired_revision"] == gateway.catalog["desiredRevision"]
        docs, jira = block["servers"]
        assert (docs["id"], docs["inventory"]["tool_count"]) == ("docs", 1)
        assert docs["health"]["inventory_state"] == "current"
        assert jira["id"] == "jira"
        assert jira["desired_revision"] == gateway.catalog["servers"][1]["desiredRevision"]
        assert jira["inventory"] is None
        assert jira["health"] == {
            "inventory_state": "unknown", "checked_at": None, "failure": None,
        }

    @pytest.mark.asyncio
    async def test_reports_the_tool_policy_without_a_tool_limit(
        self, tmp_path, db, gateway, external, diagnostics_of,
    ):
        gateway.catalog_body = (FIXTURES / "catalog-all-tools.json").read_bytes()
        engine = AgentEngine(_config(tmp_path, mcp_gateway_url=gateway.url), db)
        await engine._mcp_gateway.start()
        try:
            every = (await diagnostics_of(engine))["mcp_gateway"]
            gateway.catalog_body = (FIXTURES / "catalog-review-changes.json").read_bytes()
            await engine._mcp_gateway.refresh()
            review = (await diagnostics_of(engine))["mcp_gateway"]
        finally:
            await engine._mcp_gateway.close()
        wiki = every["servers"][1]
        assert (wiki["id"], wiki["tool_mode"], wiki["tool_changes"]) == ("wiki", "all", "accept")
        # The only count is the size of the last list that the gateway served.
        assert set(wiki) == {
            "id", "desired_revision", "tool_mode", "tool_changes", "inventory", "health",
        }
        assert wiki["inventory"]["tool_count"] == 2
        assert [server["tool_changes"] for server in review["servers"]] == ["review", "accept"]

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
