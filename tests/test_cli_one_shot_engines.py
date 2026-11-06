"""The one-shot engines of ``nerve sync`` and ``nerve cron``.

Each command builds an ``AgentEngine`` for itself. It must shut that engine
down before the database closes (the agent registry flushes the records it
still queues, and the command's runtimes disconnect), and while the gateway
runs it must leave the agent records the gateway keeps live alone.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from click.testing import CliRunner

from tests.test_cli_openers import _config_dir, _migrated_but_unbootstrapped_db

_COMMANDS = pytest.mark.parametrize("argv", [["sync"], ["cron"]], ids=["sync", "cron"])


@pytest.fixture
def engine(monkeypatch):
    """The engine the command builds, recording its lifecycle calls."""
    import nerve.agent.engine as engine_mod

    instance = AsyncMock()
    calls: list[str] = []
    instance.initialize.side_effect = lambda **kw: calls.append(f"initialize{sorted(kw.items())}")
    instance.shutdown.side_effect = lambda: calls.append("shutdown")
    instance.calls = calls
    monkeypatch.setattr(engine_mod, "AgentEngine", MagicMock(return_value=instance))
    return instance


@_COMMANDS
def test_the_command_shuts_its_engine_down(tmp_path, engine, argv):
    from nerve.cli import main

    config_dir = _config_dir(tmp_path)
    _migrated_but_unbootstrapped_db()
    result = CliRunner().invoke(main, ["-c", str(config_dir), *argv])
    assert result.exit_code == 0, result.output
    assert engine.calls == ["initialize[('reconcile_agents', True)]", "shutdown"]


@_COMMANDS
def test_a_running_gateway_keeps_its_live_agents(tmp_path, engine, argv, monkeypatch):
    import nerve.cli as cli

    monkeypatch.setattr(cli, "_get_daemon_status", lambda: (True, 4242))
    config_dir = _config_dir(tmp_path)
    _migrated_but_unbootstrapped_db()
    result = CliRunner().invoke(cli.main, ["-c", str(config_dir), *argv])
    assert result.exit_code == 0, result.output
    assert engine.calls == ["initialize[('reconcile_agents', False)]", "shutdown"]


def test_a_failed_shutdown_still_closes_the_database(tmp_path, engine, monkeypatch):
    import nerve.cli as cli

    engine.shutdown.side_effect = RuntimeError("boom")
    config_dir = _config_dir(tmp_path)
    _migrated_but_unbootstrapped_db()
    result = CliRunner().invoke(cli.main, ["-c", str(config_dir), "cron"])
    assert result.exit_code == 0, result.output


@pytest.mark.asyncio
async def test_the_registry_skips_the_startup_reconcile_on_request():
    from nerve.agent.agents.registry import AgentRegistry

    registry = AgentRegistry.for_tests()
    registry.reconcile_startup = AsyncMock(return_value=0)
    await registry.start(reconcile=False)
    try:
        registry.reconcile_startup.assert_not_awaited()
    finally:
        await registry.close()
    await registry.start()
    try:
        registry.reconcile_startup.assert_awaited_once()
    finally:
        await registry.close()
