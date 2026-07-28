"""Tests for the periodic external-agents sync service.

Covers the core sweep semantics (idempotent on second sweep, picks up
source changes on third sweep, isolates per-agent failures) by driving
``run_once`` directly, plus how the background loop shuts down — the one
part that needs a real task on a real event loop.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from nerve.config import (
    ExternalAgentsConfig,
    ExternalAgentTargetConfig,
    NerveConfig,
)
from nerve.external_agents import sync_service as sync_service_module
from nerve.external_agents.sync_service import SyncService


@pytest.fixture
def fake_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Pretend ~ is tmp_path so writes stay sandboxed."""
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    ws = tmp_path / "ws"
    ws.mkdir()
    (ws / "SOUL.md").write_text("# SOUL v1\n")
    (ws / "IDENTITY.md").write_text("# IDENTITY v1\n")
    (ws / "USER.md").write_text("# USER v1\n")
    (ws / "TOOLS.md").write_text("# TOOLS v1\n")
    (ws / "MEMORY.md").write_text("# MEMORY v1\n")
    return ws


@pytest.fixture
def codex_config(fake_home: Path, workspace: Path) -> NerveConfig:
    """Minimal NerveConfig with one Codex target configured."""
    cfg = NerveConfig()
    cfg.workspace = workspace
    cfg.external_agents = ExternalAgentsConfig(
        enabled=True,
        sync_interval_minutes=60,
        conflict_policy="backup",
        targets=[ExternalAgentTargetConfig(name="codex", enabled=True, token="t")],
    )
    return cfg


def _patch_writer_allowlist(fake_home: Path):
    """Force the SyncService's writer to use a tmp-scoped allowlist.

    Otherwise the default ``~/.codex`` allowlist would point at the
    real home directory even with HOME monkeypatched, because
    ``Path('~/.codex').expanduser()`` evaluates at SyncService import
    time on some platforms. Belt-and-braces.
    """
    return patch(
        "nerve.external_agents.writer._default_allowlist",
        return_value=[(fake_home / ".codex").resolve(), (fake_home / ".claude").resolve()],
    )


@pytest.mark.asyncio
async def test_sync_creates_codex_bundle(
    fake_home: Path, codex_config: NerveConfig, workspace: Path,
) -> None:
    with _patch_writer_allowlist(fake_home):
        svc = SyncService(codex_config)
        result = await svc.run_once()

    bundle = fake_home / ".codex" / "AGENTS.md"
    assert bundle.exists()
    assert "SOUL v1" in bundle.read_text()
    assert "MEMORY v1" in bundle.read_text()

    status = result["codex"]
    assert status.name == "codex"
    files = [f for f in status.files if f.path.endswith("AGENTS.md")]
    assert files and files[0].written_at is not None
    assert not files[0].skipped


@pytest.mark.asyncio
async def test_second_sweep_is_idempotent(
    fake_home: Path, codex_config: NerveConfig, workspace: Path,
) -> None:
    with _patch_writer_allowlist(fake_home):
        svc = SyncService(codex_config)
        await svc.run_once()
        # No source changes — second run should hash-match and skip
        result = await svc.run_once()

    files = result["codex"].files
    assert any(f.skipped for f in files), "second sweep with no diff should skip"


@pytest.mark.asyncio
async def test_source_change_triggers_rewrite(
    fake_home: Path, codex_config: NerveConfig, workspace: Path,
) -> None:
    with _patch_writer_allowlist(fake_home):
        svc = SyncService(codex_config)
        await svc.run_once()

        # Mutate a source file
        (workspace / "MEMORY.md").write_text("# MEMORY v2 — updated\n")

        result = await svc.run_once()

    files = [f for f in result["codex"].files if f.path.endswith("AGENTS.md")]
    assert files and not files[0].skipped
    bundle = (fake_home / ".codex" / "AGENTS.md").read_text()
    assert "MEMORY v2 — updated" in bundle


@pytest.mark.asyncio
async def test_disabled_target_is_skipped(
    fake_home: Path, codex_config: NerveConfig,
) -> None:
    codex_config.external_agents.targets[0].enabled = False
    with _patch_writer_allowlist(fake_home):
        svc = SyncService(codex_config)
        result = await svc.run_once()

    status = result["codex"]
    assert status.enabled is False
    assert status.files == []
    assert not (fake_home / ".codex" / "AGENTS.md").exists()


@pytest.mark.asyncio
async def test_unknown_agent_logs_and_skips(
    fake_home: Path, codex_config: NerveConfig, caplog,
) -> None:
    codex_config.external_agents.targets.append(
        ExternalAgentTargetConfig(name="not-a-real-agent", enabled=True),
    )
    with _patch_writer_allowlist(fake_home):
        svc = SyncService(codex_config)
        result = await svc.run_once()

    assert "codex" in result
    assert "not-a-real-agent" not in result


@pytest.mark.asyncio
async def test_status_for_api_is_serializable(
    fake_home: Path, codex_config: NerveConfig,
) -> None:
    import json

    with _patch_writer_allowlist(fake_home):
        svc = SyncService(codex_config)
        await svc.run_once()
        payload = svc.status_for_api()

    # Must round-trip through json without losing data
    json.dumps(payload)
    assert "codex" in payload
    assert payload["codex"]["name"] == "codex"


def _sweep_on_a_test_timescale(
    monkeypatch: pytest.MonkeyPatch, config: NerveConfig, seconds: float,
) -> None:
    """Shrink the gap between sweeps to something a test can wait for.

    Both halves of ``max(floor, configured)`` have to move, or the untouched
    one keeps the loop parked for the best part of an hour.
    """
    config.external_agents.sync_interval_minutes = 0
    monkeypatch.setattr(
        sync_service_module, "_MIN_SWEEP_INTERVAL_SECONDS", seconds,
    )


class TestShutdownWaitsForTheSweep:
    """How the gateway takes the sync service down at shutdown.

    The stop event is the mechanism and cancellation is the backstop, not the
    other way around: the loop only looks at the event between sweeps, so a
    task cancelled where it stands can be partway down its target list, with
    some bundles rendered from the current sources and some still from the
    previous ones.
    """

    @pytest.mark.asyncio
    async def test_an_in_flight_sweep_is_allowed_to_finish(
        self, codex_config: NerveConfig, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _sweep_on_a_test_timescale(monkeypatch, codex_config, 0.05)
        svc = SyncService(codex_config)
        started, finished = asyncio.Event(), []

        async def slow_sweep():
            started.set()
            await asyncio.sleep(0.2)  # stands in for rendering + writing
            finished.append(1)
            return {}

        monkeypatch.setattr(svc, "run_once", slow_sweep)
        await svc.start()  # consumes the eager first sweep
        started.clear()
        finished.clear()
        # Now wait for a sweep the *loop* starts, so stop() lands mid-sweep.
        await asyncio.wait_for(started.wait(), timeout=2)
        task = svc._task

        await svc.stop()

        assert finished == [1], "stop() abandoned the sweep it waits for"
        assert task.done() and not task.cancelled()  # left by its own exit
        assert svc._task is None

    @pytest.mark.asyncio
    async def test_a_wedged_sweep_does_not_hold_shutdown_open(
        self, codex_config: NerveConfig, monkeypatch: pytest.MonkeyPatch,
        caplog,
    ) -> None:
        """The backstop still exists: one renderer blocking forever must not
        keep the process alive, and giving up should say so out loud."""
        _sweep_on_a_test_timescale(monkeypatch, codex_config, 0.05)
        svc = SyncService(codex_config)
        wedged = asyncio.Event()
        sweeps = 0

        async def wedges_after_the_first_sweep():
            nonlocal sweeps
            sweeps += 1
            if sweeps > 1:  # let start()'s eager sweep through
                wedged.set()
                while True:
                    await asyncio.sleep(0.01)
            return {}

        monkeypatch.setattr(svc, "run_once", wedges_after_the_first_sweep)
        await svc.start()
        await asyncio.wait_for(wedged.wait(), timeout=2)
        task = svc._task

        with caplog.at_level(logging.WARNING, logger="nerve.utils.aio"):
            await svc.stop(timeout=0.2)

        assert task.cancelled()
        assert "did not stop within" in caplog.text
        assert svc._task is None

    @pytest.mark.asyncio
    async def test_stop_without_start_is_harmless(
        self, codex_config: NerveConfig,
    ) -> None:
        """Lifespan shutdown runs even when startup failed before start()."""
        await SyncService(codex_config).stop()


@pytest.mark.asyncio
async def test_update_config_swaps_targets_and_conflict_policy(
    fake_home: Path, codex_config: NerveConfig, workspace: Path,
) -> None:
    """A config reload replaces the process config object, and the routes that
    add / remove / toggle a target edit that object in place — so a sweeper still
    holding the previous one would keep rendering the old target list while every
    toggle reported success.
    """
    with _patch_writer_allowlist(fake_home):
        svc = SyncService(codex_config)
        assert svc._writer.policy == "backup"

        reloaded = NerveConfig()
        reloaded.workspace = workspace
        reloaded.external_agents = ExternalAgentsConfig(
            enabled=True,
            sync_interval_minutes=5,
            conflict_policy="skip",
            targets=[
                ExternalAgentTargetConfig(name="codex", enabled=False, token="t"),
            ],
        )
        svc.update_config(reloaded)

        assert svc._config is reloaded
        assert svc._writer.policy == "skip"
        result = await svc.run_once()
        assert result["codex"].enabled is False


@pytest.mark.asyncio
async def test_loop_reads_the_interval_every_cycle(
    fake_home: Path, codex_config: NerveConfig,
) -> None:
    """The interval used to be computed once before the loop, so a reload that
    changed it could never take effect."""
    import asyncio

    with _patch_writer_allowlist(fake_home):
        svc = SyncService(codex_config)

    timeouts: list[float] = []
    real_wait_for = asyncio.wait_for

    async def fake_wait_for(coro, timeout=None):
        timeouts.append(timeout)
        coro.close()
        if len(timeouts) == 1:
            # Between cycles: a reload halves the configured interval.
            svc._config.external_agents.sync_interval_minutes = 30
            raise asyncio.TimeoutError
        svc._stop_event.set()
        return True

    with patch.object(asyncio, "wait_for", fake_wait_for), \
            patch.object(svc, "run_once", new=_noop_sweep):
        await svc._loop()

    assert real_wait_for is asyncio.wait_for  # patch cleanly undone
    assert timeouts == [3600, 1800]


async def _noop_sweep():
    return {}
