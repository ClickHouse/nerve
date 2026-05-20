"""Tests for the periodic external-agents sync service.

Covers the core loop semantics (idempotent on second sweep, picks up
source changes on third sweep, isolates per-agent failures) without
spinning up a real asyncio loop — we drive ``run_once`` directly.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from nerve.config import (
    ExternalAgentsConfig,
    ExternalAgentTargetConfig,
    NerveConfig,
)
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
