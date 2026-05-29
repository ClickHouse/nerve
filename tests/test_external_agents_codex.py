"""End-to-end tests for :class:`CodexAgent.write_config`.

Uses a writer pointed at a tmp_path-shaped allowlist so we exercise the
real ConfigWriter without scribbling on the host's ``~/.codex``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from nerve.external_agents.agents.codex import CodexAgent
from nerve.external_agents.writer import ConfigWriter


@pytest.fixture
def fake_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect ``~`` to tmp_path so the agent writes into the sandbox."""
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    ws = tmp_path / "ws"
    ws.mkdir()
    (ws / "SOUL.md").write_text("# SOUL\n")
    (ws / "USER.md").write_text("# USER\n\nName: Test.\n")
    return ws


@pytest.fixture
def writer(fake_home: Path) -> ConfigWriter:
    return ConfigWriter(
        conflict_policy="backup",
        allowlist=[(fake_home / ".codex").resolve()],
    )


@pytest.mark.asyncio
async def test_codex_writes_config_and_agents_md(
    fake_home: Path, workspace: Path, writer: ConfigWriter,
) -> None:
    agent = CodexAgent()
    result = await agent.write_config(
        nerve_url="https://localhost:8900/mcp/v1/",
        mcp_token="test-jwt",
        workspace=workspace,
        writer=writer,
    )

    config_path = fake_home / ".codex" / "config.toml"
    agents_md_path = fake_home / ".codex" / "AGENTS.md"

    assert config_path.exists()
    assert agents_md_path.exists()
    assert config_path in result.config_files_written
    assert agents_md_path in result.config_files_written

    toml_text = config_path.read_text()
    # URL + token landed
    assert "https://localhost:8900/mcp/v1/" in toml_text
    assert "Bearer test-jwt" in toml_text
    # Workspace path is in the [projects.*] header
    assert f'[projects."{workspace}"]' in toml_text
    # Default trust posture
    assert 'approval_policy = "never"' in toml_text
    assert 'sandbox_mode = "danger-full-access"' in toml_text


@pytest.mark.asyncio
async def test_codex_agents_md_includes_workspace_files(
    fake_home: Path, workspace: Path, writer: ConfigWriter,
) -> None:
    agent = CodexAgent()
    await agent.write_config(
        nerve_url="https://localhost:8900/mcp/v1/",
        mcp_token="abc",
        workspace=workspace,
        writer=writer,
    )

    agents_md_path = fake_home / ".codex" / "AGENTS.md"
    content = agents_md_path.read_text()
    assert "session_context(" in content
    assert "Name: Test." in content


@pytest.mark.asyncio
async def test_codex_backup_on_existing_config(
    fake_home: Path, workspace: Path, writer: ConfigWriter,
) -> None:
    config_path = fake_home / ".codex" / "config.toml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("# user's existing config\nmodel = 'gpt-5'\n")

    agent = CodexAgent()
    result = await agent.write_config(
        nerve_url="https://localhost:8900/mcp/v1/",
        mcp_token="xyz",
        workspace=workspace,
        writer=writer,
    )

    assert result.backups_created, "should have backed up existing config.toml"
    backup_pattern = re.compile(r"config\.toml\.nerve-backup-\d+")
    assert any(backup_pattern.search(str(p)) for p in result.backups_created)

    # Original content lives in the backup
    backup = next(p for p in result.backups_created if "config.toml" in str(p))
    assert "user's existing config" in backup.read_text()


@pytest.mark.asyncio
async def test_codex_emits_warning_when_cli_missing(
    fake_home: Path, workspace: Path, writer: ConfigWriter, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Force smoke check to fail by pointing PATH at /nonexistent
    monkeypatch.setenv("PATH", "/nonexistent")

    agent = CodexAgent()
    result = await agent.write_config(
        nerve_url="https://localhost:8900/mcp/v1/",
        mcp_token="t",
        workspace=workspace,
        writer=writer,
    )
    assert any("codex" in w for w in result.warnings)
