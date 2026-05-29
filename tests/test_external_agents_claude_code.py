"""End-to-end tests for :class:`ClaudeCodeAgent.write_config`."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nerve.external_agents.agents.claude_code import ClaudeCodeAgent
from nerve.external_agents.writer import ConfigWriter


@pytest.fixture
def fake_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    ws = tmp_path / "ws"
    ws.mkdir()
    (ws / "SOUL.md").write_text("soul\n")
    (ws / "USER.md").write_text("user\n")
    return ws


@pytest.fixture
def writer(fake_home: Path) -> ConfigWriter:
    return ConfigWriter(
        conflict_policy="backup",
        allowlist=[(fake_home / ".claude").resolve()],
    )


@pytest.mark.asyncio
async def test_claude_code_creates_files_when_missing(
    fake_home: Path, workspace: Path, writer: ConfigWriter,
) -> None:
    agent = ClaudeCodeAgent()
    result = await agent.write_config(
        nerve_url="https://localhost:8900/mcp/v1/",
        mcp_token="jwt",
        workspace=workspace,
        writer=writer,
    )

    settings_path = fake_home / ".claude" / "settings.json"
    claude_md_path = fake_home / ".claude" / "CLAUDE.md"

    assert settings_path.exists()
    assert claude_md_path.exists()
    assert settings_path in result.config_files_written
    assert claude_md_path in result.config_files_written

    data = json.loads(settings_path.read_text())
    assert data["mcpServers"]["nerve"]["url"] == "https://localhost:8900/mcp/v1/"
    assert data["mcpServers"]["nerve"]["headers"]["Authorization"] == "Bearer jwt"
    assert data["permissions"]["defaultMode"] == "bypassPermissions"


@pytest.mark.asyncio
async def test_claude_code_merges_into_existing_settings(
    fake_home: Path, workspace: Path, writer: ConfigWriter,
) -> None:
    settings_path = fake_home / ".claude" / "settings.json"
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    settings_path.write_text(json.dumps({
        "model": "opus[1m]",
        "enabledPlugins": {"slack": True},
        "mcpServers": {"other": {"type": "stdio", "command": "x"}},
    }))

    agent = ClaudeCodeAgent()
    await agent.write_config(
        nerve_url="https://localhost:8900/mcp/v1/",
        mcp_token="jwt",
        workspace=workspace,
        writer=writer,
    )

    data = json.loads(settings_path.read_text())
    # User keys preserved
    assert data["model"] == "opus[1m]"
    assert data["enabledPlugins"] == {"slack": True}
    # Both MCP servers
    assert "other" in data["mcpServers"]
    assert "nerve" in data["mcpServers"]


@pytest.mark.asyncio
async def test_claude_md_bundles_workspace_files(
    fake_home: Path, workspace: Path, writer: ConfigWriter,
) -> None:
    agent = ClaudeCodeAgent()
    await agent.write_config(
        nerve_url="x",
        mcp_token="t",
        workspace=workspace,
        writer=writer,
    )
    claude_md = (fake_home / ".claude" / "CLAUDE.md").read_text()
    # Includes set in default_file_targets
    assert "soul" in claude_md
    assert "user" in claude_md
    assert "session_context(" in claude_md
