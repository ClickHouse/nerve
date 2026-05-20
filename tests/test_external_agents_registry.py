"""Smoke tests for the external-agent registry.

Verifies the registry exposes Codex and Claude Code, that each agent
declares sensible defaults, and that ``smoke_check`` degrades gracefully
when the CLI isn't on PATH (the common case in CI).
"""

from __future__ import annotations

from pathlib import Path

from nerve.external_agents.registry import AGENT_REGISTRY


def test_registry_contains_codex_and_claude_code() -> None:
    assert "codex" in AGENT_REGISTRY
    assert "claude-code" in AGENT_REGISTRY


def test_codex_paths_are_under_dot_codex() -> None:
    agent = AGENT_REGISTRY["codex"]
    paths = agent.default_config_paths()
    assert any(str(p).endswith("/.codex/config.toml") for p in paths)
    assert any(str(p).endswith("/.codex/AGENTS.md") for p in paths)


def test_claude_code_paths_are_under_dot_claude() -> None:
    agent = AGENT_REGISTRY["claude-code"]
    paths = agent.default_config_paths()
    assert any(str(p).endswith("/.claude/settings.json") for p in paths)
    assert any(str(p).endswith("/.claude/CLAUDE.md") for p in paths)


def test_default_file_targets_match_config_paths() -> None:
    """The sync service derives its work list from default_file_targets;
    each target's output should be on the agent's config paths list so
    the conflict-detection step in the wizard sees them too."""
    workspace = Path("/tmp/nowhere")
    for agent in AGENT_REGISTRY.values():
        config_paths = set(agent.default_config_paths())
        for target in agent.default_file_targets(workspace):
            assert target.output in config_paths


def test_smoke_check_returns_none_when_cli_missing() -> None:
    """If a real CLI happens to be installed on the test host this test
    isn't asserting anything useful, but in CI both CLIs are absent."""
    # Make the registry check a non-existent CLI by temporarily
    # overriding cli_command on a fresh instance.
    from nerve.external_agents.agents.codex import CodexAgent

    agent = CodexAgent()
    agent.cli_command = "definitely-not-installed-xyz-zzz"
    assert agent.smoke_check() is None


def test_smoke_check_returns_none_when_cli_command_unset() -> None:
    from nerve.external_agents.agents.codex import CodexAgent

    agent = CodexAgent()
    agent.cli_command = None  # type: ignore[assignment]
    assert agent.smoke_check() is None
