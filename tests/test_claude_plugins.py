"""Tests for Claude Code plugin discovery and configured plugin dirs."""

from __future__ import annotations

import json
from pathlib import Path

from nerve.config import AgentConfig, load_claude_code_plugins


def _make_claude_dir(tmp_path: Path) -> Path:
    claude = tmp_path / ".claude"
    (claude / "plugins").mkdir(parents=True)
    return claude


def _install_cached_plugin(claude: Path, name: str, with_mcp: bool) -> Path:
    d = claude / "plugins" / "cache" / "mp" / name / "1.0.0"
    d.mkdir(parents=True)
    if with_mcp:
        (d / ".mcp.json").write_text("{}")
    else:
        (d / "README.md").write_text("docs only")
    return d


def _enable(claude: Path, *keys: str) -> None:
    (claude / "settings.json").write_text(
        json.dumps({"enabledPlugins": {k: True for k in keys}})
    )


def _make_local_plugin(tmp_path: Path, name: str) -> Path:
    plug = tmp_path / name
    (plug / ".claude-plugin").mkdir(parents=True)
    (plug / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": name, "version": "1.0.0"})
    )
    return plug


class TestAutoDiscovery:
    def test_enabled_plugin_with_mcp_json(self, tmp_path: Path) -> None:
        claude = _make_claude_dir(tmp_path)
        cached = _install_cached_plugin(claude, "alpha", with_mcp=True)
        _enable(claude, "alpha@mp")
        assert load_claude_code_plugins(claude) == [
            {"type": "local", "path": str(cached)}
        ]

    def test_plugin_without_mcp_json_is_not_forwarded(self, tmp_path: Path) -> None:
        claude = _make_claude_dir(tmp_path)
        _install_cached_plugin(claude, "beta-lsp", with_mcp=False)
        _enable(claude, "beta-lsp@mp")
        assert load_claude_code_plugins(claude) == []


class TestConfiguredPluginDirs:
    def test_extra_dir_with_manifest(self, tmp_path: Path) -> None:
        claude = _make_claude_dir(tmp_path)
        plug = _make_local_plugin(tmp_path, "gopls-lsp")
        assert load_claude_code_plugins(claude, extra_dirs=[str(plug)]) == [
            {"type": "local", "path": str(plug)}
        ]

    def test_extra_dir_without_manifest_is_skipped(self, tmp_path: Path) -> None:
        claude = _make_claude_dir(tmp_path)
        plug = tmp_path / "empty"
        plug.mkdir()
        assert load_claude_code_plugins(claude, extra_dirs=[str(plug)]) == []

    def test_missing_extra_dir_is_skipped(self, tmp_path: Path) -> None:
        claude = _make_claude_dir(tmp_path)
        gone = tmp_path / "does-not-exist"
        assert load_claude_code_plugins(claude, extra_dirs=[str(gone)]) == []

    def test_extra_dirs_follow_discovered(self, tmp_path: Path) -> None:
        claude = _make_claude_dir(tmp_path)
        cached = _install_cached_plugin(claude, "alpha", with_mcp=True)
        _enable(claude, "alpha@mp")
        plug = _make_local_plugin(tmp_path, "local-lsp")
        assert load_claude_code_plugins(claude, extra_dirs=[str(plug)]) == [
            {"type": "local", "path": str(cached)},
            {"type": "local", "path": str(plug)},
        ]

    def test_expanduser(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("HOME", str(tmp_path))
        claude = _make_claude_dir(tmp_path)
        plug = _make_local_plugin(tmp_path, "home-plug")
        rel = "~/" + plug.name
        assert load_claude_code_plugins(claude, extra_dirs=[rel]) == [
            {"type": "local", "path": str(plug)}
        ]

    def test_relative_dir_resolves_to_absolute(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        claude = _make_claude_dir(tmp_path)
        plug = _make_local_plugin(tmp_path, "rel-plug")
        assert load_claude_code_plugins(claude, extra_dirs=["rel-plug"]) == [
            {"type": "local", "path": str(plug)}
        ]


def test_agent_config_parses_claude_plugin_dirs() -> None:
    cfg = AgentConfig.from_dict({"claude_plugin_dirs": [" /a/b ", ""]})
    assert cfg.claude_plugin_dirs == ["/a/b"]


def test_agent_config_wraps_scalar_as_single_entry() -> None:
    cfg = AgentConfig.from_dict({"claude_plugin_dirs": "~/plugins/gopls"})
    assert cfg.claude_plugin_dirs == ["~/plugins/gopls"]


def test_agent_config_defaults_to_empty() -> None:
    assert AgentConfig.from_dict({}).claude_plugin_dirs == []
