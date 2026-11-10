"""``sync.linear``: its settings, and the Linear MCP server it registers."""

from __future__ import annotations

from pathlib import Path

import pytest

from nerve.agent.backends.codex.backend import CodexBackend
from nerve.config import (
    LINEAR_MCP_URL,
    LinearSyncConfig,
    NerveConfig,
    _parse_mcp_servers,
    load_config,
    load_mcp_servers,
)

KEY = "lin_api_secret"


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _config_dir(tmp_path: Path, enabled: bool, local: str = "") -> Path:
    _write(
        tmp_path / "config.yaml",
        f"workspace: {tmp_path / 'ws'}\nsync:\n  linear:\n    enabled: {str(enabled).lower()}\n",
    )
    if local:
        _write(tmp_path / "config.local.yaml", local)
    return tmp_path


def test_the_defaults():
    linear = NerveConfig.from_dict({}).sync.linear
    assert linear == LinearSyncConfig()
    assert (linear.enabled, linear.schedule, linear.api_key, linear.types) == (
        False, "*/5 * * * *", "", ["assigned", "mentioned", "comments"],
    )
    assert (linear.include_bots, linear.mcp, linear.batch_size, linear.condense) == (
        False, True, 50, False,
    )
    assert linear.deny_actors == linear.allow_teams == linear.deny_teams == []


@pytest.mark.parametrize(("value", "types"), [
    (None, ["assigned", "mentioned", "comments"]),
    ("", ["assigned", "mentioned", "comments"]),
    ("Mentioned", ["mentioned"]),
    (["comments", "ASSIGNED", "comments"], ["assigned", "comments"]),
])
def test_types_are_read_in_canonical_order(value, types):
    assert LinearSyncConfig.from_dict({"types": value}).types == types


@pytest.mark.parametrize("value", [["reactions"], [], ["assigned", "status"]])
def test_an_unknown_or_empty_types_refuses_to_load(value):
    with pytest.raises(ValueError, match="sync.linear.types"):
        LinearSyncConfig.from_dict({"types": value})


def test_the_key_comes_from_the_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("LINEAR_API_KEY", f"  {KEY} ")
    config_dir = _config_dir(
        tmp_path, True, "sync:\n  linear:\n    api_key: ${LINEAR_API_KEY}\n",
    )
    assert load_config(config_dir).sync.linear.api_key == KEY


def test_enabled_without_a_key_loads():
    linear = NerveConfig.from_dict({"sync": {"linear": {"enabled": True}}}).sync.linear
    assert linear.enabled and not linear.api_key
    assert linear.mcp_server is None


def _servers(sync_linear: dict, mcp_servers: dict | None = None) -> dict:
    d: dict = {"sync": {"linear": sync_linear}}
    if mcp_servers is not None:
        d["mcp_servers"] = mcp_servers
    return {s.name: s for s in NerveConfig.from_dict(d).mcp_servers}


def test_the_mcp_server_is_registered_with_the_key_as_a_bearer_token():
    server = _servers({"enabled": True, "api_key": KEY})["linear"]
    assert (server.type, server.url, server.enabled) == ("http", LINEAR_MCP_URL, True)
    assert server.headers == {"Authorization": f"Bearer {KEY}"}


@pytest.mark.parametrize("sync_linear", [
    {"enabled": False, "api_key": KEY},
    {"enabled": True, "api_key": ""},
    {"enabled": True, "api_key": KEY, "mcp": False},
    {"enabled": True, "api_key": KEY, "types": ["bogus"]},
])
def test_no_mcp_server_when_off_without_key_unwanted_or_unreadable(sync_linear):
    servers = _parse_mcp_servers({"sync": {"linear": sync_linear}})
    assert "linear" not in {s.name for s in servers}


def test_a_user_entry_named_linear_wins():
    servers = _parse_mcp_servers({
        "sync": {"linear": {"enabled": True, "api_key": KEY}},
        "mcp_servers": {"linear": {"type": "http", "url": "https://example.com/mcp", "enabled": False}},
    })
    (linear,) = [s for s in servers if s.name == "linear"]
    assert (linear.url, linear.enabled) == ("https://example.com/mcp", False)


def test_load_mcp_servers_agrees_and_follows_the_switch(tmp_path):
    local = f"sync:\n  linear:\n    api_key: {KEY}\n"
    config_dir = _config_dir(tmp_path, True, local)
    startup = {s.name: s for s in load_config(config_dir).mcp_servers}
    reloaded = {s.name: s for s in load_mcp_servers(config_dir)}
    assert startup["linear"] == reloaded["linear"]

    _config_dir(tmp_path, False)
    assert "linear" not in {s.name for s in load_mcp_servers(config_dir)}


def test_the_codex_translation_carries_the_header_by_reference():
    server = _servers({"enabled": True, "api_key": KEY})["linear"]
    rendered = "\n".join(CodexBackend._translate_mcp_server(server))
    assert f'mcp_servers.linear.url="{LINEAR_MCP_URL}"' in rendered
    assert "env_http_headers" in rendered and "Authorization" in rendered
    assert KEY not in rendered


def test_sync_linear_is_an_effectful_settings_key():
    from nerve.config_pr import _EFFECTFUL_SETTINGS_KEYS

    assert ("sync", "linear") in _EFFECTFUL_SETTINGS_KEYS


# ---------------------------------------------------------------------- #
#  Doctor                                                                 #
# ---------------------------------------------------------------------- #


def _doctor(sync_linear: dict, mcp_servers: dict | None = None) -> tuple[list[str], list[str]]:
    from nerve.cli import _linear_lines

    d: dict = {"sync": {"linear": sync_linear}}
    if mcp_servers is not None:
        d["mcp_servers"] = mcp_servers
    warnings: list[str] = []
    return _linear_lines(NerveConfig.from_dict(d), warnings), warnings


def test_doctor_off():
    assert _doctor({}) == (["[--] Linear source off (sync.linear.enabled is false)"], [])


def test_doctor_warns_without_a_key():
    lines, warnings = _doctor({"enabled": True})
    assert lines == []
    assert len(warnings) == 1 and "sync.linear.api_key is empty" in warnings[0]


@pytest.mark.parametrize(("extra", "mcp_servers", "mcp"), [
    ({}, None, "MCP registered as linear"),
    ({"mcp": False}, None, "MCP off"),
    ({}, {"linear": {"type": "http", "url": "https://example.com/mcp"}}, "MCP from mcp_servers.linear"),
])
def test_doctor_on(monkeypatch, extra, mcp_servers, mcp):
    monkeypatch.delenv("NERVE_AUTH_MODE", raising=False)
    lines, warnings = _doctor(
        {"enabled": True, "api_key": KEY, "types": ["assigned"], **extra}, mcp_servers,
    )
    assert lines == [f"[OK] Linear source: types assigned; schedule */5 * * * *; {mcp}"]
    assert warnings == []


def test_doctor_warns_that_external_mode_ignores_the_mcp_server(monkeypatch):
    monkeypatch.setenv("NERVE_AUTH_MODE", "external")
    _, warnings = _doctor({"enabled": True, "api_key": KEY})
    assert len(warnings) == 1 and "external auth mode" in warnings[0]
