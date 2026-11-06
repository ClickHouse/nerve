"""``memory.backend`` and ``memory.loom``: the switch to the Loom backend.

Locks in that memU stays the default, that a typo in the backend name
refuses to load, that ``memory.loom`` is checked only while Loom is the
backend, and that the backend takes nothing from the environment: a platform
writes the switch into the settings, and Nerve reaches Loom through the MCP
gateway with no URL or credential of its own.
"""

from __future__ import annotations

import pytest

from nerve import config_reload
from nerve.config import (
    LoomMemoryConfig,
    MemoryConfig,
    NerveConfig,
    load_config,
    validate_config_keys,
)


def test_memu_stays_the_default() -> None:
    memory = MemoryConfig.from_dict({})
    assert memory.backend == "memu"
    assert memory.loom == LoomMemoryConfig(org="", namespace="", dashboard_url="")
    assert NerveConfig().memory.backend == "memu"


def test_the_backend_name_is_case_insensitive() -> None:
    assert MemoryConfig.from_dict({"backend": " Loom "}).backend == "loom"


def test_an_unknown_backend_refuses_to_load() -> None:
    with pytest.raises(ValueError, match="memory.backend must be one of"):
        NerveConfig.from_dict({"memory": {"backend": "lom"}})


def test_the_environment_selects_nothing(monkeypatch) -> None:
    """The switch is a setting; no variable turns Loom on or names Loom."""
    for name, value in {
        "NERVE_MEMORY_BACKEND": "loom",
        "LOOM_BASE_URL": "http://loom.example.test",
        "LOOM_API_KEY": "placeholder-token",
        "LOOM_DASHBOARD_URL": "https://loom.example.test",
    }.items():
        monkeypatch.setenv(name, value)
    memory = MemoryConfig.from_dict({})
    assert memory.backend == "memu"
    assert memory.loom.dashboard_url == ""


def test_loom_settings_are_read() -> None:
    config = NerveConfig.from_dict({"memory": {"backend": "loom", "loom": {
        "org": " acme ", "namespace": "agent-home", "dashboard_url": "https://loom.example.test/",
    }}})
    assert config.memory.loom == LoomMemoryConfig(
        org="acme", namespace="agent-home", dashboard_url="https://loom.example.test",
    )


@pytest.mark.parametrize("url", [
    "loom.example.test",
    "ftp://loom.example.test",
    "https://user:pw@loom.example.test",
    "https://loom.example.test/?tab=memories",
    "https://loom.example.test/#memories",
])
def test_an_unusable_dashboard_url_refuses_to_load(url) -> None:
    with pytest.raises(ValueError, match="memory.loom.dashboard_url"):
        NerveConfig.from_dict({"memory": {"backend": "loom", "loom": {"dashboard_url": url}}})


def test_extraction_waits_unless_deferred() -> None:
    assert LoomMemoryConfig().defer_extraction is False
    config = NerveConfig.from_dict({"memory": {"backend": "loom", "loom": {"defer_extraction": "true"}}})
    assert config.memory.loom.defer_extraction is True


@pytest.mark.parametrize(("key", "value"), [
    ("namespace", "Agent Home"),
    ("namespace", "-home"),
    ("namespace", "x" * 129),
    ("org", "acme corp"),
    ("org", "acme/east"),
])
def test_an_unusable_loom_name_refuses_to_load(key, value) -> None:
    with pytest.raises(ValueError, match=f"memory.loom.{key}"):
        NerveConfig.from_dict({"memory": {"backend": "loom", "loom": {key: value}}})


def test_the_loom_section_is_inert_under_memu() -> None:
    """A stale Loom value must not stop a memU instance from starting."""
    config = NerveConfig.from_dict({
        "memory": {"backend": "memu", "loom": {"dashboard_url": "not a url", "namespace": "Not One"}},
    })
    assert config.memory.backend == "memu"


def test_a_non_mapping_loom_section_uses_the_defaults() -> None:
    assert MemoryConfig.from_dict({"loom": "https://oops"}).loom == LoomMemoryConfig()


def test_loom_keys_are_known_to_the_validator() -> None:
    merged = {"memory": {"backend": "loom", "loom": {
        "org": "o", "namespace": "n", "dashboard_url": "https://d", "defer_extraction": True,
    }}}
    assert validate_config_keys(merged) == []
    removed = validate_config_keys({"memory": {"loom": {"base_url": "x", "token": "t"}}})
    assert any("memory.loom.base_url" in w for w in removed)
    assert any("memory.loom.token" in w for w in removed)


def test_a_backend_change_needs_a_restart() -> None:
    new = NerveConfig()
    new.memory = MemoryConfig(backend="loom")
    assert config_reload.restart_required(NerveConfig(), new) == ["memory: changed"]


def test_a_settings_file_that_names_only_the_backend(tmp_path) -> None:
    """What first boot writes: the switch alone. The gateway URL is the one
    Nerve already has; nothing else is needed."""
    workspace = tmp_path / "ws"
    (workspace / "config").mkdir(parents=True)
    (workspace / "config" / "settings.yaml").write_text("memory:\n  backend: loom\n", encoding="utf-8")
    (tmp_path / "config.yaml").write_text(f"workspace: {workspace}\n", encoding="utf-8")
    (tmp_path / "config.local.yaml").write_text(
        "mcp_gateway_url: http://192.0.2.1:3130/\n", encoding="utf-8",
    )
    config = load_config(tmp_path)
    assert config.memory.backend == "loom"
    assert config.mcp_gateway_url == "http://192.0.2.1:3130"
    assert config.memory.loom == LoomMemoryConfig()


def test_the_switch_in_config_local_yaml_outranks_the_tracked_settings(tmp_path) -> None:
    """A platform switches the instance after its first `nerve init` by
    writing memory.backend into config.local.yaml, which outranks the
    tracked workspace settings."""
    workspace = tmp_path / "ws"
    (workspace / "config").mkdir(parents=True)
    (workspace / "config" / "settings.yaml").write_text("memory:\n  backend: memu\n", encoding="utf-8")
    (tmp_path / "config.yaml").write_text(f"workspace: {workspace}\n", encoding="utf-8")
    (tmp_path / "config.local.yaml").write_text(
        "mcp_gateway_url: http://192.0.2.1:3130\nmemory:\n  backend: loom\n", encoding="utf-8",
    )
    assert load_config(tmp_path).memory.backend == "loom"


def test_a_locked_instance_takes_the_switch_from_its_environment(tmp_path, monkeypatch) -> None:
    """Lockdown reads no machine-local file: the tracked settings name the
    switch as ${VAR:-default} references the service's environment fills,
    and empty values mean memU, no gateway and no link."""
    import subprocess

    workspace = tmp_path / "ws"
    (workspace / "config").mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(workspace)], check=True)
    subprocess.run(
        ["git", "-C", str(workspace), "remote", "add", "origin", "https://example.com/config.git"],
        check=True,
    )
    (workspace / "config" / "settings.yaml").write_text(
        "lockdown: true\n"
        "mcp_gateway_url: ${NERVE_MCP_BASE_URL:-}\n"
        "memory:\n  backend: ${NERVE_MEMORY_BACKEND:-memu}\n"
        "  loom:\n    dashboard_url: ${LOOM_DASHBOARD_URL:-}\n",
        encoding="utf-8",
    )
    (tmp_path / "config.yaml").write_text(f"workspace: {workspace}\n", encoding="utf-8")
    # Ignored under lockdown, like every machine-local file.
    (tmp_path / "config.local.yaml").write_text("memory:\n  backend: loom\n", encoding="utf-8")
    for name in ("NERVE_MCP_BASE_URL", "NERVE_MEMORY_BACKEND", "LOOM_DASHBOARD_URL"):
        monkeypatch.delenv(name, raising=False)
    config = load_config(tmp_path)
    assert (config.lockdown, config.memory.backend, config.mcp_gateway_url) == (True, "memu", "")
    assert config.memory.loom.dashboard_url == ""
    monkeypatch.setenv("NERVE_MEMORY_BACKEND", "loom")
    monkeypatch.setenv("NERVE_MCP_BASE_URL", "http://192.0.2.1:3130")
    monkeypatch.setenv("LOOM_DASHBOARD_URL", "https://loom.example.test/")
    config = load_config(tmp_path)
    assert (config.memory.backend, config.mcp_gateway_url) == ("loom", "http://192.0.2.1:3130")
    assert config.memory.loom.dashboard_url == "https://loom.example.test"
