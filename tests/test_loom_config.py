"""Config for the Loom memory backend: ``memory.backend`` and ``memory.loom``.

Locks in the parts an environment-provisioned instance depends on: every Loom
setting falls back to an environment variable when no layer sets it, a value
in a file always wins, a typo in the backend name refuses to load, the Loom
section is only validated while it is in use, and the token never reaches a
repr or a reload summary.
"""

from __future__ import annotations

import pytest

from nerve import config_reload
from nerve.config import (
    DEFAULT_LOOM_BASE_URL,
    LoomMemoryConfig,
    MemoryConfig,
    NerveConfig,
    load_config,
    normalize_loom_base_url,
    validate_config_keys,
)

_ENV = (
    "NERVE_MEMORY_BACKEND",
    "LOOM_BASE_URL",
    "LOOM_API_KEY",
    "LOOM_ORG",
    "LOOM_NAMESPACE",
    "NERVE_AGENT_ID",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """The developer's shell must not decide what these tests read."""
    for name in _ENV:
        monkeypatch.delenv(name, raising=False)


def test_defaults_keep_memu() -> None:
    memory = MemoryConfig.from_dict({})
    assert memory.backend == "memu"
    assert memory.loom.base_url == DEFAULT_LOOM_BASE_URL
    assert memory.loom.token == ""
    assert memory.loom.org == "" and memory.loom.namespace == ""
    assert memory.loom.agent == "" and memory.loom.agent_name == "nerve"
    assert memory.loom.timeout_s == 5.0
    assert memory.loom.verify_tls is True
    assert NerveConfig().memory.backend == "memu"


def test_environment_fills_every_unset_key(monkeypatch) -> None:
    monkeypatch.setenv("NERVE_MEMORY_BACKEND", "Loom")
    monkeypatch.setenv("LOOM_BASE_URL", "http://loom.example.test:8080")
    monkeypatch.setenv("LOOM_API_KEY", "env-token")
    monkeypatch.setenv("LOOM_ORG", "acme")
    monkeypatch.setenv("LOOM_NAMESPACE", "agent-home")
    monkeypatch.setenv("NERVE_AGENT_ID", "agent-1")

    memory = MemoryConfig.from_dict({})

    assert memory.backend == "loom"  # case-insensitive
    assert memory.loom.base_url == "http://loom.example.test:8080"
    assert memory.loom.token == "env-token"
    assert memory.loom.org == "acme"
    assert memory.loom.namespace == "agent-home"
    assert memory.loom.agent_name == "agent-1"


def test_a_value_in_a_file_wins_over_the_environment(monkeypatch) -> None:
    monkeypatch.setenv("NERVE_MEMORY_BACKEND", "loom")
    monkeypatch.setenv("LOOM_NAMESPACE", "from-env")
    monkeypatch.setenv("LOOM_API_KEY", "env-token")

    memory = MemoryConfig.from_dict({
        "backend": "memu",
        "loom": {"namespace": "from-file", "token": "file-token"},
    })

    assert memory.backend == "memu"
    assert memory.loom.namespace == "from-file"
    assert memory.loom.token == "file-token"


def test_a_blank_value_falls_through_to_the_environment(monkeypatch) -> None:
    monkeypatch.setenv("LOOM_NAMESPACE", "from-env")
    monkeypatch.setenv("NERVE_MEMORY_BACKEND", "loom")

    memory = MemoryConfig.from_dict({"backend": "  ", "loom": {"namespace": ""}})

    assert memory.backend == "loom"
    assert memory.loom.namespace == "from-env"


def test_scalars_from_env_references_are_coerced() -> None:
    loom = LoomMemoryConfig.from_dict({"timeout_s": "7.5", "verify_tls": "false"})
    assert loom.timeout_s == 7.5
    assert loom.verify_tls is False


def test_a_non_mapping_loom_section_uses_the_defaults() -> None:
    memory = MemoryConfig.from_dict({"loom": "http://oops"})
    assert memory.loom.base_url == DEFAULT_LOOM_BASE_URL


def test_the_token_is_not_in_repr() -> None:
    loom = LoomMemoryConfig(token="secret-token-value")
    assert "secret-token-value" not in repr(loom)
    assert "secret-token-value" not in repr(MemoryConfig(loom=loom))


def test_unknown_backend_refuses_to_load() -> None:
    with pytest.raises(ValueError, match="memory.backend must be one of"):
        NerveConfig.from_dict({"memory": {"backend": "lom"}})


def test_loom_url_is_normalized_when_loom_is_the_backend() -> None:
    config = NerveConfig.from_dict({
        "memory": {"backend": "loom", "loom": {"base_url": "https://loom.example.test/v1/"}},
    })
    assert config.memory.loom.base_url == "https://loom.example.test"


@pytest.mark.parametrize("url", [
    "loom.example.test",
    "ftp://loom.example.test",
    "https://user:pw@loom.example.test",
    "https://loom.example.test/?token=x",
])
def test_unusable_loom_url_refuses_to_load(url) -> None:
    with pytest.raises(ValueError, match="memory.loom.base_url"):
        NerveConfig.from_dict({"memory": {"backend": "loom", "loom": {"base_url": url}}})


def test_the_loom_section_is_inert_under_memu() -> None:
    """A stale Loom value must not stop a memU instance from starting."""
    config = NerveConfig.from_dict({
        "memory": {"backend": "memu", "loom": {"base_url": "not a url", "timeout_s": 0}},
    })
    assert config.memory.backend == "memu"


def test_non_positive_timeout_refuses_to_load_under_loom() -> None:
    with pytest.raises(ValueError, match="timeout_s"):
        NerveConfig.from_dict({"memory": {"backend": "loom", "loom": {"timeout_s": 0}}})


def test_normalize_strips_slash_and_v1() -> None:
    assert normalize_loom_base_url("http://localhost:7777/") == "http://localhost:7777"
    assert normalize_loom_base_url("http://localhost:7777/v1") == "http://localhost:7777"


def test_loom_keys_are_known_to_the_validator() -> None:
    merged = {"memory": {"backend": "loom", "loom": {
        "base_url": "http://x", "token": "t", "org": "o", "namespace": "n",
        "agent": "a", "timeout_s": 5, "verify_tls": True,
    }}}
    assert validate_config_keys(merged) == []
    typo = validate_config_keys({"memory": {"loom": {"namespcae": "n"}}})
    assert any("memory.loom.namespcae" in w for w in typo)


def test_reload_reports_a_backend_change_without_values() -> None:
    old = NerveConfig()
    new = NerveConfig()
    new.memory = MemoryConfig(
        backend="loom", loom=LoomMemoryConfig(token="secret-token-value"),
    )
    changed = config_reload.restart_required(old, new)
    assert changed == ["memory: changed"]
    assert "memory.loom.token" in config_reload._SECRET_PATHS


def _workspace_settings(tmp_path, text: str):
    workspace = tmp_path / "ws"
    (workspace / "config").mkdir(parents=True)
    (workspace / "config" / "settings.yaml").write_text(text, encoding="utf-8")
    (tmp_path / "config.yaml").write_text(f"workspace: {workspace}\n", encoding="utf-8")
    return tmp_path


def test_a_provisioned_settings_file_names_only_the_backend(tmp_path, monkeypatch) -> None:
    """What a provisioning step writes into the tracked settings: the switch
    alone, with the connection supplied by the environment."""
    monkeypatch.setenv("LOOM_BASE_URL", "http://10.0.0.2:7777")
    monkeypatch.setenv("LOOM_API_KEY", "placeholder-token")
    config = load_config(_workspace_settings(tmp_path, "memory:\n  backend: loom\n"))
    assert config.memory.backend == "loom"
    assert config.memory.loom.base_url == "http://10.0.0.2:7777"
    assert config.memory.loom.token == "placeholder-token"
    assert config.memory.loom.org == "" and config.memory.loom.namespace == ""


def test_the_backend_switch_alone_loads_without_the_variables(tmp_path) -> None:
    """A process that lacks the variables (a shell, an older environment)
    still loads the config: nothing in the file references them."""
    config = load_config(_workspace_settings(tmp_path, "memory:\n  backend: loom\n"))
    assert config.memory.backend == "loom"
    assert config.memory.loom.base_url == "http://localhost:7777"
    assert config.memory.loom.token == ""


def test_a_settings_file_with_env_references(tmp_path, monkeypatch) -> None:
    """Loom keys spelled as ``${VAR}`` references in the tracked settings."""
    monkeypatch.setenv("LOOM_BASE_URL", "http://10.0.0.2:7777")
    monkeypatch.setenv("LOOM_API_KEY", "placeholder-token")
    config_dir = _workspace_settings(tmp_path, (
        "memory:\n"
        "  backend: loom\n"
        "  loom:\n"
        "    base_url: ${LOOM_BASE_URL}\n"
        "    token: ${LOOM_API_KEY}\n"
    ))
    config = load_config(config_dir)
    assert config.memory.backend == "loom"
    assert config.memory.loom.base_url == "http://10.0.0.2:7777"
    assert config.memory.loom.token == "placeholder-token"
    assert config.memory.loom.namespace == ""  # resolved from the token at runtime


def test_environment_alone_selects_loom(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NERVE_MEMORY_BACKEND", "loom")
    monkeypatch.setenv("LOOM_BASE_URL", "http://10.0.0.2:7777")
    monkeypatch.setenv("LOOM_API_KEY", "placeholder-token")
    config = load_config(_workspace_settings(tmp_path, "timezone: UTC\n"))
    assert config.memory.backend == "loom"
    assert config.memory.loom.base_url == "http://10.0.0.2:7777"


def test_loom_variables_without_the_backend_switch_keep_memu(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LOOM_BASE_URL", "http://10.0.0.2:7777")
    monkeypatch.setenv("LOOM_API_KEY", "placeholder-token")
    config = load_config(_workspace_settings(tmp_path, "timezone: UTC\n"))
    assert config.memory.backend == "memu"


def test_doctor_names_the_loom_backend_and_never_the_token(tmp_path, monkeypatch) -> None:
    from nerve import cli

    monkeypatch.setenv("NERVE_MEMORY_BACKEND", "loom")
    monkeypatch.setenv("LOOM_BASE_URL", "http://loom.internal.test:7777")
    monkeypatch.setenv("LOOM_API_KEY", "secret-token-value")
    report = cli.doctor_report(load_config(_workspace_settings(tmp_path, "timezone: UTC\n")))
    assert "[OK] Memory backend: Loom at http://loom.internal.test:7777 (token set)" in report
    assert "OpenAI API key not set" not in report
    assert "secret-token-value" not in report


def test_load_config_reads_the_section_from_a_file(tmp_path, monkeypatch, caplog) -> None:
    monkeypatch.setenv("LOOM_API_KEY", "env-token")
    (tmp_path / "config.yaml").write_text(
        f"workspace: {tmp_path / 'ws'}\n"
        "memory:\n"
        "  backend: loom\n"
        "  loom:\n"
        "    base_url: http://loom.internal.test:7777/\n"
        "    timeout_s: 3\n",
        encoding="utf-8",
    )
    with caplog.at_level("WARNING", logger="nerve.config"):
        config = load_config(tmp_path)
    assert config.memory.backend == "loom"
    assert config.memory.loom.base_url == "http://loom.internal.test:7777"
    assert config.memory.loom.token == "env-token"
    assert config.memory.loom.timeout_s == 3.0
    assert "unknown key" not in caplog.text
