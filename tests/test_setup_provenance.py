"""Where a configured connection value came from, per layer kind.

A credential the operator provisioned — from the environment, from the
configuration repository, held by the channel gateway — reads as configured
and managed on the setup page and in ``nerve doctor``; one written by the
page itself reads as ``file``. The detection reads the raw layers, so a
``${VAR}`` reference is told apart from the literal it resolved to.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
import yaml

from nerve import cli, setup_provenance as prov
from nerve.config import NerveConfig, load_config
from nerve.setup_writer import (
    CONFIG_YAML_HEADER,
    SetupChoices,
    merge_machine_paths,
    merge_private_paths,
    merge_settings_paths,
    write_slim_install,
)

_KEY = "sk-ant-REPLACE0000000000"
_TG = "123456:REPLACEabcdefghijklmnopqrstuvwxyz0123"


def _install(tmp_path: Path) -> tuple[Path, Path]:
    config_dir = tmp_path / "config"
    workspace = tmp_path / "workspace"
    write_slim_install(
        SetupChoices(deployment="server", workspace_path=workspace, gateway_host="127.0.0.1", gateway_port=8976),
        config_dir,
    )
    return config_dir, workspace


def _layers(*, settings=None, machine=None, local=None, locked=False) -> prov.RawLayers:
    return prov.RawLayers(settings=settings or {}, machine=machine or {}, local=local or {}, locked=locked)


class TestSourceOf:
    """The pure rule, one row per layer kind."""

    def test_nothing_stated_is_none(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        assert prov.source_of("anthropic_api_key", _layers()) is None
        assert prov.source_of("telegram.bot_token", _layers(local={"telegram": {"bot_token": ""}})) is None
        assert prov.source_of("sync.telegram.api_id", _layers(local={"sync": {"telegram": {"api_id": 0}}})) is None
        assert prov.source_of("sync.gmail.accounts", _layers(machine={"sync": {"gmail": {"accounts": []}}})) is None

    def test_a_literal_in_a_machine_file_is_file(self):
        assert prov.source_of("anthropic_api_key", _layers(local={"anthropic_api_key": _KEY})) == "file"
        assert prov.source_of("sync.gmail.accounts", _layers(machine={"sync": {"gmail": {"accounts": ["alice@example.com"]}}})) == "file"
        # A literal in the tracked file is still replaceable here (the
        # machine layers win over it) — so it reads as a file, not as managed.
        assert prov.source_of("openai_api_key", _layers(settings={"openai_api_key": "sk-REPLACE00000000"})) == "file"

    @pytest.mark.parametrize("ref", ["${TELEGRAM_BOT_TOKEN}", "${TELEGRAM_BOT_TOKEN:-}", "  ${TG_TOKEN_1:-fallback} "])
    def test_an_environment_reference_in_any_layer_is_environment(self, ref):
        for layer in ("local", "machine", "settings"):
            layers = _layers(**{layer: {"telegram": {"bot_token": ref}}})
            assert prov.source_of("telegram.bot_token", layers) == "environment", layer

    def test_a_list_carrying_a_reference_is_environment(self):
        layers = _layers(machine={"sync": {"gmail": {"accounts": ["${GMAIL_ACCOUNT}"]}}})
        assert prov.source_of("sync.gmail.accounts", layers) == "environment"

    def test_the_winning_layer_decides(self):
        """config.local.yaml beats config.yaml beats settings.yaml — the same
        order the loader merges in, so the spelling that produced the
        effective value is the one judged."""
        both = _layers(local={"anthropic_api_key": _KEY}, settings={"anthropic_api_key": "${ANTHROPIC_API_KEY}"})
        assert prov.source_of("anthropic_api_key", both) == "file"
        reversed_ = _layers(local={"anthropic_api_key": "${ANTHROPIC_API_KEY}"}, settings={"anthropic_api_key": _KEY})
        assert prov.source_of("anthropic_api_key", reversed_) == "environment"

    def test_under_lockdown_only_the_tracked_file_counts(self):
        locked = _layers(
            local={"anthropic_api_key": _KEY}, settings={"anthropic_api_key": "sk-ant-REPLACE1111111111"}, locked=True,
        )
        assert prov.source_of("anthropic_api_key", locked) == "config-repo"
        ref = _layers(settings={"anthropic_api_key": "${ANTHROPIC_API_KEY}"}, locked=True)
        assert prov.source_of("anthropic_api_key", ref) == "environment"
        only_local = _layers(local={"anthropic_api_key": _KEY}, locked=True)
        assert prov.source_of("anthropic_api_key", only_local) is None, "lockdown never reads config.local.yaml"

    def test_variables_nerve_reads_directly_are_environment(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", _KEY)
        monkeypatch.setenv("GH_TOKEN", "ghp_REPLACE00000000000000")
        monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
        assert prov.source_of("anthropic_api_key", _layers()) == "environment"
        assert prov.source_of("github_token", _layers()) == "environment"
        assert prov.source_of("claude_oauth_token", _layers()) is None
        # A file wins over the variable: the loader takes the file's value.
        assert prov.source_of("anthropic_api_key", _layers(local={"anthropic_api_key": _KEY})) == "file"

    def test_hosted_slack_tokens_belong_to_the_gateway(self):
        layers = _layers(local={"slack": {"bot_token": "xoxb-REPLACE", "app_token": "xapp-REPLACE"}})
        assert prov.source_of("slack.bot_token", layers, hosted=True) == "gateway"
        assert prov.source_of("slack.app_token", layers, hosted=True) == "gateway"
        assert prov.source_of("slack.bot_token", layers, hosted=False) == "file"
        assert prov.source_of("telegram.bot_token", _layers(local={"telegram": {"bot_token": _TG}}), hosted=True) == "file"

    def test_describe(self):
        assert prov.describe(None) == "" and prov.describe("file") == ""
        assert prov.describe("environment") == " (environment)"
        assert prov.describe("config-repo") == " (config-repo)"


class TestCredentialSourcesOnAnInstall:
    """The three real files of a slim install, read raw."""

    def test_every_path_is_reported_and_nothing_is_a_value(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", _TG)
        monkeypatch.setenv("OPENAI_KEY_FOR_NERVE", "sk-REPLACE00000000")
        monkeypatch.setenv("GMAIL_ACCOUNT", "alice@example.com")
        config_dir, ws = _install(tmp_path)
        merge_private_paths(config_dir / "config.local.yaml", {
            "anthropic_api_key": _KEY,
            "telegram.bot_token": "${TELEGRAM_BOT_TOKEN}",
            "sync.telegram.api_id": 1234567,
            "sync.telegram.api_hash": "REPLACE_HASH_VALUE",
        })
        merge_machine_paths(config_dir / "config.yaml", {"sync.gmail.accounts": "${GMAIL_ACCOUNT}"}, header=CONFIG_YAML_HEADER)
        merge_settings_paths(ws, {"openai_api_key": "${OPENAI_KEY_FOR_NERVE}"})
        config = load_config(config_dir)
        assert config.telegram.bot_token == _TG and config.openai_api_key == "sk-REPLACE00000000"
        assert config.sync.gmail.accounts == ["alice@example.com"]

        sources = prov.credential_sources(config)
        assert set(sources) == set(prov.CREDENTIAL_PATHS)
        assert sources == {
            "anthropic_api_key": "file",
            "claude_oauth_token": None,
            "openai_api_key": "environment",
            "telegram.bot_token": "environment",
            "slack.bot_token": None,
            "slack.app_token": None,
            "sync.telegram.api_id": "file",
            "sync.telegram.api_hash": "file",
            "github_token": None,
            "sync.gmail.accounts": "environment",
        }
        for value in (_KEY, _TG, "REPLACE_HASH_VALUE", "alice@example.com"):
            assert value not in str(sources)

    def test_an_unreadable_layer_reads_as_empty(self, tmp_path):
        config_dir, ws = _install(tmp_path)
        (ws / "config" / "settings.yaml").write_text("- not: [a mapping", encoding="utf-8")
        (config_dir / "config.local.yaml").write_text("anthropic_api_key: " + _KEY + "\n", encoding="utf-8")
        config = NerveConfig.from_dict({"workspace": str(ws), "anthropic_api_key": _KEY})
        config.config_dir = config_dir
        sources = prov.credential_sources(config)
        assert sources["anthropic_api_key"] == "file"
        assert sources["openai_api_key"] is None

    def test_no_config_dir_reads_only_the_environment_and_settings(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "sk-ant-oat01-REPLACE0000")
        config = NerveConfig.from_dict({"workspace": str(tmp_path / "nowhere")})
        config.config_dir = tmp_path / "missing"
        sources = prov.credential_sources(config)
        assert sources["claude_oauth_token"] == "environment"
        assert sources["anthropic_api_key"] is None

    def test_lockdown_reads_the_tracked_file_as_the_repository(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", _KEY)
        config_dir, ws = _install(tmp_path)
        subprocess.run(["git", "init", "-q", "-b", "main"], cwd=ws, check=True)
        subprocess.run(["git", "remote", "add", "origin", "https://example.com/config.git"], cwd=ws, check=True)
        local = yaml.safe_load((config_dir / "config.local.yaml").read_text(encoding="utf-8"))
        settings = yaml.safe_load((ws / "config" / "settings.yaml").read_text(encoding="utf-8")) or {}
        settings.update({
            "lockdown": True,
            "auth": {"jwt_secret": local["auth"]["jwt_secret"]},
            "anthropic_api_key": "${ANTHROPIC_API_KEY}",
            "telegram": {"bot_token": _TG},
            "sync": {**settings.get("sync", {}), "gmail": {"enabled": True, "accounts": ["alice@example.com"]}},
        })
        (ws / "config" / "settings.yaml").write_text(yaml.safe_dump(settings, sort_keys=False), encoding="utf-8")
        merge_private_paths(config_dir / "config.local.yaml", {"openai_api_key": "sk-REPLACE00000000"})
        config = load_config(config_dir)
        assert config.lockdown is True
        sources = prov.credential_sources(config)
        assert sources["anthropic_api_key"] == "environment"
        assert sources["telegram.bot_token"] == "config-repo"
        assert sources["sync.gmail.accounts"] == "config-repo"
        assert sources["openai_api_key"] is None, "lockdown does not read config.local.yaml"


class TestDoctorNamesTheSource:
    """``nerve doctor`` prints where every configured connection came from."""

    def _report(self, config_dir: Path) -> str:
        return cli.doctor_report(load_config(config_dir))

    def test_file_values_carry_no_suffix(self, tmp_path):
        config_dir, _ws = _install(tmp_path)
        merge_private_paths(config_dir / "config.local.yaml", {
            "anthropic_api_key": _KEY, "openai_api_key": "sk-REPLACE00000000", "telegram.bot_token": _TG,
        })
        merge_machine_paths(config_dir / "config.yaml", {"telegram.enabled": True}, header=CONFIG_YAML_HEADER)
        report = self._report(config_dir)
        assert "[OK] Anthropic API key: ...0000\n" in report
        assert "[OK] OpenAI API key: ...0000 (vector embeddings enabled)" in report
        assert "[OK] Telegram bot token: ...0123\n" in report
        assert "(environment)" not in report

    def test_environment_values_are_named(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", _TG)
        monkeypatch.setenv("ANTHROPIC_API_KEY", _KEY)
        config_dir, _ws = _install(tmp_path)
        merge_private_paths(config_dir / "config.local.yaml", {
            "telegram.bot_token": "${TELEGRAM_BOT_TOKEN}", "openai_api_key": "sk-REPLACE00000000",
        })
        merge_machine_paths(config_dir / "config.yaml", {"telegram.enabled": True}, header=CONFIG_YAML_HEADER)
        report = self._report(config_dir)
        assert "[OK] Anthropic API key: configured (environment)" in report
        assert "[OK] Telegram bot token: ...0123 (environment)" in report
        assert "[OK] OpenAI API key: ...0000 (vector embeddings enabled)" in report
        assert "[ERR] Anthropic API key not set" not in report
        assert _KEY not in report and _TG not in report

    def test_a_subscription_token_from_the_environment_is_named_with_its_limit(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "sk-ant-oat01-REPLACE0000")
        config_dir, _ws = _install(tmp_path)
        report = self._report(config_dir)
        assert "[OK] Claude subscription token: configured (environment)" in report
        assert "background jobs (memory, titles, cron) need an API key" in report
        assert "sk-ant-oat01-REPLACE0000" not in report
