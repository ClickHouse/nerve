"""Hosted mode configuration: parsing, doctor, reload, and the Slack runtime.

Hosted mode must start without Slack tokens, and nothing that assumes tokens
may stand in its way: the config parser, ``nerve doctor``, the reload report,
and the Socket Mode lifecycle owner.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from nerve.channels.hosted.runtime import HostedChannelRuntime, hosted_providers
from nerve.channels.slack_runtime import SlackRuntime
from nerve.cli import doctor_report
from nerve.config import (
    HostedChannelsConfig,
    NerveConfig,
    SlackConfig,
    validate_config_keys,
)
from nerve.config_reload import restart_required

HOSTED = {
    "issuer": "http://cp.localhost:8080/workload-identity",
    "jwks_url": "http://cp.localhost:8080/workload-identity/jwks.json",
    "tenant_id": "6f1d3a2c-0b4e-4f7a-9c1d-2e5b8a3f7c04",
    "agent_id": "b28c5e91-7d4a-4c3b-8f61-0a9e2d4b6c17",
}


def hosted_config(**hosted: Any) -> NerveConfig:
    return NerveConfig.from_dict({
        "slack": {"mode": "hosted"},
        "channels": {"hosted": {**HOSTED, **hosted}},
    })


class TestParsing:
    def test_hosted_mode_turns_slack_on_without_tokens(self):
        config = hosted_config()

        assert (config.slack.enabled, config.slack.mode) == (True, "hosted")
        assert config.slack.bot_token == config.slack.app_token == ""
        assert hosted_providers(config) == ["slack"]

    def test_hosted_mode_under_lockdown_needs_no_local_opt_in(self):
        config = NerveConfig.from_dict({"lockdown": True, "slack": {"mode": "hosted"}})

        assert config.slack.enabled

    def test_an_explicit_enabled_false_still_wins(self):
        config = NerveConfig.from_dict({"slack": {"mode": "hosted", "enabled": False}})

        assert not config.slack.enabled
        assert hosted_providers(config) == []

    def test_socket_mode_is_the_default(self):
        config = NerveConfig.from_dict({"slack": {"bot_token": "xoxb-1", "app_token": "xapp-1"}})

        assert (config.slack.enabled, config.slack.mode) == (True, "socket")
        assert hosted_providers(config) == []

    def test_an_unknown_mode_falls_back_to_socket(self, caplog):
        with caplog.at_level(logging.WARNING):
            config = NerveConfig.from_dict({"slack": {"mode": "gateway"}})

        assert config.slack.mode == "socket"
        assert not config.slack.enabled
        assert "slack.mode" in caplog.text

    def test_the_hosted_settings_are_read(self):
        config = hosted_config(max_streams="4", audience="nerve-channel")

        assert config.channels.hosted == HostedChannelsConfig(**HOSTED, max_streams=4)
        assert config.channels.hosted.problems() == []

    @pytest.mark.parametrize(("key", "value", "problem"), [
        ("issuer", "", "issuer"),
        ("jwks_url", "ftp://cp.example/jwks.json", "jwks_url"),
        ("tenant_id", "6F1D3A2C-0B4E-4F7A-9C1D-2E5B8A3F7C04", "tenant_id"),
        ("agent_id", "agent-1", "agent_id"),
        ("max_streams", 0, "max_streams"),
    ])
    def test_unusable_settings_are_named(self, key, value, problem):
        problems = hosted_config(**{key: value}).channels.hosted.problems()

        assert len(problems) == 1 and problem in problems[0]

    @pytest.mark.parametrize(("issuer", "allowed"), [
        ("https://cp.example.com/workload-identity", True),
        ("http://cp.localhost:8080/workload-identity", True),
        ("http://localhost:8080/workload-identity", True),
        ("http://127.0.0.1:8080/workload-identity", True),
        ("http://[::1]:8080/workload-identity", True),
        ("http://cp.example.com/workload-identity", False),
        ("http://10.0.0.5/workload-identity", False),
    ])
    def test_plain_http_is_for_local_hosts_only(self, issuer, allowed):
        problems = hosted_config(issuer=issuer, jwks_url=issuer + "/jwks.json").channels.hosted.problems()

        assert (problems == []) is allowed
        if not allowed:
            assert all("must use https" in problem for problem in problems)

    def test_the_new_keys_are_known(self):
        merged = {"slack": {"mode": "hosted"}, "channels": {"hosted": {**HOSTED, "max_streams": 2}}}

        assert validate_config_keys(merged) == []


class TestDoctor:
    def test_hosted_mode_reports_no_missing_token(self):
        report = doctor_report(hosted_config())

        assert "[OK] Slack hosted by the channel gateway" in report
        assert "Slack enabled but" not in report

    def test_unusable_hosted_settings_are_errors(self):
        report = doctor_report(hosted_config(agent_id=""))

        assert "[ERR] Slack is hosted but channels.hosted.agent_id must be a lowercase UUID" in report

    def test_socket_mode_still_reports_missing_tokens(self):
        report = doctor_report(NerveConfig.from_dict({"slack": {"enabled": True}}))

        assert "bot_token, app_token not set" in report


class TestReload:
    def test_mode_and_hosted_changes_need_a_restart(self):
        before = hosted_config()
        after = hosted_config(max_streams=3)
        after.slack.mode = "socket"

        changed = restart_required(before, after)

        assert any(line.startswith("channels.hosted") for line in changed)
        assert any(line.startswith("slack.mode") for line in changed)


class _Router:
    def __init__(self) -> None:
        self.channels: dict[str, Any] = {}

    def register(self, channel) -> None:
        self.channels[channel.name] = channel

    def unregister(self, channel) -> bool:
        if self.channels.get(channel.name) is not channel:
            return False
        del self.channels[channel.name]
        return True

    def get_channel(self, name: str):
        return self.channels.get(name)


class _SocketChannel:
    name = "slack"
    started = 0

    def __init__(self, config, router) -> None:
        self.stopped_with: list[bool] = []

    def set_notification_service(self, service) -> None:
        pass

    @property
    def is_available(self) -> bool:
        return True

    async def start(self) -> None:
        type(self).started += 1

    async def stop(self, *, drain: bool = False) -> None:
        self.stopped_with.append(drain)


@pytest.mark.asyncio
class TestSlackRuntime:
    @pytest.fixture(autouse=True)
    def _socket_channel(self, monkeypatch):
        from nerve.channels import slack_runtime

        _SocketChannel.started = 0
        monkeypatch.setattr(slack_runtime, "SlackChannel", _SocketChannel)

    async def test_hosted_mode_starts_no_socket_mode_connection(self):
        router = _Router()

        outcome = await SlackRuntime(router).reconcile(hosted_config())

        assert outcome is None
        assert _SocketChannel.started == 0
        assert router.channels == {}

    async def test_moving_to_hosted_mode_stops_the_socket_connection(self):
        router = _Router()
        runtime = SlackRuntime(router)
        socket_mode = NerveConfig()
        socket_mode.slack = SlackConfig(enabled=True, bot_token="xoxb-1", app_token="xapp-1")
        assert await runtime.reconcile(socket_mode) == "enabled"
        channel = router.channels["slack"]

        assert await runtime.reconcile(hosted_config()) == "disabled"
        assert channel.stopped_with == [True]
        assert router.channels == {}

    async def test_socket_mode_refuses_while_the_hosted_channel_holds_the_name(self):
        router = _Router()
        config = hosted_config()
        hosted = HostedChannelRuntime(config, router, lambda: config)
        await hosted.start()
        socket_mode = NerveConfig()
        socket_mode.slack = SlackConfig(enabled=True, bot_token="xoxb-1", app_token="xapp-1")
        try:
            with pytest.raises(Exception, match="outside the runtime lifecycle"):
                await SlackRuntime(router).reconcile(socket_mode)
        finally:
            await hosted.stop()


@pytest.mark.asyncio
class TestHostedRuntime:
    async def test_it_registers_the_provider_channel_and_unregisters_it(self):
        router = _Router()
        config = hosted_config()
        runtime = HostedChannelRuntime(config, router, lambda: config)

        await runtime.start()
        channel = router.channels["slack"]
        await runtime.stop()

        assert channel.name == "slack"
        assert not channel.is_available
        assert router.channels == {}

    async def test_unusable_settings_are_refused(self):
        config = hosted_config(jwks_url="")

        with pytest.raises(ValueError, match="jwks_url"):
            HostedChannelRuntime(config, _Router(), lambda: config)

    async def test_it_refuses_to_shadow_a_registered_channel(self):
        router = _Router()
        router.register(_SocketChannel(None, router))
        config = hosted_config()
        runtime = HostedChannelRuntime(config, router, lambda: config)

        with pytest.raises(RuntimeError, match="already registered"):
            await runtime.start()
