"""Slack lifecycle ownership, serialization, and config generations."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from nerve.channels.slack_runtime import SlackRuntime, SlackRuntimeError
from nerve.config import NerveConfig, SlackConfig


class _Router:
    def __init__(self) -> None:
        self.channels: dict[str, Any] = {}
        self.registered: list[Any] = []
        self.unregistered: list[Any] = []

    def register(self, channel) -> None:
        self.channels[channel.name] = channel
        self.registered.append(channel)

    def unregister(self, channel) -> bool:
        if self.channels.get(channel.name) is not channel:
            return False
        del self.channels[channel.name]
        self.unregistered.append(channel)
        return True

    def get_channel(self, name: str):
        return self.channels.get(name)


class _FakeChannel:
    name = "slack"
    instances: list[_FakeChannel] = []
    start_hook = None
    stop_hook = None
    reload_hook = None
    rebuild_hook = None
    connected = True

    def __init__(self, config, router) -> None:
        self.config = config
        self.router = router
        self.service = None
        self.state = "stopped"
        self.web = None
        self.bot_token = config.slack.bot_token
        self.app_token = config.slack.app_token
        self.stop_calls: list[bool] = []
        type(self).instances.append(self)

    @property
    def is_available(self) -> bool:
        return self.state == "running" and self.web is not None

    def set_notification_service(self, service) -> None:
        self.service = service

    async def start(self) -> None:
        self.state = "starting"
        hook = type(self).start_hook
        if hook:
            await hook(self)
        self.web = object()
        self.state = "running"

    async def stop(self, *, drain: bool = False) -> None:
        self.state = "quiescing"
        self.stop_calls.append(drain)
        hook = type(self).stop_hook
        if hook:
            await hook(self, drain)
        self.web = None
        self.state = "stopped"

    def needs_credential_reload(self, bot_token: str, app_token: str) -> bool:
        return (bot_token, app_token) != (self.bot_token, self.app_token)

    async def reload_credentials(self, config) -> None:
        hook = type(self).reload_hook
        if hook:
            await hook(self, config)
        self.bot_token = config.slack.bot_token
        self.app_token = config.slack.app_token
        self.config = config

    def apply_config(self, config) -> None:
        self.config = config

    async def transport_connected(self) -> bool:
        return type(self).connected

    async def rebuild_transport(self) -> None:
        hook = type(self).rebuild_hook
        if hook:
            await hook(self)

    @property
    def seconds_since_last_event(self) -> float:
        return 0.0


def _config(
    *,
    enabled: bool = True,
    bot_token: str = "xoxb-old",
    app_token: str = "xapp-old",
    allow_users: list[str] | None = None,
) -> NerveConfig:
    config = NerveConfig()
    config.slack = SlackConfig(
        enabled=enabled,
        bot_token=bot_token,
        app_token=app_token,
        allow_users=allow_users or [],
    )
    return config


@pytest.fixture(autouse=True)
def _fake_channel(monkeypatch):
    from nerve.channels import slack_runtime

    _FakeChannel.instances = []
    _FakeChannel.start_hook = None
    _FakeChannel.stop_hook = None
    _FakeChannel.reload_hook = None
    _FakeChannel.rebuild_hook = None
    _FakeChannel.connected = True
    monkeypatch.setattr(slack_runtime, "SlackChannel", _FakeChannel)


@pytest.mark.asyncio
async def test_concurrent_enables_construct_and_register_once():
    router = _Router()
    runtime = SlackRuntime(router)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_start(channel):
        entered.set()
        await release.wait()

    _FakeChannel.start_hook = slow_start
    first = asyncio.create_task(runtime.reconcile(_config()))
    second = asyncio.create_task(runtime.reconcile(_config()))
    await entered.wait()
    await asyncio.sleep(0)

    assert len(_FakeChannel.instances) == 1
    assert len(router.registered) == 1
    assert not second.done()

    release.set()
    assert await asyncio.gather(first, second) == ["enabled", None]
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_enable_is_routable_before_the_socket_can_deliver():
    router = _Router()
    runtime = SlackRuntime(router)

    async def assert_publication(channel):
        assert router.get_channel("slack") is channel
        assert not channel.is_available

    _FakeChannel.start_hook = assert_publication
    await runtime.reconcile(_config())
    assert runtime.channel is router.get_channel("slack")
    assert runtime.channel.is_available
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_failed_enable_is_absent_redacted_and_retryable():
    router = _Router()
    runtime = SlackRuntime(router)
    attempts = 0

    async def fail_once(channel):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("rejected xoxb-secret and xapp-secret")

    _FakeChannel.start_hook = fail_once
    desired = _config(bot_token="xoxb-secret", app_token="xapp-secret")
    with pytest.raises(SlackRuntimeError) as failure:
        await runtime.reconcile(desired)

    assert "xoxb-secret" not in str(failure.value)
    assert "xapp-secret" not in str(failure.value)
    assert runtime.channel is None
    assert router.get_channel("slack") is None
    assert await runtime.reconcile(desired) == "enabled"
    assert attempts == 2
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_disable_quiesces_and_drains_before_unregistering():
    router = _Router()
    runtime = SlackRuntime(router)
    await runtime.reconcile(_config())
    channel = runtime.channel
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_stop(candidate, drain):
        assert drain is True
        assert router.get_channel("slack") is candidate
        entered.set()
        await release.wait()

    _FakeChannel.stop_hook = slow_stop
    disabling = asyncio.create_task(runtime.reconcile(_config(enabled=False)))
    await entered.wait()

    assert router.get_channel("slack") is channel
    assert not channel.is_available
    release.set()
    assert await disabling == "disabled"
    assert router.get_channel("slack") is None


@pytest.mark.asyncio
async def test_failed_rotation_retains_the_whole_active_generation():
    router = _Router()
    runtime = SlackRuntime(router)
    original = _config(allow_users=["U1"])
    await runtime.reconcile(original)
    channel = runtime.channel

    async def fail_rotation(candidate, desired):
        raise RuntimeError(
            f"could not use {desired.slack.bot_token} with {desired.slack.app_token}",
        )

    _FakeChannel.reload_hook = fail_rotation
    desired = _config(
        bot_token="xoxb-new",
        app_token="xapp-new",
        allow_users=["U2"],
    )
    with pytest.raises(SlackRuntimeError) as failure:
        await runtime.reconcile(desired)

    assert "xoxb-new" not in str(failure.value)
    assert "xapp-new" not in str(failure.value)
    assert channel.config.slack.allow_users == ["U1"]
    assert runtime.active_config.slack.allow_users == ["U1"]
    assert router.get_channel("slack") is channel
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_config_only_reload_advances_without_transport_replacement():
    router = _Router()
    runtime = SlackRuntime(router)
    await runtime.reconcile(_config(allow_users=["U1"]))
    channel = runtime.channel
    desired = _config(allow_users=["U2"])

    assert await runtime.reconcile(desired) is None
    assert runtime.channel is channel
    assert channel.config.slack.allow_users == ["U2"]
    assert len(_FakeChannel.instances) == 1
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_unrecoverable_rotation_fails_closed():
    router = _Router()
    runtime = SlackRuntime(router)
    await runtime.reconcile(_config())

    async def lose_transport(channel, desired):
        channel.state = "stopped"
        channel.web = None
        raise RuntimeError("rollback failed")

    _FakeChannel.reload_hook = lose_transport
    with pytest.raises(SlackRuntimeError, match="rollback failed"):
        await runtime.reconcile(
            _config(bot_token="xoxb-new", app_token="xapp-new"),
        )

    assert runtime.channel is None
    assert router.get_channel("slack") is None


@pytest.mark.asyncio
async def test_watchdog_rebuild_serializes_with_config_reconcile(monkeypatch):
    from nerve.channels import slack_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "WATCHDOG_INTERVAL", 0.001)
    router = _Router()
    runtime = SlackRuntime(router)
    entered = asyncio.Event()
    release = asyncio.Event()
    _FakeChannel.connected = False

    async def slow_rebuild(channel):
        entered.set()
        await release.wait()
        type(channel).connected = True

    _FakeChannel.rebuild_hook = slow_rebuild
    await runtime.reconcile(_config(allow_users=["U1"]))
    await entered.wait()

    reconcile = asyncio.create_task(
        runtime.reconcile(_config(allow_users=["U2"])),
    )
    await asyncio.sleep(0)
    assert not reconcile.done()

    release.set()
    assert await reconcile is None
    assert runtime.channel.config.slack.allow_users == ["U2"]
    await runtime.shutdown()
