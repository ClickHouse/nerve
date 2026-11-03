"""Hosted chat providers other than Slack, and the behaviour of a connection.

A second provider, ``fixture_chat``, joins Slack in the table of hosted chat
providers. It has no config section and no settings of its own. The harness
is the one of ``test_hosted_channel``: the fake gateway against the real
stream endpoint, with a real router, session manager, and database, and a
stand-in for the agent turn.

Covered here: the table and its defaults, a switched-off provider that
refuses its events while Slack goes on, and channels that use only the
connections of their own provider.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio

from nerve.channels.base import ChannelCapability, OutboundMessage
from nerve.channels.hosted import providers as providers_module
from nerve.channels.hosted.providers import (
    HOSTED_CHAT_PROVIDERS,
    SLACK,
    HostedChannelSettings,
    HostedChatProvider,
)
from nerve.channels.hosted.runtime import hosted_channel_providers
from nerve.config import NerveConfig, SlackConfig

from tests.fake_channel_gateway import CONNECTION_ID, message_event
from tests.test_hosted_channel import Hosted, start_hosted, stop_hosted

CHAT = "fixture_chat"
CHAT_CONNECTION = "0d7c4b1e-6a3f-4e92-8b5d-2f1a9c7e3b60"
SLACK_BEHAVIOUR = {"answer": "post", "session": "rotate"}
OPERATIONS = ["send", "edit", "delete", "reaction", "notify", "file_send"]


@dataclass
class Switch:
    """The switch of the test provider, which a test turns as a reload would."""

    on: bool = True
    hosted: bool = True


def chat_provider(switch: Switch, **fields: Any) -> HostedChatProvider:
    """A provider without a config section: only a name, a mode, and a switch."""
    return HostedChatProvider(
        name=CHAT,
        hosted=lambda config: switch.hosted,
        switched_on=lambda config: switch.on,
        **fields,
    )


def chat_event(**changes: Any) -> dict[str, Any]:
    """A direct message of the test provider on its own connection."""
    values: dict[str, Any] = {
        "provider": CHAT, "conversation": "room-1", "conversation_kind": "direct",
        "message_id": "m-1", "connection_id": CHAT_CONNECTION, "author": "member-1",
    }
    values.update(changes)
    return message_event(**values)


def slack_event(message_id: str = "1700000100.000100", **changes: Any) -> dict[str, Any]:
    return message_event(conversation="D0DIRECT", conversation_kind="direct", message_id=message_id, **changes)


async def start_two(tmp_path, db, monkeypatch, switch: Switch, **fields: Any) -> Hosted:
    """The hosted harness with Slack and the test provider in the table."""
    monkeypatch.setattr(providers_module, "HOSTED_CHAT_PROVIDERS", (SLACK, chat_provider(switch, **fields)))
    return await start_hosted(tmp_path, db, monkeypatch)


@pytest.fixture
def switch() -> Switch:
    return Switch()


@pytest_asyncio.fixture
async def two(tmp_path, db, monkeypatch, switch):
    harness = await start_two(tmp_path, db, monkeypatch, switch)
    try:
        yield harness
    finally:
        await stop_hosted(harness)


async def announce(stream, provider: str | None, connection_id: str, **behaviour: str) -> None:
    await stream.advertise(
        connection_id=connection_id, provider=provider, behaviour=behaviour or None, operations=OPERATIONS,
    )


# ---------------------------------------------------------------------- #
#  The table of hosted chat providers                                     #
# ---------------------------------------------------------------------- #


class TestProviderTable:
    def test_slack_is_the_only_provider_and_holds_its_events_while_off(self):
        assert HOSTED_CHAT_PROVIDERS == (SLACK,)
        assert SLACK.hold_while_off

    def test_a_provider_is_hosted_as_its_config_says(self):
        config = NerveConfig()
        assert hosted_channel_providers(config) == []
        config.slack = SlackConfig(mode="hosted", enabled=False)
        assert hosted_channel_providers(config) == ["slack"]

    def test_slack_settings_come_from_its_section(self):
        config = NerveConfig()
        config.slack = SlackConfig(
            mode="hosted", enabled=True, stream_mode="full", allow_outbound=True,
            allow_channels=["C0123ABCD"], deny_channels=["C0DENIED"],
        )

        settings = SLACK.settings(config)

        assert (settings.stream_mode, settings.allow_outbound) == ("full", True)
        assert (settings.allow_channels, settings.deny_channels) == (("C0123ABCD",), ("C0DENIED",))
        assert settings.source is config.slack.source
        assert SLACK.switched_on(config)
        config.slack.enabled = False
        assert not SLACK.switched_on(config)

    def test_a_new_provider_refuses_its_events_while_off_and_has_default_settings(self, switch):
        provider = chat_provider(switch)

        assert not provider.hold_while_off
        assert provider.settings(NerveConfig()) == HostedChannelSettings()
        assert provider.channel_options() == {}


@pytest.mark.asyncio
class TestProviderWithoutSettings:
    async def test_its_channel_takes_a_turn_with_the_default_settings(self, two):
        stream = await two.stream()
        inbox_id = two.gateway.store(chat_event(text="hello from the test provider"))

        await stream.nudge("invoke")

        assert (await two.acknowledged(inbox_id))[inbox_id] == "accepted"
        await two.turns.wait_for(1)
        mapping = await two.db.get_channel_session(f"{CHAT}:room-1")
        run = two.turns.runs[0]
        assert (run["session_id"], run["channel"], run["user_message"]) == (
            mapping["session_id"], CHAT, "hello from the test provider",
        )
        channel = two.runtime.channels[CHAT]
        assert two.router.get_channel(CHAT) is channel
        assert ChannelCapability.STREAMING in channel.capabilities

    async def test_it_refuses_addressed_delivery_and_observations(self, two):
        stream = await two.stream()
        channel = two.runtime.channels[CHAT]
        inbox_id = two.gateway.store(chat_event(purpose="observe", conversation_kind="channel"))

        await stream.nudge("observe")

        assert (await two.acknowledged(inbox_id))[inbox_id] == "rejected"
        assert two.gateway.acks[-1][0]["reason_code"] == "policy_denied"
        verdict = await channel.authorize_outbound("room-1")
        assert not verdict.allowed
        assert "allow_outbound" in verdict.reason

    async def test_switched_off_it_refuses_its_events_and_slack_goes_on(self, two, switch):
        switch.on = False
        stream = await two.stream()
        refused = two.gateway.store(chat_event())
        taken = two.gateway.store(slack_event())

        await stream.nudge("invoke")

        assert await two.acknowledged(refused, taken) == {refused: "rejected", taken: "accepted"}
        assert two.gateway.acks[-1][0]["reason_code"] == "configuration_invalid"
        await two.turns.wait_for(1)
        assert [run["channel"] for run in two.turns.runs] == ["slack"]
        assert two.runtime.channels[CHAT].can_accept()

        switch.on = True
        again = two.gateway.store(chat_event(message_id="m-2"))
        await stream.nudge("invoke")
        assert (await two.acknowledged(again))[again] == "accepted"

    async def test_a_switched_off_slack_holds_its_events_and_the_test_providers_too(self, two):
        two.config.slack.enabled = False
        stream = await two.stream()
        held = two.gateway.store(chat_event())

        await stream.nudge("invoke")
        await asyncio.sleep(0.3)

        assert two.gateway.reads == []
        two.config.slack.enabled = True
        assert (await two.acknowledged(held))[held] == "accepted"

    async def test_a_provider_that_the_config_does_not_host_has_no_channel(self, tmp_path, db, monkeypatch):
        harness = await start_two(tmp_path, db, monkeypatch, Switch(hosted=False))
        try:
            stream = await harness.stream()
            refused = harness.gateway.store(chat_event())
            taken = harness.gateway.store(slack_event())

            await stream.nudge("invoke")

            assert await harness.acknowledged(refused, taken) == {refused: "rejected", taken: "accepted"}
            assert harness.gateway.acks[-1][0]["reason_code"] == "configuration_invalid"
            assert list(harness.runtime.channels) == ["slack"]
        finally:
            await stop_hosted(harness)


# ---------------------------------------------------------------------- #
#  Connections of a provider                                              #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestProviderScoping:
    async def test_each_channel_uses_only_the_connections_of_its_provider(self, two):
        two.config.slack.allow_outbound = True
        stream = await two.stream(advertise=False)
        await announce(stream, "slack", CONNECTION_ID, **SLACK_BEHAVIOUR)
        await stream.advertise(
            connection_id=CHAT_CONNECTION, provider=CHAT, operations=["send"], text_characters=500,
        )
        streams = two.runtime.streams
        await two.gateway.wait_for(lambda: len(streams.connections("send")) == 2)
        slack, chat = two.runtime.channels["slack"], two.runtime.channels[CHAT]

        assert streams.connections("send", "slack") == {uuid.UUID(CONNECTION_ID)}
        assert streams.connections("send", CHAT) == {uuid.UUID(CHAT_CONNECTION)}
        assert (slack.constraints.max_message_length, chat.constraints.max_message_length) == (4000, 500)
        assert slack.constraints.supports_message_edit and not chat.constraints.supports_message_edit
        assert slack.is_available and chat.is_available
        assert (await slack.authorize_outbound("C0123ABCD")).allowed

        await slack.send(OutboundMessage(target="C0123ABCD", text="to slack"))
        await chat.send(OutboundMessage(target="room-9", text="to the test provider"))
        assert [(record["connection_id"], record["kind"]) for record in two.gateway.operations] == [
            (CONNECTION_ID, "send"), (CHAT_CONNECTION, "send"),
        ]

    async def test_notify_goes_on_the_only_connection_of_the_provider(self, two):
        stream = await two.stream(advertise=False)
        await announce(stream, "slack", CONNECTION_ID, **SLACK_BEHAVIOUR)
        await announce(stream, CHAT, CHAT_CONNECTION)
        await two.gateway.wait_for(lambda: len(two.runtime.streams.connections("notify")) == 2)

        posted = await two.runtime.channels["slack"].post_notification("n-1", "Deploy now?", [("Yes", "yes")])

        assert posted is not None
        [record] = [record for record in two.gateway.operations if record["kind"] == "notify"]
        assert record["connection_id"] == CONNECTION_ID

    async def test_a_connection_that_names_no_provider_serves_every_channel(self, two):
        stream = await two.stream(advertise=False)
        await announce(stream, None, CONNECTION_ID)
        await two.gateway.wait_for(lambda: two.runtime.streams.connections("send"))

        assert two.runtime.streams.connections("send", "slack") == {uuid.UUID(CONNECTION_ID)}
        assert two.runtime.streams.connections("send", CHAT) == {uuid.UUID(CONNECTION_ID)}
        assert two.runtime.channels[CHAT].is_available

    async def test_a_connection_of_another_provider_does_not_make_a_channel_available(self, two):
        stream = await two.stream(advertise=False)
        await announce(stream, CHAT, CHAT_CONNECTION)
        await two.gateway.wait_for(lambda: two.runtime.streams.connections("send"))

        assert two.runtime.channels[CHAT].is_available
        assert not two.runtime.channels["slack"].is_available
        assert (await two.runtime.channels["slack"].post_notification("n-2", "Deploy?", [("Yes", "yes")])) is None
