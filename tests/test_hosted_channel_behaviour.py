"""Hosted chat providers other than Slack, and the behaviour of a connection.

A second provider, ``fixture_chat``, joins Slack in the table of hosted chat
providers. It has no config section and no settings of its own. The harness
is the one of ``test_hosted_channel``: the fake gateway against the real
stream endpoint, with a real router, session manager, and database, and a
stand-in for the agent turn.

Covered here: the table and its defaults, a switched-off provider that
refuses its events while Slack goes on, channels that use only the
connections of their own provider, the ``tool`` answer mode, in which the
router sends nothing to the conversation and the turn's text starts with a
note for the agent, the ``keep`` session mode, in which a conversation
keeps its last session past the sticky period, and simultaneous first
messages of one conversation. The router's part is also tested on its own,
with a channel that records every call.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any

from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from nerve.agent.sessions import SessionManager
from nerve.agent.streaming import broadcaster
from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    ChannelConstraints,
    InboundMessage,
    OutboundMessage,
)
from nerve.channels.hosted.channel import answer_note
from nerve.channels.hosted import providers as providers_module
from nerve.channels.hosted.providers import (
    HOSTED_CHAT_PROVIDERS,
    SLACK,
    HostedChannelSettings,
    HostedChatProvider,
)
from nerve.channels.hosted.runtime import hosted_channel_providers
from nerve.channels.router import ChannelRouter
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


# ---------------------------------------------------------------------- #
#  The answer mode                                                        #
# ---------------------------------------------------------------------- #


class StreamingTurns:
    """An agent turn that streams an answer, or an error, into its session."""

    def __init__(self, *, error: str | None = None) -> None:
        self.runs: list[dict[str, Any]] = []
        self.error = error

    async def run(self, **kwargs: Any) -> str:
        self.runs.append(kwargs)
        session_id = kwargs["session_id"]
        if self.error is not None:
            await broadcaster.broadcast_error(session_id, self.error)
            return ""
        for word in ("The ", "report ", "is ", "fine."):
            await broadcaster.broadcast_token(session_id, word)
        await broadcaster.broadcast_done(session_id)
        return "The report is fine."

    async def wait_for(self, count: int) -> None:
        for _ in range(500):
            if len(self.runs) >= count:
                return
            await asyncio.sleep(0.01)
        raise AssertionError(f"expected {count} turns, saw {len(self.runs)}")


async def turn_of(hosted: Hosted, stream, event: dict[str, Any], turns: StreamingTurns) -> dict[str, Any]:
    """Deliver *event*, wait for its turn, and give the turn's settled output time to arrive."""
    hosted.router.engine.run = turns.run
    inbox_id = hosted.gateway.store(event)
    await stream.nudge("invoke")
    assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
    await turns.wait_for(len(turns.runs) + 1)
    await asyncio.sleep(0.3)
    return turns.runs[-1]


def kinds(hosted: Hosted) -> list[str]:
    return [record["kind"] for record in hosted.gateway.operations]


@pytest.mark.asyncio
class TestToolAnswer:
    async def test_the_turn_posts_nothing_and_its_text_starts_with_the_note(self, two):
        stream = await two.stream(advertise=False)
        await announce(stream, CHAT, CHAT_CONNECTION, answer="tool", reply_tool="send_reply", session="rotate")
        turns = StreamingTurns()

        run = await turn_of(two, stream, chat_event(text="Please check the report."), turns)

        assert kinds(two) == []
        assert run["user_message"] == (
            "This message came through fixture_chat. Your answer in this session is not sent to anyone. "
            "To answer, use the send_reply tool.\n\nPlease check the report."
        )
        mapping = await two.db.get_channel_session(f"{CHAT}:room-1")
        assert run["session_id"] == mapping["session_id"]

    async def test_without_a_reply_tool_the_note_says_that_nothing_answers(self, two):
        stream = await two.stream(advertise=False)
        await announce(stream, CHAT, CHAT_CONNECTION, answer="tool")

        run = await turn_of(two, stream, chat_event(text="FYI"), StreamingTurns())

        assert kinds(two) == []
        assert run["user_message"] == f"{answer_note(CHAT)}\n\nFYI"
        assert answer_note(CHAT).endswith("This channel does not send answers.")

    async def test_an_error_of_the_turn_is_not_posted(self, two):
        stream = await two.stream(advertise=False)
        await announce(stream, CHAT, CHAT_CONNECTION, answer="tool")

        await turn_of(two, stream, chat_event(), StreamingTurns(error="the model is unavailable"))

        assert kinds(two) == []

    async def test_the_same_provider_with_the_post_mode_posts_the_answer(self, two):
        stream = await two.stream(advertise=False)
        await announce(stream, CHAT, CHAT_CONNECTION, answer="post", session="rotate")

        run = await turn_of(two, stream, chat_event(text="Please check the report."), StreamingTurns())

        assert run["user_message"] == "Please check the report."
        assert "send" in kinds(two) and "reaction" in kinds(two)
        final = two.gateway.sent_operations("send")[-1]
        assert final["content"][0]["text"]["body"] == "The report is fine."

    @pytest.mark.parametrize("behaviour", [None, SLACK_BEHAVIOUR], ids=["absent", "explicit"])
    async def test_slack_posts_its_answer_as_before(self, two, behaviour):
        stream = await two.stream(advertise=False)
        await announce(stream, "slack", CONNECTION_ID, **(behaviour or {}))

        run = await turn_of(two, stream, slack_event(text="how are the deploys?"), StreamingTurns())

        assert run["user_message"] == "how are the deploys?"
        assert {"reaction", "send", "delete"} <= set(kinds(two))
        assert two.gateway.sent_operations("send")[-1]["content"][0]["text"]["body"] == "The report is fine."

    async def test_a_connection_without_capabilities_posts_its_answer(self, two):
        stream = await two.stream(advertise=False)

        run = await turn_of(two, stream, chat_event(text="hello"), StreamingTurns())

        assert run["user_message"] == "hello"


class RecordingChannel(BaseChannel):
    """A channel that streams and shows typing, and records every call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    @property
    def name(self) -> str:
        return "recording"

    @property
    def capabilities(self) -> ChannelCapability:
        return ChannelCapability.SEND_TEXT | ChannelCapability.STREAMING | ChannelCapability.TYPING_INDICATOR

    @property
    def constraints(self) -> ChannelConstraints:
        return ChannelConstraints(max_message_length=4000, supports_message_edit=True)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, message: OutboundMessage) -> None:
        self.calls.append(("send", message.text))

    async def send_placeholder(self, target: str, session_id: str) -> str | None:
        self.calls.append(("placeholder", target))
        return "placeholder-1"

    async def edit_message(self, target: str, message_id: str, text: str, *, throttle: bool = False) -> None:
        self.calls.append(("edit", text))

    async def delete_message(self, target: str, message_id: str) -> None:
        self.calls.append(("delete", message_id))

    async def send_typing(self, target: str) -> None:
        self.calls.append(("typing", target))


def recording_router(db, turns: StreamingTurns) -> tuple[ChannelRouter, RecordingChannel]:
    engine = MagicMock()
    engine.db = db
    engine.sessions = SessionManager(db)
    engine.run = turns.run
    router = ChannelRouter(engine)
    router.BATCH_DEBOUNCE = 0.0
    channel = RecordingChannel()
    router.register(channel)
    return router, channel


def inbound(text: str = "hello", *, post_answer: bool = True) -> InboundMessage:
    return InboundMessage(
        channel_name="recording", channel_key="recording:room-1", sender_id="room-1", text=text,
        post_answer=post_answer,
    )


@pytest.mark.asyncio
class TestRouterAnswerMode:
    async def test_a_turn_that_posts_shows_typing_streams_and_sends(self, db):
        router, channel = recording_router(db, StreamingTurns())

        assert await router.handle_message(inbound()) == "The report is fine."

        assert {"typing", "placeholder", "send", "delete"} <= {call[0] for call in channel.calls}

    @pytest.mark.parametrize("error", [None, "the model is unavailable"], ids=["answer", "error"])
    async def test_a_turn_that_does_not_post_sends_nothing_to_the_channel(self, db, error):
        turns = StreamingTurns(error=error)
        router, channel = recording_router(db, turns)

        await router.handle_message(inbound(post_answer=False))

        assert channel.calls == []
        [run] = turns.runs
        assert run["user_message"] == "hello"
        assert await db.get_channel_session("recording:room-1") is not None

    async def test_a_batch_posts_only_when_every_message_asks_for_it(self, db):
        turns = StreamingTurns()
        router, channel = recording_router(db, turns)
        router.BATCH_DEBOUNCE = 0.1
        await router.create_session("recording:room-1", source="recording")

        await asyncio.gather(
            router.handle_message(inbound("first", post_answer=False)),
            router.handle_message(inbound("second")),
        )

        assert channel.calls == []
        [run] = turns.runs
        assert run["user_message"] == "first\n\nsecond"


# ---------------------------------------------------------------------- #
#  The session mode                                                       #
# ---------------------------------------------------------------------- #


# Long before any sticky period.
LONG_AGO = "2020-01-01T00:00:00+00:00"


async def idle_session(hosted: Hosted, channel_key: str, *, status: str = "idle") -> str:
    """The conversation's session, idle since long ago."""
    session_id = await hosted.router.create_session(channel_key, source=CHAT)
    await hosted.db.update_session_fields(session_id, {"status": status, "last_activity_at": LONG_AGO})
    return session_id


@pytest.mark.asyncio
class TestKeepSession:
    async def test_keep_continues_the_last_session_however_long_it_was_idle(self, two):
        old = await idle_session(two, f"{CHAT}:room-1")
        stream = await two.stream(advertise=False)
        await announce(stream, CHAT, CHAT_CONNECTION, answer="tool", session="keep")

        run = await turn_of(two, stream, chat_event(), StreamingTurns())

        assert run["session_id"] == old
        assert (await two.db.get_channel_session(f"{CHAT}:room-1"))["session_id"] == old

    @pytest.mark.parametrize("behaviour", [{"session": "rotate"}, {}], ids=["rotate", "absent"])
    async def test_rotate_starts_a_new_session_after_the_sticky_period(self, two, behaviour):
        old = await idle_session(two, f"{CHAT}:room-1")
        stream = await two.stream(advertise=False)
        await announce(stream, CHAT, CHAT_CONNECTION, answer="tool", **behaviour)

        run = await turn_of(two, stream, chat_event(), StreamingTurns())

        assert run["session_id"] != old
        assert (await two.db.get_channel_session(f"{CHAT}:room-1"))["session_id"] == run["session_id"]

    async def test_keep_starts_a_new_session_when_the_last_one_is_archived(self, two):
        old = await idle_session(two, f"{CHAT}:room-1", status="archived")
        stream = await two.stream(advertise=False)
        await announce(stream, CHAT, CHAT_CONNECTION, answer="tool", session="keep")

        run = await turn_of(two, stream, chat_event(), StreamingTurns())

        assert run["session_id"] != old

    async def test_keep_starts_the_first_session_and_keeps_it_for_the_next_message(self, two):
        stream = await two.stream(advertise=False)
        await announce(stream, CHAT, CHAT_CONNECTION, answer="tool", session="keep")
        turns = StreamingTurns()

        first = await turn_of(two, stream, chat_event(message_id="m-1"), turns)
        await two.db.update_session_fields(first["session_id"], {"status": "idle", "last_activity_at": LONG_AGO})
        second = await turn_of(two, stream, chat_event(message_id="m-2"), turns)

        assert second["session_id"] == first["session_id"]
        assert [run["channel"] for run in turns.runs] == [CHAT, CHAT]

    async def test_keep_holds_each_conversation_to_its_own_session(self, two):
        first = await idle_session(two, f"{CHAT}:room-1")
        second = await idle_session(two, f"{CHAT}:room-2")
        stream = await two.stream(advertise=False)
        await announce(stream, CHAT, CHAT_CONNECTION, answer="tool", session="keep")
        turns = StreamingTurns()

        one = await turn_of(two, stream, chat_event(conversation="room-2", message_id="m-3"), turns)
        other = await turn_of(two, stream, chat_event(conversation="room-1", message_id="m-4"), turns)

        assert (one["session_id"], other["session_id"]) == (second, first)


# ---------------------------------------------------------------------- #
#  Simultaneous first messages                                            #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestSimultaneousFirstMessages:
    async def test_two_first_messages_of_one_conversation_share_one_session(self, two):
        stream = await two.stream(advertise=False)
        await announce(stream, CHAT, CHAT_CONNECTION, answer="tool", session="keep")
        await two.gateway.wait_for(lambda: two.runtime.streams.connections("send", CHAT))
        turns = StreamingTurns()
        two.router.engine.run = turns.run
        first = two.gateway.store(chat_event(message_id="m-1", text="first"))
        second = two.gateway.store(chat_event(message_id="m-2", text="second"))

        await stream.nudge("invoke")

        assert await two.acknowledged(first, second) == {first: "accepted", second: "accepted"}
        await turns.wait_for(1)
        await asyncio.sleep(0.3)
        mapping = await two.db.get_channel_session(f"{CHAT}:room-1")
        assert {run["session_id"] for run in turns.runs} == {mapping["session_id"]}
        said = "\n\n".join(run["user_message"] for run in turns.runs)
        assert said.index("first") < said.index("second")

    async def test_the_router_resolves_one_session_for_simultaneous_messages(self, db):
        turns = StreamingTurns()
        router, _ = recording_router(db, turns)

        await asyncio.gather(*(router.handle_message(inbound(f"message {n}")) for n in range(3)))

        mapping = await db.get_channel_session("recording:room-1")
        assert {run["session_id"] for run in turns.runs} == {mapping["session_id"]}

