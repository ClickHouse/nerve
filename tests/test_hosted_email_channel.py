"""The email channel: the ``email`` setting, its provider entry, its
subscription, member mail over real streams, and ``nerve doctor``.

``fixtures/channel_contract`` holds copies of the channel contract's samples:
the ``email_message`` event as the gateway delivers member mail, the
``capabilities_behaviour`` frame that announces the email connection, and the
``subscriptions_email`` frame. Nerve must send that frame exactly, and must
take that event into a turn of the session of its conversation without
posting anything.

The end-to-end tests run the fake gateway against the real stream endpoint,
with a real router, session manager, and database; only the agent turn is a
stand-in, and it streams an answer into its session.
"""

from __future__ import annotations

import copy
import hashlib
import json
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio

from nerve import config_reload
from nerve.channels.hosted.contract import (
    Envelope,
    Event,
    Payload,
    Rejected,
    SourceFilters,
    SourceSubscription,
    Subscriptions,
    encode_envelope,
)
from nerve.channels.hosted.contract.wire import decode_record, dumps
from nerve.channels.hosted.providers import (
    EMAIL,
    HOSTED_CHAT_PROVIDERS,
    HostedChannelSettings,
    email_warning,
)
from nerve.channels.hosted.runtime import hosted_channel_providers, hosted_providers
from nerve.channels.hosted.sources import chat_subscription
from nerve.cli import doctor_report
from nerve.config import (
    EmailConfig,
    GitHubSyncConfig,
    NerveConfig,
    SlackConfig,
    validate_config_keys,
)
from tests.fake_channel_gateway import CONNECTION_ID, FakeChannelGateway, message_event
from tests.test_hosted_channel import Hosted, start_hosted, stop_hosted
from tests.test_hosted_channel_behaviour import StreamingTurns, kinds, turn_of
from tests.test_hosted_channel_config import HOSTED
from tests.test_hosted_sources import start, stop

FIXTURES = Path(__file__).parent / "fixtures" / "channel_contract"


def fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


EVENT = fixture("email_message.json")
FRAME = fixture("subscriptions_email.json")
ANNOUNCEMENT = fixture("capabilities_behaviour.json")
EMAIL_CONNECTION = uuid.UUID(ANNOUNCEMENT["connection_id"])
CONVERSATION = EVENT["conversation"]["id"]
CHANNEL_KEY = f"email:{CONVERSATION}"
MAIL_TEXT = EVENT["content"][0]["text"]["body"]
# The line that Nerve puts before the mail, from the announced behaviour.
NOTE = (
    "This message came through email. Your answer in this session is not sent to anyone. "
    "To answer, use the mcp__email__send tool."
)
# Long before any sticky period.
LONG_AGO = "2020-01-01T00:00:00+00:00"


def mail(number: int = 1, *, conversation: str = CONVERSATION) -> dict[str, Any]:
    """The contract's email event without its delivery, as the fake gateway
    stores it. Another *number* is another mail of the same thread, with its
    own Message-ID and event ID; another *conversation* is another thread."""
    event = copy.deepcopy(EVENT)
    del event["delivery"]
    if number != 1:
        receipt = f"0100019a2b3c4d5e-example-message-{number:04d}-000000"
        event["event_id"] = f"email:{receipt}"
        event["message_revision_id"] = receipt
        event["message"] = {"id": f"<reply-{number:04d}@mail.example.test>"}
    event["conversation"]["id"] = conversation
    return event


def hosted_config(**email: Any) -> NerveConfig:
    """A config with usable hosted settings and nothing else hosted."""
    config = NerveConfig.from_dict({"channels": {"hosted": HOSTED}})
    config.email = EmailConfig(**email)
    return config


async def announce(hosted: Hosted, stream) -> None:
    """Announce the Slack connection, then the email connection with the contract's frame."""
    await stream.advertise(
        connection_id=CONNECTION_ID, provider="slack", behaviour={"answer": "post", "session": "rotate"},
    )
    await stream.send(
        "capabilities", ANNOUNCEMENT["payload"]["capabilities"], connection_id=ANNOUNCEMENT["connection_id"],
    )
    await hosted.gateway.wait_for(
        lambda: hosted.runtime.streams.capabilities_for(EMAIL_CONNECTION) is not None,
    )


async def idle_session(hosted: Hosted, channel_key: str = CHANNEL_KEY) -> str:
    """The conversation's session, idle since long ago."""
    session_id = await hosted.router.create_session(channel_key, source="email")
    await hosted.db.update_session_fields(session_id, {"status": "idle", "last_activity_at": LONG_AGO})
    return session_id


def republisher(runtime) -> SimpleNamespace:
    """An engine that holds only the hosted runtime, for the reload step."""
    return SimpleNamespace(get_channel_runtime={"hosted": runtime}.get)


@pytest_asyncio.fixture
async def hosted(tmp_path, db, monkeypatch):
    """Hosted Slack and the email channel, with email switched on."""
    harness = await start_hosted(tmp_path, db, monkeypatch)
    harness.config.email.enabled = True
    try:
        yield harness
    finally:
        await stop_hosted(harness)


# ---------------------------------------------------------------------- #
#  The contract samples                                                   #
# ---------------------------------------------------------------------- #


# The SHA-256 of the contract's samples, which the copies must match byte
# for byte: testdata/events/email_message.json and
# testdata/frames/nerve/subscriptions_email.json.
SAMPLE_SHA256 = {
    "email_message.json": "3c03f271d08846824f3842f67b9b10bc68b5ca4c3ed49d24f825571d2fd5ddb1",
    "subscriptions_email.json": "e3a1b26391acc2785d127a0b9e275be9c1395f316478dfd6c1998adccc36e1b9",
}


class TestContractSamples:
    @pytest.mark.parametrize("name", sorted(SAMPLE_SHA256))
    def test_the_copy_of_the_contract_sample_has_its_exact_bytes(self, name):
        assert hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest() == SAMPLE_SHA256[name]

    def test_the_email_event_is_an_invocation_by_its_member(self):
        event = decode_record(Event, EVENT)
        event.validate()
        principal = event.admission.invoke.resolved_principal_id

        assert (event.provider, event.kind, event.connection_id) == ("email", "message", EMAIL_CONNECTION)
        assert (event.conversation.kind, len(event.conversation.id)) == ("direct", 64)
        assert event.author.id == str(principal) == str(event.author.linked_person.id)
        assert event.attachments == ()
        assert [part.text.format for part in event.content] == ["plain"]
        assert not {"issuer", "installation_id", "attachments", "extensions"} & set(EVENT)


# ---------------------------------------------------------------------- #
#  Settings and the provider entry                                        #
# ---------------------------------------------------------------------- #


class TestSettings:
    def test_email_is_off_by_default_and_the_setting_is_read(self):
        assert NerveConfig().email == EmailConfig() == EmailConfig.from_dict({})
        assert EmailConfig().enabled is False
        assert NerveConfig.from_dict({"email": {"enabled": "true"}}).email.enabled is True
        assert NerveConfig.from_dict({"email": None}).email.enabled is False
        assert validate_config_keys({"email": {"enabled": True}}) == []
        assert validate_config_keys({"email": {"enabeld": True}}) == [
            "unknown key 'email.enabeld' — it is ignored (typo or removed option?)",
        ]

    def test_email_is_a_hosted_chat_provider_that_refuses_while_off_and_subscribes(self):
        assert EMAIL in HOSTED_CHAT_PROVIDERS
        assert (EMAIL.name, EMAIL.hold_while_off, EMAIL.subscribes) == ("email", False, True)
        assert EMAIL.settings(hosted_config()) == HostedChannelSettings()
        assert EMAIL.channel_options() == {}

    def test_usable_hosted_settings_give_the_email_channel_also_while_email_is_off(self):
        config = hosted_config()

        assert hosted_channel_providers(config) == hosted_providers(config) == ["email"]
        assert not EMAIL.switched_on(config)
        config.email.enabled = True
        assert EMAIL.switched_on(config)
        config.slack = SlackConfig(mode="hosted")
        config.sync.github = GitHubSyncConfig(mode="hosted")
        assert hosted_providers(config) == ["slack", "email", "github", "github_repos"]

    @pytest.mark.parametrize("settings", [
        {},
        {**HOSTED, "gateway_jwks_file": "relative/gateway-jwks.json"},
        {**HOSTED, "agent_id": "not-a-uuid"},
    ], ids=["absent", "relative-key-file", "agent-id"])
    def test_without_usable_hosted_settings_there_is_no_email_channel(self, settings):
        config = NerveConfig.from_dict({"email": {"enabled": True}, "channels": {"hosted": settings}})

        assert hosted_providers(config) == []
        warning = email_warning(config)
        assert warning.startswith(
            "email.enabled is set, but mail comes only through the channel gateway and the "
            "channels.hosted settings are not usable (",
        )
        assert warning.endswith("); no email channel is registered")
        assert "; ".join(config.channels.hosted.problems()) in warning

    def test_a_switched_off_email_gives_no_warning(self):
        assert email_warning(NerveConfig()) is None
        assert email_warning(hosted_config()) is None
        assert email_warning(hosted_config(enabled=True)) is None

    def test_the_switch_needs_no_restart(self):
        off, on = hosted_config(), hosted_config(enabled=True)

        assert config_reload.restart_required(off, on) == []
        assert config_reload.restart_required(on, off) == []


# ---------------------------------------------------------------------- #
#  The subscription entry                                                 #
# ---------------------------------------------------------------------- #


class TestSubscriptionEntry:
    def test_the_entry_carries_the_provider_and_empty_filters_only(self):
        assert dumps(chat_subscription("email")) == '{"provider":"email","filters":{}}'

    @pytest.mark.parametrize("source", [
        SourceSubscription(provider="email", filters=SourceFilters(deny_actors=("someone",))),
        SourceSubscription(provider="email", pr_watch=True),
        SourceSubscription(provider="email", inbox="all"),
    ], ids=["filter", "pr-watch", "inbox"])
    def test_an_email_subscription_with_another_member_is_not_sent(self, source):
        """The gateway refuses it and closes the stream, so Nerve checks first."""
        with pytest.raises(Rejected) as refused:
            Subscriptions(version="1", sources=(source,)).validate()

        assert refused.value.reason == "malformed_frame"


# ---------------------------------------------------------------------- #
#  Member mail over real streams                                          #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestMemberMail:
    async def test_the_contract_event_starts_a_turn_in_its_conversation_and_posts_nothing(self, hosted):
        stream = await hosted.stream(advertise=False)
        await announce(hosted, stream)

        run = await turn_of(hosted, stream, mail(), StreamingTurns())

        mapping = await hosted.db.get_channel_session(CHANNEL_KEY)
        assert (run["session_id"], run["channel"], run["source"]) == (mapping["session_id"], "email", "email")
        assert run["user_message"] == f"{NOTE}\n\n{MAIL_TEXT}"
        assert run["actor"] is None
        assert kinds(hosted) == []
        assert hosted.gateway.acks[-1][0]["outcome"] == "accepted"

    async def test_the_conversation_keeps_its_session_past_the_sticky_period(self, hosted):
        old = await idle_session(hosted)
        stream = await hosted.stream(advertise=False)
        await announce(hosted, stream)
        turns = StreamingTurns()

        first = await turn_of(hosted, stream, mail(1), turns)
        await hosted.db.update_session_fields(old, {"status": "idle", "last_activity_at": LONG_AGO})
        second = await turn_of(hosted, stream, mail(2), turns)

        assert first["session_id"] == second["session_id"] == old
        assert (await hosted.db.get_channel_session(CHANNEL_KEY))["session_id"] == old
        assert kinds(hosted) == []

    async def test_another_thread_of_the_sender_has_its_own_session(self, hosted):
        stream = await hosted.stream(advertise=False)
        await announce(hosted, stream)
        turns = StreamingTurns()
        other = "0" * 64

        first = await turn_of(hosted, stream, mail(1), turns)
        second = await turn_of(hosted, stream, mail(2, conversation=other), turns)

        assert first["session_id"] != second["session_id"]
        assert (await hosted.db.get_channel_session(f"email:{other}"))["session_id"] == second["session_id"]

    async def test_while_email_is_off_its_mail_is_refused_and_slack_goes_on(self, hosted):
        hosted.config.email.enabled = False
        stream = await hosted.stream(advertise=False)
        await announce(hosted, stream)
        refused = hosted.gateway.store(mail())
        taken = hosted.gateway.store(message_event(conversation="D0DIRECT", conversation_kind="direct"))

        await stream.nudge("invoke")

        assert await hosted.acknowledged(refused, taken) == {refused: "rejected", taken: "accepted"}
        [rejection] = [item for ack in hosted.gateway.acks for item in ack if item["inbox_id"] == refused]
        assert rejection["reason_code"] == "configuration_invalid"
        await hosted.turns.wait_for(1)
        assert [run["channel"] for run in hosted.turns.runs] == ["slack"]
        assert await hosted.db.get_channel_session(CHANNEL_KEY) is None
        assert hosted.runtime.channels["email"].can_accept()

    async def test_the_agent_cannot_post_through_the_email_channel(self, hosted):
        verdict = await hosted.runtime.channels["email"].authorize_outbound(CONVERSATION)

        assert not verdict.allowed
        assert verdict.reason == "the email channel does not post to a conversation that the agent names"


# ---------------------------------------------------------------------- #
#  The subscriptions frame over real streams                              #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestSubscriptionsFrame:
    async def test_without_a_hosted_source_the_frame_is_the_contract_sample(self, hosted):
        assert hosted.runtime.sources == {}
        await hosted.stream()

        subscriptions = await self.first(hosted)

        assert subscriptions == FRAME["payload"]["subscriptions"]
        envelope = Envelope(
            version="1", kind="subscriptions", request_id=FRAME["request_id"],
            payload=Payload(subscriptions=hosted.runtime.subscriptions()),
        )
        assert json.loads(encode_envelope(envelope)) == FRAME

    async def test_while_email_is_off_the_frame_leaves_it_out(self, hosted):
        hosted.config.email.enabled = False
        await hosted.stream()

        assert await self.first(hosted) == {"version": "1"}

    async def test_a_reload_switches_email_on_and_off_at_the_gateway(self, hosted):
        hosted.config.email.enabled = False
        stream = await hosted.stream(advertise=False)
        await announce(hosted, stream)
        assert await self.first(hosted) == {"version": "1"}
        engine = republisher(hosted.runtime)

        hosted.config.email.enabled = True
        assert await config_reload._republish_hosted_subscriptions(engine) == "published on 1 stream(s)"
        await hosted.gateway.wait_for(lambda: len(hosted.gateway.subscriptions) == 2)
        assert hosted.gateway.subscriptions[-1] == FRAME["payload"]["subscriptions"]
        await turn_of(hosted, stream, mail(1), StreamingTurns())

        hosted.config.email.enabled = False
        assert await config_reload._republish_hosted_subscriptions(engine) == "published on 1 stream(s)"
        await hosted.gateway.wait_for(lambda: len(hosted.gateway.subscriptions) == 3)
        assert hosted.gateway.subscriptions[-1] == {"version": "1"}
        refused = hosted.gateway.store(mail(2))
        await stream.nudge("invoke")
        assert (await hosted.acknowledged(refused))[refused] == "rejected"

    async def test_a_switch_off_without_a_stream_is_not_sent_yet_and_reaches_the_next_stream(self, hosted):
        """The gateway keeps the email entry until a stream sends the frame,
        so the mail that it stores in the gap is rejected when Nerve reads it,
        as the switch asks."""
        first = await hosted.stream(advertise=False)
        assert await self.first(hosted) == FRAME["payload"]["subscriptions"]
        await first.close()
        await hosted.gateway.wait_for(lambda: not any(s.ready for s in hosted.runtime.streams.streams))
        hosted.config.email.enabled = False

        outcome = await config_reload._republish_hosted_subscriptions(republisher(hosted.runtime))

        assert outcome == NOT_SENT
        assert config_reload.reload_failures({"subscriptions": outcome}) == {"subscriptions": NOT_SENT[7:]}
        stored = hosted.gateway.store(mail())
        stream = await hosted.stream(advertise=False)
        await announce(hosted, stream)
        await stream.nudge("invoke")
        assert (await hosted.acknowledged(stored))[stored] == "rejected"
        assert hosted.gateway.subscriptions[-1] == {"version": "1"}
        assert sent_first(stream) == {"version": "1"}

    async def test_a_switch_on_without_a_stream_is_not_sent_yet_and_reaches_the_next_stream(self, hosted):
        hosted.config.email.enabled = False
        hosted.config.email.enabled = True

        outcome = await config_reload._republish_hosted_subscriptions(republisher(hosted.runtime))

        assert outcome == NOT_SENT
        stream = await hosted.stream(advertise=False)
        await announce(hosted, stream)
        await turn_of(hosted, stream, mail(), StreamingTurns())
        assert sent_first(stream) == FRAME["payload"]["subscriptions"]

    async def test_the_email_entry_follows_the_github_sources(self, tmp_path, db, monkeypatch):
        harness = await start(tmp_path, db, monkeypatch)
        try:
            harness.config.email.enabled = True
            assert set(harness.runtime.channels) == {"email"}
            await harness.stream()

            [subscriptions] = await harness.subscriptions()

            assert [source["provider"] for source in subscriptions["sources"]] == ["github", "email"]
            assert subscriptions["sources"][-1] == {"provider": "email", "filters": {}}
        finally:
            await stop(harness)

    @staticmethod
    async def first(hosted: Hosted) -> dict[str, Any]:
        await hosted.gateway.wait_for(lambda: bool(hosted.gateway.subscriptions))
        return hosted.gateway.subscriptions[0]


# What a reload reports when no gateway stream is open.
NOT_SENT = (
    "error: not sent yet: no gateway stream is open; the gateway gets them when a stream "
    "connects, before Nerve reads from it"
)


def sent_first(stream) -> dict[str, Any]:
    """The subscriptions that *stream* sent, which come before its first read."""
    kinds_sent = [frame["kind"] for frame in stream.frames if frame["kind"] != "heartbeat"]
    assert kinds_sent[:2] == ["negotiation", "subscriptions"]
    assert kinds_sent.index("subscriptions") < kinds_sent.index("inbox_read")
    [frame] = stream.sent("subscriptions")
    return frame["payload"]["subscriptions"]


# ---------------------------------------------------------------------- #
#  nerve doctor                                                           #
# ---------------------------------------------------------------------- #


def doctor_config(tmp_path: Path, **settings: Any) -> NerveConfig:
    gateway = FakeChannelGateway(jwks_file=tmp_path / "gateway-jwks.json")
    return NerveConfig.from_dict({
        "email": {"enabled": True},
        "channels": {"hosted": {**HOSTED, "gateway_jwks_file": str(gateway.jwks_file), **settings}},
    })


class TestDoctor:
    def test_email_from_the_gateway_is_reported(self, tmp_path):
        report = doctor_report(doctor_config(tmp_path))

        assert "[OK] Email from members comes from the channel gateway; Nerve posts no answer\n" in report

    def test_a_switched_off_email_shows_nothing(self, tmp_path):
        config = doctor_config(tmp_path)
        config.email.enabled = False

        assert "Email" not in doctor_report(config)

    def test_unusable_hosted_settings_are_a_warning(self, tmp_path):
        report = doctor_report(doctor_config(tmp_path, agent_id=""))

        assert "[WARN] email.enabled is set, but mail comes only through the channel gateway" in report
        assert "channels.hosted.agent_id must be a lowercase UUID" in report

    def test_an_unusable_key_file_is_an_error(self, tmp_path):
        report = doctor_report(doctor_config(tmp_path, gateway_jwks_file=str(tmp_path / "missing.json")))

        assert "[ERR] Email is on but channels.hosted.gateway_jwks_file: cannot read" in report

    @pytest.mark.parametrize(("slack", "warned"), [
        ({"mode": "hosted", "enabled": False}, True),
        ({"mode": "hosted"}, False),
        ({"mode": "socket", "enabled": False}, False),
    ])
    def test_a_switched_off_hosted_slack_is_said_to_hold_mail_back(self, tmp_path, slack, warned):
        config = doctor_config(tmp_path)
        config.slack = SlackConfig.from_dict(slack)

        report = doctor_report(config)

        assert ("so member mail waits there too" in report) is warned
