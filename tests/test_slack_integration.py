"""Slack end-to-end over a real Socket Mode connection.

These drive the whole channel — connect, receive an envelope, ack it,
authorize, dispatch, reply — against :mod:`tests.fake_slack` rather than
mocks. What they cover that the unit tests cannot: the transport wiring,
the ack contract, and the shape of the calls actually put on the wire.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve.channels.slack import SlackChannel
from nerve.config import NerveConfig, SlackConfig
from tests.fake_slack import FakeSlack
from tests.slack_live import start_event_sink, wait_until_receiving


def _config(**slack_kwargs) -> NerveConfig:
    cfg = NerveConfig()
    cfg.slack = SlackConfig(
        enabled=True,
        bot_token="xoxb-fake",
        app_token="xapp-fake",
        **slack_kwargs,
    )
    return cfg


@pytest_asyncio.fixture
async def slack():
    async with FakeSlack() as server:
        yield server


async def _started(server: FakeSlack, monkeypatch, **slack_kwargs):
    """Bring a SlackChannel up against the fake, with a stub router."""
    cfg = _config(**slack_kwargs)
    router = MagicMock()
    router.handle_message = AsyncMock(return_value="ok")
    router.get_last_session = AsyncMock(return_value=None)
    channel = SlackChannel(lambda: cfg, router)
    server.patch_client(monkeypatch)
    await channel.start()
    await server.wait_connected()
    return channel, router


@pytest.mark.asyncio
class TestSocketMode:
    async def test_an_ack_only_sink_consumes_events_without_dispatching(
        self, slack, monkeypatch,
    ):
        slack.patch_client(monkeypatch)
        sink = await start_event_sink("xoxb-fake", "xapp-fake")
        try:
            class ProbeClient:
                posts = 0

                async def chat_postMessage(self, **kwargs):
                    self.posts += 1
                    ts = f"1.{self.posts}"
                    event = {
                        "type": "message", "channel": kwargs["channel"],
                        "channel_type": "channel", "user": "U0BOT",
                        "ts": ts, "text": kwargs["text"],
                    }
                    if self.posts == 2:
                        # The first probe missed the immediate attempts and
                        # arrives only as a retry. Readiness must ack it before
                        # returning, or its next retry poisons a later run.
                        await slack.push_event(
                            {**event, "ts": "1.1"},
                            retry_attempt=2,
                            retry_reason="timeout",
                        )
                        await slack.push_event(event)
                    return {"ok": True, "ts": ts}

                async def chat_delete(self, **kwargs):
                    await slack.push_event({
                        "type": "message", "subtype": "message_deleted",
                        "channel": kwargs["channel"], "ts": "2.1",
                        "deleted_ts": kwargs["ts"],
                    })
                    return {"ok": True}

            await wait_until_receiving(
                sink,
                timeout=2.0,
                web_client=ProbeClient(),
                probe_interval=0.05,
            )
            await slack.settle()
            assert len(slack.acks) == 4
        finally:
            await sink.close()

    async def test_the_channel_connects_and_learns_its_own_id(
        self, slack, monkeypatch,
    ):
        channel, _ = await _started(slack, monkeypatch, allow_users=["U1"])
        try:
            assert channel._bot_user_id == "U0BOT"
            assert slack.calls_to("auth.test")
            assert slack.calls_to("apps.connections.open")
        finally:
            await channel.stop()

    async def test_every_envelope_is_acked(self, slack, monkeypatch):
        # Slack redelivers anything unacked within three seconds, and an
        # agent turn is far longer than that.
        channel, router = await _started(slack, monkeypatch, allow_users=["U1"])
        try:
            envelope_id = await slack.push_event({
                "type": "message", "channel": "D1", "channel_type": "im",
                "user": "U1", "ts": "1.1", "text": "hello",
            })
            await slack.settle()
            assert envelope_id in slack.acks
            router.handle_message.assert_called_once()
        finally:
            await channel.stop()

    async def test_an_unauthorized_envelope_is_still_acked(
        self, slack, monkeypatch,
    ):
        # A refusal must not look like a delivery failure, or Slack retries
        # the same rejected message until it gives up.
        channel, router = await _started(slack, monkeypatch, allow_users=["U-other"])
        try:
            envelope_id = await slack.push_event({
                "type": "message", "channel": "D1", "channel_type": "im",
                "user": "U1", "ts": "1.1", "text": "hello",
            })
            await slack.settle()
            assert envelope_id in slack.acks
            router.handle_message.assert_not_called()
        finally:
            await channel.stop()

    async def test_stop_closes_the_socket(self, slack, monkeypatch):
        channel, _ = await _started(slack, monkeypatch, allow_users=["U1"])
        await channel.stop()
        assert not await channel._client.is_connected()


@pytest.mark.asyncio
class TestConversation:
    async def test_a_direct_message_produces_a_reply_in_the_dm(
        self, slack, monkeypatch,
    ):
        channel, router = await _started(slack, monkeypatch, allow_users=["U1"])
        try:
            async def _reply(msg):
                from nerve.channels.base import OutboundMessage
                await channel.send(
                    OutboundMessage(target=msg.sender_id, text="**done**"),
                )
                return "done"

            router.handle_message = AsyncMock(side_effect=_reply)
            await slack.push_event({
                "type": "message", "channel": "D1", "channel_type": "im",
                "user": "U1", "ts": "1.1", "text": "run it",
            })
            posted = await slack.wait_for("chat.postMessage")
            assert posted[0]["channel"] == "D1"
            assert posted[0]["text"] == "*done*"
        finally:
            await channel.stop()

    async def test_a_channel_mention_replies_inside_a_thread(
        self, slack, monkeypatch,
    ):
        channel, router = await _started(slack, monkeypatch, allow_users=["U1"])
        try:
            captured = {}

            async def _capture(msg):
                captured["key"] = msg.channel_key
                from nerve.channels.base import OutboundMessage
                await channel.send(OutboundMessage(target=msg.sender_id, text="hi"))
                return "hi"

            router.handle_message = AsyncMock(side_effect=_capture)
            await slack.push_event({
                "type": "app_mention", "channel": "C1", "channel_type": "channel",
                "user": "U1", "ts": "1.1", "text": "<@U0BOT> status",
            })
            posted = await slack.wait_for("chat.postMessage")
            assert captured["key"] == "slack:C1:1.1"
            assert posted[0]["thread_ts"] == "1.1"
        finally:
            await channel.stop()

    async def test_a_name_allow_list_resolves_through_the_api(
        self, slack, monkeypatch,
    ):
        slack.users["U1"] = {
            "id": "U1", "name": "alex.soffronow",
            "profile": {"email": "alex@example.com"},
        }
        channel, router = await _started(
            slack, monkeypatch, allow_users=["alex.soffronow"],
        )
        try:
            await slack.push_event({
                "type": "message", "channel": "D1", "channel_type": "im",
                "user": "U1", "ts": "1.1", "text": "hello",
            })
            await slack.settle()
            router.handle_message.assert_called_once()
            assert slack.calls_to("users.info")
        finally:
            await channel.stop()

    async def test_a_channel_glob_is_checked_against_the_real_name(
        self, slack, monkeypatch,
    ):
        slack.conversations["C1"] = {"id": "C1", "name": "eng-platform"}
        slack.conversations["C2"] = {"id": "C2", "name": "random"}
        channel, router = await _started(
            slack, monkeypatch, allow_users=["U1"], allow_channels=["eng-*"],
        )
        try:
            await slack.push_event({
                "type": "app_mention", "channel": "C2", "channel_type": "channel",
                "user": "U1", "ts": "1.1", "text": "<@U0BOT> hi",
            })
            await slack.settle()
            router.handle_message.assert_not_called()

            await slack.push_event({
                "type": "app_mention", "channel": "C1", "channel_type": "channel",
                "user": "U1", "ts": "2.1", "text": "<@U0BOT> hi",
            })
            await slack.settle()
            router.handle_message.assert_called_once()
        finally:
            await channel.stop()

    async def test_a_denied_lookup_refuses_rather_than_guesses(
        self, slack, monkeypatch,
    ):
        # users.info fails, and a deny list cannot be evaluated without it.
        slack.errors["users.info"] = "missing_scope"
        channel, router = await _started(
            slack, monkeypatch, allow_users=["U1"], deny_users=["*-bot"],
        )
        try:
            await slack.push_event({
                "type": "message", "channel": "D1", "channel_type": "im",
                "user": "U1", "ts": "1.1", "text": "hello",
            })
            await slack.settle()
            router.handle_message.assert_not_called()
        finally:
            await channel.stop()


@pytest.mark.asyncio
class TestStreaming:
    async def test_a_placeholder_is_posted_then_edited(self, slack, monkeypatch):
        channel, _ = await _started(slack, monkeypatch, allow_users=["U1"])
        try:
            message_id = await channel.send_placeholder("D1", "s1")
            assert message_id
            await channel.edit_message("D1", message_id, "partial **output**")
            edits = await slack.wait_for("chat.update")
            assert edits[0]["ts"] == message_id
            assert edits[0]["text"] == "partial *output*"
        finally:
            await channel.stop()
