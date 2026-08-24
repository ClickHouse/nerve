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

    async def test_credentials_rotate_on_the_running_channel(
        self, slack, monkeypatch,
    ):
        channel, router = await _started(
            slack, monkeypatch, allow_users=["U1"],
        )
        old_client = channel._client
        try:
            await channel.reload_credentials("xoxb-replaced", "xapp-replaced")

            assert channel._client is not old_client
            assert not await old_client.is_connected()
            assert await channel._client.is_connected()
            assert channel._web.token == "xoxb-replaced"
            assert channel._active_app_token == "xapp-replaced"

            await slack.push_event({
                "type": "message", "channel": "D1", "channel_type": "im",
                "user": "U1", "ts": "1.1", "text": "after rotation",
            })
            await slack.settle()
            router.handle_message.assert_called_once()
        finally:
            await channel.stop()

    @pytest.mark.parametrize(("failed_method", "bot_token", "app_token"), [
        ("auth.test", "xoxb-invalid", "xapp-replaced"),
        ("apps.connections.open", "xoxb-replaced", "xapp-invalid"),
    ])
    async def test_invalid_new_credentials_keep_the_old_connection(
        self, slack, monkeypatch, failed_method, bot_token, app_token,
    ):
        channel, _ = await _started(slack, monkeypatch, allow_users=["U1"])
        old_client = channel._client
        slack.errors[failed_method] = "invalid_auth"
        try:
            with pytest.raises(RuntimeError, match="token failed validation"):
                await channel.reload_credentials(bot_token, app_token)

            assert channel._client is old_client
            assert await old_client.is_connected()
            assert channel._active_bot_token == "xoxb-fake"
            assert channel._active_app_token == "xapp-fake"
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
