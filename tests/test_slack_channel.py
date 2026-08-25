"""Slack channel — formatting, addressing, dispatch, and guardrails.

Most of the surface is pure functions at module level, so they need no
transport. The event handlers are driven with a real SlackChannel whose
web client and router are stubs.
"""

from __future__ import annotations

import asyncio
import copy
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.channels.base import ChannelCapability, OutboundMessage
from nerve.channels.slack import (
    MAX_MSG_LEN,
    SlackChannel,
    SlackUnavailable,
    _md_to_slack,
    build_sessions_blocks,
    format_target,
    is_slack_id,
    parse_target,
    slack_emoji_name,
    slack_to_plain,
    split_message,
)
from nerve.channels.slack_presentation import build_notification_blocks
from nerve.config import NerveConfig, SlackConfig


def _config(**slack_kwargs) -> NerveConfig:
    cfg = NerveConfig()
    cfg.slack = SlackConfig(
        enabled=True,
        bot_token="xoxb-test",
        app_token="xapp-test",
        **slack_kwargs,
    )
    return cfg


def _channel(**slack_kwargs) -> SlackChannel:
    """A channel with a stub transport, ready to take events."""
    cfg = _config(**slack_kwargs)
    channel = SlackChannel(cfg, router=MagicMock())
    channel._web = MagicMock()
    channel._web.chat_postMessage = AsyncMock(return_value={"ts": "1.1"})
    channel._web.chat_update = AsyncMock(return_value={"ok": True})
    channel._web.chat_delete = AsyncMock(return_value={"ok": True})
    channel._web.reactions_add = AsyncMock(return_value={"ok": True})
    channel._state = "running"
    channel._bot_user_id = "U0BOT"
    channel.router.handle_message = AsyncMock(return_value="done")
    channel.router.get_last_session = AsyncMock(return_value=None)
    return channel


def _with_credentials(
    channel: SlackChannel,
    bot_token: str,
    app_token: str,
) -> NerveConfig:
    config = copy.deepcopy(channel.config)
    config.slack.bot_token = bot_token
    config.slack.app_token = app_token
    return config


# ---------------------------------------------------------------------- #
#  Addressing                                                             #
# ---------------------------------------------------------------------- #


class TestTargets:
    def test_a_channel_without_a_thread_round_trips(self):
        assert parse_target(format_target("C1")) == ("C1", None)

    def test_a_thread_round_trips(self):
        assert parse_target(format_target("C1", "169.9")) == ("C1", "169.9")

    def test_a_trailing_colon_is_not_a_thread(self):
        assert parse_target("C1:") == ("C1", None)


# ---------------------------------------------------------------------- #
#  Formatting                                                             #
# ---------------------------------------------------------------------- #


class TestMarkdownToSlack:
    def test_double_star_becomes_slack_bold(self):
        assert _md_to_slack("**bold**") == "*bold*"

    def test_single_star_becomes_slack_italic(self):
        assert _md_to_slack("*emphasis*") == "_emphasis_"

    def test_a_heading_becomes_a_bold_line_not_an_italic_one(self):
        # The heading rewrite emits a bold marker, which the italic pass
        # would otherwise consume and turn into _H_.
        assert _md_to_slack("## Heading") == "*Heading*"

    def test_code_spans_are_left_alone(self):
        assert _md_to_slack("`**kwargs`") == "`**kwargs`"

    def test_code_fences_are_left_alone(self):
        assert _md_to_slack("```\na = **b**\n```") == "```\na = **b**\n```"

    def test_links_become_slack_link_syntax(self):
        assert _md_to_slack("[docs](http://x/y)") == "<http://x/y|docs>"

    def test_reserved_characters_are_escaped(self):
        assert _md_to_slack("a < b & c > d") == "a &lt; b &amp; c &gt; d"

    def test_bullets_become_real_bullets(self):
        assert _md_to_slack("- one\n- two") == "• one\n• two"


class TestSlackToPlain:
    def test_the_bots_own_mention_is_dropped(self):
        assert slack_to_plain("<@U0BOT> hello", "U0BOT") == "hello"

    def test_a_channel_reference_reads_as_a_channel(self):
        assert slack_to_plain("see <#C1|general>") == "see #general"

    def test_a_link_keeps_both_label_and_url(self):
        assert slack_to_plain("<http://x|X>") == "X (http://x)"

    def test_broadcast_mentions_survive(self):
        assert slack_to_plain("<!here> ping") == "@here ping"

    def test_entities_are_unescaped(self):
        assert slack_to_plain("a &amp; b &lt;c&gt;") == "a & b <c>"


class TestSplitMessage:
    def test_short_text_is_one_chunk(self):
        assert split_message("hi", 100) == ["hi"]

    def test_empty_text_produces_nothing(self):
        assert split_message("", 100) == []

    def test_splitting_prefers_line_boundaries(self):
        assert split_message("aaaa\nbbbb\ncccc", 9) == ["aaaa\nbbbb", "cccc"]

    def test_an_overlong_single_line_is_cut(self):
        assert split_message("a" * 10, 4) == ["aaaa", "aaaa", "aa"]

    def test_every_chunk_respects_the_limit(self):
        text = "\n".join("line %d" % i for i in range(500))
        assert all(len(c) <= 40 for c in split_message(text, 40))

    def test_nothing_is_lost(self):
        text = "\n".join("line %d" % i for i in range(200))
        assert "\n".join(split_message(text, 40)) == text


class TestEmojiNames:
    def test_a_unicode_emoji_maps_to_a_short_name(self):
        assert slack_emoji_name("👍") == "thumbsup"

    def test_a_short_name_passes_through_without_colons(self):
        assert slack_emoji_name(":tada:") == "tada"

    def test_an_unmapped_emoji_is_refused_rather_than_guessed(self):
        assert slack_emoji_name("🫥") is None


# ---------------------------------------------------------------------- #
#  Block Kit                                                              #
# ---------------------------------------------------------------------- #


class TestSessionBlocks:
    def test_the_current_session_is_marked(self):
        blocks = build_sessions_blocks([{"id": "a1", "title": "Work"}], "a1")
        labels = [
            e["text"]["text"]
            for b in blocks if b["type"] == "actions" for e in b["elements"]
        ]
        assert any(label.startswith("✓ ") for label in labels)

    def test_a_starred_session_shows_a_filled_star(self):
        blocks = build_sessions_blocks(
            [{"id": "a1", "title": "Work", "starred": True}], None,
        )
        stars = [
            e["text"]["text"]
            for b in blocks if b["type"] == "actions" for e in b["elements"]
        ]
        assert "⭐" in stars

    def test_the_session_id_rides_in_the_action_id(self):
        blocks = build_sessions_blocks([{"id": "a1", "title": "W"}], None)
        ids = [
            e["action_id"]
            for b in blocks if b["type"] == "actions" for e in b["elements"]
        ]
        assert "sess:a1" in ids
        assert "sessstar:a1" in ids

    def test_an_empty_list_still_offers_a_new_session(self):
        blocks = build_sessions_blocks([], None)
        assert any(
            e["action_id"] == "sess:new"
            for b in blocks if b["type"] == "actions" for e in b["elements"]
        )

    def test_button_labels_stay_inside_slacks_limit(self):
        blocks = build_sessions_blocks([{"id": "a1", "title": "x" * 300}], None)
        labels = [
            e["text"]["text"]
            for b in blocks if b["type"] == "actions" for e in b["elements"]
        ]
        assert all(len(label) <= 75 for label in labels)


class TestNotificationBlocks:
    def test_a_plain_notification_has_no_buttons(self):
        blocks = build_notification_blocks("hi", "n1")
        assert all(b["type"] != "actions" for b in blocks)

    def test_the_notification_id_and_value_ride_on_the_button(self):
        blocks = build_notification_blocks(
            "Deploy?", "n1", [("✅ Approve", "approve")],
        )
        button = blocks[1]["elements"][0]
        assert button["action_id"] == "notif:n1:approve"
        assert button["value"] == "approve"

    def test_approval_decisions_are_colour_coded(self):
        blocks = build_notification_blocks(
            "Deploy?", "n1", [("Approve", "approve"), ("Decline", "decline")],
        )
        styles = [e.get("style") for e in blocks[1]["elements"]]
        assert styles == ["primary", "danger"]

    def test_section_text_stays_inside_slacks_limit(self):
        blocks = build_notification_blocks("x" * 5000, "n1")
        assert len(blocks[0]["text"]["text"]) <= 3000


# ---------------------------------------------------------------------- #
#  Channel wiring                                                         #
# ---------------------------------------------------------------------- #


class TestCapabilities:
    def test_partial_stream_mode_declares_streaming(self):
        assert ChannelCapability.STREAMING in _channel(stream_mode="partial").capabilities

    def test_full_stream_mode_does_not(self):
        assert ChannelCapability.STREAMING not in _channel(stream_mode="full").capabilities

    def test_constraints_match_slacks_edit_rate_limit(self):
        constraints = _channel().constraints
        assert constraints.supports_message_edit
        assert constraints.max_message_length == MAX_MSG_LEN
        assert constraints.min_edit_interval >= 1.0

    def test_the_policy_follows_a_config_reload(self):
        # The channel outlives a reload, so every guardrail is read per use.
        cfg = _config(allow_users=["U1"])
        channel = SlackChannel(cfg, router=MagicMock())
        assert channel.policy.users.allow == ["U1"]
        assert channel.policy.allow_direct_messages is False
        cfg.slack.allow_users = ["U2"]
        cfg.slack.allow_direct_messages = True
        assert channel.policy.users.allow == ["U2"]
        assert channel.policy.allow_direct_messages is True


class TestAuthorization:
    @pytest.mark.asyncio
    async def test_an_unconfigured_policy_refuses_without_calling_slack(self):
        channel = _channel()
        channel._web.users_info = AsyncMock()
        assert not await channel._authorize("U1", "D1", "im")
        channel._web.users_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_an_id_allow_list_needs_no_name_lookup(self):
        channel = _channel(
            allow_users=["U0123ABC"],
            allow_direct_messages=True,
        )
        channel._web.users_info = AsyncMock()
        assert await channel._authorize("U0123ABC", "D1", "im")
        channel._web.users_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_handle_allow_list_resolves_the_name(self):
        channel = _channel(allow_users=["alex"], allow_direct_messages=True)
        channel._web.users_info = AsyncMock(
            return_value={"user": {"name": "alex", "profile": {}}},
        )
        assert await channel._authorize("U1", "D1", "im")

    @pytest.mark.asyncio
    async def test_a_spoofed_profile_name_does_not_grant_access(self):
        # A member edits their own full name, so an allow list must not
        # grant on it: anyone could rename themselves onto the list.
        channel = _channel(
            allow_users=["alex.soffronow"], allow_direct_messages=True,
        )
        channel._web.users_info = AsyncMock(
            return_value={"user": {
                "name": "mallory",
                "profile": {
                    "real_name": "alex.soffronow",
                    "display_name": "alex.soffronow",
                },
            }},
        )
        assert not await channel._authorize("U-mallory", "D1", "im")

    @pytest.mark.asyncio
    async def test_a_deny_rule_still_matches_a_profile_name(self):
        channel = _channel(
            allow_users=["*"], deny_users=["*-bot"],
            allow_direct_messages=True,
        )
        channel._web.users_info = AsyncMock(
            return_value={"user": {
                "name": "integration-42",
                "profile": {"display_name": "deploy-bot", "email": "i@x.test"},
            }},
        )
        assert not await channel._authorize("U-int", "D1", "im")

    @pytest.mark.asyncio
    async def test_an_email_allow_list_grants(self):
        channel = _channel(
            allow_users=["alex@clickhouse.com"], allow_direct_messages=True,
        )
        channel._web.users_info = AsyncMock(
            return_value={"user": {
                "name": "alex",
                "profile": {"email": "alex@clickhouse.com"},
            }},
        )
        assert await channel._authorize("U1", "D1", "im")

    @pytest.mark.asyncio
    async def test_a_failed_lookup_with_a_deny_list_refuses(self):
        channel = _channel(
            allow_users=["U1"], deny_users=["*-bot"],
            allow_direct_messages=True,
        )
        channel._web.users_info = AsyncMock(side_effect=RuntimeError("no scope"))
        assert not await channel._authorize("U1", "D1", "im")

    @pytest.mark.asyncio
    async def test_resolved_names_are_cached(self):
        channel = _channel(allow_users=["alex"], allow_direct_messages=True)
        channel._web.users_info = AsyncMock(
            return_value={"user": {"name": "alex", "profile": {}}},
        )
        await channel._authorize("U1", "D1", "im")
        await channel._authorize("U1", "D1", "im")
        assert channel._web.users_info.await_count == 1

    @pytest.mark.asyncio
    async def test_direct_messages_are_refused_by_default(self):
        channel = _channel(allow_users=["U1"])
        channel._web.users_info = AsyncMock()
        assert not await channel._authorize("U1", "D1", "im")
        channel._web.users_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_direct_message_setting_allows_them(self):
        channel = _channel(
            allow_users=["U1"],
            allow_direct_messages=True,
        )
        assert await channel._authorize("U1", "D1", "im")

    @pytest.mark.asyncio
    async def test_dm_is_not_a_magic_channel_name(self):
        channel = _channel(allow_users=["U1"], allow_channels=["dm"])
        assert not await channel._authorize("U1", "D1", "im")


class TestMessageEvents:
    @pytest.mark.asyncio
    async def test_a_direct_message_reaches_the_router(self):
        channel = _channel(allow_users=["U1"], allow_direct_messages=True)
        await channel._handle_message_event({
            "type": "message", "channel": "D1", "channel_type": "im",
            "user": "U1", "ts": "1.1", "text": "hello",
        })
        msg = channel.router.handle_message.await_args[0][0]
        assert msg.channel_name == "slack"
        assert msg.text == "hello"
        assert msg.sender_id == "D1"
        assert msg.metadata["message_id"] == "1.1"

    @pytest.mark.asyncio
    async def test_an_unauthorized_sender_never_reaches_the_router(self):
        channel = _channel(
            allow_users=["U-other"],
            allow_direct_messages=True,
        )
        await channel._handle_message_event(
            {
                "type": "message",
                "channel": "D1",
                "channel_type": "im",
                "user": "U1",
                "ts": "1.1",
                "text": "hello",
            }
        )
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_bots_own_message_is_ignored(self):
        channel = _channel(
            allow_users=["U0BOT"],
            allow_direct_messages=True,
        )
        await channel._handle_message_event(
            {
                "type": "message",
                "channel": "D1",
                "channel_type": "im",
                "user": "U0BOT",
                "ts": "1.1",
                "text": "hi",
            }
        )
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_join_notice_is_ignored(self):
        channel = _channel(allow_users=["U1"])
        await channel._handle_message_event(
            {
                "type": "message",
                "subtype": "channel_join",
                "channel": "C1",
                "user": "U1",
                "ts": "1.1",
                "text": "joined",
            }
        )
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_channel_chatter_without_a_mention_is_ignored(self):
        # Adding the bot to a busy channel must not start a turn per remark.
        channel = _channel(allow_users=["U1"])
        await channel._handle_message_event(
            {
                "type": "message",
                "channel": "C1",
                "channel_type": "channel",
                "user": "U1",
                "ts": "1.1",
                "text": "morning all",
            }
        )
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_mention_in_a_channel_is_answered(self):
        channel = _channel(allow_users=["U1"])
        await channel._handle_message_event(
            {
                "type": "message",
                "channel": "C1",
                "channel_type": "channel",
                "user": "U1",
                "ts": "1.1",
                "text": "<@U0BOT> status?",
            }
        )
        msg = channel.router.handle_message.await_args[0][0]
        assert msg.text == "status?"

    @pytest.mark.asyncio
    async def test_a_channel_reply_opens_a_thread_on_the_message(self):
        channel = _channel(allow_users=["U1"])
        await channel._handle_message_event(
            {
                "type": "message",
                "channel": "C1",
                "channel_type": "channel",
                "user": "U1",
                "ts": "1.1",
                "text": "<@U0BOT> hi",
            }
        )
        msg = channel.router.handle_message.await_args[0][0]
        assert msg.sender_id == "C1:1.1"
        assert msg.channel_key == "slack:C1:1.1"

    @pytest.mark.asyncio
    async def test_each_thread_is_its_own_session(self):
        channel = _channel(allow_users=["U1"])
        for ts, thread in (("1.1", "1.0"), ("2.1", "2.0")):
            await channel._handle_message_event({
                "type": "message", "channel": "C1", "channel_type": "channel",
                "user": "U1", "ts": ts, "thread_ts": thread,
                "text": "<@U0BOT> hi",
            })
        keys = [
            call[0][0].channel_key
            for call in channel.router.handle_message.await_args_list
        ]
        assert keys == ["slack:C1:1.0", "slack:C1:2.0"]

    @pytest.mark.asyncio
    async def test_thread_replies_continue_a_session_without_a_mention(self):
        channel = _channel(allow_users=["U1"])
        channel.router.get_last_session = AsyncMock(return_value="s1")
        await channel._handle_message_event(
            {
                "type": "message",
                "channel": "C1",
                "channel_type": "channel",
                "user": "U1",
                "ts": "1.2",
                "thread_ts": "1.0",
                "text": "and then?",
            }
        )
        channel.router.handle_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_unowned_thread_reply_without_a_mention_is_ignored(self):
        channel = _channel(allow_users=["U1"])
        await channel._handle_message_event(
            {
                "type": "message",
                "channel": "C1",
                "channel_type": "channel",
                "user": "U1",
                "ts": "1.2",
                "thread_ts": "1.0",
                "text": "hello",
            }
        )
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_mention_claims_an_unowned_thread(self):
        channel = _channel(allow_users=["U1"])
        await channel._handle_message_event(
            {
                "type": "app_mention",
                "channel": "C1",
                "channel_type": "channel",
                "user": "U1",
                "ts": "1.2",
                "thread_ts": "1.0",
                "text": "<@U0BOT> help",
            }
        )
        message = channel.router.handle_message.await_args.args[0]
        assert message.channel_key == "slack:C1:1.0"

    @pytest.mark.asyncio
    async def test_an_existing_thread_never_uses_the_channel_key(self):
        channel = _channel(allow_users=["U1"])
        await channel._handle_message_event(
            {
                "type": "message",
                "channel": "C1",
                "channel_type": "channel",
                "user": "U1",
                "ts": "1.1",
                "thread_ts": "1.0",
                "text": "<@U0BOT> hi",
            }
        )
        msg = channel.router.handle_message.await_args[0][0]
        assert msg.channel_key == "slack:C1:1.0"

    @pytest.mark.asyncio
    async def test_a_redelivered_event_runs_once(self):
        # Slack retries anything it thinks was not acked.
        channel = _channel(allow_users=["U1"], allow_direct_messages=True)
        event = {
            "type": "message", "channel": "D1", "channel_type": "im",
            "user": "U1", "ts": "1.1", "text": "hello",
        }
        await channel._handle_message_event(event)
        await channel._handle_message_event(dict(event))
        channel.router.handle_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_a_message_and_its_app_mention_twin_run_once(self):
        channel = _channel(allow_users=["U1"])
        base = {
            "channel": "C1", "channel_type": "channel", "user": "U1",
            "ts": "1.1", "text": "<@U0BOT> hi",
        }
        await channel._handle_message_event({"type": "app_mention", **base})
        await channel._handle_message_event({"type": "message", **base})
        channel.router.handle_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_an_empty_message_is_dropped(self):
        channel = _channel(allow_users=["U1"], allow_direct_messages=True)
        await channel._handle_message_event({
            "type": "message", "channel": "D1", "channel_type": "im",
            "user": "U1", "ts": "1.1", "text": "",
        })
        channel.router.handle_message.assert_not_called()


class TestReactionEvents:
    @pytest.mark.asyncio
    async def test_a_reaction_on_a_known_message_reaches_the_router(self):
        channel = _channel(allow_users=["U1"], allow_direct_messages=True)
        channel._cache_message("1.1", "D1", "the original")
        await channel._handle_reaction_event({
            "type": "reaction_added", "user": "U1", "reaction": "tada",
            "item": {"channel": "D1", "ts": "1.1"},
        })
        msg = channel.router.handle_message.await_args[0][0]
        assert ":tada:" in msg.text
        assert "the original" in msg.text

    @pytest.mark.asyncio
    async def test_a_reaction_on_an_unknown_message_is_ignored(self):
        # Otherwise a stray emoji anywhere in the workspace opens a session.
        channel = _channel(allow_users=["U1"])
        await channel._handle_reaction_event(
            {
                "type": "reaction_added",
                "user": "U1",
                "reaction": "tada",
                "item": {"channel": "D1", "ts": "9.9"},
            }
        )
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_an_unauthorized_reaction_is_ignored(self):
        channel = _channel(
            allow_users=["U-other"],
            allow_direct_messages=True,
        )
        channel._cache_message("1.1", "D1", "the original")
        await channel._handle_reaction_event(
            {
                "type": "reaction_added",
                "user": "U1",
                "reaction": "tada",
                "item": {"channel": "D1", "ts": "1.1"},
            }
        )
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_reaction_does_not_cross_conversations_on_a_shared_ts(self):
        # A Slack ts is unique inside one conversation only, so the same ts
        # in another channel must not reach the first channel's session.
        channel = _channel(allow_channels=["C1", "C2"])
        channel._cache_message("1.1", "C1:1.0", "the original in C1")
        await channel._handle_reaction_event({
            "type": "reaction_added", "user": "U1", "reaction": "tada",
            "item": {"channel": "C2", "ts": "1.1"},
        })
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_reaction_still_reaches_its_own_thread_session(self):
        channel = _channel(allow_channels=["C1"])
        channel._cache_message("1.1", "C1:1.0", "the original in C1")
        await channel._handle_reaction_event({
            "type": "reaction_added", "user": "U1", "reaction": "tada",
            "item": {"channel": "C1", "ts": "1.1"},
        })
        msg = channel.router.handle_message.await_args[0][0]
        assert msg.channel_key == "slack:C1:1.0"


class TestOutbound:
    @pytest.mark.asyncio
    async def test_send_converts_markdown_and_targets_the_thread(self):
        channel = _channel()
        await channel.send(OutboundMessage(target="C1:1.0", text="**hi**"))
        kwargs = channel._web.chat_postMessage.await_args.kwargs
        assert kwargs["channel"] == "C1"
        assert kwargs["thread_ts"] == "1.0"
        assert kwargs["text"] == "*hi*"

    @pytest.mark.asyncio
    async def test_a_long_reply_is_split_without_truncation(self):
        channel = _channel()
        body = "\n".join(f"line {i}" for i in range(2000))
        await channel.send(OutboundMessage(target="C1", text=body))
        assert channel._web.chat_postMessage.await_count > 1
        sent = "\n".join(
            c.kwargs["text"] for c in channel._web.chat_postMessage.await_args_list
        )
        assert sent == body

    @pytest.mark.asyncio
    async def test_an_edit_stays_inside_the_length_limit(self):
        channel = _channel()
        await channel.edit_message("C1", "1.1", "y" * (MAX_MSG_LEN + 500))
        assert len(channel._web.chat_update.await_args.kwargs["text"]) <= MAX_MSG_LEN + 1

    @pytest.mark.asyncio
    async def test_sending_while_rotating_is_refused_not_dropped(self):
        # A rotation publishes the next Web client before it connects the
        # next socket, so _web alone is set while the generation serving
        # events is still the previous one. Returning quietly here told
        # StreamAdapter the reply had been sent and it deleted the
        # placeholder, leaving the user with neither.
        channel = _channel()
        channel._state = "rotating"
        with pytest.raises(SlackUnavailable):
            await channel.send(OutboundMessage(target="C1", text="hi"))
        channel._web.chat_postMessage.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_stopped_channel_refuses_to_post(self):
        channel = _channel()
        channel._state = "quiescing"
        with pytest.raises(SlackUnavailable):
            await channel._post("C1", "hi")

    @pytest.mark.asyncio
    async def test_streaming_keeps_the_placeholder_when_the_channel_goes_away(
        self,
    ):
        # The user-visible half: a quiet refusal read as success, so the
        # placeholder was deleted and the reply never posted.
        from nerve.channels.stream_adapter import StreamAdapter

        channel = _channel()
        adapter = StreamAdapter(channel, "C1", "s1")
        adapter._placeholder_id = "1.1"
        adapter._buffer = "the answer"
        channel.edit_message = AsyncMock()
        channel.delete_message = AsyncMock()
        channel._state = "rotating"

        await adapter._handle_done()

        # The recovery branch, not the "sent, so drop the placeholder" one.
        channel.edit_message.assert_awaited_once()
        channel.delete_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_best_effort_paths_stay_quiet_while_rotating(self):
        channel = _channel()
        channel._state = "rotating"
        channel._remember(channel._last_inbound_ts, "C1", "1.1", 10)
        await channel.edit_message("C1", "1.1", "text")
        await channel.delete_message("C1", "1.1")
        await channel.send_typing("C1")
        await channel.set_reaction("C1", "1.1", "🎉")
        assert not await channel.send_file("C1", __file__)
        channel._web.chat_update.assert_not_called()
        channel._web.chat_delete.assert_not_called()
        channel._web.reactions_add.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_failed_post_is_reported_not_swallowed(self):
        # The caller has to see this: StreamAdapter's recovery path is the
        # only thing that keeps the reply from disappearing.
        channel = _channel()
        channel._web.chat_postMessage = AsyncMock(side_effect=RuntimeError("ratelimited"))
        with pytest.raises(RuntimeError):
            await channel.send(OutboundMessage(target="C1", text="hi"))

    @pytest.mark.asyncio
    async def test_set_reaction_translates_the_emoji(self):
        channel = _channel()
        await channel.set_reaction("C1:1.0", "1.1", "👍")
        kwargs = channel._web.reactions_add.await_args.kwargs
        assert kwargs == {"channel": "C1", "timestamp": "1.1", "name": "thumbsup"}

    @pytest.mark.asyncio
    async def test_an_unmappable_reaction_is_skipped(self):
        channel = _channel()
        await channel.set_reaction("C1", "1.1", "🫥")
        channel._web.reactions_add.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_typing_ack_reacts_to_the_message_being_answered(self):
        channel = _channel(allow_users=["U1"], allow_direct_messages=True)
        await channel._handle_message_event({
            "type": "message", "channel": "D1", "channel_type": "im",
            "user": "U1", "ts": "1.1", "text": "hello",
        })
        await channel.send_typing("D1")
        assert channel._web.reactions_add.await_args.kwargs["timestamp"] == "1.1"

    @pytest.mark.asyncio
    async def test_send_file_refuses_a_missing_path(self):
        assert not await _channel().send_file("C1", "/nope/missing.txt")

    @pytest.mark.asyncio
    async def test_rebuild_uses_the_active_credential_pair(self):
        channel = _channel()
        channel._active_app_token = "xapp-active"
        active_web = channel._web
        dead = MagicMock()
        dead.is_connected = AsyncMock(return_value=False)
        dead.close = AsyncMock()
        channel._client = dead

        fresh = MagicMock()
        fresh.is_connected = AsyncMock(return_value=True)
        fresh.connect = AsyncMock()
        channel._build_socket_client = MagicMock(return_value=fresh)

        await channel.rebuild_transport()

        fresh.connect.assert_awaited()
        # The old socket must be closed first. Slack gives each event to one
        # connection only, so a leftover one quietly takes a share of them.
        dead.close.assert_awaited()
        assert channel._client is fresh
        channel._build_socket_client.assert_called_once_with(
            app_token="xapp-active", web_client=active_web,
        )

    @pytest.mark.asyncio
    async def test_a_failed_new_socket_restores_the_previous_credentials(self):
        channel = _channel()
        events = []

        old_web = channel._web
        old_client = MagicMock()

        async def close_old():
            events.append("old closed")

        old_client.close = AsyncMock(side_effect=close_old)
        channel._client = old_client
        channel._active_bot_token = "xoxb-old"
        channel._active_app_token = "xapp-old"
        channel._bot_user_id = "U0OLD"
        channel._bot_id = "B0OLD"
        channel._team_id = "T0OLD"

        candidate_web = MagicMock()
        candidate = MagicMock()

        async def connect_candidate():
            events.append("candidate connected")
            assert not channel.is_available
            raise RuntimeError("invalid app token")

        async def close_candidate():
            events.append("candidate closed")

        candidate.connect = AsyncMock(side_effect=connect_candidate)
        candidate.close = AsyncMock(side_effect=close_candidate)
        channel._prepare_transport = AsyncMock(return_value=(
            candidate_web,
            candidate,
            {"user_id": "U0NEW", "bot_id": "B0NEW", "team_id": "T0OLD"},
        ))

        rollback = MagicMock()

        async def connect_rollback():
            events.append("rollback connected")

        rollback.connect = AsyncMock(side_effect=connect_rollback)
        channel._build_socket_client = MagicMock(return_value=rollback)

        desired = _with_credentials(channel, "xoxb-new", "xapp-new")
        desired.slack.allow_users = ["U2"]
        with pytest.raises(RuntimeError, match="previous connection was restored"):
            await channel.reload_credentials(desired)

        assert events == [
            "old closed",
            "candidate connected",
            "candidate closed",
            "rollback connected",
        ]
        assert channel._client is rollback
        assert channel._web is old_web
        assert channel._active_bot_token == "xoxb-old"
        assert channel._active_app_token == "xapp-old"
        assert channel._bot_user_id == "U0OLD"
        assert channel._bot_id == "B0OLD"
        assert channel._team_id == "T0OLD"
        assert channel.config.slack.allow_users == []
        assert channel.is_available
        assert channel.needs_credential_reload("xoxb-new", "xapp-new")

    @pytest.mark.asyncio
    async def test_credentials_for_another_workspace_need_a_restart(self):
        channel = _channel()
        old_web = channel._web
        old_client = MagicMock()
        old_client.close = AsyncMock()
        channel._client = old_client
        channel._active_bot_token = "xoxb-old"
        channel._active_app_token = "xapp-old"
        channel._team_id = "T0OLD"

        candidate = MagicMock()
        candidate.close = AsyncMock()
        channel._prepare_transport = AsyncMock(
            return_value=(
                MagicMock(),
                candidate,
                {"team_id": "T0NEW"},
            )
        )

        desired = _with_credentials(channel, "xoxb-new", "xapp-new")
        desired.slack.allow_users = ["U2"]
        with pytest.raises(RuntimeError, match="different workspace"):
            await channel.reload_credentials(desired)

        candidate.close.assert_awaited_once()
        old_client.close.assert_not_awaited()
        assert channel._web is old_web
        assert channel._team_id == "T0OLD"
        assert channel.config.slack.allow_users == []

    @pytest.mark.asyncio
    async def test_a_socket_handshake_cannot_hold_reload_open_forever(
        self, monkeypatch,
    ):
        import nerve.channels.slack as slack_module

        monkeypatch.setattr(slack_module, "SOCKET_CONNECT_TIMEOUT", 0.01)
        client = MagicMock()

        async def never_connect():
            await asyncio.sleep(30)

        client.connect = AsyncMock(side_effect=never_connect)

        with pytest.raises(RuntimeError, match="connection timed out"):
            await SlackChannel._connect_socket(client)

    @pytest.mark.asyncio
    async def test_send_file_refuses_without_a_target(self):
        # The router passes an empty target when the session was not bound
        # to this channel; uploading then would leak the file.
        assert not await _channel().send_file("", "/etc/hostname")


# ---------------------------------------------------------------------- #
#  Review regressions                                                     #
# ---------------------------------------------------------------------- #


class TestIdPredicate:
    def test_real_slack_ids_are_recognised(self):
        assert is_slack_id("U0123ABC")
        assert is_slack_id("C0456DEF")
        assert is_slack_id("W01ABCDEFGH")

    def test_an_uppercase_name_is_not_an_id(self):
        # This is the bug: a case heuristic read ALICE as an id, skipped the
        # users.info lookup, and let deny_users=["ALICE"] admit her.
        assert not is_slack_id("ALICE")
        assert not is_slack_id("ENGINEERING")

    def test_a_handle_or_email_is_not_an_id(self):
        assert not is_slack_id("alex.soffronow")
        assert not is_slack_id("a@b.com")

    def test_a_too_short_token_is_not_an_id(self):
        assert not is_slack_id("U012")


class TestGuardrailRegressions:
    @pytest.mark.asyncio
    async def test_an_uppercase_deny_name_still_forces_a_lookup(self):
        channel = _channel(deny_users=["ALICE"], allow_channels=["C0456DEF"])
        channel._web.users_info = AsyncMock(
            return_value={
                "user": {"id": "U999", "name": "ALICE", "profile": {"email": "a@b.c"}},
            }
        )
        channel._web.conversations_info = AsyncMock(
            return_value={
                "channel": {"id": "C0456DEF", "name": "eng"},
            }
        )
        assert not await channel._authorize("U999", "C0456DEF", "channel")
        channel._web.users_info.assert_awaited()

    @pytest.mark.asyncio
    async def test_a_missing_email_refuses_an_email_deny_rule(self):
        # users.info answers 200 without profile.email when the token lacks
        # users:read.email, so the deny pattern silently matched nothing.
        channel = _channel(
            allow_users=["U999"],
            deny_users=["blocked@x.com"],
            allow_direct_messages=True,
        )
        channel._web.users_info = AsyncMock(
            return_value={
                "user": {"id": "U999", "name": "blocked", "profile": {}},
            }
        )
        assert not await channel._authorize("U999", "D1", "im")

    @pytest.mark.asyncio
    async def test_an_email_deny_rule_still_works_with_the_scope(self):
        channel = _channel(
            allow_users=["*"],
            deny_users=["blocked@x.com"],
            allow_direct_messages=True,
        )
        channel._web.users_info = AsyncMock(
            return_value={
                "user": {
                    "id": "U9",
                    "name": "b",
                    "profile": {"email": "blocked@x.com"},
                },
            }
        )
        assert not await channel._authorize("U9", "D1", "im")

    @pytest.mark.asyncio
    async def test_an_innocent_user_is_not_caught_by_an_email_deny_rule(self):
        channel = _channel(
            allow_users=["*"],
            deny_users=["blocked@x.com"],
            allow_direct_messages=True,
        )
        channel._web.users_info = AsyncMock(
            return_value={
                "user": {"id": "U1", "name": "ok", "profile": {"email": "ok@x.com"}},
            }
        )
        assert await channel._authorize("U1", "D1", "im")

    @pytest.mark.asyncio
    async def test_a_nameless_channel_lookup_refuses_a_channel_deny_rule(self):
        channel = _channel(allow_users=["U1"], deny_channels=["*-secret"])
        channel._web.conversations_info = AsyncMock(
            return_value={"channel": {"id": "C1"}},
        )
        assert not await channel._authorize("U1", "C1", "channel")


class TestOutboundFailureRegressions:
    @pytest.mark.asyncio
    async def test_a_failed_placeholder_returns_none_without_raising(self):
        channel = _channel()
        channel._web.chat_postMessage = AsyncMock(side_effect=RuntimeError("ratelimited"))
        assert await channel.send_placeholder("C1", "s1") is None

    @pytest.mark.asyncio
    async def test_a_missing_placeholder_still_delivers_the_reply(self):
        # supports STREAMING + edit, but the placeholder post failed: this
        # matched neither adapter branch and the turn vanished.
        from nerve.channels.stream_adapter import StreamAdapter

        channel = _channel(stream_mode="partial")
        channel._web.chat_postMessage = AsyncMock(
            side_effect=[RuntimeError("ratelimited"), {"ts": "2.2"}],
        )
        adapter = StreamAdapter(channel, "C1", "s1")
        await adapter.initialize()
        assert adapter._placeholder_id is None
        await adapter.on_event("s1", {"type": "token", "content": "the answer"})
        await adapter.on_event("s1", {"type": "done"})
        sent = [c.kwargs["text"] for c in channel._web.chat_postMessage.await_args_list]
        assert "the answer" in sent[-1]


class TestDispatchBounds:
    @pytest.mark.asyncio
    async def test_stopping_acks_without_starting_more_work(self):
        channel = _channel()
        channel._stopping = True
        channel._dispatch = AsyncMock()
        client = MagicMock()
        client.send_socket_mode_response = AsyncMock()
        req = MagicMock(envelope_id="e1", type="events_api", payload={})

        await channel._on_request(client, req)

        client.send_socket_mode_response.assert_awaited_once()
        channel._dispatch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_envelopes_past_the_cap_are_dropped(self):
        import nerve.channels.slack as slack_module

        channel = _channel(allow_users=["U1"])
        monkeypatched = asyncio.Event()
        channel._inflight = {  # type: ignore[assignment]
            asyncio.create_task(monkeypatched.wait())
            for _ in range(slack_module._MAX_INFLIGHT)
        }
        client = MagicMock()
        client.send_socket_mode_response = AsyncMock()
        req = MagicMock(envelope_id="e1", type="events_api", payload={})

        before = len(channel._inflight)
        await channel._on_request(client, req)
        # Acked regardless — the drop must not look like a delivery failure.
        client.send_socket_mode_response.assert_awaited_once()
        assert len(channel._inflight) == before

        monkeypatched.set()
        await asyncio.gather(*channel._inflight, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_stop_waits_for_inflight_dispatches(self):
        channel = _channel()
        started = asyncio.Event()

        async def _slow():
            started.set()
            await asyncio.sleep(30)

        task = asyncio.create_task(_slow())
        channel._inflight.add(task)
        await started.wait()
        await channel.stop()
        assert task.done()

    @pytest.mark.asyncio
    async def test_live_disable_can_drain_inflight_dispatches(self):
        channel = _channel()
        channel._client = MagicMock()
        channel._client.close = AsyncMock()
        finished = asyncio.Event()

        async def _finish():
            await asyncio.sleep(0)
            finished.set()

        task = asyncio.create_task(_finish())
        channel._inflight.add(task)
        await channel.stop(drain=True)

        assert finished.is_set()
        assert not task.cancelled()
        assert channel._web is None

    @pytest.mark.asyncio
    async def test_live_disable_bounds_the_drain(self, monkeypatch):
        import nerve.channels.slack as slack_module

        monkeypatch.setattr(slack_module, "_STOP_DRAIN_TIMEOUT", 0)
        channel = _channel()
        channel._client = MagicMock()
        channel._client.close = AsyncMock()
        task = asyncio.create_task(asyncio.Event().wait())
        channel._inflight.add(task)

        await channel.stop(drain=True)

        assert task.cancelled()


class TestNotificationBlockLimits:
    def test_options_are_chunked_to_slacks_actions_limit(self):
        import nerve.channels.slack_presentation as presentation_module

        options = [(f"opt{i}", f"v{i}") for i in range(60)]
        blocks = build_notification_blocks("pick", "n1", options)
        actions = [b for b in blocks if b["type"] == "actions"]
        assert all(
            len(b["elements"]) <= presentation_module._MAX_ACTION_ELEMENTS
            for b in actions
        )
        assert sum(len(b["elements"]) for b in actions) == 60

    def test_block_ids_stay_unique_across_chunks(self):
        options = [(f"opt{i}", f"v{i}") for i in range(60)]
        blocks = build_notification_blocks("pick", "n1", options)
        ids = [b["block_id"] for b in blocks if b["type"] == "actions"]
        assert len(ids) == len(set(ids))


class TestSubtypes:
    @pytest.mark.asyncio
    async def test_a_thread_broadcast_reply_is_answered(self):
        # "Also send to channel" from inside a live agent thread was being
        # discarded as a system subtype.
        channel = _channel(allow_users=["U1"])
        channel.router.get_last_session = AsyncMock(return_value="s1")
        await channel._handle_message_event({
            "type": "message", "subtype": "thread_broadcast",
            "channel": "C1", "channel_type": "channel",
            "user": "U1", "ts": "1.2", "thread_ts": "1.0", "text": "and then?",
        })
        channel.router.handle_message.assert_called_once()


class TestOwnMessageDetection:
    """Regression cover for a bug only a real workspace surfaced.

    Slack stamps a ``bot_id`` onto a message a *person* sent through an app
    or integration, while still naming them in ``user``. Treating every
    ``bot_id`` as our own silently dropped those people.
    """

    def _event(self, **over):
        base = {
            "type": "message", "channel": "C1", "channel_type": "channel",
            "user": "U-human", "ts": "1.1", "text": "hi",
        }
        base.update(over)
        return base

    def _ch(self):
        channel = _channel(allow_users=["U-human"])
        channel._bot_user_id = "U0BOT"
        channel._bot_id = "B0SELF"
        return channel

    def test_our_own_bot_id_is_ours(self):
        assert self._ch()._is_own_message(self._event(bot_id="B0SELF", user=None))

    def test_our_own_bot_user_id_is_ours(self):
        assert self._ch()._is_own_message(self._event(user="U0BOT"))

    def test_a_human_posting_through_an_app_is_not_ours(self):
        # The live workspace produced exactly this: a real user id alongside
        # another app's bot_id.
        assert not self._ch()._is_own_message(
            self._event(bot_id="B0OTHERAPP", user="U-human"),
        )

    def test_a_plain_human_message_is_not_ours(self):
        assert not self._ch()._is_own_message(self._event())

    @pytest.mark.asyncio
    async def test_a_human_posting_through_an_app_reaches_the_router(self):
        channel = self._ch()
        channel._web.users_info = AsyncMock(
            return_value={"user": {"is_bot": False, "profile": {}}},
        )
        await channel._handle_message_event(
            self._event(bot_id="B0OTHERAPP", text="<@U0BOT> via an integration"),
        )
        msg = channel.router.handle_message.await_args[0][0]
        assert msg.text == "via an integration"

    @pytest.mark.asyncio
    async def test_a_legacy_webhook_post_is_ignored(self):
        channel = self._ch()
        await channel._handle_message_event(
            self._event(subtype="bot_message", bot_id="B0OTHERAPP", user=None),
        )
        channel.router.handle_message.assert_not_called()

    def _open_channel(self):
        """A channel-only grant, which admits any member of C1.

        This is the config that makes the loop reachable, and it is the one
        docs/config.md offers for a shared channel, so the loop guard has to
        hold without help from the user gate.
        """
        channel = _channel(allow_channels=["C1"])
        channel._bot_user_id = "U0BOT"
        channel._bot_id = "B0SELF"
        channel.router.get_last_session = AsyncMock(return_value="s1")
        return channel

    @pytest.mark.asyncio
    async def test_an_authorized_human_reply_reaches_the_router(self):
        # The control for the two tests below: this config really does admit
        # a thread reply with no mention, which is what makes a loop possible.
        channel = self._open_channel()
        await channel._handle_message_event(
            self._event(thread_ts="1.0", text="on it"),
        )
        channel.router.handle_message.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_another_agents_own_reply_is_ignored(self):
        # Two of these in one channel would otherwise answer each other for
        # ever: a reply continues an owned thread with no mention needed.
        channel = self._open_channel()
        channel._web.users_info = AsyncMock(
            return_value={"user": {"is_bot": True, "profile": {}}},
        )
        await channel._handle_message_event(
            self._event(
                bot_id="B0OTHERAGENT",
                user="U0OTHERAGENT",
                thread_ts="1.0",
                text="on it",
            ),
        )
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_an_unresolvable_sender_beside_a_bot_id_is_ignored(self):
        channel = self._open_channel()
        channel._web.users_info = AsyncMock(side_effect=RuntimeError("no scope"))
        await channel._handle_message_event(
            self._event(
                bot_id="B0OTHERAGENT",
                user="U0OTHERAGENT",
                thread_ts="1.0",
                text="on it",
            ),
        )
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_message_with_no_bot_id_costs_no_lookup(self):
        channel = self._ch()
        channel._web.users_info = AsyncMock()
        assert not await channel._is_another_app_talking(self._event())
        channel._web.users_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_bot_verdict_is_cached(self):
        channel = self._ch()
        channel._web.users_info = AsyncMock(
            return_value={"user": {"is_bot": True, "profile": {}}},
        )
        event = self._event(bot_id="B0OTHERAGENT", user="U0OTHERAGENT")
        assert await channel._is_another_app_talking(event)
        assert await channel._is_another_app_talking(event)
        assert channel._web.users_info.await_count == 1


class TestAmpersandEscaping:
    def test_an_ampersand_in_a_link_url_is_escaped(self):
        # Slack rewrites a bare & inside a link, so emitting it unescaped
        # made what we sent differ from what Slack stored.
        assert _md_to_slack("[q](http://x?a=1&b=2)") == "<http://x?a=1&amp;b=2|q>"

    def test_an_ampersand_in_a_link_label_is_escaped(self):
        assert _md_to_slack("[a & b](http://x)") == "<http://x|a &amp; b>"

    def test_an_existing_entity_is_not_double_escaped(self):
        assert _md_to_slack("a &amp; b") == "a &amp; b"
        assert _md_to_slack("x &lt; y") == "x &lt; y"

    def test_a_bare_ampersand_in_prose_is_still_escaped(self):
        assert _md_to_slack("a & b") == "a &amp; b"


class TestSlashCommandsAreThreadBlind:
    """Slack refuses `/nerve` inside a thread — it answers "not supported in
    threads" — so a command never carries thread context and must resolve
    across the whole conversation. Verified against a live workspace.
    """

    def _ch(self, sessions, **kw):
        channel = _channel(allow_users=["U1"], **kw)
        channel.router.list_conversation_sessions = AsyncMock(return_value=sessions)
        channel.router.stop_session = AsyncMock(return_value=True)
        channel._web.chat_postEphemeral = AsyncMock(return_value={"ok": True})
        return channel

    @staticmethod
    def _row(sid, thread=None, title=None):
        key = f"slack:C1:{thread}" if thread else "slack:C1"
        return {"channel_key": key, "session_id": sid, "title": title or sid}

    @pytest.mark.asyncio
    async def test_a_thread_session_is_found_from_a_channel_command(self):
        # The regression: the command's own key owns nothing while a thread
        # beside it is busy, and stop used to report "No active session".
        channel = self._ch([self._row("s1", thread="1.0")])
        await channel._cmd_stop("C1", "U1", "slack:C1")
        channel.router.stop_session.assert_awaited_once_with("s1")
        said = channel._web.chat_postEphemeral.await_args.kwargs["text"]
        assert "s1" in said and "thread" in said

    @pytest.mark.asyncio
    async def test_nothing_live_says_so_plainly(self):
        channel = self._ch([])
        await channel._cmd_stop("C1", "U1", "slack:C1")
        channel.router.stop_session.assert_not_called()
        assert (
            "No active session"
            in channel._web.chat_postEphemeral.await_args.kwargs["text"]
        )

    @pytest.mark.asyncio
    async def test_several_live_sessions_ask_instead_of_guessing(self):
        # Stopping someone else's thread silently would be worse than asking.
        channel = self._ch(
            [
                self._row("s1", thread="1.0", title="Deploy"),
                self._row("s2", thread="2.0", title="Triage"),
            ]
        )
        await channel._cmd_stop("C1", "U1", "slack:C1")
        channel.router.stop_session.assert_not_called()
        blocks = channel._web.chat_postEphemeral.await_args.kwargs["blocks"]
        action_ids = [
            e["action_id"]
            for b in blocks
            if b["type"] == "actions"
            for e in b["elements"]
        ]
        assert action_ids == ["sessstop:s1", "sessstop:s2"]

    @pytest.mark.asyncio
    async def test_the_picker_button_stops_the_chosen_session(self):
        channel = self._ch([])
        channel._replace_via_url = AsyncMock()
        await channel._handle_interactive(
            {
                "type": "block_actions",
                "user": {"id": "U1"},
                "channel": {"id": "C1"},
                "response_url": "https://hooks.slack.test/x",
                "actions": [{"action_id": "sessstop:s2", "value": "s2"}],
            }
        )
        channel.router.stop_session.assert_awaited_once_with("s2")

    @pytest.mark.asyncio
    async def test_a_press_from_an_unauthorized_sender_is_refused(self):
        channel = self._ch([])
        channel.config.slack.allow_users = ["U-owner"]
        channel._replace_via_url = AsyncMock()
        await channel._handle_interactive(
            {
                "type": "block_actions",
                "user": {"id": "U-mallory"},
                "channel": {"id": "C1"},
                "response_url": "https://hooks.slack.test/x",
                "actions": [{"action_id": "sessstop:s2", "value": "s2"}],
            }
        )
        channel.router.stop_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_press_with_no_conversation_is_refused(self):
        # The conversation half of the policy cannot run without a channel,
        # so the press is refused rather than checked against half of it.
        channel = self._ch([])
        channel.config.slack.allow_users = ["U-owner"]
        channel._replace_via_url = AsyncMock()
        channel._notification_service = MagicMock()
        channel._notification_service.answer_delivered_notification = AsyncMock()
        for action_id, value in (
            ("sessstop:s2", "s2"),
            ("starpick:s2", "s2"),
            ("notif:n1:yes", "yes"),
        ):
            await channel._handle_interactive(
                {
                    "type": "block_actions",
                    "user": {"id": "U-mallory"},
                    "response_url": "https://hooks.slack.test/x",
                    "actions": [{"action_id": action_id, "value": value}],
                }
            )
        channel.router.stop_session.assert_not_called()
        channel.router.set_session_starred.assert_not_called()
        channel._notification_service.answer_delivered_notification\
            .assert_not_awaited()

    @pytest.mark.asyncio
    async def test_another_channels_sessions_are_not_touched(self):
        # Keep the adapter defensive even though the shared query is exact.
        channel = self._ch(
            [
                {"channel_key": "slack:C12:9.9", "session_id": "other"},
                self._row("mine", thread="1.0"),
            ]
        )
        found = await channel._live_sessions_for_channel("C1")
        assert [r["session_id"] for r in found] == ["mine"]

    @pytest.mark.asyncio
    async def test_a_legacy_channel_level_mapping_is_never_consumed(self):
        channel = self._ch(
            [
                self._row("legacy"),
                self._row("thread", thread="1.0"),
            ]
        )
        found = await channel._live_sessions_for_channel("C1")
        assert [row["session_id"] for row in found] == ["thread"]

    @pytest.mark.asyncio
    async def test_star_also_resolves_across_threads(self):
        channel = self._ch([self._row("s1", thread="1.0")])
        channel.router.set_session_starred = AsyncMock(return_value=True)
        await channel._cmd_star("C1", "U1", "slack:C1", True)
        channel.router.set_session_starred.assert_awaited_once_with("s1", True)


class TestCommandExposure:
    def _ch(self, **kw):
        channel = _channel(
            allow_users=["U1"], allow_direct_messages=True, **kw,
        )
        channel._web.chat_postEphemeral = AsyncMock(return_value={"ok": True})
        return channel

    async def _run(self, channel, text):
        await channel._handle_slash_command({
            "user_id": "U1", "channel_id": "C1", "text": text,
        })
        return channel._web.chat_postEphemeral.await_args.kwargs["text"]

    def test_operator_commands_are_off_by_default(self):
        # doctor prints host health into a shared workspace and restart lets
        # anyone on the allow list bounce the daemon.
        enabled = self._ch().enabled_commands
        assert "doctor" not in enabled
        assert "restart" not in enabled
        assert {"new", "stop", "star", "unstar", "reply"} <= enabled

    def test_only_the_globally_scoped_session_list_is_off_by_default(self):
        enabled = self._ch().enabled_commands
        assert "sessions" not in enabled
        assert "reply" in enabled

    @pytest.mark.asyncio
    async def test_sessions_is_refused_unless_it_was_asked_for(self):
        channel = self._ch()
        channel.router.list_sessions = AsyncMock(return_value=[])
        said = await self._run(channel, "sessions")
        assert "turned off" in said
        channel.router.list_sessions.assert_not_called()

    @pytest.mark.asyncio
    async def test_reply_is_scoped_to_the_slack_conversation_and_actor(self):
        channel = self._ch()
        channel._notification_service = MagicMock()
        channel._notification_service.answer_latest_question = AsyncMock(
            return_value={"title": "Proceed?"},
        )
        said = await self._run(channel, "reply yes")
        assert "Proceed?" in said
        channel._notification_service.answer_latest_question.assert_awaited_once_with(
            "yes",
            channel="slack",
            target="C1",
            actor="U1",
        )

    def test_both_are_still_available_on_request(self):
        enabled = self._ch(commands=["sessions", "reply"]).enabled_commands
        assert enabled == frozenset({"sessions", "reply"})

    def test_an_explicit_list_narrows_the_set(self):
        assert self._ch(commands=["reply"]).enabled_commands == frozenset({"reply"})

    def test_an_empty_list_turns_every_command_off(self):
        assert self._ch(commands=[]).enabled_commands == frozenset()

    def test_all_enables_everything(self):
        from nerve.config import SLACK_ALL_COMMANDS

        assert self._ch(commands=["all"]).enabled_commands == frozenset(
            SLACK_ALL_COMMANDS
        )

    @pytest.mark.asyncio
    async def test_a_disabled_command_is_refused_not_run(self):
        channel = self._ch(commands=["reply"])
        channel.router.stop_session = AsyncMock()
        said = await self._run(channel, "stop")
        assert "turned off" in said
        channel.router.stop_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_restart_is_refused_by_default(self):
        # The one that spawns a process, so it must not fall through.
        channel = self._ch()
        said = await self._run(channel, "restart")
        assert "turned off" in said

    @pytest.mark.asyncio
    async def test_an_unknown_command_reads_differently_from_a_disabled_one(self):
        assert "No such command" in await self._run(self._ch(), "frobnicate")

    @pytest.mark.asyncio
    async def test_help_lists_only_what_is_enabled(self):
        said = await self._run(self._ch(commands=["stop", "reply"]), "help")
        assert "/nerve stop" in said and "/nerve reply" in said
        assert "doctor" not in said and "sessions" not in said

    @pytest.mark.asyncio
    async def test_help_says_so_when_nothing_is_enabled(self):
        assert "No `/nerve` commands" in await self._run(self._ch(commands=[]), "help")


class TestCommandsBindTheKeyMessagesRead:
    """Slash commands bind only DMs; shared messages always bind threads."""

    def _ch(self, **kw):
        channel = _channel(
            allow_users=["U1"], allow_direct_messages=True, **kw,
        )
        channel._web.chat_postEphemeral = AsyncMock(return_value={"ok": True})
        channel.router.create_session = AsyncMock(return_value="s-new")
        channel.router.switch_session = AsyncMock()
        channel.router.stop_session = AsyncMock(return_value=True)
        channel.router.list_sessions = AsyncMock(return_value=[])
        return channel

    async def _route(self, bot, **event) -> str:
        """The channel key an ordinary message in this conversation lands on."""
        base = {"type": "message", "user": "U1", "text": "<@U0BOT> hi"}
        await bot._handle_message_event({**base, **event})
        return bot.router.handle_message.await_args[0][0].channel_key

    async def _run(self, channel, channel_id, text) -> str:
        await channel._handle_slash_command({
            "user_id": "U1", "channel_id": channel_id, "text": text,
        })
        return channel._web.chat_postEphemeral.await_args.kwargs["text"]

    @pytest.mark.asyncio
    async def test_a_dm_command_binds_the_key_a_dm_message_reads(self):
        channel = self._ch()
        routed = await self._route(
            channel, channel="D1", channel_type="im", ts="1.1",
        )
        await self._run(channel, "D1", "new")
        bound = channel.router.create_session.await_args[0][0]
        assert bound == routed == "slack:D1"

    @pytest.mark.asyncio
    async def test_a_dm_thread_uses_the_dm_conversation(self):
        channel = self._ch()
        routed = await self._route(
            channel,
            channel="D1",
            channel_type="im",
            ts="1.2",
            thread_ts="1.0",
        )
        assert routed == "slack:D1"

    @pytest.mark.asyncio
    async def test_a_threaded_channel_refuses_rather_than_orphaning_a_session(self):
        channel = self._ch()
        routed = await self._route(
            channel, channel="C1", channel_type="channel", ts="1.1",
        )
        assert routed == "slack:C1:1.1"

        said = await self._run(channel, "C1", "new")
        channel.router.create_session.assert_not_called()
        channel.router.stop_session.assert_not_called()
        assert "needs a thread" in said

    @pytest.mark.asyncio
    async def test_a_thread_reply_is_not_stopped_by_a_channel_command(self):
        channel = self._ch()
        await self._route(
            channel,
            channel="C1",
            channel_type="channel",
            ts="1.2",
            thread_ts="1.0",
        )
        await self._run(channel, "C1", "new")
        channel.router.stop_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_session_picker_is_refused_in_a_threaded_channel(self):
        # Its selection is written under the channel-level key too.
        channel = self._ch(commands=["sessions"])
        said = await self._run(channel, "C1", "sessions")
        channel.router.list_sessions.assert_not_called()
        assert "needs a thread" in said

    @pytest.mark.asyncio
    async def test_the_session_picker_still_opens_in_a_dm(self):
        channel = self._ch(commands=["sessions"])
        channel.router.get_last_session = AsyncMock(return_value=None)
        await self._run(channel, "D1", "sessions")
        channel.router.list_sessions.assert_awaited()

    @pytest.mark.asyncio
    async def test_a_picker_button_refuses_to_bind_in_a_threaded_channel(self):
        channel = self._ch(commands=["sessions"])
        channel._replace_via_url = AsyncMock()
        await channel._handle_interactive({
            "type": "block_actions",
            "user": {"id": "U1"},
            "channel": {"id": "C1"},
            "response_url": "https://hooks.slack.test/x",
            "actions": [{"action_id": "sess:s9", "value": "s9"}],
        })
        channel.router.switch_session.assert_not_called()
        assert "needs a thread" in channel._replace_via_url.await_args[0][1]

    @pytest.mark.asyncio
    async def test_a_picker_button_still_switches_in_a_dm(self):
        channel = self._ch(commands=["sessions"])
        channel._replace_via_url = AsyncMock()
        channel.router.get_last_session = AsyncMock(return_value=None)
        await channel._handle_interactive({
            "type": "block_actions",
            "user": {"id": "U1"},
            "channel": {"id": "D1"},
            "response_url": "https://hooks.slack.test/x",
            "actions": [{"action_id": "sess:s9", "value": "s9"}],
        })
        channel.router.switch_session.assert_awaited_once_with("slack:D1", "s9")

    @pytest.mark.asyncio
    async def test_starring_from_the_card_works_in_a_threaded_channel(self):
        # Starring changes no mapping, so the thread guard must not block it.
        channel = self._ch(commands=["sessions"])
        channel._replace_via_url = AsyncMock()
        channel.router.toggle_session_starred = AsyncMock(return_value=True)
        channel.router.get_last_session = AsyncMock(return_value=None)
        await channel._handle_interactive({
            "type": "block_actions",
            "user": {"id": "U1"},
            "channel": {"id": "C1"},
            "response_url": "https://hooks.slack.test/x",
            "actions": [{"action_id": "sessstar:s9", "value": "s9"}],
        })
        channel.router.toggle_session_starred.assert_awaited_once_with("s9")


class TestStarPicker:
    """`/nerve star` used to act on ``candidates[0]``.

    `/nerve stop` shows a picker and the documentation said both did, so a
    star landed on whichever thread the query returned first.
    """

    def _ch(self, sessions):
        channel = _channel(allow_users=["U1"])
        channel.router.list_conversation_sessions = AsyncMock(return_value=sessions)
        channel.router.set_session_starred = AsyncMock(return_value=True)
        channel._web.chat_postEphemeral = AsyncMock(return_value={"ok": True})
        return channel

    @staticmethod
    def _row(sid, thread=None, title=None):
        key = f"slack:C1:{thread}" if thread else "slack:C1"
        return {"channel_key": key, "session_id": sid, "title": title or sid}

    @pytest.mark.asyncio
    async def test_nothing_live_says_so_plainly(self):
        channel = self._ch([])
        await channel._cmd_star("C1", "U1", "slack:C1", True)
        channel.router.set_session_starred.assert_not_called()
        said = channel._web.chat_postEphemeral.await_args.kwargs["text"]
        assert "No active session to star" in said

    @pytest.mark.asyncio
    async def test_nothing_live_names_the_unstar_verb(self):
        channel = self._ch([])
        await channel._cmd_star("C1", "U1", "slack:C1", False)
        said = channel._web.chat_postEphemeral.await_args.kwargs["text"]
        assert "unstar" in said

    @pytest.mark.asyncio
    async def test_one_candidate_is_starred_and_named(self):
        channel = self._ch([self._row("s1", thread="1.0")])
        await channel._cmd_star("C1", "U1", "slack:C1", True)
        channel.router.set_session_starred.assert_awaited_once_with("s1", True)
        assert "s1" in channel._web.chat_postEphemeral.await_args.kwargs["text"]

    @pytest.mark.asyncio
    async def test_several_candidates_ask_instead_of_guessing(self):
        channel = self._ch([
            self._row("s1", thread="1.0", title="Deploy"),
            self._row("s2", thread="2.0", title="Triage"),
        ])
        await channel._cmd_star("C1", "U1", "slack:C1", True)
        channel.router.set_session_starred.assert_not_called()
        blocks = channel._web.chat_postEphemeral.await_args.kwargs["blocks"]
        action_ids = [
            e["action_id"]
            for b in blocks if b["type"] == "actions" for e in b["elements"]
        ]
        assert action_ids == ["starpick:1:s1", "starpick:1:s2"]

    @pytest.mark.asyncio
    async def test_the_unstar_picker_carries_the_state_it_will_set(self):
        # The session card's own toggle flips whatever the row holds; a
        # picker has to set what the command asked for.
        channel = self._ch([
            self._row("s1", thread="1.0"), self._row("s2", thread="2.0"),
        ])
        await channel._cmd_star("C1", "U1", "slack:C1", False)
        blocks = channel._web.chat_postEphemeral.await_args.kwargs["blocks"]
        action_ids = [
            e["action_id"]
            for b in blocks if b["type"] == "actions" for e in b["elements"]
        ]
        assert action_ids == ["starpick:0:s1", "starpick:0:s2"]

    @pytest.mark.asyncio
    async def test_the_picker_button_stars_the_chosen_session(self):
        channel = self._ch([])
        channel._replace_via_url = AsyncMock()
        await channel._handle_interactive({
            "type": "block_actions",
            "user": {"id": "U1"},
            "channel": {"id": "C1"},
            "response_url": "https://hooks.slack.test/x",
            "actions": [{"action_id": "starpick:1:s2", "value": "s2"}],
        })
        channel.router.set_session_starred.assert_awaited_once_with("s2", True)

    @pytest.mark.asyncio
    async def test_the_picker_button_unstars_the_chosen_session(self):
        channel = self._ch([])
        channel._replace_via_url = AsyncMock()
        await channel._handle_interactive({
            "type": "block_actions",
            "user": {"id": "U1"},
            "channel": {"id": "C1"},
            "response_url": "https://hooks.slack.test/x",
            "actions": [{"action_id": "starpick:0:s2", "value": "s2"}],
        })
        channel.router.set_session_starred.assert_awaited_once_with("s2", False)

    @pytest.mark.asyncio
    async def test_a_vanished_session_is_reported_not_raised(self):
        channel = self._ch([])
        channel._replace_via_url = AsyncMock()
        channel.router.set_session_starred = AsyncMock(side_effect=ValueError("gone"))
        await channel._handle_interactive({
            "type": "block_actions",
            "user": {"id": "U1"},
            "channel": {"id": "C1"},
            "response_url": "https://hooks.slack.test/x",
            "actions": [{"action_id": "starpick:1:s2", "value": "s2"}],
        })
        assert "no longer available" in channel._replace_via_url.await_args[0][1]


class TestNotificationSections:
    """3000 characters is the limit on one section, not on the message.

    Slicing there dropped everything past it with no sign anything was
    missing, while Block Kit allows the content to be spread over several
    sections instead.
    """

    @staticmethod
    def _sections(blocks) -> list[str]:
        return [b["text"]["text"] for b in blocks if b["type"] == "section"]

    def test_content_just_below_the_limit_is_one_section(self):
        assert len(self._sections(build_notification_blocks("x" * 2999, "n1"))) == 1

    def test_content_exactly_at_the_limit_is_one_section(self):
        assert len(self._sections(build_notification_blocks("x" * 3000, "n1"))) == 1

    def test_content_one_past_the_limit_is_split_not_cut(self):
        sections = self._sections(build_notification_blocks("x" * 3001, "n1"))
        assert len(sections) == 2
        assert "".join(sections) == "x" * 3001

    def test_a_long_body_keeps_every_line(self):
        body = "\n".join(f"line {i:04d}" for i in range(800))
        sections = self._sections(build_notification_blocks(body, "n1"))
        assert len(sections) > 1
        assert "\n".join(sections) == body

    def test_every_section_fits_slacks_limit(self):
        body = "\n".join(f"line {i:04d}" for i in range(2000))
        sections = self._sections(build_notification_blocks(body, "n1"))
        assert all(len(s) <= 3000 for s in sections)

    def test_the_option_buttons_still_follow_the_text(self):
        blocks = build_notification_blocks(
            "y" * 7000, "n1", [("Approve", "approve")],
        )
        assert blocks[-1]["type"] == "actions"
        assert blocks[-1]["elements"][0]["action_id"] == "notif:n1:approve"

    def test_a_body_past_the_block_limit_says_what_it_dropped(self):
        # 50 blocks is a hard limit on the whole message, so the only
        # content that can be lost is content Slack would refuse anyway.
        import nerve.channels.slack_presentation as presentation_module

        blocks = build_notification_blocks("z" * 400_000, "n1")
        sections = self._sections(blocks)
        assert len(sections) == presentation_module._MAX_SECTION_BLOCKS
        assert "more characters" in sections[-1]


class _CapturedPosts:
    """Stand-in for ``httpx.AsyncClient`` recording response_url bodies."""

    def __init__(self) -> None:
        self.bodies: list[dict] = []

    def __call__(self, *args, **kwargs):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None):
        self.bodies.append(json)


class TestNotificationCardRoundTrip:
    """The card's section text is the mrkdwn Slack already stores.

    Running it back through the Markdown converter when the button was
    pressed turned *bold* into _italic_ and escaped the link markup, so the
    message visibly changed on answering.
    """

    RAW = (
        "## Deploy to production\n"
        "**Ship it?** See [the run](https://ci.example.com/x?a=1&b=2).\n"
        "Threshold is a < b & c > d, ask <@U9>."
    )

    async def _answer(self, monkeypatch, raw: str = RAW):
        import httpx

        posts = _CapturedPosts()
        monkeypatch.setattr(httpx, "AsyncClient", posts)

        channel = _channel(allow_users=["U1"])
        service = MagicMock()
        service.answer_delivered_notification = AsyncMock(
            return_value={
                "status": "answered",
                "redeliver_at": None,
            }
        )
        channel._notification_service = service

        blocks = build_notification_blocks(raw, "n1", [("Approve", "approve")])
        await channel._handle_interactive({
            "type": "block_actions",
            "user": {"id": "U1"},
            "channel": {"id": "C1"},
            "response_url": "https://hooks.slack.test/x",
            "actions": [{"action_id": "notif:n1:approve", "value": "approve"}],
            "message": {"blocks": blocks},
        })
        return channel, blocks, posts.bodies[-1]["text"]

    @pytest.mark.asyncio
    async def test_the_card_text_is_carried_across_unchanged(self, monkeypatch):
        _, blocks, updated = await self._answer(monkeypatch)
        original = blocks[0]["text"]["text"]
        assert updated.startswith(original)

    @pytest.mark.asyncio
    async def test_a_heading_stays_bold_instead_of_turning_italic(self, monkeypatch):
        _, _, updated = await self._answer(monkeypatch)
        assert "*Deploy to production*" in updated
        assert "_Deploy to production_" not in updated

    @pytest.mark.asyncio
    async def test_bold_stays_bold(self, monkeypatch):
        _, _, updated = await self._answer(monkeypatch)
        assert "*Ship it?*" in updated
        assert "_Ship it?_" not in updated

    @pytest.mark.asyncio
    async def test_a_link_with_query_parameters_survives(self, monkeypatch):
        _, _, updated = await self._answer(monkeypatch)
        assert "<https://ci.example.com/x?a=1&amp;b=2|the run>" in updated
        assert "&lt;https://" not in updated

    @pytest.mark.asyncio
    async def test_escaping_is_not_applied_twice(self, monkeypatch):
        _, _, updated = await self._answer(monkeypatch)
        assert "a &lt; b &amp; c &gt; d" in updated
        assert "&amp;lt;" not in updated
        assert "&amp;amp;" not in updated

    @pytest.mark.asyncio
    async def test_a_mention_keeps_the_escaping_it_was_sent_with(self, monkeypatch):
        _, _, updated = await self._answer(monkeypatch)
        assert "&lt;@U9&gt;" in updated

    @pytest.mark.asyncio
    async def test_the_answer_is_appended(self, monkeypatch):
        _, _, updated = await self._answer(monkeypatch)
        assert "✅ Answered: approve" in updated

    @pytest.mark.asyncio
    async def test_a_split_card_keeps_every_section(self, monkeypatch):
        body = "\n".join(f"line {i:04d}" for i in range(800))
        _, blocks, updated = await self._answer(monkeypatch, raw=body)
        assert updated.startswith(body)


class TestApprovalAttribution:
    """`answered_by="slack"` alone loses which member pressed the button."""

    async def _press(self, action_id="notif:n1:approve", value="approve"):
        channel = _channel(allow_users=["U0123ABC"])
        channel._replace_via_url = AsyncMock()
        service = MagicMock()
        service.answer_delivered_notification = AsyncMock(
            return_value={
                "status": "answered",
                "redeliver_at": None,
            }
        )
        channel._notification_service = service
        await channel._handle_interactive(
            {
                "type": "block_actions",
                "user": {"id": "U0123ABC"},
                "channel": {"id": "C1"},
                "response_url": "https://hooks.slack.test/x",
                "actions": [{"action_id": action_id, "value": value}],
                "message": {"blocks": build_notification_blocks("Ship it?", "n1")},
            }
        )
        return channel, service

    @pytest.mark.asyncio
    async def test_the_settled_card_names_who_answered(self):
        channel, service = await self._press()
        assert "(by <@U0123ABC>)" in channel._replace_via_url.await_args[0][1]
        service.answer_delivered_notification.assert_awaited_once_with(
            "n1",
            "approve",
            channel="slack",
            target="C1",
            actor="U0123ABC",
        )
