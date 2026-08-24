"""Slack channel — formatting, addressing, dispatch, and guardrails.

Most of the surface is pure functions at module level, so they need no
transport. The event handlers are driven with a real SlackChannel whose
web client and router are stubs.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.channels.base import ChannelCapability, OutboundMessage
from nerve.channels.slack import (
    MAX_MSG_LEN,
    SlackChannel,
    _md_to_slack,
    build_notification_blocks,
    build_sessions_blocks,
    format_target,
    is_slack_id,
    parse_target,
    slack_emoji_name,
    slack_to_plain,
    split_message,
)
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
    channel = SlackChannel(lambda: cfg, router=MagicMock())
    channel._web = MagicMock()
    channel._web.chat_postMessage = AsyncMock(return_value={"ts": "1.1"})
    channel._web.chat_update = AsyncMock(return_value={"ok": True})
    channel._web.chat_delete = AsyncMock(return_value={"ok": True})
    channel._web.reactions_add = AsyncMock(return_value={"ok": True})
    channel._bot_user_id = "U0BOT"
    channel.router.handle_message = AsyncMock(return_value="done")
    channel.router.get_last_session = AsyncMock(return_value=None)
    return channel


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
        # The channel outlives a reload, so the lists are read per use.
        cfg = _config(allow_users=["U1"])
        channel = SlackChannel(lambda: cfg, router=MagicMock())
        assert channel.policy.users.allow == ["U1"]
        cfg.slack.allow_users = ["U2"]
        assert channel.policy.users.allow == ["U2"]


class TestAuthorization:
    @pytest.mark.asyncio
    async def test_an_unconfigured_policy_refuses_without_calling_slack(self):
        channel = _channel()
        channel._web.users_info = AsyncMock()
        assert not await channel._authorize("U1", "D1", "im")
        channel._web.users_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_an_id_allow_list_needs_no_name_lookup(self):
        channel = _channel(allow_users=["U0123ABC"])
        channel._web.users_info = AsyncMock()
        assert await channel._authorize("U0123ABC", "D1", "im")
        channel._web.users_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_handle_allow_list_resolves_the_name(self):
        channel = _channel(allow_users=["alex"])
        channel._web.users_info = AsyncMock(
            return_value={"user": {"name": "alex", "profile": {}}},
        )
        assert await channel._authorize("U1", "D1", "im")

    @pytest.mark.asyncio
    async def test_a_failed_lookup_with_a_deny_list_refuses(self):
        channel = _channel(allow_users=["U1"], deny_users=["*-bot"])
        channel._web.users_info = AsyncMock(side_effect=RuntimeError("no scope"))
        assert not await channel._authorize("U1", "D1", "im")

    @pytest.mark.asyncio
    async def test_resolved_names_are_cached(self):
        channel = _channel(allow_users=["alex"])
        channel._web.users_info = AsyncMock(
            return_value={"user": {"name": "alex", "profile": {}}},
        )
        await channel._authorize("U1", "D1", "im")
        await channel._authorize("U1", "D1", "im")
        assert channel._web.users_info.await_count == 1

    @pytest.mark.asyncio
    async def test_a_direct_message_is_matched_as_dm(self):
        channel = _channel(allow_users=["U1"], allow_channels=["dm"])
        assert await channel._authorize("U1", "D1", "im")


class TestMessageEvents:
    @pytest.mark.asyncio
    async def test_a_direct_message_reaches_the_router(self):
        channel = _channel(allow_users=["U1"])
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
        channel = _channel(allow_users=["U-other"])
        await channel._handle_message_event({
            "type": "message", "channel": "D1", "channel_type": "im",
            "user": "U1", "ts": "1.1", "text": "hello",
        })
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_bots_own_message_is_ignored(self):
        channel = _channel(allow_users=["U0BOT"])
        await channel._handle_message_event({
            "type": "message", "channel": "D1", "channel_type": "im",
            "user": "U0BOT", "ts": "1.1", "text": "hi",
        })
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_join_notice_is_ignored(self):
        channel = _channel(allow_users=["U1"])
        await channel._handle_message_event({
            "type": "message", "subtype": "channel_join",
            "channel": "C1", "user": "U1", "ts": "1.1", "text": "joined",
        })
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_channel_chatter_without_a_mention_is_ignored(self):
        # Adding the bot to a busy channel must not start a turn per remark.
        channel = _channel(allow_users=["U1"])
        await channel._handle_message_event({
            "type": "message", "channel": "C1", "channel_type": "channel",
            "user": "U1", "ts": "1.1", "text": "morning all",
        })
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_mention_in_a_channel_is_answered(self):
        channel = _channel(allow_users=["U1"])
        await channel._handle_message_event({
            "type": "message", "channel": "C1", "channel_type": "channel",
            "user": "U1", "ts": "1.1", "text": "<@U0BOT> status?",
        })
        msg = channel.router.handle_message.await_args[0][0]
        assert msg.text == "status?"

    @pytest.mark.asyncio
    async def test_a_channel_reply_opens_a_thread_on_the_message(self):
        channel = _channel(allow_users=["U1"])
        await channel._handle_message_event({
            "type": "message", "channel": "C1", "channel_type": "channel",
            "user": "U1", "ts": "1.1", "text": "<@U0BOT> hi",
        })
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
        await channel._handle_message_event({
            "type": "message", "channel": "C1", "channel_type": "channel",
            "user": "U1", "ts": "1.2", "thread_ts": "1.0", "text": "and then?",
        })
        channel.router.handle_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_reply_in_thread_off_keeps_one_session_per_channel(self):
        channel = _channel(allow_users=["U1"], reply_in_thread=False)
        await channel._handle_message_event({
            "type": "message", "channel": "C1", "channel_type": "channel",
            "user": "U1", "ts": "1.1", "thread_ts": "1.0",
            "text": "<@U0BOT> hi",
        })
        msg = channel.router.handle_message.await_args[0][0]
        assert msg.channel_key == "slack:C1"

    @pytest.mark.asyncio
    async def test_a_redelivered_event_runs_once(self):
        # Slack retries anything it thinks was not acked.
        channel = _channel(allow_users=["U1"])
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
        channel = _channel(allow_users=["U1"])
        await channel._handle_message_event({
            "type": "message", "channel": "D1", "channel_type": "im",
            "user": "U1", "ts": "1.1", "text": "",
        })
        channel.router.handle_message.assert_not_called()


class TestReactionEvents:
    @pytest.mark.asyncio
    async def test_a_reaction_on_a_known_message_reaches_the_router(self):
        channel = _channel(allow_users=["U1"])
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
        await channel._handle_reaction_event({
            "type": "reaction_added", "user": "U1", "reaction": "tada",
            "item": {"channel": "D1", "ts": "9.9"},
        })
        channel.router.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_an_unauthorized_reaction_is_ignored(self):
        channel = _channel(allow_users=["U-other"])
        channel._cache_message("1.1", "D1", "the original")
        await channel._handle_reaction_event({
            "type": "reaction_added", "user": "U1", "reaction": "tada",
            "item": {"channel": "D1", "ts": "1.1"},
        })
        channel.router.handle_message.assert_not_called()


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
        channel = _channel(allow_users=["U1"])
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
    async def test_the_watchdog_reconnects_a_dropped_socket(self, monkeypatch):
        # is_connected() is a coroutine. Reading it without awaiting yields a
        # truthy coroutine object, so the watchdog would call a dead socket
        # healthy forever and never reconnect.
        import nerve.channels.slack as slack_module

        monkeypatch.setattr(slack_module, "WATCHDOG_INTERVAL", 0.01)
        channel = _channel()
        dead = MagicMock()
        dead.is_connected = AsyncMock(return_value=False)
        dead.close = AsyncMock()
        channel._client = dead

        fresh = MagicMock()
        fresh.is_connected = AsyncMock(return_value=True)
        fresh.connect = AsyncMock()
        channel._build_socket_client = MagicMock(return_value=fresh)

        task = asyncio.create_task(channel._run_watchdog())
        await asyncio.sleep(0.05)
        channel._stopping = True
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        fresh.connect.assert_awaited()
        # The old socket must be closed first. Slack gives each event to one
        # connection only, so a leftover one quietly takes a share of them.
        dead.close.assert_awaited()
        assert channel._client is fresh

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
        channel._web.users_info = AsyncMock(return_value={
            "user": {"id": "U999", "name": "ALICE", "profile": {"email": "a@b.c"}},
        })
        channel._web.conversations_info = AsyncMock(return_value={
            "channel": {"id": "C0456DEF", "name": "eng"},
        })
        assert not await channel._authorize("U999", "C0456DEF", "channel")
        channel._web.users_info.assert_awaited()

    @pytest.mark.asyncio
    async def test_a_missing_email_refuses_an_email_deny_rule(self):
        # users.info answers 200 without profile.email when the token lacks
        # users:read.email, so the deny pattern silently matched nothing.
        channel = _channel(allow_users=["U999"], deny_users=["blocked@x.com"])
        channel._web.users_info = AsyncMock(return_value={
            "user": {"id": "U999", "name": "blocked", "profile": {}},
        })
        assert not await channel._authorize("U999", "D1", "im")

    @pytest.mark.asyncio
    async def test_an_email_deny_rule_still_works_with_the_scope(self):
        channel = _channel(allow_users=["*"], deny_users=["blocked@x.com"])
        channel._web.users_info = AsyncMock(return_value={
            "user": {"id": "U9", "name": "b", "profile": {"email": "blocked@x.com"}},
        })
        assert not await channel._authorize("U9", "D1", "im")

    @pytest.mark.asyncio
    async def test_an_innocent_user_is_not_caught_by_an_email_deny_rule(self):
        channel = _channel(allow_users=["*"], deny_users=["blocked@x.com"])
        channel._web.users_info = AsyncMock(return_value={
            "user": {"id": "U1", "name": "ok", "profile": {"email": "ok@x.com"}},
        })
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


class TestNotificationBlockLimits:
    def test_options_are_chunked_to_slacks_actions_limit(self):
        import nerve.channels.slack as slack_module

        options = [(f"opt{i}", f"v{i}") for i in range(60)]
        blocks = build_notification_blocks("pick", "n1", options)
        actions = [b for b in blocks if b["type"] == "actions"]
        assert all(
            len(b["elements"]) <= slack_module._MAX_ACTION_ELEMENTS for b in actions
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
        await channel._handle_message_event(
            self._event(bot_id="B0OTHERAPP", text="<@U0BOT> via an integration"),
        )
        msg = channel.router.handle_message.await_args[0][0]
        assert msg.text == "via an integration"

    @pytest.mark.asyncio
    async def test_another_bot_is_still_ignored(self):
        # Loop prevention now rests on the subtype, which is what a message
        # with no human behind it carries.
        channel = self._ch()
        await channel._handle_message_event(
            self._event(subtype="bot_message", bot_id="B0OTHERAPP", user=None),
        )
        channel.router.handle_message.assert_not_called()


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
        channel.router.engine.stop_session = AsyncMock(return_value=True)
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
        channel.router.engine.stop_session.assert_awaited_once_with("s1")
        said = channel._web.chat_postEphemeral.await_args.kwargs["text"]
        assert "s1" in said and "thread" in said

    @pytest.mark.asyncio
    async def test_nothing_live_says_so_plainly(self):
        channel = self._ch([])
        await channel._cmd_stop("C1", "U1", "slack:C1")
        channel.router.engine.stop_session.assert_not_called()
        assert "No active session" in channel._web.chat_postEphemeral.await_args.kwargs["text"]

    @pytest.mark.asyncio
    async def test_several_live_sessions_ask_instead_of_guessing(self):
        # Stopping someone else's thread silently would be worse than asking.
        channel = self._ch([
            self._row("s1", thread="1.0", title="Deploy"),
            self._row("s2", thread="2.0", title="Triage"),
        ])
        await channel._cmd_stop("C1", "U1", "slack:C1")
        channel.router.engine.stop_session.assert_not_called()
        blocks = channel._web.chat_postEphemeral.await_args.kwargs["blocks"]
        action_ids = [
            e["action_id"]
            for b in blocks if b["type"] == "actions" for e in b["elements"]
        ]
        assert action_ids == ["sessstop:s1", "sessstop:s2"]

    @pytest.mark.asyncio
    async def test_the_picker_button_stops_the_chosen_session(self):
        channel = self._ch([])
        channel._replace_via_url = AsyncMock()
        await channel._handle_interactive({
            "type": "block_actions",
            "user": {"id": "U1"},
            "channel": {"id": "C1"},
            "response_url": "https://hooks.slack.test/x",
            "actions": [{"action_id": "sessstop:s2", "value": "s2"}],
        })
        channel.router.engine.stop_session.assert_awaited_once_with("s2")

    @pytest.mark.asyncio
    async def test_another_channels_sessions_are_not_touched(self):
        # "slack:C1" is a prefix of "slack:C12", so the query result is
        # re-checked per row.
        channel = self._ch([
            {"channel_key": "slack:C12:9.9", "session_id": "other"},
            self._row("mine", thread="1.0"),
        ])
        found = await channel._live_sessions_for_channel("C1")
        assert [r["session_id"] for r in found] == ["mine"]

    @pytest.mark.asyncio
    async def test_star_also_resolves_across_threads(self):
        channel = self._ch([self._row("s1", thread="1.0")])
        channel.router.set_session_starred = AsyncMock(return_value=True)
        await channel._cmd_star("C1", "U1", "slack:C1", True)
        channel.router.set_session_starred.assert_awaited_once_with("s1", True)


class TestCommandExposure:
    def _ch(self, **kw):
        channel = _channel(allow_users=["U1"], **kw)
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
        assert {"new", "stop", "star", "unstar"} <= enabled

    def test_globally_scoped_commands_are_off_by_default(self):
        # sessions lists and attaches every session in the instance, and
        # reply answers whichever question is pending anywhere. In a
        # workspace where several people may DM the bot, that is one
        # member reading and continuing another's work.
        enabled = self._ch().enabled_commands
        assert "sessions" not in enabled
        assert "reply" not in enabled

    @pytest.mark.asyncio
    async def test_sessions_is_refused_unless_it_was_asked_for(self):
        channel = self._ch()
        channel.router.list_sessions = AsyncMock(return_value=[])
        said = await self._run(channel, "sessions")
        assert "turned off" in said
        channel.router.list_sessions.assert_not_called()

    @pytest.mark.asyncio
    async def test_reply_is_refused_unless_it_was_asked_for(self):
        channel = self._ch()
        channel._notification_service = MagicMock()
        said = await self._run(channel, "reply yes")
        assert "turned off" in said

    def test_both_are_still_available_on_request(self):
        enabled = self._ch(commands=["sessions", "reply"]).enabled_commands
        assert enabled == frozenset({"sessions", "reply"})

    def test_an_explicit_list_narrows_the_set(self):
        assert self._ch(commands=["reply"]).enabled_commands == frozenset({"reply"})

    def test_an_empty_list_turns_every_command_off(self):
        assert self._ch(commands=[]).enabled_commands == frozenset()

    def test_all_enables_everything(self):
        from nerve.config import SLACK_ALL_COMMANDS

        assert self._ch(commands=["all"]).enabled_commands == frozenset(SLACK_ALL_COMMANDS)

    @pytest.mark.asyncio
    async def test_a_disabled_command_is_refused_not_run(self):
        channel = self._ch(commands=["reply"])
        channel.router.engine.stop_session = AsyncMock()
        said = await self._run(channel, "stop")
        assert "turned off" in said
        channel.router.engine.stop_session.assert_not_called()

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
    """A slash command can only name ``slack:<channel>``.

    With ``reply_in_thread`` on, a channel message opens a thread and routes
    to ``slack:<channel>:<ts>``, so a session bound at channel level was
    never read again: `/nerve new` reported a new session, left the running
    thread alone, and the next mention started somewhere else.
    """

    def _ch(self, **kw):
        channel = _channel(allow_users=["U1"], **kw)
        channel._web.chat_postEphemeral = AsyncMock(return_value={"ok": True})
        channel.router.create_session = AsyncMock(return_value="s-new")
        channel.router.switch_session = AsyncMock()
        channel.router.engine.stop_session = AsyncMock(return_value=True)
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
    async def test_a_dm_thread_keeps_a_session_of_its_own(self):
        # A reply inside a DM thread is its own conversation, so it does not
        # pick up what the command bound to the DM itself.
        channel = self._ch()
        routed = await self._route(
            channel, channel="D1", channel_type="im", ts="1.2", thread_ts="1.0",
        )
        assert routed == "slack:D1:1.0"

    @pytest.mark.asyncio
    async def test_a_threaded_channel_refuses_rather_than_orphaning_a_session(self):
        channel = self._ch()
        routed = await self._route(
            channel, channel="C1", channel_type="channel", ts="1.1",
        )
        assert routed == "slack:C1:1.1"

        said = await self._run(channel, "C1", "new")
        channel.router.create_session.assert_not_called()
        channel.router.engine.stop_session.assert_not_called()
        assert "needs a thread" in said

    @pytest.mark.asyncio
    async def test_a_thread_reply_is_not_stopped_by_a_channel_command(self):
        channel = self._ch()
        await self._route(
            channel, channel="C1", channel_type="channel",
            ts="1.2", thread_ts="1.0",
        )
        await self._run(channel, "C1", "new")
        channel.router.engine.stop_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_an_unthreaded_channel_binds_the_key_a_message_reads(self):
        channel = self._ch(reply_in_thread=False)
        routed = await self._route(
            channel, channel="C1", channel_type="channel", ts="1.1",
        )
        await self._run(channel, "C1", "new")
        bound = channel.router.create_session.await_args[0][0]
        assert bound == routed == "slack:C1"

    @pytest.mark.asyncio
    async def test_an_unthreaded_thread_reply_shares_the_channel_session(self):
        channel = self._ch(reply_in_thread=False)
        routed = await self._route(
            channel, channel="C1", channel_type="channel",
            ts="1.2", thread_ts="1.0",
        )
        assert routed == "slack:C1"

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
        import nerve.channels.slack as slack_module

        blocks = build_notification_blocks("z" * 400_000, "n1")
        sections = self._sections(blocks)
        assert len(sections) == slack_module._MAX_SECTION_BLOCKS
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
        service.handle_answer = AsyncMock(return_value=True)
        service.db.get_notification = AsyncMock(return_value=None)
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
        service.handle_answer = AsyncMock(return_value=True)
        service.db.get_notification = AsyncMock(return_value=None)
        channel._notification_service = service
        await channel._handle_interactive({
            "type": "block_actions",
            "user": {"id": "U0123ABC"},
            "channel": {"id": "C1"},
            "response_url": "https://hooks.slack.test/x",
            "actions": [{"action_id": action_id, "value": value}],
            "message": {"blocks": build_notification_blocks("Ship it?", "n1")},
        })
        return channel, service

    @pytest.mark.asyncio
    async def test_the_settled_card_names_who_answered(self):
        channel, _ = await self._press()
        assert "(by <@U0123ABC>)" in channel._replace_via_url.await_args[0][1]
