"""Live Slack integration tests.

Everything here skips unless the credentials in :data:`tests.slack_live.SETUP`
are present, so the ordinary suite and CI on a fork are unaffected.

The unit tests already prove the channel is self-consistent. These exist for
the claims that only Slack can settle: whether a Block Kit payload is
accepted, whether an emoji short name exists, whether ``users.info`` really
withholds an email rather than failing, and whether an event makes the whole
trip from a human's keystroke to an InboundMessage.

Run just these with::

    pytest tests/test_slack_live.py -v
"""

from __future__ import annotations

import asyncio
import time
import uuid
from types import SimpleNamespace

import pytest
import pytest_asyncio

from nerve.agent.tools.handlers.notifications import send_channel_message_handler
from nerve.agent.tools.registry import ToolContext
from nerve.channels.base import OutboundMessage
from nerve.channels.router import ChannelRouter
from nerve.channels.slack import (
    format_target,
    is_slack_id,
    SlackChannel,
)
from nerve.channels.slack_presentation import (
    _EMOJI_TO_SLACK,
    MAX_MSG_LEN,
    _md_to_slack,
    build_notification_blocks,
    split_message,
)
from tests.slack_live import (
    BOT_TOKEN,
    HAVE_OUTBOUND,
    NO_EMAIL_BOT_TOKEN,
    TEST_CHANNEL,
    USER_TOKEN,
    Posted,
    RecordingRouter,
    build_channel,
    build_outbound_channel,
    direct_message_guardrails,
    make_client,
    requires_no_email_token,
    requires_outbound,
    start_event_sink,
    wait_until_receiving,
)

# One event loop for the whole module. The fixtures below hold aiohttp
# sessions, and a per-function loop leaves those bound to a loop that has
# already closed.
pytestmark = pytest.mark.asyncio(loop_scope="module")


async def test_the_diagnostics_wrapper_preserves_socket_builder_arguments(
    monkeypatch,
):
    calls = []
    socket = SimpleNamespace(
        message_listeners=[],
        socket_mode_request_listeners=[],
    )

    def build_socket(_channel, *, app_token=None, web_client=None):
        calls.append((app_token, web_client))
        return socket

    monkeypatch.setattr(SlackChannel, "_build_socket_client", build_socket)
    channel, _ = build_channel(
        RecordingRouter(), diagnostics_label="wrapper-test",
    )
    web_client = object()

    assert channel._build_socket_client(
        app_token="xapp-test", web_client=web_client,
    ) is socket
    assert calls == [("xapp-test", web_client)]


@pytest.fixture(autouse=True)
def _keep_the_measured_clock_offset():
    """Leave the harness calibration as the live fixtures found it.

    It is module state measured from a real Slack timestamp, so a test that
    fakes it must not hand a fabricated offset to the live tests.
    """
    import tests.slack_live as harness

    saved = harness._clock_offset
    yield
    harness._clock_offset = saved


class TestStalenessCutoff:
    """The cutoff has to survive a runner clock that disagrees with Slack."""

    async def test_an_uncalibrated_harness_calls_nothing_stale(self, monkeypatch):
        import tests.slack_live as harness

        monkeypatch.setattr(harness, "_clock_offset", None)
        assert harness.event_age_seconds(time.time()) is None

    async def test_a_fresh_event_is_fresh_despite_a_skewed_runner(self, monkeypatch):
        # The runner sits ten minutes ahead of Slack. Comparing the two
        # clocks directly made every fresh event look stale, and a dropped
        # event reads as a refusal on the guardrail tests.
        import tests.slack_live as harness

        slack_now = 1_700_000_000.0
        monkeypatch.setattr(harness.time, "time", lambda: slack_now + 600.0)
        harness.note_slack_timestamp(str(slack_now))
        age = harness.event_age_seconds(slack_now)
        assert age is not None
        assert abs(age) < 1.0
        assert age <= harness.EVENT_TIMEOUT

    async def test_an_event_from_an_earlier_run_is_still_stale(self, monkeypatch):
        import tests.slack_live as harness

        slack_now = 1_700_000_000.0
        monkeypatch.setattr(harness.time, "time", lambda: slack_now + 600.0)
        harness.note_slack_timestamp(str(slack_now))
        age = harness.event_age_seconds(slack_now - 3600.0)
        assert age is not None
        assert age > harness.EVENT_TIMEOUT

    async def test_a_missing_stamp_is_not_stale(self, monkeypatch):
        import tests.slack_live as harness

        harness.note_slack_timestamp("1700000000.000100")
        assert harness.event_age_seconds(None) is None
        assert harness.event_age_seconds("not-a-number") is None


class TestRefusalControl:
    """A quiet router only means a refusal if the event actually arrived."""

    @staticmethod
    def _req(text: str):
        return SimpleNamespace(
            payload={"event": {"text": text}},
            retry_attempt=0,
            retry_reason=None,
            type="events_api",
            envelope_id="e1",
        )

    async def test_diagnostics_separate_forwarded_from_dropped(self):
        from tests.slack_live import SocketDiagnostics

        diagnostics = SocketDiagnostics("control-test")
        diagnostics.note_forwarded(self._req("hello nvz-aaa"))
        diagnostics.note_dropped(self._req("stale nvz-bbb"))

        assert diagnostics.forwarded("nvz-aaa")
        assert not diagnostics.dropped("nvz-aaa")
        assert diagnostics.dropped("nvz-bbb")
        # The distinction is the point: a dropped envelope never met the
        # policy, so it cannot stand in for a refusal.
        assert not diagnostics.forwarded("nvz-bbb")
        assert not diagnostics.forwarded("nvz-never-sent")

    async def test_a_refusal_needs_the_envelope_to_have_arrived(self):
        from tests.slack_live import SocketDiagnostics

        router = RecordingRouter()
        diagnostics = SocketDiagnostics("control-test")
        channel = SimpleNamespace(
            _client=SimpleNamespace(_live_diagnostics=diagnostics),
        )
        with pytest.raises(AssertionError, match="never delivered it"):
            await router.expect_no_message("nvz-absent", channel, timeout=0.3)

    async def test_a_dropped_envelope_does_not_count_as_a_refusal(self):
        from tests.slack_live import SocketDiagnostics

        router = RecordingRouter()
        diagnostics = SocketDiagnostics("control-test")
        diagnostics.note_dropped(self._req("stale nvz-old"))
        channel = SimpleNamespace(
            _client=SimpleNamespace(_live_diagnostics=diagnostics),
        )
        with pytest.raises(AssertionError, match="dropped it as stale"):
            await router.expect_no_message("nvz-old", channel, timeout=0.3)


async def test_live_direct_messages_use_the_explicit_guardrail():
    assert direct_message_guardrails("U123") == {
        "allow_users": ["U123"],
        "allow_direct_messages": True,
    }


# ---------------------------------------------------------------------- #
#  Fixtures                                                               #
# ---------------------------------------------------------------------- #


@pytest_asyncio.fixture(scope="module", loop_scope="module", autouse=True)
async def _ack_outbound_events():
    """Keep this module's Web API mutations from poisoning a later run.

    Each post, edit, reaction, upload, and cleanup can produce a Socket Mode
    envelope.  With no socket open Slack retries those envelopes at +60s and
    again around +5min; the pending retries are what made a later inbound
    connection miss fresh events.  The sink shares this module's lifecycle,
    so it is connected before the first mutation and closes after cleanup.
    """
    if not HAVE_OUTBOUND:
        yield
        return

    sink = await start_event_sink(diagnostics_label="outbound")
    try:
        # A WebSocket handshake is not sufficient when an older retry schedule
        # exists.  Prove Slack is routing fresh events here before tests post.
        await wait_until_receiving(sink)
        yield
        # Fence fixture cleanup as well: Posted deletes messages after the
        # tests, and closing before those events arrive would recreate the
        # exact backlog this fixture exists to prevent.
        await wait_until_receiving(sink)
    finally:
        await sink.close()
        sink._live_diagnostics.emit_summary()


@pytest_asyncio.fixture(loop_scope="module")
async def bot():
    """A Web API client on the bot token."""
    if not BOT_TOKEN:
        pytest.skip("no bot token")
    yield make_client(BOT_TOKEN)


@pytest_asyncio.fixture(loop_scope="module")
async def human():
    """A Web API client on the user token, for posting as a person."""
    if not USER_TOKEN:
        pytest.skip("no user token")
    yield make_client(USER_TOKEN)


@pytest_asyncio.fixture(loop_scope="module")
async def posted(bot, _ack_outbound_events):
    """Track messages the test creates and delete them afterwards."""
    tracker = Posted()
    yield tracker
    for token, entries in ((BOT_TOKEN, tracker.bot), (USER_TOKEN, tracker.user)):
        if not token or not entries:
            continue
        client = make_client(token)
        for channel, ts in entries:
            try:
                await client.chat_delete(channel=channel, ts=ts)
            except Exception:
                pass  # A test that already deleted it, or a stale ts.
    if BOT_TOKEN and tracker.files:
        client = make_client(BOT_TOKEN)
        for file_id in tracker.files:
            try:
                await client.files_delete(file=file_id)
            except Exception:
                pass  # Already gone, or removed with its message.


# ---------------------------------------------------------------------- #
#  Outbound — what Slack accepts                                          #
# ---------------------------------------------------------------------- #


@requires_outbound
class TestSlackAcceptsWhatWeSend:
    async def test_the_bot_authenticates_and_the_channel_id_is_real(self, bot):
        auth = await bot.auth_test()
        assert auth["ok"]
        assert is_slack_id(auth["user_id"]), auth["user_id"]
        info = await bot.conversations_info(channel=TEST_CHANNEL)
        assert info["ok"], "the bot must be invited to NERVE_SLACK_TEST_CHANNEL"

    async def test_converted_markdown_survives_a_round_trip(self, bot, posted):
        source = (
            "## Heading\n"
            "**bold** and *italic* and `code`\n"
            "- bullet one\n"
            "- bullet two\n"
            "[docs](https://example.com/a?x=1&y=2)\n"
            "```\nliteral **not bold**\n```\n"
            "a < b & c > d"
        )
        sent = _md_to_slack(source)
        resp = await bot.chat_postMessage(channel=TEST_CHANNEL, text=sent)
        posted.note_bot(TEST_CHANNEL, resp["ts"])

        history = await bot.conversations_history(
            channel=TEST_CHANNEL, latest=resp["ts"], inclusive=True, limit=1,
        )
        stored = history["messages"][0]["text"]
        # Slack stores mrkdwn verbatim. A mismatch means the converter emitted
        # something Slack rewrote, which is the bug the fake cannot see.
        assert stored == sent

    async def test_a_long_reply_splits_into_messages_slack_accepts(
        self, bot, posted,
    ):
        body = "\n".join(f"line {i} " + "x" * 60 for i in range(200))
        chunks = split_message(body, MAX_MSG_LEN)
        assert len(chunks) > 1, "test needs a body that actually splits"
        for chunk in chunks:
            resp = await bot.chat_postMessage(
                channel=TEST_CHANNEL, text=_md_to_slack(chunk),
            )
            posted.note_bot(TEST_CHANNEL, resp["ts"])
            assert resp["ok"]

    async def test_a_notification_card_is_valid_block_kit(self, bot, posted):
        blocks = build_notification_blocks(
            "Deploy to production?", "n-live-1",
            [("✅ Approve", "approve"), ("❌ Decline", "decline"),
             ("💤 Snooze 24h", "snooze_24h")],
        )
        resp = await bot.chat_postMessage(
            channel=TEST_CHANNEL, text="Deploy to production?", blocks=blocks,
        )
        posted.note_bot(TEST_CHANNEL, resp["ts"])
        assert resp["ok"]

    async def test_a_long_option_list_is_chunked_below_the_actions_limit(
        self, bot, posted,
    ):
        # Slack rejects the whole message with invalid_blocks past 25
        # elements in one actions block. Its validator is the authority here,
        # not our arithmetic.
        options = [(f"Option {i}", f"v{i}") for i in range(60)]
        blocks = build_notification_blocks("Pick one", "n-live-2", options)
        resp = await bot.chat_postMessage(
            channel=TEST_CHANNEL, text="Pick one", blocks=blocks,
        )
        posted.note_bot(TEST_CHANNEL, resp["ts"])
        assert resp["ok"]

    async def test_clearing_blocks_removes_the_buttons(self, bot, posted):
        # The notification-expiry path relies on blocks=[] dropping the dead
        # buttons rather than being ignored as falsy.
        blocks = build_notification_blocks(
            "Answer me", "n-live-3", [("Yes", "yes"), ("No", "no")],
        )
        resp = await bot.chat_postMessage(
            channel=TEST_CHANNEL, text="Answer me", blocks=blocks,
        )
        posted.note_bot(TEST_CHANNEL, resp["ts"])

        await bot.chat_update(
            channel=TEST_CHANNEL, ts=resp["ts"],
            text="Answer me\n\n⏰ Expired unanswered", blocks=[],
        )
        history = await bot.conversations_history(
            channel=TEST_CHANNEL, latest=resp["ts"], inclusive=True, limit=1,
        )
        message = history["messages"][0]
        # Slack does not leave the message block-less: it synthesises a
        # rich_text block from the new text. What must be gone is the
        # actions block, because that is what carries the dead buttons.
        kinds = {b.get("type") for b in message.get("blocks") or []}
        assert "actions" not in kinds, f"expired card still has buttons: {kinds}"
        assert "Expired" in message["text"]

    async def test_every_mapped_emoji_short_name_exists(self, bot, posted):
        """Every entry in the emoji table must be a name Slack knows.

        The table is hand-written, and a wrong short name fails at
        ``reactions.add`` with ``invalid_name``, which the production path
        swallows — the agent's reaction just never appears.

        Reactions are spread over several anchor messages because Slack caps
        the distinct reactions on one message at about two dozen, and one
        test reports every bad name at once rather than making you re-run to
        find the next.
        """
        per_anchor = 15
        items = sorted(set(_EMOJI_TO_SLACK.items()), key=lambda kv: kv[1])
        rejected: list[str] = []

        for start in range(0, len(items), per_anchor):
            anchor = await bot.chat_postMessage(
                channel=TEST_CHANNEL, text=f"emoji probe {start}",
            )
            posted.note_bot(TEST_CHANNEL, anchor["ts"])
            for emoji, name in items[start:start + per_anchor]:
                try:
                    await bot.reactions_add(
                        channel=TEST_CHANNEL, timestamp=anchor["ts"], name=name,
                    )
                except Exception as exc:
                    if "already_reacted" not in str(exc):
                        rejected.append(f"{emoji} → :{name}: ({exc})")
                await asyncio.sleep(0.3)

        assert not rejected, "Slack rejected these reactions:\n" + "\n".join(rejected)

    async def test_a_file_upload_lands_in_the_conversation(
        self, bot, tmp_path, posted,
    ):
        from nerve.channels.slack import SlackChannel
        from nerve.config import NerveConfig, SlackConfig

        path = tmp_path / "report.txt"
        path.write_text("nerve live upload\n", encoding="utf-8")

        cfg = NerveConfig()
        cfg.slack = SlackConfig(bot_token=BOT_TOKEN, app_token="unused")
        channel = SlackChannel(cfg, router=None)  # type: ignore[arg-type]
        channel._web = bot
        channel._state = "running"
        assert await channel.send_file(format_target(TEST_CHANNEL), str(path))

        # chat.delete cannot remove an upload, so the file id has to be
        # recorded for files.delete or the scratch channel keeps every copy.
        listed = await bot.files_list(channel=TEST_CHANNEL, count=20)
        for entry in listed.get("files") or []:
            if entry.get("name") == "report.txt":
                posted.note_file(entry.get("id"))

    async def test_streaming_edits_then_removes_the_placeholder(
        self, bot, posted,
    ):
        placeholder = await bot.chat_postMessage(channel=TEST_CHANNEL, text="⏳")
        ts = placeholder["ts"]
        # Tracked before the edits, so a rate limit part way through the loop
        # does not leave the placeholder behind. Teardown tolerates a ts this
        # test has already deleted itself.
        posted.note_bot(TEST_CHANNEL, ts)
        for fragment in ("partial one", "partial one and two"):
            await bot.chat_update(
                channel=TEST_CHANNEL, ts=ts, text=_md_to_slack(fragment),
            )
            await asyncio.sleep(1.2)   # the per-channel chat.update limit
        final = await bot.chat_postMessage(channel=TEST_CHANNEL, text="final answer")
        posted.note_bot(TEST_CHANNEL, final["ts"])
        await bot.chat_delete(channel=TEST_CHANNEL, ts=ts)

        history = await bot.conversations_history(
            channel=TEST_CHANNEL, latest=ts, inclusive=True, limit=1,
        )
        assert not history["messages"] or history["messages"][0]["ts"] != ts


# ---------------------------------------------------------------------- #
#  Scope behaviour — the premise the deny-list rule rests on              #
# ---------------------------------------------------------------------- #


@requires_outbound
@requires_no_email_token
class TestScopeOmission:
    async def test_users_info_omits_email_without_the_scope_instead_of_failing(
        self, bot,
    ):
        """A token without ``users:read.email`` still gets a 200.

        This is why an email deny rule cannot be trusted on a short response:
        the absent field looks exactly like a user who has no email, so the
        rule matches nothing and would admit the person it names. The channel
        refuses instead, and this test is what says that premise is real.
        """
        auth = await bot.auth_test()
        with_scope = await bot.users_info(user=auth["user_id"])
        assert with_scope["user"]["profile"].get("email"), (
            "the main bot token needs users:read.email for this comparison"
        )

        limited = make_client(NO_EMAIL_BOT_TOKEN)
        response = await limited.users_info(user=auth["user_id"])
        assert response["ok"], "expected a successful response, not an error"
        assert not response["user"]["profile"].get("email"), (
            "the no-email token returned an email; it still has the scope"
        )


# ---------------------------------------------------------------------- #
#  Addressed delivery — the agent names the destination                   #
# ---------------------------------------------------------------------- #


@requires_outbound
class TestAddressedDelivery:
    """``send_channel_message``, end to end against the real workspace.

    The unit tests settle the policy branches, which are pure. What they
    cannot settle is whether the conversation Slack describes is the one
    ``allow_channels`` was written against: a name grant matches
    ``conversations.info``'s ``name`` field, and a fixture returning
    ``{"name": "general"}`` proves only that the test knows what the code
    reads. These run the same policy over Slack's own answer, then post.
    """

    async def test_the_scratch_channel_resolves_to_a_name_we_can_grant_on(
        self, bot,
    ):
        # The premise the two name tests below rest on. Slack returns the
        # name without a leading '#', which is what allow_channels matches.
        info = await bot.conversations_info(channel=TEST_CHANNEL)
        name = info["channel"]["name"]
        assert name and not name.startswith("#"), name

    async def test_an_id_grant_posts_to_the_conversation(self, bot, posted):
        channel = build_outbound_channel(allow_channels=[TEST_CHANNEL])
        marker = f"nvz-outbound-{uuid.uuid4().hex[:8]}"

        verdict = await channel.authorize_outbound(TEST_CHANNEL)
        assert verdict.allowed, verdict.reason
        await channel.send(OutboundMessage(target=TEST_CHANNEL, text=marker))

        ts = await _find_posted(bot, marker)
        posted.note_bot(TEST_CHANNEL, ts)
        assert ts, "the message never reached the conversation"

    async def test_a_name_grant_matches_what_slack_calls_the_conversation(
        self, bot, posted,
    ):
        info = await bot.conversations_info(channel=TEST_CHANNEL)
        channel = build_outbound_channel(
            allow_channels=[info["channel"]["name"]],
        )
        marker = f"nvz-outbound-name-{uuid.uuid4().hex[:8]}"

        verdict = await channel.authorize_outbound(TEST_CHANNEL)
        assert verdict.allowed, verdict.reason
        await channel.send(OutboundMessage(target=TEST_CHANNEL, text=marker))

        ts = await _find_posted(bot, marker)
        posted.note_bot(TEST_CHANNEL, ts)
        assert ts

    async def test_a_deny_pattern_on_the_real_name_refuses_it(self, bot):
        info = await bot.conversations_info(channel=TEST_CHANNEL)
        channel = build_outbound_channel(
            allow_channels=[TEST_CHANNEL],
            deny_channels=[info["channel"]["name"]],
        )

        verdict = await channel.authorize_outbound(TEST_CHANNEL)

        assert not verdict.allowed

    async def test_a_name_slack_cannot_resolve_cannot_clear_a_deny_list(self):
        # Slack answers channel_not_found, so the identity is short the name
        # the deny list is written against. An unread name must not walk past
        # the list that might have named it.
        channel = build_outbound_channel(
            allow_channels=["*"], deny_channels=["secrets"],
        )

        verdict = await channel.authorize_outbound("C00000000000")

        assert not verdict.allowed

    async def test_a_thread_target_posts_inside_the_thread(self, bot, posted):
        root = await bot.chat_postMessage(
            channel=TEST_CHANNEL, text="nvz-outbound-thread-root",
        )
        posted.note_bot(TEST_CHANNEL, root["ts"])
        channel = build_outbound_channel(allow_channels=[TEST_CHANNEL])
        marker = f"nvz-outbound-reply-{uuid.uuid4().hex[:8]}"
        target = format_target(TEST_CHANNEL, root["ts"])

        verdict = await channel.authorize_outbound(target)
        assert verdict.allowed, verdict.reason
        await channel.send(OutboundMessage(target=target, text=marker))

        replies = await bot.conversations_replies(
            channel=TEST_CHANNEL, ts=root["ts"],
        )
        texts = [m["text"] for m in replies["messages"]]
        assert marker in texts, texts

    async def test_the_tool_posts_through_the_router(self, bot, posted):
        # The whole path the agent actually takes: handler → router →
        # authorize_outbound → Slack.
        channel = build_outbound_channel(allow_channels=[TEST_CHANNEL])
        router = ChannelRouter(engine=SimpleNamespace(db=None))
        router.register(channel)
        ctx = ToolContext(
            session_id="live-outbound",
            engine=SimpleNamespace(router=router),
        )
        marker = f"nvz-outbound-tool-{uuid.uuid4().hex[:8]}"

        result = await send_channel_message_handler(ctx, {
            "channel": "slack", "target": TEST_CHANNEL, "text": marker,
        })

        assert not result.is_error, result.content[0]["text"]
        ts = await _find_posted(bot, marker)
        posted.note_bot(TEST_CHANNEL, ts)
        assert ts, "the tool reported success but nothing was posted"

    async def test_the_tool_refuses_a_conversation_off_the_allow_list(self, bot):
        channel = build_outbound_channel(allow_channels=["C0NOTTHISONE"])
        router = ChannelRouter(engine=SimpleNamespace(db=None))
        router.register(channel)
        ctx = ToolContext(
            session_id="live-outbound",
            engine=SimpleNamespace(router=router),
        )
        marker = f"nvz-outbound-refused-{uuid.uuid4().hex[:8]}"

        result = await send_channel_message_handler(ctx, {
            "channel": "slack", "target": TEST_CHANNEL, "text": marker,
        })

        assert "Refused" in result.content[0]["text"]
        assert await _find_posted(bot, marker) is None, "a refusal still posted"


async def _find_posted(bot, marker: str) -> "str | None":
    """The ts of the bot message carrying *marker*, or None.

    Reads recent history rather than a returned ts: ``send`` splits and
    posts without handing one back, and the question here is whether Slack
    holds the message, not whether the call returned.
    """
    history = await bot.conversations_history(channel=TEST_CHANNEL, limit=30)
    for message in history["messages"]:
        if marker in (message.get("text") or ""):
            return message["ts"]
    return None
