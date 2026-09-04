"""Live Slack tests for the inbound half — events reaching the router.

Everything here skips unless the credentials in :data:`tests.slack_live.SETUP`
are present, so the ordinary suite and CI on a fork are unaffected.

The unit tests already prove the channel is self-consistent. These exist for
the claims that only Slack can settle: whether a Block Kit payload is
accepted, whether an emoji short name exists, whether ``users.info`` really
withholds an email rather than failing, and whether an event makes the whole
trip from a human's keystroke to an InboundMessage.

These hold a Socket Mode connection, which is why they are kept apart from
the outbound tests. Slack gives each event to exactly one of an app's open
connections. The outbound module uses its own ack-only socket so its Web API
traffic cannot schedule future retries; a separate process ensures that sink
can never steal one of the inbound events asserted here.

Run with::

    pytest tests/test_slack_live_inbound.py -v

If a test here reports "no inbound message reached the router", suspect the
connection rather than the routing, and prefer diagnosing it to muting it —
every time this suite has looked flaky it has been describing something real.
The two causes found so far were a connection per test, which lost events
into the gap where Slack had not yet started routing to the new socket, and
outbound API traffic with no listener, whose scheduled retries made a later
socket deaf. Hence a single shared connection here, and an ack-only connection
around the outbound file.
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import time

import pytest
import pytest_asyncio

from types import SimpleNamespace

from nerve.channels.router import ChannelRouter
from nerve.channels.slack import format_target, slack_ts_to_iso
from nerve.config import ChannelSourceConfig
from nerve.db import Database
from nerve.sources.channel import ChannelSource
from nerve.sources.runner import SourceRunner
from tests.slack_live import (
    BOT_TOKEN,
    EVENT_TIMEOUT,
    TEST_CHANNEL,
    USER_TOKEN,
    Posted,
    RecordingRouter,
    REFUSAL_SETTLE_SECONDS,
    SOCKET_DRAIN_SECONDS,
    build_channel,
    direct_message_guardrails,
    make_client,
    wait_until_quiet,
    ignore_stale_events,
    wait_until_receiving,
    requires_inbound,
    requires_outbound,
    unique_marker,
)

# One event loop for the whole module. The fixtures below hold aiohttp
# sessions, and a per-function loop leaves those bound to a loop that has
# already closed.
pytestmark = pytest.mark.asyncio(loop_scope="module")


# ---------------------------------------------------------------------- #
#  Fixtures                                                               #
# ---------------------------------------------------------------------- #


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
async def posted(bot, _connected_channel):
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


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _connected_channel():
    """One Socket Mode connection for the whole module.

    Slack does not start routing to a freshly-opened connection the instant
    the handshake completes, and it hands each event to exactly one of an
    app's connections. Opening one per test therefore loses events into the
    gap: a message posted by one test would surface in the next test's
    router, which is what CI kept catching.

    Sharing one connection removes the churn. It works because the channel
    resolves its config per event, so a test can retarget the guardrails
    between messages — the same property a config reload depends on.

    This needs the outbound tests to live in another process. Their ack-only
    socket would otherwise be a second connection competing for exactly the
    inbound events this module asserts.
    """
    channel, cfg = build_channel(
        RecordingRouter(), diagnostics_label="inbound",
    )
    await channel.start()
    # Order matters. Old events are dropped first so a readiness retry cannot
    # reach the router, then readiness waits for the probe's own ts to prove
    # Slack is delivering fresh events to this socket.
    ignore_stale_events(channel)
    await wait_until_receiving(channel._client)
    await wait_until_quiet(channel)
    try:
        yield channel, cfg
    finally:
        # The Posted fixture deletes its messages before this module-scoped
        # connection tears down. Fence those final mutations so closing the
        # socket cannot seed the next run with a retry schedule.
        try:
            await wait_until_receiving(channel._client)
        finally:
            await channel.stop()
            channel._live_diagnostics.emit_summary()


@pytest_asyncio.fixture(loop_scope="module")
async def live_channel(_connected_channel):
    """Point the shared channel at this test's router and guardrails."""
    channel, _ = _connected_channel

    async def _use(router: RecordingRouter, **slack_kwargs):
        assert await channel._client.is_connected(), (
            "the shared Socket Mode connection went down before this test"
        )
        # Let the previous test's events land before moving the goalposts.
        # Retargeting the router and the guardrails while an event is still
        # in flight judges it under the wrong policy: a message posted by a
        # test that expects a refusal would be admitted by the next test's
        # allow list, and surface in the next test's router. CI is slower
        # than a laptop, so it saw this where local runs did not.
        await wait_until_quiet(channel)
        config = copy.deepcopy(channel.config)
        for field, default in (
            ("allow_users", []), ("deny_users", []),
            ("allow_direct_messages", False),
            ("allow_channels", []), ("deny_channels", []),
            ("commands", None),
        ):
            setattr(config.slack, field, slack_kwargs.get(field, default))
        # The source grant is reset like the rest: left standing it would
        # collect the next test's traffic under this test's policy.
        config.slack.source = slack_kwargs.get(
            "source", ChannelSourceConfig(),
        )
        channel.apply_config(config)
        channel.router = router
        router.channel = channel
        # Resolved identities are policy-specific, so a later test must not
        # inherit a verdict computed under different lists.
        channel._name_cache.clear()
        return channel, config

    return _use


# ---------------------------------------------------------------------- #
#  Inbound — the whole trip, human keystroke to InboundMessage            #
# ---------------------------------------------------------------------- #


@requires_inbound
class TestInboundLoop:
    async def test_a_mention_reaches_the_router_and_is_answered_in_thread(
        self, live_channel, human, bot, posted,
    ):
        marker = unique_marker()
        auth = await human.auth_test()
        user_id = auth["user_id"]
        router = RecordingRouter(reply_text="**live reply**")
        channel, _ = await live_channel(router, allow_users=[user_id])

        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL,
            text=f"<@{channel._bot_user_id}> live mention test {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])

        msg = await router.wait_for_message(marker)
        assert msg.channel_name == "slack"
        assert msg.channel_key == f"slack:{TEST_CHANNEL}:{sent['ts']}"
        assert msg.text == f"live mention test {marker}", (
            "the bot's own mention should be stripped before the prompt"
        )

        replies = await bot.conversations_replies(
            channel=TEST_CHANNEL, ts=sent["ts"],
        )
        bot_replies = [
            m for m in replies["messages"] if m.get("user") == channel._bot_user_id
        ]
        assert bot_replies, "the bot did not reply in the thread"
        # Recorded before the text is checked, so a mismatch does not leave
        # the bot's reply behind in the channel.
        posted.note_bot(TEST_CHANNEL, bot_replies[0]["ts"])
        assert bot_replies[0]["text"] == "*live reply*"

    async def test_channel_chatter_without_a_mention_is_left_alone(
        self, live_channel, human, posted,
    ):
        marker = unique_marker()
        auth = await human.auth_test()
        router = RecordingRouter()
        channel, _ = await live_channel(router, allow_users=[auth["user_id"]])

        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL, text=f"just talking to my colleagues {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])
        await router.expect_no_message(marker, channel)

    async def test_a_thread_reply_continues_without_another_mention(
        self, live_channel, human, posted,
    ):
        marker = unique_marker()
        auth = await human.auth_test()
        user_id = auth["user_id"]
        router = RecordingRouter()
        channel, _ = await live_channel(router, allow_users=[user_id])

        reply_marker = unique_marker()
        opener = await human.chat_postMessage(
            channel=TEST_CHANNEL, text=f"<@{channel._bot_user_id}> start a thread {marker}",
        )
        posted.note_user(TEST_CHANNEL, opener["ts"])
        await router.wait_for_message(marker)

        follow = await human.chat_postMessage(
            channel=TEST_CHANNEL, thread_ts=opener["ts"], text=f"and then? {reply_marker}",
        )
        posted.note_user(TEST_CHANNEL, follow["ts"])
        msg = await router.wait_for_message(reply_marker)
        assert msg.text == f"and then? {reply_marker}"
        assert msg.channel_key == f"slack:{TEST_CHANNEL}:{opener['ts']}"

    async def test_a_direct_message_reaches_the_router(
        self, live_channel, human, posted,
    ):
        marker = unique_marker()
        auth = await human.auth_test()
        user_id = auth["user_id"]
        router = RecordingRouter()
        channel, _ = await live_channel(
            router, **direct_message_guardrails(user_id),
        )

        dm = await human.conversations_open(users=channel._bot_user_id)
        dm_id = dm["channel"]["id"]
        sent = await human.chat_postMessage(channel=dm_id, text=f"live dm test {marker}")
        posted.note_user(dm_id, sent["ts"])

        msg = await router.wait_for_message(marker)
        assert msg.channel_key == f"slack:{dm_id}"
        assert msg.text == f"live dm test {marker}"


async def test_the_live_dm_contract_uses_the_explicit_guardrail():
    source = inspect.getsource(
        TestInboundLoop.test_a_direct_message_reaches_the_router,
    )
    assert "direct_message_guardrails(user_id)" in source


@requires_inbound
class TestGuardrailsAgainstRealSlack:
    async def test_a_denied_user_never_reaches_the_router(
        self, live_channel, human, posted,
    ):
        marker = unique_marker()
        auth = await human.auth_test()
        user_id = auth["user_id"]
        router = RecordingRouter()
        # Allowed by id, then denied by id: deny must win.
        channel, _ = await live_channel(
            router, allow_users=[user_id], deny_users=[user_id],
        )
        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL, text=f"<@{channel._bot_user_id}> let me in {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])
        await router.expect_no_message(marker, channel)

    async def test_an_unconfigured_policy_refuses_a_real_message(
        self, live_channel, human, posted,
    ):
        marker = unique_marker()
        router = RecordingRouter()
        channel, _ = await live_channel(router)   # no allow lists at all
        assert not channel.policy.configured
        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL, text=f"<@{channel._bot_user_id}> anyone home {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])
        await router.expect_no_message(marker, channel)

    async def test_allowing_by_handle_resolves_through_users_info(
        self, live_channel, human, bot, posted,
    ):
        marker = unique_marker()
        auth = await human.auth_test()
        profile = await bot.users_info(user=auth["user_id"])
        handle = profile["user"]["name"]

        router = RecordingRouter()
        channel, _ = await live_channel(router, allow_users=[handle])
        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL, text=f"<@{channel._bot_user_id}> handle test {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])
        msg = await router.wait_for_message(marker)
        assert msg.text == f"handle test {marker}"

    async def test_allowing_by_channel_name_glob_resolves_the_real_name(
        self, live_channel, human, bot, posted,
    ):
        marker = unique_marker()
        info = await bot.conversations_info(channel=TEST_CHANNEL)
        name = info["channel"]["name"]
        auth = await human.auth_test()

        router = RecordingRouter()
        channel, _ = await live_channel(
            router, allow_users=[auth["user_id"]], allow_channels=[f"{name[:3]}*"],
        )
        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL, text=f"<@{channel._bot_user_id}> glob test {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])
        msg = await router.wait_for_message(marker)
        assert msg.text == f"glob test {marker}"

    async def test_a_channel_outside_the_glob_is_refused(
        self, live_channel, human, posted,
    ):
        marker = unique_marker()
        auth = await human.auth_test()
        router = RecordingRouter()
        channel, _ = await live_channel(
            router,
            allow_users=[auth["user_id"]],
            allow_channels=["nerve-no-such-channel-*"],
        )
        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL, text=f"<@{channel._bot_user_id}> should not pass {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])
        await router.expect_no_message(marker, channel)

    async def test_a_reaction_from_a_human_is_forwarded(
        self, live_channel, human, bot, posted,
    ):
        marker = unique_marker()
        auth = await human.auth_test()
        user_id = auth["user_id"]
        router = RecordingRouter()
        channel, _ = await live_channel(router, allow_users=[user_id])

        anchor = await bot.chat_postMessage(
            channel=TEST_CHANNEL, text=f"react to me {marker}",
        )
        posted.note_bot(TEST_CHANNEL, anchor["ts"])
        # The channel only forwards reactions on messages it still has
        # context for, which is what the outbound cache is for. Every
        # shared-channel entry the channel writes carries a thread, because a
        # top-level message is the root of its own, so the target here is the
        # one production would have cached.
        channel._cache_message(
            anchor["ts"],
            format_target(TEST_CHANNEL, anchor["ts"]),
            f"react to me {marker}",
        )
        await human.reactions_add(
            channel=TEST_CHANNEL, timestamp=anchor["ts"], name="tada",
        )
        msg = await router.wait_for_message(marker)
        assert ":tada:" in msg.text


@requires_outbound
class TestReconnectWatchdog:
    async def test_the_watchdog_restores_a_dropped_socket(self):
        """Exercise reconnect only after tests that need exclusive delivery.

        This test owns its connection because it deliberately breaks it. Its
        reconnect briefly overlaps the module's shared socket, and Slack may
        hand an event to either connection, so running this test earlier can
        perturb an otherwise-correct inbound assertion.
        """
        import nerve.channels.slack as slack_module

        router = RecordingRouter()
        channel, _ = build_channel(
            router,
            diagnostics_label="watchdog",
            allow_users=["U0000000"],
        )
        await channel.start()
        original = slack_module.WATCHDOG_INTERVAL
        slack_module.WATCHDOG_INTERVAL = 1
        try:
            await channel._client.disconnect()
            await asyncio.sleep(0.5)
            assert not await channel._client.is_connected()

            deadline = time.monotonic() + EVENT_TIMEOUT
            while time.monotonic() < deadline:
                if await channel._client.is_connected():
                    break
                await asyncio.sleep(0.5)
            assert await channel._client.is_connected(), (
                "the socket stayed down; the watchdog did not recover it"
            )
        finally:
            slack_module.WATCHDOG_INTERVAL = original
            await channel.stop()
            channel._live_diagnostics.emit_summary()
            # Let Slack drop this connection before module fixture cleanup
            # relies on the shared socket receiving every deletion event.
            await asyncio.sleep(SOCKET_DRAIN_SECONDS)


# ---------------------------------------------------------------------- #
#  Channel source — what a watched conversation feeds the inbox           #
# ---------------------------------------------------------------------- #


@requires_inbound
class TestChannelSourceCollectsRealTraffic:
    """The source gate, driven by messages a real person actually sent.

    The unit tests hand ``_observe`` an event dict the test wrote, so they
    settle the policy branches and nothing about the events Slack sends. The
    interesting claims here are that ordinary chatter carries the fields the
    payload is built from, that ``channel_type`` on a real public-channel
    message clears the DM guard, and that the live route and the source route
    really are decided independently of one another.
    """

    async def test_ordinary_chatter_is_collected_but_not_answered(
        self, live_channel, human, posted,
    ):
        marker = unique_marker()
        auth = await human.auth_test()
        router = RecordingRouter()
        channel, _ = await live_channel(
            router,
            allow_users=[auth["user_id"]],
            source=ChannelSourceConfig(
                enabled=True, allow_conversations=[TEST_CHANNEL],
            ),
        )

        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL, text=f"team chatter {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])

        observed = await router.wait_for_observation(marker)
        assert observed.channel_name == "slack"
        assert observed.conversation_id == TEST_CHANNEL
        assert observed.sender_id == auth["user_id"]
        assert observed.message_id == sent["ts"]
        assert observed.channel_key == f"slack:{TEST_CHANNEL}:{sent['ts']}"
        # The stamp Slack wrote, not the moment we read it.
        assert observed.timestamp == slack_ts_to_iso(sent["ts"])
        # No mention, so the live route declined it. That is the whole point
        # of the source: it reaches the inbox without starting a turn.
        assert not router.messages

    async def test_a_conversation_off_the_grant_is_not_collected(
        self, live_channel, human, posted,
    ):
        marker = unique_marker()
        auth = await human.auth_test()
        router = RecordingRouter()
        channel, _ = await live_channel(
            router,
            allow_users=[auth["user_id"]],
            source=ChannelSourceConfig(
                enabled=True, allow_conversations=["C0NOTTHISONE"],
            ),
        )

        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL, text=f"unwatched chatter {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])

        await router.expect_no_observation(marker, channel)

    async def test_a_channel_name_grant_resolves_the_real_name(
        self, live_channel, human, bot, posted,
    ):
        marker = unique_marker()
        auth = await human.auth_test()
        info = await bot.conversations_info(channel=TEST_CHANNEL)
        name = info["channel"]["name"]
        router = RecordingRouter()
        channel, _ = await live_channel(
            router,
            allow_users=[auth["user_id"]],
            source=ChannelSourceConfig(
                enabled=True, allow_conversations=[name],
            ),
        )

        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL, text=f"named grant {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])

        observed = await router.wait_for_observation(marker)
        # Resolved for the grant, so it is recorded rather than left empty.
        assert observed.conversation_title == name

    async def test_a_sender_denied_by_handle_is_not_collected(
        self, live_channel, human, posted,
    ):
        # The deny pattern is a handle, so the gate has to resolve the real
        # sender through users.info to find out it matches.
        marker = unique_marker()
        auth = await human.auth_test()
        router = RecordingRouter()
        channel, _ = await live_channel(
            router,
            allow_users=[auth["user_id"]],
            source=ChannelSourceConfig(
                enabled=True,
                allow_conversations=[TEST_CHANNEL],
                deny_senders=[auth["user"]],
            ),
        )

        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL, text=f"denied sender {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])

        await router.expect_no_observation(marker, channel)

    async def test_an_answered_mention_is_not_collected_by_default(
        self, live_channel, human, bot, posted,
    ):
        marker = unique_marker()
        auth = await human.auth_test()
        router = RecordingRouter(reply_text="ack")
        channel, _ = await live_channel(
            router,
            allow_users=[auth["user_id"]],
            source=ChannelSourceConfig(
                enabled=True, allow_conversations=[TEST_CHANNEL],
            ),
        )

        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL,
            text=f"<@{channel._bot_user_id}> handled {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])

        await router.wait_for_message(marker)
        replies = await bot.conversations_replies(
            channel=TEST_CHANNEL, ts=sent["ts"],
        )
        for reply in replies["messages"]:
            if reply.get("user") == channel._bot_user_id:
                posted.note_bot(TEST_CHANNEL, reply["ts"])
        # Answered live, so the source leaves it alone: one message should
        # not arrive twice.
        assert not router._matching_observed(marker)

    async def test_include_handled_messages_sends_a_mention_to_both(
        self, live_channel, human, bot, posted,
    ):
        marker = unique_marker()
        auth = await human.auth_test()
        router = RecordingRouter(reply_text="ack")
        channel, _ = await live_channel(
            router,
            allow_users=[auth["user_id"]],
            source=ChannelSourceConfig(
                enabled=True,
                allow_conversations=[TEST_CHANNEL],
                include_handled_messages=True,
            ),
        )

        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL,
            text=f"<@{channel._bot_user_id}> both routes {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])

        await router.wait_for_message(marker)
        await router.wait_for_observation(marker)
        replies = await bot.conversations_replies(
            channel=TEST_CHANNEL, ts=sent["ts"],
        )
        for reply in replies["messages"]:
            if reply.get("user") == channel._bot_user_id:
                posted.note_bot(TEST_CHANNEL, reply["ts"])

    async def test_the_bots_own_post_is_not_collected(
        self, live_channel, bot, posted,
    ):
        marker = unique_marker()
        router = RecordingRouter()
        channel, _ = await live_channel(
            router,
            source=ChannelSourceConfig(
                enabled=True, allow_conversations=[TEST_CHANNEL],
            ),
        )

        sent = await bot.chat_postMessage(
            channel=TEST_CHANNEL, text=f"the agent talking {marker}",
        )
        posted.note_bot(TEST_CHANNEL, sent["ts"])

        # An agent reading its own output back is a loop, not a source.
        await asyncio.sleep(REFUSAL_SETTLE_SECONDS)
        assert not router._matching_observed(marker)

    async def test_a_collected_message_drains_into_the_inbox(
        self, live_channel, human, posted, tmp_path,
    ):
        # The whole bridge, on one real message: Slack event → buffer row →
        # ChannelSource → an inbox record a consumer tool would read.
        marker = unique_marker()
        auth = await human.auth_test()
        router = RecordingRouter()
        channel, _ = await live_channel(
            router,
            allow_users=[auth["user_id"]],
            source=ChannelSourceConfig(
                enabled=True, allow_conversations=[TEST_CHANNEL],
            ),
        )

        sent = await human.chat_postMessage(
            channel=TEST_CHANNEL, text=f"drain me {marker}",
        )
        posted.note_user(TEST_CHANNEL, sent["ts"])
        observed = await router.wait_for_observation(marker)

        db = Database(tmp_path / "observations.db")
        await db.connect()
        try:
            real_router = ChannelRouter(engine=SimpleNamespace(db=db))
            assert await real_router.observe(observed)

            result = await SourceRunner(
                source=ChannelSource("slack", db), db=db,
            ).run()

            assert result.records_ingested == 1
            rows, _ = await db.list_source_messages(source="slack:observed")
            assert len(rows) == 1
            record_id = f"{TEST_CHANNEL}:{sent['ts']}"
            assert rows[0]["id"] == record_id
            stored = await db.get_source_message("slack:observed", record_id)
            assert marker in stored["content"]
            assert stored["metadata"]["sender_id"] == auth["user_id"]
        finally:
            await db.close()
