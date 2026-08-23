"""Live Slack tests for the inbound half — events reaching the router.

Everything here skips unless the credentials in :data:`tests.slack_live.SETUP`
are present, so the ordinary suite and CI on a fork are unaffected.

The unit tests already prove the channel is self-consistent. These exist for
the claims that only Slack can settle: whether a Block Kit payload is
accepted, whether an emoji short name exists, whether ``users.info`` really
withholds an email rather than failing, and whether an event makes the whole
trip from a human's keystroke to an InboundMessage.

These hold a Socket Mode connection, which is why they are kept apart
from the outbound tests. Slack gives each event to exactly one of an
app's open connections and replays whatever queued while none was
listening, so a minute of outbound API traffic in the same process
leaves a backlog the first test here reads instead of its own message.

Run with::

    pytest tests/test_slack_live_inbound.py -v

If a test here reports "no inbound message reached the router", suspect the
connection rather than the routing, and prefer diagnosing it to muting it —
every time this suite has looked flaky it has been describing something real.
The two causes found so far were a connection per test, which lost events
into the gap where Slack had not yet started routing to the new socket, and
outbound API traffic in the same process, whose queued events Slack replayed
down this one. Hence a single shared connection, and a separate file.
"""

from __future__ import annotations

import asyncio
import time

import pytest
import pytest_asyncio

from nerve.channels.slack import (
    format_target,
    is_slack_id,
)
from tests.slack_live import (
    BOT_TOKEN,
    EVENT_TIMEOUT,
    TEST_CHANNEL,
    USER_TOKEN,
    Posted,
    RecordingRouter,
    SOCKET_DRAIN_SECONDS,
    build_channel,
    make_client,
    wait_until_quiet,
    ignore_replays,
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
async def posted(bot):
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

    This needs the outbound tests to live in another file. Sharing was tried
    while they ran in this process and failed, because a minute of their API
    traffic queues events that Slack replays down this socket.
    """
    channel, cfg = build_channel(RecordingRouter())
    await channel.start()
    # Order matters. Replays are dropped first so the readiness probe cannot
    # be satisfied by an old envelope, then readiness waits for the probe's
    # own ts to prove Slack is delivering fresh events to this socket.
    ignore_replays(channel)
    await wait_until_receiving(channel)
    await wait_until_quiet(channel)
    try:
        yield channel, cfg
    finally:
        await channel.stop()


@pytest_asyncio.fixture(loop_scope="module")
async def live_channel(_connected_channel):
    """Point the shared channel at this test's router and guardrails."""
    channel, cfg = _connected_channel

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
        for field, default in (
            ("allow_users", []), ("deny_users", []),
            ("allow_channels", []), ("deny_channels", []),
            ("reply_in_thread", True), ("commands", None),
        ):
            setattr(cfg.slack, field, slack_kwargs.get(field, default))
        channel.router = router
        router.channel = channel
        # Resolved identities are policy-specific, so a later test must not
        # inherit a verdict computed under different lists.
        channel._name_cache.clear()
        return channel, cfg

    return _use


@requires_outbound
class TestTransport:
    async def test_the_channel_connects_over_socket_mode(self, live_channel):
        router = RecordingRouter()
        channel, _ = await live_channel(router, allow_users=["U0000000"])
        assert is_slack_id(channel._bot_user_id)
        assert await channel._client.is_connected()

    async def test_the_watchdog_restores_a_dropped_socket(self):
        # This one owns its connection instead of borrowing the shared one:
        # it breaks the socket on purpose, and the reconnect leaves two
        # connections briefly. Slack hands each event to exactly one of an
        # app's connections, so doing that to the shared channel stole
        # events from whichever test ran next.
        import nerve.channels.slack as slack_module

        router = RecordingRouter()
        channel, _ = build_channel(router, allow_users=["U0000000"])
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
            # Let Slack drop this connection before the next test relies on
            # the shared one receiving everything.
            await asyncio.sleep(SOCKET_DRAIN_SECONDS)


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
        assert bot_replies[0]["text"] == "*live reply*"
        posted.note_bot(TEST_CHANNEL, bot_replies[0]["ts"])

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
        await router.expect_no_message(marker)

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
        channel, _ = await live_channel(router, allow_users=[user_id])

        dm = await human.conversations_open(users=channel._bot_user_id)
        dm_id = dm["channel"]["id"]
        sent = await human.chat_postMessage(channel=dm_id, text=f"live dm test {marker}")
        posted.note_user(dm_id, sent["ts"])

        msg = await router.wait_for_message(marker)
        assert msg.channel_key == f"slack:{dm_id}"
        assert msg.text == f"live dm test {marker}"


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
        await router.expect_no_message(marker)

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
        await router.expect_no_message(marker)

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
        await router.expect_no_message(marker)

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
        # context for, which is what the outbound cache is for.
        channel._cache_message(
            anchor["ts"], format_target(TEST_CHANNEL), f"react to me {marker}",
        )
        await human.reactions_add(
            channel=TEST_CHANNEL, timestamp=anchor["ts"], name="tada",
        )
        msg = await router.wait_for_message(marker)
        assert ":tada:" in msg.text


