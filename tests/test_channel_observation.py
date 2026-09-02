"""Channel → source bridge — what gets watched, spooled, and drained.

Three layers, tested separately because they fail differently:

* the observation policy, which is the one place a mistake is a security
  bug rather than a missing feature;
* the spool, whose id must stay monotonic across pruning or the drain
  silently skips messages;
* :class:`ChannelSource`, which turns spooled rows into inbox records and
  hands the rest — filtering, TTL, health, cursor — to ``SourceRunner``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.channels.access import Identity, PatternGate
from nerve.channels.base import ObservedMessage
from nerve.channels.observation import ObservationPolicy
from nerve.channels.router import ChannelRouter
from nerve.channels.slack import SlackChannel
from nerve.config import NerveConfig, ObserveConfig, SlackConfig
from nerve.db.observations import _TRIM_EVERY
from nerve.sources.channel import ChannelSource
from nerve.sources.registry import build_source_runners

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------- #
#  Helpers                                                                #
# ---------------------------------------------------------------------- #


def _policy(**kwargs) -> ObservationPolicy:
    return ObservationPolicy(
        enabled=kwargs.pop("enabled", True),
        conversations=PatternGate(
            "conversation",
            allow=kwargs.pop("allow_conversations", ["C0123ABCD"]),
            deny=kwargs.pop("deny_conversations", []),
        ),
        senders=PatternGate(
            "sender",
            allow=kwargs.pop("allow_senders", []),
            deny=kwargs.pop("deny_senders", []),
        ),
    )


def _slack_channel(
    router=None, allow_channels=None, **observe_kwargs,
) -> SlackChannel:
    """A Slack channel with a stub transport and an observe policy.

    ``allow_channels`` is the *access* grant, deliberately separate from the
    observe kwargs — the two policies are independent and the tests here
    depend on that.
    """
    cfg = NerveConfig()
    cfg.slack = SlackConfig(
        enabled=True,
        bot_token="xoxb-test",
        app_token="xapp-test",
        allow_channels=list(allow_channels or []),
        observe=ObserveConfig(**observe_kwargs),
    )
    channel = SlackChannel(cfg, router=router or MagicMock())
    channel._web = MagicMock()
    channel._web.chat_postMessage = AsyncMock(return_value={"ts": "1.1"})
    channel._web.conversations_info = AsyncMock(
        return_value={"channel": {"name": "general"}},
    )
    channel._web.users_info = AsyncMock(
        return_value={"user": {"name": "alice", "profile": {}}},
    )
    channel._state = "running"
    channel._bot_user_id = "U0BOT"
    if router is None:
        channel.router.observe = AsyncMock(return_value=True)
        channel.router.get_last_session = AsyncMock(return_value=None)
        channel.router.handle_message = AsyncMock(return_value="done")
    return channel


def _event(**kwargs) -> dict:
    base = {
        "type": "message",
        "channel": "C0123ABCD",
        "channel_type": "channel",
        "user": "U0456DEFG",
        "ts": "1700000000.000100",
        "text": "just chatting",
    }
    base.update(kwargs)
    return base


# ---------------------------------------------------------------------- #
#  Observation policy — the part where a mistake is a security bug        #
# ---------------------------------------------------------------------- #


class TestObservationPolicy:
    def test_an_allowed_conversation_is_observed(self):
        verdict = _policy().check(Identity(id="C0123ABCD"), Identity(id="U1"))

        assert verdict.allowed

    def test_disabled_observes_nothing(self):
        verdict = _policy(enabled=False).check(
            Identity(id="C0123ABCD"), Identity(id="U1"),
        )

        assert not verdict.allowed
        assert "not enabled" in verdict.reason

    def test_an_empty_allow_list_means_nothing_not_everything(self):
        # The inverse of PatternGate's own default, and the whole reason
        # this policy exists separately. A standing grant to record other
        # people's messages must be written down, not inferred from silence.
        verdict = _policy(allow_conversations=[]).check(
            Identity(id="C0123ABCD"), Identity(id="U1"),
        )

        assert not verdict.allowed
        assert "no conversations are approved" in verdict.reason

    def test_an_unlisted_conversation_is_refused(self):
        verdict = _policy().check(Identity(id="C0999ZZZZ"), Identity(id="U1"))

        assert not verdict.allowed

    def test_a_denied_conversation_is_refused(self):
        verdict = _policy(
            allow_conversations=["*"], deny_conversations=["C0999ZZZZ"],
        ).check(Identity(id="C0999ZZZZ"), Identity(id="U1"))

        assert not verdict.allowed
        assert "deny" in verdict.reason

    def test_a_denied_sender_is_refused_in_an_allowed_conversation(self):
        verdict = _policy(deny_senders=["U0BOT"]).check(
            Identity(id="C0123ABCD"), Identity(id="U0BOT"),
        )

        assert not verdict.allowed

    def test_active_requires_both_enabled_and_a_grant(self):
        assert _policy().active
        assert not _policy(enabled=False).active
        assert not _policy(allow_conversations=[]).active

    def test_observation_is_not_the_access_policy(self):
        # Observing a room the agent takes no orders from is the point.
        # If these two were the same object, enabling one would enable the
        # other, which is the failure this separation exists to prevent.
        cfg = SlackConfig(allow_channels=["C0AAA1111"])
        channel = _slack_channel(
            enabled=True, allow_conversations=["C0BBB2222"],
        )
        channel.config.slack.allow_channels = cfg.allow_channels

        assert channel.policy.channels.allow == ["C0AAA1111"]
        assert channel.observation.conversations.allow == ["C0BBB2222"]


# ---------------------------------------------------------------------- #
#  Slack hook                                                             #
# ---------------------------------------------------------------------- #


class TestSlackObserve:
    async def test_an_unanswered_message_in_a_watched_channel_is_spooled(self):
        channel = _slack_channel(enabled=True, allow_conversations=["C0123ABCD"])

        await channel._handle_message_event(_event())

        channel.router.observe.assert_awaited_once()
        observed = channel.router.observe.await_args.args[0]
        assert observed.channel_name == "slack"
        assert observed.conversation_id == "C0123ABCD"
        assert observed.sender_id == "U0456DEFG"
        assert observed.text == "just chatting"
        assert observed.message_id == "1700000000.000100"
        assert observed.timestamp.startswith("2023-11-14T")

    async def test_an_answered_message_is_not_spooled(self):
        # It becomes a real turn instead; spooling it too would show the
        # agent its own conversation as third-party inbox traffic.
        channel = _slack_channel(
            allow_channels=["C0123ABCD"],
            enabled=True,
            allow_conversations=["C0123ABCD"],
        )

        await channel._handle_message_event(
            _event(text="<@U0BOT> hello", type="app_mention"),
        )

        channel.router.observe.assert_not_awaited()
        channel.router.handle_message.assert_awaited_once()

    async def test_observation_off_spools_nothing(self):
        channel = _slack_channel(enabled=False, allow_conversations=["C0123ABCD"])

        await channel._handle_message_event(_event())

        channel.router.observe.assert_not_awaited()

    async def test_an_unwatched_channel_is_not_spooled(self):
        channel = _slack_channel(enabled=True, allow_conversations=["C0AAA1111"])

        await channel._handle_message_event(_event())

        channel.router.observe.assert_not_awaited()

    async def test_a_direct_message_is_never_observed(self):
        # Declining to answer a DM is a refusal. Filing it away instead is
        # not what the silence led the sender to expect.
        channel = _slack_channel(enabled=True, allow_conversations=["*"])

        await channel._handle_message_event(
            _event(channel="D0123ABCD", channel_type="im"),
        )

        channel.router.observe.assert_not_awaited()

    async def test_the_agents_own_post_is_not_spooled(self):
        channel = _slack_channel(enabled=True, allow_conversations=["*"])

        await channel._handle_message_event(_event(user="U0BOT"))

        channel.router.observe.assert_not_awaited()

    async def test_join_and_leave_noise_is_not_spooled(self):
        channel = _slack_channel(enabled=True, allow_conversations=["*"])

        await channel._handle_message_event(_event(subtype="channel_join"))

        channel.router.observe.assert_not_awaited()

    async def test_another_app_is_not_spooled(self):
        channel = _slack_channel(enabled=True, allow_conversations=["*"])
        channel._web.users_info = AsyncMock(
            return_value={"user": {"is_bot": True, "profile": {}}},
        )

        await channel._handle_message_event(_event(bot_id="B0OTHER"))

        channel.router.observe.assert_not_awaited()

    async def test_an_id_only_policy_costs_no_api_call(self):
        # Observation sits on the dispatch path of a busy channel. A lookup
        # per message would make watching one expensive.
        channel = _slack_channel(enabled=True, allow_conversations=["C0123ABCD"])

        await channel._handle_message_event(_event())

        channel._web.conversations_info.assert_not_awaited()
        channel._web.users_info.assert_not_awaited()

    async def test_a_name_policy_resolves_and_records_the_name(self):
        channel = _slack_channel(enabled=True, allow_conversations=["general"])

        await channel._handle_message_event(_event())

        observed = channel.router.observe.await_args.args[0]
        assert observed.conversation_title == "general"

    async def test_the_thread_is_recorded_for_a_reader_to_expand(self):
        channel = _slack_channel(enabled=True, allow_conversations=["C0123ABCD"])
        channel.router.get_last_session = AsyncMock(return_value=None)

        await channel._handle_message_event(
            _event(thread_ts="1699999999.000000"),
        )

        observed = channel.router.observe.await_args.args[0]
        assert observed.metadata["thread_ts"] == "1699999999.000000"


# ---------------------------------------------------------------------- #
#  Router seam                                                            #
# ---------------------------------------------------------------------- #


def _observed(**kwargs) -> ObservedMessage:
    base = dict(
        channel_name="slack",
        channel_key="slack:C0123ABCD",
        conversation_id="C0123ABCD",
        sender_id="U0456DEFG",
        text="hello",
        message_id="1700000000.000100",
        timestamp="2023-11-14T22:13:20+00:00",
    )
    base.update(kwargs)
    return ObservedMessage(**base)


class TestRouterObserve:
    async def test_an_observation_reaches_the_database(self, db):
        engine = MagicMock()
        engine.db = db
        router = ChannelRouter(engine)

        assert await router.observe(_observed())

        rows = await db.read_channel_observations("slack")
        assert len(rows) == 1
        assert rows[0][1]["text"] == "hello"

    async def test_a_database_failure_does_not_escape(self):
        # This runs on the dispatch path of a channel that already decided
        # not to answer. A DB hiccup must not take down message handling for
        # traffic the agent was never going to act on.
        engine = MagicMock()
        engine.db.insert_channel_observation = AsyncMock(
            side_effect=RuntimeError("boom"),
        )
        router = ChannelRouter(engine)

        assert not await router.observe(_observed())


# ---------------------------------------------------------------------- #
#  Spool                                                                  #
# ---------------------------------------------------------------------- #


class TestSpool:
    async def test_rows_come_back_in_order_past_a_cursor(self, db):
        for i in range(5):
            await db.insert_channel_observation(
                "slack", "slack:C1", {"n": i},
            )

        rows = await db.read_channel_observations("slack", after_id=0, limit=3)

        assert [p["n"] for _, p in rows] == [0, 1, 2]
        rest = await db.read_channel_observations("slack", after_id=rows[-1][0])
        assert [p["n"] for _, p in rest] == [3, 4]

    async def test_channels_do_not_see_each_other(self, db):
        await db.insert_channel_observation("slack", "slack:C1", {"n": 1})
        await db.insert_channel_observation("telegram", "telegram:9", {"n": 2})

        rows = await db.read_channel_observations("slack")

        assert [p["n"] for _, p in rows] == [1]

    async def test_ids_keep_climbing_after_a_prune(self, db):
        # The reason the migration uses AUTOINCREMENT. A plain rowid is
        # reused once the highest row is deleted, so a drained-and-pruned
        # spool would reissue ids the cursor has already passed and the next
        # observations would be skipped for good.
        first = await db.insert_channel_observation("slack", "k", {"n": 1})
        await db._write("DELETE FROM channel_observations", ())

        second = await db.insert_channel_observation("slack", "k", {"n": 2})

        assert second > first

    async def test_the_row_cap_drops_the_oldest(self, db):
        # Trimming is amortized, so the cap is a bound the spool returns to
        # rather than one it never crosses: it may overshoot by up to one
        # trim interval. That beats a COUNT on the dispatch path.
        cap = 5
        written = _TRIM_EVERY * 2 + cap
        for i in range(written):
            await db.insert_channel_observation(
                "slack", "k", {"n": i}, max_rows=cap,
            )

        rows = await db.read_channel_observations("slack", limit=10_000)

        assert len(rows) <= cap + _TRIM_EVERY
        assert len(rows) < written
        # What survives is the newest end — a stale backlog nobody drained
        # is the right thing to lose.
        assert rows[-1][1]["n"] == written - 1

    async def test_an_expired_observation_is_swept(self, db):
        await db.insert_channel_observation("slack", "k", {"n": 1}, ttl_days=-1)
        await db.insert_channel_observation("slack", "k", {"n": 2}, ttl_days=7)

        deleted = await db.cleanup_expired_channel_observations()

        assert deleted == 1
        rows = await db.read_channel_observations("slack")
        assert [p["n"] for _, p in rows] == [2]

    async def test_an_unreadable_payload_is_reported_not_hidden(self, db):
        # It comes back with a None payload rather than being dropped, so
        # the drain can skip the row and still advance past its id.
        await db.insert_channel_observation("slack", "k", {"n": 1})
        await _write_garbage(db)
        await db.insert_channel_observation("slack", "k", {"n": 3})

        rows = await db.read_channel_observations("slack")

        assert [p["n"] if p else None for _, p in rows] == [1, None, 3]


# ---------------------------------------------------------------------- #
#  ChannelSource                                                          #
# ---------------------------------------------------------------------- #


async def _write_garbage(db) -> None:
    """Put a row in the spool whose payload will never parse."""
    await db._write(
        "INSERT INTO channel_observations "
        "(channel, channel_key, payload, created_at, expires_at) "
        "VALUES ('slack', 'k', 'not json', '2026-01-01', '2099-01-01')",
        (),
    )


class TestChannelSource:
    async def test_spooled_rows_become_records(self, db):
        await db.insert_channel_observation(
            "slack", "slack:C0123ABCD",
            {
                "conversation_id": "C0123ABCD",
                "conversation_title": "general",
                "sender_id": "U0456DEFG",
                "sender_name": "alice",
                "text": "ship it",
                "message_id": "1700000000.000100",
                "timestamp": "2023-11-14T22:13:20+00:00",
                "channel_key": "slack:C0123ABCD",
                "metadata": {"thread_ts": "1699999999.000000"},
            },
        )

        result = await ChannelSource("slack", db).fetch(None)

        assert len(result.records) == 1
        record = result.records[0]
        assert record.id == "C0123ABCD:1700000000.000100"
        assert record.source == "slack"
        assert record.record_type == "slack_message"
        assert record.summary == "[general] alice: ship it"
        assert record.content == "ship it"
        assert record.metadata["thread_ts"] == "1699999999.000000"
        assert record.metadata["conversation_id"] == "C0123ABCD"

    async def test_the_cursor_is_the_spool_id(self, db):
        last = 0
        for i in range(3):
            last = await db.insert_channel_observation(
                "slack", "k", {"text": str(i), "message_id": str(i)},
            )

        result = await ChannelSource("slack", db).fetch(None)

        assert result.next_cursor == str(last)

    async def test_a_cursor_resumes_where_it_left_off(self, db):
        source = ChannelSource("slack", db)
        for i in range(3):
            await db.insert_channel_observation(
                "slack", "k", {"text": str(i), "message_id": str(i)},
            )
        first = await source.fetch(None, limit=2)

        second = await source.fetch(first.next_cursor)

        assert [r.content for r in first.records] == ["0", "1"]
        assert [r.content for r in second.records] == ["2"]

    async def test_a_full_batch_reports_more(self, db):
        for i in range(4):
            await db.insert_channel_observation(
                "slack", "k", {"text": str(i), "message_id": str(i)},
            )

        result = await ChannelSource("slack", db).fetch(None, limit=2)

        assert result.has_more

    async def test_an_empty_spool_holds_the_cursor(self, db):
        result = await ChannelSource("slack", db).fetch("42")

        assert result.records == []
        assert result.next_cursor == "42"
        assert not result.has_more

    async def test_an_unreadable_cursor_starts_over_rather_than_failing(self, db):
        # source_messages is keyed (source, id), so re-reading a bounded,
        # TTL-capped spool re-inserts nothing. Failing closed here would
        # instead wedge the source until someone edited the database.
        await db.insert_channel_observation(
            "slack", "k", {"text": "hi", "message_id": "1"},
        )

        result = await ChannelSource("slack", db).fetch("not-a-number")

        assert len(result.records) == 1

    async def test_an_unreadable_row_is_skipped_and_passed(self, db):
        # The wedge this guards against: if the cursor only ever advanced to
        # the last row that *parsed*, a batch of entirely unreadable rows
        # would move it nowhere, be re-read every run, and hide everything
        # behind them for good.
        source = ChannelSource("slack", db)
        await _write_garbage(db)
        await _write_garbage(db)

        first = await source.fetch(None)

        assert first.records == []
        assert first.next_cursor == "2"

        await db.insert_channel_observation(
            "slack", "k", {"text": "reachable", "message_id": "3"},
        )
        second = await source.fetch(first.next_cursor)
        assert [r.content for r in second.records] == ["reachable"]

    async def test_the_same_message_observed_twice_lands_once(self, db):
        payload = {
            "conversation_id": "C1",
            "message_id": "1700000000.000100",
            "text": "hi",
            "timestamp": "2023-11-14T22:13:20+00:00",
        }
        await db.insert_channel_observation("slack", "k", payload)
        await db.insert_channel_observation("slack", "k", dict(payload))

        result = await ChannelSource("slack", db).fetch(None)
        inserted = await db.insert_source_messages(result.records, source="slack")

        assert len(result.records) == 2
        assert inserted == 1


# ---------------------------------------------------------------------- #
#  Registry wiring                                                        #
# ---------------------------------------------------------------------- #


class TestRegistry:
    def test_an_observing_channel_gets_a_runner(self, tmp_path):
        cfg = NerveConfig()
        cfg.slack.observe = ObserveConfig(
            enabled=True, allow_conversations=["C0123ABCD"], schedule="*/7 * * * *",
        )

        runners = build_source_runners(cfg, MagicMock())

        slack = [r for r in runners if r.source.source_name == "slack"]
        assert len(slack) == 1
        # The runner carries its own cadence: the config lives at
        # slack.observe, and CronService would otherwise find no
        # config.sync.slack section and never schedule it.
        assert slack[0].schedule == "*/7 * * * *"

    def test_the_carried_schedule_is_what_cron_uses(self):
        # Without this the runner is built and then silently never
        # scheduled: there is no config.sync.slack section to look up, so
        # the lookup returns None and _plan_source_runners drops it.
        from nerve.cron.service import CronService

        service = CronService.__new__(CronService)
        service.config = NerveConfig()
        runner = MagicMock()
        runner.source.source_name = "slack"
        runner.schedule = "*/7 * * * *"

        assert service._source_schedule(runner) == "*/7 * * * *"

    def test_a_non_string_schedule_is_ignored(self):
        # _source_schedule reads a duck-typed attribute, and a bare
        # MagicMock answers truthily to anything asked of it — which would
        # otherwise put a mock where a crontab expression belongs.
        from nerve.cron.service import CronService

        service = CronService.__new__(CronService)
        service.config = NerveConfig()
        runner = MagicMock()
        runner.source.source_name = "github"

        assert service._source_schedule(runner) == NerveConfig().sync.github.schedule

    def test_no_runner_without_a_conversation_grant(self, tmp_path):
        cfg = NerveConfig()
        cfg.slack.observe = ObserveConfig(enabled=True, allow_conversations=[])

        runners = build_source_runners(cfg, MagicMock())

        assert not [r for r in runners if r.source.source_name == "slack"]

    def test_no_runner_when_observation_is_off(self, tmp_path):
        cfg = NerveConfig()

        runners = build_source_runners(cfg, MagicMock())

        assert not [r for r in runners if r.source.source_name == "slack"]

    def test_telegram_gets_the_same_treatment(self, tmp_path):
        cfg = NerveConfig()
        cfg.telegram.observe = ObserveConfig(
            enabled=True, allow_conversations=["-100123"],
        )

        runners = build_source_runners(cfg, MagicMock())

        assert [r for r in runners if r.source.source_name == "telegram"]


class TestConfig:
    def test_observe_parses_from_a_slack_block(self):
        cfg = SlackConfig.from_dict({
            "observe": {
                "enabled": True,
                "allow_conversations": ["C1"],
                "deny_senders": ["U0BOT"],
                "schedule": "*/9 * * * *",
            },
        })

        assert cfg.observe.enabled
        assert cfg.observe.allow_conversations == ["C1"]
        assert cfg.observe.deny_senders == ["U0BOT"]
        assert cfg.observe.schedule == "*/9 * * * *"

    def test_observation_is_off_by_default(self):
        cfg = SlackConfig.from_dict({})

        assert not cfg.observe.enabled
        assert cfg.observe.allow_conversations == []

    def test_enabling_without_a_grant_warns(self, caplog):
        with caplog.at_level("WARNING"):
            SlackConfig.from_dict({"observe": {"enabled": True}})

        assert "allow_conversations" in caplog.text
