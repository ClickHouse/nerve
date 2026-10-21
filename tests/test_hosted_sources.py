"""The hosted GitHub source: settings, subscriptions, the record consumer, the
drain into the inbox, and the jobs that a delivery runs.

The end-to-end tests run the fake gateway against the real stream endpoint, as
the hosted channel tests do, with a real database, source runner, and cron
service; only the agent turn is a stand-in.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
import yaml

import nerve.config as config_module
from nerve import config_reload
from nerve.agent.sessions import SessionManager
from nerve.channels.hosted.contract import (
    SourceFilters,
    SourceSubscription,
    Subscriptions,
)
from nerve.channels.hosted.contract.model import MAX_SUBSCRIPTION_PATTERNS
from nerve.channels.hosted.runtime import (
    HostedChannelRuntime,
    hosted_channel_providers,
    hosted_providers,
)
from nerve.channels.hosted.sources import (
    BUFFER_MAX_ROWS,
    HostedSourceConsumer,
    github_subscription,
    subscriptions_for,
)
from nerve.channels.router import ChannelRouter
from nerve.cli import doctor_report
from nerve.config import (
    GitHubSyncConfig,
    NerveConfig,
    SlackConfig,
    validate_config_keys,
)
from nerve.cron.gates import GateContext, GitHubPrActivityGate
from nerve.cron.service import CronService
from nerve.gateway import server as gateway_server
from nerve.sources.github import GitHubSource
from nerve.sources.github_hosted import HostedGitHubSource, record_from_buffer
from nerve.sources.registry import build_source_runners
from tests.fake_channel_gateway import (
    EVENT_TIMEOUT,
    FakeChannelGateway,
    FakeStream,
    NerveServer,
    message_event,
    source_record_event,
)
from tests.test_hosted_channel import FAST_READER, FAST_STREAM
from tests.test_hosted_channel import hosted_config as slack_hosted_config
from tests.test_hosted_channel_config import HOSTED

GITHUB_DENY = {"deny_repos": ["owner/noisy"], "deny_reasons": ["subscribed"]}


def hosted_github(**settings: Any) -> GitHubSyncConfig:
    return GitHubSyncConfig(mode="hosted", **settings)


async def eventually(predicate, timeout: float = EVENT_TIMEOUT) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("timed out waiting for the condition")
        await asyncio.sleep(0.01)


# ---------------------------------------------------------------------- #
#  Settings                                                               #
# ---------------------------------------------------------------------- #


class TestSettings:
    def test_poll_is_the_default_and_hosted_is_read(self):
        assert NerveConfig().sync.github.mode == "poll"
        config = NerveConfig.from_dict({"sync": {"github": {"mode": " Hosted ", "pr_watch": "false"}}})

        assert (config.sync.github.mode, config.sync.github.pr_watch) == ("hosted", False)
        assert NerveConfig().sync.github.pr_watch is True

    def test_an_unknown_mode_falls_back_to_poll(self, caplog):
        with caplog.at_level(logging.WARNING):
            config = NerveConfig.from_dict({"sync": {"github": {"mode": "webhook"}}})

        assert config.sync.github.mode == "poll"
        assert "sync.github.mode" in caplog.text

    @pytest.mark.parametrize(("value", "expected"), [
        (None, 60), (0, 0), (15, 15), ("120", 120), (900, 900), (-5, 0), ("soon", 60),
    ])
    def test_the_delivery_debounce_is_read_as_a_whole_number_of_seconds(self, value, expected):
        cron = {} if value is None else {"delivery_debounce_seconds": value}

        assert NerveConfig.from_dict({"cron": cron}).cron.delivery_debounce_seconds == expected

    def test_config_validate_warns_about_a_debounce_above_the_maximum(self, tmp_path):
        from nerve.config_validate import ValidationResult, _validate_delivery_debounce

        result = ValidationResult()
        _validate_delivery_debounce(
            NerveConfig.from_dict({"cron": {"delivery_debounce_seconds": 900}}), result,
        )
        quiet = ValidationResult()
        _validate_delivery_debounce(NerveConfig(), quiet)

        assert result.warnings == [(
            "cron.delivery_debounce_seconds: 900 is above the maximum of 600, so the jobs "
            "a hosted delivery starts wait 600s"
        )]
        assert quiet.warnings == []

    def test_the_new_keys_are_known(self):
        merged = {
            "sync": {"github": {"mode": "hosted", "pr_watch": True}},
            "cron": {"delivery_debounce_seconds": 30},
        }

        assert validate_config_keys(merged) == []

    def test_the_mode_needs_a_restart_and_the_filters_do_not(self):
        before = NerveConfig()
        after = NerveConfig()
        after.sync.github = hosted_github(deny_repos=["owner/noisy"], pr_watch=False)

        changed = config_reload.restart_required(before, after)

        assert changed == ["sync.github.mode: 'poll' → 'hosted'"]

    def test_hosted_github_is_a_hosted_provider_without_hosted_slack(self):
        config = NerveConfig()
        config.sync.github = hosted_github()

        assert hosted_providers(config) == ["github"]
        assert hosted_channel_providers(config) == []
        config.slack = SlackConfig(mode="hosted")
        assert hosted_providers(config) == ["slack", "github"]


# ---------------------------------------------------------------------- #
#  Subscriptions                                                          #
# ---------------------------------------------------------------------- #


class TestSubscriptionsBuilder:
    def test_the_guardrail_patterns_and_the_pull_request_watch_are_published(self):
        config = NerveConfig()
        config.sync.github = hosted_github(
            allow_repos=["owner/*", " "], deny_repos=[" owner/secret "], allow_actors=["alice"],
            deny_actors=["codecov"], allow_reasons=["mention"], deny_reasons=["subscribed"],
            deny_ci_branches=["main"], pr_watch=False,
        )

        assert subscriptions_for(config, ["github"]) == Subscriptions(version="1", sources=(
            SourceSubscription(
                provider="github",
                filters=SourceFilters(
                    allow_repos=("owner/*",), deny_repos=("owner/secret",), allow_actors=("alice",),
                    deny_actors=("codecov",), allow_reasons=("mention",), deny_reasons=("subscribed",),
                    deny_ci_branches=("main",),
                ),
                pr_watch=False,
            ),
        ))

    def test_a_switched_off_source_subscribes_to_nothing(self):
        config = NerveConfig()
        config.sync.github = hosted_github(enabled=False)

        assert subscriptions_for(config, ["github"]) == Subscriptions(version="1")

    def test_an_allow_list_the_gateway_cannot_take_is_published_empty(self, caplog):
        patterns = [f"owner/repo-{n}" for n in range(MAX_SUBSCRIPTION_PATTERNS + 1)]

        with caplog.at_level(logging.WARNING):
            subscription = github_subscription(hosted_github(allow_repos=patterns))

        assert subscription.filters.allow_repos == ()
        assert "sync.github.allow_repos" in caplog.text

    @pytest.mark.parametrize("bad", ["x" * 257, "owner/\x07bell", "line\nbreak"])
    def test_an_unusable_allow_pattern_widens_the_whole_list(self, bad):
        subscription = github_subscription(hosted_github(allow_actors=["alice", bad]))

        assert subscription.filters.allow_actors == ()

    def test_a_deny_list_keeps_the_patterns_that_fit(self):
        patterns = ["x" * 257, "owner/noisy", "tab\there"] + [f"owner/r{n}" for n in range(300)]

        subscription = github_subscription(hosted_github(deny_repos=patterns))

        assert subscription.filters.deny_repos[0] == "owner/noisy"
        assert len(subscription.filters.deny_repos) == MAX_SUBSCRIPTION_PATTERNS
        Subscriptions(version="1", sources=(subscription,)).validate()


# ---------------------------------------------------------------------- #
#  Consumer and buffer                                                    #
# ---------------------------------------------------------------------- #


def decoded(event: dict[str, Any]):
    """*event* as the reader hands it to a consumer: decoded and checked."""
    from nerve.channels.hosted.contract.model import Event
    from nerve.channels.hosted.contract.wire import decode_record

    row = {**event, "delivery": {"inbox_id": "1001"}}
    value = decode_record(Event, row)
    value.validate()
    return value


@pytest.mark.asyncio
class TestConsumer:
    @pytest.fixture
    def config(self) -> NerveConfig:
        config = NerveConfig()
        config.sync.github = hosted_github()
        return config

    async def test_a_record_is_accepted_once_its_buffer_row_is_stored(self, db, config):
        heard: list[str] = []
        consumer = HostedSourceConsumer("github", db, lambda: config)
        consumer.set_delivery_listener(heard.append)

        disposition = await consumer.deliver(decoded(source_record_event()))

        assert disposition.outcome == "accepted"
        [(_, payload)] = await db.read_channel_observations("github")
        assert payload["conversation_id"] == "owner/repo"
        assert payload["record"]["id"] == "9000000001"
        assert payload["record"]["timestamp"] == "2026-09-16T10:00:00Z"
        assert {"key": "actors", "value": "", "values": ["alice", "bob"]} in payload["record"]["metadata"]
        assert heard == ["github"]

    async def test_it_never_refuses_capacity(self, db, config):
        config.sync.github.enabled = False

        assert HostedSourceConsumer("github", db, lambda: config).can_accept()

    async def test_an_async_listener_runs_on_its_own(self, db, config):
        started = asyncio.Event()
        release = asyncio.Event()

        async def listener(provider: str) -> None:
            started.set()
            await release.wait()

        consumer = HostedSourceConsumer("github", db, lambda: config)
        consumer.set_delivery_listener(listener)

        assert (await consumer.deliver(decoded(source_record_event()))).outcome == "accepted"
        await asyncio.wait_for(started.wait(), EVENT_TIMEOUT)
        await consumer.stop()

    @pytest.mark.parametrize("event", [
        message_event(purpose="observe"),
        source_record_event(purpose="invoke"),
    ])
    async def test_anything_else_is_rejected_with_a_reason_the_gateway_takes(self, db, config, event):
        disposition = await HostedSourceConsumer("github", db, lambda: config).deliver(decoded(event))

        assert (disposition.outcome, disposition.reason_code) == ("rejected", "admission_rejected")
        assert await db.read_channel_observations("github") == []

    async def test_a_failed_write_defers_the_row(self, db, config, monkeypatch):
        async def locked(*args, **kwargs):
            raise RuntimeError("database is locked")

        monkeypatch.setattr(db, "insert_channel_observation", locked)
        heard: list[str] = []
        consumer = HostedSourceConsumer("github", db, lambda: config)
        consumer.set_delivery_listener(heard.append)

        assert (await consumer.deliver(decoded(source_record_event()))).outcome == "deferred"
        assert heard == []

    async def test_the_buffer_has_its_own_row_target(self, db, config, monkeypatch):
        seen: dict[str, Any] = {}
        original = db.insert_channel_observation

        async def spy(**kwargs):
            seen.update(kwargs)
            return await original(**kwargs)

        monkeypatch.setattr(db, "insert_channel_observation", spy)
        await HostedSourceConsumer("github", db, lambda: config).deliver(decoded(source_record_event()))

        assert (seen["channel"], seen["max_rows"]) == ("github", BUFFER_MAX_ROWS)


class TestBufferedRecords:
    def payload(self, **record: Any) -> dict[str, Any]:
        base = {
            "id": "9000000001", "record_type": "github_notification",
            "summary": "[owner/repo] Fix it (mention)", "content": "Repository: owner/repo",
            "timestamp": "2026-09-16T10:00:00Z",
            "metadata": [
                {"key": "reason", "value": "mention", "values": []},
                {"key": "unread", "value": "false", "values": []},
                {"key": "actors", "value": "", "values": ["alice"]},
                {"key": "changes", "value": "", "values": []},
                {"key": "ci_branch", "value": "", "values": []},
                {"key": "", "value": "ignored", "values": []},
            ],
        }
        base.update(record)
        return {"record": base}

    def test_a_payload_maps_to_the_polling_sources_record_shape(self):
        record = record_from_buffer(self.payload())

        assert (record.id, record.source, record.record_type) == ("9000000001", "github", "github_notification")
        assert record.timestamp == "2026-09-16T10:00:00Z"
        assert record.metadata == {
            "reason": "mention", "unread": False, "actors": ["alice"], "changes": [], "ci_branch": "",
        }

    @pytest.mark.parametrize("change", [{"id": ""}, {"summary": None}, {"record_type": 7}])
    def test_an_unusable_payload_gives_no_record(self, change):
        assert record_from_buffer(self.payload(**change)) is None
        assert record_from_buffer({"record": "text"}) is None

    @pytest.mark.asyncio
    async def test_the_drain_reads_past_a_polling_cursor_and_skips_unreadable_rows(self, db):
        await db.insert_channel_observation("github", "github:owner/repo", self.payload())
        await db.insert_channel_observation("github", "github:owner/repo", {"record": {}})
        await db.insert_channel_observation("slack", "slack:C1", {"text": "not ours"})

        result = await HostedGitHubSource(db).fetch("2026-09-16T09:00:00Z", limit=10)

        assert [r.id for r in result.records] == ["9000000001"]
        assert (result.next_cursor, result.has_more) == ("2", False)
        again = await HostedGitHubSource(db).fetch(result.next_cursor, limit=10)
        assert (again.records, again.next_cursor) == ([], "2")


@pytest.mark.asyncio
class TestRegistry:
    def runner(self, config, db):
        runners = [r for r in build_source_runners(config, db) if r.job_id == "source:github"]
        return runners[0] if runners else None

    async def test_hosted_mode_drains_the_buffer_with_the_github_guardrail(self, db):
        config = NerveConfig()
        config.sync.github = hosted_github(**GITHUB_DENY, drop_hook=["/bin/true"])

        runner = self.runner(config, db)

        assert isinstance(runner.source, HostedGitHubSource)
        assert runner.source.source_name == "github"
        assert [rule.field for rule in runner.inbox_filter.rules if rule.active] == ["repo_name", "reason"]
        assert runner.drop_hook == ["/bin/true"]

    async def test_poll_mode_is_unchanged(self, db):
        runner = self.runner(NerveConfig(), db)

        assert isinstance(runner.source, GitHubSource)

    async def test_a_switched_off_source_has_no_runner(self, db):
        config = NerveConfig()
        config.sync.github = hosted_github(enabled=False)

        assert self.runner(config, db) is None


@pytest.mark.asyncio
class TestPollingCursor:
    async def test_a_cursor_that_is_no_timestamp_looks_back_a_day(self, monkeypatch):
        seen: list[str] = []

        class _Proc:
            returncode = 0

            async def communicate(self):
                return b"", b""

        async def fake_exec(*argv, **kwargs):
            seen.append(argv[2])
            return _Proc()

        monkeypatch.setattr("nerve.sources.github.asyncio.create_subprocess_exec", fake_exec)

        result = await GitHubSource().fetch("57")

        assert result.next_cursor == "57"
        assert seen and "since=57" not in seen[0] and "since=20" in seen[0]


# ---------------------------------------------------------------------- #
#  End to end over real streams                                           #
# ---------------------------------------------------------------------- #


@dataclass
class Harness:
    config: NerveConfig
    db: Any
    router: ChannelRouter
    gateway: FakeChannelGateway
    runtime: HostedChannelRuntime
    server: NerveServer

    async def stream(self, **kwargs: Any) -> FakeStream:
        return await self.gateway.open_stream(self.server.stream_url, **kwargs)

    async def acknowledged(self, *inbox_ids: str) -> dict[str, str]:
        await self.gateway.wait_for(lambda: all(i in self.gateway.acknowledged for i in inbox_ids))
        return {i: self.gateway.acknowledged[i] for i in inbox_ids}

    async def subscriptions(self, count: int = 1) -> list[dict[str, Any]]:
        await self.gateway.wait_for(lambda: len(self.gateway.subscriptions) >= count)
        return self.gateway.subscriptions

    def github_runner(self):
        (runner,) = [r for r in build_source_runners(self.config, self.db) if r.job_id == "source:github"]
        return runner


async def start(tmp_path, db, monkeypatch, *, slack: bool = False, github: bool = True, **settings: Any) -> Harness:
    config = slack_hosted_config(tmp_path)
    if not slack:
        config.slack = SlackConfig()
    if github:
        config.sync.github = hosted_github(**settings)
    monkeypatch.setattr(config_module, "_config", config)
    engine = MagicMock()
    engine.db = db
    engine.sessions = SessionManager(db)
    router = ChannelRouter(engine)
    engine.router = router
    gateway = FakeChannelGateway(jwks_file=config.channels.hosted.gateway_jwks_file)
    runtime = HostedChannelRuntime(
        config, router, lambda: config, stream_timing=FAST_STREAM, reader_settings=FAST_READER, db=db,
    )
    await runtime.start()
    monkeypatch.setattr(gateway_server, "_hosted_channels", runtime)
    server = NerveServer(gateway_server.create_app())
    await server.__aenter__()
    return Harness(config, db, router, gateway, runtime, server)


async def stop(harness: Harness) -> None:
    await harness.runtime.stop()
    await harness.gateway.close_all()
    await harness.server.__aexit__(None, None, None)


@pytest_asyncio.fixture
async def hosted(tmp_path, db, monkeypatch):
    harness = await start(tmp_path, db, monkeypatch, **GITHUB_DENY)
    try:
        yield harness
    finally:
        await stop(harness)


@pytest.mark.asyncio
class TestStreams:
    async def test_a_stream_publishes_the_subscriptions_right_after_negotiation(self, hosted):
        stream = await hosted.stream()

        [subscriptions] = await hosted.subscriptions()

        assert subscriptions == {"version": "1", "sources": [{
            "provider": "github",
            "filters": {"deny_repos": ["owner/noisy"], "deny_reasons": ["subscribed"]},
            "pr_watch": True,
        }]}
        await hosted.gateway.wait_for(lambda: bool(stream.sent("inbox_read")))
        kinds = [frame["kind"] for frame in stream.frames if frame["kind"] != "heartbeat"]
        assert kinds[:3] == ["negotiation", "subscriptions", "inbox_read"]
        frame = stream.sent("subscriptions")[0]
        assert frame["request_id"].startswith("o-") and "connection_id" not in frame

    async def test_a_record_is_buffered_then_drained_into_the_inbox(self, hosted):
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(source_record_event())

        await stream.nudge("observe")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        assert len(await hosted.db.read_channel_observations("github")) == 1
        assert hosted.router.get_channel("github") is None
        result = await hosted.github_runner().run()
        assert result.records_ingested == 1
        stored = await hosted.db.get_source_message("github", "9000000001")
        assert stored["summary"] == "[owner/repo] Fix the flaky retry test (mention)"
        assert stored["content"].endswith("@example-agent could you take a look?")
        assert stored["metadata"]["actors"] == ["alice", "bob"]
        assert stored["metadata"]["unread"] is True
        assert stored["metadata"]["ci_branch"] == ""

    async def test_a_changed_record_resurfaces_in_place_of_the_stored_one(self, hosted):
        stream = await hosted.stream()
        runner = hosted.github_runner()
        first = hosted.gateway.store(source_record_event())
        await stream.nudge("observe")
        assert (await hosted.acknowledged(first))[first] == "accepted"
        assert (await runner.run()).records_ingested == 1
        before = await hosted.db.get_source_max_rowid("github")

        second = hosted.gateway.store(source_record_event(
            title="Fix the flaky retry test, take two", updated_at="2026-09-16T11:00:00Z", version=1789556400,
        ))
        await stream.nudge("observe")

        assert (await hosted.acknowledged(second))[second] == "accepted"
        assert (await runner.run()).records_ingested == 1
        stored = await hosted.db.get_source_message("github", "9000000001")
        assert stored["summary"] == "[owner/repo] Fix the flaky retry test, take two (mention)"
        assert await hosted.db.get_source_max_rowid("github") > before

    async def test_the_inbox_filter_drops_what_the_published_filters_deny(self, hosted):
        stream = await hosted.stream()
        noisy = hosted.gateway.store(source_record_event(record_id="9000000002", repository="owner/noisy"))
        kept = hosted.gateway.store(source_record_event(record_id="9000000003"))

        await stream.nudge("observe")

        assert await hosted.acknowledged(noisy, kept) == {noisy: "accepted", kept: "accepted"}
        result = await hosted.github_runner().run()
        assert (result.records_ingested, result.records_dropped) == (1, 1)
        assert await hosted.db.get_source_message("github", "9000000002") is None

    async def test_a_pull_request_record_meets_the_repo_rule_only(self, tmp_path, db, monkeypatch):
        harness = await start(
            tmp_path, db, monkeypatch, allow_actors=["someone-else"], allow_reasons=["mention"],
        )
        try:
            stream = await harness.stream()
            pull_request = harness.gateway.store(source_record_event(
                record_id="pr:owner/repo#12", record_type="github_pr_activity", reason="author",
            ))
            notification = harness.gateway.store(source_record_event(record_id="9000000004"))

            await stream.nudge("observe")

            await harness.acknowledged(pull_request, notification)
            result = await harness.github_runner().run()
            assert (result.records_ingested, result.records_dropped) == (1, 1)
            stored = await db.get_source_message("github", "pr:owner/repo#12")
            assert stored["record_type"] == "github_pr_activity"
            assert stored["metadata"]["reason"] == "author"
            assert await db.get_source_message("github", "9000000004") is None
        finally:
            await stop(harness)

    async def test_other_github_events_are_rejected_and_settled(self, hosted):
        stream = await hosted.stream()
        invoke = hosted.gateway.store(source_record_event(purpose="invoke"))
        message = message_event(purpose="observe")
        message["provider"] = "github"
        chat = hosted.gateway.store(message)

        await stream.nudge("observe")

        assert await hosted.acknowledged(invoke, chat) == {invoke: "rejected", chat: "rejected"}
        assert {item["reason_code"] for ack in hosted.gateway.acks for item in ack} == {"admission_rejected"}

    async def test_a_failed_buffer_write_leaves_the_row_for_a_later_read(self, hosted, monkeypatch):
        calls = 0
        original = hosted.db.insert_channel_observation

        async def failing_once(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("database is locked")
            return await original(*args, **kwargs)

        monkeypatch.setattr(hosted.db, "insert_channel_observation", failing_once)
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(source_record_event())

        await stream.nudge("observe")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        assert calls == 2

    async def test_a_reload_publishes_the_changed_settings_on_every_stream(self, hosted):
        await hosted.stream()
        await hosted.stream()
        await hosted.subscriptions(2)
        hosted.config.sync.github.deny_reasons = ["subscribed", "comment"]
        hosted.config.sync.github.pr_watch = False
        engine = SimpleNamespace(get_channel_runtime={"hosted": hosted.runtime}.get)

        outcome = await config_reload._republish_hosted_subscriptions(engine)

        assert outcome == "published on 2 stream(s)"
        latest = (await hosted.subscriptions(4))[-1]
        assert latest["sources"][0]["filters"]["deny_reasons"] == ["subscribed", "comment"]
        assert "pr_watch" not in latest["sources"][0]

    async def test_a_stalled_stream_does_not_hold_up_a_republish(self, hosted, monkeypatch, caplog):
        monkeypatch.setattr("nerve.channels.hosted.stream._SUBSCRIPTIONS_SEND_SECONDS", 0.2)
        await hosted.stream()
        await hosted.stream()
        await hosted.subscriptions(2)
        stalled = hosted.runtime.streams.streams[0]
        caplog.set_level(logging.WARNING, logger="nerve.channels.hosted.stream")
        await stalled._send_lock.acquire()
        try:
            outcome = await asyncio.wait_for(hosted.runtime.republish_subscriptions(), timeout=3)
        finally:
            stalled._send_lock.release()

        assert outcome == "published on 1 stream(s)"
        assert (
            f"Channel stream {stalled.id}: source subscriptions not sent: the stream could not "
            "send them within 0.2s"
        ) in caplog.text

    async def test_a_stream_that_fails_to_publish_is_logged_and_the_others_still_send(
        self, hosted, monkeypatch, caplog,
    ):
        await hosted.stream()
        await hosted.stream()
        await hosted.subscriptions(2)
        failing = hosted.runtime.streams.streams[0]

        async def broken() -> bool:
            raise RuntimeError("socket gone")

        monkeypatch.setattr(failing, "publish_subscriptions", broken)
        caplog.set_level(logging.ERROR, logger="nerve.channels.hosted.manager")

        outcome = await hosted.runtime.republish_subscriptions()

        assert outcome == "published on 1 stream(s)"
        assert f"Channel stream {failing.id}: publishing the source subscriptions failed" in caplog.text

    async def test_a_publish_after_negotiation_that_fails_is_logged_at_once(self, hosted, monkeypatch, caplog):
        class Unchecked:
            def validate(self) -> None:
                raise RuntimeError("cannot check")

        monkeypatch.setattr(
            "nerve.channels.hosted.runtime.subscriptions_for", lambda config, providers: Unchecked(),
        )
        caplog.set_level(logging.ERROR, logger="nerve.channels.hosted.stream")

        stream = await hosted.stream()

        await eventually(lambda: "publishing the source subscriptions failed" in caplog.text)
        assert stream.sent("subscriptions") == []
        assert stream.close_code is None

    async def test_subscriptions_above_the_gateways_frame_limit_go_without_patterns(self, hosted):
        hosted.config.sync.github.allow_repos = [f"owner/{'r' * 200}-{n}" for n in range(100)]
        stream = await hosted.stream(negotiate=False)
        await stream.negotiate(frame_bytes=16384)

        [subscriptions] = await hosted.subscriptions()

        assert subscriptions["sources"] == [{"provider": "github", "filters": {}, "pr_watch": True}]
        assert stream.close_code is None

    async def test_subscriptions_refused_even_without_patterns_are_not_sent(self, hosted, monkeypatch, caplog):
        refused = Subscriptions(version="1", sources=(
            SourceSubscription(provider="github", filters=SourceFilters(allow_repos=("",))),
            SourceSubscription(provider="github"),
        ))
        monkeypatch.setattr(
            "nerve.channels.hosted.runtime.subscriptions_for", lambda config, providers: refused,
        )
        caplog.set_level(logging.WARNING, logger="nerve.channels.hosted.stream")

        stream = await hosted.stream()
        await hosted.gateway.wait_for(lambda: bool(stream.sent("inbox_read")))

        assert stream.sent("subscriptions") == []
        assert stream.close_code is None
        assert "source subscriptions not sent: the gateway would refuse them" in caplog.text

    async def test_a_switched_off_source_subscribes_to_nothing_and_keeps_what_arrives(self, hosted):
        hosted.config.sync.github.enabled = False
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(source_record_event())

        [subscriptions] = await hosted.subscriptions()
        await stream.nudge("observe")

        assert subscriptions == {"version": "1"}
        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        assert len(await hosted.db.read_channel_observations("github")) == 1


@pytest.mark.asyncio
class TestSharedStreams:
    async def test_slack_and_github_share_the_streams_and_the_reader(self, tmp_path, db, monkeypatch):
        harness = await start(tmp_path, db, monkeypatch, slack=True)
        try:
            assert set(harness.runtime.channels) == {"slack"}
            assert set(harness.runtime.sources) == {"github"}
            assert harness.router.get_channel("github") is None
            stream = await harness.stream()
            observed = harness.gateway.store(message_event(
                purpose="observe", conversation="C_FIXTURE_OBSERVATION", message_id="1700002000.000100",
            ))
            record = harness.gateway.store(source_record_event())

            await stream.nudge("observe")

            assert await harness.acknowledged(observed, record) == {observed: "accepted", record: "accepted"}
            assert len(await db.read_channel_observations("slack")) == 1
            assert len(await db.read_channel_observations("github")) == 1
            assert len(await harness.subscriptions()) == 1
        finally:
            await stop(harness)

    async def test_a_switched_off_hosted_slack_holds_the_github_records_back(self, tmp_path, db, monkeypatch):
        """One ordered inbox for every provider, which Nerve stops reading
        while a channel cannot take events: GitHub records wait while hosted
        Slack is off and arrive once it is on again."""
        harness = await start(tmp_path, db, monkeypatch, slack=True)
        try:
            harness.config.slack.enabled = False
            stream = await harness.stream()
            record = harness.gateway.store(source_record_event())

            await stream.nudge("observe")
            await asyncio.sleep(0.5)

            assert record not in harness.gateway.acknowledged
            assert await db.read_channel_observations("github") == []
            harness.config.slack.enabled = True
            assert (await harness.acknowledged(record))[record] == "accepted"
            assert len(await db.read_channel_observations("github")) == 1
        finally:
            await stop(harness)

    async def test_without_hosted_sources_no_subscriptions_are_sent(self, tmp_path, db, monkeypatch):
        harness = await start(tmp_path, db, monkeypatch, slack=True, github=False)
        try:
            stream = await harness.stream()
            await harness.gateway.wait_for(lambda: bool(stream.sent("inbox_read")))

            assert harness.runtime.subscriptions() is None
            assert stream.sent("subscriptions") == []
            assert await harness.runtime.republish_subscriptions() is None
        finally:
            await stop(harness)

    async def test_a_runtime_without_any_hosted_provider_is_refused(self, tmp_path, db):
        config = slack_hosted_config(tmp_path)
        config.slack = SlackConfig()

        with pytest.raises(ValueError, match="no provider is in hosted mode"):
            HostedChannelRuntime(config, ChannelRouter(MagicMock()), lambda: config, db=db)


# ---------------------------------------------------------------------- #
#  Jobs that a delivery runs                                              #
# ---------------------------------------------------------------------- #


def write_jobs(path: Path, jobs: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump({"jobs": jobs}), encoding="utf-8")


@pytest.mark.asyncio
class TestRunsOnDelivery:
    async def test_a_delivery_runs_the_gated_job_once_and_not_the_others(self, tmp_path, db, monkeypatch):
        harness = await start(tmp_path, db, monkeypatch)
        cron_dir = tmp_path / "cron"
        harness.config.cron.jobs_file = cron_dir / "jobs.yaml"
        harness.config.cron.system_file = cron_dir / "system.yaml"
        harness.config.cron.gate_plugins_dir = cron_dir / "gates"
        harness.config.cron.delivery_debounce_seconds = 1
        never = "0 0 1 1 *"
        write_jobs(harness.config.cron.jobs_file, [
            {"id": "pr-monitor", "schedule": never, "prompt": "Look at the PRs",
             "run_if": [{"type": "messages", "sources": ["github"]}]},
            {"id": "mail-triage", "schedule": never, "prompt": "Look at the mail",
             "run_if": [{"type": "messages", "sources": ["gmail"]}]},
            {"id": "daily-briefing", "schedule": never, "prompt": "Brief me"},
        ])
        turns: list[str] = []

        async def run_cron(**kwargs: Any):
            turns.append(kwargs["job_id"])
            return SimpleNamespace(text="done", outcome=SimpleNamespace(ok=True, describe=lambda: ""))

        engine = MagicMock()
        engine.run_cron = run_cron
        cron = CronService(harness.config, engine, db)
        await cron.start()
        harness.runtime.set_delivery_listener(cron.on_source_delivered)
        try:
            # The consumer cursor exists before the records arrive, as it does for a
            # job that has polled before.
            await db.get_consumer_cursor("inbox", "github")
            stream = await harness.stream()
            inbox_ids = [
                harness.gateway.store(source_record_event(record_id=str(9000000010 + n)))
                for n in range(3)
            ]

            await stream.nudge("observe")

            await harness.acknowledged(*inbox_ids)
            await eventually(lambda: turns == ["pr-monitor"])
            assert await db.get_source_message("github", "9000000012") is not None
            await asyncio.sleep(1.5)
            assert turns == ["pr-monitor"]
            logs = await db.get_cron_logs(job_id="source:github", limit=10)
            assert logs and all(log["status"] == "success" for log in logs)
        finally:
            await cron.stop()
            await stop(harness)


# ---------------------------------------------------------------------- #
#  The pull request gate, doctor, and the setup page                      #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestPullRequestGate:
    async def test_in_hosted_mode_it_passes_without_gh(self, db, monkeypatch):
        config = NerveConfig()
        config.sync.github = hosted_github()
        monkeypatch.setattr(config_module, "_config", config)

        async def no_process(*args, **kwargs):
            raise AssertionError("gh must not run in hosted mode")

        monkeypatch.setattr("nerve.cron.gates.asyncio.create_subprocess_exec", no_process)
        gate = GitHubPrActivityGate(author="example-agent")

        assert await gate.is_satisfied(GateContext(job_id="pr-monitor", db=db)) is True

    async def test_in_poll_mode_it_still_runs_gh(self, db, monkeypatch):
        monkeypatch.setattr(config_module, "_config", NerveConfig())
        calls: list[tuple[str, ...]] = []

        async def failing(*args, **kwargs):
            calls.append(args)
            raise FileNotFoundError("gh")

        monkeypatch.setattr("nerve.cron.gates.asyncio.create_subprocess_exec", failing)

        assert await GitHubPrActivityGate(author="example-agent").is_satisfied(
            GateContext(job_id="pr-monitor", db=db),
        ) is True
        assert calls and calls[0][0] == "gh"


def doctor_config(tmp_path: Path, **github: Any) -> NerveConfig:
    gateway = FakeChannelGateway(jwks_file=tmp_path / "gateway-jwks.json")
    return NerveConfig.from_dict({
        "sync": {"github": {"mode": "hosted", **github}},
        "channels": {"hosted": {**HOSTED, "gateway_jwks_file": str(gateway.jwks_file)}},
    })


class TestDoctor:
    def test_hosted_github_is_reported(self, tmp_path):
        report = doctor_report(doctor_config(tmp_path, pr_watch=False))

        assert (
            "[OK] GitHub notifications come from the channel gateway: filters published "
            "from sync.github, PR watch off"
        ) in report

    def test_unusable_hosted_settings_are_errors(self, tmp_path):
        config = doctor_config(tmp_path)
        config.channels.hosted.gateway_jwks_file = tmp_path / "missing.json"

        report = doctor_report(config)

        assert "[ERR] GitHub is hosted but channels.hosted.gateway_jwks_file: cannot read" in report

    def test_a_switched_off_hosted_source_says_so(self, tmp_path):
        report = doctor_report(doctor_config(tmp_path, enabled=False))

        assert "[--] GitHub is hosted but switched off (sync.github.enabled is false)" in report

    @pytest.mark.parametrize(("slack", "github", "warned"), [
        ({"mode": "hosted", "enabled": False}, {}, True),
        ({"mode": "hosted"}, {}, False),
        ({"mode": "socket", "enabled": False}, {}, False),
        ({"mode": "hosted", "enabled": False}, {"enabled": False}, False),
        ({"mode": "hosted", "enabled": False}, {"mode": "poll"}, False),
    ])
    def test_a_switched_off_hosted_slack_is_said_to_hold_github_back(self, tmp_path, slack, github, warned):
        config = doctor_config(tmp_path, **github)
        config.slack = SlackConfig.from_dict(slack)

        report = doctor_report(config)

        assert ("[WARN] Slack is hosted but switched off (slack.enabled is false)" in report) is warned

    def test_jobs_on_the_pull_request_gate_are_named(self, tmp_path):
        config = doctor_config(tmp_path)
        config.cron.jobs_file = tmp_path / "cron" / "jobs.yaml"
        config.cron.system_file = tmp_path / "cron" / "system.yaml"
        write_jobs(config.cron.jobs_file, [
            {"id": "pr-monitor", "schedule": "30m", "prompt": "PRs",
             "run_if": [{"type": "github_pr_activity", "author": "example-agent"}]},
            {"id": "inbox", "schedule": "30m", "prompt": "Inbox",
             "run_if": [{"type": "messages", "sources": ["github"]}]},
        ])

        report = doctor_report(config)

        assert "[WARN] Cron job(s) pr-monitor use the github_pr_activity gate" in report
        assert "inbox use the" not in report

    @pytest.mark.parametrize(("mode", "expected"), [
        ("hosted", "[--] gh CLI not found (not needed: GitHub comes from the channel gateway)"),
        ("poll", "[WARN] gh CLI not found (needed for sync)"),
    ])
    def test_gh_is_asked_for_only_when_something_runs_it(self, tmp_path, monkeypatch, mode, expected):
        import shutil

        monkeypatch.setattr(shutil, "which", lambda name: None)
        config = doctor_config(tmp_path)
        config.sync.github.mode = mode

        assert expected in doctor_report(config)


@pytest.mark.asyncio
class TestSetupPage:
    async def test_the_gateway_status_is_reported_when_only_github_is_hosted(self, monkeypatch):
        from nerve.gateway.routes.setup import _hosted_gateway_status

        config = NerveConfig()
        assert _hosted_gateway_status(config) is None
        config.sync.github = hosted_github()
        monkeypatch.setattr(gateway_server, "_hosted_channels", None)

        assert _hosted_gateway_status(config) == {"streams": 0, "connected": False, "last_event_at": None}

    async def test_the_sources_preflight_does_not_run_gh_in_hosted_mode(self, monkeypatch):
        import subprocess

        from nerve import setup_checks

        setup_checks.reset_caches()

        def no_gh(argv, **kwargs):
            raise AssertionError(f"ran {argv}")

        monkeypatch.setattr(subprocess, "run", no_gh)
        monkeypatch.setattr(setup_checks.shutil, "which", lambda name: None)
        config = NerveConfig()
        config.sync.github = hosted_github()
        try:
            result = await setup_checks.sources_preflight(config)
        finally:
            setup_checks.reset_caches()

        assert result["github"] == {
            "cli": False, "authenticated": False, "login": None,
            "hint": setup_checks.GH_HOSTED_HINT, "hosted": True,
        }

    async def test_the_environment_check_does_not_ask_for_gh_in_hosted_mode(self, monkeypatch):
        from nerve import setup_checks

        setup_checks.reset_caches()
        monkeypatch.setattr(setup_checks, "_version_of", lambda argv: None)
        config = NerveConfig()
        config.sync.github = hosted_github()
        try:
            checks = {check["id"]: check for check in await setup_checks.environment(config)}
            config.sync.github.mode = "poll"
            setup_checks.reset_caches()
            polling = {check["id"]: check for check in await setup_checks.environment(config)}
        finally:
            setup_checks.reset_caches()

        assert (checks["gh"]["status"], checks["gh"]["detail"]) == ("ok", "not needed")
        assert polling["gh"]["status"] == "missing"
