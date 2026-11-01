"""The hosted email source: settings, the subscription, the record consumer,
the drain into the inbox, and the jobs that a delivery runs.

``fixtures/channel_contract`` holds copies of the channel contract's samples
for the ``email`` provider: the subscriptions frame that Nerve sends, and an
``email_message`` source record event as the gateway delivers it. Nerve must
send that frame exactly, and must take that event into the inbox.

The end-to-end tests run the fake gateway against the real stream endpoint,
with a real database, source runner, and cron service; only the agent turn is
a stand-in.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import logging
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve import config_reload
from nerve.channels.hosted.contract import (
    Envelope,
    Payload,
    Rejected,
    SourceFilters,
    SourceSubscription,
    Subscriptions,
    encode_envelope,
)
from nerve.channels.hosted.contract.wire import dumps
from nerve.channels.hosted.runtime import hosted_providers
from nerve.channels.hosted.sources import (
    HostedSourceConsumer,
    email_subscription,
    hosted_email,
    hosted_sources,
    subscriptions_for,
)
from nerve.config import (
    EmailSyncConfig,
    GitHubSyncConfig,
    NerveConfig,
    SlackConfig,
    validate_config_keys,
)
from nerve.cron.service import CronService
from nerve.sources.email_hosted import HostedEmailSource
from nerve.sources.github_hosted import HostedGitHubSource
from nerve.sources.models import SourceRecord
from nerve.sources.registry import build_source_runners
from tests.fake_channel_gateway import source_record_event
from tests.test_hosted_channel_config import HOSTED
from tests.test_hosted_sources import (
    Harness,
    decoded,
    eventually,
    start,
    stop,
    write_jobs,
)

FIXTURES = Path(__file__).parent / "fixtures" / "channel_contract"


def fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


EVENT = fixture("email_message.json")
FRAME = fixture("subscriptions_email.json")
RECORD_ID = EVENT["record"]["id"]
SENDER = EVENT["conversation"]["id"]


def email_event(**record: Any) -> dict[str, Any]:
    """The contract's ``email_message`` event without its delivery, as the
    fake gateway stores it, with *record* members changed. The event ID
    follows the record ID, as the gateway builds it."""
    event = copy.deepcopy(EVENT)
    del event["delivery"]
    event["record"].update(record)
    event["event_id"] = f"email:{event['record']['id']}"
    event["message_revision_id"] = event["record"]["id"]
    return event


def drained_metadata() -> dict[str, Any]:
    """The metadata of the contract's record as the inbox keeps it."""
    metadata: dict[str, Any] = {}
    for entry in EVENT["record"]["metadata"]:
        if "values" in entry or entry["key"] == "attachments":
            metadata[entry["key"]] = entry.get("values", [])
        elif entry["key"] == "truncated":
            metadata[entry["key"]] = entry["value"] == "true"
        else:
            metadata[entry["key"]] = entry.get("value", "")
    return metadata


def hosted_config(**email: Any) -> NerveConfig:
    """A config with usable hosted settings and the email source on."""
    config = NerveConfig.from_dict({"channels": {"hosted": HOSTED}})
    config.sync.email = EmailSyncConfig(enabled=True, **email)
    return config


def email_runner(harness: Harness):
    (runner,) = [r for r in build_source_runners(harness.config, harness.db) if r.job_id == "source:email"]
    return runner


# ---------------------------------------------------------------------- #
#  Settings                                                               #
# ---------------------------------------------------------------------- #


class TestSettings:
    def test_the_source_is_off_by_default_with_the_github_defaults(self):
        email = NerveConfig().sync.email

        assert (email.enabled, email.schedule, email.batch_size, email.condense) == (
            False, "*/15 * * * *", 30, False,
        )
        assert EmailSyncConfig.from_dict({}) == email

    def test_the_settings_are_read(self):
        email = NerveConfig.from_dict({"sync": {"email": {
            "enabled": "true", "schedule": "*/5 * * * *", "batch_size": "10", "condense": "yes",
        }}}).sync.email

        assert (email.enabled, email.schedule, email.batch_size, email.condense) == (
            True, "*/5 * * * *", 10, True,
        )
        assert validate_config_keys({"sync": {"email": {
            "enabled": True, "schedule": "*/5 * * * *", "batch_size": 10, "condense": False,
        }}}) == []

    def test_a_schedule_the_scheduler_refuses_fails_the_validation(self, tmp_path):
        from nerve.config_validate import validate_config_bundle

        (tmp_path / "config.yaml").write_text(
            "sync:\n  email:\n    schedule: \"99 * * * *\"\n", encoding="utf-8",
        )

        errors = validate_config_bundle(tmp_path).errors
        assert any(e.startswith("sync.email.schedule:") for e in errors)

    def test_email_is_hosted_only_with_usable_hosted_settings(self):
        config = hosted_config()

        assert hosted_email(config) is True
        assert hosted_sources(config) == hosted_providers(config) == ["email"]
        config.sync.github = GitHubSyncConfig(mode="hosted")
        config.slack = SlackConfig(mode="hosted")
        assert hosted_providers(config) == ["slack", "github", "github_repos", "email"]

    @pytest.mark.parametrize("hosted", [
        {},
        {**HOSTED, "gateway_jwks_file": "relative/gateway-jwks.json"},
        {**HOSTED, "agent_id": "not-a-uuid"},
    ])
    def test_without_usable_hosted_settings_email_is_not_hosted(self, hosted):
        config = NerveConfig.from_dict({"sync": {"email": {"enabled": True}}, "channels": {"hosted": hosted}})

        assert hosted_email(config) is False
        assert hosted_sources(config) == hosted_providers(config) == []

    def test_switching_email_on_reports_the_restart_and_off_does_not(self):
        off, on = NerveConfig(), NerveConfig()
        on.sync.email.enabled = True

        assert config_reload.restart_required(off, on) == ["sync.email.enabled: False → True"]
        assert config_reload.restart_required(on, off) == []

    def test_a_switched_off_source_is_not_hosted(self):
        config = hosted_config()
        config.sync.email.enabled = False

        assert hosted_email(config) is False
        assert hosted_sources(config) == []


# ---------------------------------------------------------------------- #
#  The subscription                                                       #
# ---------------------------------------------------------------------- #


class TestSubscription:
    def test_the_frame_is_the_contract_sample(self):
        config = hosted_config()
        body = subscriptions_for(config, hosted_sources(config))
        body.validate()
        envelope = Envelope(
            version="1", kind="subscriptions", request_id=FRAME["request_id"],
            payload=Payload(subscriptions=body),
        )

        assert json.loads(encode_envelope(envelope)) == FRAME

    def test_the_entry_carries_the_provider_and_empty_filters_only(self):
        assert dumps(email_subscription()) == '{"provider":"email","filters":{}}'

    def test_email_follows_the_github_sources(self):
        config = hosted_config()
        config.sync.github = GitHubSyncConfig(mode="hosted")

        assert dumps(subscriptions_for(config, hosted_sources(config))) == (
            '{"version":"1","sources":[{"provider":"github","filters":{},"pr_watch":true},'
            '{"provider":"email","filters":{}}]}'
        )

    def test_a_source_switched_off_by_a_reload_is_left_out(self):
        """The runtime keeps the email consumer it started with; the
        subscriptions follow the setting, so the gateway refuses the mail."""
        config = hosted_config()
        providers = hosted_sources(config)
        config.sync.email.enabled = False

        assert subscriptions_for(config, providers) == Subscriptions(version="1")

    def test_a_github_frame_is_unchanged_while_email_is_off(self):
        config = NerveConfig.from_dict({"channels": {"hosted": HOSTED}})
        config.sync.github = GitHubSyncConfig(mode="hosted")

        assert dumps(subscriptions_for(config, hosted_sources(config))) == (
            '{"version":"1","sources":[{"provider":"github","filters":{},"pr_watch":true}]}'
        )

    @pytest.mark.parametrize("source", [
        SourceSubscription(provider="email", filters=SourceFilters(deny_actors=("someone",))),
        SourceSubscription(provider="email", pr_watch=True),
        SourceSubscription(provider="email", inbox="all"),
    ])
    def test_an_email_subscription_with_another_member_is_not_sent(self, source):
        """The gateway refuses it and closes the stream, so Nerve checks first."""
        with pytest.raises(Rejected) as refused:
            Subscriptions(version="1", sources=(source,)).validate()

        assert refused.value.reason == "malformed_frame"


# ---------------------------------------------------------------------- #
#  Consumer, buffer, and drain                                            #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestRecords:
    async def test_the_contract_event_is_buffered_and_drained_with_typed_metadata(self, db):
        config = hosted_config()
        heard: list[str] = []
        consumer = HostedSourceConsumer("email", db, lambda: config)
        consumer.set_delivery_listener(heard.append)

        disposition = await consumer.deliver(decoded(email_event()))

        assert disposition.outcome == "accepted"
        assert heard == ["email"]
        [(_, payload)] = await db.read_channel_observations("email")
        assert payload["conversation_id"] == SENDER
        assert payload["connection_id"] == EVENT["connection_id"]
        result = await HostedEmailSource(db).fetch(None, limit=10)
        [record] = result.records
        assert (record.id, record.source, record.record_type) == (RECORD_ID, "email", "email_message")
        assert (record.summary, record.content) == (EVENT["record"]["summary"], EVENT["record"]["content"])
        assert record.timestamp == EVENT["record"]["timestamp"]
        assert record.metadata == drained_metadata()
        assert isinstance(record.metadata["attachments"], list)
        assert record.metadata["truncated"] is False

    async def test_a_cut_mail_without_attachments(self, db):
        metadata = [
            {"key": "sender_principal_id", "value": SENDER},
            {"key": "attachments"},
            {"key": "truncated", "value": "true"},
        ]
        await HostedSourceConsumer("email", db, hosted_config).deliver(
            decoded(email_event(metadata=metadata)),
        )

        [record] = (await HostedEmailSource(db).fetch(None)).records

        assert record.metadata == {"sender_principal_id": SENDER, "attachments": [], "truncated": True}

    async def test_the_source_is_push_fed_and_drains_only_its_own_buffer(self, db):
        await db.insert_channel_observation("github", "github:owner/repo", {"record": {
            "id": "9000000001", "record_type": "github_notification", "summary": "not email",
        }})
        source = HostedEmailSource(db)

        assert (source.source_name, source.push_fed) == ("email", True)
        assert await source.has_undrained(None) is False
        await HostedSourceConsumer("email", db, hosted_config).deliver(decoded(email_event()))
        assert await source.has_undrained(None) is True
        result = await source.fetch(None)
        assert [r.id for r in result.records] == [RECORD_ID]
        assert await source.has_undrained(result.next_cursor) is False


# ---------------------------------------------------------------------- #
#  Registry and schedule                                                  #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestRegistry:
    def runners(self, config, db) -> list:
        return [r for r in build_source_runners(config, db) if r.job_id == "source:email"]

    async def test_a_hosted_agent_drains_the_email_buffer_with_its_settings(self, db):
        [runner] = self.runners(hosted_config(batch_size=12, condense=True), db)

        assert isinstance(runner.source, HostedEmailSource)
        assert (runner.batch_size, runner.condense) == (12, True)
        assert (runner.inbox_filter, runner.drop_hook) == (None, [])

    async def test_without_hosted_settings_nothing_is_registered_and_a_warning_says_why(self, db, caplog):
        config = NerveConfig.from_dict({"sync": {"email": {"enabled": True}}})

        with caplog.at_level(logging.WARNING, logger="nerve.sources.registry"):
            assert self.runners(config, db) == []

        [warning] = [r.getMessage() for r in caplog.records if r.name == "nerve.sources.registry"]
        assert warning.startswith("sync.email.enabled is set, but the email source works only in hosted mode")
        assert "channels.hosted.gateway_jwks_file must name the gateway's public key file" in warning

    async def test_a_switched_off_source_has_no_runner_and_no_warning(self, db, caplog):
        config = hosted_config()
        config.sync.email.enabled = False

        with caplog.at_level(logging.WARNING, logger="nerve.sources.registry"):
            assert self.runners(config, db) == []
            assert self.runners(NerveConfig(), db) == []

        assert [r for r in caplog.records if r.name == "nerve.sources.registry"] == []

    async def test_a_reload_keeps_the_drain_until_email_is_switched_off(self, db):
        """channels.hosted is restart-only: the runtime keeps taking email
        after a reload breaks those settings, so the drain stays too."""
        service = CronService(hosted_config(), AsyncMock(), db)
        service.scheduler.start(paused=True)
        try:
            service._register_source_runners()
            service.config = NerveConfig.from_dict({"sync": {"email": {"enabled": True}}})
            assert "source:email" in (await service.reload_sources())["sources"]
            service.config.sync.email.enabled = False
            assert "source:email" not in (await service.reload_sources())["sources"]
        finally:
            service.scheduler.shutdown(wait=False)

    async def forecast(self, config: NerveConfig, db) -> dict[str, Any]:
        service = CronService(config, AsyncMock(), db)
        service.scheduler.start(paused=True)
        try:
            service._register_source_runners()
            schedule = {r.job_id: service._source_schedule(r) for r in service._source_runners}
            report = await service.schedule_report()
        finally:
            service.scheduler.shutdown(wait=False)
        return {"schedule": schedule["source:email"], **{job["id"]: job for job in report["jobs"]}["source:email"]}

    async def test_the_drain_runs_on_its_schedule_and_is_no_wake_while_nothing_waits(self, db):
        """The gateway delivers the mail and the delivery runs the drain; with
        nothing waiting, the scheduled run is not counted as the next task."""
        from nerve.cron import service as cron_service

        config = hosted_config(schedule="*/20 * * * *")
        idle = await self.forecast(config, db)
        await HostedSourceConsumer("email", db, lambda: config).deliver(decoded(email_event()))
        waiting = await self.forecast(config, db)

        assert idle["schedule"] == "*/20 * * * *"
        assert (idle["forecast"], idle["reason"]) == ("skip", cron_service._PUSH_FED_REASON)
        assert waiting["forecast"] == "run"


# ---------------------------------------------------------------------- #
#  End to end over real streams                                           #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestStreams:
    async def test_a_stream_publishes_the_contract_subscriptions(self, tmp_path, db, monkeypatch):
        harness = await start(tmp_path, db, monkeypatch, github=False, email=True)
        try:
            assert set(harness.runtime.sources) == {"email"}
            await harness.stream()

            [subscriptions] = await harness.subscriptions()

            assert subscriptions == FRAME["payload"]["subscriptions"]
        finally:
            await stop(harness)

    async def test_a_member_mail_reaches_the_inbox(self, tmp_path, db, monkeypatch):
        harness = await start(tmp_path, db, monkeypatch, github=False, email=True)
        try:
            stream = await harness.stream()
            inbox_id = harness.gateway.store(email_event())

            await stream.nudge("observe")

            assert (await harness.acknowledged(inbox_id))[inbox_id] == "accepted"
            assert harness.router.get_channel("email") is None
            result = await email_runner(harness).run()
            assert result.records_ingested == 1
            stored = await db.get_source_message("email", RECORD_ID)
            assert stored["record_type"] == "email_message"
            assert stored["summary"] == EVENT["record"]["summary"]
            assert stored["content"] == EVENT["record"]["content"]
            assert stored["metadata"] == drained_metadata()
        finally:
            await stop(harness)

    async def test_a_runtime_without_the_email_source_rejects_and_settles_email(self, tmp_path, db, monkeypatch):
        """The email source was off at startup: the subscriptions leave email
        out, and a record that still arrives is settled, not kept."""
        harness = await start(tmp_path, db, monkeypatch)
        try:
            assert "email" not in harness.runtime.sources
            stream = await harness.stream()
            [subscriptions] = await harness.subscriptions()
            assert [s["provider"] for s in subscriptions["sources"]] == ["github"]
            inbox_id = harness.gateway.store(email_event())

            await stream.nudge("observe")

            assert (await harness.acknowledged(inbox_id))[inbox_id] == "rejected"
            assert {item["reason_code"] for ack in harness.gateway.acks for item in ack} == {
                "configuration_invalid",
            }
            assert await db.read_channel_observations("email") == []
        finally:
            await stop(harness)

    async def test_a_reload_that_switches_email_off_unsubscribes(self, tmp_path, db, monkeypatch):
        harness = await start(tmp_path, db, monkeypatch, github=False, email=True)
        try:
            await harness.stream()
            await harness.subscriptions()
            harness.config.sync.email.enabled = False
            engine = SimpleNamespace(get_channel_runtime={"hosted": harness.runtime}.get)

            outcome = await config_reload._republish_hosted_subscriptions(engine)

            assert outcome == "published on 1 stream(s)"
            assert (await harness.subscriptions(2))[-1] == {"version": "1"}
            assert [r.job_id for r in build_source_runners(harness.config, db)] == ["source:github"]
        finally:
            await stop(harness)

    async def test_github_and_email_share_the_streams(self, tmp_path, db, monkeypatch):
        harness = await start(tmp_path, db, monkeypatch, email=True)
        try:
            assert set(harness.runtime.sources) == {"github", "github_repos", "email"}
            stream = await harness.stream()
            [subscriptions] = await harness.subscriptions()
            assert [s["provider"] for s in subscriptions["sources"]] == ["github", "email"]
            mail = harness.gateway.store(email_event())
            notification = harness.gateway.store(source_record_event())

            await stream.nudge("observe")

            assert await harness.acknowledged(mail, notification) == {mail: "accepted", notification: "accepted"}
            assert len(await db.read_channel_observations("email")) == 1
            assert len(await db.read_channel_observations("github")) == 1
        finally:
            await stop(harness)


# ---------------------------------------------------------------------- #
#  New mail is unread, and the jobs that a delivery runs                  #
# ---------------------------------------------------------------------- #


NEVER = "0 0 1 1 *"


def default_inbox_job() -> dict[str, Any]:
    """The default inbox processor, as the setup page writes it, on a
    schedule that does not come in the test."""
    from nerve.setup_writer import PRODUCTIVITY_CRONS

    [job] = [job for job in PRODUCTIVITY_CRONS if job["id"] == "inbox-processor"]
    return {"id": job["id"], "schedule": NEVER, "prompt": job["prompt"], "run_if": job["run_if"]}


@contextlib.asynccontextmanager
async def running_cron(harness: Harness, jobs: list[dict[str, Any]]) -> AsyncIterator[tuple[CronService, list[str]]]:
    """The cron service on *harness*, with *jobs*, hearing the deliveries.
    Yields it with the IDs of the jobs that ran, in order."""
    cron_dir = harness.config.workspace.parent / "cron"
    harness.config.cron.jobs_file = cron_dir / "jobs.yaml"
    harness.config.cron.system_file = cron_dir / "system.yaml"
    harness.config.cron.gate_plugins_dir = cron_dir / "gates"
    harness.config.cron.delivery_debounce_seconds = 1
    # No polling GitHub source: the test makes no gh call.
    harness.config.sync.github.enabled = harness.config.sync.github.mode == "hosted"
    write_jobs(harness.config.cron.jobs_file, jobs)
    turns: list[str] = []

    async def run_cron(**kwargs: Any):
        turns.append(kwargs["job_id"])
        return SimpleNamespace(text="done", outcome=SimpleNamespace(ok=True, describe=lambda: ""))

    engine = MagicMock()
    engine.run_cron = run_cron
    cron = CronService(harness.config, engine, harness.db)
    await cron.start()
    harness.runtime.set_delivery_listener(cron.on_source_delivered)
    try:
        yield cron, turns
    finally:
        await cron.stop()


async def poll_all(harness: Harness, consumer: str) -> str:
    """What ``poll_all_sources`` gives *consumer*."""
    from nerve.agent.tools.handlers.sources import poll_all_sources_handler
    from nerve.agent.tools.registry import ToolContext

    ctx = ToolContext(session_id="test", db=harness.db, config=harness.config)
    result = await poll_all_sources_handler(ctx, {"consumer": consumer})
    return result.content[0]["text"]


async def cursors(db) -> dict[tuple[str, str], int]:
    async with db.db.execute("SELECT consumer, source, cursor_seq FROM consumer_cursors") as rows:
        return {(row[0], row[1]): row[2] async for row in rows}


async def deliver(harness: Harness, stream, **record: Any) -> None:
    """Store one mail at the gateway and wait until Nerve takes it."""
    inbox_id = harness.gateway.store(email_event(**record))
    await stream.nudge("observe")
    assert (await harness.acknowledged(inbox_id))[inbox_id] == "accepted"


def email_runner_of(cron: CronService):
    (runner,) = [r for r in cron._source_runners if r.job_id == "source:email"]
    return runner


@pytest.mark.asyncio
class TestNewMailIsUnread:
    async def test_a_fresh_agent_reads_its_first_mail(self, tmp_path, db, monkeypatch):
        """No consumer has read anything yet. The default inbox processor's
        gate passes for the first mail, which the delivery drained at once,
        and its poll returns that mail. The other jobs do not run."""
        harness = await start(tmp_path, db, monkeypatch, github=False, email=True)
        try:
            jobs = [
                default_inbox_job(),
                {"id": "pr-monitor", "schedule": NEVER, "prompt": "Look at the PRs",
                 "run_if": [{"type": "messages", "sources": ["github"]}]},
                {"id": "daily-briefing", "schedule": NEVER, "prompt": "Brief me"},
            ]
            async with running_cron(harness, jobs) as (_, turns):
                assert await cursors(db) == {}
                stream = await harness.stream()

                await deliver(harness, stream)

                await eventually(lambda: turns == ["inbox-processor"])
                await asyncio.sleep(1.5)
                assert turns == ["inbox-processor"]
                assert await cursors(db) == {("inbox", "email"): 0}
                logs = await db.get_cron_logs(job_id="source:email", limit=10)
                assert logs and all(log["status"] == "success" for log in logs)
            text = await poll_all(harness, "inbox")
            assert f"email: {EVENT['record']['summary']}" in text
            assert "email: 1 new" in text
        finally:
            await stop(harness)

    async def test_an_agent_that_reads_other_sources_reads_its_first_mail(self, tmp_path, db, monkeypatch):
        """Email is switched on later. The consumers that already hold cursors
        get one on email too, also a consumer that no gate names."""
        await db.insert_source_messages([source_record(n) for n in range(2)], "github")
        await db.set_consumer_cursor("inbox", "github", await db.get_source_max_rowid("github"))
        await db.set_consumer_cursor("weekly", "github", 0)
        harness = await start(tmp_path, db, monkeypatch, github=False, email=True)
        try:
            async with running_cron(harness, [default_inbox_job()]) as (_, turns):
                stream = await harness.stream()

                await deliver(harness, stream)

                await eventually(lambda: turns == ["inbox-processor"])
            inbox = await poll_all(harness, "inbox")
            weekly = await poll_all(harness, "weekly")
            assert "email: 1 new" in inbox and "github: 0 new" in inbox
            assert "email: 1 new" in weekly and "github: 2 new" in weekly
        finally:
            await stop(harness)

    async def test_the_first_mail_after_a_long_park_is_read(self, tmp_path, db, monkeypatch):
        """The machine was parked for longer than the cursor TTL: every cursor
        of the inbox consumer expired. The mail that wakes it passes the gate,
        and the poll returns it instead of starting after it."""
        await db.insert_source_messages([email_record(n) for n in range(2)], "email")
        read = await db.get_source_max_rowid("email")
        # Written three days ago with a TTL of two days: expired a day ago.
        await db.set_consumer_cursor("inbox", "email", read, ttl_days=-1)
        await db.set_consumer_cursor("inbox", "github", 0, ttl_days=-1)
        harness = await start(tmp_path, db, monkeypatch, github=False, email=True)
        try:
            async with running_cron(harness, [default_inbox_job()]) as (_, turns):
                stream = await harness.stream()

                await deliver(harness, stream)

                await eventually(lambda: turns == ["inbox-processor"])
                assert await db.peek_consumer_cursor("inbox", "email") == read
            text = await poll_all(harness, "inbox")
            assert "email: 1 new" in text
            assert f"email: {EVENT['record']['summary']}" in text
        finally:
            await stop(harness)

    async def test_a_live_cursor_with_unread_mail_does_not_move(self, tmp_path, db, monkeypatch):
        """A scheduled drain and a manual one leave a live cursor where it is,
        so the mail it has not read stays unread."""
        await db.insert_source_messages([email_record(n) for n in range(3)], "email")
        await db.set_consumer_cursor("inbox", "email", 1)
        harness = await start(tmp_path, db, monkeypatch, github=False, email=True)
        try:
            async with running_cron(harness, [default_inbox_job()]) as (cron, _):
                runner = email_runner_of(cron)
                for n, drain in enumerate((
                    cron._run_source_wrapper(runner), runner.run(),
                )):
                    await HostedSourceConsumer("email", db, lambda: harness.config).deliver(
                        decoded(email_event(id=f"{RECORD_ID}-new-{n}")),
                    )
                    await drain
                    assert await db.peek_consumer_cursor("inbox", "email") == 1
            assert "email: 4 new" in await poll_all(harness, "inbox")
        finally:
            await stop(harness)

    async def test_a_consumer_that_a_cron_reload_adds_reads_the_next_mail(self, tmp_path, db, monkeypatch):
        harness = await start(tmp_path, db, monkeypatch, github=False, email=True)
        try:
            async with running_cron(harness, [default_inbox_job()]) as (cron, turns):
                stream = await harness.stream()
                await deliver(harness, stream)
                await eventually(lambda: turns == ["inbox-processor"])
                first = await db.get_source_max_rowid("email")
                write_jobs(harness.config.cron.jobs_file, [
                    default_inbox_job(),
                    {"id": "triage", "schedule": NEVER, "prompt": "Triage the mail",
                     "run_if": [{"type": "messages", "consumer": "triage"}]},
                ])
                await cron.reload()
                assert ("triage", "email") not in await cursors(db)

                await deliver(harness, stream, id=f"{RECORD_ID}-2", summary="Email from Robin Example: Second")

                await eventually(lambda: "triage" in turns)
                assert await db.peek_consumer_cursor("triage", "email") == first
            text = await poll_all(harness, "triage")
            assert "email: 1 new" in text and "email: Email from Robin Example: Second" in text
        finally:
            await stop(harness)

    async def test_a_restart_or_a_second_step_moves_no_cursor(self, db):
        await db.insert_source_messages([email_record(n) for n in range(3)], "email")
        await db.set_consumer_cursor("inbox", "email", 1)

        for _ in range(2):
            assert await db.start_consumer_cursors("email", ["inbox"], ttl_days=2) == []

        assert await cursors(db) == {("inbox", "email"): 1}

    async def test_a_missing_or_expired_cursor_starts_at_the_newest_mail(self, db):
        """A mail stored before, say before a restart, is not new to a
        consumer that starts reading now."""
        await db.insert_source_messages([email_record(n) for n in range(2)], "email")
        newest = await db.get_source_max_rowid("email")
        await db.set_consumer_cursor("expired", "email", 0, ttl_days=-1)

        started = await db.start_consumer_cursors("email", ["triage", ""], ttl_days=2)

        assert started == ["expired", "triage"]
        assert await db.peek_consumer_cursor("triage", "email") == newest
        assert await db.peek_consumer_cursor("expired", "email") == newest

    async def test_a_failed_step_still_stores_the_mail(self, db, monkeypatch, caplog):
        async def locked(*args, **kwargs):
            raise RuntimeError("database is locked")

        [runner] = [r for r in build_source_runners(hosted_config(), db) if r.job_id == "source:email"]
        monkeypatch.setattr(db, "start_consumer_cursors", locked)
        await HostedSourceConsumer("email", db, hosted_config).deliver(decoded(email_event()))

        with caplog.at_level(logging.WARNING, logger="nerve.sources.runner"):
            result = await runner.run()

        assert (result.records_ingested, result.error) == (1, None)
        assert "could not start the consumer cursors: database is locked" in caplog.text


@pytest.mark.parametrize(("source", "starts"), [
    (HostedEmailSource, True),
    (HostedGitHubSource, False),
    (lambda db: HostedGitHubSource(db, "github_repos"), False),
])
def test_only_the_email_source_starts_consumer_cursors(source, starts):
    """Hosted GitHub keeps the cursors that start when a consumer first reads it."""
    assert source(AsyncMock()).starts_consumer_cursors is starts


def email_record(n: int) -> SourceRecord:
    return SourceRecord(
        id=f"{RECORD_ID}-{n}", source="email", record_type="email_message",
        summary=f"Email from Example Member: note {n}", content="Hi", timestamp="2026-09-16T11:00:00Z",
    )


def source_record(n: int) -> SourceRecord:
    return SourceRecord(
        id=f"900000000{n}", source="github", record_type="github_notification",
        summary=f"[owner/repo] Fix {n} (mention)", content="Repository: owner/repo",
        timestamp="2026-09-16T10:00:00Z",
    )
