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
import copy
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

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
    fake gateway stores it, with *record* members changed."""
    event = copy.deepcopy(EVENT)
    del event["delivery"]
    event["record"].update(record)
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
        from nerve import config_reload

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
#  Jobs that a delivery runs                                              #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_mail_runs_the_inbox_job_once_and_not_the_others(tmp_path, db, monkeypatch):
    """The push wake: the delivery drains the mail at once, and a job gated
    on messages for any source (as the default inbox processor is) runs once."""
    harness = await start(tmp_path, db, monkeypatch, github=False, email=True)
    cron_dir = tmp_path / "cron"
    harness.config.cron.jobs_file = cron_dir / "jobs.yaml"
    harness.config.cron.system_file = cron_dir / "system.yaml"
    harness.config.cron.gate_plugins_dir = cron_dir / "gates"
    harness.config.cron.delivery_debounce_seconds = 1
    never = "0 0 1 1 *"
    write_jobs(harness.config.cron.jobs_file, [
        {"id": "inbox-processor", "schedule": never, "prompt": "Process the inbox",
         "run_if": [{"type": "messages", "consumer": "inbox"}]},
        {"id": "pr-monitor", "schedule": never, "prompt": "Look at the PRs",
         "run_if": [{"type": "messages", "sources": ["github"]}]},
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
        # The inbox consumer has read email before, so its cursor exists.
        await db.get_consumer_cursor("inbox", "email")
        stream = await harness.stream()
        inbox_ids = [
            harness.gateway.store(email_event(id=f"{RECORD_ID}-{n}")) for n in range(2)
        ]

        await stream.nudge("observe")

        await harness.acknowledged(*inbox_ids)
        await eventually(lambda: turns == ["inbox-processor"])
        assert await db.get_source_message("email", f"{RECORD_ID}-1") is not None
        await asyncio.sleep(1.5)
        assert turns == ["inbox-processor"]
        logs = await db.get_cron_logs(job_id="source:email", limit=10)
        assert logs and all(log["status"] == "success" for log in logs)
    finally:
        await cron.stop()
        await stop(harness)
