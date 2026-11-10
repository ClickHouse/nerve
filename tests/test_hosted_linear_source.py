"""The hosted Linear source: settings, the subscription, the record consumer, the
drain into the inbox with the rules as guardrail, and ``nerve doctor``.

Linear is a generic hosted source: the channel gateway sends each record's
``data``, a JSON object that the provider defines (``linear.issue_event/1``),
and Nerve keeps it, with the schema, as the record's metadata.

The fixtures follow the gateway's Linear provider: every record data is valid
against a byte-exact copy of its schema file
(``fixtures/sources/linear/linear_issue_event.v1.schema.json``), and the IDs
follow its rules. ``data.server.id`` is the catalog server ID (a slug); the
connection ID is a UUIDv5 of the tenant and that server; the record ID is
``<server>:<event>:<entity>``, the same for every version of the event, so a
changed event replaces the earlier copy; the event ID adds the version in
Unix milliseconds. The values are test data.

The end-to-end tests run the fake gateway against the real stream endpoint,
as the hosted GitHub tests do, with a real database and source runner.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import jsonschema
import pytest
import pytest_asyncio
import yaml

import nerve.config as config_module
from nerve import config_reload
from nerve.agent.sessions import SessionManager
from nerve.agent.tools.handlers.sources import _format_source_batch
from nerve.channels.hosted.contract import MAX_FRAME_BYTES, Rejected, SourceRule, Subscriptions
from nerve.channels.hosted.contract.model import (
    MAX_SOURCE_RULE_PATTERNS,
    MAX_SOURCE_RULES,
    MAX_SUBSCRIPTION_PATTERN_BYTES,
)
from nerve.channels.hosted.contract.wire import dumps
from nerve.channels.hosted.runtime import HostedChannelRuntime, hosted_providers
from nerve.channels.hosted.sources import (
    LINEAR,
    LINEAR_SCHEMAS,
    HostedSourceConsumer,
    buffered_record,
    hosted_sources,
    linear_subscription,
    subscriptions_for,
    subscriptions_frame_bytes,
)
from nerve.channels.router import ChannelRouter
from nerve.cli import doctor_report
from nerve.config import (
    LINEAR_EVENTS,
    SOURCE_RULE_PATTERN_MAX_BYTES,
    SOURCE_RULE_PATTERNS_MAX,
    SOURCE_RULES_MAX,
    LinearSyncConfig,
    NerveConfig,
    SlackConfig,
    SourceRuleConfig,
    validate_config_keys,
)
from nerve.gateway import server as gateway_server
from nerve.sources.filters import DataRule
from nerve.sources.github_hosted import HostedGitHubSource
from nerve.sources.hosted import HostedSource, record_from_buffer
from nerve.sources.registry import build_source_runners
from nerve.sources.source_json import record_metadata
from tests.fake_channel_gateway import TENANT_ID, FakeChannelGateway, FakeStream, NerveServer, source_record_event
from tests.test_hosted_channel import FAST_READER, FAST_STREAM
from tests.test_hosted_channel import hosted_config as slack_hosted_config
from tests.test_hosted_channel_config import HOSTED
from tests.test_hosted_sources import decoded, hosted_github

ROOT = Path(__file__).parent.parent
# A copy of the schema file of linear.issue_event/1 of the hosted gateway's
# contract (see fixtures/sources/README.md), byte for byte.
SCHEMA_FILE = Path(__file__).parent / "fixtures" / "sources" / "linear" / "linear_issue_event.v1.schema.json"
SCHEMA_SHA256 = "ef43b2e7e5b63d94ee4cf00893f4baa7cbf19258fa3890d75a268fa7e1ad9c85"
SCHEMA = LINEAR_SCHEMAS[0]
# The record data that the Sources page tests render.
WEB_FIXTURES = ROOT / "web" / "src" / "test" / "linearIssueEvents.json"

# The catalog server ID of the Linear connector, and the connection ID that the
# gateway derives from the tenant and that server.
SERVER = "linear"
CONNECTION_NAMESPACE = uuid.UUID("6c1f3a8e-2b47-5d90-8e1c-4a7b9d2e5f30")
CONNECTION_ID = str(uuid.uuid5(CONNECTION_NAMESPACE, uuid.UUID(TENANT_ID).bytes + SERVER.encode()))
ISSUE_ID = "5f0c1b9e-3c2a-4d7e-9b1f-8a6d2e4c7b30"
HISTORY_ID = "2d4f6a8c-1e3b-4d5f-8a7c-9e1b3d5f7a90"
ALICE_ID = "0b7d4c2a-6e1f-4a3b-9c8d-2f5e7a1b3c49"
AGENT_USER_ID = "8d1e3f5a-7b9c-4d2e-a6f8-1c3e5a7b9d20"
VERSION_MS = 1793433600000  # 2026-10-31T08:00:00Z


def linear_data(**changes: Any) -> dict[str, Any]:
    """The data of an assignment of the issue to the agent's Linear user."""
    data: dict[str, Any] = {
        "event": "assigned",
        "via": "watch",
        "server": {"id": SERVER, "name": "Linear"},
        "issue": {
            "id": ISSUE_ID,
            "identifier": "ENG-12",
            "title": "Fix the login page",
            "url": "https://linear.example.test/acme/issue/ENG-12",
            "team": {"key": "ENG"},
            "project": {"id": "7a2c4e6f-8b1d-4f3a-9c5e-0d2b4f6a8c1e", "name": "Web"},
            "state": {"name": "Todo", "type": "unstarted"},
            "priority": 2,
            "assignee": {"id": AGENT_USER_ID, "name": "Example Agent"},
        },
        "actor": {"id": ALICE_ID, "name": "Alice Example", "kind": "user"},
        "changes": [{"field": "assignee", "to": "Example Agent"}],
    }
    data.update(changes)
    return data


def comment_data() -> dict[str, Any]:
    """A comment on the issue, cut to its bound, from the agent's inbox."""
    return linear_data(
        event="commented", via="inbox", changes=[], comment={
            "id": "9e2f6a4b-1c3d-4e5f-a7b8-c9d0e1f2a3b4",
            "url": "https://linear.example.test/acme/issue/ENG-12#comment-9e2f6a4b",
            "body": "x" * 4000, "truncated": True,
        },
    )


def bot_data() -> dict[str, Any]:
    return linear_data(event="state_changed", actor={"id": "b0b0b0b0", "name": "Triage", "kind": "bot"},
                       changes=[{"field": "state", "from": "Backlog", "to": "Todo"}])


def system_data() -> dict[str, Any]:
    """A change without an actor, such as an automatic close."""
    return linear_data(event="state_changed", actor={"kind": "system"},
                       changes=[{"field": "state", "from": "Todo", "to": "Canceled"}])


def own_data() -> dict[str, Any]:
    """An action of the Linear user of a tenant API key: kept and marked."""
    return linear_data(actor={"id": ALICE_ID, "name": "Alice Example", "kind": "user", "credential_user": True})


def record_id(event: str = "assigned", entity: str = HISTORY_ID) -> str:
    return f"{SERVER}:{event}:{entity}"


def linear_event(
    *, event: str = "assigned", entity: str = HISTORY_ID, version_ms: int = VERSION_MS,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A Linear record as the gateway delivers it, without a delivery."""
    data = data if data is not None else linear_data(event=event)
    actor = data["actor"]
    return {
        "event_id": f"linear:{record_id(event, entity)}:{version_ms}",
        "message_revision_id": f"{record_id(event, entity)}:{version_ms}",
        "connection_id": CONNECTION_ID,
        "provider": "linear",
        "kind": "source_record",
        "conversation": {"id": ISSUE_ID, "kind": "item", "display_name": "ENG-12"},
        **({"author": {"id": actor["id"], "kind": "human", "display_name": actor["name"]}} if "id" in actor else {}),
        "admission": {
            "purpose": "observe",
            "observe": {"provenance": {"provider": "linear", "issuer": "", "installation_id": ""}},
        },
        "occurred_at": "2026-10-31T08:00:00Z",
        "record": {
            "id": record_id(event, entity),
            "record_type": "linear_issue_event",
            "schema": SCHEMA,
            "summary": "[ENG-12] Alice Example assigned you: Fix the login page",
            "content": (
                "**ENG-12: Fix the login page**\nhttps://linear.example.test/acme/issue/ENG-12\n"
                "Alice Example assigned the issue to you.\nMCP server: linear"
            ),
            "timestamp": "2026-10-31T08:00:00Z",
            "data": data,
        },
    }


def linear_config(**settings: Any) -> NerveConfig:
    return NerveConfig.from_dict({"sync": {"linear": {"enabled": True, **settings}}})


# ---------------------------------------------------------------------- #
#  Settings                                                               #
# ---------------------------------------------------------------------- #


class TestSettings:
    def test_the_source_is_off_hosted_and_takes_every_event_by_default(self):
        linear = NerveConfig().sync.linear

        assert linear == NerveConfig.from_dict({}).sync.linear
        assert (linear.enabled, linear.mode, linear.events) == (False, "hosted", list(LINEAR_EVENTS))
        assert (linear.inbox, linear.include_bots, linear.drop_own_actions) == (True, False, True)
        assert (linear.rules, linear.schedule, linear.batch_size, linear.condense) == ([], "*/15 * * * *", 50, False)

    def test_the_settings_are_read(self):
        linear = linear_config(
            mode="Hosted", events=["Commented", "assigned", "commented"], inbox="false", include_bots=True,
            drop_own_actions="no", schedule="*/5 * * * *", batch_size="20", condense=True,
            rules=[{"path": "/issue/team/key", "allow": [" ENG* ", "", None, "OPS"]}],
        ).sync.linear

        assert (linear.enabled, linear.mode, linear.events) == (True, "hosted", ["assigned", "commented"])
        assert (linear.inbox, linear.include_bots, linear.drop_own_actions) == (False, True, False)
        assert (linear.schedule, linear.batch_size, linear.condense) == ("*/5 * * * *", 20, True)
        assert linear.rules == [SourceRuleConfig(path="/issue/team/key", allow=["ENG*", "OPS"])]

    @pytest.mark.parametrize("blank", [None, "", "  "])
    def test_blank_events_and_mode_mean_the_defaults(self, blank):
        linear = linear_config(events=blank, mode=blank).sync.linear

        assert (linear.events, linear.mode) == (list(LINEAR_EVENTS), "hosted")

    def test_poll_is_refused_with_a_clear_message(self):
        with pytest.raises(ValueError, match="sync.linear.mode 'poll' is not available: Linear records "
                                             "come only from the hosted channel gateway"):
            NerveConfig.from_dict({"sync": {"linear": {"mode": "poll"}}})

    @pytest.mark.parametrize(("settings", "message"), [
        ({"mode": "webhook"}, r"sync.linear.mode must be one of \('hosted',\), got 'webhook'"),
        ({"events": ["assigned", "closed"]}, "sync.linear.events must be one or more of .*, got 'closed'"),
        ({"events": []}, "sync.linear.events must be one or more of .*, got none"),
        ({"rules": "deny bots"}, "sync.linear.rules must be a list of rules"),
        ({"rules": ["/actor/kind"]}, r"sync.linear.rules\[0\] must be a mapping"),
        ({"rules": [{"path": "/actor/kind", "deny": ["bot"], "alow": ["x"]}]},
         r"sync.linear.rules\[0\] has unknown keys \['alow'\]"),
        ({"rules": [{"path": "actor/kind", "deny": ["bot"]}]}, r"sync.linear.rules\[0\].path must be a JSON Pointer"),
        ({"rules": [{"path": "/a~2b", "deny": ["bot"]}]}, r"\.path must be a JSON Pointer"),
        ({"rules": [{"path": "/a/b/c/d/e/f/g/h/i", "deny": ["bot"]}]}, r"\.path must be a JSON Pointer"),
        ({"rules": [{"path": "/a\u0001", "deny": ["bot"]}]}, r"\.path must be a JSON Pointer"),
        ({"rules": [{"path": "/" + "p" * 256, "deny": ["bot"]}]}, r"\.path must be a JSON Pointer"),
        ({"rules": [{"path": "/actor/kind"}]}, r"sync.linear.rules\[0\] needs exactly one non-empty list"),
        ({"rules": [{"path": "/actor/kind", "deny": [" "]}]}, "needs exactly one non-empty list"),
        ({"rules": [{"path": "/actor/kind", "deny": ["bot"], "allow": ["user"]}]}, "needs exactly one"),
        ({"rules": [{"path": "/a", "deny": [f"p{n}" for n in range(65)]}]}, "at most 64 patterns, got 65"),
        ({"rules": [{"path": "/a", "deny": ["p" * 257]}]}, "at most 256 bytes and no control characters"),
        ({"rules": [{"path": "/a", "deny": ["bot\u0007"]}]}, "no control characters"),
        ({"rules": [{"path": "/a", "deny": ["x"]}] * 17}, "at most 16 rules, got 17"),
    ])
    def test_settings_the_gateway_would_refuse_refuse_to_load(self, settings, message):
        with pytest.raises(ValueError, match=message):
            NerveConfig.from_dict({"sync": {"linear": settings}})

    def test_the_rule_bounds_are_the_gateways(self):
        assert (SOURCE_RULES_MAX, SOURCE_RULE_PATTERNS_MAX, SOURCE_RULE_PATTERN_MAX_BYTES) == (
            MAX_SOURCE_RULES, MAX_SOURCE_RULE_PATTERNS, MAX_SUBSCRIPTION_PATTERN_BYTES,
        )

    def test_the_largest_rules_that_load_are_valid_for_the_gateway(self):
        rules = [
            {"path": "/a/b/c/d/e/f/g/" + "h" * 241, "deny": ["é" * 128] * SOURCE_RULE_PATTERNS_MAX},
            {"path": "/m~0n/a~1b/", "allow": ["*"]},
        ] * (SOURCE_RULES_MAX // 2)

        linear = linear_config(rules=rules).sync.linear

        linear_subscription(linear)  # builds
        Subscriptions(version="1", sources=(linear_subscription(linear),)).validate()

    def test_the_keys_are_known_and_turning_it_on_is_documented_as_a_restart(self):
        merged = {"sync": {"linear": {
            "enabled": True, "mode": "hosted", "events": ["assigned"], "inbox": False, "include_bots": True,
            "drop_own_actions": False, "rules": [], "schedule": "*/5 * * * *", "condense": False,
            "batch_size": 10,
        }}}

        assert validate_config_keys(merged) == []
        assert config_reload.restart_required(NerveConfig.from_dict({}), linear_config()) == []
        assert "Turning `sync.linear.enabled` **on**" in (
            Path(__file__).parent.parent / "docs" / "config.md"
        ).read_text(encoding="utf-8")

    def test_linear_is_a_hosted_source_only_while_it_is_on(self):
        assert hosted_sources(NerveConfig()) == []
        assert hosted_sources(linear_config()) == ["linear"]
        assert hosted_providers(linear_config(enabled=False)) == []
        both = linear_config()
        both.sync.github = hosted_github()
        assert hosted_sources(both) == ["github", "github_repos", "linear"]


# ---------------------------------------------------------------------- #
#  Subscription                                                           #
# ---------------------------------------------------------------------- #


class TestSubscription:
    def test_the_defaults_send_only_the_record_schema(self):
        source = linear_subscription(LinearSyncConfig())

        assert json.loads(dumps(source)) == {
            "provider": "linear", "filters": {}, "schemas": ["linear.issue_event/1"],
        }
        assert source.schemas == LINEAR_SCHEMAS

    def test_settings_that_differ_from_the_defaults_and_the_rules_are_sent(self):
        settings = linear_config(
            events=["mentioned", "assigned"], inbox=False, include_bots=True, drop_own_actions=False,
            rules=[{"path": "/actor/kind", "deny": ["integration"]}, {"path": "/issue/team/key", "allow": ["ENG*"]}],
        ).sync.linear

        assert json.loads(dumps(linear_subscription(settings))) == {
            "provider": "linear",
            "filters": {},
            "settings": {
                "events": ["assigned", "mentioned"], "inbox": False, "include_bots": True,
                "drop_own_actions": False,
            },
            "schemas": ["linear.issue_event/1"],
            "rules": [{"path": "/actor/kind", "deny": ["integration"]}, {"path": "/issue/team/key", "allow": ["ENG*"]}],
        }

    def test_every_event_named_is_the_default(self):
        assert linear_subscription(linear_config(events=list(reversed(LINEAR_EVENTS))).sync.linear).settings is None

    def test_the_frame_names_linear_only_while_it_is_on_and_hosted(self):
        config = linear_config()
        config.sync.github = hosted_github()

        body = subscriptions_for(config, hosted_sources(config))

        assert [source.provider for source in body.sources] == ["github", "linear"]
        body.validate()
        config.sync.linear.enabled = False
        assert [source.provider for source in subscriptions_for(config, ["github", "github_repos", "linear"]).sources] == [
            "github",
        ]
        # A runtime that started without Linear has no Linear consumer.
        assert LINEAR not in [source.provider for source in subscriptions_for(linear_config(), ["github", "github_repos"]).sources]

    def test_the_subscription_is_generic_and_valid(self):
        source = linear_subscription(linear_config(rules=[{"path": "/actor/kind", "deny": ["bot"]}]).sync.linear)

        Subscriptions(version="1", sources=(source,)).validate()
        assert source.rules == (SourceRule(path="/actor/kind", deny=("bot",)),)
        with pytest.raises(Rejected):
            Subscriptions(version="1", sources=(dataclasses.replace(source, schemas=()),)).validate()


# ---------------------------------------------------------------------- #
#  Consumer, buffer and drain                                             #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestConsumerAndDrain:
    async def test_a_record_is_buffered_with_its_schema_and_its_data(self, db):
        config = linear_config()
        consumer = HostedSourceConsumer(LINEAR, db, lambda: config)

        disposition = await consumer.deliver(decoded(linear_event()))

        assert disposition.outcome == "accepted"
        [(_, payload)] = await db.read_channel_observations("linear")
        assert payload["conversation_id"] == ISSUE_ID
        assert payload["connection_id"] == CONNECTION_ID
        assert payload["event_id"] == f"linear:linear:assigned:{HISTORY_ID}:{VERSION_MS}"
        record = payload["record"]
        assert (record["record_type"], record["schema"], record["metadata"]) == (
            "linear_issue_event", "linear.issue_event/1", [],
        )
        assert record["data"] == linear_data()

    async def test_a_github_buffer_row_is_unchanged(self):
        record = buffered_record(decoded(source_record_event()))["record"]

        assert "schema" not in record and "data" not in record

    async def test_the_drain_keeps_the_schema_and_every_member_of_the_data(self, db):
        data = linear_data(estimate={"points": 3}, labels=["bug"], archived=False)
        await db.insert_channel_observation("linear", f"linear:{ISSUE_ID}", buffered_record(decoded(
            linear_event(data=data),
        )))
        await db.insert_channel_observation("linear", f"linear:{ISSUE_ID}", {"record": {"id": ""}})
        await db.insert_channel_observation("github", "github:owner/repo", {"record": {}})

        source = HostedSource(db, LINEAR)
        result = await source.fetch(None, limit=10)

        (record,) = result.records
        assert (record.id, record.source, record.record_type) == (record_id(), "linear", "linear_issue_event")
        assert record.summary == "[ENG-12] Alice Example assigned you: Fix the login page"
        assert record.timestamp == "2026-10-31T08:00:00Z"
        assert record.metadata == {"schema": SCHEMA, "data": data} == record_metadata(SCHEMA, data)
        assert (result.next_cursor, result.has_more) == ("2", False)
        assert await source.has_undrained(None) is True
        assert await source.has_undrained(result.next_cursor) is False
        assert (source.source_name, source.push_fed) == ("linear", True)

    async def test_a_payload_without_data_has_no_metadata(self):
        record = record_from_buffer(
            {"record": {"id": "1", "record_type": "linear_issue_event", "summary": "s", "data": "text"}}, "linear",
        )

        assert record.metadata == {} and record.content == ""

    async def test_the_registry_drains_with_the_rules_as_guardrail(self, db):
        config = linear_config(rules=[{"path": "/actor/kind", "deny": ["bot", "integration"]}])

        (runner,) = [r for r in build_source_runners(config, db) if r.job_id == "source:linear"]

        assert type(runner.source) is HostedSource and runner.source.source_name == "linear"
        assert runner.inbox_filter.rules == [DataRule("/actor/kind", deny=("bot", "integration"))]
        assert (runner.batch_size, runner.schedule) == (50, "")

    async def test_a_switched_off_source_has_no_runner(self, db):
        assert [r for r in build_source_runners(linear_config(enabled=False), db) if r.job_id == "source:linear"] == []

    async def test_the_github_hosted_source_is_still_its_own(self, db):
        assert isinstance(HostedGitHubSource(db), HostedSource)
        with pytest.raises(ValueError, match="no hosted GitHub source for provider 'linear'"):
            HostedGitHubSource(db, "linear")


# ---------------------------------------------------------------------- #
#  End to end over real streams                                           #
# ---------------------------------------------------------------------- #


@dataclass
class Harness:
    config: NerveConfig
    db: Any
    gateway: FakeChannelGateway
    runtime: HostedChannelRuntime
    server: NerveServer

    async def stream(self) -> FakeStream:
        return await self.gateway.open_stream(self.server.stream_url)

    async def acknowledged(self, *inbox_ids: str) -> dict[str, str]:
        await self.gateway.wait_for(lambda: all(i in self.gateway.acknowledged for i in inbox_ids))
        return {i: self.gateway.acknowledged[i] for i in inbox_ids}

    async def subscriptions(self, count: int = 1) -> list[dict[str, Any]]:
        await self.gateway.wait_for(lambda: len(self.gateway.subscriptions) >= count)
        return self.gateway.subscriptions

    def runner(self):
        (runner,) = [r for r in build_source_runners(self.config, self.db) if r.job_id == "source:linear"]
        return runner


async def start(tmp_path, db, monkeypatch, *, config_getter: Callable[[], NerveConfig] | None = None,
                github: bool = False, **settings: Any) -> Harness:
    config = slack_hosted_config(tmp_path)
    config.slack = SlackConfig()
    if github:
        config.sync.github = hosted_github()
    config.sync.linear = LinearSyncConfig.from_dict({"enabled": True, **settings})
    monkeypatch.setattr(config_module, "_config", config)
    engine = MagicMock()
    engine.db = db
    engine.sessions = SessionManager(db)
    router = ChannelRouter(engine)
    engine.router = router
    gateway = FakeChannelGateway(jwks_file=config.channels.hosted.gateway_jwks_file)
    runtime = HostedChannelRuntime(
        config, router, config_getter or (lambda: config),
        stream_timing=FAST_STREAM, reader_settings=FAST_READER, db=db,
    )
    await runtime.start()
    monkeypatch.setattr(gateway_server, "_hosted_channels", runtime)
    server = NerveServer(gateway_server.create_app())
    await server.__aenter__()
    return Harness(config, db, gateway, runtime, server)


@pytest_asyncio.fixture
async def hosted(tmp_path, db, monkeypatch):
    harness = await start(tmp_path, db, monkeypatch, rules=[{"path": "/actor/kind", "deny": ["bot"]}])
    try:
        yield harness
    finally:
        await stop(harness)


@pytest.mark.asyncio
class TestStreams:
    async def test_a_stream_subscribes_to_linear_with_the_schema_and_the_rules(self, hosted):
        await hosted.stream()

        [subscriptions] = await hosted.subscriptions()

        assert set(hosted.runtime.sources) == {"linear"}
        assert subscriptions == {"version": "1", "sources": [{
            "provider": "linear",
            "filters": {},
            "schemas": ["linear.issue_event/1"],
            "rules": [{"path": "/actor/kind", "deny": ["bot"]}],
        }]}

    async def test_a_record_is_buffered_then_drained_into_the_inbox_with_its_data(self, hosted):
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(linear_event())

        await stream.nudge("observe")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        result = await hosted.runner().run()
        assert result.records_ingested == 1
        stored = await hosted.db.get_source_message("linear", record_id())
        assert stored["record_type"] == "linear_issue_event"
        assert stored["summary"] == "[ENG-12] Alice Example assigned you: Fix the login page"
        assert stored["metadata"] == {"schema": SCHEMA, "data": linear_data()}
        assert stored["metadata"]["data"]["server"] == {"id": "linear", "name": "Linear"}

    async def test_a_changed_event_replaces_the_earlier_copy(self, hosted):
        """A new version of an event keeps its record ID, so the inbox holds one
        record of it, the newest, and it is unread again."""
        stream = await hosted.stream()
        runner = hosted.runner()
        first = hosted.gateway.store(linear_event())
        await stream.nudge("observe")
        assert (await hosted.acknowledged(first))[first] == "accepted"
        assert (await runner.run()).records_ingested == 1
        before = await hosted.db.get_source_max_rowid("linear")

        changed = linear_data(issue={**linear_data()["issue"], "state": {"name": "In Progress", "type": "started"}})
        second = hosted.gateway.store(linear_event(version_ms=VERSION_MS + 60_000, data=changed))
        await stream.nudge("observe")

        assert (await hosted.acknowledged(second))[second] == "accepted"
        assert (await runner.run()).records_ingested == 1
        rows = await hosted.db.read_source_messages_by_rowid("linear", 0, limit=10)
        assert [row["id"] for row in rows] == [record_id()]
        assert rows[0]["metadata"]["data"]["issue"]["state"]["name"] == "In Progress"
        assert await hosted.db.get_source_max_rowid("linear") > before

    async def test_the_agent_reads_the_schema_and_every_member_of_the_data_as_json(self, hosted):
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(linear_event(data=own_data()))
        await stream.nudge("observe")
        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        await hosted.runner().run()
        rows = await hosted.db.read_source_messages_by_rowid("linear", 0, limit=10)

        text = _format_source_batch(rows, "linear")

        assert f"**Schema:** {SCHEMA}\n" in text
        data_line = next(line for line in text.splitlines() if line.startswith("**Data:** "))
        assert json.loads(data_line.removeprefix("**Data:** ")) == own_data()
        assert '"server":{"id":"linear","name":"Linear"}' in data_line
        assert "**Metadata:**" not in text

    async def test_the_guardrail_applies_the_rules_again(self, hosted):
        stream = await hosted.stream()
        bot = linear_event(event="state_changed", data=bot_data())
        person = linear_event()
        dropped, kept = hosted.gateway.store(bot), hosted.gateway.store(person)

        await stream.nudge("observe")

        assert await hosted.acknowledged(dropped, kept) == {dropped: "accepted", kept: "accepted"}
        result = await hosted.runner().run()
        assert (result.records_ingested, result.records_dropped) == (1, 1)
        assert await hosted.db.get_source_message("linear", record_id("state_changed")) is None

    async def test_a_record_the_contract_refuses_closes_the_stream(self, hosted):
        stream = await hosted.stream()
        await hosted.subscriptions()
        broken = linear_event()
        del broken["record"]["data"]
        hosted.gateway.store(broken)

        await stream.nudge("observe")

        await hosted.gateway.wait_for(lambda: stream.close_code is not None)
        assert stream.close_reason == "malformed_frame"
        assert await hosted.db.read_channel_observations("linear") == []

    async def test_switching_it_off_and_on_follows_a_reload(self, hosted):
        await hosted.stream()
        await hosted.subscriptions()

        hosted.config.sync.linear = dataclasses.replace(hosted.config.sync.linear, enabled=False)
        await hosted.runtime.republish_subscriptions()
        off = (await hosted.subscriptions(2))[-1]
        hosted.config.sync.linear = dataclasses.replace(hosted.config.sync.linear, enabled=True, include_bots=True)
        await hosted.runtime.republish_subscriptions()
        on = (await hosted.subscriptions(3))[-1]

        assert off == {"version": "1"}
        assert on["sources"][0]["settings"] == {"include_bots": True}


# ---------------------------------------------------------------------- #
#  The frame limit: a subscription with rules is never sent without them  #
# ---------------------------------------------------------------------- #

# The frame limit that these tests negotiate, the smallest that the contract
# permits.
FRAME_LIMIT = 16384


def frame_bytes(config: NerveConfig) -> int:
    """The bytes of the subscriptions frame that the Linear-only runtime sends for *config*."""
    return subscriptions_frame_bytes(subscriptions_for(config, ["linear"]))


def rules_of_frame(config: NerveConfig, target: int) -> list[SourceRuleConfig]:
    """Linear rules with which the subscriptions frame has exactly *target* bytes."""
    rules = [SourceRuleConfig(path="/issue/team/key", deny=["p"])]
    while True:
        config.sync.linear = dataclasses.replace(config.sync.linear, rules=rules)
        gap = target - frame_bytes(config)
        if gap == 0:
            return rules
        patterns = rules[-1].deny
        room = SOURCE_RULE_PATTERN_MAX_BYTES - len(patterns[-1])
        if gap < 0:
            patterns[-1] = patterns[-1][:gap]
        elif gap <= room:
            patterns[-1] += "p" * gap
        elif len(patterns) < SOURCE_RULE_PATTERNS_MAX:
            patterns[-1] += "p" * room
            patterns.append("p")
        else:
            rules.append(SourceRuleConfig(path="/issue/team/key", deny=["p"]))


@pytest.mark.asyncio
class TestFrameLimit:
    async def nerve_side(self, harness: Harness):
        stream = await harness.gateway.open_stream(harness.server.stream_url, negotiate=False)
        await stream.negotiate(frame_bytes=FRAME_LIMIT)
        await harness.subscriptions()
        [nerve_side] = harness.runtime.streams.streams
        return stream, nerve_side

    async def test_a_frame_at_the_limit_carries_the_rules(self, hosted):
        _, nerve_side = await self.nerve_side(hosted)
        rules = rules_of_frame(hosted.config, FRAME_LIMIT)

        text = nerve_side._subscriptions_frame("o-9")

        assert len(text.encode("utf-8")) == FRAME_LIMIT
        [source] = json.loads(text)["payload"]["subscriptions"]["sources"]
        assert source["rules"] == [{"path": rule.path, "deny": rule.deny} for rule in rules]

    async def test_one_byte_more_leaves_linear_out_and_says_what_to_shorten(self, hosted, caplog):
        _, nerve_side = await self.nerve_side(hosted)
        rules_of_frame(hosted.config, FRAME_LIMIT + 1)
        caplog.set_level(logging.WARNING, logger="nerve.channels.hosted.stream")

        text = nerve_side._subscriptions_frame("o-9")

        assert json.loads(text)["payload"]["subscriptions"] == {"version": "1"}
        [record] = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert "the subscriptions of linear are not sent" in record.getMessage()
        assert "sync.linear.rules are shorter" in record.getMessage()

    async def test_the_gateway_gets_no_linear_subscription_and_the_stream_stays_open(self, tmp_path, db, monkeypatch):
        config = slack_hosted_config(tmp_path)
        config.sync.linear = LinearSyncConfig(enabled=True)
        rules = rules_of_frame(config, FRAME_LIMIT + 1)
        harness = await start(tmp_path, db, monkeypatch, rules=[
            {"path": rule.path, "deny": rule.deny} for rule in rules
        ])
        try:
            stream, _ = await self.nerve_side(harness)

            assert harness.gateway.subscriptions == [{"version": "1"}]
            assert stream.close_code is None
        finally:
            await stop(harness)

    async def test_github_patterns_go_first_and_linear_keeps_its_rules(self, tmp_path, db, monkeypatch):
        harness = await start(tmp_path, db, monkeypatch, github=True, rules=[
            {"path": "/actor/kind", "deny": ["bot"]},
        ])
        try:
            harness.config.sync.github.allow_repos = [f"owner/{'r' * 200}-{n}" for n in range(100)]
            await self.nerve_side(harness)

            [subscriptions] = harness.gateway.subscriptions
            github, linear = subscriptions["sources"]
            assert github == {"provider": "github", "filters": {}, "pr_watch": True}
            assert linear["rules"] == [{"path": "/actor/kind", "deny": ["bot"]}]
        finally:
            await stop(harness)

    async def test_the_largest_rules_that_load_do_not_fit_and_doctor_says_so(self, tmp_path):
        rules = [{"path": "/issue/team/key", "deny": ["é" * 128] * SOURCE_RULE_PATTERNS_MAX}] * SOURCE_RULES_MAX
        config = doctor_config(tmp_path, rules=rules)

        report = doctor_report(config)

        assert frame_bytes(config) > MAX_FRAME_BYTES
        assert "[ERR] Linear: with sync.linear.rules the subscriptions frame has about" in report
        assert "the gateway stores no Linear records. Shorten sync.linear.rules" in report
        assert "[OK] Linear records" not in report


async def stop(harness: Harness) -> None:
    await harness.runtime.stop()
    await harness.gateway.close_all()
    await harness.server.__aexit__(None, None, None)


# ---------------------------------------------------------------------- #
#  Record fixtures                                                        #
# ---------------------------------------------------------------------- #


def schema_validator() -> jsonschema.Draft202012Validator:
    return jsonschema.Draft202012Validator(json.loads(SCHEMA_FILE.read_text(encoding="utf-8")))


class TestRecordFixtures:
    def test_the_copy_of_the_schema_has_its_exact_bytes(self):
        assert hashlib.sha256(SCHEMA_FILE.read_bytes()).hexdigest() == SCHEMA_SHA256

    @pytest.mark.parametrize("data", [
        linear_data(), comment_data(), bot_data(), system_data(), own_data(),
        linear_data(event="moved_in", changes=[{"field": "project", "to": "Web"}]),
        linear_data(event="priority_changed", changes=[{"field": "priority", "from": 3, "to": 1}]),
    ], ids=lambda data: f"{data['event']}-{data['actor']['kind']}")
    def test_every_fixture_is_data_that_the_gateway_can_send(self, data):
        schema_validator().validate(data)

    @pytest.mark.parametrize("name", sorted(json.loads(WEB_FIXTURES.read_text(encoding="utf-8"))))
    def test_every_fixture_of_the_sources_page_is_data_that_the_gateway_can_send(self, name):
        schema_validator().validate(json.loads(WEB_FIXTURES.read_text(encoding="utf-8"))[name])

    @pytest.mark.parametrize("change", [
        {"server": {"id": "3c9a7e15-2b4d-4f60-8e1a-5d7c9b2f4a68-and-more", "name": "Linear"}},
        {"changes": [{"field": "assignee", "from": None, "to": "Example Agent"}]},
        {"actor": {"kind": "robot"}},
    ])
    def test_the_schema_refuses_what_the_gateway_never_sends(self, change):
        with pytest.raises(jsonschema.ValidationError):
            schema_validator().validate(linear_data(**change))

    def test_the_ids_follow_the_gateways_rules(self):
        event = linear_event()

        assert event["record"]["id"] == f"linear:assigned:{HISTORY_ID}"
        assert event["event_id"] == f"linear:linear:assigned:{HISTORY_ID}:{VERSION_MS}"
        assert event["connection_id"] == CONNECTION_ID
        assert uuid.UUID(CONNECTION_ID).version == 5
        assert event["record"]["data"]["server"]["id"] == SERVER
        assert "author" not in linear_event(event="state_changed", data=system_data())


# ---------------------------------------------------------------------- #
#  Doctor                                                                 #
# ---------------------------------------------------------------------- #


def doctor_config(tmp_path: Path, **linear: Any) -> NerveConfig:
    gateway = FakeChannelGateway(jwks_file=tmp_path / "gateway-jwks.json")
    return NerveConfig.from_dict({
        "sync": {"linear": {"enabled": True, **linear}},
        "channels": {"hosted": {**HOSTED, "gateway_jwks_file": str(gateway.jwks_file)}},
    })


class TestDoctor:
    def test_an_enabled_source_is_reported_with_its_settings(self, tmp_path):
        report = doctor_report(doctor_config(tmp_path, rules=[{"path": "/actor/kind", "deny": ["bot"]}]))

        assert (
            "[OK] Linear records come from the channel gateway (the agent's Linear connector): "
            "events all; inbox on; bots left out; own actions dropped; 1 rule\n"
        ) in report

    def test_narrowed_events_and_changed_switches_are_reported(self, tmp_path):
        report = doctor_report(doctor_config(
            tmp_path, events=["commented", "assigned"], inbox=False, include_bots=True, drop_own_actions=False,
        ))

        assert "events assigned, commented; inbox off; bots included; own actions kept; 0 rules\n" in report

    def test_a_switched_off_source_says_so(self, tmp_path):
        assert "[--] Linear source off (sync.linear.enabled is false)" in doctor_report(NerveConfig())

    def test_unusable_hosted_settings_are_errors(self, tmp_path):
        config = doctor_config(tmp_path)
        config.channels.hosted.gateway_jwks_file = tmp_path / "missing.json"

        report = doctor_report(config)

        assert "[ERR] Linear is on but channels.hosted.gateway_jwks_file: cannot read" in report
        assert "[OK] Linear records" not in report

    def test_a_switched_off_hosted_slack_is_said_to_hold_linear_back(self, tmp_path):
        config = doctor_config(tmp_path)
        config.slack = SlackConfig.from_dict({"mode": "hosted", "enabled": False})

        report = doctor_report(config)

        assert "so the Linear records wait there too" in report


def test_a_config_file_with_the_example_section_loads(tmp_path):
    """The commented ``sync.linear`` example of config.example.yaml loads as written."""
    text = (Path(__file__).parent.parent / "config.example.yaml").read_text(encoding="utf-8")
    block = text.split("  # linear:\n", 1)[1].split("\n\n", 1)[0]
    section = yaml.safe_load("linear:\n" + "\n".join(line.replace("  #", "", 1) for line in block.splitlines()))

    linear = LinearSyncConfig.from_dict(section["linear"])

    assert linear.rules == [SourceRuleConfig(path="/issue/team/key", allow=["ENG*"])]
    assert linear.events == ["assigned", "commented", "mentioned"]
