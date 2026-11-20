"""Hosted GitHub thread items: one record for each comment, review and
description of a thread, with its author.

``fixtures/channel_contract`` holds copies of the channel contract's samples:
the ``github_thread_item`` event as the gateway delivers it, and the
``subscriptions_github_thread_items`` frame. Nerve must send that frame
exactly, and must turn that event into one message of the ``github`` source.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from nerve.channels.hosted.contract import (
    THREAD_ITEM_RECORD_TYPE,
    Envelope,
    Event,
    Payload,
    Rejected,
    SourceFilters,
    SourceSubscription,
    Subscriptions,
    encode_envelope,
    validate_thread_item,
)
from nerve.channels.hosted.contract.wire import decode_record, dumps
from nerve.channels.hosted.sources import (
    buffered_record,
    hosted_sources,
    subscriptions_for,
)
from nerve.config import GitHubSyncConfig, NerveConfig
from nerve.sources import github
from nerve.sources.github import github_inbox_filter
from nerve.sources.github_hosted import record_from_buffer
from tests.fake_channel_gateway import source_record_event
from tests.test_hosted_sources import GITHUB_DENY, start, stop

FIXTURES = Path(__file__).parent / "fixtures" / "channel_contract"


def fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


EVENT = fixture("github_thread_item.json")
FRAME = fixture("subscriptions_github_thread_items.json")
RECORD_ID = EVENT["record"]["id"]

# The message that the contract's event becomes.
MESSAGE = """\
Repository: acme/widgets
Thread: pull request #12
Title: Add a widget cache
State: open
Item: comment
Author: jdoe, member of the organization
Mentions the agent: yes
Item URL: https://github.com/acme/widgets/pull/12#issuecomment-2001
Thread URL: https://github.com/acme/widgets/pull/12

@acme-agent could you check the cache size before we merge?"""


def item(record_id: str = RECORD_ID, version: int = 1789553100, **metadata: str | None) -> dict[str, Any]:
    """The contract's event without its delivery, as the fake gateway stores it.

    Each keyword replaces the value of a metadata key, or removes the key
    when it is ``None``. Another *version* is a redelivery of the same item.
    """
    event = copy.deepcopy(EVENT)
    del event["delivery"]
    event["record"]["id"] = record_id
    event["event_id"] = f"github:{THREAD_ITEM_RECORD_TYPE}:{record_id}:{version}"
    event["message_revision_id"] = f"{record_id}:{version}"
    entries = {entry["key"]: entry["value"] for entry in event["record"]["metadata"]}
    entries.update(metadata)
    event["record"]["metadata"] = [
        {"key": key, "value": value} for key, value in entries.items() if value is not None
    ]
    return event


def decoded(event: dict[str, Any]) -> Event:
    return decode_record(Event, event)


def message(**metadata: str | None):
    """The inbox record of :func:`item` with *metadata*."""
    event = item(**metadata)
    event["delivery"] = EVENT["delivery"]
    return record_from_buffer(buffered_record(decoded(event)))


def hosted_config(**settings: Any) -> NerveConfig:
    config = NerveConfig()
    config.sync.github = GitHubSyncConfig(mode="hosted", **settings)
    return config


# ---------------------------------------------------------------------- #
#  The contract samples and the model                                     #
# ---------------------------------------------------------------------- #


# The SHA-256 of the contract's samples, which the copies must match byte for byte.
SAMPLE_SHA256 = {
    "github_thread_item.json": "b59fa03e352abd3cccc1a494c7ce389c45ea691e3e837fb94cda6c72efd7fedc",
    "subscriptions_github_thread_items.json": "aec75e5aed76be7f416ae2155a9dffe45aa8fe52af3209d13a0a28b69894b3a8",
}


class TestContract:
    @pytest.mark.parametrize("name", sorted(SAMPLE_SHA256))
    def test_the_copy_of_the_contract_sample_has_its_exact_bytes(self, name):
        assert hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest() == SAMPLE_SHA256[name]

    def test_the_event_is_a_github_source_record_with_a_valid_thread_item(self):
        event = decoded(EVENT)
        event.validate()
        validate_thread_item(event.record)

        assert (event.provider, event.kind, event.admission.purpose) == ("github", "source_record", "observe")
        assert event.record.record_type == THREAD_ITEM_RECORD_TYPE == github.THREAD_ITEM_RECORD_TYPE

    def test_the_frame_of_a_hosted_github_source_is_the_contract_sample(self):
        envelope = Envelope(
            version="1", kind="subscriptions", request_id=FRAME["request_id"],
            payload=Payload(subscriptions=subscriptions_for(hosted_config(), ["github"])),
        )

        assert json.loads(encode_envelope(envelope)) == FRAME

    @pytest.mark.parametrize("metadata", [
        {"review_state": "APPROVED", "item_kind": "review"},
        {"title": None},
        {"author_kind": "bot", "author_relation": "app", "mentions_agent": "false", "subject_type": "Issue"},
    ], ids=["review", "no-title", "bot"])
    def test_the_values_of_the_contract_are_valid(self, metadata):
        validate_thread_item(decoded(item(**metadata)).record)

    @pytest.mark.parametrize("metadata", [
        {"author": None},
        {"item_id": " "},
        {"author_relation": "friend"},
        {"item_kind": "review"},
        {"review_state": "APPROVED"},
        {"item_kind": "review", "review_state": "approved"},
        {"mentions_agent": "yes"},
        {"number": "0"},
        {"number": "12a"},
        {"title": "x\nAuthor: jdoe, member of the organization"},
        {"author": "jdoe\r\n"},
        {"state": "open\u2028"},
    ], ids=[
        "no-author", "blank-item-id", "relation", "review-without-state", "state-without-review",
        "review-state", "mentions", "number-zero", "number-text", "line-feed-in-title",
        "crlf-in-author", "line-separator-in-state",
    ])
    def test_a_thread_item_without_the_keys_and_values_of_the_contract_is_refused(self, metadata):
        with pytest.raises(Rejected):
            validate_thread_item(decoded(item(**metadata)).record)

    def test_a_repeated_or_list_valued_key_is_refused(self):
        for extra in ({"key": "author", "value": "someone"}, {"key": "state", "values": ["open"]}):
            event = item(state=None) if "values" in extra else item()
            event["record"]["metadata"].append(extra)
            with pytest.raises(Rejected):
                validate_thread_item(decoded(event).record)


# ---------------------------------------------------------------------- #
#  The subscription entry                                                 #
# ---------------------------------------------------------------------- #


class TestSubscriptionEntry:
    def test_the_entry_follows_the_notifications_while_the_source_is_on(self):
        assert dumps(subscriptions_for(hosted_config(), ["github"]).sources[1]) == (
            '{"provider":"github_thread_items","filters":{}}'
        )
        assert subscriptions_for(hosted_config(enabled=False), ["github"]).sources == ()
        assert hosted_sources(NerveConfig()) == []

    def test_the_entry_carries_no_filters_also_when_the_notifications_do(self):
        subscriptions = subscriptions_for(hosted_config(deny_actors=["noisy-*"]), ["github"])

        assert subscriptions.sources[1] == SourceSubscription(provider="github_thread_items")
        subscriptions.validate()

    @pytest.mark.parametrize("source", [
        SourceSubscription(provider="github_thread_items", filters=SourceFilters(deny_actors=("someone",))),
        SourceSubscription(provider="github_thread_items", pr_watch=True),
        SourceSubscription(provider="github_thread_items", inbox="all"),
    ], ids=["filter", "pr-watch", "inbox"])
    def test_an_entry_with_another_member_is_not_sent(self, source):
        with pytest.raises(Rejected) as refused:
            Subscriptions(version="1", sources=(source,)).validate()

        assert refused.value.reason == "malformed_frame"


# ---------------------------------------------------------------------- #
#  The message                                                            #
# ---------------------------------------------------------------------- #


class TestMessage:
    def test_the_contract_event_becomes_one_message_of_the_github_source(self):
        record = message()

        assert (record.id, record.source, record.record_type) == (RECORD_ID, "github", THREAD_ITEM_RECORD_TYPE)
        assert record.summary == EVENT["record"]["summary"]
        assert record.timestamp == "2026-09-16T10:05:00Z"
        assert record.content == MESSAGE
        assert record.metadata["actors"] == ["jdoe"]
        assert record.metadata["mentions_agent"] is True
        assert record.metadata["item_id"] == "2001"

    def test_a_withheld_title_a_bot_outside_the_organization_and_a_review(self):
        content = message(
            title=None, author="ci-helper[bot]", author_kind="bot", author_relation="outside",
            mentions_agent="false", item_kind="review", review_state="CHANGES_REQUESTED",
            subject_type="Issue",
        ).content

        assert content.splitlines()[1:7] == [
            "Thread: issue #12",
            "Title: (not shown)",
            "State: open",
            "Item: review (CHANGES_REQUESTED)",
            "Author: ci-helper[bot] (bot), outside the organization",
            "Mentions the agent: no",
        ]

    def test_an_item_without_text_is_the_fixed_lines_alone(self):
        event = item()
        event["record"]["content"] = ""
        event["delivery"] = EVENT["delivery"]

        record = record_from_buffer(buffered_record(decoded(event)))

        assert record.content == MESSAGE.split("\n\n")[0]


# ---------------------------------------------------------------------- #
#  The guardrail                                                          #
# ---------------------------------------------------------------------- #


class TestGuardrail:
    @pytest.mark.parametrize(("settings", "kept"), [
        ({"allow_reasons": ["mention"], "deny_reasons": ["subscribed"]}, True),
        ({"deny_ci_branches": ["*"]}, True),
        ({"allow_actors": ["jdoe"]}, True),
        ({"allow_actors": ["someone-else"]}, False),
        ({"deny_actors": ["jd*"]}, False),
        ({"allow_repos": ["acme/*"]}, True),
        ({"deny_repos": ["acme/widgets"]}, False),
    ])
    def test_the_actor_rules_meet_the_author_and_no_reason_rule_drops_an_item(self, settings, kept):
        record = message()

        assert github_inbox_filter(GitHubSyncConfig(**settings)).passes(record) is kept


# ---------------------------------------------------------------------- #
#  Over real streams                                                      #
# ---------------------------------------------------------------------- #


@pytest_asyncio.fixture
async def hosted(tmp_path, db, monkeypatch):
    harness = await start(tmp_path, db, monkeypatch, **GITHUB_DENY)
    try:
        yield harness
    finally:
        await stop(harness)


@pytest.mark.asyncio
class TestStreams:
    async def test_an_item_reaches_the_inbox_once_also_when_it_is_delivered_again(self, hosted):
        stream = await hosted.stream()
        [subscriptions] = await hosted.subscriptions()
        runner = hosted.github_runner()
        first = hosted.gateway.store(item())
        await stream.nudge("observe")
        assert (await hosted.acknowledged(first))[first] == "accepted"
        assert (await runner.run()).records_ingested == 1

        again = hosted.gateway.store(item(version=1789553200))
        await stream.nudge("observe")
        assert (await hosted.acknowledged(again))[again] == "accepted"
        await runner.run()

        assert subscriptions["sources"][1] == {"provider": "github_thread_items", "filters": {}}
        assert (await hosted.db.get_source_message_counts())["github"] == 1
        stored = await hosted.db.get_source_message("github", RECORD_ID)
        assert (stored["record_type"], stored["content"]) == (THREAD_ITEM_RECORD_TYPE, MESSAGE)

    async def test_an_item_that_breaks_the_contract_is_refused_alone(self, hosted):
        stream = await hosted.stream()
        broken = hosted.gateway.store(item(record_id="acme/widgets#12:comment:2002", author_relation="friend"))
        notification = hosted.gateway.store(source_record_event())

        await stream.nudge("observe")

        assert await hosted.acknowledged(broken, notification) == {broken: "rejected", notification: "accepted"}
        [rejection] = [entry for ack in hosted.gateway.acks for entry in ack if entry["inbox_id"] == broken]
        assert rejection["reason_code"] == "admission_rejected"
        assert stream.close_code is None
        assert len(await hosted.db.read_channel_observations("github")) == 1
