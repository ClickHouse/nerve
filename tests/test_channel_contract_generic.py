"""The generic sources of the channel contract, against the contract's vectors.

A provider outside the closed vocabularies (github, github_repos, email) is a
generic source: its records carry a ``schema`` and ``data``, a JSON object
that the provider defines, and a subscription to it carries ``settings``,
``schemas`` and ``rules``. ``fixtures/channel_contract`` holds byte-exact
copies of the contract's synthetic vectors (testdata/source_json.json,
source_records.json, source_subscriptions.json, event_digest_generic.json,
events/example_note.json, frames/nerve/subscriptions_generic.json, and the
test-only schema sources/example/example_note.v1.schema.json). The rule
vectors (source_rules.json) run against the inbox guardrail in
test_source_filters.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import struct
import uuid
from pathlib import Path
from typing import Any

import jsonschema
import pytest

from nerve.channels.hosted.contract import (
    Envelope,
    Payload,
    Rejected,
    RepoWatch,
    SourceFilters,
    SourceRule,
    SourceSubscription,
    Subscriptions,
    decode_envelope,
    encode_envelope,
)
from nerve.channels.hosted.contract.model import (
    CLOSED_PROVIDERS,
    CONVERSATION_KIND_ITEM,
    MAX_SOURCE_DATA_BYTES,
    MAX_SOURCE_SETTINGS_BYTES,
    schema_record_type,
)
from nerve.channels.hosted.contract.wire import decode_record, dumps, parse_json
from nerve.sources.source_json import canonical_json

FIXTURES = Path(__file__).parent / "fixtures" / "channel_contract"

# The SHA-256 of the contract's vectors, which the copies must match byte for
# byte (the contract's testdata, under the names above).
VECTOR_SHA256 = {
    "source_json.json": "43651bdc7631b88573030d048e7ae9b94f50eb60cacf4d67c19e1a5d5c3f8c2f",
    "source_records.json": "67029d8b3301200215176cd1b0b7fc59f46e62cc7b0be5a6e8c39fe8441d349c",
    "source_rules.json": "e49acca284612c0d270b896d6aa5f5a8c452082a7ca31c536e6b0e4bf414341d",
    "source_subscriptions.json": "c68335d8634f3a4a801242dadc8b63a17a3d1a60603f66a158154554068f8802",
    "event_digest_generic.json": "9caaa75dc67b90068c921cf4aa3d6e3284f2d248af73042eddf17d06ae0a4ef4",
    "example_note.json": "44b4b47e907276316740bd4d20ad502140c6bf144252bb0ddac1349189f34f77",
    "example_note.v1.schema.json": "3b3b93c1561607e2d7017036db3bc18e257e1e8338dddfba2923834a6e1f5b90",
    "subscriptions_generic.json": "7188e0efd42a679c0710c4ee28d55e8d4bac238146883b9b4096ecf524fe8804",
}

def vectors(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def reason(check) -> str:
    """The rejection reason of *check*, or ``""`` when it passes."""
    try:
        check()
    except Rejected as error:
        return error.reason
    return ""


def page_text(event_text: str) -> str:
    """An inbox page with one event, written as text so that the event keeps its bytes."""
    return (
        '{"version":"1","kind":"inbox_read_result","correlation_id":"n-1",'
        '"payload":{"inbox_read_result":{"outcome":"succeeded","events":[' + event_text + "]}}}"
    )


def record_event(provider: str, record: Any) -> dict[str, Any]:
    """A delivered source record event of *provider* that carries *record*."""
    return {
        "event_id": f"{provider}:3c9a7e15-2b4d-4f60-8e1a-5d7c9b2f4a68:created:1",
        "connection_id": "3c9a7e15-2b4d-4f60-8e1a-5d7c9b2f4a68",
        "provider": provider,
        "kind": "source_record",
        "conversation": {"id": "5f0c1b9e-3c2a-4d7e-9b1f-8a6d2e4c7b30", "kind": "item"},
        "admission": {"purpose": "observe"},
        "delivery": {"inbox_id": "1020"},
        "occurred_at": "2026-11-10T08:00:00Z",
        "record": record,
    }


def data_page(data_text: str) -> str:
    """A page whose one generic record has *data_text* as its data, byte for byte."""
    event = record_event("example", {
        "id": "note:1", "record_type": "example_note", "schema": "example.note/1",
        "summary": "[NOTE-7] New note: Plan", "timestamp": "2026-11-10T08:00:00Z", "data": {},
    })
    text = json.dumps(event)
    return page_text(text.replace('"data": {}', '"data": ' + data_text))


def settings_check(settings_text: str):
    """A check of *settings_text* as the settings of a generic subscription that Nerve sends."""
    def check() -> None:
        source = decode_record(SourceSubscription, parse_json(
            '{"provider":"example","schemas":["example.note/1"],"settings":' + settings_text + "}",
        ))
        Subscriptions(version="1", sources=(source,)).validate()
    return check


class TestVectorCopies:
    @pytest.mark.parametrize("name", sorted(VECTOR_SHA256))
    def test_the_copy_of_the_contract_vector_has_its_exact_bytes(self, name):
        assert hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest() == VECTOR_SHA256[name]


class TestSourceJSON:
    @pytest.mark.parametrize("case", vectors("source_json.json")["canonical"], ids=lambda case: case["name"])
    def test_the_canonical_form_is_the_contracts(self, case):
        assert canonical_json(parse_json(case["json"])) == case["canonical"]

    @pytest.mark.parametrize("case", vectors("source_json.json")["rejected"], ids=lambda case: case["name"])
    def test_a_value_that_breaks_a_rule_is_refused_as_data_and_as_settings(self, case):
        assert reason(lambda: decode_envelope(data_page(case["json"]))) == case["reason"]
        assert reason(settings_check(case["json"])) == case["reason"]

    @pytest.mark.parametrize("case", vectors("source_json.json")["bounds"], ids=lambda case: case["name"])
    def test_the_bounds_count_the_canonical_bytes(self, case):
        text = case["prefix"] + case["repeat"] * case["count"] + case["suffix"]
        check = (
            (lambda: decode_envelope(data_page(text))) if case["object"] == "data"
            else settings_check(text)
        )

        assert reason(check) == case["reason"]

    def test_the_bounds_are_the_contracts(self):
        assert (MAX_SOURCE_DATA_BYTES, MAX_SOURCE_SETTINGS_BYTES) == (32768, 8192)


class TestParser:
    """The frame parser refuses what the gateway's parser refuses."""

    def test_a_repeated_member_name_anywhere_in_a_frame_is_malformed(self):
        envelope = '{"version":"1","kind":"heartbeat","payload":{"heartbeat":{"sequence":1,"sequence":2}}}'

        assert reason(lambda: decode_envelope(envelope)) == "malformed_frame"

    @pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
    def test_non_finite_literals_are_malformed(self, literal):
        envelope = '{"version":"1","kind":"heartbeat","payload":{"heartbeat":{"sequence":' + literal + "}}}"

        assert reason(lambda: decode_envelope(envelope)) == "malformed_frame"


class TestDigestVector:
    """Nerve computes no event digest, but the data enters it in the canonical form of
    :func:`canonical_json`: the generic digest vector ends with the record's schema and
    that form, each as a 4-byte big-endian length and the UTF-8 bytes."""

    def test_the_projection_ends_with_the_schema_and_the_canonical_data(self):
        vector = vectors("event_digest_generic.json")
        event = vectors("example_note.json")
        projection = base64.b64decode(vector["normalized_base64"])
        # The fixture's data is unsorted and writes escapes and 2.50.
        data = parse_json(json.dumps(event["record"]["data"]))

        tail = b"".join(
            struct.pack(">I", len(text.encode("utf-8"))) + text.encode("utf-8")
            for text in (event["record"]["schema"], canonical_json(data))
        )

        assert vector["event_fixture"] == "events/example_note.json"
        assert "sha256:" + hashlib.sha256(projection).hexdigest() == vector["digest"]
        assert projection.endswith(tail)
        assert '"score":2.5}' in canonical_json(data) and "été" in canonical_json(data)


def example_note_page() -> str:
    """The contract's generic record event, with its record exactly as written."""
    return page_text((FIXTURES / "example_note.json").read_text(encoding="utf-8"))


class TestGenericRecords:
    def test_the_contract_sample_decodes_with_its_schema_data_and_item(self):
        event = decode_envelope(example_note_page()).body.events[0]

        assert (event.provider, event.kind, event.admission.purpose) == ("example", "source_record", "observe")
        assert event.conversation.kind == CONVERSATION_KIND_ITEM == "item"
        assert (event.conversation.id, event.conversation.display_name) == (
            "5f0c1b9e-3c2a-4d7e-9b1f-8a6d2e4c7b30", "NOTE-7",
        )
        assert event.connection_id == uuid.UUID("3c9a7e15-2b4d-4f60-8e1a-5d7c9b2f4a68")
        record = event.record
        assert (record.record_type, record.schema, record.metadata) == ("example_note", "example.note/1", ())
        assert record.data["note"]["title"] == "Plan the release été"
        assert record.data["score"] == 2.5
        assert record.data["actor"] == {"kind": "user", "name": "Alice Example", "id": "0b7d4c2a-6e1f-4a3b-9c8d-2f5e7a1b3c49"}

    def test_members_of_the_data_that_nerve_does_not_know_are_kept(self):
        event = vectors("example_note.json")
        event["record"]["data"]["timestamp"] = "not a time: protocol names mean nothing here"
        event["record"]["data"]["note"]["tenant_id"] = {"nested": [1, {"deep": True}]}

        data = decode_envelope(page_text(json.dumps(event))).body.events[0].record.data

        assert data["timestamp"] == "not a time: protocol names mean nothing here"
        assert data["note"]["tenant_id"] == {"nested": [1, {"deep": True}]}

    @pytest.mark.parametrize("case", vectors("source_records.json")["records"], ids=lambda case: case["name"])
    def test_a_record_gets_the_contracts_verdict(self, case):
        page = page_text(json.dumps(record_event(case["provider"], case["record"])))

        assert reason(lambda: decode_envelope(page)) == case["reason"]

    @pytest.mark.parametrize("provider", sorted(CLOSED_PROVIDERS))
    def test_a_record_of_a_closed_provider_carries_no_schema_or_data(self, provider):
        record = {"id": "1", "record_type": f"{provider}_message", "summary": "s",
                  "timestamp": "2026-11-10T08:00:00Z", "data": {}}

        assert reason(lambda: decode_envelope(page_text(json.dumps(record_event(provider, record))))) == (
            "malformed_frame"
        )

    def test_the_test_schema_takes_the_sample_and_refuses_an_unknown_member(self):
        """The producer writes strictly to the schema file; Nerve reads tolerantly."""
        schema = vectors("example_note.v1.schema.json")
        data = vectors("example_note.json")["record"]["data"]
        validator = jsonschema.Draft202012Validator(schema)

        validator.validate(data)
        with pytest.raises(jsonschema.ValidationError):
            validator.validate({**data, "unreviewed": "value"})

    def test_the_schema_names_its_record_type(self):
        assert schema_record_type("linear", "linear.issue_event/1") == "linear_issue_event"
        assert reason(lambda: schema_record_type("linear", "linear.issue_event/0")) == "malformed_frame"
        assert reason(lambda: schema_record_type("github", "github.notification/1")) == "malformed_frame"


class TestGenericSubscriptions:
    @pytest.mark.parametrize(
        "case", vectors("source_subscriptions.json")["sources"], ids=lambda case: case["name"],
    )
    def test_a_subscription_gets_the_contracts_verdict(self, case):
        def check() -> None:
            source = decode_record(SourceSubscription, parse_json(json.dumps(case["source"])))
            Subscriptions(version="1", sources=(source,)).validate()

        assert reason(check) == case["reason"]

    def test_nerve_sends_the_contracts_generic_frame(self):
        sample = vectors("subscriptions_generic.json")
        body = Subscriptions(version="1", sources=(
            SourceSubscription(provider="github", filters=SourceFilters(deny_actors=("*-bot",))),
            SourceSubscription(
                provider="example",
                settings={"events": ["created", "commented"], "inbox": True},
                schemas=("example.note/1",),
                rules=(
                    SourceRule(path="/actor/kind", deny=("bot",)),
                    SourceRule(path="/note/key", allow=("NOTE-*",)),
                ),
            ),
        ))
        envelope = Envelope(version="1", kind="subscriptions", request_id="n-26", payload=Payload(subscriptions=body))

        body.validate()
        assert json.loads(encode_envelope(envelope)) == sample

    def test_empty_settings_are_sent_and_absent_ones_are_not(self):
        def sent(settings: dict[str, Any] | None) -> dict[str, Any]:
            return json.loads(dumps(SourceSubscription(
                provider="linear", settings=settings, schemas=("linear.issue_event/1",),
            )))

        assert sent(None) == {"provider": "linear", "filters": {}, "schemas": ["linear.issue_event/1"]}
        assert sent({})["settings"] == {}

    def test_without_filters_never_widens_a_generic_source(self):
        """A generic source keeps its rules; one that cannot be sent with them
        leaves the frame instead (see test_hosted_linear_source)."""
        generic = SourceSubscription(
            provider="example", settings={"inbox": False}, schemas=("example.note/1",),
            rules=(SourceRule(path="/actor/kind", deny=("bot",)),),
        )
        narrow = Subscriptions(version="1", sources=(
            SourceSubscription(provider="github", filters=SourceFilters(deny_actors=("*-bot",))),
            generic,
            SourceSubscription(provider="other", schemas=("other.note/1",)),
        ))

        wide = narrow.without_filters()

        assert wide.sources == (SourceSubscription(provider="github"), generic, narrow.sources[2])
        assert wide.ruled_providers() == ("example",)
        assert wide.without_providers(wide.ruled_providers()).sources == (
            SourceSubscription(provider="github"), narrow.sources[2],
        )
        wide.validate()

    def test_repositories_follow_the_providers_rules(self):
        """github_repos must name them; github, email and a generic source must
        not; another provider without schemas may, within the bounds."""
        def check(source: SourceSubscription) -> str:
            return reason(Subscriptions(version="1", sources=(source,)).validate)

        repo = RepoWatch(name="acme/widgets", types=("issue",))
        assert check(SourceSubscription(provider="gitlab", repos=(repo,))) == ""
        assert check(SourceSubscription(provider="gitlab", repos=(RepoWatch(name="a/b/c", types=("issue",)),))) == (
            "malformed_frame"
        )
        for provider in ("github", "email"):
            assert check(SourceSubscription(provider=provider, repos=(repo,))) == "malformed_frame"
        assert check(SourceSubscription(provider="gitlab", schemas=("gitlab.note/1",), repos=(repo,))) == (
            "malformed_frame"
        )

    def test_the_email_subscription_is_not_generic(self):
        Subscriptions(version="1", sources=(SourceSubscription(provider="email"),)).validate()
        with pytest.raises(Rejected) as error:
            Subscriptions(version="1", sources=(
                SourceSubscription(provider="email", schemas=("email.message/1",)),
            )).validate()
        assert error.value.reason == "malformed_frame"

    def test_settings_that_nerve_builds_are_checked_like_received_data(self):
        deep: dict[str, Any] = {}
        node = deep
        for _ in range(8):
            node["a"] = {}
            node = node["a"]
        for settings, expected in [
            ({"events": ["assigned"], "inbox": False}, ""),
            ({"score": float("nan")}, "malformed_frame"),
            ({"big": 10 ** 400}, "malformed_frame"),
            ({"text": "\ud800"}, "malformed_frame"),
            (deep, "limit_exceeded"),
            ({"a": "x" * MAX_SOURCE_SETTINGS_BYTES}, "limit_exceeded"),
        ]:
            subscriptions = Subscriptions(version="1", sources=(SourceSubscription(
                provider="linear", settings=copy.deepcopy(settings), schemas=("linear.issue_event/1",),
            ),))
            assert reason(subscriptions.validate) == expected, settings
