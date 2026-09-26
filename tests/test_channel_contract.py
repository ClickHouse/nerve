"""The checks that Nerve applies to channel contract frames.

The gateway checks the contract itself. These tests cover only what Nerve's
own code relies on: the frame kinds that it handles, the members that it
reads, the values that it branches on, and its size bounds.
"""

from __future__ import annotations

import copy
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from nerve.channels.hosted.contract import (
    Drain,
    Envelope,
    Heartbeat,
    InboxAck,
    InboxAckItem,
    InboxRead,
    Negotiation,
    Payload,
    Rejected,
    decode_envelope,
    encode_envelope,
    validate_inbox_read_result,
)
from nerve.channels.hosted.runtime import RECEIVE_LIMITS
from tests.fake_channel_gateway import CONNECTION_ID, capabilities, gateway_negotiation, message_event


def frame(kind: str, body: dict[str, Any], **envelope: Any) -> dict[str, Any]:
    return {"version": "1", "kind": kind, "request_id": "g-1", **envelope, "payload": {kind: body}}


def page(*events: dict[str, Any]) -> dict[str, Any]:
    stored = []
    for index, event in enumerate(events):
        event = copy.deepcopy(event)
        event["delivery"] = {"inbox_id": str(1000 + index), "received_at": "2026-09-16T10:00:00.350Z"}
        stored.append(event)
    body = {"outcome": "succeeded", "events": stored}
    return {"version": "1", "kind": "inbox_read_result", "correlation_id": "n-1",
            "payload": {"inbox_read_result": body}}


def decode(value: dict[str, Any] | str, limit: int = 262144) -> Envelope:
    return decode_envelope(value if isinstance(value, str) else json.dumps(value), limit)


def reason(value: dict[str, Any] | str, limit: int = 262144) -> str:
    with pytest.raises(Rejected) as error:
        decode(value, limit)
    return error.value.reason


class TestDecoding:
    def test_a_page_decodes_into_the_members_that_nerve_reads(self):
        event = decode(page(message_event(thread="1.0"))).body.events[0]

        assert event.delivery.inbox_id == "1000"
        assert event.admission.purpose == "invoke"
        assert event.connection_id == uuid.UUID(CONNECTION_ID)
        assert event.thread.id == "1.0"
        assert event.content[0].text.body == "hello"
        assert event.occurred_at == datetime(2026, 9, 16, 10, 0, 0, 100000, tzinfo=timezone.utc)

    def test_unknown_members_are_ignored(self):
        value = page(message_event())
        value["trace"] = {"traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01"}
        value["payload"]["inbox_read_result"]["events"][0]["new_member"] = {"any": ["shape"]}

        assert decode(value).body.events[0].kind == "message"

    def test_a_member_of_another_type_is_malformed(self):
        value = page(message_event())
        value["payload"]["inbox_read_result"]["events"][0]["connection_id"] = 7

        assert reason(value) == "malformed_frame"

    def test_invalid_json_is_malformed(self):
        assert reason('{"version": "1",') == "malformed_frame"

    def test_a_timestamp_with_an_offset_and_nanoseconds_decodes(self):
        value = frame("heartbeat", {"sequence": 1, "sent_at": "2026-09-16T12:00:00.123456789+02:00"})

        sent_at = decode(value).body.sent_at
        assert sent_at == datetime(2026, 9, 16, 10, 0, 0, 123456, tzinfo=timezone.utc)

    def test_a_frame_above_the_receive_limit_is_refused_before_parsing(self):
        assert reason("x" * 16385, 16384) == "frame_too_large"


class TestFrameChecks:
    def test_another_protocol_version_is_refused(self):
        assert reason({**frame("heartbeat", {"sequence": 1}), "version": "2"}) == "version_unsupported"

    @pytest.mark.parametrize("kind", ["inbox_read", "operation", "platform_control", "unknown"])
    def test_a_kind_that_nerve_does_not_take_from_the_gateway_is_refused(self, kind):
        assert reason(frame(kind, {})) == "kind_unsupported"

    def test_a_frame_without_its_payload_member_is_malformed(self):
        value = frame("heartbeat", {"sequence": 1})
        value["payload"] = {"drain": {"reason": "rollout"}}

        assert reason(value) == "malformed_frame"

    def test_capabilities_name_their_connection(self):
        assert reason(frame("capabilities", capabilities())) == "scope_mismatch"
        body = decode(frame("capabilities", capabilities(), connection_id=CONNECTION_ID)).body
        assert body.self.id == "U_FIXTURE_AGENT"

    @pytest.mark.parametrize("change", [{"supported_versions": ["2"]}, {"delivery_modes": ["push"]}])
    def test_a_negotiation_needs_version_1_and_pull(self, change):
        assert reason(frame("negotiation", {**gateway_negotiation(), **change})) == "version_unsupported"

    @pytest.mark.parametrize("limits", [{"frame_bytes": 16383}, {"in_flight_requests": 0}])
    def test_a_negotiation_needs_usable_limits(self, limits):
        assert reason(frame("negotiation", gateway_negotiation(**limits))) == "limit_exceeded"


class TestInboxPages:
    @pytest.mark.parametrize("member", ["event_id", "delivery", "admission", "message", "author"])
    def test_an_event_needs_the_members_that_nerve_reads(self, member):
        value = page(message_event())
        del value["payload"]["inbox_read_result"]["events"][0][member]

        assert reason(value) == "malformed_frame"

    def test_an_invoke_needs_its_invoke_member(self):
        value = page(message_event())
        del value["payload"]["inbox_read_result"]["events"][0]["admission"]["invoke"]

        assert reason(value) == "malformed_frame"

    def test_an_unknown_admission_purpose_is_refused(self):
        value = page(message_event())
        value["payload"]["inbox_read_result"]["events"][0]["admission"]["purpose"] = "inspect"

        assert reason(value) == "kind_unsupported"

    def test_an_unknown_inbox_outcome_is_refused(self):
        value = page()
        value["payload"]["inbox_read_result"]["outcome"] = "partial"

        assert reason(value) == "kind_unsupported"

    def test_a_page_larger_than_the_read_is_refused(self):
        result = decode(page(message_event(message_id="1"), message_event(message_id="2"))).body

        with pytest.raises(Rejected) as error:
            validate_inbox_read_result(InboxRead(maximum_events=1, maximum_bytes=16384), result, 1000)
        assert error.value.reason == "limit_exceeded"
        with pytest.raises(Rejected):
            validate_inbox_read_result(InboxRead(maximum_events=2, maximum_bytes=16384), result, 16385)

    def test_one_event_may_exceed_the_read_byte_limit(self):
        result = decode(page(message_event())).body

        validate_inbox_read_result(InboxRead(maximum_events=1, maximum_bytes=16384), result, 20000)


TIME = datetime(2026, 9, 16, 10, 0, 0, 250000, tzinfo=timezone.utc)

# The payloads that Nerve sends, member for member. The gateway refuses a
# frame with an unknown member or without a required one.
SENT: list[tuple[str, Any, dict[str, Any]]] = [
    ("negotiation", Negotiation(
        preferred_version="1", supported_versions=("1",), delivery_modes=("pull",), receive_limits=RECEIVE_LIMITS,
    ), {
        "preferred_version": "1", "supported_versions": ["1"], "delivery_modes": ["pull"],
        "receive_limits": {
            "frame_bytes": 262144, "transfer_bytes": 16777216, "memory_bytes": 33554432, "in_flight_requests": 64,
        },
    }),
    ("heartbeat", Heartbeat(sequence=3, sent_at=TIME), {"sequence": 3, "sent_at": "2026-09-16T10:00:00.25Z"}),
    ("drain", Drain(reason="shutdown", initiated_at=TIME, deadline=TIME.replace(second=10)), {
        "reason": "shutdown", "initiated_at": "2026-09-16T10:00:00.25Z", "deadline": "2026-09-16T10:00:10.25Z",
    }),
    ("inbox_read", InboxRead(maximum_events=25, maximum_bytes=131072), {"maximum_events": 25, "maximum_bytes": 131072}),
    ("inbox_ack", InboxAck(items=(
        InboxAckItem(inbox_id="1007", outcome="accepted"),
        InboxAckItem(inbox_id="1008", outcome="rejected", reason_code="admission_rejected"),
    )), {"items": [
        {"inbox_id": "1007", "outcome": "accepted"},
        {"inbox_id": "1008", "outcome": "rejected", "reason_code": "admission_rejected"},
    ]}),
]


class TestEncoding:
    @pytest.mark.parametrize(("kind", "body", "expected"), SENT, ids=[kind for kind, _, _ in SENT])
    def test_nerve_sends_exactly_the_contract_members(self, kind, body, expected):
        envelope = Envelope(version="1", kind=kind, request_id="n-1", payload=Payload(**{kind: body}))

        value = json.loads(encode_envelope(envelope))

        assert value == {"version": "1", "kind": kind, "request_id": "n-1", "payload": {kind: expected}}

    def test_nerve_frames_are_compact_and_omit_empty_members(self):
        envelope = Envelope(
            version="1", kind="inbox_ack", request_id="n-1",
            payload=Payload(inbox_ack=InboxAck(items=(InboxAckItem(inbox_id="1000", outcome="accepted"),))),
        )

        assert encode_envelope(envelope) == (
            '{"version":"1","kind":"inbox_ack","request_id":"n-1",'
            '"payload":{"inbox_ack":{"items":[{"inbox_id":"1000","outcome":"accepted"}]}}}'
        )

    def test_times_are_sent_in_utc(self):
        sent_at = datetime(2026, 9, 16, 14, 0, 0, 120000, tzinfo=timezone(timedelta(hours=2)))
        envelope = Envelope(
            version="1", kind="heartbeat", request_id="o-1",
            payload=Payload(heartbeat=Heartbeat(sequence=1, sent_at=sent_at)),
        )

        assert '"sent_at":"2026-09-16T12:00:00.12Z"' in encode_envelope(envelope)

    def test_a_frame_larger_than_the_gateway_limit_is_not_sent(self):
        items = tuple(InboxAckItem(inbox_id=str(n), outcome="accepted") for n in range(1000))
        envelope = Envelope(version="1", kind="inbox_ack", request_id="n-1", payload=Payload(inbox_ack=InboxAck(items)))

        with pytest.raises(Rejected) as error:
            encode_envelope(envelope, 16384)
        assert error.value.reason == "frame_too_large"
