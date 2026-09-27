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
    Operation,
    OperationResult,
    Payload,
    Rejected,
    decode_envelope,
    encode_envelope,
    validate_inbox_read_result,
    validate_operation_result,
)
from nerve.channels.hosted.contract.model import (
    ActionElement,
    ActionsContent,
    AttachmentReference,
    AttachmentTarget,
    ContentPart,
    ContentReference,
    ConversationReference,
    DeleteOperation,
    EditOperation,
    FileReadOperation,
    FileSendOperation,
    FileUpload,
    InteractionOperation,
    InteractionTarget,
    MessageContainer,
    MessageReference,
    MessageTarget,
    Reaction,
    ReactionOperation,
    SendOperation,
    TextContent,
    ThreadReference,
    TransferChunk,
    TypingOperation,
)
from nerve.channels.hosted.runtime import RECEIVE_LIMITS
from tests.fake_channel_gateway import CONNECTION_ID, capabilities, gateway_negotiation, message_event


def direct_message(**changes: Any) -> dict[str, Any]:
    return message_event(conversation="D0DIRECT", conversation_kind="direct", **changes)


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
        event = decode(page(direct_message(thread="1.0"))).body.events[0]

        assert event.delivery.inbox_id == "1000"
        assert event.admission.purpose == "invoke"
        assert event.connection_id == uuid.UUID(CONNECTION_ID)
        assert event.thread.id == "1.0"
        assert event.content[0].text.body == "hello"
        assert event.occurred_at == datetime(2026, 9, 16, 10, 0, 0, 100000, tzinfo=timezone.utc)

    def test_unknown_members_are_ignored(self):
        value = page(direct_message())
        value["trace"] = {"traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01"}
        value["payload"]["inbox_read_result"]["events"][0]["new_member"] = {"any": ["shape"]}

        assert decode(value).body.events[0].kind == "message"

    def test_a_mention_of_the_agent_carries_the_self_flag(self):
        [mention, text] = decode(page(message_event(mention_agent=True))).body.events[0].content

        assert (mention.reference.id, mention.reference.self) == ("U_FIXTURE_AGENT", True)
        assert text.reference is None

    def test_a_member_of_another_type_is_malformed(self):
        value = page(direct_message())
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
        assert "send" in body.operations and body.limits.text_characters == 4000

    @pytest.mark.parametrize("change", [{"supported_versions": ["2"]}, {"delivery_modes": ["push"]}])
    def test_a_negotiation_needs_version_1_and_pull(self, change):
        assert reason(frame("negotiation", {**gateway_negotiation(), **change})) == "version_unsupported"

    @pytest.mark.parametrize("limits", [{"frame_bytes": 16383}, {"in_flight_requests": 0}])
    def test_a_negotiation_needs_usable_limits(self, limits):
        assert reason(frame("negotiation", gateway_negotiation(**limits))) == "limit_exceeded"


class TestInboxPages:
    @pytest.mark.parametrize("member", ["event_id", "delivery", "admission", "message", "author"])
    def test_an_event_needs_the_members_that_nerve_reads(self, member):
        value = page(direct_message())
        del value["payload"]["inbox_read_result"]["events"][0][member]

        assert reason(value) == "malformed_frame"

    def test_an_invoke_needs_its_invoke_member(self):
        value = page(direct_message())
        del value["payload"]["inbox_read_result"]["events"][0]["admission"]["invoke"]

        assert reason(value) == "malformed_frame"

    def test_an_unknown_admission_purpose_is_refused(self):
        value = page(direct_message())
        value["payload"]["inbox_read_result"]["events"][0]["admission"]["purpose"] = "inspect"

        assert reason(value) == "kind_unsupported"

    def test_an_unknown_inbox_outcome_is_refused(self):
        value = page()
        value["payload"]["inbox_read_result"]["outcome"] = "partial"

        assert reason(value) == "kind_unsupported"

    def test_a_page_larger_than_the_read_is_refused(self):
        result = decode(page(direct_message(message_id="1"), direct_message(message_id="2"))).body

        with pytest.raises(Rejected) as error:
            validate_inbox_read_result(InboxRead(maximum_events=1, maximum_bytes=16384), result, 1000)
        assert error.value.reason == "limit_exceeded"
        with pytest.raises(Rejected):
            validate_inbox_read_result(InboxRead(maximum_events=2, maximum_bytes=16384), result, 16385)

    def test_one_event_may_exceed_the_read_byte_limit(self):
        result = decode(page(direct_message())).body

        validate_inbox_read_result(InboxRead(maximum_events=1, maximum_bytes=16384), result, 20000)


class TestOperationResults:
    SEND = Operation(kind="send", deadline_millis=30000, send=SendOperation())
    READ = Operation(kind="file_read", deadline_millis=30000, file_read=FileReadOperation(length_bytes=10))

    def result(self, **members: Any) -> OperationResult:
        value = frame("operation_result", members, connection_id=CONNECTION_ID)
        del value["request_id"]
        value["correlation_id"] = "n-1"
        return decode(value).body

    def refused(self, operation: Operation, **members: Any) -> str:
        with pytest.raises(Rejected) as error:
            validate_operation_result(operation, self.result(**members))
        return error.value.reason

    def test_a_result_needs_a_known_outcome(self):
        value = frame("operation_result", {"kind": "send", "outcome": "done"}, connection_id=CONNECTION_ID)

        assert reason(value) == "kind_unsupported"

    def test_a_result_answers_its_own_operation_kind(self):
        assert self.refused(self.SEND, kind="edit", outcome="succeeded") == "scope_mismatch"

    def test_a_successful_send_and_read_carry_the_output_that_nerve_reads(self):
        assert self.refused(self.SEND, kind="send", outcome="succeeded") == "malformed_frame"
        assert self.refused(self.READ, kind="file_read", outcome="succeeded") == "malformed_frame"
        validate_operation_result(self.SEND, self.result(kind="send", outcome="forbidden"))

    def test_results_and_transfers_name_their_connection(self):
        assert reason(frame("operation_result", {"kind": "send", "outcome": "forbidden"})) == "scope_mismatch"
        assert reason(frame("transfer", {"transfer_id": "t", "total_bytes": 1, "data": "AA=="})) == "scope_mismatch"

    def test_only_a_successful_read_carries_a_transfer(self):
        transfer = {"transfer_id": "t", "total_bytes": 4}
        target = {"conversation": {"id": "C1"}, "message": {"id": "1.2"}}

        assert self.refused(self.READ, kind="file_read", outcome="unavailable", transfer=transfer) == "malformed_frame"
        assert self.refused(
            self.SEND, kind="send", outcome="succeeded", target=target, transfer=transfer,
        ) == "malformed_frame"

    def test_a_chunk_stays_inside_its_transfer_and_ends_with_it(self):
        def chunk(**members: Any) -> dict[str, Any]:
            body = {"transfer_id": "t", "offset": 0, "total_bytes": 3, "data": "AAA=", "final": False, **members}
            return frame("transfer", body, connection_id=CONNECTION_ID)

        assert reason(chunk(offset=2, final=True)) == "limit_exceeded"
        assert reason(chunk(final=True)) == "malformed_frame"
        assert reason(chunk(offset=1)) == "malformed_frame"

    def test_capabilities_need_usable_limits(self):
        assert reason(frame("capabilities", capabilities(text_characters=0), connection_id=CONNECTION_ID)) == (
            "limit_exceeded"
        )
        value = frame("capabilities", capabilities(), connection_id=CONNECTION_ID)
        del value["payload"]["capabilities"]["limits"]
        assert reason(value) == "limit_exceeded"


TIME = datetime(2026, 9, 16, 10, 0, 0, 250000, tzinfo=timezone.utc)
MESSAGE = MessageTarget(conversation=ConversationReference(id="C1"), message=MessageReference(id="1.2"))
MESSAGE_JSON = {"conversation": {"id": "C1"}, "message": {"id": "1.2"}}

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
    ("operation", Operation(kind="send", deadline_millis=30000, send=SendOperation(
        destination=MessageContainer(conversation=ConversationReference(id="C1"), thread=ThreadReference(id="1.1")),
        content=(
            ContentPart(kind="text", text=TextContent(format="markdown", body="Deploy now?")),
            ContentPart(kind="actions", actions=ActionsContent(elements=(
                ActionElement(kind="button", action_id="notif:n1:yes", label="Yes", value="yes", style="primary"),
            ))),
        ),
    )), {"kind": "send", "deadline_millis": 30000, "send": {
        "destination": {"conversation": {"id": "C1"}, "thread": {"id": "1.1"}},
        "content": [
            {"kind": "text", "text": {"format": "markdown", "body": "Deploy now?"}},
            {"kind": "actions", "actions": {"elements": [{
                "kind": "button", "action_id": "notif:n1:yes", "label": "Yes", "value": "yes", "style": "primary",
            }]}},
        ],
    }}),
    ("operation", Operation(kind="edit", deadline_millis=30000, edit=EditOperation(
        target=MESSAGE, content=(ContentPart(kind="text", text=TextContent(format="markdown", body="Done")),),
    )), {"kind": "edit", "deadline_millis": 30000, "edit": {
        "target": MESSAGE_JSON, "content": [{"kind": "text", "text": {"format": "markdown", "body": "Done"}}],
    }}),
    ("operation", Operation(kind="delete", deadline_millis=30000, delete=DeleteOperation(target=MESSAGE)), {
        "kind": "delete", "deadline_millis": 30000, "delete": {"target": MESSAGE_JSON},
    }),
    ("operation", Operation(kind="reaction", deadline_millis=30000, reaction=ReactionOperation(
        target=MESSAGE, reaction=Reaction(action="add", name="custom:partyparrot"),
    )), {"kind": "reaction", "deadline_millis": 30000, "reaction": {
        "target": MESSAGE_JSON, "reaction": {"action": "add", "name": "custom:partyparrot"},
    }}),
    ("operation", Operation(kind="interaction", deadline_millis=30000, interaction=InteractionOperation(
        target=InteractionTarget(origin=MESSAGE, interaction_id="press-1"), response="update",
        content=(ContentPart(kind="reference", reference=ContentReference(
            kind="mention", mention_kind="user", id="U1",
        )),),
    )), {"kind": "interaction", "deadline_millis": 30000, "interaction": {
        "target": {"origin": MESSAGE_JSON, "interaction_id": "press-1"}, "response": "update",
        "content": [{"kind": "reference", "reference": {"kind": "mention", "mention_kind": "user", "id": "U1"}}],
    }}),
    ("operation", Operation(kind="file_read", deadline_millis=30000, file_read=FileReadOperation(
        target=AttachmentTarget(origin=MESSAGE, attachment=AttachmentReference(id="F1", name="a.txt")),
        offset_bytes=0, length_bytes=4096,
    )), {"kind": "file_read", "deadline_millis": 30000, "file_read": {
        "target": {"origin": MESSAGE_JSON, "attachment": {"id": "F1", "name": "a.txt"}},
        "offset_bytes": 0, "length_bytes": 4096,
    }}),
    ("operation", Operation(kind="file_send", deadline_millis=30000, file_send=FileSendOperation(
        destination=MessageContainer(conversation=ConversationReference(id="C1")),
        file=FileUpload(transfer_id="upload-1", name="a.txt", media_type="text/plain", total_bytes=3),
    )), {"kind": "file_send", "deadline_millis": 30000, "file_send": {
        "destination": {"conversation": {"id": "C1"}},
        "file": {"transfer_id": "upload-1", "name": "a.txt", "media_type": "text/plain", "total_bytes": 3},
    }}),
    ("operation", Operation(kind="typing", deadline_millis=30000, typing=TypingOperation(
        target=MessageContainer(conversation=ConversationReference(id="C1")),
    )), {"kind": "typing", "deadline_millis": 30000, "typing": {"target": {"conversation": {"id": "C1"}}}}),
    ("transfer", TransferChunk(transfer_id="upload-1", offset=0, total_bytes=3, data=b"abc", final=True), {
        "transfer_id": "upload-1", "offset": 0, "total_bytes": 3, "data": "YWJj", "final": True,
    }),
    ("inbox_ack", InboxAck(items=(
        InboxAckItem(inbox_id="1007", outcome="accepted"),
        InboxAckItem(inbox_id="1008", outcome="rejected", reason_code="admission_rejected"),
    )), {"items": [
        {"inbox_id": "1007", "outcome": "accepted"},
        {"inbox_id": "1008", "outcome": "rejected", "reason_code": "admission_rejected"},
    ]}),
]


class TestEncoding:
    @pytest.mark.parametrize(
        ("kind", "body", "expected"), SENT,
        ids=[getattr(body, "kind", kind) for kind, body, _ in SENT],
    )
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
