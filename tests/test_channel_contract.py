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
    MetadataEntry,
    Negotiation,
    Operation,
    OperationResult,
    Payload,
    Rejected,
    SourceFilters,
    SourceSubscription,
    Subscriptions,
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
    NotifyOperation,
    Reaction,
    ReactionOperation,
    SendOperation,
    TextContent,
    ThreadReference,
    TransferChunk,
    TypingOperation,
)
from nerve.channels.hosted.contract.model import MAX_TEXT_BYTES
from nerve.channels.hosted.runtime import RECEIVE_LIMITS
from tests.fake_channel_gateway import (
    CONNECTION_ID,
    capabilities,
    gateway_negotiation,
    message_event,
    source_record_event,
)


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

    NOTIFY = Operation(kind="notify", deadline_millis=30000, notify=NotifyOperation())
    NOTIFY_TARGET: dict[str, Any] = {"conversation": {"id": "C0NOTICES"}, "message": {"id": "1.2"}}

    def test_a_successful_notify_names_the_conversation_that_the_gateway_chose(self):
        result = self.result(kind="notify", outcome="succeeded", target=self.NOTIFY_TARGET)

        validate_operation_result(self.NOTIFY, result)
        assert (result.target.conversation.id, result.target.message.id) == ("C0NOTICES", "1.2")
        assert self.refused(self.NOTIFY, kind="send", outcome="succeeded", target=self.NOTIFY_TARGET) == (
            "scope_mismatch"
        )
        validate_operation_result(
            self.NOTIFY, self.result(kind="notify", outcome="forbidden", reason_code="invalid_target"),
        )

    def test_a_notify_message_may_root_its_own_thread(self):
        target = {**self.NOTIFY_TARGET, "thread": {"id": "1.2"}}

        validate_operation_result(self.NOTIFY, self.result(kind="notify", outcome="succeeded", target=target))

    def test_a_successful_notify_needs_a_target(self):
        assert self.refused(self.NOTIFY, kind="notify", outcome="succeeded") == "malformed_frame"

    @pytest.mark.parametrize(("change", "expected"), [
        ({"message": {}}, "malformed_frame"),
        ({"message": {"id": ""}}, "malformed_frame"),
        ({"message": {"id": "1" * 257}}, "malformed_frame"),
        ({"message": {"id": "1.\x072"}}, "malformed_frame"),
        ({"message": {"id": "   "}}, "malformed_frame"),
        ({"conversation": {}}, "malformed_frame"),
        ({"conversation": {"id": "C0NOTICES", "kind": "channel"}}, "malformed_frame"),
        ({"conversation": {"id": "C0NOTICES", "display_name": "notices"}}, "malformed_frame"),
        ({"thread": {"id": ""}}, "malformed_frame"),
        ({"thread": {"id": "1.1"}}, "scope_mismatch"),
        ({"topic": {"id": "T1"}}, "scope_mismatch"),
    ], ids=[
        "no_message", "empty_message", "long_message", "control_character", "blank_message", "no_conversation",
        "conversation_kind", "conversation_name", "empty_thread", "another_thread", "topic",
    ])
    def test_a_notify_target_that_the_gateway_would_refuse_is_refused(self, change, expected):
        target = {**self.NOTIFY_TARGET, **change}

        assert self.refused(self.NOTIFY, kind="notify", outcome="succeeded", target=target) == expected

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
    ("operation", Operation(kind="notify", deadline_millis=10000, notify=NotifyOperation(content=(
        ContentPart(kind="text", text=TextContent(format="markdown", body="Deploy now?")),
        ContentPart(kind="actions", actions=ActionsContent(elements=(
            ActionElement(kind="button", action_id="notif:n1:yes", label="Yes", value="yes"),
        ))),
    ))), {"kind": "notify", "deadline_millis": 10000, "notify": {"content": [
        {"kind": "text", "text": {"format": "markdown", "body": "Deploy now?"}},
        {"kind": "actions", "actions": {"elements": [
            {"kind": "button", "action_id": "notif:n1:yes", "label": "Yes", "value": "yes"},
        ]}},
    ]}}),
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


def record_page(**changes: Any) -> dict[str, Any]:
    return page(source_record_event(**changes))


def record_of(value: dict[str, Any]) -> dict[str, Any]:
    return value["payload"]["inbox_read_result"]["events"][0]["record"]


class TestSourceRecords:
    def test_a_record_decodes_into_its_typed_members(self):
        event = decode(record_page()).body.events[0]

        assert (event.provider, event.kind, event.admission.purpose) == ("github", "source_record", "observe")
        assert (event.conversation.id, event.conversation.kind) == ("owner/repo", "repository")
        assert event.message is None
        record = event.record
        assert (record.id, record.record_type) == ("9000000001", "github_notification")
        assert record.summary == "[owner/repo] Fix the flaky retry test (mention)"
        assert record.content.startswith("Repository: owner/repo\nType: PullRequest")
        assert record.timestamp.isoformat() == "2026-09-16T10:00:00+00:00"
        entries = {entry.key: entry for entry in record.metadata}
        assert entries["actors"] == MetadataEntry(key="actors", values=("alice", "bob"))
        assert entries["unread"] == MetadataEntry(key="unread", value="true")
        assert entries["ci_branch"] == MetadataEntry(key="ci_branch")

    def test_a_record_needs_no_message_author_or_content(self):
        value = record_page()
        event = value["payload"]["inbox_read_result"]["events"][0]
        del event["author"]
        del event["record"]["content"]
        del event["record"]["metadata"]

        record = decode(value).body.events[0].record
        assert (record.content, record.metadata) == ("", ())

    @pytest.mark.parametrize("member", ["id", "record_type", "summary"])
    def test_a_record_needs_its_id_type_and_summary(self, member):
        value = record_page()
        del record_of(value)[member]

        assert reason(value) == "malformed_frame"

    def test_a_source_record_event_needs_its_record(self):
        value = record_page()
        del value["payload"]["inbox_read_result"]["events"][0]["record"]

        assert reason(value) == "malformed_frame"

    def test_the_base_checks_still_apply(self):
        value = record_page()
        value["payload"]["inbox_read_result"]["events"][0]["conversation"] = {"kind": "repository"}

        assert reason(value) == "malformed_frame"

    def test_a_record_time_needs_a_time_zone(self):
        value = record_page()
        record_of(value)["timestamp"] = "2026-09-16T10:00:00"

        assert reason(value) == "malformed_frame"

    def test_list_metadata_of_another_type_is_malformed(self):
        value = record_page()
        record_of(value)["metadata"][6] = {"key": "actors", "values": "alice"}

        assert reason(value) == "malformed_frame"

    @pytest.mark.parametrize(("member", "size"), [
        ("id", 257), ("record_type", 257), ("summary", MAX_TEXT_BYTES + 1), ("content", MAX_TEXT_BYTES + 1),
    ])
    def test_record_members_above_nerves_limits_are_refused(self, member, size):
        value = record_page()
        record_of(value)[member] = "x" * size

        assert reason(value) == "limit_exceeded"

    def test_the_gateways_own_caps_fit_inside_nerves_limits(self):
        value = record_page()
        record_of(value)["summary"] = "é" * 256        # 512 bytes
        record_of(value)["content"] = "x" * 64 * 1024

        record = decode(value).body.events[0].record
        assert len(record.content) == 64 * 1024

    def test_a_record_member_on_another_kind_is_not_checked(self):
        value = page(direct_message())
        value["payload"]["inbox_read_result"]["events"][0]["record"] = {"id": ""}

        assert decode(value).body.events[0].kind == "message"


def github_subscription(**filters: tuple[str, ...]) -> Subscriptions:
    return Subscriptions(version="1", sources=(
        SourceSubscription(provider="github", filters=SourceFilters(**filters), pr_watch=True),
    ))


class TestSubscriptions:
    def test_nerve_sends_exactly_the_contract_members(self):
        body = github_subscription(
            allow_repos=("owner/*",), deny_repos=("owner/secret",), deny_actors=("codecov", "*-bot"),
            deny_reasons=("subscribed",), deny_ci_branches=("main", "release/*"),
        )
        envelope = Envelope(version="1", kind="subscriptions", request_id="o-2", payload=Payload(subscriptions=body))

        value = json.loads(encode_envelope(envelope))

        assert value == {"version": "1", "kind": "subscriptions", "request_id": "o-2", "payload": {
            "subscriptions": {"version": "1", "sources": [{
                "provider": "github",
                "filters": {
                    "allow_repos": ["owner/*"],
                    "deny_repos": ["owner/secret"],
                    "deny_actors": ["codecov", "*-bot"],
                    "deny_reasons": ["subscribed"],
                    "deny_ci_branches": ["main", "release/*"],
                },
                "pr_watch": True,
            }]},
        }}

    def test_no_sources_subscribes_to_nothing(self):
        envelope = Envelope(
            version="1", kind="subscriptions", request_id="o-2",
            payload=Payload(subscriptions=Subscriptions(version="1")),
        )

        assert json.loads(encode_envelope(envelope))["payload"] == {"subscriptions": {"version": "1"}}

    def test_without_filters_keeps_the_sources_and_the_pull_request_watch(self):
        wide = github_subscription(allow_repos=("owner/*",)).without_filters()

        assert wide.sources == (SourceSubscription(provider="github", filters=SourceFilters(), pr_watch=True),)
        wide.validate()

    @pytest.mark.parametrize(("subscriptions", "expected"), [
        (Subscriptions(version="2"), "version_unsupported"),
        (Subscriptions(version="1", sources=(SourceSubscription(provider="github"),) * 17), "limit_exceeded"),
        (Subscriptions(version="1", sources=(SourceSubscription(),)), "malformed_frame"),
        (github_subscription(deny_repos=tuple(f"owner/r{n}" for n in range(257))), "limit_exceeded"),
        (github_subscription(deny_repos=("x" * 257,)), "limit_exceeded"),
        (github_subscription(allow_actors=("",)), "limit_exceeded"),
        (github_subscription(deny_reasons=("sub\tscribed",)), "limit_exceeded"),
        (Subscriptions(version="1", sources=(SourceSubscription(provider="github"),) * 2), "malformed_frame"),
        (Subscriptions(version="1", sources=(SourceSubscription(provider="GitHub"),)), "malformed_frame"),
        (Subscriptions(version="1", sources=(SourceSubscription(provider="git hub"),)), "malformed_frame"),
        (Subscriptions(version="1", sources=(SourceSubscription(provider="gitlab/self"),)), "malformed_frame"),
        (Subscriptions(version="1", sources=(SourceSubscription(provider="g" * 65),)), "limit_exceeded"),
    ])
    def test_the_gateways_bounds_are_checked_before_sending(self, subscriptions, expected):
        with pytest.raises(Rejected) as error:
            subscriptions.validate()
        assert error.value.reason == expected

    def test_a_provider_is_any_lowercase_token_within_its_limit(self):
        Subscriptions(version="1", sources=(
            SourceSubscription(provider="github"),
            SourceSubscription(provider="git_hub-2.x"),
            SourceSubscription(provider="g" * 64),
        )).validate()

    def test_the_largest_subscription_within_bounds_is_valid(self):
        github_subscription(deny_repos=tuple("é" * 128 for _ in range(256))).validate()

    def test_the_gateway_never_sends_subscriptions(self):
        assert reason(frame("subscriptions", {"version": "1"})) == "kind_unsupported"
