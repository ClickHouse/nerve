"""Web Push in external mode: the fan-out hands each notification to the platform.

``fixtures/channel_contract`` holds copies of two samples of the channel
contract: ``capabilities_platform.json``, the platform connection that the
gateway advertises, and ``operation_platform_notify.json``, the operation
that Nerve sends on it. Nerve must accept the first and send the second
exactly.

The end-to-end tests run the fake gateway against the real stream endpoint,
with a real notification service and database. No test reaches a push
service, and in external mode nothing reads the VAPID key or the push
subscriptions.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from nerve.channels.hosted.contract import (
    PLATFORM_NOTIFY,
    OperationResult,
    PLATFORM_PROVIDER,
    Capabilities,
    Operation,
    PlatformNotifyOperation,
    Rejected,
    validate_operation_result,
)
from nerve.channels.hosted.contract.model import (
    MAX_PLATFORM_AGENT_NAME_BYTES,
    MAX_PLATFORM_BODY_BYTES,
    MAX_PLATFORM_TITLE_BYTES,
)
from nerve.channels.hosted.contract.wire import decode_record, dumps, encode_record
from nerve.gateway.auth import AUTH_MODE_EXTERNAL, pin_auth_mode, unpin_auth_mode
from nerve.notifications import platform_push, webpush
from tests.fake_channel_gateway import CAPABILITIES, NOTIFICATION_CONVERSATION
from tests.test_hosted_channel import Hosted, start_hosted, stop_hosted

FIXTURES = Path(__file__).parent / "fixtures" / "channel_contract"
# The SHA-256 pins of the channel contract's samples, which the copies in
# tests/fixtures/channel_contract/ must match byte for byte.
SAMPLE_SHA256 = {
    "capabilities_platform.json": "389745e6122140c9170196ce1cdd1fb938e2148992709b896d1a86e9df64b7f9",
    "operation_platform_notify.json": "27157e595cc5addd5324cf8e53dbeaa0b7b96328d60d7916c9f75047280c4899",
}


def fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


ANNOUNCEMENT = fixture("capabilities_platform.json")
PLATFORM_CONNECTION = ANNOUNCEMENT["connection_id"]
OPERATION = fixture("operation_platform_notify.json")
SAMPLE = OPERATION["payload"]["operation"][PLATFORM_NOTIFY]
NOTIFY_OPERATIONS = [*CAPABILITIES["operations"], "notify"]


class TestContractSamples:
    @pytest.mark.parametrize("name", sorted(SAMPLE_SHA256))
    def test_the_copy_of_the_contract_sample_has_its_exact_bytes(self, name):
        assert hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest() == SAMPLE_SHA256[name]

    def test_the_platform_connection_is_valid_and_serves_no_chat_channel(self):
        capabilities = decode_record(Capabilities, ANNOUNCEMENT["payload"]["capabilities"])
        capabilities.validate()

        assert capabilities.provider == PLATFORM_PROVIDER
        assert capabilities.operations == (PLATFORM_NOTIFY,)
        assert not capabilities.serves("slack") and not capabilities.serves("email")

    def test_the_operation_sample_decodes_and_encodes_to_the_same_json(self):
        operation = decode_record(Operation, OPERATION["payload"]["operation"])
        operation.platform_notify.validate()

        assert operation.kind == PLATFORM_NOTIFY
        assert encode_record(operation) == OPERATION["payload"]["operation"]

    def test_nerve_builds_the_sample_from_its_notification(self):
        built = platform_push.payload(
            notification_id=SAMPLE["notification_id"], notif_type=SAMPLE["type"], title=SAMPLE["title"],
            body=SAMPLE["body"], priority=SAMPLE["priority"], silent=False, agent_name=SAMPLE["agent_name"],
        )

        assert encode_record(built) == SAMPLE


class TestResult:
    PLATFORM_OPERATION = Operation(kind=PLATFORM_NOTIFY, platform_notify=PlatformNotifyOperation())
    TARGET = {"conversation": {"id": "C1"}, "message": {"id": "1.2"}}

    def result(self, **members: Any) -> OperationResult:
        return decode_record(OperationResult, {"kind": PLATFORM_NOTIFY, **members})

    def test_a_success_without_output_is_accepted(self):
        validate_operation_result(self.PLATFORM_OPERATION, self.result(outcome="succeeded"))

    @pytest.mark.parametrize("members", [
        {"target": TARGET},
        {"transfer": {"transfer_id": "t", "total_bytes": 4}},
    ], ids=["target", "transfer"])
    def test_a_success_with_output_is_refused(self, members):
        with pytest.raises(Rejected) as error:
            validate_operation_result(self.PLATFORM_OPERATION, self.result(outcome="succeeded", **members))
        assert error.value.reason == "malformed_frame"

    def test_a_refusal_without_output_is_accepted(self):
        validate_operation_result(
            self.PLATFORM_OPERATION, self.result(outcome="forbidden", reason_code="invalid_target"),
        )


def operation(**members: Any) -> PlatformNotifyOperation:
    values = {"notification_id": "notif-1a2b3c4d", "type": "notify", "title": "Build finished",
              "priority": "normal", **members}
    return PlatformNotifyOperation(**values)


class TestValidate:
    @pytest.mark.parametrize("members", [
        {},
        {"type": "question", "priority": "urgent", "silent": True, "body": "x", "agent_name": "Ada"},
        {"type": "some_future_type", "priority": "p2"},
        {"notification_id": "a" * 64},
        {"notification_id": "A.b_c:d-9"},
        {"title": ""},
        {"title": "é" * (MAX_PLATFORM_TITLE_BYTES // 2)},
        {"body": "b" * MAX_PLATFORM_BODY_BYTES},
        {"agent_name": "n" * MAX_PLATFORM_AGENT_NAME_BYTES},
    ], ids=["minimal", "full", "open_tokens", "longest_id", "id_alphabet", "empty_title", "longest_title",
            "longest_body", "longest_agent_name"])
    def test_a_payload_within_the_bounds_is_accepted(self, members):
        operation(**members).validate()

    @pytest.mark.parametrize(("members", "expected"), [
        ({"notification_id": ""}, "malformed_frame"),
        ({"notification_id": "a" * 65}, "malformed_frame"),
        ({"notification_id": "notif 1"}, "malformed_frame"),
        ({"type": ""}, "malformed_frame"),
        ({"type": "Notify"}, "malformed_frame"),
        ({"type": "1notify"}, "malformed_frame"),
        ({"priority": "a" * 33}, "malformed_frame"),
        ({"title": "t" * (MAX_PLATFORM_TITLE_BYTES + 1)}, "limit_exceeded"),
        ({"title": "\ud800"}, "limit_exceeded"),
        ({"body": "b" * (MAX_PLATFORM_BODY_BYTES + 1)}, "limit_exceeded"),
        ({"agent_name": "é" * (MAX_PLATFORM_AGENT_NAME_BYTES // 2 + 1)}, "limit_exceeded"),
    ], ids=["no_id", "long_id", "id_space", "no_type", "upper_type", "digit_type", "long_priority",
            "long_title", "lone_surrogate", "long_body", "long_agent_name"])
    def test_a_payload_that_the_gateway_refuses_is_refused(self, members, expected):
        with pytest.raises(Rejected) as error:
            operation(**members).validate()
        assert error.value.reason == expected

    DECODED = {"notification_id": "notif-1a2b3c4d", "type": "notify", "priority": "normal"}

    def test_a_decoded_empty_title_is_accepted_and_encoded(self):
        decoded = decode_record(PlatformNotifyOperation, {**self.DECODED, "title": ""})
        decoded.validate()
        assert encode_record(decoded)["title"] == ""

    @pytest.mark.parametrize("raw", [{}, {"title": None}], ids=["missing", "null"])
    def test_a_decoded_missing_or_null_title_is_refused(self, raw):
        decoded = decode_record(PlatformNotifyOperation, {**self.DECODED, **raw})
        with pytest.raises(Rejected) as error:
            decoded.validate()
        assert error.value.reason == "malformed_frame"


class TestPayload:
    def build(self, **members: Any) -> dict[str, Any]:
        values = {"notification_id": "notif-1a2b3c4d", "notif_type": "notify", "title": "Build finished",
                  "body": "", "priority": "normal", "silent": False, "agent_name": "", **members}
        return encode_record(platform_push.payload(**values))

    def test_the_text_is_cut_as_a_push_shows_it(self):
        body = "\n\nline one\n  \nline two\nline three\nline four\n"

        built = self.build(title="t" * 200, body=body)

        assert built["title"] == "t" * webpush.TITLE_CHARS
        assert built["body"] == "line one\nline two\nline three"
        assert len(self.build(body="b" * 500)["body"]) == webpush.BODY_CHARS

    def test_empty_members_are_left_out(self):
        assert self.build() == {
            "notification_id": "notif-1a2b3c4d", "type": "notify", "title": "Build finished", "priority": "normal",
        }

    def test_an_empty_title_is_sent_as_an_empty_member(self):
        frame = json.loads(dumps(Operation(kind=PLATFORM_NOTIFY, platform_notify=platform_push.payload(
            notification_id="n-1", notif_type="notify", title="", body="", priority="normal", silent=False,
            agent_name="",
        ))))
        assert frame[PLATFORM_NOTIFY]["title"] == ""

    def test_silent_and_the_agent_name_are_sent(self):
        built = self.build(silent=True, agent_name="Ada")
        assert (built["silent"], built["agent_name"]) == (True, "Ada")

    def test_a_long_agent_name_is_cut_on_a_character_boundary(self):
        name = self.build(agent_name="é" * 200)["agent_name"]
        assert name == "é" * (MAX_PLATFORM_AGENT_NAME_BYTES // 2)

    def test_a_lone_surrogate_becomes_a_question_mark(self):
        built = self.build(title="a\ud800b", body="c\udfffd", agent_name="e\ud800")
        assert (built["title"], built["body"], built["agent_name"]) == ("a?b", "c?d", "e?")

    @pytest.mark.parametrize("priority", ["", "High", "very important", "p" * 40])
    def test_a_priority_that_is_not_a_token_is_sent_as_normal(self, priority):
        built = self.build(priority=priority)
        assert built["priority"] == platform_push.DEFAULT_PRIORITY
        platform_push.payload(
            notification_id="n-1", notif_type="notify", title="t", body="", priority=priority, silent=False,
            agent_name="",
        ).validate()


# ---------------------------------------------------------------------- #
#  The hand-over through a stream                                          #
# ---------------------------------------------------------------------- #


def forbid_local_push(monkeypatch, db) -> None:
    """Fail the test when Nerve reads the push subscriptions or the VAPID key,
    or opens a connection to a push service."""
    async def no_subscriptions(*args, **kwargs):
        raise AssertionError("external mode read the push subscriptions")

    def no_http():
        raise AssertionError("external mode opened a connection to a push service")

    async def no_key(*args, **kwargs):
        raise AssertionError("external mode read the VAPID key")

    monkeypatch.setattr(db, "list_push_subscriptions", no_subscriptions)
    monkeypatch.setattr(webpush, "_http_session", no_http)
    monkeypatch.setattr(webpush, "vapid_key", no_key)


async def start(tmp_path, db, monkeypatch, *, external: bool) -> Hosted:
    harness = await start_hosted(tmp_path, db, monkeypatch, notifications=True)
    harness.notifications.engine.get_channel_runtime = (
        lambda name: harness.runtime if name == "hosted" else None
    )
    harness.gateway.notification_conversation = NOTIFICATION_CONVERSATION
    # No quiet hours, so that the time of day does not set the silent flag.
    harness.config.quiet_start = harness.config.quiet_end = ""
    await db.create_session("s1", actor=None)
    if external:
        # create_app() pinned local mode for the stream endpoint.
        unpin_auth_mode()
        pin_auth_mode(AUTH_MODE_EXTERNAL)
    return harness


@pytest_asyncio.fixture
async def hosted_external(tmp_path, db, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    monkeypatch.delenv("NERVE_DISPLAY_NAME", raising=False)
    harness = await start(tmp_path, db, monkeypatch, external=True)
    forbid_local_push(monkeypatch, db)
    try:
        yield harness
    finally:
        await stop_hosted(harness)


@pytest_asyncio.fixture
async def hosted_local(tmp_path, db, monkeypatch):
    harness = await start(tmp_path, db, monkeypatch, external=False)
    try:
        yield harness
    finally:
        await stop_hosted(harness)


async def platform_stream(hosted: Hosted, *, slack: bool = False):
    """A stream with the platform connection, and the Slack connection when *slack*."""
    stream = await hosted.stream(advertise=False)
    if slack:
        await stream.advertise(operations=NOTIFY_OPERATIONS)
    await stream.send("capabilities", ANNOUNCEMENT["payload"]["capabilities"], connection_id=PLATFORM_CONNECTION)
    await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections(PLATFORM_NOTIFY))
    if slack:
        await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections("notify"))
    return stream


def handed_over(hosted: Hosted) -> list[dict[str, Any]]:
    return [record for record in hosted.gateway.operations if record["kind"] == PLATFORM_NOTIFY]


async def delivered(hosted: Hosted, notification_id: str) -> list[str]:
    row = await hosted.db.get_notification(notification_id)
    return json.loads(row["channels_delivered"] or "[]")


def push_logs(caplog) -> list[logging.LogRecord]:
    return [record for record in caplog.records if record.name == platform_push.logger.name]


@pytest.mark.asyncio
class TestHandOver:
    async def test_a_question_is_handed_over_on_the_platform_connection(self, hosted_external):
        hosted = hosted_external
        await platform_stream(hosted)
        (Path(hosted.config.workspace) / "IDENTITY.md").write_text("# Identity\n- **Name:** Release helper\n")

        asked = await hosted.notifications.ask_question(
            "s1", "Deploy the release?", body="The tests passed.\n\nThe release notes are ready.",
            options=["yes", "no"], priority="high",
        )

        [record] = handed_over(hosted)
        assert record["connection_id"] == PLATFORM_CONNECTION
        assert set(record["operation"]) == {"kind", "deadline_millis", PLATFORM_NOTIFY}
        assert record["operation"]["deadline_millis"] == ANNOUNCEMENT["payload"]["capabilities"]["limits"][
            "operation_deadline_millis"
        ]
        assert record["operation"][PLATFORM_NOTIFY] == {**SAMPLE, "notification_id": asked["notification_id"]}
        assert webpush.PUSH_CHANNEL in await delivered(hosted, asked["notification_id"])
        assert hosted.gateway.sent_operations("notify") == []

    async def test_the_key_is_never_made(self, hosted_external):
        hosted = hosted_external
        await platform_stream(hosted)

        await hosted.notifications.send_notification("s1", "Build finished", "All green")

        assert len(handed_over(hosted)) == 1
        assert await hosted.db._get_instance_secret(webpush.VAPID_SECRET_NAME) is None

    async def test_during_quiet_hours_the_hand_over_is_silent(self, hosted_external, monkeypatch):
        from nerve.notifications import quiet_hours

        hosted = hosted_external
        await platform_stream(hosted)
        monkeypatch.setattr(quiet_hours, "in_quiet_hours", lambda config, now=None: True)

        await hosted.notifications.send_notification("s1", "Build finished")
        await hosted.notifications.send_notification("s1", "Disk full", priority="urgent")

        silent = [record["operation"][PLATFORM_NOTIFY].get("silent", False) for record in handed_over(hosted)]
        assert silent == [True, False]

    async def test_the_channel_level_applies_before_the_hand_over(self, hosted_external):
        hosted = hosted_external
        hosted.config.notifications.levels = {"webpush": "important"}
        await platform_stream(hosted)

        await hosted.notifications.send_notification("s1", "Build finished")
        assert handed_over(hosted) == []

        await hosted.notifications.send_notification("s1", "Disk full", priority="urgent")
        assert len(handed_over(hosted)) == 1

    async def test_without_a_platform_connection_nothing_is_handed_over(self, hosted_external, caplog):
        hosted = hosted_external
        stream = await hosted.stream(advertise=False)
        await stream.advertise(operations=NOTIFY_OPERATIONS)
        await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections("notify"))
        loop = asyncio.get_running_loop()

        started = loop.time()
        notification_id = await hosted.notifications.send_notification("s1", "Build finished", "All green")

        assert loop.time() - started < 1.0
        assert handed_over(hosted) == []
        assert webpush.PUSH_CHANNEL not in await delivered(hosted, notification_id)
        [line] = [record.getMessage() for record in push_logs(caplog)]
        assert notification_id in line and "All green" not in line and "Build finished" not in line

    async def test_without_a_hosted_runtime_nothing_is_handed_over(self, hosted_external):
        hosted = hosted_external
        hosted.notifications.engine.get_channel_runtime = lambda name: None

        notification_id = await hosted.notifications.send_notification("s1", "Build finished")

        assert webpush.PUSH_CHANNEL not in await delivered(hosted, notification_id)

    @pytest.mark.parametrize(("reply", "level"), [
        ({"outcome": "forbidden", "reason_code": "invalid_target"}, logging.DEBUG),
        ({"outcome": "unsupported"}, logging.INFO),
        ({"outcome": "unavailable", "reason_code": "maintenance"}, logging.INFO),
        ({"outcome": "unavailable", "reason_code": "configuration_invalid"}, logging.INFO),
        ({"outcome": "unavailable", "reason_code": "overloaded"}, logging.INFO),
        ({"outcome": "rate_limited", "retry_after_millis": 100}, logging.INFO),
        ({"outcome": "ambiguous"}, logging.INFO),
    ], ids=["no_browser", "unsupported", "maintenance", "configuration_invalid", "overloaded", "rate_limited",
            "ambiguous"])
    async def test_a_refusal_records_no_delivery_and_is_not_sent_again(self, hosted_external, caplog, reply, level):
        hosted = hosted_external
        await platform_stream(hosted)
        hosted.gateway.script(PLATFORM_NOTIFY, reply)

        notification_id = await hosted.notifications.send_notification("s1", "Build finished", "All green")
        await asyncio.sleep(0.2)

        assert len(handed_over(hosted)) == 1
        assert webpush.PUSH_CHANNEL not in await delivered(hosted, notification_id)
        [record] = push_logs(caplog)
        assert record.levelno == level
        line = record.getMessage()
        assert notification_id in line and reply["outcome"] in line
        assert "All green" not in line and "Build finished" not in line

    @pytest.mark.parametrize("reply", ["close", "hold"], ids=["stream_closed", "no_result_in_time"])
    async def test_a_lost_result_records_no_delivery(self, hosted_external, reply):
        hosted = hosted_external
        stream = await hosted.stream(advertise=False)
        capabilities = json.loads(json.dumps(ANNOUNCEMENT["payload"]["capabilities"]))
        capabilities["limits"]["operation_deadline_millis"] = 200
        await stream.send("capabilities", capabilities, connection_id=PLATFORM_CONNECTION)
        await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections(PLATFORM_NOTIFY))
        hosted.gateway.script(PLATFORM_NOTIFY, reply)

        notification_id = await hosted.notifications.send_notification("s1", "Build finished")

        assert len(handed_over(hosted)) == 1
        assert webpush.PUSH_CHANNEL not in await delivered(hosted, notification_id)

    async def test_an_empty_title_is_handed_over_with_its_title_member(self, hosted_external):
        hosted = hosted_external
        stream = await platform_stream(hosted)

        notification_id = await hosted.notifications.send_notification("s1", "", "All green")

        [record] = handed_over(hosted)
        sent = record["operation"][PLATFORM_NOTIFY]
        assert "title" in sent
        assert (sent["title"], sent["body"]) == ("", "All green")
        assert webpush.PUSH_CHANNEL in await delivered(hosted, notification_id)
        assert not stream.closed.is_set()

    async def test_an_id_the_gateway_would_refuse_is_not_sent_and_the_stream_stays(self, hosted_external):
        hosted = hosted_external
        stream = await platform_stream(hosted)

        taken = await platform_push.hand_over(
            hosted.runtime, hosted.config, notification_id="notif 1", notif_type="notify", title="Build finished",
            body="", priority="normal", silent=False,
        )

        assert taken is False
        assert handed_over(hosted) == []
        assert not stream.closed.is_set()

    async def test_a_full_connection_refuses_at_once(self, hosted_external):
        hosted = hosted_external
        stream = await hosted.stream(advertise=False)
        capabilities = json.loads(json.dumps(ANNOUNCEMENT["payload"]["capabilities"]))
        capabilities["limits"]["in_flight_operations"] = 1
        await stream.send("capabilities", capabilities, connection_id=PLATFORM_CONNECTION)
        await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections(PLATFORM_NOTIFY))
        hosted.gateway.script(PLATFORM_NOTIFY, "hold")
        first = asyncio.create_task(hosted.notifications.send_notification("s1", "Build finished"))
        await hosted.gateway.wait_for(lambda: stream.held)
        loop = asyncio.get_running_loop()

        started = loop.time()
        notification_id = await hosted.notifications.send_notification("s1", "Disk full")

        # The operation runner of these tests waits up to 1 s for a stream.
        assert loop.time() - started < 0.5
        assert len(handed_over(hosted)) == 1
        assert webpush.PUSH_CHANNEL not in await delivered(hosted, notification_id)
        first.cancel()

    async def test_slack_cards_still_go_beside_the_platform_connection(self, hosted_external):
        hosted = hosted_external
        await platform_stream(hosted, slack=True)

        asked = await hosted.notifications.ask_question("s1", "Deploy now?", options=["yes", "no"])
        await hosted.gateway.wait_for(lambda: hosted.gateway.sent_operations("notify"))

        assert len(hosted.gateway.sent_operations("notify")) == 1
        assert len(handed_over(hosted)) == 1
        assert sorted(await delivered(hosted, asked["notification_id"])) == ["slack", "web", webpush.PUSH_CHANNEL]


@pytest.mark.asyncio
class TestLocalMode:
    async def test_the_platform_connection_is_not_used(self, hosted_local, monkeypatch):
        hosted = hosted_local
        await platform_stream(hosted, slack=True)
        sent: list[str] = []

        async def deliver(db, config, *, notification_id, **kwargs):
            sent.append(notification_id)
            return False

        monkeypatch.setattr(webpush, "deliver", deliver)

        asked = await hosted.notifications.ask_question("s1", "Deploy now?", options=["yes", "no"])
        await hosted.gateway.wait_for(lambda: hosted.gateway.sent_operations("notify"))

        assert sent == [asked["notification_id"]]
        assert handed_over(hosted) == []
        assert len(hosted.gateway.sent_operations("notify")) == 1

    async def test_a_connection_of_an_unknown_provider_is_ignored(self, hosted_local):
        hosted = hosted_local
        stream = await hosted.stream(advertise=False)
        await stream.advertise(operations=NOTIFY_OPERATIONS)
        await stream.advertise(
            connection_id=str(uuid.uuid4()), operations=["some_future_operation"], provider="some_future_provider",
        )
        await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections("some_future_operation"))

        await hosted.notifications.ask_question("s1", "Deploy now?", options=["yes", "no"])
        await hosted.gateway.wait_for(lambda: hosted.gateway.sent_operations("notify"))

        assert len(hosted.gateway.sent_operations("notify")) == 1
        assert {record["kind"] for record in hosted.gateway.operations} == {"notify"}
        assert not stream.closed.is_set()
