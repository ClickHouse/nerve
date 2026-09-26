"""Hosted channel outbound operations against the fake gateway.

The router, the stream adapter, the session manager, and the database are
real, as in ``test_hosted_channel``; the agent turn is a stand-in that
streams tokens through the broadcaster. The fake gateway records every
operation and answers with scripted results.

Covered here: the streamed reply (placeholder, paced edits, final message,
delete, and the final edit when the message is refused), long messages in
parts, reactions and the typing mark, the send tool allowed and refused,
rate-limited, ambiguous, unavailable, and unsupported operations, a stream
that closes during an operation, late results, and the in-flight operation
limit. Also file uploads and attachment reads through transfers, the checks
on read transfers, and notification cards with their answers and expiry.
"""

from __future__ import annotations

import asyncio
import base64
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio

import nerve.config as config_module
from nerve.agent.streaming import broadcaster
from nerve.agent.tools.handlers.notifications import send_channel_message_handler
from nerve.agent.tools.registry import ToolContext
from nerve.channels.archives import MAX_TEXT_SIZE
from nerve.channels.hosted.contract.model import (
    MAX_TRANSFER_BYTES,
    AttachmentReference,
    AttachmentTarget,
    DeleteOperation,
    FileReadOperation,
    FileSendOperation,
    FileUpload,
    ReactionOperation,
    TypingOperation,
)
from nerve.channels.hosted.operations import OperationFailed
from nerve.channels.hosted.outbound import container_for, message_target
from nerve.channels.hosted.runtime import HostedChannelRuntime
from nerve.gateway import server as gateway_server

from tests.fake_channel_gateway import (
    CONNECTION_ID,
    FakeChannelGateway,
    NerveServer,
    message_event,
)
from tests.slack_live import RecordingRouter
from tests.test_hosted_channel import (
    FAST_OPERATIONS,
    FAST_READER,
    FAST_STREAM,
    Hosted,
    hosted_config,
    start_hosted,
    stop_hosted,
)

pytestmark = pytest.mark.asyncio

CONNECTION = uuid.UUID(CONNECTION_ID)
PLACEHOLDER_ID = "1700009000.000001"
TOOL_TARGET = "C0123ABCD"


@pytest_asyncio.fixture
async def hosted(tmp_path, db, monkeypatch):
    harness = await start_hosted(tmp_path, db, monkeypatch)
    harness.config.slack.allow_outbound = True
    harness.router.engine.router = harness.router
    try:
        yield harness
    finally:
        await stop_hosted(harness)


def body(operation: dict[str, Any]) -> str:
    return "".join(part["text"]["body"] for part in operation["content"])


def gaps(records: list[dict[str, Any]]) -> list[float]:
    return [later["at"] - earlier["at"] for earlier, later in zip(records, records[1:])]


def text_of(content: list[dict[str, Any]]) -> str:
    return "".join(part["text"]["body"] for part in content if part["kind"] == "text")


def attachment_event(
    attachments: list[dict[str, Any]], *, text: str = "see the files", message_id: str = "1700000400.000300",
) -> dict[str, Any]:
    event = message_event(
        text=text, conversation="D0DIRECT", conversation_kind="direct", message_id=message_id,
    )
    event["attachments"] = attachments
    return event


def interaction_event(action_id: str, *, message_id: str, conversation: str = TOOL_TARGET) -> dict[str, Any]:
    """A button press on *message_id*, whose message shows its own text."""
    event = message_event(
        kind="interaction", conversation=conversation, message_id=message_id,
        event_id=f"ev-press-{conversation}-{message_id}-{action_id}",
    )
    event["content"] = [{"kind": "text", "text": {"format": "plain", "body": "the card as shown"}}]
    event["interaction"] = {"id": f"press-{message_id}", "kind": "action", "action_id": action_id}
    return event


def read_payload(length: int, attachment: str = "F1") -> FileReadOperation:
    return FileReadOperation(
        target=AttachmentTarget(
            origin=message_target(TOOL_TARGET, None, "1.1"), attachment=AttachmentReference(id=attachment),
        ),
        offset_bytes=0,
        length_bytes=length,
    )


def records(hosted: Hosted, kind: str) -> list[dict[str, Any]]:
    return [record for record in hosted.gateway.operations if record["kind"] == kind]


async def direct_turn(hosted: Hosted, stream, message_id: str = "1700000400.000100") -> str:
    inbox_id = hosted.gateway.store(message_event(
        text="how are the deploys?", conversation="D0DIRECT", conversation_kind="direct",
        message_id=message_id,
    ))
    await stream.nudge("invoke")
    await hosted.acknowledged(inbox_id)
    return message_id


async def send_tool(hosted: Hosted, target: str = TOOL_TARGET, text: str = "Deploy finished."):
    result = await send_channel_message_handler(
        ToolContext(session_id="s1", engine=hosted.router.engine),
        {"channel": "slack", "target": target, "text": text},
    )
    return result.content[0]["text"], result.is_error


def streaming_turn(words: list[str], pause: float):
    """An agent turn that streams *words* as tokens, then finishes."""

    async def run(**kwargs: Any) -> str:
        session_id = kwargs["session_id"]
        for word in words:
            await broadcaster.broadcast_token(session_id, word)
            await asyncio.sleep(pause)
        await broadcaster.broadcast_done(session_id)
        return "".join(words)

    return run


# ---------------------------------------------------------------------- #
#  Streamed replies                                                        #
# ---------------------------------------------------------------------- #


class TestStreamedReply:
    async def test_a_placeholder_paced_edits_a_final_message_and_a_delete(self, hosted):
        stream = await hosted.stream(advertise=False)
        await stream.advertise(edit_interval_millis=200)
        words = ["Deploys ", "look ", "fine ", "today. "] * 6
        hosted.router.engine.run = streaming_turn(words, 0.03)

        message_id = await direct_turn(hosted, stream)
        await hosted.gateway.wait_for(lambda: len(records(hosted, "delete")) == 1)

        sends = hosted.gateway.sent_operations("send")
        assert [body(send) for send in sends] == ["⏳", "".join(words).strip()]
        assert sends[0]["destination"] == {"conversation": {"id": "D0DIRECT"}}
        edits = records(hosted, "edit")
        assert 1 <= len(edits) < len(words)
        assert all(gap >= 0.15 for gap in gaps(edits))
        for edit in edits:
            assert edit["operation"]["edit"]["target"]["message"]["id"] == PLACEHOLDER_ID
            assert body(edit["operation"]["edit"]).endswith("⏳⏳⏳")
        assert hosted.gateway.sent_operations("delete")[0]["target"]["message"]["id"] == PLACEHOLDER_ID
        [typing] = hosted.gateway.sent_operations("reaction")
        assert typing["target"]["message"]["id"] == message_id
        assert typing["reaction"] == {"action": "add", "name": "\U0001f440"}

    async def test_a_refused_final_message_becomes_a_final_edit(self, hosted):
        stream = await hosted.stream(advertise=False)
        await stream.advertise(edit_interval_millis=200)
        hosted.gateway.script("send", {"outcome": "succeeded"}, {
            "outcome": "forbidden", "reason_code": "policy_denied",
        })
        hosted.router.engine.run = streaming_turn(["All ", "green."], 0.01)

        await direct_turn(hosted, stream)
        await hosted.gateway.wait_for(
            lambda: any(body(edit) == "All green." for edit in hosted.gateway.sent_operations("edit")),
        )

        assert len(hosted.gateway.sent_operations("send")) == 2
        final = hosted.gateway.sent_operations("edit")[-1]
        assert final["target"]["message"]["id"] == PLACEHOLDER_ID
        assert hosted.gateway.sent_operations("delete") == []

    async def test_throttled_edits_are_dropped_inside_the_interval_and_final_edits_wait(self, hosted):
        stream = await hosted.stream(advertise=False)
        await stream.advertise(edit_interval_millis=300)
        await direct_turn(hosted, stream)
        await hosted.gateway.wait_for(lambda: records(hosted, "send"))
        channel = hosted.runtime.channels["slack"]

        await channel.edit_message("D0DIRECT", PLACEHOLDER_ID, "one", throttle=True)
        await channel.edit_message("D0DIRECT", PLACEHOLDER_ID, "two", throttle=True)
        await channel.edit_message("D0DIRECT", PLACEHOLDER_ID, "three")

        edits = records(hosted, "edit")
        assert [body(edit["operation"]["edit"]) for edit in edits] == ["one", "three"]
        assert gaps(edits)[0] >= 0.25

    async def test_a_throttled_edit_without_room_is_dropped_at_once(self, hosted):
        stream = await hosted.stream(advertise=False)
        await stream.advertise(in_flight_operations=1)
        await direct_turn(hosted, stream)
        await hosted.gateway.wait_for(lambda: records(hosted, "send"))
        [nerve_stream] = hosted.runtime.streams.streams
        await hosted.gateway.wait_for(lambda: nerve_stream.operations_in_flight(CONNECTION) == 0)
        hosted.gateway.script("delete", "hold")
        channel = hosted.runtime.channels["slack"]
        delete = asyncio.create_task(channel.delete_message("D0DIRECT", PLACEHOLDER_ID))
        await hosted.gateway.wait_for(lambda: records(hosted, "delete"))

        await asyncio.wait_for(
            channel.edit_message("D0DIRECT", PLACEHOLDER_ID, "partial", throttle=True), 0.5,
        )

        assert records(hosted, "edit") == []
        delete.cancel()


# ---------------------------------------------------------------------- #
#  Messages                                                                #
# ---------------------------------------------------------------------- #


class TestSend:
    async def test_a_long_reply_is_sent_in_parts_at_the_text_limit(self, tmp_path, monkeypatch):
        config = hosted_config(tmp_path)
        monkeypatch.setattr(config_module, "_config", config)
        text = "alpha beta gamma\ndelta epsilon\n" + "z" * 25
        router = RecordingRouter(reply_text=text)
        gateway = FakeChannelGateway(jwks_file=config.channels.hosted.gateway_jwks_file)
        runtime = HostedChannelRuntime(
            config, router, lambda: config, stream_timing=FAST_STREAM,
            reader_settings=FAST_READER, operation_timing=FAST_OPERATIONS,
        )
        await runtime.start()
        monkeypatch.setattr(gateway_server, "_hosted_channels", runtime)
        try:
            async with NerveServer(gateway_server.create_app()) as server:
                stream = await gateway.open_stream(server.stream_url, advertise=False)
                await stream.advertise(text_characters=12)
                gateway.store(message_event(
                    conversation="D0DIRECT", conversation_kind="direct", message_id="7.1",
                ))
                await stream.nudge("invoke")
                await asyncio.wait_for(router.wait_for_message("", 5.0), 10.0)
                await gateway.close_all()
        finally:
            await runtime.stop()

        parts = [body(send) for send in gateway.sent_operations("send")]
        assert parts == ["alpha beta", "gamma", "delta", "epsilon", "z" * 12, "z" * 12, "z"]

    async def test_a_rate_limited_message_is_sent_again_after_the_delay(self, hosted):
        await hosted.stream()
        hosted.gateway.script("send", {"outcome": "rate_limited", "retry_after_millis": 300})

        text, is_error = await send_tool(hosted)

        assert (text, is_error) == (f"Message sent to slack target {TOOL_TARGET}.", False)
        sends = records(hosted, "send")
        assert len(sends) == 2
        assert gaps(sends)[0] >= 0.25

    @pytest.mark.parametrize("reply", ["close", {"outcome": "ambiguous"}, "hold"])
    async def test_an_ambiguous_message_is_never_sent_again(self, hosted, reply):
        stream = await hosted.stream(advertise=False)
        await stream.advertise(operation_deadline_millis=200)
        hosted.gateway.script("send", reply)

        text, is_error = await send_tool(hosted)
        await hosted.stream()
        await asyncio.sleep(0.2)

        assert is_error and "ambiguous: it may already have taken effect" in text
        assert "stream closed" not in text and "no result in time" not in text
        assert len(hosted.gateway.sent_operations("send")) == 1

    async def test_a_long_advised_delay_is_not_waited_for(self, hosted):
        await hosted.stream()
        hosted.gateway.script("send", {"outcome": "rate_limited", "retry_after_millis": 60000})

        text, is_error = await send_tool(hosted)

        assert is_error and "rate_limited" in text
        assert len(records(hosted, "send")) == 1

    async def test_a_forbidden_later_part_is_a_failure_not_a_refusal(self, hosted):
        stream = await hosted.stream(advertise=False)
        await stream.advertise(text_characters=10)
        hosted.gateway.script("send", {"outcome": "succeeded"}, {
            "outcome": "forbidden", "reason_code": "policy_denied",
        })

        text, is_error = await send_tool(hosted, text="first part\nsecond part\nthird")

        assert is_error and text.startswith("Failed to send message on slack")
        assert "policy_denied" not in text
        assert [body(send) for send in hosted.gateway.sent_operations("send")] == [
            "first part", "second",
        ]

    async def test_a_part_too_large_for_the_frame_is_split_again(self, hosted):
        stream = await hosted.stream(negotiate=False)
        await stream.negotiate(frame_bytes=16384, memory_bytes=16384 + 16777216)
        await stream.advertise(text_characters=20000)

        text, is_error = await send_tool(hosted, text="a" * 20000)

        assert not is_error, text
        assert [len(body(send)) for send in hosted.gateway.sent_operations("send")] == [10000, 10000]

    async def test_an_unavailable_result_is_a_failure(self, hosted):
        await hosted.stream()
        hosted.gateway.script("send", {"outcome": "unavailable", "reason_code": "provider_unavailable"})

        text, is_error = await send_tool(hosted)

        assert is_error and "send operation unavailable" in text
        assert "provider_unavailable" not in text
        assert len(hosted.gateway.sent_operations("send")) == 1

    async def test_without_a_stream_the_message_fails_as_unavailable(self, hosted):
        text, is_error = await send_tool(hosted)

        assert is_error and "unavailable" in text
        assert hosted.gateway.operations == []

    async def test_availability_follows_the_advertised_streams(self, hosted):
        channel = hosted.runtime.channels["slack"]
        assert not channel.is_available
        stream = await hosted.stream(advertise=False)
        await stream.advertise(text_characters=500, edit_interval_millis=2000)
        await hosted.gateway.wait_for(lambda: channel.is_available)

        assert channel.constraints.max_message_length == 500
        assert channel.constraints.min_edit_interval == 2.0
        await stream.close()
        await hosted.gateway.wait_for(lambda: not channel.is_available)


# ---------------------------------------------------------------------- #
#  The send tool                                                           #
# ---------------------------------------------------------------------- #


class TestSendTool:
    async def test_an_allowed_message_is_posted_in_the_named_conversation(self, hosted):
        await hosted.stream()

        text, is_error = await send_tool(hosted, text="Deploy **finished**.")

        assert (text, is_error) == (f"Message sent to slack target {TOOL_TARGET}.", False)
        [send] = hosted.gateway.sent_operations("send")
        assert send["destination"] == {"conversation": {"id": TOOL_TARGET}}
        assert send["content"] == [
            {"kind": "text", "text": {"format": "markdown", "body": "Deploy **finished**."}},
        ]

    async def test_a_message_the_gateway_forbids_is_a_coarse_refusal(self, hosted):
        await hosted.stream()
        hosted.gateway.script("send", {"outcome": "forbidden", "reason_code": "policy_denied"})

        text, is_error = await send_tool(hosted, target=f"{TOOL_TARGET}:1700000000.000100")

        assert not is_error
        assert text.startswith("Refused: cannot send to slack target")
        assert "the channel gateway does not allow a message to this destination" in text
        assert "policy_denied" not in text
        assert hosted.gateway.sent_operations("send")[0]["destination"]["thread"] == {
            "id": "1700000000.000100",
        }

    @pytest.mark.parametrize(("target", "reason"), [
        ("#general", "not a name"),
        (":1700000000.000100", "no slack conversation ID"),
    ])
    async def test_a_malformed_target_is_refused_without_an_operation(self, hosted, target, reason):
        await hosted.stream()

        text, is_error = await send_tool(hosted, target=target)

        assert not is_error and reason in text
        assert hosted.gateway.operations == []

    async def test_the_local_switch_still_applies(self, hosted):
        await hosted.stream()
        hosted.config.slack.allow_outbound = False

        text, _ = await send_tool(hosted)

        assert "slack.allow_outbound is not enabled" in text
        assert hosted.gateway.operations == []

    async def test_several_connections_without_history_are_refused(self, hosted):
        stream = await hosted.stream()
        await stream.advertise(connection_id=str(uuid.uuid4()), self_id="U_OTHER_AGENT")
        channel = hosted.runtime.channels["slack"]
        await hosted.gateway.wait_for(
            lambda: len(hosted.runtime.streams.connections("send")) == 2,
        )

        text, _ = await send_tool(hosted)

        assert "several slack connections" in text
        assert channel.connection_for(TOOL_TARGET) is None


# ---------------------------------------------------------------------- #
#  Reactions                                                               #
# ---------------------------------------------------------------------- #


class TestReactions:
    async def test_typing_marks_the_turn_message_with_eyes_once(self, hosted):
        stream = await hosted.stream()
        message_id = await direct_turn(hosted, stream)
        await hosted.turns.wait_for(1)
        await hosted.gateway.wait_for(lambda: len(records(hosted, "reaction")) == 1)

        await hosted.runtime.channels["slack"].send_typing("D0DIRECT")
        await asyncio.sleep(0.1)

        [reaction] = hosted.gateway.sent_operations("reaction")
        assert reaction["target"]["message"]["id"] == message_id
        assert reaction["reaction"]["name"] == "\U0001f440"

    async def test_reactions_use_emoji_and_map_short_names(self, hosted):
        stream = await hosted.stream()
        message_id = await direct_turn(hosted, stream)
        await hosted.gateway.wait_for(lambda: len(records(hosted, "reaction")) == 1)
        channel = hosted.runtime.channels["slack"]

        for emoji in ("white_check_mark", "👍", ":partyparrot:", "not an emoji"):
            await channel.set_reaction("D0DIRECT", message_id, emoji)

        names = [op["reaction"]["name"] for op in hosted.gateway.sent_operations("reaction")[1:]]
        assert names == ["✅", "👍", "custom:partyparrot"]

    async def test_a_message_id_that_the_gateway_refuses_fails_locally(self, hosted):
        stream = await hosted.stream()
        message_id = await direct_turn(hosted, stream)
        await hosted.gateway.wait_for(lambda: len(records(hosted, "reaction")) == 1)

        await hosted.runtime.channels["slack"].set_reaction("D0DIRECT", message_id + "\n", "eyes")
        await asyncio.sleep(0.1)

        assert len(hosted.gateway.sent_operations("reaction")) == 1
        assert not stream.closed.is_set()

    async def test_an_unadvertised_operation_is_unsupported_without_a_request(self, hosted):
        stream = await hosted.stream(advertise=False)
        await stream.advertise(operations=["send"])
        await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections("send"))

        with pytest.raises(OperationFailed) as failure:
            await hosted.runtime.operations.perform(CONNECTION, "reaction", ReactionOperation(
                target=message_target(TOOL_TARGET, None, "1.2"),
            ))

        assert failure.value.outcome == "unsupported"
        assert hosted.gateway.operations == []


# ---------------------------------------------------------------------- #
#  Operation plumbing                                                      #
# ---------------------------------------------------------------------- #


class TestOperationPlumbing:
    async def test_a_retry_safe_operation_moves_to_another_stream_when_its_stream_closes(self, hosted):
        first = await hosted.stream()
        second = await hosted.stream()
        hosted.gateway.script("typing", "close")

        result = await hosted.runtime.operations.perform(CONNECTION, "typing", TypingOperation(
            target=container_for(TOOL_TARGET, None),
        ))

        assert result.outcome == "succeeded"
        typing = records(hosted, "typing")
        assert [record["stream"] for record in typing] == [first, second]

    async def test_a_message_lost_with_its_stream_is_ambiguous(self, hosted):
        await hosted.stream()
        await hosted.stream()
        hosted.gateway.script("delete", "close")

        with pytest.raises(OperationFailed) as failure:
            await hosted.runtime.operations.perform(CONNECTION, "delete", DeleteOperation(
                target=message_target(TOOL_TARGET, None, "1.2"),
            ))

        assert (failure.value.outcome, failure.value.local) == ("ambiguous", True)
        assert len(records(hosted, "delete")) == 1

    async def test_operations_wait_for_room_under_the_connection_limit(self, hosted):
        stream = await hosted.stream(advertise=False)
        await stream.advertise(in_flight_operations=1)
        await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections("delete"))
        hosted.gateway.script("delete", "hold")
        operations = hosted.runtime.operations

        def delete(message_id: str):
            return asyncio.create_task(operations.perform(CONNECTION, "delete", DeleteOperation(
                target=message_target(TOOL_TARGET, None, message_id),
            )))

        first, second = delete("1.1"), delete("1.2")
        await hosted.gateway.wait_for(lambda: len(records(hosted, "delete")) == 1)
        await asyncio.sleep(0.2)
        assert len(records(hosted, "delete")) == 1
        [held] = stream.held
        await stream.respond(
            held["request_id"], "operation_result", {"kind": "delete", "outcome": "succeeded"},
            connection_id=CONNECTION_ID,
        )

        await asyncio.gather(first, second)
        assert len(records(hosted, "delete")) == 2

    async def test_a_late_result_is_checked_and_frees_its_place(self, hosted):
        stream = await hosted.stream(advertise=False)
        await stream.advertise(operation_deadline_millis=100, in_flight_operations=1)
        await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections("delete"))
        hosted.gateway.script("delete", "hold")
        operations = hosted.runtime.operations
        [nerve_stream] = hosted.runtime.streams.streams

        with pytest.raises(OperationFailed) as failure:
            await operations.perform(CONNECTION, "delete", DeleteOperation(
                target=message_target(TOOL_TARGET, None, "1.1"),
            ))
        assert failure.value.outcome == "ambiguous"
        assert nerve_stream.operations_in_flight(CONNECTION) == 1

        [held] = stream.held
        await stream.respond(
            held["request_id"], "operation_result", {"kind": "delete", "outcome": "succeeded"},
            connection_id=CONNECTION_ID,
        )
        await hosted.gateway.wait_for(lambda: nerve_stream.operations_in_flight(CONNECTION) == 0)
        hosted.gateway.script("delete", "hold")
        with pytest.raises(OperationFailed):
            await operations.perform(CONNECTION, "delete", DeleteOperation(
                target=message_target(TOOL_TARGET, None, "1.2"),
            ))
        late = stream.held[-1]
        await stream.respond(
            late["request_id"], "operation_result", {"kind": "send", "outcome": "succeeded", "target": {
                "conversation": {"id": TOOL_TARGET}, "message": {"id": "1.3"},
            }},
            connection_id=CONNECTION_ID,
        )

        await asyncio.wait_for(stream.closed.wait(), 5.0)
        assert stream.close_code == 1002


class TestSendToolPolicy:
    async def test_allow_channels_still_limits_where_the_tool_posts(self, hosted):
        await hosted.stream()
        hosted.config.slack.allow_channels = ["C0OTHER"]

        text, _ = await send_tool(hosted)

        assert "not approved by the slack channel policy" in text
        assert "C0OTHER" not in text
        assert hosted.gateway.operations == []

        hosted.config.slack.allow_channels = [TOOL_TARGET]
        text, is_error = await send_tool(hosted)

        assert (text, is_error) == (f"Message sent to slack target {TOOL_TARGET}.", False)

    async def test_a_name_rule_in_the_deny_list_refuses_every_conversation(self, hosted):
        await hosted.stream()
        hosted.config.slack.deny_channels = ["#random"]

        text, _ = await send_tool(hosted)

        assert "not approved by the slack channel policy" in text
        assert hosted.gateway.operations == []

    async def test_a_name_rule_cannot_grant_a_hosted_conversation(self, hosted):
        await hosted.stream()
        hosted.config.slack.allow_channels = ["#deployments"]

        text, _ = await send_tool(hosted)

        assert "not approved by the slack channel policy" in text
        assert hosted.gateway.operations == []


async def small_frame_stream(hosted: Hosted, **limits: int):
    """A stream whose gateway takes frames of at most 16 KiB."""
    stream = await hosted.stream(negotiate=False)
    await stream.negotiate(frame_bytes=16384)
    await stream.advertise(**limits)
    await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections("file_send"))
    return stream


class TestFileSend:
    async def test_a_file_goes_up_in_chunks_that_fit_the_gateway_frame(self, hosted, tmp_path):
        stream = await small_frame_stream(hosted)
        data = bytes(range(256)) * 160
        path = tmp_path / "report.csv"
        path.write_bytes(data)

        sent = await hosted.runtime.channels["slack"].send_file(
            f"{TOOL_TARGET}:1700000000.000100", str(path),
        )

        assert sent
        [upload] = hosted.gateway.uploads
        assert upload["data"] == data
        assert (upload["file"]["name"], upload["file"]["media_type"]) == ("report.csv", "text/csv")
        [record] = records(hosted, "file_send")
        assert record["operation"]["file_send"]["destination"] == {
            "conversation": {"id": TOOL_TARGET}, "thread": {"id": "1700000000.000100"},
        }
        kinds = [frame["kind"] for frame in stream.frames]
        chunks = [frame for frame in stream.frames if frame["kind"] == "transfer"]
        assert len(chunks) > 3
        assert kinds.index("operation") < kinds.index("transfer")
        assert all(len(json.dumps(frame, separators=(",", ":"))) <= 16384 for frame in chunks)
        assert {frame["correlation_id"] for frame in chunks} == {record["request_id"]}
        assert {frame["payload"]["transfer"]["transfer_id"] for frame in chunks} == {
            upload["file"]["transfer_id"],
        }

    async def test_a_result_before_the_last_chunk_stops_the_upload(self, hosted, tmp_path):
        stream = await small_frame_stream(hosted)
        hosted.gateway.script("file_send", {"outcome": "forbidden", "reason_code": "policy_denied"})
        path = tmp_path / "large.bin"
        path.write_bytes(b"x" * 2_000_000)

        sent = await hosted.runtime.channels["slack"].send_file(TOOL_TARGET, str(path))

        assert not sent
        chunks = [frame for frame in stream.frames if frame["kind"] == "transfer"]
        assert not any(frame["payload"]["transfer"]["final"] for frame in chunks)
        assert hosted.gateway.uploads == []

    async def test_uploads_wait_while_they_do_not_fit_the_gateway_memory(self, hosted, tmp_path):
        stream = await hosted.stream(negotiate=False)
        await stream.negotiate(frame_bytes=16384, transfer_bytes=100_000, memory_bytes=116_384)
        await stream.advertise(file_bytes=100_000)
        await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections("file_send"))
        hosted.gateway.script("file_send", "hold")
        first, second = tmp_path / "first.bin", tmp_path / "second.bin"
        first.write_bytes(b"1" * 60_000)
        second.write_bytes(b"2" * 60_000)
        channel = hosted.runtime.channels["slack"]

        held = asyncio.create_task(channel.send_file(TOOL_TARGET, str(first)))
        await hosted.gateway.wait_for(lambda: stream.held)
        waiting = asyncio.create_task(channel.send_file(TOOL_TARGET, str(second)))
        await asyncio.sleep(0.3)

        assert len(hosted.gateway.sent_operations("file_send")) == 1
        [frame] = stream.held
        await stream.respond(frame["request_id"], "operation_result", {
            "kind": "file_send", "outcome": "unavailable", "reason_code": "provider_unavailable",
        }, connection_id=CONNECTION_ID)
        assert not await held
        assert await waiting
        assert [upload["data"][:1] for upload in hosted.gateway.uploads] == [b"2"]

    async def test_a_file_above_the_connection_limit_is_not_sent(self, hosted, tmp_path):
        await small_frame_stream(hosted, file_bytes=1000)
        path = tmp_path / "big.txt"
        path.write_bytes(b"x" * 1001)

        assert not await hosted.runtime.channels["slack"].send_file(TOOL_TARGET, str(path))
        assert hosted.gateway.operations == []

    async def test_an_edit_too_large_for_the_frame_is_shortened_before_it_is_sent(self, hosted):
        await small_frame_stream(hosted, text_characters=8000)

        await hosted.runtime.channels["slack"].edit_message(TOOL_TARGET, "1.1", "\U0001f600" * 8000)

        [edit] = hosted.gateway.sent_operations("edit")
        text = text_of(edit["content"])
        assert text.endswith("…") and 0 < len(text) <= 4000

    async def test_an_upload_above_the_gateway_limit_is_refused_at_once(self, hosted):
        await small_frame_stream(hosted, file_bytes=1000)
        operations = hosted.runtime.operations
        loop = asyncio.get_running_loop()
        started = loop.time()

        with pytest.raises(OperationFailed) as failure:
            await operations.perform(CONNECTION, "file_send", FileSendOperation(
                destination=container_for(TOOL_TARGET, None),
                file=FileUpload(transfer_id="u", name="big.bin", media_type="application/octet-stream",
                                total_bytes=1001),
            ), upload=b"x" * 1001)

        assert failure.value.reason_code == "content_too_large"
        assert loop.time() - started < 0.5
        assert hosted.gateway.operations == []

    async def test_a_missing_file_is_not_sent(self, hosted, tmp_path):
        await hosted.stream()

        assert not await hosted.runtime.channels["slack"].send_file(TOOL_TARGET, str(tmp_path / "none"))
        assert hosted.gateway.operations == []

    async def test_an_upload_lost_with_its_stream_is_not_sent_again(self, hosted, tmp_path):
        await hosted.stream()
        await hosted.stream()
        hosted.gateway.script("file_send", "close")
        path = tmp_path / "notes.txt"
        path.write_bytes(b"notes")

        assert not await hosted.runtime.channels["slack"].send_file(TOOL_TARGET, str(path))
        await asyncio.sleep(0.2)
        assert len(hosted.gateway.sent_operations("file_send")) == 1

    async def test_the_channel_declares_file_delivery(self, hosted):
        from nerve.channels.base import ChannelCapability

        assert ChannelCapability.SEND_FILES in hosted.runtime.channels["slack"].capabilities


class TestAttachments:
    async def test_attachments_are_read_through_the_gateway_into_the_turn(self, hosted):
        stream = await hosted.stream()
        image = b"\x89PNG\r\n\x1a\n" + bytes(100)
        notes = b"line one\n" * 400
        hosted.gateway.chunk_bytes = 1000
        hosted.gateway.files = {"F_NOTES": notes, "F_IMAGE": image, "F_BINARY": b"\0" * 10}
        inbox_id = hosted.gateway.store(attachment_event([
            {"id": "F_NOTES", "name": "notes.txt", "media_type": "text/plain", "size_bytes": len(notes)},
            {"id": "F_IMAGE", "name": "chart.png", "media_type": "image/png", "size_bytes": len(image)},
            {"id": "F_BINARY", "name": "data.bin", "size_bytes": 10},
        ]))

        await stream.nudge("invoke")
        await hosted.acknowledged(inbox_id)
        await hosted.turns.wait_for(1)

        run = hosted.turns.runs[0]
        text = run["user_message"]
        assert f"[File: notes.txt (4 KB, text/plain)]\n```\n{notes.decode()}\n```" in text
        assert "[File: data.bin (0 KB, unknown type)]" in text
        assert text.endswith("see the files")
        assert run["images"] == [
            {"type": "base64", "media_type": "image/png", "data": base64.b64encode(image).decode()},
        ]
        reads = hosted.gateway.sent_operations("file_read")
        assert [(read["target"]["attachment"]["id"], read["length_bytes"]) for read in reads] == [
            ("F_NOTES", len(notes)), ("F_IMAGE", len(image)),
        ]
        assert reads[0]["target"]["origin"]["message"] == {"id": "1700000400.000300"}

    async def test_a_file_of_unknown_size_is_read_one_byte_past_the_limit(self, hosted):
        stream = await hosted.stream()
        hosted.gateway.files = {"F_LOG": b"a" * (MAX_TEXT_SIZE + 10)}
        inbox_id = hosted.gateway.store(attachment_event([
            {"id": "F_LOG", "name": "big.log", "media_type": "text/plain"},
        ]))

        await stream.nudge("invoke")
        await hosted.acknowledged(inbox_id)
        await hosted.turns.wait_for(1)

        text = hosted.turns.runs[0]["user_message"]
        assert "[File: big.log (0 KB, text/plain)]\n(Too large or not downloadable)" in text
        assert "```" not in text
        [read] = hosted.gateway.sent_operations("file_read")
        assert read["length_bytes"] == MAX_TEXT_SIZE + 1

    async def test_a_failed_read_leaves_the_metadata_line(self, hosted):
        stream = await hosted.stream()
        hosted.gateway.script("file_read", {"outcome": "unavailable", "reason_code": "provider_unavailable"})
        inbox_id = hosted.gateway.store(attachment_event([
            {"id": "F_NOTES", "name": "notes.txt", "media_type": "text/plain", "size_bytes": 5},
        ]))

        await stream.nudge("invoke")
        await hosted.acknowledged(inbox_id)
        await hosted.turns.wait_for(1)

        assert hosted.turns.runs[0]["user_message"] == "[File: notes.txt (0 KB, text/plain)]\n\nsee the files"


class TestReadTransfers:
    @pytest.mark.parametrize("case", ["before the result", "skips bytes", "another transfer", "too large"])
    async def test_a_transfer_that_breaks_the_rules_closes_the_stream(self, hosted, case):
        stream = await hosted.stream()
        hosted.gateway.script("file_read", "hold")
        read = asyncio.create_task(hosted.runtime.operations.read(CONNECTION, "file_read", read_payload(100)))
        await hosted.gateway.wait_for(lambda: stream.held)
        [held] = stream.held
        total = 200 if case == "too large" else 10
        if case != "before the result":
            await stream.respond(held["request_id"], "operation_result", {
                "kind": "file_read", "outcome": "succeeded", "transfer": {
                    "transfer_id": "t1", "kind": "file_bytes", "serialization": "raw", "total_bytes": total,
                },
            }, connection_id=CONNECTION_ID)
        await stream.respond(held["request_id"], "transfer", {
            "transfer_id": "t2" if case == "another transfer" else "t1",
            "offset": 5 if case == "skips bytes" else 0,
            "total_bytes": 10,
            "data": base64.b64encode(b"abcde" * (1 if case == "skips bytes" else 2)).decode(),
            "final": True,
        }, connection_id=CONNECTION_ID)

        await asyncio.wait_for(stream.closed.wait(), 5.0)
        assert stream.close_code == 1002
        with pytest.raises(OperationFailed):
            await read

    async def test_an_abandoned_read_drops_late_data_and_keeps_its_place_until_the_end(self, hosted):
        stream = await hosted.stream(advertise=False)
        await stream.advertise(operation_deadline_millis=100)
        await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections("file_read"))
        # The first request and the two requests sent again after it is lost.
        hosted.gateway.script("file_read", "hold", "hold", "hold")
        [nerve_stream] = hosted.runtime.streams.streams

        with pytest.raises(OperationFailed):
            await hosted.runtime.operations.read(CONNECTION, "file_read", read_payload(10))
        held = stream.held[0]
        await stream.respond(held["request_id"], "operation_result", {
            "kind": "file_read", "outcome": "succeeded", "transfer": {
                "transfer_id": "t1", "kind": "file_bytes", "serialization": "raw", "total_bytes": 10,
            },
        }, connection_id=CONNECTION_ID)
        await stream.respond(held["request_id"], "transfer", {
            "transfer_id": "t1", "offset": 0, "total_bytes": 10,
            "data": base64.b64encode(b"abcde").decode(), "final": False,
        }, connection_id=CONNECTION_ID)
        await asyncio.sleep(0.1)

        pending = nerve_stream._pending[held["request_id"]]
        assert pending.data is None and pending.received == 5
        assert not stream.closed.is_set()

    async def test_a_read_waits_while_its_length_does_not_fit_nerve_memory(self, hosted):
        stream = await hosted.stream()
        hosted.gateway.script("file_read", "hold")
        read = asyncio.create_task(
            hosted.runtime.operations.read(CONNECTION, "file_read", read_payload(MAX_TRANSFER_BYTES)),
        )
        await hosted.gateway.wait_for(lambda: stream.held)
        [nerve_stream] = hosted.runtime.streams.streams

        assert not nerve_stream.has_operation_capacity(CONNECTION, MAX_TRANSFER_BYTES)
        assert nerve_stream.has_operation_capacity(CONNECTION, 1024)
        read.cancel()


@pytest_asyncio.fixture
async def noticed(tmp_path, db, monkeypatch):
    harness = await start_hosted(tmp_path, db, monkeypatch, notifications=True)
    harness.config.notifications.slack_channel_id = TOOL_TARGET
    await db.create_session("s1", actor=None)
    try:
        yield harness
    finally:
        await stop_hosted(harness)


async def question_card(hosted: Hosted) -> tuple[str, str]:
    """Ask a yes-or-no question, and return its ID and the card's message ID."""
    await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections("send"))
    asked = await hosted.notifications.ask_question(
        session_id="s1", title="Deploy now?", options=["yes", "no"],
    )
    notification_id = asked["notification_id"]
    await hosted.gateway.wait_for(lambda: hosted.gateway.sent_operations("send"))
    delivery = await hosted.db.get_latest_notification_delivery(notification_id, "slack")
    return notification_id, str(delivery["message_id"])


class TestNotifications:
    async def test_a_card_without_text_is_not_sent(self, noticed):
        stream = await noticed.stream()

        posted = await noticed.runtime.channels["slack"].post_notification("n-empty", "", [("Yes", "yes")])

        assert posted is None
        assert noticed.gateway.sent_operations("send") == []
        assert not stream.closed.is_set()

    async def test_a_question_is_a_card_with_buttons_and_a_press_answers_it(self, noticed):
        stream = await noticed.stream()
        notification_id, message_id = await question_card(noticed)

        [card] = noticed.gateway.sent_operations("send")
        assert card["destination"] == {"conversation": {"id": TOOL_TARGET}}
        assert "Deploy now?" in text_of(card["content"])
        elements = card["content"][-1]["actions"]["elements"]
        assert [(e["action_id"], e["value"], e.get("style")) for e in elements] == [
            (f"notif:{notification_id}:yes", "yes", "primary"),
            (f"notif:{notification_id}:no", "no", "danger"),
        ]

        inbox_id = noticed.gateway.store(interaction_event(f"notif:{notification_id}:yes", message_id=message_id))
        await stream.nudge("invoke")

        assert (await noticed.acknowledged(inbox_id))[inbox_id] == "accepted"
        await noticed.gateway.wait_for(lambda: noticed.gateway.sent_operations("interaction"))
        [update] = noticed.gateway.sent_operations("interaction")
        assert update["response"] == "update"
        assert update["target"]["interaction_id"] == f"press-{message_id}"
        assert update["target"]["origin"]["message"] == {"id": message_id}
        assert "Deploy now?" in text_of(update["content"])
        assert "✅ Answered: yes (by )" in text_of(update["content"])
        assert {"kind": "reference", "reference": {
            "kind": "mention", "mention_kind": "user", "id": "U_FIXTURE_MEMBER",
        }} in update["content"]
        assert not any(part["kind"] == "actions" for part in update["content"])
        notification = await noticed.db.get_notification(notification_id)
        assert (notification["status"], notification["answer"]) == ("answered", "yes")

    async def test_a_press_in_another_conversation_does_not_answer(self, noticed):
        stream = await noticed.stream()
        notification_id, message_id = await question_card(noticed)

        inbox_id = noticed.gateway.store(interaction_event(
            f"notif:{notification_id}:yes", message_id=message_id, conversation="C0ELSEWHERE",
        ))
        await stream.nudge("invoke")
        await noticed.acknowledged(inbox_id)

        await noticed.gateway.wait_for(lambda: noticed.gateway.sent_operations("interaction"))
        [notice] = noticed.gateway.sent_operations("interaction")
        assert notice["response"] == "acknowledge"
        assert text_of(notice["content"]) == "Already answered or expired."
        assert (await noticed.db.get_notification(notification_id))["status"] == "pending"
        await asyncio.sleep(0.1)
        assert noticed.gateway.sent_operations("edit") == []

    async def test_a_press_without_a_notification_service_gets_a_notice(self, hosted):
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(interaction_event("notif:n1:yes", message_id="1700000500.000200"))

        await stream.nudge("invoke")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        await hosted.gateway.wait_for(lambda: hosted.gateway.sent_operations("interaction"))
        [notice] = hosted.gateway.sent_operations("interaction")
        assert (notice["response"], text_of(notice["content"])) == ("acknowledge", "Service unavailable.")

    async def test_an_answer_that_cannot_be_recorded_is_deferred(self, noticed, monkeypatch):
        stream = await noticed.stream()
        notification_id, message_id = await question_card(noticed)

        calls: list[str] = []

        async def broken(notification_id, *args, **kwargs):
            calls.append(notification_id)
            raise RuntimeError("database is locked")

        monkeypatch.setattr(noticed.notifications, "answer_delivered_notification", broken)
        inbox_id = noticed.gateway.store(interaction_event(f"notif:{notification_id}:yes", message_id=message_id))
        await stream.nudge("invoke")

        await noticed.gateway.wait_for(lambda: calls)
        await asyncio.sleep(0.1)
        assert inbox_id not in noticed.gateway.acknowledged
        assert noticed.gateway.sent_operations("interaction") == []

    async def test_a_press_the_provider_no_longer_takes_edits_the_card(self, noticed):
        stream = await noticed.stream()
        notification_id, message_id = await question_card(noticed)
        noticed.gateway.script("interaction", {"outcome": "unavailable", "reason_code": "expired"})

        inbox_id = noticed.gateway.store(interaction_event(f"notif:{notification_id}:no", message_id=message_id))
        await stream.nudge("invoke")
        await noticed.acknowledged(inbox_id)

        await noticed.gateway.wait_for(lambda: noticed.gateway.sent_operations("edit"))
        [edit] = noticed.gateway.sent_operations("edit")
        assert edit["target"]["message"] == {"id": message_id}
        assert "✅ Answered: no" in text_of(edit["content"])

    async def test_a_press_on_another_element_is_rejected(self, noticed):
        stream = await noticed.stream()
        inbox_id = noticed.gateway.store(interaction_event("preview_fixture", message_id="1700000500.000100"))

        await stream.nudge("invoke")

        assert (await noticed.acknowledged(inbox_id))[inbox_id] == "rejected"
        assert noticed.gateway.sent_operations("interaction") == []

    async def test_an_expired_card_is_edited_without_buttons(self, noticed):
        await noticed.stream()
        notification_id, message_id = await question_card(noticed)
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        await noticed.db.update_notification(notification_id, expires_at=past)

        assert await noticed.notifications.expire_stale() == 1

        await noticed.gateway.wait_for(lambda: noticed.gateway.sent_operations("edit"))
        [edit] = noticed.gateway.sent_operations("edit")
        assert edit["target"]["message"] == {"id": message_id}
        assert "Expired unanswered" in text_of(edit["content"])
        assert not any(part["kind"] == "actions" for part in edit["content"])

    async def test_without_a_notification_conversation_nothing_is_posted(self, noticed):
        await noticed.stream()
        noticed.config.notifications.slack_channel_id = ""
        await noticed.gateway.wait_for(lambda: noticed.runtime.streams.connections("send"))

        await noticed.notifications.ask_question(session_id="s1", title="Deploy now?", options=["yes"])

        assert noticed.gateway.sent_operations("send") == []
