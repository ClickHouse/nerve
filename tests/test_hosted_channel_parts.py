"""Hosted channel parts that need no stream: content, stream choice, caches,
the text, reaction, and button helpers of outbound operations, and the
extraction of attachments."""

from __future__ import annotations

import asyncio
import io
import uuid
import zipfile
from typing import Any

import pytest

from nerve.channels.hosted import attachments as hosted_attachments
from nerve.channels.hosted.attachments import extract_attachments
from nerve.channels.hosted.channel import render_content
from nerve.channels.hosted.contract import ContentPart
from nerve.channels.hosted.contract.model import (
    MAX_ACTION_ELEMENTS,
    MAX_IDENTIFIER_BYTES,
    AttachmentReference,
)
from nerve.channels.hosted.contract.wire import decode_record
from nerve.channels.hosted.intake import Backoff, DuplicateCache
from nerve.channels.hosted import stream as stream_module
from nerve.channels.hosted.manager import StreamManager
from nerve.channels.hosted.runtime import RECEIVE_LIMITS
from nerve.channels.hosted.outbound import (
    file_name,
    notification_actions,
    notification_answer,
    parse_target,
    reaction_name,
    split_text,
    truncate_text,
)
from nerve.channels.slack_presentation import approval_style, slack_emoji_by_name


def text(body: str, format: str = "markdown") -> dict[str, Any]:
    return {"kind": "text", "text": {"format": format, "body": body}}


def reference(**members: str) -> dict[str, Any]:
    return {"kind": "reference", "reference": members}


# The content of the gateway's sample events.
LINKS_AND_MENTIONS = [
    reference(kind="mention", mention_kind="user", id="U_FIXTURE_AGENT", label="Nerve"),
    text(" please summarize "),
    reference(kind="link", url="https://example.com/release-notes", label="the release notes"),
    text(" for "),
    reference(kind="mention", mention_kind="broadcast", id="here"),
    text(" and post it in "),
    reference(kind="mention", mention_kind="conversation", id="C_FIXTURE_ANNOUNCE", label="announcements"),
    reference(kind="unsupported", id="slack.canvas", label="[Canvas: Release checklist]"),
]
MESSAGE = [
    text("Could someone review the deployment notes?"),
    reference(kind="mention", mention_kind="user", id="U_FIXTURE_REVIEWER", label="Reviewer"),
]
MESSAGE_UTF8 = [
    text("Déploiement prêt — 東京 🚀", format="plain"),
    reference(kind="mention", mention_kind="user", id="U_FIXTURE_MEMBER", label="Réviseur 🧪"),
]


def content(parts: list[dict[str, Any]]) -> tuple[ContentPart, ...]:
    return tuple(decode_record(ContentPart, part) for part in parts)


class TestRenderContent:
    def test_mentions_links_and_unsupported_content_become_text(self):
        rendered = render_content(content(LINKS_AND_MENTIONS), "U_FIXTURE_AGENT")

        assert rendered == (
            "please summarize the release notes (https://example.com/release-notes) "
            "for @here and post it in #announcements[Canvas: Release checklist]"
        )

    def test_the_agent_mention_stays_when_the_agent_is_unknown(self):
        assert render_content(content(LINKS_AND_MENTIONS)).startswith("@Nerve please summarize")

    def test_another_member_is_named_by_label(self):
        assert render_content(content(MESSAGE), "U_FIXTURE_AGENT") == (
            "Could someone review the deployment notes?@Reviewer"
        )

    def test_multibyte_text_is_kept(self):
        assert render_content(content(MESSAGE_UTF8)) == "Déploiement prêt — 東京 🚀@Réviseur 🧪"


class _Stream:
    def __init__(self, stream_id: int, *, draining: bool = False, room: bool = True, ready: bool = True):
        self.id = stream_id
        self.draining = draining
        self.ready = ready
        self._room = room
        self.capabilities: dict = {}

    def has_capacity(self) -> bool:
        return self.ready and self._room


def manager(*streams: _Stream) -> StreamManager:
    subject = StreamManager(
        verifier=None, receive_limits=None, max_streams=8,
    )
    subject._streams = {stream.id: stream for stream in streams}
    return subject


class TestPreferredStream:
    def test_the_oldest_ready_stream_is_preferred(self):
        assert manager(_Stream(2), _Stream(1)).preferred().id == 1

    def test_a_draining_stream_is_passed_over(self):
        assert manager(_Stream(1, draining=True), _Stream(2)).preferred().id == 2

    def test_a_full_stream_is_passed_over(self):
        assert manager(_Stream(1, room=False), _Stream(2)).preferred().id == 2

    def test_a_draining_stream_serves_when_no_other_stream_is_open(self):
        assert manager(_Stream(1, draining=True)).preferred().id == 1

    def test_no_draining_stream_while_a_steady_one_is_only_full(self):
        assert manager(_Stream(1, draining=True), _Stream(2, room=False)).preferred() is None

    def test_a_stream_that_has_not_negotiated_is_not_used(self):
        assert manager(_Stream(1, ready=False)).preferred() is None


class TestCaches:
    def test_the_duplicate_cache_forgets_the_oldest_entry(self):
        cache = DuplicateCache(2)
        keys = [(uuid.UUID(int=1), str(n)) for n in range(3)]
        for key in keys:
            cache.add(key)

        assert keys[0] not in cache
        assert keys[1] in cache and keys[2] in cache

    def test_backoff_grows_with_jitter_and_stops_at_its_maximum(self):
        backoff = Backoff(1.0, 4.0, rng=lambda: 1.0)

        assert [backoff.next() for _ in range(4)] == [1.0, 2.0, 4.0, 4.0]
        backoff.reset()
        assert Backoff(1.0, 4.0, rng=lambda: 0.0).next() == 0.5
        assert backoff.next() == 1.0

    def test_backoff_stays_at_its_maximum_after_any_number_of_failures(self):
        backoff = Backoff(0.5, 30.0, rng=lambda: 1.0)

        delays = [backoff.next() for _ in range(5000)]

        assert max(delays) == delays[-1] == 30.0


class TestOutboundHelpers:
    def test_text_is_split_at_line_breaks_then_spaces_then_the_limit(self):
        text = "alpha beta gamma\ndelta epsilon\n" + "z" * 25

        assert split_text(text, 12) == [
            "alpha beta", "gamma", "delta", "epsilon", "z" * 12, "z" * 12, "z",
        ]

    def test_the_limit_counts_code_points(self):
        assert [len(part) for part in split_text("é" * 30, 12)] == [12, 12, 6]
        assert split_text("   \n  ", 3) == []
        assert truncate_text("abcdef", 4) == "abc…"

    def test_reaction_names(self):
        names = slack_emoji_by_name()

        assert reaction_name("eyes", names) == "\U0001f440"
        assert reaction_name(":heart:", names) == "❤️"
        assert reaction_name("❤", names) == "❤️"
        assert reaction_name("🦜", names) == "🦜"
        assert reaction_name("party-parrot", names) == "custom:party-parrot"
        assert reaction_name("two words", names) is None
        assert reaction_name("🦜 🦜", names) is None
        assert reaction_name("x" * MAX_IDENTIFIER_BYTES, names) is None

    def test_a_target_with_an_id_that_the_gateway_refuses_names_nothing(self):
        assert parse_target("C1:1.2") == ("C1", "1.2")
        assert parse_target("C1") == ("C1", None)
        for target in ("", "  ", "C1\n", "C1:1.2\t", "C" * (MAX_IDENTIFIER_BYTES + 1)):
            assert parse_target(target) == ("", None)

    def test_an_upload_name_has_no_control_characters_and_fits(self):
        assert file_name("report\n.pdf") == "report_.pdf"
        assert len(file_name("é" * 200).encode()) <= MAX_IDENTIFIER_BYTES
        assert file_name("\u00a0") == "file"


class TestNotificationButtons:
    def test_each_option_is_a_button_that_carries_its_answer(self):
        [part] = notification_actions("n1", [("✅ Approve", "approve"), ("Later", "later")], approval_style)

        assert [
            (element.action_id, element.label, element.value, element.style)
            for element in part.actions.elements
        ] == [("notif:n1:approve", "✅ Approve", "approve", "primary"), ("notif:n1:later", "Later", "later", "")]

    def test_an_answer_that_does_not_fit_gets_no_button(self):
        long_value = "x" * MAX_IDENTIFIER_BYTES

        [part] = notification_actions("n1", [("Long", long_value), ("Short", "short")])

        assert [element.value for element in part.actions.elements] == ["short"]
        assert notification_actions("n1", [("Long", long_value)]) == ()

    def test_labels_lose_control_characters_and_answers_with_them_get_no_button(self):
        [part] = notification_actions("n1", [("Two\nlines", "yes"), ("Tab", "a\tb")])

        assert [(element.label, element.value) for element in part.actions.elements] == [("Two lines", "yes")]

    def test_at_most_the_element_limit_is_kept(self):
        options = [(f"Option {n}", f"o{n}") for n in range(MAX_ACTION_ELEMENTS + 3)]

        [part] = notification_actions("n1", options)

        assert len(part.actions.elements) == MAX_ACTION_ELEMENTS

    def test_a_press_names_its_notification_and_answer(self):
        assert notification_answer("notif:n1:yes", ()) == ("n1", "yes")
        assert notification_answer("notif:n1:yes", ("chosen",)) == ("n1", "chosen")
        assert notification_answer("notif:n1:a:b", ()) == ("n1", "a:b")
        assert notification_answer("notif:n1", ()) is None
        assert notification_answer("preview:n1:yes", ()) is None


def _reader(files: dict[str, bytes], reads: list[tuple[str, int]]):
    async def read(attachment: AttachmentReference, length: int) -> bytes | None:
        reads.append((attachment.id, length))
        data = files.get(attachment.id)
        return None if data is None else data[:length]
    return read


@pytest.mark.asyncio
class TestAttachmentExtraction:
    async def test_a_zip_is_unpacked_and_an_unknown_type_is_not_read(self):
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as zipped:
            zipped.writestr("inside.txt", "packed text")
        reads: list[tuple[str, int]] = []

        text, blocks = await extract_attachments((
            AttachmentReference(id="F_ZIP", name="bundle.zip", size_bytes=len(archive.getvalue())),
            AttachmentReference(id="F_BIN", name="data.bin", size_bytes=10),
        ), _reader({"F_ZIP": archive.getvalue(), "F_BIN": b"0" * 10}, reads))

        assert "packed text" in text
        assert "[File: data.bin (0 KB, unknown type)]" in text
        assert blocks == []
        assert [read[0] for read in reads] == ["F_ZIP"]

    async def test_a_file_above_the_limit_is_refused_without_a_read(self):
        reads: list[tuple[str, int]] = []

        text, _ = await extract_attachments((
            AttachmentReference(
                id="F_PDF", name="big.pdf", size_bytes=hosted_attachments.MAX_FILE_BYTES + 1,
            ),
        ), _reader({}, reads))

        assert text.endswith("(Too large or not downloadable)")
        assert reads == []

    async def test_later_files_are_skipped_past_the_message_budget(self, monkeypatch):
        monkeypatch.setattr(hosted_attachments, "MAX_MESSAGE_BYTES", 15)
        reads: list[tuple[str, int]] = []

        text, _ = await extract_attachments((
            AttachmentReference(id="F1", name="one.txt", size_bytes=10),
            AttachmentReference(id="F2", name="two.txt", size_bytes=10),
        ), _reader({"F1": b"1" * 10, "F2": b"2" * 10}, reads))

        assert reads == [("F1", 10)]
        assert "[File: two.txt (0 KB, unknown type)]\n(Skipped: the message's files are too large together)" in text


class _SlowSocket:
    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.sent: list[str] = []
        self.closed = False

    async def send_text(self, text: str) -> None:
        await asyncio.sleep(self.seconds)
        self.sent.append(text)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed = True


class _Listener:
    def stream_changed(self, stream) -> None:
        pass


def _stream(socket: _SlowSocket) -> stream_module.ChannelStream:
    return stream_module.ChannelStream(
        socket, stream_id=1, receive_limits=RECEIVE_LIMITS, listener=_Listener(),
    )


@pytest.mark.asyncio
class TestCancelledWrite:
    async def test_a_cancelled_sender_lets_its_frame_finish_and_keeps_the_stream(self):
        socket = _SlowSocket(0.05)
        stream = _stream(socket)

        task = asyncio.create_task(stream._send_locked("frame"))
        await asyncio.sleep(0.01)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert socket.sent == ["frame"]
        assert stream.state != "closed"

    async def test_a_write_cancelled_by_itself_is_a_closed_stream_for_the_caller(self):
        socket = _SlowSocket(5.0)
        stream = _stream(socket)
        writes: list[asyncio.Task] = []
        original = asyncio.ensure_future

        def spy(awaitable):
            task = original(awaitable)
            writes.append(task)
            return task

        stream_module.asyncio.ensure_future = spy
        try:
            sender = asyncio.create_task(stream._send_locked("frame"))
            await asyncio.sleep(0.01)
            writes[0].cancel()
            with pytest.raises(stream_module.StreamClosed):
                await sender
        finally:
            stream_module.asyncio.ensure_future = original
        assert stream.state == "closed"

    async def test_a_frame_that_does_not_finish_in_time_closes_the_stream(self, monkeypatch):
        monkeypatch.setattr(stream_module, "_WRITE_FINISH_SECONDS", 0.02)
        socket = _SlowSocket(5.0)
        stream = _stream(socket)

        task = asyncio.create_task(stream._send_locked("frame"))
        await asyncio.sleep(0.01)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.state == "closed"
        assert socket.sent == []
