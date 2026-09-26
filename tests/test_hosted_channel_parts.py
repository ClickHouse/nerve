"""Hosted channel parts that need no stream: content, stream choice, and caches."""

from __future__ import annotations

import uuid
from typing import Any

from nerve.channels.hosted.channel import render_content
from nerve.channels.hosted.contract import ContentPart
from nerve.channels.hosted.contract.wire import decode_record
from nerve.channels.hosted.intake import Backoff, DuplicateCache
from nerve.channels.hosted.manager import StreamManager


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
