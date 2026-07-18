"""Unit tests for :class:`ImapSource`.

These tests avoid the network entirely. They exercise:
  * cursor parse/format helpers,
  * MIME part extraction + Deutsche Post envelope detection,
  * the async record builders (vision success, vision failure → fallback,
    envelope with no image → fallback, plain email),
by stubbing the blocking ``_imap_fetch_blocking`` step and the vision client.
"""

from __future__ import annotations

import base64
from email.message import EmailMessage

import json

import pytest

from nerve.sources.imap import (
    ImapSource,
    _decode_hdr,
    _extract_parts,
    _extract_status_int,
    _first_line_after,
    _parse_cursor,
)

# 1x1 transparent PNG (valid, tiny) for envelope image tests.
_PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
    "+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def test_parse_cursor():
    assert _parse_cursor("123:456") == (123, 456)
    assert _parse_cursor(None) == (0, None)
    assert _parse_cursor("garbage") == (0, None)
    assert _parse_cursor("123") == (0, None)


def test_extract_status_int():
    line = "INBOX (UIDVALIDITY 1781468017 UIDNEXT 42)"
    assert _extract_status_int(line, "UIDVALIDITY") == 1781468017
    assert _extract_status_int(line, "UIDNEXT") == 42
    assert _extract_status_int(line, "MISSING") == 0


def test_decode_hdr():
    # RFC 2047 encoded-word (UTF-8 base64 for "Grüße")
    assert _decode_hdr("=?UTF-8?B?R3LDvMOfZQ==?=") == "Grüße"
    assert _decode_hdr(None) == ""
    assert _decode_hdr("plain") == "plain"


def test_first_line_after():
    text = "Sender: Allianz\nType: insurance"
    assert _first_line_after(text, "Sender:") == "Allianz"
    assert _first_line_after(text, "Nope:") == ""


# ---------------------------------------------------------------------------
# MIME extraction + envelope detection
# ---------------------------------------------------------------------------

def _plain_email(from_addr: str, subject: str, body: str) -> EmailMessage:
    m = EmailMessage()
    m["From"] = from_addr
    m["Subject"] = subject
    m["Date"] = "Sat, 20 Jun 2026 13:34:40 +0000"
    m.set_content(body)
    return m


def test_extract_parts_plain_text():
    m = _plain_email("a@b.com", "Hi", "Hello world")
    body, png, mt, hint = _extract_parts(m)
    assert "Hello world" in body
    assert png is None and mt is None and hint is False


def test_extract_parts_envelope_image_by_cid():
    m = EmailMessage()
    m["From"] = "Deutsche Post <noreply@example.net>"
    m["Subject"] = "Briefankündigung"
    m["Date"] = "Sat, 20 Jun 2026 13:34:40 +0000"
    m.set_content("Ihre Briefankündigung")
    m.add_related(
        _PNG_BYTES, maintype="image", subtype="png",
        cid="<Umschlag_12345>", filename="Umschlag.png",
    )
    body, png, mt, hint = _extract_parts(m)
    assert png == _PNG_BYTES
    assert mt == "image/png"
    assert hint is True


# ---------------------------------------------------------------------------
# Record builders (async) with stubbed vision
# ---------------------------------------------------------------------------

class _FakeContentBlock:
    def __init__(self, text):
        self.text = text


class _FakeResp:
    def __init__(self, text):
        self.content = [_FakeContentBlock(text)]


class _FakeVisionClient:
    def __init__(self, text=None, raise_exc=False):
        self._text = text
        self._raise = raise_exc
        self.closed = False
        self.last_kwargs = None

        class _Messages:
            async def create(_self, **kwargs):
                self.last_kwargs = kwargs
                if self._raise:
                    raise RuntimeError("vision boom")
                return _FakeResp(self._text)

        self.messages = _Messages()

    async def close(self):
        self.closed = True


def _src(**kw) -> ImapSource:
    return ImapSource(
        host="h", username="u@x", password="p", label="post",
        vision_model="claude-haiku-4-5-20251001", **kw,
    )


@pytest.mark.asyncio
async def test_envelope_vision_success():
    client = _FakeVisionClient(text="Sender: Allianz\nType: insurance")
    src = _src(vision_client_factory=lambda: client)
    msg = {
        "id": "1-2", "subject": "Briefankündigung", "from": "Deutsche Post",
        "date": "d", "timestamp": "2026-06-20T00:00:00+00:00",
        "body": "b", "is_envelope": True,
        "envelope_png": _PNG_BYTES, "envelope_media_type": "image/png",
    }
    summary, content = await src._build_envelope_record(msg)
    assert "Allianz" in summary
    assert "📬" in summary
    assert "Allianz" in content
    assert client.closed is True


@pytest.mark.asyncio
async def test_envelope_vision_failure_falls_back():
    client = _FakeVisionClient(raise_exc=True)
    src = _src(vision_client_factory=lambda: client)
    msg = {
        "id": "1-2", "subject": "Briefankündigung", "from": "Deutsche Post",
        "date": "d", "timestamp": "2026-06-20T00:00:00+00:00",
        "body": "b", "is_envelope": True,
        "envelope_png": _PNG_BYTES, "envelope_media_type": "image/png",
    }
    summary, content = await src._build_envelope_record(msg)
    assert "physical letter is on its way" in summary
    assert "could not be read" in content


@pytest.mark.asyncio
async def test_envelope_without_image_falls_back():
    src = _src(vision_client_factory=lambda: _FakeVisionClient(text="x"))
    msg = {
        "id": "1-2", "subject": "Briefankündigung", "from": "Deutsche Post",
        "date": "d", "timestamp": "2026-06-20T00:00:00+00:00",
        "body": "b", "is_envelope": True,
        "envelope_png": None, "envelope_media_type": None,
    }
    summary, content = await src._build_envelope_record(msg)
    assert "physical letter is on its way" in summary


@pytest.mark.asyncio
async def test_fetch_builds_records_from_stub(monkeypatch):
    """fetch() should route envelope vs plain and produce SourceRecords."""
    client = _FakeVisionClient(text="Sender: Allianz\nType: insurance")
    src = _src(vision_client_factory=lambda: client)

    stub_messages = [
        {
            "id": "9-1", "subject": "Welcome", "from": "gmx@x", "date": "d",
            "timestamp": "2026-06-20T00:00:00+00:00", "body": "hi",
            "is_envelope": False, "envelope_png": None,
            "envelope_media_type": None,
        },
        {
            "id": "9-2", "subject": "Briefankündigung", "from": "Deutsche Post",
            "date": "d", "timestamp": "2026-06-20T00:00:00+00:00", "body": "b",
            "is_envelope": True, "envelope_png": _PNG_BYTES,
            "envelope_media_type": "image/png",
        },
    ]

    def _fake_blocking(cursor, limit):
        return stub_messages, "9:2"

    monkeypatch.setattr(src, "_imap_fetch_blocking", _fake_blocking)

    result = await src.fetch(cursor=None, limit=10)
    assert result.next_cursor == "9:2"
    assert len(result.records) == 2
    plain, envelope = result.records
    assert plain.record_type == "imap_message"
    assert plain.metadata["is_envelope"] is False
    assert "Welcome" in plain.summary
    assert envelope.metadata["is_envelope"] is True
    assert "Allianz" in envelope.summary


@pytest.mark.asyncio
async def test_envelope_only_drops_non_envelope(monkeypatch):
    """envelope_only=True keeps only envelope records but still advances cursor."""
    client = _FakeVisionClient(text="Sender: Allianz\nType: insurance")
    src = _src(vision_client_factory=lambda: client, envelope_only=True)

    stub_messages = [
        {
            "id": "9-1", "subject": "GMX Werbung", "from": "gmx@x", "date": "d",
            "timestamp": "2026-06-20T00:00:00+00:00", "body": "ad",
            "is_envelope": False, "envelope_png": None,
            "envelope_media_type": None,
        },
        {
            "id": "9-2", "subject": "Ein Brief ist unterwegs", "from": "Deutsche Post",
            "date": "d", "timestamp": "2026-06-20T00:00:00+00:00", "body": "b",
            "is_envelope": True, "envelope_png": _PNG_BYTES,
            "envelope_media_type": "image/png",
        },
    ]
    monkeypatch.setattr(
        src, "_imap_fetch_blocking", lambda cursor, limit: (stub_messages, "9:2"),
    )

    result = await src.fetch(cursor=None, limit=10)
    # Only the envelope survives; cursor still advances past the dropped ad.
    assert len(result.records) == 1
    assert result.records[0].metadata["is_envelope"] is True
    assert result.next_cursor == "9:2"


@pytest.mark.asyncio
async def test_fetch_swallows_blocking_errors(monkeypatch):
    src = _src(vision_client_factory=lambda: _FakeVisionClient(text="x"))

    def _boom(cursor, limit):
        raise RuntimeError("imap down")

    monkeypatch.setattr(src, "_imap_fetch_blocking", _boom)
    result = await src.fetch(cursor="5:5", limit=10)
    assert result.records == []
    assert result.next_cursor == "5:5"  # cursor preserved on error


# ---------------------------------------------------------------------------
# Envelope wording is configuration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_envelope_catalog_is_configurable():
    """A translated catalog must carry prompt AND parse key together.

    These two are coupled: the prompt tells the model which label to emit and
    the parser reads the line after exactly that label. Overriding only one
    yields unknown_sender for every letter, so the test pins them moving as a
    pair.
    """
    from nerve.config import ImapEnvelopeMessagesConfig

    catalog = ImapEnvelopeMessagesConfig.from_dict({
        "vision_prompt": "Wer ist der Absender? Antworte: Absender: <name>",
        "sender_key": "Absender:",
        "unknown_sender": "unlesbar",
        "summary": "[{label}] Brief: {sender}",
    })
    client = _FakeVisionClient(text="Absender: Allianz\nTyp: Versicherung")
    src = _src(vision_client_factory=lambda: client, envelope_messages=catalog)
    msg = {
        "id": "1-2", "subject": "Briefankündigung", "from": "Deutsche Post",
        "date": "d", "timestamp": "2026-06-20T00:00:00+00:00",
        "body": "b", "is_envelope": True,
        "envelope_png": _PNG_BYTES, "envelope_media_type": "image/png",
    }
    summary, _content = await src._build_envelope_record(msg)
    assert summary == "[post] Brief: Allianz"
    # The prompt actually sent is the configured one.
    assert "Absender" in json.dumps(client.last_kwargs, ensure_ascii=False)


@pytest.mark.asyncio
async def test_mismatched_key_degrades_to_unknown():
    """The failure mode the coupling guards against, pinned explicitly."""
    from nerve.config import ImapEnvelopeMessagesConfig

    catalog = ImapEnvelopeMessagesConfig.from_dict({
        "vision_prompt": "Antworte: Absender: <name>",
        # sender_key deliberately left at the English default
        "unknown_sender": "unlesbar",
    })
    client = _FakeVisionClient(text="Absender: Allianz")
    src = _src(vision_client_factory=lambda: client, envelope_messages=catalog)
    msg = {
        "id": "1-2", "subject": "s", "from": "Deutsche Post",
        "date": "d", "timestamp": "2026-06-20T00:00:00+00:00",
        "body": "b", "is_envelope": True,
        "envelope_png": _PNG_BYTES, "envelope_media_type": "image/png",
    }
    summary, _ = await src._build_envelope_record(msg)
    assert "unlesbar" in summary
