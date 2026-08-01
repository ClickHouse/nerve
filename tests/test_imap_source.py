"""Unit tests for :class:`ImapSource`.

These tests avoid the network entirely. They exercise:
  * cursor parse/format helpers and template formatting,
  * MIME part extraction + the configured match rules,
  * the async record builders (image pass success, failure → fallback,
    match with no image → fallback, plain email),
by stubbing the blocking ``_imap_fetch_blocking`` step and the vision client.

The scenario used throughout is a mailbox where some senders deliver their
payload only as an inline scan, so the model has to read the image.
"""

from __future__ import annotations

import base64
import json
from email.message import EmailMessage

import pytest

from nerve.config import ImapMatchConfig, ImapVisionConfig
from nerve.sources.imap import (
    ImapSource,
    _decode_hdr,
    _extract_parts,
    _extract_status_int,
    _first_line_after,
    _first_nonempty_line,
    _format,
    _parse_cursor,
)

# 1x1 transparent PNG (valid, tiny) for the image tests.
_PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
    "+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
)

_MATCH = ImapMatchConfig(
    sender_contains=["scans@example.net"],
    attachment_contains=["scan"],
)

_VISION = ImapVisionConfig(
    enabled=True,
    model="claude-haiku-4-5-20251001",
    prompt="Read the scan. Answer with one line:\nSender: <name>",
    answer_key="Sender:",
    unknown_answer="unreadable",
    summary="[{label}] scan from {answer}",
    content="{vision}\n\nSubject: {subject}",
    summary_unknown="[{label}] unreadable scan",
    content_unknown="The image could not be read.\n\nSubject: {subject}",
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
    text = "Sender: Acme GmbH\nType: invoice"
    assert _first_line_after(text, "Sender:") == "Acme GmbH"
    assert _first_line_after(text, "Nope:") == ""


def test_first_nonempty_line():
    assert _first_nonempty_line("\n\n  Acme GmbH \nrest") == "Acme GmbH"
    assert _first_nonempty_line("   \n\n") == ""


def test_format_survives_a_bad_placeholder():
    """A typo in configured wording must not lose the whole fetch batch."""
    fields = {"label": "post", "answer": "Acme GmbH"}
    assert _format("[{label}] {answer}", fields) == "[post] Acme GmbH"
    # Unknown key: template comes back verbatim instead of raising.
    assert _format("[{label}] {nope}", fields) == "[{label}] {nope}"


# ---------------------------------------------------------------------------
# MIME extraction + match rules
# ---------------------------------------------------------------------------

def _plain_email(from_addr: str, subject: str, body: str) -> EmailMessage:
    m = EmailMessage()
    m["From"] = from_addr
    m["Subject"] = subject
    m["Date"] = "Sat, 20 Jun 2026 13:34:40 +0000"
    m.set_content(body)
    return m


def _email_with_image(
    from_addr: str, *, cid: str, filename: str, payload: bytes = _PNG_BYTES,
) -> EmailMessage:
    m = _plain_email(from_addr, "Your document", "See attached")
    m.add_related(
        payload, maintype="image", subtype="png", cid=cid, filename=filename,
    )
    return m


def test_extract_parts_plain_text():
    m = _plain_email("a@b.com", "Hi", "Hello world")
    body, image, mt, hit = _extract_parts(m, _MATCH.attachment_contains)
    assert "Hello world" in body
    assert image is None and mt is None and hit is False


def test_extract_parts_hinted_image_by_cid():
    m = _email_with_image(
        "noreply@example.net", cid="<scan_12345>", filename="page1.png",
    )
    _body, image, mt, hit = _extract_parts(m, _MATCH.attachment_contains)
    assert image == _PNG_BYTES
    assert mt == "image/png"
    assert hit is True


def test_extract_parts_image_without_hint_is_not_a_hit():
    """An image still comes back as a candidate, but it does not match."""
    m = _email_with_image(
        "noreply@example.net", cid="<logo_1>", filename="logo.png",
    )
    _body, image, _mt, hit = _extract_parts(m, _MATCH.attachment_contains)
    assert image == _PNG_BYTES  # largest-image fallback candidate
    assert hit is False


def test_extract_parts_with_no_rules_never_hits():
    m = _email_with_image(
        "noreply@example.net", cid="<scan_12345>", filename="scan.png",
    )
    _body, _image, _mt, hit = _extract_parts(m, [])
    assert hit is False


# ---------------------------------------------------------------------------
# Match evaluation through the real parse path
# ---------------------------------------------------------------------------

class _FakeIMAP:
    """Minimal stand-in for imaplib.IMAP4_SSL.uid("fetch", ...)."""

    def __init__(self, message: EmailMessage):
        self._raw = message.as_bytes()

    def uid(self, command, *args):
        assert command == "fetch"
        return "OK", [(b"1 (RFC822 {n}", self._raw)]


def _parse(src: ImapSource, message: EmailMessage) -> dict:
    return src._fetch_one(_FakeIMAP(message), 7, 99)


def test_match_by_sender():
    src = _src()
    parsed = _parse(src, _plain_email("Scans <scans@example.net>", "Doc", "x"))
    assert parsed["matched"] is True
    assert parsed["id"] == "99-7"


def test_match_by_sender_is_case_insensitive():
    src = _src()
    parsed = _parse(src, _plain_email("SCANS@EXAMPLE.NET", "Doc", "x"))
    assert parsed["matched"] is True


def test_match_by_attachment_hint():
    src = _src()
    parsed = _parse(
        src,
        _email_with_image("other@elsewhere.org", cid="<scan_1>", filename="a.png"),
    )
    assert parsed["matched"] is True
    assert parsed["image"] == _PNG_BYTES


def test_unmatched_message():
    src = _src()
    parsed = _parse(src, _plain_email("newsletter@elsewhere.org", "Ad", "buy"))
    assert parsed["matched"] is False


def test_no_rules_configured_matches_nothing():
    """Empty match config = plain IMAP source, whatever the mail looks like."""
    src = _src(match=ImapMatchConfig())
    parsed = _parse(
        src,
        _email_with_image("scans@example.net", cid="<scan_1>", filename="scan.png"),
    )
    assert parsed["matched"] is False


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
    kw.setdefault("match", _MATCH)
    kw.setdefault("vision", _VISION)
    return ImapSource(
        host="h", username="u@x", password="p", label="scans", **kw,
    )


def _matched_msg(**over) -> dict:
    msg = {
        "id": "1-2", "subject": "Your document", "from": "scans@example.net",
        "date": "d", "timestamp": "2026-06-20T00:00:00+00:00",
        "body": "b", "matched": True,
        "image": _PNG_BYTES, "media_type": "image/png",
    }
    msg.update(over)
    return msg


@pytest.mark.asyncio
async def test_vision_success():
    client = _FakeVisionClient(text="Sender: Acme GmbH\nType: invoice")
    src = _src(vision_client_factory=lambda: client)
    summary, content = await src._build_vision_record(_matched_msg())
    assert summary == "[scans] scan from Acme GmbH"
    assert "Acme GmbH" in content
    assert client.closed is True


@pytest.mark.asyncio
async def test_vision_failure_falls_back():
    client = _FakeVisionClient(raise_exc=True)
    src = _src(vision_client_factory=lambda: client)
    summary, content = await src._build_vision_record(_matched_msg())
    assert summary == "[scans] unreadable scan"
    assert "could not be read" in content


@pytest.mark.asyncio
async def test_match_without_image_falls_back():
    src = _src(vision_client_factory=lambda: _FakeVisionClient(text="x"))
    summary, _ = await src._build_vision_record(
        _matched_msg(image=None, media_type=None),
    )
    assert summary == "[scans] unreadable scan"


@pytest.mark.asyncio
async def test_empty_answer_key_takes_the_first_line():
    client = _FakeVisionClient(text="Acme GmbH\nsecond line")
    src = _src(
        vision=ImapVisionConfig(**{**_VISION.__dict__, "answer_key": ""}),
        vision_client_factory=lambda: client,
    )
    summary, _ = await src._build_vision_record(_matched_msg())
    assert summary == "[scans] scan from Acme GmbH"


@pytest.mark.asyncio
async def test_fetch_builds_records_from_stub(monkeypatch):
    """fetch() should route matched vs plain and produce SourceRecords."""
    client = _FakeVisionClient(text="Sender: Acme GmbH\nType: invoice")
    src = _src(vision_client_factory=lambda: client)

    stub_messages = [
        {
            "id": "9-1", "subject": "Welcome", "from": "hello@example.org",
            "date": "d", "timestamp": "2026-06-20T00:00:00+00:00", "body": "hi",
            "matched": False, "image": None, "media_type": None,
        },
        _matched_msg(id="9-2"),
    ]

    monkeypatch.setattr(
        src, "_imap_fetch_blocking", lambda cursor, limit: (stub_messages, "9:2"),
    )

    result = await src.fetch(cursor=None, limit=10)
    assert result.next_cursor == "9:2"
    assert len(result.records) == 2
    plain, matched = result.records
    assert plain.record_type == "imap_message"
    assert plain.metadata["matched"] is False
    assert "Welcome" in plain.summary
    assert matched.metadata["matched"] is True
    assert "Acme GmbH" in matched.summary


@pytest.mark.asyncio
async def test_vision_disabled_matched_message_uses_fallback(monkeypatch):
    """With the image pass off, a match still gets its configured wording."""
    src = _src(vision=ImapVisionConfig(**{**_VISION.__dict__, "enabled": False}))
    monkeypatch.setattr(
        src, "_imap_fetch_blocking",
        lambda cursor, limit: ([_matched_msg(id="9-2")], "9:2"),
    )
    result = await src.fetch(cursor=None, limit=10)
    assert result.records[0].summary == "[scans] unreadable scan"


@pytest.mark.asyncio
async def test_only_matched_drops_the_rest(monkeypatch):
    """only_matched=True keeps only matches but still advances the cursor."""
    client = _FakeVisionClient(text="Sender: Acme GmbH")
    src = _src(
        match=ImapMatchConfig(**{**_MATCH.__dict__, "only_matched": True}),
        vision_client_factory=lambda: client,
    )

    stub_messages = [
        {
            "id": "9-1", "subject": "Ad", "from": "newsletter@elsewhere.org",
            "date": "d", "timestamp": "2026-06-20T00:00:00+00:00", "body": "ad",
            "matched": False, "image": None, "media_type": None,
        },
        _matched_msg(id="9-2"),
    ]
    monkeypatch.setattr(
        src, "_imap_fetch_blocking", lambda cursor, limit: (stub_messages, "9:2"),
    )

    result = await src.fetch(cursor=None, limit=10)
    # Only the match survives; cursor still advances past the dropped ad.
    assert len(result.records) == 1
    assert result.records[0].metadata["matched"] is True
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
# Prompt and answer key are a pair
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_prompt_and_answer_key_move_together():
    """A reworded prompt must carry its answer_key along.

    The two are coupled: the prompt tells the model which label to emit and
    the parser reads the line after exactly that label.
    """
    vision = ImapVisionConfig.from_dict({
        "enabled": True,
        "model": "claude-haiku-4-5-20251001",
        "prompt": "Wer ist der Absender? Antworte: Absender: <name>",
        "answer_key": "Absender:",
        "unknown_answer": "unlesbar",
        "summary": "[{label}] Brief: {answer}",
    })
    client = _FakeVisionClient(text="Absender: Acme GmbH\nTyp: Rechnung")
    src = _src(vision=vision, vision_client_factory=lambda: client)

    summary, _content = await src._build_vision_record(_matched_msg())
    assert summary == "[scans] Brief: Acme GmbH"
    # The prompt actually sent is the configured one.
    assert "Absender" in json.dumps(client.last_kwargs, ensure_ascii=False)


@pytest.mark.asyncio
async def test_mismatched_answer_key_degrades_to_unknown():
    """The failure mode the coupling guards against, pinned explicitly."""
    vision = ImapVisionConfig.from_dict({
        "enabled": True,
        "model": "claude-haiku-4-5-20251001",
        "prompt": "Antworte: Absender: <name>",
        # answer_key deliberately left pointing at the old label
        "answer_key": "Sender:",
        "unknown_answer": "unlesbar",
        "summary": "[{label}] Brief: {answer}",
    })
    client = _FakeVisionClient(text="Absender: Acme GmbH")
    src = _src(vision=vision, vision_client_factory=lambda: client)

    summary, _ = await src._build_vision_record(_matched_msg())
    assert "unlesbar" in summary
