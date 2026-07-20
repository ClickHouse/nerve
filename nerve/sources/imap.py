"""Generic IMAP source — fetches emails from any IMAP server.

Each ImapSource instance handles ONE mailbox with its own cursor. The
registry creates one instance per configured account, so each gets
independent cursor tracking in the DB.

Primary use case: Deutsche Post "Briefankündigung" — a free German service
that emails a daily preview scan of the FRONT of envelopes that will be
delivered soon. The letter contents are NOT scanned; the sender is only
visible in the inline envelope image. For those emails we run a multimodal
vision pass (Haiku) at ingest time to read the sender off the envelope, so
the inbox-processor cron gets plain text it can act on. If the image is
missing or vision fails, we fall back to "a physical letter is on its way".

Cursor semantics: ``<UIDVALIDITY>:<max_uid>``. IMAP UIDs are only stable
within a given UIDVALIDITY; if the server resets it, we detect the mismatch
and re-baseline from a SINCE lookback window instead of trusting old UIDs.

imaplib is blocking, so the whole IMAP conversation runs in a worker thread
via ``asyncio.to_thread``. The vision enrichment is async (AsyncAnthropic).
"""

from __future__ import annotations

import asyncio
import email
import imaplib
import logging
from datetime import datetime, timedelta, timezone
from email.header import decode_header, make_header
from email.message import Message
from typing import Any, Callable

from nerve.sources.base import Source
from nerve.sources.gmail import _html_to_text, _parse_to_epoch
from nerve.sources.models import FetchResult, SourceRecord

logger = logging.getLogger(__name__)

# Deutsche Post Briefankündigung fingerprints. Match on sender domain OR an
# inline image whose Content-ID / filename mentions the envelope ("Umschlag").
_DP_SENDER_HINTS = (
    "deutschepost",
    "brief.deutschepost",
    "briefankuendigung",
    "briefankündigung",
)
_ENVELOPE_IMG_HINTS = ("umschlag", "envelope", "briefankuend")



class ImapSource(Source):
    """Generic IMAP mailbox source for a single account."""

    def __init__(
        self,
        *,
        host: str,
        username: str,
        password: str,
        label: str,
        port: int = 993,
        mailbox: str = "INBOX",
        analyze_envelopes: bool = True,
        envelope_only: bool = False,
        initial_lookback_days: int = 1,
        vision_model: str = "",
        vision_client_factory: Callable[[], Any] | None = None,
        envelope_messages: Any | None = None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self._password = password
        self.mailbox = mailbox
        self.label = label
        self.source_name = f"imap:{label}"
        self.analyze_envelopes = analyze_envelopes
        # When True, only Deutsche Post envelope-scan announcements are emitted;
        # all other mail (ads, spam, provider system mail) is dropped at the
        # source and never reaches the inbox.
        self.envelope_only = envelope_only
        self.initial_lookback_days = max(1, int(initial_lookback_days))
        self.vision_model = vision_model
        self._vision_client_factory = vision_client_factory
        # Envelope wording (prompt, parse key, summaries). Defaults are the
        # English catalog; the operator overrides them under
        # sync.imap.envelope_messages.
        if envelope_messages is None:
            from nerve.config import ImapEnvelopeMessagesConfig

            envelope_messages = ImapEnvelopeMessagesConfig()
        self.envelope_messages = envelope_messages

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Fetch new messages since cursor and enrich Deutsche Post envelopes."""
        try:
            parsed, next_cursor = await asyncio.to_thread(
                self._imap_fetch_blocking, cursor, limit,
            )
        except Exception as e:
            logger.error("IMAP error for %s: %s", self.source_name, e)
            return FetchResult(records=[], next_cursor=cursor, has_more=False)

        records: list[SourceRecord] = []
        for msg in parsed:
            # Envelope-only mode: drop everything that isn't a physical-letter
            # announcement before it ever reaches the inbox.
            if self.envelope_only and not msg.get("is_envelope"):
                logger.debug(
                    "IMAP %s: dropping non-envelope message %s (envelope_only)",
                    self.source_name, msg.get("id"),
                )
                continue
            if msg.get("is_envelope") and self.analyze_envelopes:
                summary, content = await self._build_envelope_record(msg)
            elif msg.get("is_envelope"):
                summary, content = self._envelope_fallback(msg)
            else:
                summary, content = self._plain_record(msg)

            records.append(SourceRecord(
                id=msg["id"],
                source=self.source_name,
                record_type="imap_message",
                summary=summary,
                content=content,
                timestamp=msg["timestamp"],
                metadata={
                    "account": self.username,
                    "label": self.label,
                    "mailbox": self.mailbox,
                    "is_envelope": bool(msg.get("is_envelope")),
                    "from": msg.get("from", ""),
                },
            ))

        return FetchResult(records=records, next_cursor=next_cursor, has_more=False)

    # ------------------------------------------------------------------
    # Blocking IMAP conversation (runs in a worker thread)
    # ------------------------------------------------------------------

    def _imap_fetch_blocking(
        self, cursor: str | None, limit: int,
    ) -> tuple[list[dict], str | None]:
        """Connect, select the mailbox, and parse new messages.

        Returns (parsed_messages, next_cursor). Runs entirely in a worker
        thread — no async here.
        """
        M = imaplib.IMAP4_SSL(self.host, self.port)
        try:
            M.login(self.username, self._password)
            M.select(self.mailbox, readonly=True)

            uidvalidity, uidnext = self._mailbox_status(M)

            prev_validity, last_uid = _parse_cursor(cursor)
            incremental = (
                cursor is not None
                and prev_validity == uidvalidity
                and last_uid is not None
            )

            if incremental:
                typ, data = M.uid("search", None, f"{last_uid + 1}:*")
            else:
                since = (
                    datetime.now(timezone.utc)
                    - timedelta(days=self.initial_lookback_days)
                ).strftime("%d-%b-%Y")
                typ, data = M.uid("search", None, "SINCE", since)

            if typ != "OK" or not data or not data[0]:
                # Nothing matched. Baseline the cursor so we don't re-scan.
                baseline = last_uid if incremental else (uidnext - 1 if uidnext else 0)
                return [], f"{uidvalidity}:{max(0, baseline)}"

            uids = [int(u) for u in data[0].split()]
            # The `N:*` range trick always returns at least the highest UID even
            # when nothing is newer — filter defensively against the cursor.
            if incremental and last_uid is not None:
                uids = [u for u in uids if u > last_uid]
            uids.sort()
            if not uids:
                return [], f"{uidvalidity}:{last_uid}" if last_uid is not None else \
                    f"{uidvalidity}:{max(0, (uidnext - 1) if uidnext else 0)}"

            # Cap to the newest `limit` messages, but track the true max UID so
            # the cursor advances past everything we saw.
            max_uid = uids[-1]
            if len(uids) > limit:
                uids = uids[-limit:]

            parsed: list[dict] = []
            for uid in uids:
                try:
                    parsed.append(self._fetch_one(M, uid, uidvalidity))
                except Exception as e:
                    logger.warning(
                        "IMAP %s: failed to parse uid %d: %s",
                        self.source_name, uid, e,
                    )

            return parsed, f"{uidvalidity}:{max_uid}"
        finally:
            try:
                M.logout()
            except Exception:
                pass

    def _mailbox_status(self, M: imaplib.IMAP4_SSL) -> tuple[int, int]:
        """Return (UIDVALIDITY, UIDNEXT) for the selected mailbox."""
        uidvalidity = 0
        uidnext = 0
        try:
            typ, data = M.status(self.mailbox, "(UIDVALIDITY UIDNEXT)")
            if typ == "OK" and data and data[0]:
                text = data[0].decode() if isinstance(data[0], bytes) else str(data[0])
                uidvalidity = _extract_status_int(text, "UIDVALIDITY")
                uidnext = _extract_status_int(text, "UIDNEXT")
        except Exception as e:
            logger.warning("IMAP %s: STATUS failed: %s", self.source_name, e)
        return uidvalidity, uidnext

    def _fetch_one(
        self, M: imaplib.IMAP4_SSL, uid: int, uidvalidity: int,
    ) -> dict:
        """Fetch and parse a single message by UID into a raw dict."""
        typ, data = M.uid("fetch", str(uid), "(RFC822)")
        if typ != "OK" or not data or not isinstance(data[0], tuple):
            raise ValueError(f"empty fetch for uid {uid}")

        raw_bytes = data[0][1]
        msg: Message = email.message_from_bytes(raw_bytes)

        subject = _decode_hdr(msg.get("Subject", "(no subject)"))
        sender = _decode_hdr(msg.get("From", "?"))
        date_str = msg.get("Date", "")
        epoch = _parse_to_epoch(date_str)
        timestamp = (
            datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
            if epoch
            else datetime.now(timezone.utc).isoformat()
        )

        body, envelope_png, envelope_mt, img_hint = _extract_parts(msg)

        sender_l = sender.lower()
        is_envelope = (
            any(h in sender_l for h in _DP_SENDER_HINTS)
            or img_hint
        )

        return {
            "id": f"{uidvalidity}-{uid}",
            "subject": subject,
            "from": sender,
            "date": date_str,
            "timestamp": timestamp,
            "body": body,
            "is_envelope": bool(is_envelope),
            "envelope_png": envelope_png,
            "envelope_media_type": envelope_mt,
        }

    # ------------------------------------------------------------------
    # Record builders
    # ------------------------------------------------------------------

    async def _build_envelope_record(self, msg: dict) -> tuple[str, str]:
        """Run vision on the envelope image; fall back to a generic notice."""
        png = msg.get("envelope_png")
        media_type = msg.get("envelope_media_type") or "image/png"
        if not png or not self._vision_client_factory or not self.vision_model:
            return self._envelope_fallback(msg)

        try:
            vision_text = await self._analyze_envelope(png, media_type)
        except Exception as e:
            logger.warning(
                "IMAP %s: envelope vision failed: %s", self.source_name, e,
            )
            return self._envelope_fallback(msg)

        if not vision_text:
            return self._envelope_fallback(msg)

        m = self.envelope_messages
        sender_line = (
            _first_line_after(vision_text, m.sender_key) or m.unknown_sender
        )
        summary = m.summary.format(label=self.label, sender=sender_line)
        content = m.content.format(
            vision=vision_text,
            subject=msg.get("subject", ""),
            date=msg.get("date", ""),
        )
        return summary, content

    def _envelope_fallback(self, msg: dict) -> tuple[str, str]:
        m = self.envelope_messages
        summary = m.summary_unknown.format(label=self.label)
        content = m.content_unknown.format(
            subject=msg.get("subject", ""),
            date=msg.get("date", ""),
        )
        return summary, content

    def _plain_record(self, msg: dict) -> tuple[str, str]:
        subject = msg.get("subject", "(no subject)")
        sender = msg.get("from", "?")
        summary = f"[{self.label}] {subject} — from {sender}"
        content = (
            f"Subject: {subject}\n"
            f"From: {sender}\n"
            f"Date: {msg.get('date', '')}\n\n"
            f"{msg.get('body', '')}"
        )
        return summary, content

    async def _analyze_envelope(self, png: bytes, media_type: str) -> str:
        """Send the envelope image to the multimodal model and return its text."""
        import base64

        client = self._vision_client_factory()
        b64 = base64.standard_b64encode(png).decode("ascii")
        try:
            resp = await client.messages.create(
                model=self.vision_model,
                max_tokens=300,
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": b64,
                            },
                        },
                        {"type": "text", "text": self.envelope_messages.vision_prompt},
                    ],
                }],
            )
        finally:
            close = getattr(client, "close", None)
            if close:
                try:
                    await close()
                except Exception:
                    pass

        parts = []
        for block in getattr(resp, "content", []) or []:
            text = getattr(block, "text", None)
            if text:
                parts.append(text)
        return "\n".join(parts).strip()


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _parse_cursor(cursor: str | None) -> tuple[int, int | None]:
    """Parse a ``<UIDVALIDITY>:<max_uid>`` cursor. Returns (validity, uid)."""
    if not cursor:
        return 0, None
    try:
        validity_s, uid_s = cursor.split(":", 1)
        return int(validity_s), int(uid_s)
    except (ValueError, AttributeError):
        return 0, None


def _extract_status_int(text: str, key: str) -> int:
    """Pull an integer value for *key* out of an IMAP STATUS response line."""
    import re

    m = re.search(rf"{key}\s+(\d+)", text)
    return int(m.group(1)) if m else 0


def _decode_hdr(raw: str | None) -> str:
    """Decode an RFC 2047 encoded email header into a plain string."""
    if not raw:
        return ""
    try:
        return str(make_header(decode_header(raw)))
    except Exception:
        return str(raw)


def _part_text(part: Message) -> str:
    """Decode a text MIME part to a string using its declared charset."""
    payload = part.get_payload(decode=True)
    if payload is None:
        return ""
    charset = part.get_content_charset() or "utf-8"
    try:
        return payload.decode(charset, errors="replace")
    except (LookupError, TypeError):
        return payload.decode("utf-8", errors="replace")


def _extract_parts(msg: Message) -> tuple[str, bytes | None, str | None, bool]:
    """Walk a message and extract body text + the best envelope image.

    Returns ``(body_text, envelope_png_bytes, media_type, img_hint_matched)``.
    ``img_hint_matched`` is True when an inline image's Content-ID/filename
    looked like a Deutsche Post envelope scan.
    """
    text_plain: str | None = None
    text_html: str | None = None
    best_img: tuple[bytes, str] | None = None      # (bytes, media_type)
    hinted_img: tuple[bytes, str] | None = None
    img_hint = False

    if msg.is_multipart():
        for part in msg.walk():
            if part.is_multipart():
                continue
            ctype = (part.get_content_type() or "").lower()
            disp = str(part.get("Content-Disposition") or "").lower()

            if ctype == "text/plain" and "attachment" not in disp and text_plain is None:
                text_plain = _part_text(part)
            elif ctype == "text/html" and text_html is None:
                text_html = _part_text(part)
            elif ctype.startswith("image/"):
                payload = part.get_payload(decode=True)
                if not payload:
                    continue
                cid = str(part.get("Content-ID") or "").lower()
                fname = str(part.get_filename() or "").lower()
                marker = f"{cid} {fname}"
                # Track the largest image as a fallback envelope candidate.
                if best_img is None or len(payload) > len(best_img[0]):
                    best_img = (payload, ctype)
                if any(h in marker for h in _ENVELOPE_IMG_HINTS):
                    img_hint = True
                    hinted_img = (payload, ctype)
    else:
        ctype = (msg.get_content_type() or "").lower()
        if ctype == "text/html":
            text_html = _part_text(msg)
        else:
            text_plain = _part_text(msg)

    if text_plain:
        body = text_plain
    elif text_html:
        body = _html_to_text(text_html)
    else:
        body = ""

    envelope = hinted_img or best_img
    if envelope:
        return body, envelope[0], envelope[1], img_hint
    return body, None, None, img_hint


def _first_line_after(text: str, marker: str) -> str:
    """Return the text following *marker* on the line where it appears."""
    for line in text.splitlines():
        if marker in line:
            return line.split(marker, 1)[1].strip()
    return ""
