"""Generic IMAP source — fetches emails from any IMAP server.

Each ImapSource instance handles ONE mailbox with its own cursor. The
registry creates one instance per configured account, so each gets
independent cursor tracking in the DB.

Cursor semantics: ``<UIDVALIDITY>:<max_uid>``. IMAP UIDs are only stable
within a given UIDVALIDITY; if the server resets it, we detect the mismatch
and re-baseline from a SINCE lookback window instead of trusting old UIDs.

imaplib is blocking, so the whole IMAP conversation runs in a worker thread
via ``asyncio.to_thread``.

Optional image pass
-------------------

Some mail carries its payload only as an inline image — a scan, a photo, a
rendered document — so the text an agent would act on is not in the body at
all. For those messages the source can run a multimodal model over the image
at ingest time and fold the answer into the record, so downstream consumers
get plain text.

Which messages that applies to, what to ask the model, and how to word the
result are entirely configuration (``sync.imap.match`` / ``sync.imap.vision``).
With no match rules configured nothing is singled out and this is a plain
IMAP mailbox source. See ``docs/sources.md`` for a worked example.
"""

from __future__ import annotations

import asyncio
import email
import imaplib
import logging
from datetime import datetime, timedelta, timezone
from email.header import decode_header, make_header
from email.message import Message
from typing import Any, Callable, Sequence

from nerve.sources.base import Source
from nerve.sources.gmail import _html_to_text, _parse_to_epoch
from nerve.sources.models import FetchResult, SourceRecord

logger = logging.getLogger(__name__)


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
        initial_lookback_days: int = 1,
        match: Any | None = None,
        vision: Any | None = None,
        vision_client_factory: Callable[[], Any] | None = None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self._password = password
        self.mailbox = mailbox
        self.label = label
        self.source_name = f"imap:{label}"
        self.initial_lookback_days = max(1, int(initial_lookback_days))
        self._vision_client_factory = vision_client_factory

        # Match rules decide which messages are "of interest"; the vision
        # block decides what to do with them. Both default to inert, so an
        # unconfigured source is a plain mailbox reader.
        if match is None or vision is None:
            from nerve.config import ImapMatchConfig, ImapVisionConfig

            match = ImapMatchConfig() if match is None else match
            vision = ImapVisionConfig() if vision is None else vision
        self.match = match
        self.vision = vision

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Fetch new messages since cursor, running the image pass on matches."""
        try:
            parsed, next_cursor = await asyncio.to_thread(
                self._imap_fetch_blocking, cursor, limit,
            )
        except Exception as e:
            logger.error("IMAP error for %s: %s", self.source_name, e)
            return FetchResult(records=[], next_cursor=cursor, has_more=False)

        records: list[SourceRecord] = []
        for msg in parsed:
            # only_matched: drop everything the rules did not single out
            # before it ever reaches the inbox.
            if self.match.only_matched and not msg.get("matched"):
                logger.debug(
                    "IMAP %s: dropping unmatched message %s (only_matched)",
                    self.source_name, msg.get("id"),
                )
                continue
            if msg.get("matched") and self.vision.enabled:
                summary, content = await self._build_vision_record(msg)
            elif msg.get("matched"):
                summary, content = self._vision_fallback(msg)
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
                    "matched": bool(msg.get("matched")),
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

        body, image, media_type, attachment_hit = _extract_parts(
            msg, self.match.attachment_contains,
        )

        sender_l = sender.lower()
        matched = attachment_hit or any(
            h.lower() in sender_l for h in self.match.sender_contains if h
        )

        return {
            "id": f"{uidvalidity}-{uid}",
            "subject": subject,
            "from": sender,
            "date": date_str,
            "timestamp": timestamp,
            "body": body,
            "matched": bool(matched),
            "image": image,
            "media_type": media_type,
        }

    # ------------------------------------------------------------------
    # Record builders
    # ------------------------------------------------------------------

    async def _build_vision_record(self, msg: dict) -> tuple[str, str]:
        """Run the image pass on a matched message; fall back if it can't."""
        image = msg.get("image")
        media_type = msg.get("media_type") or "image/png"
        if not image or not self._vision_client_factory or not self.vision.model:
            return self._vision_fallback(msg)

        try:
            vision_text = await self._analyze_image(image, media_type)
        except Exception as e:
            logger.warning(
                "IMAP %s: image analysis failed: %s", self.source_name, e,
            )
            return self._vision_fallback(msg)

        if not vision_text:
            return self._vision_fallback(msg)

        v = self.vision
        # answer_key is the label the prompt asked the model to emit; with no
        # key configured, take the first non-empty line of the answer.
        answer = (
            _first_line_after(vision_text, v.answer_key)
            if v.answer_key
            else _first_nonempty_line(vision_text)
        ) or v.unknown_answer
        fields = self._template_fields(msg, vision=vision_text, answer=answer)
        return _format(v.summary, fields), _format(v.content, fields)

    def _vision_fallback(self, msg: dict) -> tuple[str, str]:
        v = self.vision
        fields = self._template_fields(
            msg, vision="", answer=v.unknown_answer,
        )
        return _format(v.summary_unknown, fields), _format(v.content_unknown, fields)

    def _template_fields(self, msg: dict, *, vision: str, answer: str) -> dict:
        """Placeholders available to every configured wording template."""
        return {
            "label": self.label,
            "answer": answer,
            "vision": vision,
            "subject": msg.get("subject", ""),
            "sender": msg.get("from", ""),
            "date": msg.get("date", ""),
            "body": msg.get("body", ""),
        }

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

    async def _analyze_image(self, image: bytes, media_type: str) -> str:
        """Send the image to the multimodal model and return its text."""
        import base64

        client = self._vision_client_factory()
        b64 = base64.standard_b64encode(image).decode("ascii")
        try:
            resp = await client.messages.create(
                model=self.vision.model,
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
                        {"type": "text", "text": self.vision.prompt},
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


def _extract_parts(
    msg: Message, attachment_hints: Sequence[str] = (),
) -> tuple[str, bytes | None, str | None, bool]:
    """Walk a message and extract body text + the most relevant image.

    Returns ``(body_text, image_bytes, media_type, attachment_hit)``.
    ``attachment_hit`` is True when an inline image's Content-ID or filename
    contains one of *attachment_hints* — that image then wins over the merely
    largest one, which is the fallback candidate.
    """
    text_plain: str | None = None
    text_html: str | None = None
    best_img: tuple[bytes, str] | None = None      # (bytes, media_type)
    hinted_img: tuple[bytes, str] | None = None
    hints = [h.lower() for h in attachment_hints if h]
    attachment_hit = False

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
                # Track the largest image as a fallback candidate.
                if best_img is None or len(payload) > len(best_img[0]):
                    best_img = (payload, ctype)
                if any(h in marker for h in hints):
                    attachment_hit = True
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

    image = hinted_img or best_img
    if image:
        return body, image[0], image[1], attachment_hit
    return body, None, None, attachment_hit


def _first_line_after(text: str, marker: str) -> str:
    """Return the text following *marker* on the line where it appears."""
    for line in text.splitlines():
        if marker in line:
            return line.split(marker, 1)[1].strip()
    return ""


def _first_nonempty_line(text: str) -> str:
    """Return the first line of *text* that carries anything."""
    for line in text.splitlines():
        if line.strip():
            return line.strip()
    return ""


def _format(template: str, fields: dict) -> str:
    """Fill a configured wording template, surviving a bad placeholder.

    A typo in config would otherwise raise mid-fetch and lose the whole
    batch, so an unknown ``{placeholder}`` leaves the template visible
    instead — the operator sees their own broken wording in the inbox.
    """
    try:
        return template.format(**fields)
    except (KeyError, IndexError, ValueError) as e:
        logger.warning("IMAP: bad placeholder in configured template (%s)", e)
        return template
