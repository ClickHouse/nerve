"""Gmail source — fetches emails via the gog CLI.

Each GmailSource instance handles ONE account with its own cursor.
The registry creates one instance per configured account, so each
gets independent cursor tracking in the DB.

Two-step fetch: search for message IDs, then get each message body.
- `gog gmail messages search <query>` → list of {id, subject, from, date, labels}
- `gog gmail get <id>` → {body, headers, message} with full HTML body

Cursor semantics: JSON state containing the newest successfully fetched
Gmail `internalDate` epoch timestamp and any messages whose bodies still need
to be fetched. Failed message metadata is kept in the cursor so retries can
continue even after a message falls out of the search result window. Legacy
epoch-only cursors are still accepted.
On first run (no cursor), uses `newer_than:1d`.

Note: gog always returns HTML email bodies.  We extract clean text for
agent processing and store the original HTML for UI rendering.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import html2text

from nerve.sources.base import Source
from nerve.sources.models import FetchResult, SourceRecord

logger = logging.getLogger(__name__)

# Max concurrent message fetches to avoid overwhelming gog
_MAX_CONCURRENT_GETS = 5


def _decode_cursor(
    cursor: str | None,
) -> tuple[int | None, dict[str, dict[str, Any]]]:
    """Decode a Gmail cursor, accepting the legacy epoch-only format."""
    if cursor is None:
        return None, {}

    try:
        raw_state = json.loads(cursor)
    except (json.JSONDecodeError, TypeError):
        try:
            return int(cursor), {}
        except (TypeError, ValueError):
            logger.warning("Invalid Gmail cursor %r — starting from the lookback window", cursor)
            return None, {}

    # ``json.loads("123")`` returns an int, which is the old cursor format.
    if isinstance(raw_state, int):
        return raw_state, {}
    if not isinstance(raw_state, dict):
        logger.warning("Invalid Gmail cursor %r — starting from the lookback window", cursor)
        return None, {}

    raw_epoch = raw_state.get("epoch")
    try:
        epoch = int(raw_epoch) if raw_epoch is not None else None
    except (TypeError, ValueError):
        epoch = None

    pending: dict[str, dict[str, Any]] = {}
    raw_pending = raw_state.get("pending", {})
    if isinstance(raw_pending, dict):
        for message_id, raw_entry in raw_pending.items():
            if not isinstance(message_id, str) or not message_id:
                continue
            if not isinstance(raw_entry, dict):
                continue

            message = raw_entry.get("message", {"id": message_id})
            if not isinstance(message, dict):
                message = {"id": message_id}
            else:
                message = dict(message)
            message.setdefault("id", message_id)

            try:
                attempts = max(1, int(raw_entry.get("attempts", 1)))
            except (TypeError, ValueError):
                attempts = 1
            pending[message_id] = {
                "status": raw_entry.get("status", "failed"),
                "attempts": attempts,
                "message": message,
            }

    return epoch, pending


def _encode_cursor(
    epoch: int | None,
    pending: dict[str, dict[str, Any]],
) -> str:
    """Encode the Gmail cursor and its durable per-message retry state."""
    return json.dumps({"epoch": epoch, "pending": pending}, sort_keys=True)


def _message_snapshot(message: dict[str, Any]) -> dict[str, Any]:
    """Keep the search metadata needed to build a record on a later retry."""
    fields = ("id", "threadId", "subject", "from", "date", "labels")
    return {field: message[field] for field in fields if field in message}


def _mark_pending(
    pending: dict[str, dict[str, Any]],
    message: dict[str, Any],
) -> None:
    """Record a body-fetch failure without blocking newer messages."""
    message_id = str(message.get("id", ""))
    if not message_id:
        return

    previous = pending.get(message_id, {})
    try:
        attempts = int(previous.get("attempts", 0)) + 1
    except (AttributeError, TypeError, ValueError):
        attempts = 1
    pending[message_id] = {
        "status": "failed",
        "attempts": attempts,
        "message": _message_snapshot(message),
    }


def _candidate_messages(
    messages: list[dict],
    pending: dict[str, dict[str, Any]],
) -> list[dict]:
    """Merge pending retries with fresh search results, deduplicated by ID."""
    candidates: dict[str, dict] = {}
    for message_id, entry in pending.items():
        message = entry.get("message", {})
        if not isinstance(message, dict):
            message = {}
        message = dict(message)
        message.setdefault("id", message_id)
        candidates[message_id] = message

    for message in messages:
        message_id = message.get("id")
        if message_id:
            # Fresh search metadata wins if a pending message is returned again.
            candidates[str(message_id)] = message

    return list(candidates.values())

# ---------------------------------------------------------------------------
# Email boilerplate detection
# ---------------------------------------------------------------------------

# Patterns that identify boilerplate paragraphs.  Matched against each
# paragraph (text between blank lines) — if any pattern hits, the whole
# paragraph is dropped.
_BOILERPLATE_RE = re.compile(
    r'(?:'
    r'LIMITATION OF LIABILITY'
    r'|REFUND POLICY'
    r'|TERMS OF SERVICE'
    r'|PRIVACY POLICY'
    r'|DISCLAIMER'
    r'|You(?:\'re| are) receiving this (?:email|message|notification)'
    r'|This (?:email|message) was sent (?:to|by|from)'
    r'|Payments processed by'
    r'|The following applies only to'
    r'|If you no longer (?:wish|want) to receive'
    r'|To (?:unsubscribe|stop receiving|opt[\s\-]?out)'
    r'|(?:Click|Tap) here to (?:unsubscribe|manage)'
    r'|(?:Manage|Update) (?:your )?(?:email )?(?:preferences|subscriptions|notifications)'
    r'|(?:Unsubscribe|Opt[\s\-]?out) (?:from|of)'
    r'|Do not reply (?:directly )?to this (?:email|message)'
    r'|This is an? (?:automated|auto[\s\-]?generated|no[\s\-]?reply)'
    r'|Sent (?:from|via) (?:my )?(?:iPhone|iPad|Android|Samsung|Outlook|Gmail)'
    r'|View (?:this )?(?:email )?in (?:your )?browser'
    r'|Add \S+ to your address book'
    r'|Problems? viewing this'
    r'|Copyright\s*(?:©|\(c\))\s*\d{4}'
    r'|All rights reserved'
    r')',
    re.IGNORECASE,
)

# Standalone URL on its own line (tracking pixels, unsubscribe links, etc.)
_STANDALONE_URL_RE = re.compile(r'^\s*<?https?://\S+>?\s*$', re.MULTILINE)


class GmailSource(Source):
    """Gmail source for a single account using the gog CLI."""

    def __init__(self, account: str, config: dict[str, Any]):
        self.account = account
        self.source_name = f"gmail:{account}"
        self._config = config

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Fetch new emails since cursor.

        On first run (cursor=None): uses `newer_than:1d`.
        On subsequent runs: uses `after:<epoch+1>` and retries pending
        message IDs independently of the search window.

        The cursor stores Gmail's internalDate (second precision), plus search
        metadata for body fetches that need retrying. Using +1 is sufficient
        because the cursor and Gmail's `after:` filter use the same timescale.
        """
        cursor_epoch, pending = _decode_cursor(cursor)
        had_pending = bool(pending)

        # Build query filter from cursor.
        # The cursor is derived from Gmail's internalDate (epoch seconds) —
        # the same timestamp that `after:` filters on.  +1 ensures we skip
        # the message the cursor was set from.
        if cursor_epoch is not None:
            after_epoch = cursor_epoch + 1
            query_filter = f"after:{after_epoch} -in:spam -in:trash"
        else:
            query_filter = "newer_than:1d -in:spam -in:trash"

        env = {**os.environ}
        keyring_password = self._config.get("keyring_password", "")
        if keyring_password:
            env["GOG_KEYRING_PASSWORD"] = keyring_password

        records: list[SourceRecord] = []
        newest_epoch = cursor_epoch

        try:
            # Step 1: Search for message IDs + metadata
            messages = await self._search_messages(query_filter, limit, env)
            candidates = _candidate_messages(messages, pending)

            # Pending messages are retried by ID even if they no longer appear
            # in the bounded search result window.
            if not candidates:
                return FetchResult(records=[], next_cursor=cursor, has_more=False)

            # Step 2: Fetch body + internalDate for each message
            sem = asyncio.Semaphore(_MAX_CONCURRENT_GETS)
            tasks = [
                self._fetch_message_body(msg["id"], env, sem)
                for msg in candidates
            ]
            bodies = await asyncio.gather(*tasks, return_exceptions=True)

            for msg, body_result in zip(candidates, bodies):
                message_id = msg["id"]
                date_str = msg.get("date", "")

                # Extract body text, HTML, and internalDate from the fetch result
                body = ""
                html_body: str | None = None
                internal_date: int | None = None
                if isinstance(body_result, tuple):
                    body, html_body, internal_date = body_result
                elif isinstance(body_result, Exception):
                    logger.warning("Failed to fetch body for %s: %s", message_id, body_result)
                    _mark_pending(pending, msg)
                    continue
                else:
                    logger.warning("Failed to fetch body for %s", message_id)
                    _mark_pending(pending, msg)
                    continue

                # A successful retry removes the message from the durable
                # pending set before the record is returned.
                was_pending = message_id in pending
                pending.pop(message_id, None)

                # Use internalDate for cursor tracking (matches Gmail's `after:`
                # filter).  Fall back to the Date header only if internalDate
                # is unavailable.
                msg_epoch = internal_date or _parse_to_epoch(date_str)

                # Client-side dedup: skip messages at or before the cursor.
                # Gmail's `after:` has a small tolerance window (~2s) so a
                # message right at the boundary can slip through.
                if (
                    not was_pending
                    and cursor_epoch is not None
                    and msg_epoch
                    and msg_epoch <= cursor_epoch
                ):
                    logger.debug(
                        "Gmail %s: skipping already-seen message %s (epoch=%d, cursor=%s)",
                        self.account, message_id, msg_epoch, cursor_epoch,
                    )
                    continue

                if msg_epoch and (newest_epoch is None or msg_epoch > newest_epoch):
                    newest_epoch = msg_epoch

                subject = msg.get("subject", "(no subject)")
                sender = msg.get("from", "?")

                # Keep original HTML for UI rendering.
                raw_html: str | None = html_body
                # Use HTML-to-text when it's more complete than plain text
                # (e.g. Amazon pickup emails include DHL tracking only in
                # HTML).  Fall back to gog's plain-text body otherwise.
                if html_body:
                    html_text = _html_to_text(html_body)
                    if not body or _looks_like_html(body) or len(html_text) > len(body):
                        body = html_text

                records.append(SourceRecord(
                    id=msg.get("id", ""),
                    source=self.source_name,
                    record_type="gmail_message",
                    summary=f"[{self.account}] {subject} — from {sender}",
                    content=(
                        f"Subject: {subject}\n"
                        f"From: {sender}\n"
                        f"Date: {date_str}\n"
                        f"Labels: {', '.join(msg.get('labels', []))}\n\n"
                        f"{body}"
                    ),
                    timestamp=(
                        datetime.fromtimestamp(msg_epoch, tz=timezone.utc).isoformat()
                        if msg_epoch
                        else date_str.replace(' ', 'T') or datetime.now(timezone.utc).isoformat()
                    ),
                    metadata={
                        "account": self.account,
                        "thread_id": msg.get("threadId", ""),
                        "labels": msg.get("labels", []),
                    },
                    raw_content=raw_html,
                ))

        except FileNotFoundError:
            logger.error("gog CLI not found — install gog for Gmail sync")
        except asyncio.TimeoutError:
            logger.error("gog gmail timed out for %s", self.account)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse gog output for %s: %s", self.account, e)
        except Exception as e:
            logger.error("Gmail error for %s: %s", self.account, e)

        # The timestamp can move past a failed message because its search
        # metadata is retained in ``pending`` and the body is retried by ID.
        # This prevents one permanently failing message from pinning the whole
        # Gmail stream or disappearing when it falls out of the search window.
        if pending or newest_epoch != cursor_epoch or had_pending:
            next_cursor = _encode_cursor(newest_epoch, pending)
        else:
            next_cursor = cursor
        return FetchResult(records=records, next_cursor=next_cursor, has_more=False)

    async def preprocess(self, records: list[SourceRecord]) -> list[SourceRecord]:
        """Strip common email boilerplate from message bodies."""
        for record in records:
            record.content = _clean_email_content(record.content)
        return records

    async def _search_messages(
        self, query: str, limit: int, env: dict,
    ) -> list[dict]:
        """Search for messages, returns list of metadata dicts."""
        proc = await asyncio.create_subprocess_exec(
            "gog", "gmail", "messages", "search",
            query,
            "--account", self.account,
            "--json",
            "--max", str(min(100, limit)),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)

        if proc.returncode != 0:
            logger.error("gog gmail search failed for %s: %s", self.account, stderr.decode())
            return []

        stdout_text = stdout.decode()
        raw = json.loads(stdout_text) if stdout_text.strip() else {}
        return raw.get("messages", []) if isinstance(raw, dict) else raw

    async def _fetch_message_body(
        self, message_id: str, env: dict, sem: asyncio.Semaphore,
    ) -> tuple[str, str | None, int | None] | None:
        """Fetch the body text, HTML body, and internalDate of a single message.

        Returns:
            (text_body, html_body_or_none, internal_date_epoch_seconds).
            ``None`` when the message could not be retrieved.

        ``gog gmail get`` puts one body variant in its top-level ``body``
        field.  For multipart/alternative messages it picks text/plain,
        dropping the HTML.  We dig into ``message.payload.parts`` to
        recover the text/html part when available.
        """
        async with sem:
            proc = await asyncio.create_subprocess_exec(
                "gog", "gmail", "get", message_id,
                "--account", self.account,
                "--json",
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)

            if proc.returncode != 0:
                logger.warning("gog gmail get %s failed: %s", message_id, stderr.decode()[:200])
                return None

            stdout_text = stdout.decode()
            if not stdout_text.strip():
                logger.warning("gog gmail get %s returned no data", message_id)
                return None
            data = json.loads(stdout_text)
            body = data.get("body", "")

            # Extract internalDate from the raw Gmail API message object.
            internal_date: int | None = None
            msg_obj = data.get("message", {})
            if isinstance(msg_obj, dict):
                raw_ts = msg_obj.get("internalDate")
                if raw_ts:
                    try:
                        internal_date = int(raw_ts) // 1000  # ms → seconds
                    except (ValueError, TypeError):
                        pass

            # Try to extract the text/html part from the payload.
            # gog's top-level "body" prefers text/plain for multipart emails,
            # so the HTML is only available by walking the payload parts tree.
            html_body: str | None = None
            payload = msg_obj.get("payload", {}) if isinstance(msg_obj, dict) else {}
            if payload:
                html_body = _extract_mime_part(payload, "text/html")

            # If gog's body is already HTML (single-part HTML emails),
            # use it directly as the HTML body.
            if not html_body and _looks_like_html(body):
                html_body = body

            return body, html_body, internal_date


# ---------------------------------------------------------------------------
# Gmail payload MIME helpers
# ---------------------------------------------------------------------------

def _extract_mime_part(payload: dict, mime_type: str) -> str | None:
    """Recursively walk a Gmail API payload tree and return the decoded
    body of the first part matching *mime_type* (e.g. ``text/html``).

    Gmail encodes part bodies as URL-safe base64 (RFC 4648 §5) without
    padding.  Returns ``None`` if the requested type isn't found.
    """
    if payload.get("mimeType") == mime_type:
        data = payload.get("body", {}).get("data")
        if data:
            try:
                # Gmail uses URL-safe base64 without padding
                return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
            except Exception:
                return None

    for part in payload.get("parts", []):
        result = _extract_mime_part(part, mime_type)
        if result:
            return result
    return None


# ---------------------------------------------------------------------------
# HTML → text extraction (via html2text)
# ---------------------------------------------------------------------------

def _html_to_text(html: str) -> str:
    """Convert HTML email body to clean Markdown-ish text via html2text."""
    h = html2text.HTML2Text()
    h.ignore_images = True       # skip image refs — not useful in text mode
    h.ignore_emphasis = False    # keep bold/italic as markdown
    h.body_width = 0             # no hard line wrapping
    h.unicode_snob = True        # use unicode chars instead of ascii approximations
    h.protect_links = True       # don't wrap long URLs
    h.wrap_links = False         # keep link URLs inline
    try:
        return h.handle(html).strip()
    except Exception:
        # Malformed HTML — fall back to naive tag stripping
        return re.sub(r'<[^>]+>', ' ', html).strip()


def _looks_like_html(text: str) -> bool:
    """Quick check if content looks like HTML."""
    stripped = text.lstrip()[:500].lower()
    return (
        '<html' in stripped
        or '<body' in stripped
        or '<!doctype' in stripped
        or '<div' in stripped
        or '<table' in stripped
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean_email_content(content: str) -> str:
    """Clean email record content, preserving the header block."""
    # Split header (Subject/From/Date/Labels) from body at first blank line
    parts = content.split('\n\n', 1)
    if len(parts) < 2:
        return content
    header, body = parts
    cleaned = _strip_boilerplate(body)
    return f"{header}\n\n{cleaned}"


def _strip_boilerplate(body: str) -> str:
    """Remove boilerplate paragraphs from plain-text email body."""
    paragraphs = re.split(r'\n\n+', body)
    cleaned: list[str] = []

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        # Drop short paragraphs that match a boilerplate pattern.
        # Long paragraphs (>300 chars) likely contain real content with
        # some boilerplate mixed in — e.g. when html2text collapses
        # table-based layouts into a single block.
        if _BOILERPLATE_RE.search(para) and len(para) < 300:
            continue
        # Strip standalone URL lines that are clearly boilerplate
        # (tracking pixels, unsubscribe, social media, etc.)
        # but keep actionable URLs (booking, messaging, payment, etc.)
        def _is_boilerplate_url(m: re.Match) -> str:
            url = m.group(0).strip().strip('<>')
            _boilerplate_url_patterns = (
                'unsubscribe', 'optout', 'opt-out', 'manage-preferences',
                'email-preferences', 'notification-settings',
                'tracking', 'click.', 'trk.', 'sng.link',
                'tiktok.com', 'instagram.com', 'twitter.com', 'facebook.com',
                'youtube.com', 'linkedin.com',
                'account-settings/notification', 'email-unsubscribe',
                '%opentrack%',
            )
            url_lower = url.lower()
            if any(p in url_lower for p in _boilerplate_url_patterns):
                return ''
            return m.group(0)
        para = _STANDALONE_URL_RE.sub(_is_boilerplate_url, para).strip()
        if para:
            cleaned.append(para)

    return '\n\n'.join(cleaned)


def _parse_to_epoch(date_str: str) -> int | None:
    """Parse an RFC 2822 or ISO 8601 date string to epoch seconds."""
    if not date_str:
        return None
    try:
        # Try RFC 2822 first (Gmail format)
        dt = parsedate_to_datetime(date_str)
        return int(dt.timestamp())
    except Exception:
        pass
    try:
        # Try ISO 8601
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        pass
    try:
        # Try "YYYY-MM-DD HH:MM" format (gog's default)
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None
