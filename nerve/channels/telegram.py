"""Telegram bot channel — receive messages, run agent, respond.

Uses python-telegram-bot (v21+) for async Telegram bot communication.
Supports partial message streaming (edit-in-place) via the StreamAdapter.
Session management is delegated to ChannelRouter.
"""

from __future__ import annotations

import asyncio
import base64
import collections
import functools
import html as _html
import logging
import re
import socket
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, NamedTuple, TYPE_CHECKING
from zoneinfo import ZoneInfo

from telegram import ForceReply, InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ChatAction, ParseMode
from telegram.error import EndPointNotFound, RetryAfter
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, MessageReactionHandler, filters

from nerve.agent.engine import NotAcceptingTurnsError
from nerve.channels.archives import (
    IMAGE_EXT_TO_MIME,
    MAX_TEXT_SIZE,
    TEXT_EXTENSIONS,
    extract_zip,
)
from nerve.channels.access import Identity, PatternGate
from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    ChannelConstraints,
    InboundMessage,
    ObservedMessage,
    OutboundMessage,
)
from nerve.channels.observation import ObservationPolicy
from nerve.channels.router import turn_stop_note
from nerve.config import NerveConfig

if TYPE_CHECKING:
    from nerve.channels.router import ChannelRouter

logger = logging.getLogger(__name__)

# Telegram message length limit
MAX_MSG_LEN = 4096
# Shown (no LLM) when a user replies to a message we can't tie to a live session,
# rather than silently mis-routing the reply into the chat's active session.
REPLY_UNRESOLVED_MSG = (
    "⚠️ I couldn't find the session that produced the message you replied to "
    "(it may be too old, or its session was archived), so nothing was sent. "
    "Reply with /new <your message> to start a new session with it quoted, "
    "or send the message without replying to reach the active session."
)
# A rich message's limit (Bot API 10.1), in characters of its text
RICH_MSG_LEN = 32768
# Blocks in one rich message, as _line_weights counts them: lines outside
# code, and one per code block. Telegram's cap is 500 blocks, and a list item
# is two of them (the item and its paragraph).
_RICH_MAX_BLOCKS = 200
# Read/write timeout for sending a rich message: up to 32 KB to parse
_RICH_TIMEOUT = 20.0
# Minimum interval between message edits (seconds) to avoid rate limits
EDIT_INTERVAL = 1.5
# Chats whose last streaming edit time is remembered (see _claim_chat_edit)
_EDIT_CLOCK_MAX = 500
# The longest Telegram's "retry after" that a final send waits out
_SEND_RETRY_MAX_WAIT = 30.0
# A forum's General topic. Telegram refuses it as a message_thread_id, and
# Nerve treats it as the chat itself.
GENERAL_TOPIC_ID = 1
# The name of a topic that /new opens without a title, until the session's
# generated title replaces it
NEW_TOPIC_NAME = "New session"
# Telegram's limit for a topic name
_TOPIC_NAME_MAX = 128
# What Telegram's errors say (lower-cased) about a topic that is gone, and
# about closing a topic that is closed already
_TOPIC_GONE_ERRORS = ("thread not found", "topic_id_invalid", "topic_deleted")
_TOPIC_UNCHANGED_ERRORS = ("topic_not_modified",)
# Messages in a "Send transcript" document, newest kept when a session has more
_TRANSCRIPT_MAX_MESSAGES = 10_000
# Watchdog: check every 30s, log heartbeat every ~5 min
WATCHDOG_INTERVAL = 30
WATCHDOG_HEARTBEAT_EVERY = 10

# TCP keepalive: prevent NAT/firewall from silently dropping the
# long-poll connection.  These values tell the OS to send a keepalive
# probe after 60s idle, retry every 10s, give up after 3 failures.
#
# The fine-grained tuning constants (TCP_KEEPIDLE/INTVL/CNT) are Linux-only.
# macOS exposes only TCP_KEEPALIVE (semantically equivalent to TCP_KEEPIDLE).
# On Windows / unknown platforms we fall back to enabling SO_KEEPALIVE alone
# and let the OS use its default keepalive timing.
if sys.platform == "linux":
    _TCP_KEEPALIVE_OPTS: tuple[tuple[int, int, int], ...] = (
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        (socket.SOL_TCP, socket.TCP_KEEPIDLE, 60),
        (socket.SOL_TCP, socket.TCP_KEEPINTVL, 10),
        (socket.SOL_TCP, socket.TCP_KEEPCNT, 3),
    )
elif sys.platform == "darwin":
    _TCP_KEEPALIVE_OPTS = (
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        (socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, 60),
    )
else:
    _TCP_KEEPALIVE_OPTS = ((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),)


def format_target(chat_id: int | str, thread_id: int | None = None) -> str:
    """The channel target for a chat, or for one topic in it.

    ``"<chat>"`` for a chat or its General topic, ``"<chat>:<thread>"`` for a
    topic. Chat ids never contain ``:``, so the split in :func:`parse_target`
    is unambiguous, negative group ids included.
    """
    return f"{chat_id}:{thread_id}" if thread_id is not None else str(chat_id)


def parse_target(target: str) -> tuple[int, int | None]:
    """Split a channel target into ``(chat_id, thread_id)``; no topic is ``None``."""
    chat, sep, thread = str(target).partition(":")
    return int(chat), (int(thread) if sep else None)


def _topic_name(text: str | None) -> str:
    """A topic name Telegram accepts: one line of 1 to 128 characters."""
    name = " ".join(str(text or "").split())
    if len(name) > _TOPIC_NAME_MAX:
        name = name[:_TOPIC_NAME_MAX - 1].rstrip() + "…"
    return name or NEW_TOPIC_NAME


def _in_thread(thread_id: int | None) -> dict[str, int]:
    """The ``message_thread_id`` keyword for a topic, and none for the chat itself."""
    return {"message_thread_id": thread_id} if thread_id is not None else {}


def _retry_after_seconds(exc: RetryAfter) -> float:
    """Telegram's advised wait, in seconds (an int or a timedelta, by PTB version)."""
    wait = exc.retry_after
    return float(wait.total_seconds() if hasattr(wait, "total_seconds") else wait)


@dataclass(frozen=True)
class _Address:
    """Where a Telegram update came from: a chat, and a topic in it or ``None``."""

    chat_id: int
    thread_id: int | None = None

    @property
    def target(self) -> str:
        return format_target(self.chat_id, self.thread_id)

    @property
    def channel_key(self) -> str:
        return f"telegram:{self.target}"


def _address_of_key(channel_key: str) -> _Address | None:
    """The address behind a ``telegram:<target>`` channel key, else ``None``."""
    prefix, _, target = channel_key.partition(":")
    if prefix != "telegram" or not target:
        return None
    try:
        return _Address(*parse_target(target))
    except ValueError:
        return None


def _error_says(exc: BaseException, phrases: tuple[str, ...]) -> bool:
    """Whether Telegram's error *exc* contains one of *phrases* (lower case)."""
    text = str(exc).lower()
    return any(phrase in text for phrase in phrases)


class SessionTopic(NamedTuple):
    """What :meth:`TelegramChannel.open_session_topic` did."""

    channel_key: str | None  # ``telegram:<chat>:<thread>``; None when it failed
    opened: bool = False     # a topic opened now, not the one the session had
    error: str = ""          # why it failed, in words a person can read
    retry_after: float | None = None  # seconds Telegram asked to wait, if rate limited


class ClosedTopic(NamedTuple):
    """A topic :meth:`TelegramChannel.close_session_topic` let go of."""

    channel_key: str
    # "closed"; "left_open" in a private chat, where Telegram cannot close a
    # topic; "gone" when the topic had been deleted already
    outcome: str


def _md_to_tg_html(text: str) -> str:
    """Convert standard Markdown to Telegram-compatible HTML.

    Telegram's legacy ``ParseMode.MARKDOWN`` only supports ``*bold*``,
    but LLMs emit ``**bold**`` (standard Markdown).  This converts the
    common constructs to HTML so we can use ``ParseMode.HTML`` instead,
    which is more predictable and doesn't choke on special characters.

    Handles: ``**bold**``, ``*italic*``, `` `code` ``, code fences,
    and ``[text](url)``.  Unmatched markers pass through as-is.
    """
    protected: list[str] = []

    def _protect(replacement: str) -> str:
        idx = len(protected)
        protected.append(replacement)
        return f"\x00{idx}\x00"

    # -- protect constructs that contain chars we'd otherwise escape --

    # Expandable blockquotes: <blockquote expandable>...</blockquote>
    def _expandable_bq(m: re.Match) -> str:
        inner = _md_to_tg_html(m.group(1))
        return _protect(f"<blockquote expandable>{inner}</blockquote>")
    text = re.sub(
        r"<blockquote\s+expandable>(.*?)</blockquote>",
        _expandable_bq, text, flags=re.DOTALL,
    )

    # Code fences: ```lang\n...\n```
    def _fence(m: re.Match) -> str:
        return _protect(f"<pre>{_html.escape(m.group(2))}</pre>")
    text = re.sub(r"```(\w*)\n?(.*?)```", _fence, text, flags=re.DOTALL)

    # Inline code: `...`
    def _code(m: re.Match) -> str:
        return _protect(f"<code>{_html.escape(m.group(1))}</code>")
    text = re.sub(r"`([^`]+)`", _code, text)

    # Markdown links: [text](url)
    def _link(m: re.Match) -> str:
        label = _html.escape(m.group(1))
        url = m.group(2)
        return _protect(f'<a href="{url}">{label}</a>')
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _link, text)

    # -- escape remaining HTML entities --
    text = _html.escape(text, quote=False)

    # -- inline formatting --
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", text)

    # -- restore protected spans --
    for i, repl in enumerate(protected):
        text = text.replace(f"\x00{i}\x00", repl)

    return text


# ---------------------------------------------------------------------- #
#  Markdown blocks: splitting and rich messages                            #
# ---------------------------------------------------------------------- #

# A code fence's opening line: up to three spaces, then ``` or ~~~ and an
# optional info string (the language).
_FENCE_OPEN_RE = re.compile(r"^( {0,3})(`{3,}|~{3,})(.*)$")
# A table's delimiter row, such as "|:---|---:|" or "--- | ---".
_TABLE_DELIM_RE = re.compile(r"^\s*\|?\s*:?-+:?\s*(\|\s*:?-+:?\s*)*\|?\s*$")
_HEADING_RE = re.compile(r"^ {0,3}#{1,6}(\s|$)")
_RULE_RE = re.compile(r"^ {0,3}([-*_=])(\s*\1){2,}\s*$")
# An inline code span: a run of backticks, its text, the same run again.
_CODE_SPAN_RE = re.compile(r"(`+)(.+?)\1")
# HTML tags a rich message renders and an answer may use on purpose. Telegram
# drops anything else that looks like a tag ("Vec<String>" shows as "Vec"),
# so every other "<" is escaped.
_RICH_TAGS = (
    "a", "b", "blockquote", "br", "code", "del", "details", "em", "i", "ins",
    "pre", "s", "strike", "strong", "sub", "summary", "sup", "tg-spoiler", "u",
)
_RICH_TAG_RE = re.compile(
    r"</?(?:%s)(?=[\s>/])" % "|".join(map(re.escape, _RICH_TAGS)), re.IGNORECASE,
)
_AUTOLINK_RE = re.compile(r"<(?:https?|mailto|tg):[^\s<>]*>", re.IGNORECASE)


def _is_table_delim(line: str) -> bool:
    """Whether *line* is a table's delimiter row ("---" alone is a rule, not one)."""
    return "|" in line and bool(_TABLE_DELIM_RE.match(line))


def _closes_fence(line: str, closer: str) -> bool:
    """Whether *line* closes a fence opened with *closer* (e.g. ``` or ~~~~)."""
    marker = closer.strip()
    stripped = line.strip()
    return (
        len(line) - len(line.lstrip(" ")) <= 3
        and len(stripped) >= len(marker)
        and set(stripped) == {marker[0]}
    )


# Where a line sits relative to code fences (see _fence_marks)
_OUTSIDE, _OPENS, _INSIDE = 0, 1, 2


def _fence_marks(lines: list[str]) -> list[int]:
    """For each line: outside code, opening a fence, or inside one (its closer too)."""
    marks: list[int] = []
    closer: str | None = None
    for line in lines:
        if closer is None:
            m = _FENCE_OPEN_RE.match(line)
            if m and not (m.group(2)[0] == "`" and "`" in m.group(3)):
                closer = m.group(1) + m.group(2)
                marks.append(_OPENS)
                continue
            marks.append(_OUTSIDE)
        else:
            marks.append(_INSIDE)
            if _closes_fence(line, closer):
                closer = None
    return marks


def _code_lines(lines: list[str]) -> list[bool]:
    """For each line, whether it belongs to a code fence (its markers included)."""
    return [mark != _OUTSIDE for mark in _fence_marks(lines)]


def _line_weights(lines: list[str]) -> list[int]:
    """What each line adds to a rich message's block count.

    A line outside code adds one, a code block one however long it is. A
    list item is a second block (its paragraph), which the cap leaves room
    for.
    """
    return [
        1 if mark == _OPENS else 0 if mark == _INSIDE else int(bool(line.strip()))
        for line, mark in zip(lines, _fence_marks(lines))
    ]


def _weight(text: str) -> int:
    return sum(_line_weights(text.split("\n")))


def _markdown_blocks(text: str) -> list[str]:
    """Markdown cut at blank lines, with each code fence kept whole."""
    lines = text.split("\n")
    blocks: list[str] = []
    current: list[str] = []
    for line, code in zip(lines, _code_lines(lines)):
        if not code and not line.strip():
            if current:
                blocks.append("\n".join(current))
                current = []
            continue
        current.append(line)
    if current:
        blocks.append("\n".join(current))
    return blocks


def _fits(text: str, limit: int, max_blocks: int) -> bool:
    return len(text) <= limit and (not max_blocks or _weight(text) <= max_blocks)


def _fit_block(block: str, limit: int, max_blocks: int) -> list[str]:
    """One block as pieces that fit, cut at line ends.

    A cut inside a code fence closes the fence and reopens it, language and
    all, at the top of the next piece, so each piece renders on its own. A
    line longer than a piece is cut too.
    """
    if _fits(block, limit, max_blocks):
        return [block]
    source = block.split("\n")
    pieces: list[str] = []
    lines: list[str] = []
    used = 0
    weight = 0
    opener: str | None = None  # the open fence's first line
    closer = ""
    for line, line_weight in zip(source, _line_weights(source)):
        reserve = len(closer) + 1 if opener is not None else 0
        width = max(1, limit - reserve - (len(opener) + 1 if opener else 0) - 1)
        for seg in [line[i:i + width] for i in range(0, len(line), width)] or [""]:
            full = used + len(seg) + 1 + reserve > limit or (
                max_blocks and weight + line_weight > max_blocks
            )
            if lines and full:
                if opener is not None:
                    lines.append(closer)
                pieces.append("\n".join(lines))
                lines = [opener] if opener is not None else []
                used = sum(len(kept) + 1 for kept in lines)
                weight = 1 if opener is not None else 0
            lines.append(seg)
            used += len(seg) + 1
            weight += line_weight
        if opener is None:
            m = _FENCE_OPEN_RE.match(line)
            if m:
                opener, closer = line, m.group(1) + m.group(2)
        elif _closes_fence(line, closer):
            opener, closer = None, ""
    if lines:
        pieces.append("\n".join(lines))
    return pieces


def _split_markdown(text: str, limit: int, max_blocks: int = 0) -> list[str]:
    """Markdown in parts of at most *limit* characters, cut between blocks.

    A part ends at a blank line and never inside a code fence; a block too
    long for one part is cut by :func:`_fit_block`. *max_blocks*, when set,
    also caps a part's block count as :func:`_line_weights` reckons it.
    Blank-line runs between blocks come out as a single blank line.
    """
    if not text:
        return []
    if _fits(text, limit, max_blocks):
        return [text]
    parts: list[str] = []
    current: list[str] = []
    size = 0
    blocks = 0
    for block in _markdown_blocks(text):
        for piece in _fit_block(block, limit, max_blocks):
            piece_blocks = _weight(piece) if max_blocks else 0
            if current and (
                size + 2 + len(piece) > limit
                or (max_blocks and blocks + piece_blocks > max_blocks)
            ):
                parts.append("\n\n".join(current))
                current, size, blocks = [], 0, 0
            if current:
                size += 2
            current.append(piece)
            size += len(piece)
            blocks += piece_blocks
    if current:
        parts.append("\n\n".join(current))
    return parts


def _escape_rich_text(text: str) -> str:
    """Escape what a rich message would misread in text outside code.

    ``$`` would open a formula ("$5 and $10" turns into one), and a ``<``
    that starts no tag Telegram renders would make the text after it vanish.
    A character the text already escapes with a backslash is left alone.
    """
    out: list[str] = []
    escaped = False
    for i, ch in enumerate(text):
        if escaped:
            out.append(ch)
            escaped = False
        elif ch == "\\":
            out.append(ch)
            escaped = True
        elif ch == "$":
            out.append("\\$")
        elif ch == "<" and not (
            _RICH_TAG_RE.match(text, i) or _AUTOLINK_RE.match(text, i)
        ):
            out.append("&lt;")
        else:
            out.append(ch)
    return "".join(out)


def _escape_rich_line(line: str) -> str:
    """:func:`_escape_rich_text` for one line, leaving its code spans alone."""
    out: list[str] = []
    pos = 0
    for m in _CODE_SPAN_RE.finditer(line):
        out.append(_escape_rich_text(line[pos:m.start()]))
        out.append(m.group(0))
        pos = m.end()
    out.append(_escape_rich_text(line[pos:]))
    return "".join(out)


def _rich_markdown(text: str) -> str:
    """An answer's Markdown, ready for a rich message.

    Rich messages parse GitHub-flavoured Markdown, which differs from how a
    Telegram message shows text in two ways that matter to an answer: a
    single newline joins two lines into one paragraph, and some characters
    are markup (see :func:`_escape_rich_text`). So each line break inside a
    paragraph becomes a hard break, and those characters are escaped. Code
    fences and code spans stay exactly as written, and so do tables. A table
    gets a blank line before and after it, because without one Telegram reads
    it as part of the paragraph next to it.
    """
    lines = text.split("\n")
    code = _code_lines(lines)
    count = len(lines)
    out: list[str] = []
    in_table = False
    piped_table = False  # the table's rows start with "|"
    for i, line in enumerate(lines):
        if code[i]:
            out.append(line)
            continue
        if not line.strip():
            in_table = False
            out.append(line)
            continue
        nxt = lines[i + 1] if i + 1 < count else ""
        nxt_code = code[i + 1] if i + 1 < count else True
        if _is_table_delim(line):
            in_table = True
            piped_table = line.lstrip().startswith("|")
        table_next = (
            not in_table
            and i + 2 < count
            and not code[i + 1] and not code[i + 2]
            and "|" in nxt
            and _is_table_delim(lines[i + 2])
        )
        line = _escape_rich_line(line)
        table_line = (
            in_table or line.lstrip().startswith("|") or _is_table_delim(nxt)
        )
        if (
            nxt.strip()
            and not nxt_code
            and not table_line
            and not table_next
            and not nxt.lstrip().startswith("|")
            and not _HEADING_RE.match(line)
            and not _RULE_RE.match(line)
            and not line.endswith(("  ", "\\"))
        ):
            line += "  "
        out.append(line)
        if table_next:
            out.append("")
        elif (
            in_table and piped_table and nxt.strip() and not nxt_code
            and not nxt.lstrip().startswith("|")
        ):
            out.append("")
            in_table = False
    return "\n".join(out)


def _rich_inline_text(node: Any) -> str:
    """The plain text of a rich message's inline content."""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return "".join(_rich_inline_text(part) for part in node)
    if isinstance(node, dict):
        if "expression" in node:
            return str(node["expression"])
        return _rich_inline_text(node.get("text", ""))
    return ""


def _rich_block_lines(blocks: Any) -> list[str]:
    """The plain-text lines of a list of rich message blocks."""
    lines: list[str] = []
    for block in blocks if isinstance(blocks, list) else ():
        if not isinstance(block, dict):
            continue
        if "summary" in block:
            lines.append(_rich_inline_text(block["summary"]))
        if "text" in block or "expression" in block:
            lines.append(_rich_inline_text(block))
        for row in block.get("cells") or ():
            if isinstance(row, list):
                lines.append(" | ".join(_rich_inline_text(cell) for cell in row))
        for item in block.get("items") or ():
            if isinstance(item, dict):
                lines.extend(_rich_block_lines(item.get("blocks")))
        lines.extend(_rich_block_lines(block.get("blocks")))
    return [line for line in lines if line]


def _rich_plain_text(message: Any) -> str:
    """The text of a rich message, or ``""`` for any other message.

    A rich message carries no ``text``: python-telegram-bot keeps the parsed
    blocks under ``api_kwargs["rich_message"]``, as a field it does not know.
    """
    extra = getattr(message, "api_kwargs", None)
    rich = extra.get("rich_message") if isinstance(extra, Mapping) else None
    if not isinstance(rich, Mapping):
        return ""
    return "\n".join(_rich_block_lines(rich.get("blocks")))


def _format_forward_context(message: Any) -> str:
    """Extract forward origin info from a Telegram message.

    Returns a prefix string like:
        [Forwarded from Иван Иванов]
        [Forwarded from Новости]
        [Forwarded from hidden user "Аноним"]

    Returns empty string if the message is not forwarded.
    """
    origin = getattr(message, "forward_origin", None)
    if not origin:
        return ""

    origin_type = getattr(origin, "type", None)

    if origin_type == "user":
        user = getattr(origin, "sender_user", None)
        if user:
            name = getattr(user, "first_name", "") or ""
            last = getattr(user, "last_name", "") or ""
            full = f"{name} {last}".strip() or "unknown"
            username = getattr(user, "username", None)
            if username:
                return f'[Forwarded from {full} (@{username})]'
            return f'[Forwarded from {full}]'

    elif origin_type == "hidden_user":
        name = getattr(origin, "sender_user_name", None) or "unknown"
        return f'[Forwarded from hidden user "{name}"]'

    elif origin_type == "chat":
        chat = getattr(origin, "sender_chat", None)
        if chat:
            title = getattr(chat, "title", None) or "unknown chat"
            return f'[Forwarded from chat "{title}"]'

    elif origin_type == "channel":
        chat = getattr(origin, "chat", None)
        if chat:
            title = getattr(chat, "title", None) or "unknown channel"
            return f'[Forwarded from channel "{title}"]'

    return "[Forwarded message]"


def _reply_of(message: Any) -> Any:
    """The message that *message* replies to, or ``None``.

    In a topic, Telegram sets ``reply_to_message`` on every message to the
    service message that created the topic (its id is the thread id),
    whether or not the user replied to anything. That is not a reply.
    """
    reply = getattr(message, "reply_to_message", None)
    if reply is None:
        return None
    if getattr(message, "is_topic_message", None) is True and (
        getattr(reply, "forum_topic_created", None) is not None
        or getattr(reply, "message_id", None)
        == getattr(message, "message_thread_id", None)
    ):
        return None
    return reply


def _format_reply_context(message: Any) -> str:
    """Extract reply-to context and quote from a Telegram message.

    Returns a prefix string like:
        [Reply to assistant: "original text here"]
        [Quoted: "selected portion"]

    Returns empty string if the message is not a reply.
    """
    reply = _reply_of(message)
    if not reply:
        return ""

    parts: list[str] = []

    # Determine sender label
    from_user = getattr(reply, "from_user", None)
    if from_user and getattr(from_user, "is_bot", False):
        sender = "assistant"
    elif from_user:
        sender = getattr(from_user, "first_name", None) or "user"
    else:
        sender = "user"

    # Original message text
    original = (
        getattr(reply, "text", None)
        or getattr(reply, "caption", None)
        or _rich_plain_text(reply)
    )
    if original:
        display = original if len(original) <= 500 else original[:500] + "…"
        parts.append(f'[Reply to {sender}: "{display}"]')
    else:
        parts.append(f"[Reply to {sender}'s message]")

    # Quote (manually selected text)
    quote = getattr(message, "quote", None)
    if quote:
        quote_text = getattr(quote, "text", None)
        if quote_text:
            parts.append(f'[Quoted: "{quote_text}"]')

    return "\n".join(parts)


def _describe_telegram_attachment(message: Any) -> str:
    """Name a message's attachment without downloading it.

    An uncaptioned photo or sticker still says something. ``_extract_sticker``
    and ``_extract_document`` say more but fetch the file to do it.
    """
    sticker = getattr(message, "sticker", None)
    if sticker:
        return f"[Sticker: {sticker.emoji}]" if sticker.emoji else "[Sticker]"
    document = getattr(message, "document", None)
    if document:
        return f"[File: {document.file_name or 'unnamed'}]"
    if getattr(message, "photo", None):
        return "[Photo]"
    return ""


# Inline-keyboard /sessions rendering ------------------------------------- #
_SESSIONS_PAGE_SIZE = 10        # sessions shown per /sessions page; ⬅️/➡️ page the rest
_SESSION_LABEL_MAX = 40         # Telegram wraps long button labels poorly


def _session_label(session: dict, current_id: str | None) -> str:
    """Button label for one session: title (or short id), current marked ✓,
    starred (kept-alive) marked ⭐."""
    title = (session.get("title") or "").strip() or session.get("id", "?")
    if len(title) > _SESSION_LABEL_MAX:
        title = title[: _SESSION_LABEL_MAX - 1] + "…"
    prefix = "✓ " if session.get("id") == current_id else ""
    if session.get("starred"):
        prefix += "⭐ "
    return f"{prefix}{title}"


def build_sessions_view(
    sessions: list[dict],
    current_id: str | None,
    *,
    offset: int = 0,
    has_prev: bool = False,
    has_next: bool = False,
    topic_buttons: bool = False,
) -> "tuple[str, InlineKeyboardMarkup]":
    """Render the /sessions inline keyboard (pure, sync — unit-testable).

    With ``topic_buttons`` (the list was asked for in a chat with topics,
    outside any topic) each row also offers ``↗ Topic`` (``sesstopic:<id>``),
    which opens that session in a topic of its own.

    One tap-to-switch button per session (the current one marked ✓), with the
    session id carried in ``callback_data`` as ``sess:<id>`` — so switching
    routing takes a single tap and never requires copy-pasting an id on mobile.
    ``sessions`` is one already-ordered page (the caller pins the current
    session first and starred ones ahead of merely-recent ones); ``⬅️``/``➡️``
    page through the rest via ``sess:page:<offset>`` when ``has_prev`` /
    ``has_next`` report more exist. A trailing ``➕ New session`` button
    (``sess:new``) starts a fresh one. Returns ``(message_text, keyboard)``.
    """
    rows: list[list[InlineKeyboardButton]] = []
    for s in sessions[:_SESSIONS_PAGE_SIZE]:
        sid = s.get("id")
        cb = f"sess:{sid}"
        # callback_data is capped at 64 bytes by Telegram; interactive session
        # ids are short, but guard defensively so a button always round-trips.
        if sid is None or len(cb.encode("utf-8")) > 64:
            continue
        row = [InlineKeyboardButton(_session_label(s, current_id), callback_data=cb)]
        topic_cb = f"sesstopic:{sid}"
        if topic_buttons and len(topic_cb.encode("utf-8")) <= 64:
            row.append(InlineKeyboardButton("↗ Topic", callback_data=topic_cb))
        rows.append(row)
    shown = len(rows)

    # Pager row — only the arrows that lead somewhere, so no tap is a dead end.
    nav: list[InlineKeyboardButton] = []
    if has_prev:
        nav.append(
            InlineKeyboardButton(
                "⬅️ Prev",
                callback_data=f"sess:page:{max(0, offset - _SESSIONS_PAGE_SIZE)}",
            )
        )
    if has_next:
        nav.append(
            InlineKeyboardButton(
                "➡️ More",
                callback_data=f"sess:page:{offset + _SESSIONS_PAGE_SIZE}",
            )
        )
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("➕ New session", callback_data="sess:new")])

    # The New-session button switches routing to a fresh session WITHOUT
    # stopping the current one (unlike /new). A session switched away from
    # keeps running any in-flight turn and still delivers its result to the
    # chat (output is bound per-session, not to the channel's current map).
    if shown:
        current_title = next(
            (
                (s.get("title") or s.get("id"))
                for s in sessions
                if s.get("id") == current_id
            ),
            None,
        )
        text = "🗂 Sessions — tap to switch."
        if current_title:
            text += f"\nCurrent: {current_title}"
        if offset or has_next:
            text += f"\nPage {offset // _SESSIONS_PAGE_SIZE + 1}"
        text += "\n➕ New session keeps the current one running."
        if topic_buttons:
            text += "\n↗ Topic opens a session in a topic of its own."
        text += "\n⭐ = kept alive (never auto-closed); /star or /unstar the current session."
    elif has_prev:
        text = "No more sessions — tap ⬅️ Prev to go back."
    else:
        text = "No sessions yet — tap ➕ to start one."
    return text, InlineKeyboardMarkup(rows)


# Catch-up tail shown after switching to a session -------------------------- #
_TAIL_WINDOW = 8        # messages fetched for the catch-up view on switch
_TAIL_STEP = 8          # extra messages fetched per "Load more"
_TAIL_MAX = 30          # hard cap on how many messages to fetch
# Rendered-card char budget. The newest message (usually the one you most want
# to read) gets first claim up to _NEWEST_MAX; older messages get _OLDER_CAP
# each from what remains. Bodies render as expandable blockquotes, so long
# messages collapse to a few lines and expand in place — no hard truncation for
# normal-sized messages, while the whole card stays under Telegram's 4096 limit.
_TAIL_CARD_MAX = 3800   # max chars of the assembled HTML card (< Telegram's 4096)
_NEWEST_MAX = 2000      # newest message body clip (raw) — it gets first/largest claim
_OLDER_CAP = 700        # older message body clip (raw)
_STATUS_EMOJI = {
    "active": "🟢",     # a client is connected (live / possibly mid-turn)
    "running": "🟢",    # defensive alias
    "idle": "💤",       # no client attached, resumable
    "stopped": "⏹",
    "error": "⚠️",
    "archived": "🗄",
}


def _safe_zone(tzname: str | None):
    """ZoneInfo for the user's configured tz, or None → fall back to system local."""
    if not tzname:
        return None
    try:
        return ZoneInfo(tzname)
    except Exception:
        return None


def _clip(text: str, cap: int) -> str:
    """Trim to `cap` visible chars and HTML-escape; '…' appended if truncated."""
    t = (text or "").strip()
    if len(t) > cap:
        t = t[:cap].rstrip() + "…"
    return _html.escape(t) if t else "(no text)"


# Telegram's limit for a callback-query toast (answerCallbackQuery text).
_TOAST_MAX_LEN = 200


def _fit_text(text: str, limit: int) -> str:
    """Cut `text` to at most `limit` characters; '…' appended if cut."""
    # Count UTF-16 code units (an emoji is two), which never undercounts.
    if len(text.encode("utf-16-le")) // 2 <= limit:
        return text
    units = 0
    for end, char in enumerate(text):
        units += 2 if ord(char) > 0xFFFF else 1
        if units >= limit:  # no room for this char and the ellipsis
            break
    return text[:end] + "…"


def _fmt_local(iso: str, tz) -> "tuple[str, str]":
    """Return (day label, HH:MM) for an ISO timestamp in the user's timezone."""
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local = dt.astimezone(tz) if tz else dt.astimezone()
        return local.strftime("%a %b %d"), local.strftime("%H:%M")
    except Exception:
        return "", "--:--"


def build_session_tail_view(
    session: dict,
    messages: list[dict],
    total: int,
    window: int,
    tzname: str | None,
) -> "tuple[str, InlineKeyboardMarkup]":
    """Render a session's recent history (HTML) so the user can catch up.

    Native chat order — oldest at the top, most recent at the bottom — with a
    per-message timestamp converted to the user's timezone (``tzname``) so it
    matches the Telegram client (the timestamp is often as useful as the text).
    Each body is an *expandable blockquote*: long messages collapse to a few
    lines and expand in place on tap, so there is no hard truncation for normal
    messages. The char budget is newest-first — the most recent message (usually
    the important one) gets the largest share — while the whole card stays under
    Telegram's 4096-char limit. ``messages`` is the last ``window`` messages
    (oldest→newest). "Load more" fetches more older history; it does not change
    routing. Returns ``(html_text, keyboard)`` — send/edit with ParseMode.HTML.
    """
    tz = _safe_zone(tzname)
    sid = session.get("id", "?")
    status = session.get("status") or "idle"
    emoji = _STATUS_EMOJI.get(status, "•")
    title = _html.escape(str(session.get("title") or sid))
    header = (
        f"🗂 <b>{title}</b>\n{emoji} {status} · "
        f"{total} message" + ("" if total == 1 else "s")
    )

    if not messages:
        rows = [[InlineKeyboardButton("🗂 Sessions", callback_data="sess:list")]]
        return header + "\n\n<i>(no messages yet — send one to begin)</i>", InlineKeyboardMarkup(rows)

    # Greedy newest-first: the newest message is added first with the largest
    # clip (so it gets the most budget and is never starved); older messages are
    # added while the assembled card stays within budget. The budget is measured
    # on the *rendered, HTML-escaped* block length, so entity expansion (e.g. an
    # apostrophe → "&#x27;") can never push the card past Telegram's 4096 limit.
    included: list[tuple[str, str]] = []          # (day_label, block) newest-first
    used = len(header) + 64                        # header + room for the "earlier" line
    for idx, m in enumerate(reversed(messages)):   # newest → oldest
        cap = _NEWEST_MAX if idx == 0 else _OLDER_CAP
        day, hm = _fmt_local(m.get("created_at", ""), tz)
        who = "🧑" if m.get("role") == "user" else "🤖"
        block = (
            f"[{hm}] {who}\n"
            f"<blockquote expandable>{_clip(m.get('content', ''), cap)}</blockquote>"
        )
        cost = len(block) + len(day) + 6           # + possible day separator
        if idx > 0 and used + cost > _TAIL_CARD_MAX:
            break                                  # older message doesn't fit
        included.append((day, block))
        used += cost
    included.reverse()                             # oldest→newest for display

    earlier = total - len(included)
    # "Load more" fetches more history; it only helps when everything fetched was
    # shown and more exists (window-bound). If messages were dropped to fit the
    # card (budget-bound), fetching more won't surface them — point to the session.
    can_load_more = (
        len(included) == len(messages) and total > window and window < _TAIL_MAX
    )

    lines = [header]
    if earlier > 0:
        hint = "tap Load more" if can_load_more else "open the session for the rest"
        lines.append(f"<i>… {earlier} earlier — {hint}</i>")
    last_day = None
    for day, block in included:
        if day and day != last_day:
            lines.append(f"— {day} —")
            last_day = day
        lines.append(block)
    text = "\n".join(lines)

    rows: list[list[InlineKeyboardButton]] = []
    if can_load_more:
        nxt = min(window + _TAIL_STEP, _TAIL_MAX)
        rows.append(
            [InlineKeyboardButton("⬆️ Load more", callback_data=f"sesstail:{sid}:{nxt}")]
        )
    rows.append([InlineKeyboardButton("🗂 Sessions", callback_data="sess:list")])
    return text, InlineKeyboardMarkup(rows)


# Inline-keyboard /plans rendering ---------------------------------------- #
# The plan review queue (WebUI parity for chat): list → detail → approve /
# decline / revise, all tap-driven so nothing needs a copy-pasted plan id.
_PLANS_PAGE_SIZE = 8            # plans shown per /plans page; ⬅️/➡️ page the rest
_PLAN_LABEL_MAX = 40           # Telegram wraps long button labels poorly
_PLAN_TITLE_MAX = 200          # task title clip on the detail card
_PLAN_BODY_MAX = 3400          # plan body clip on the detail card, in displayed characters
_PLAN_REVISE_TTL = 24 * 3600   # seconds an unanswered ✍️ Revise prompt stays valid
_TG_TEXT_MAX = 4096            # Telegram's message limit, counted on the displayed text
_PLAN_STATUS_EMOJI = {
    "pending": "🟠",           # awaiting your review — the actionable state
    "implementing": "⚙️",      # approved, an impl session is running
    "declined": "🚫",
    "superseded": "🗂",
    "failed": "⚠️",
}


def build_session_transcript(
    session: dict, messages: list[dict], tzname: str | None,
) -> str:
    """The session's conversation as one Markdown document (oldest first).

    Telegram cannot move messages into a topic, so a session opened in a new
    topic offers this instead of replaying its history message by message.
    """
    tz = _safe_zone(tzname)
    sid = session.get("id") or ""
    lines = [f"# {session.get('title') or sid}", "", f"Session `{sid}`", ""]
    for m in messages:
        who = {"user": "You", "assistant": "Assistant"}.get(m.get("role") or "")
        content = (m.get("content") or "").strip()
        if not who or not content:
            continue
        day, hm = _fmt_local(m.get("created_at") or "", tz)
        lines += [f"## {who} · {day} {hm}", "", content, ""]
    return "\n".join(lines)


def _transcript_filename(session: dict) -> str:
    """``<title>.md``, safe on any file system; the id when there is no title."""
    stem = re.sub(r"[^\w.-]+", "-", session.get("title") or "").strip("-.")
    return f"{stem[:60] or session.get('id') or 'session'}.md"


def _tg_len(text: str) -> int:
    """Length as Telegram counts it: UTF-16 code units."""
    return len(text.encode("utf-16-le")) // 2


def _tg_visible_len(html_text: str) -> int:
    """Telegram length of an HTML message as displayed (tags dropped, entities decoded)."""
    return _tg_len(_html.unescape(re.sub(r"<[^>]*>", "", html_text)))


def _clip_tg(text: str, cap: int) -> str:
    """Like ``_clip``, but ``cap`` counts UTF-16 code units, as Telegram does."""
    t = (text or "").strip()
    if not t:
        return "(no text)"
    if _tg_len(t) > cap:
        kept: list[str] = []
        used = 0
        for ch in t:
            used += 2 if ord(ch) > 0xFFFF else 1
            if used > cap - 1:         # leave room for the ellipsis
                break
            kept.append(ch)
        t = "".join(kept).rstrip() + "…"
    return _html.escape(t)


def _plan_label(plan: dict) -> str:
    """Button label for one plan: status emoji + task title, version if > 1."""
    emoji = _PLAN_STATUS_EMOJI.get(plan.get("status") or "", "•")
    title = (plan.get("task_title") or plan.get("task_id") or "?").strip()
    ver = plan.get("version") or 1
    suffix = f"  v{ver}" if int(ver) > 1 else ""
    room = _PLAN_LABEL_MAX - len(suffix) - 2   # 2 ≈ emoji + space
    if len(title) > room:
        title = title[: room - 1] + "…"
    return f"{emoji} {title}{suffix}"


def build_plans_view(
    plans: list[dict],
    *,
    offset: int = 0,
    has_prev: bool = False,
    has_next: bool = False,
) -> "tuple[str, InlineKeyboardMarkup]":
    """Render the /plans review queue (pure, sync — unit-testable).

    One tap-to-open button per plan (the plan id rides in ``callback_data``
    as ``plan:view:<id>``), pending listed ahead of implementing. ``⬅️``/``➡️``
    page via ``plan:list:<offset>`` when more exist, and a trailing
    ``🔄 Refresh`` button (``plan:list:0``) is always present so the keyboard
    is never empty and the queue can be re-pulled after acting on a plan.
    Returns ``(message_text, keyboard)``.
    """
    rows: list[list[InlineKeyboardButton]] = []
    for p in plans[:_PLANS_PAGE_SIZE]:
        pid = p.get("id")
        cb = f"plan:view:{pid}"
        # callback_data is capped at 64 bytes by Telegram; plan ids are short
        # (``plan-xxxxxxxx``) but guard defensively so a button always works.
        if not pid or len(cb.encode("utf-8")) > 64:
            continue
        rows.append([InlineKeyboardButton(_plan_label(p), callback_data=cb)])
    shown = len(rows)

    # Pager row — only the arrows that lead somewhere.
    nav: list[InlineKeyboardButton] = []
    if has_prev:
        nav.append(InlineKeyboardButton(
            "⬅️ Prev",
            callback_data=f"plan:list:{max(0, offset - _PLANS_PAGE_SIZE)}",
        ))
    if has_next:
        nav.append(InlineKeyboardButton(
            "➡️ More",
            callback_data=f"plan:list:{offset + _PLANS_PAGE_SIZE}",
        ))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="plan:list:0")])

    if shown:
        text = "🗒 Plans awaiting your review — tap one to read, approve, or decline."
        if offset or has_next:
            text += f"\nPage {offset // _PLANS_PAGE_SIZE + 1}"
        text += "\n🟠 pending · ⚙️ implementing"
    elif has_prev:
        text = "No more plans — tap ⬅️ Prev to go back."
    else:
        text = "✅ No plans awaiting review — nothing to approve right now."
    return text, InlineKeyboardMarkup(rows)


def build_plan_detail_view(
    plan: dict,
    *,
    tzname: str | None = None,
) -> "tuple[str, InlineKeyboardMarkup]":
    """Render one plan (HTML) with its review actions.

    The plan body is an *expandable blockquote* — long plans collapse to a
    few lines and expand in place on tap. The title and body are clipped
    before escaping, so the markup stays intact and the displayed card fits
    Telegram's 4096-character limit. A ``pending`` plan gets Approve /
    Decline / Revise buttons; any other status is read-only (with its impl
    session shown when present). Send/edit with ``ParseMode.HTML``. Returns
    ``(html_text, keyboard)``.
    """
    tz = _safe_zone(tzname)
    pid = str(plan.get("id") or "?")
    status = plan.get("status") or "?"
    emoji = _PLAN_STATUS_EMOJI.get(status, "•")
    task_title = _clip(
        str(plan.get("task_title") or plan.get("task_id") or "?"), _PLAN_TITLE_MAX,
    )
    version = plan.get("version") or 1
    ptype = plan.get("plan_type") or "generic"

    header = (
        f"🗒 <b>{task_title}</b>\n"
        f"{emoji} {status} · plan <code>{_html.escape(pid)}</code> v{version}"
    )
    if ptype and ptype != "generic":
        header += f" · {_html.escape(str(ptype))}"
    created = plan.get("created_at") or ""
    if created:
        day, hm = _fmt_local(created, tz)
        if day:
            header += f"\n<i>proposed {day} {hm}</i>"
    if plan.get("impl_session_id"):
        header += f"\nimpl: <code>{_html.escape(str(plan['impl_session_id']))}</code>"
    if plan.get("feedback"):
        header += f"\n<b>Revision feedback:</b> {_clip(str(plan['feedback']), 300)}"

    head = f"{header}\n\n"
    # Telegram applies its limit to the displayed text, so the body gets
    # whatever room the displayed header leaves.
    budget = min(_PLAN_BODY_MAX, _TG_TEXT_MAX - _tg_visible_len(head))
    body = _clip_tg(str(plan.get("content") or ""), budget)
    text = f"{head}<blockquote expandable>{body}</blockquote>"

    rows: list[list[InlineKeyboardButton]] = []
    if status == "pending":
        rows.append([
            InlineKeyboardButton("✅ Approve", callback_data=f"plan:approve:{pid}"),
            InlineKeyboardButton("❌ Decline", callback_data=f"plan:decline:{pid}"),
        ])
        rows.append([InlineKeyboardButton("✍️ Revise", callback_data=f"plan:revise:{pid}")])
    rows.append([InlineKeyboardButton("🗒 Plans", callback_data="plan:list:0")])
    return text, InlineKeyboardMarkup(rows)


def build_plan_confirm_view(
    plan: dict, action: str,
) -> "tuple[str, InlineKeyboardMarkup]":
    """Confirmation card for a consequential action (approve/decline).

    Both actions are one-way (approve spawns an implementation session;
    decline closes the task), so a fat-finger tap gets a second step. Plain
    text (no parse mode). ``◀️ Back`` returns to the plan detail. Returns
    ``(message_text, keyboard)``.
    """
    pid = str(plan.get("id") or "?")
    title = (plan.get("task_title") or plan.get("task_id") or "?")
    if action == "approve":
        text = (
            f"✅ Approve the plan for “{title}”?\n\n"
            "This starts an implementation session that carries out the plan."
        )
        rows = [
            [InlineKeyboardButton(
                "✅ Yes, approve & implement", callback_data=f"plan:approveok:{pid}")],
            [InlineKeyboardButton("◀️ Back", callback_data=f"plan:view:{pid}")],
        ]
    else:  # decline
        text = (
            f"❌ Decline the plan for “{title}”?\n\n"
            "This closes the task as done. To ask for a new version instead, "
            "go back and use ✍️ Revise."
        )
        rows = [
            [InlineKeyboardButton(
                "🗑 Yes, decline & close", callback_data=f"plan:declineok:{pid}")],
            [InlineKeyboardButton("◀️ Back", callback_data=f"plan:view:{pid}")],
        ]
    return text, InlineKeyboardMarkup(rows)


class TelegramChannel(BaseChannel):
    """Telegram bot channel.

    Handles the Telegram bot transport: polling, commands, auth.
    Delegates session management and agent execution to the ChannelRouter.
    """

    def __init__(
        self, config: Callable[[], NerveConfig], router: ChannelRouter,
    ):
        self._config = config
        self.router = router
        self._app: Application | None = None
        self._allowed_users: set[int] = set(self.config.telegram.allowed_users)
        self._notification_service = None  # Set after service is created
        self._watchdog_task: asyncio.Task | None = None
        self._stopping = False
        self._last_update_time: float = 0.0  # monotonic, set on any incoming update
        # Media group (album) collection: group_id -> list of updates
        self._media_groups: dict[str, list[Update]] = {}
        self._media_group_tasks: dict[str, asyncio.Task] = {}
        # Message cache for reaction context:
        # (chat_id, message_id) -> (thread_id, text_snippet). A reaction update
        # carries no topic, so the topic a reaction belongs to comes from here.
        self._message_cache: collections.OrderedDict[
            tuple[int, int], tuple[int | None, str]
        ] = collections.OrderedDict()
        self._message_cache_max = 200
        # First-message binding of a topic to its session: channel_key -> lock.
        # Updates run concurrently, so two quick messages in a new topic would
        # otherwise each create a session.
        self._topic_locks: dict[str, asyncio.Lock] = {}
        # Opening a session's topic in a chat: "<chat>:<session>" -> lock, so
        # two calls at once (a retried request, a tap and a call) open one.
        self._open_topic_locks: dict[str, asyncio.Lock] = {}
        # Streaming edit budget per chat: chat_id -> monotonic ts of last edit
        self._last_chat_edit: collections.OrderedDict[int, float] = (
            collections.OrderedDict()
        )
        # Set when the Bot API server has no sendRichMessage (an older
        # self-hosted one): answers go out as HTML until restart.
        self._rich_unavailable = False
        # Rate limiter for replies to unauthorized users: user_id -> monotonic ts
        self._unauth_reply_times: dict[int, float] = {}
        # /plans revise flow: (chat_id, prompt_message_id) ->
        # (plan_id, user_id, monotonic ts). A reply to one of these ForceReply
        # prompts from the user who tapped Revise is revision feedback, not a
        # message for the agent (see _handle_message).
        self._plan_revise_prompts: dict[tuple[int, int], tuple[str, int, float]] = {}

    def set_notification_service(self, service) -> None:
        """Wire the notification service for callback query handling."""
        self._notification_service = service

    @property
    def bot_username(self) -> str | None:
        """The bot's ``@username`` once ``Application.initialize()`` ran, else ``None``.

        Read by the setup page so the process that comes up after the restart
        can offer the ``t.me`` pairing link without a second ``getMe``. Never
        raises: a half-built application answers ``None``.
        """
        app = self._app
        if app is None:
            return None
        try:
            bot = app.bot
            name = getattr(bot, "username", None)
        except Exception:  # noqa: BLE001 — python-telegram-bot raises before initialize()
            return None
        return str(name) if name else None

    @property
    def allowed_users(self) -> frozenset[int]:
        """The allow-list the running bot honours (a frozen copy)."""
        return frozenset(self._allowed_users)

    @property
    def config(self) -> NerveConfig:
        """The live config, resolved per read rather than captured.

        The bot outlives every reload, and ``dm_policy`` decides on each update
        whether a stranger may talk to the agent. Holding the start-up object
        meant a reload that tightened ``open`` to ``pairing`` reported success
        and authorized everyone until the daemon was restarted.

        Only the reads that happen per use follow from this. The bot token was
        handed to the Application at build time and ``allowed_users`` was copied
        into a set, so both still need a restart.
        """
        return self._config()

    @property
    def name(self) -> str:
        return "telegram"

    @property
    def capabilities(self) -> ChannelCapability:
        caps = (
            ChannelCapability.SEND_TEXT
            | ChannelCapability.MARKDOWN
            | ChannelCapability.TYPING_INDICATOR
            | ChannelCapability.REACTIONS
            | ChannelCapability.SEND_FILES
        )
        if self.config.telegram.stream_mode == "partial":
            caps |= ChannelCapability.STREAMING
        return caps

    @property
    def constraints(self) -> ChannelConstraints:
        return ChannelConstraints(
            max_message_length=MAX_MSG_LEN,
            min_edit_interval=EDIT_INTERVAL,
            supports_message_edit=True,
        )

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def _build_application(self) -> Application:
        """Build a PTB Application with robust connection settings."""
        builder = (
            Application.builder()
            .token(self.config.telegram.bot_token)
            # Process updates concurrently so /stop can interrupt a running
            # message handler instead of being queued behind it.
            .concurrent_updates(True)
            # TCP keepalive on the polling connection — prevents NAT/firewall
            # from silently dropping the long-poll connection
            .get_updates_socket_options(_TCP_KEEPALIVE_OPTS)
            # Separate connection pool for polling vs sending, so a stuck
            # outbound request can't starve the polling connection
            .get_updates_connection_pool_size(1)
            .get_updates_read_timeout(10)
            .get_updates_connect_timeout(10)
            .get_updates_pool_timeout(1.0)
        )
        app = builder.build()

        # A CommandHandler wins over the generic MessageHandler in this group,
        # so commands need the source hook on their own callback.
        app.add_handler(CommandHandler(
            "start", self._source_routed_command(self._handle_start),
        ))
        # Never persist a one-time pairing code in the source buffer.
        app.add_handler(CommandHandler(
            "pair", self._source_routed_command(self._handle_pair, collect=False),
        ))
        app.add_handler(CommandHandler(
            "session", self._source_routed_command(self._handle_session),
        ))
        app.add_handler(CommandHandler(
            "sessions", self._source_routed_command(self._handle_sessions),
        ))
        app.add_handler(CommandHandler(
            "plans", self._source_routed_command(self._handle_plans),
        ))
        app.add_handler(CommandHandler(
            "star", self._source_routed_command(self._handle_star),
        ))
        app.add_handler(CommandHandler(
            "unstar", self._source_routed_command(self._handle_unstar),
        ))
        app.add_handler(CommandHandler(
            "new", self._source_routed_command(self._handle_new_session),
        ))
        app.add_handler(CommandHandler(
            "stop", self._source_routed_command(self._handle_stop),
        ))
        app.add_handler(CommandHandler(
            "restart", self._source_routed_command(self._handle_restart),
        ))
        app.add_handler(CommandHandler(
            "doctor", self._source_routed_command(self._handle_doctor),
        ))
        app.add_handler(CommandHandler(
            "reply", self._source_routed_command(self._handle_reply),
        ))
        app.add_handler(CallbackQueryHandler(self._handle_callback_query))
        app.add_handler(MessageHandler(
            filters.StatusUpdate.FORUM_TOPIC_CREATED
            | filters.StatusUpdate.FORUM_TOPIC_EDITED
            | filters.StatusUpdate.FORUM_TOPIC_CLOSED,
            self._handle_topic_service,
        ))
        app.add_handler(MessageHandler(
            filters.TEXT | filters.PHOTO | filters.COMMAND | filters.Sticker.ALL | filters.Document.ALL,
            self._handle_message,
        ))
        app.add_handler(MessageReactionHandler(self._handle_reaction))
        app.add_error_handler(self._handle_error)

        return app

    async def start(self) -> None:
        """Start the Telegram bot."""
        if not self.config.telegram.bot_token:
            logger.warning("Telegram bot token not configured")
            return

        self._stopping = False
        self._app = self._build_application()

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(
            # Do NOT drop updates that queued while we were offline.
            # `nerve restart` (and any daemon respawn) goes through this
            # startup path; with drop_pending_updates=True, every message
            # the user sent during the restart window was silently discarded
            # by Telegram and never redelivered — a real "lost message" bug.
            # Telegram retains updates ~24h and only advances the offset once
            # we fetch them, so False makes a restart pick up the backlog.
            # The watchdog _rebuild() path already uses False; keep parity.
            drop_pending_updates=False,
            # Retry initial connection indefinitely — don't give up on
            # transient network errors during startup
            bootstrap_retries=-1,
            # Explicitly request all update types — the default set excludes
            # message_reaction, and auto-detection only works via
            # Application.run_polling(), not Updater.start_polling().
            allowed_updates=Update.ALL_TYPES,
        )
        self._last_update_time = time.monotonic()
        logger.info("Telegram bot started polling (with TCP keepalive)")

        self._announce_auth_state()

        # Launch watchdog for monitoring + recovery
        self._watchdog_task = asyncio.create_task(
            self._run_watchdog(), name="telegram-polling-watchdog",
        )

    def _announce_auth_state(self) -> None:
        """Log how DM authorization is configured — loudly when it matters.

        A fresh install with an empty allowlist used to be silently dead:
        the bot polled fine but rejected every DM with only a log line.
        Now pairing mode generates a code and tells the operator what to do.
        """
        tg = self.config.telegram
        if tg.dm_policy == "open":
            logger.warning(
                "telegram.dm_policy is 'open' — ANY Telegram user can talk to "
                "this bot and use the agent. Set dm_policy: pairing unless you "
                "really want this."
            )
            return
        if not self._allowed_users:
            from nerve import pairing

            code = pairing.get_or_create_pairing_code()
            logger.info(
                "Telegram: no allowed_users configured — pairing mode active. "
                "In a private chat, send the bot:  /pair %s  to authorize "
                "your account "
                "(code valid for 1h; run `nerve pair` to get a fresh one).",
                code,
            )

    async def stop(self) -> None:
        self._stopping = True
        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
        if self._app:
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()

    # ------------------------------------------------------------------ #
    #  Watchdog — monitor both Updater and Application health              #
    # ------------------------------------------------------------------ #

    async def _run_watchdog(self) -> None:
        """Monitor Telegram bot health and log diagnostics.

        Checks BOTH the Updater (fetches updates from Telegram) AND the
        Application (processes updates from queue → handlers).  Previous
        versions only checked the Updater, missing cases where the
        Application's update-processing task crashed silently.
        """
        check_count = 0
        while not self._stopping:
            try:
                await asyncio.sleep(WATCHDOG_INTERVAL)
            except asyncio.CancelledError:
                break

            if self._app is None or self._stopping:
                break

            check_count += 1
            status = self._get_health_status()

            # Periodic heartbeat
            if check_count % WATCHDOG_HEARTBEAT_EVERY == 0:
                logger.info(
                    "Telegram watchdog: %s (check #%d, last_update=%s, queue=%d)",
                    status["summary"], check_count,
                    status["last_update_ago"], status["queue_size"],
                )

            if not status["healthy"]:
                logger.warning(
                    "Telegram bot unhealthy: %s (queue=%d, last_update=%s) — rebuilding",
                    status["summary"], status["queue_size"],
                    status["last_update_ago"],
                )
                try:
                    await self._rebuild()
                    logger.info("Telegram bot rebuilt successfully")
                except Exception as e:
                    logger.error("Telegram bot rebuild failed: %s", e, exc_info=True)

    def _get_health_status(self) -> dict:
        """Check health of both Updater and Application."""
        now = time.monotonic()
        stale_sec = now - self._last_update_time if self._last_update_time > 0 else 0
        last_ago = f"{stale_sec:.0f}s ago" if self._last_update_time > 0 else "never"

        result = {
            "healthy": True,
            "summary": "ok",
            "last_update_ago": last_ago,
            "queue_size": 0,
        }

        if not self._app:
            result.update(healthy=False, summary="no Application")
            return result

        # --- Check update queue ---
        # If updates pile up in the queue, the Application isn't consuming them
        try:
            result["queue_size"] = self._app.update_queue.qsize()
        except Exception:
            pass

        # --- Check Updater (the part that fetches from Telegram) ---
        updater = self._app.updater
        if not updater or not updater.running:
            result.update(healthy=False, summary="updater not running")
            return result

        polling_task: asyncio.Task | None = getattr(
            updater, "_Updater__polling_task", None,
        )
        if polling_task is not None and polling_task.done():
            exc = None
            try:
                exc = polling_task.exception()
            except (asyncio.CancelledError, asyncio.InvalidStateError):
                pass
            result.update(
                healthy=False,
                summary=f"polling task dead (exception: {exc})",
            )
            return result

        # --- Check Application (the part that processes updates → handlers) ---
        fetcher_task: asyncio.Task | None = getattr(
            self._app, "_Application__update_fetcher_task", None,
        )
        if fetcher_task is not None and fetcher_task.done():
            exc = None
            try:
                exc = fetcher_task.exception()
            except (asyncio.CancelledError, asyncio.InvalidStateError):
                pass
            result.update(
                healthy=False,
                summary=f"update fetcher task dead (exception: {exc})",
            )
            return result

        # --- Check for backed-up queue (Application alive but not consuming) ---
        if result["queue_size"] > 10:
            result.update(
                healthy=False,
                summary=f"update queue backed up ({result['queue_size']} pending)",
            )
            return result

        return result

    async def _rebuild(self) -> None:
        """Tear down and rebuild the entire PTB Application."""
        old_app = self._app
        self._app = self._build_application()

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(
            drop_pending_updates=False,
            bootstrap_retries=-1,
            allowed_updates=Update.ALL_TYPES,
        )
        self._last_update_time = time.monotonic()

        # Best-effort cleanup of the old app
        if old_app:
            try:
                await asyncio.wait_for(old_app.updater.stop(), timeout=5)
            except Exception:
                pass
            try:
                await asyncio.wait_for(old_app.stop(), timeout=5)
            except Exception:
                pass
            try:
                await asyncio.wait_for(old_app.shutdown(), timeout=5)
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    #  Outbound: send complete message                                     #
    # ------------------------------------------------------------------ #

    async def send(self, message: OutboundMessage) -> None:
        """Send a message to a Telegram chat, or to a topic in it.

        With ``telegram.rich_messages`` on, the text goes out as rich
        messages of up to 32,768 characters, and a part Telegram refuses is
        sent as HTML instead. Otherwise it goes out as HTML messages of up to
        4,096 characters. Either way a long text is cut between blocks.
        """
        if self._app is None:
            return
        chat_id, thread_id = parse_target(message.target)
        session_id = message.session_id
        if not self._rich_on():
            await self._send_html(chat_id, thread_id, message.text, session_id)
            return
        for part in _split_markdown(message.text, RICH_MSG_LEN, _RICH_MAX_BLOCKS):
            sent = await self._send_rich(chat_id, thread_id, part)
            if sent is None:
                await self._send_html(chat_id, thread_id, part, session_id)
            else:
                self._cache_message(sent.message_id, chat_id, part, thread_id)
                await self._record_reply_route(sent.message_id, chat_id, session_id)

    @staticmethod
    async def _with_retry(send: Callable[[], Awaitable[Any]]) -> Any:
        """Run *send*, and once more after Telegram's advised wait if it is rate limited.

        Topics put several sessions in one chat, and they share its rate
        limit. A final answer is worth one wait, rather than leaving the
        half-streamed placeholder.
        """
        try:
            return await send()
        except RetryAfter as exc:
            wait = _retry_after_seconds(exc)
            if wait > _SEND_RETRY_MAX_WAIT:
                raise
            await asyncio.sleep(wait)
            return await send()

    async def _send_html(
        self, chat_id: int, thread_id: int | None, text: str,
        session_id: str | None = None,
    ) -> None:
        """Send Markdown *text* as HTML messages of up to 4,096 characters, cut between blocks.

        Each message records a reply route to *session_id* (see ``_record_reply_route``).
        """
        for chunk in _split_markdown(text, MAX_MSG_LEN):
            sent = await self._with_retry(
                functools.partial(self._send_chunk, chat_id, thread_id, chunk),
            )
            self._cache_message(sent.message_id, chat_id, chunk, thread_id)
            await self._record_reply_route(sent.message_id, chat_id, session_id)

    def _rich_on(self) -> bool:
        """Whether text goes out as rich messages (Bot API 10.1)."""
        return self.config.telegram.rich_messages and not self._rich_unavailable

    async def _send_rich(
        self, chat_id: int, thread_id: int | None, text: str,
        *, silent: bool = False, link_preview: Any = None,
    ) -> Any | None:
        """Send Markdown *text* as one rich message; ``None`` means send it as HTML.

        ``None`` when rich messages are off, the text is empty or over the
        limit, or Telegram refuses it. A Bot API server without
        ``sendRichMessage`` (an older self-hosted one) turns them off until
        restart. A rate limit that outlasts one wait is raised: the HTML
        path would hit it too.
        """
        if not self._rich_on() or not text.strip() or len(text) > RICH_MSG_LEN:
            return None
        params: dict[str, Any] = {
            "chat_id": chat_id,
            "rich_message": {"markdown": _rich_markdown(text)},
            **_in_thread(thread_id),
        }
        if silent:
            params["disable_notification"] = True
        if link_preview is not None:
            # A notification's choice (NotificationService._telegram_link_preview).
            params["link_preview_options"] = link_preview.to_dict()
        try:
            return await self._with_retry(functools.partial(
                self._app.bot.do_api_request,
                "sendRichMessage",
                api_kwargs=params,
                return_type=Message,
                read_timeout=_RICH_TIMEOUT,
                write_timeout=_RICH_TIMEOUT,
            ))
        except RetryAfter:
            raise
        except EndPointNotFound as exc:
            self._rich_unavailable = True
            logger.warning(
                "The Bot API server has no sendRichMessage (%s); "
                "sending HTML until restart", exc,
            )
        except Exception as exc:
            logger.info(
                "Telegram refused a rich message in chat %s, sending HTML: %s",
                chat_id, exc,
            )
        return None

    async def send_rich_notification(
        self, chat_id: int, thread_id: int | None, text: str,
        *, silent: bool = False, link_preview: Any = None,
    ) -> Any | None:
        """A notification as one rich message; ``None`` means send it as HTML.

        Only for a card that nothing edits later. Buttons, the "Open plan"
        view and the expiry note all edit the card's text, and a rich
        message has none.
        """
        if self._app is None:
            return None
        try:
            return await self._send_rich(
                chat_id, thread_id, text, silent=silent, link_preview=link_preview,
            )
        except RetryAfter:
            return None  # the HTML path waits it out or fails, as before

    async def _send_chunk(
        self, chat_id: int, thread_id: int | None, chunk: str,
    ) -> Any:
        """Send one chunk as HTML, or as plain text if Telegram rejects the HTML."""
        try:
            return await self._app.bot.send_message(
                chat_id=chat_id,
                text=_md_to_tg_html(chunk),
                parse_mode=ParseMode.HTML,
                **_in_thread(thread_id),
            )
        except RetryAfter:
            raise
        except Exception:
            return await self._app.bot.send_message(
                chat_id=chat_id,
                text=chunk,
                **_in_thread(thread_id),
            )

    def format_response(self, text: str) -> str:
        """Return text unchanged.

        Long messages must NOT be truncated here: send() already splits text into
        parts that fit and delivers them as sequential messages. Truncating
        first (the old behaviour) defeated that chunking and dropped the tail of long
        replies with a "... (truncated)" marker.
        """
        return text

    # ------------------------------------------------------------------ #
    #  Streaming protocol                                                  #
    # ------------------------------------------------------------------ #

    async def send_placeholder(self, target: str, session_id: str) -> str | None:
        """Send a placeholder message for streaming. Returns message_id."""
        if self._app is None:
            return None
        chat_id, thread_id = parse_target(target)
        msg = await self._app.bot.send_message(
            chat_id=chat_id, text="⏳", **_in_thread(thread_id),
        )
        return str(msg.message_id)

    def _claim_chat_edit(self, chat_id: int) -> bool:
        """Whether a droppable streaming edit may go out for this chat now.

        Telegram rate limits a chat, not a topic. With topics, several
        sessions can stream into one chat at once, each adapter editing at
        its own interval, and together they would exceed the limit. A 429
        costs more than a dropped edit: the next token brings another.
        """
        now = time.monotonic()
        if now - self._last_chat_edit.get(chat_id, 0.0) < EDIT_INTERVAL:
            return False
        self._last_chat_edit[chat_id] = now
        self._last_chat_edit.move_to_end(chat_id)
        while len(self._last_chat_edit) > _EDIT_CLOCK_MAX:
            self._last_chat_edit.popitem(last=False)
        return True

    async def edit_message(
        self, target: str, message_id: str, text: str,
        *, throttle: bool = False,
    ) -> None:
        """Edit a previously sent message (for streaming updates).

        ``throttle`` marks an edit that can be dropped. Those share one
        budget per chat (see :meth:`_claim_chat_edit`).
        """
        if self._app is None:
            return
        chat_id, thread_id = parse_target(target)
        if throttle and not self._claim_chat_edit(chat_id):
            return
        html_text = _md_to_tg_html(text)
        try:
            await self._app.bot.edit_message_text(
                chat_id=chat_id,
                message_id=int(message_id),
                text=html_text,
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            exc_str = str(exc)
            if "message is not modified" in exc_str.lower():
                return  # Already up-to-date — don't clobber with plain text
            logger.warning("edit_message HTML failed: %s", exc)
            # Fallback: send without formatting if HTML parsing fails
            try:
                await self._app.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=int(message_id),
                    text=text,
                )
            except Exception:
                pass
        # Update cache with the latest text (streaming overwrites placeholder)
        self._cache_message(int(message_id), chat_id, text, thread_id)

    async def delete_message(self, target: str, message_id: str) -> None:
        """Delete a previously sent message."""
        if self._app is None:
            return
        chat_id, _ = parse_target(target)
        try:
            await self._app.bot.delete_message(
                chat_id=chat_id, message_id=int(message_id),
            )
        except Exception as exc:
            logger.warning("delete_message failed: %s", exc)

    async def send_typing(self, target: str) -> None:
        """Show typing indicator."""
        if self._app is None:
            return
        chat_id, thread_id = parse_target(target)
        await self._app.bot.send_chat_action(
            chat_id=chat_id,
            action=ChatAction.TYPING,
            **_in_thread(thread_id),
        )

    # ------------------------------------------------------------------ #
    #  Reactions: set emoji on a message                                    #
    # ------------------------------------------------------------------ #

    async def set_reaction(self, target: str, message_id: int, emoji: str) -> None:
        """Set an emoji reaction on a Telegram message."""
        if self._app is None:
            return
        chat_id, _ = parse_target(target)
        await self._app.bot.set_message_reaction(
            chat_id=chat_id,
            message_id=message_id,
            reaction=[emoji],
        )

    # ------------------------------------------------------------------ #
    #  Stickers: send a sticker by file_id                                 #
    # ------------------------------------------------------------------ #

    async def send_sticker(self, target: str, sticker: str) -> None:
        """Send a sticker to a Telegram chat by file_id."""
        if self._app is None:
            return
        chat_id, thread_id = parse_target(target)
        await self._app.bot.send_sticker(
            chat_id=chat_id,
            sticker=sticker,
            **_in_thread(thread_id),
        )

    # ------------------------------------------------------------------ #
    #  Files: deliver a workspace file as a Telegram document             #
    # ------------------------------------------------------------------ #

    async def send_file(
        self, target: str, file_path: str, *, session_id: str | None = None,
    ) -> bool:
        """Deliver a file to a Telegram chat as a document attachment.

        Size limits are enforced by the Telegram API itself (50 MiB on
        api.telegram.org, up to 2 GiB on self-hosted Bot API servers).
        A reply to the document routes to ``session_id`` (see
        ``_record_reply_route``).
        """
        if self._app is None:
            return False
        path = Path(file_path)
        if not path.is_file():
            return False
        try:
            chat_id, thread_id = parse_target(target)
            sent = await self._app.bot.send_document(
                chat_id=chat_id,
                document=path,
                filename=path.name,
                **_in_thread(thread_id),
            )
            if session_id and sent is not None:
                await self._record_reply_route(sent.message_id, chat_id, session_id)
            return True
        except Exception as e:
            logger.warning("send_file: send_document failed for %s: %s", target, e)
            return False

    # ------------------------------------------------------------------ #
    #  Message cache (for reaction context)                                #
    # ------------------------------------------------------------------ #

    def _cache_message(
        self, message_id: int, chat_id: int, text: str,
        thread_id: int | None = None,
    ) -> None:
        """Store a message snippet in the LRU cache for reaction lookups.

        Message ids are unique within a chat only, so the key carries the
        chat. The topic rides in the value: a reaction update does not say
        which topic its message is in.
        """
        snippet = text[:200] if text else ""
        if not snippet:
            return
        key = (int(chat_id), int(message_id))
        self._message_cache[key] = (thread_id, snippet)
        # Move to end (most recent) and evict oldest if over limit
        self._message_cache.move_to_end(key)
        while len(self._message_cache) > self._message_cache_max:
            self._message_cache.popitem(last=False)

    # ------------------------------------------------------------------ #
    #  Reply routing (reply → originating session)                         #
    # ------------------------------------------------------------------ #

    def _reply_routing_enabled(self) -> bool:
        """Whether reply→origin-session routing is on (a telegram config flag).

        On by default. ``false`` restores the old behaviour: a reply is then
        context-only and lands in the active session, and none of the resolve /
        reject machinery runs. Routes are still recorded (see
        ``_record_reply_route``), so turning the flag back on covers earlier
        messages too. Read live per message so a config reload can flip it
        without a restart.
        """
        return bool(self.config.telegram.reply_routes_to_origin_session)

    async def _record_reply_route(
        self, message_id: int, chat_id: int, session_id: str,
    ) -> None:
        """Remember which session a Telegram message belongs to.

        Lets a later user REPLY to that message route back to that session
        (see ``_resolve_reply``). Recorded whether or not routing is enabled:
        the flag gates routing, and a map that only filled while the flag was
        on would start empty the day it is turned on. Keyed per chat —
        message ids repeat across chats, so keying on the id alone would let
        one chat clobber another chat's identically-numbered message.

        Stored in ``channel_reply_routes`` so it survives a restart. A failed
        write is logged and otherwise ignored: the route is auxiliary to the
        message it rides on, and losing it only degrades a later reply to the
        nudge, whereas raising would abort the send or drop the inbound message.
        """
        if not session_id:
            return
        try:
            await self.router.engine.db.record_reply_route(
                "telegram", str(chat_id), str(message_id), session_id,
            )
        except Exception as e:
            logger.warning(
                "Could not record reply route for message %s in chat %s: %s",
                message_id, chat_id, e,
            )

    async def _lookup_reply_route(
        self, message_id: int, chat_id: int,
    ) -> str | None:
        """Session that produced ``message_id`` in ``chat_id``, or None.

        The key is per-chat, so a ``message_id`` from one chat can never
        route a reply into a session bound to a different chat. A notification
        card delivered before routes were stored has no route row; its
        delivery record still names the session that raised it. A failed
        lookup is logged and resolves to None, which the caller turns into
        the nudge rather than a dropped message.
        """
        db = self.router.engine.db
        target, provider_id = str(chat_id), str(message_id)
        try:
            session_id = await db.lookup_reply_route("telegram", target, provider_id)
            if session_id:
                return session_id
            delivered = await db.find_notification_for_delivery(
                "telegram", target, provider_id,
            )
        except Exception as e:
            logger.warning(
                "Could not look up the reply route for message %s in chat %s: %s",
                message_id, chat_id, e,
            )
            return None
        return (delivered or {}).get("session_id") or None

    async def _resolve_reply(
        self, reply_message_id: int | None, chat_id: int,
    ) -> tuple[str, str | None]:
        """Decide how to route an inbound message given its reply target.

        Returns ``(action, session_id)``:

        - ``("active", None)`` — not a reply; use the chat's active session
          (the default; plain messages are unchanged).
        - ``("route", sid)``  — a reply to a message this bot produced whose
          session is still live; route there and make it active.
        - ``("reject", None)`` — a reply we cannot tie to a live session (the
          message was never recorded or its route was pruned, or its session
          is gone/archived). The caller nudges the user and drops the message
          WITHOUT starting a turn — an explicit reply is never silently
          delivered to the active session, which would be a mis-route.
        """
        if reply_message_id is None:
            return ("active", None)
        session_id = await self._lookup_reply_route(reply_message_id, chat_id)
        if not session_id:
            return ("reject", None)
        try:
            row = await self.router.get_session(session_id)
        except Exception:
            row = None
        if not row or row.get("status") == "archived":
            return ("reject", None)
        return ("route", session_id)

    async def _delivery_session(
        self, routed_session: str | None, channel_key: str,
    ) -> str:
        """The session an inbound message will actually be delivered to:
        the explicit reply target if one resolved, else the chat's active
        session. Recorded against the incoming message id so a later reply to
        it — including the user replying to their OWN message — routes back to
        the same session.
        """
        if routed_session:
            return routed_session
        return await self.router.get_active_session(channel_key, source="telegram")

    # ------------------------------------------------------------------ #
    #  Topics: one forum topic is one session                              #
    # ------------------------------------------------------------------ #

    def _topics_enabled(self) -> bool:
        return self.config.telegram.topics.enabled

    def _topic_of(self, message: Any) -> int | None:
        """The topic a message was posted in, or ``None`` for the chat itself.

        Only ``is_topic_message`` marks a topic. In a plain group a reply
        carries ``message_thread_id`` too (the id of the message it replies
        to), and reading that as a topic would split the group into a
        session per reply chain. The General topic is the chat itself. With
        ``telegram.topics.enabled`` off, every message belongs to the chat.
        """
        if message is None or getattr(message, "is_topic_message", None) is not True:
            return None
        thread_id = getattr(message, "message_thread_id", None)
        if type(thread_id) is not int or thread_id == GENERAL_TOPIC_ID:
            return None
        return thread_id if self._topics_enabled() else None

    @staticmethod
    def _raw_topic(message: Any) -> int | None:
        """The topic *message* sits in, whatever ``telegram.topics`` says.

        Where a card or command physically is: notification deliveries are
        recorded with the target they went to, and a sink topic is one-way
        even with topics-as-sessions switched off.
        """
        thread_id = getattr(message, "message_thread_id", None)
        if (
            getattr(message, "is_topic_message", None) is not True
            or type(thread_id) is not int
            or thread_id == GENERAL_TOPIC_ID
        ):
            return None
        return thread_id

    def _answer_target(self, chat_id: int, message: Any) -> str:
        """The delivery target that an answer typed or tapped in *message* matches."""
        return format_target(chat_id, self._raw_topic(message))

    def _address_of(self, chat_id: int, message: Any) -> _Address:
        """The address of *message*, posted in the chat *chat_id*."""
        return _Address(chat_id, self._topic_of(message))

    def _address(self, update: Update) -> _Address:
        """The chat and topic that a message or command update came from."""
        return self._address_of(update.effective_chat.id, update.message)

    def _bot_has_dm_topics(self) -> bool:
        """Whether BotFather has topics on for the bot's private chats.

        Read from the ``getMe`` result cached at start-up, so a change in
        BotFather needs a restart to show here.
        """
        try:
            return self._app.bot.bot.has_topics_enabled is True
        except Exception:  # noqa: BLE001 — no application, or getMe not run yet
            return False

    def _chat_may_have_topics(self, chat: Any) -> bool:
        """Whether a message in *chat* can sit in a topic: a forum, or a DM with topics on."""
        if getattr(chat, "is_forum", None) is True:
            capable = True
        elif getattr(chat, "type", None) == "private":
            capable = self._bot_has_dm_topics()
        else:
            return False
        return capable and self._topics_enabled()

    async def _session_for(
        self, address: _Address, message: Any = None,
    ) -> str | None:
        """The session a topic is bound to, binding a new one on first use.

        ``None`` for the chat itself: the router picks that session as it
        always has, idle rollover included. A topic never rolls over. It
        stays bound until ``/new``, ``/session`` or ``/sessions`` rebinds it,
        or its session is archived. A new session takes the topic's name when
        *message* shows it.
        """
        if address.thread_id is None:
            return None
        return await self._bind_topic(address, title=self._topic_title_of(message))

    async def _bind_topic(self, topic: _Address, title: str | None = None) -> str:
        """The session bound to *topic*, or a new one titled *title*, bound now."""
        key = topic.channel_key
        lock = self._topic_locks.setdefault(key, asyncio.Lock())
        async with lock:
            session_id = await self.router.get_last_session(key)
            if session_id is None:
                session_id = await self.router.create_session(
                    key, title=title, source="telegram",
                )
                logger.info("Telegram topic %s bound to new session %s", key, session_id)
            return session_id

    def _bot_id(self) -> int | None:
        try:
            return self._app.bot.id
        except Exception:  # noqa: BLE001 — no application, or getMe not run yet
            return None

    def _topic_title_of(self, message: Any) -> str | None:
        """The name a person gave the topic *message* is in, if Telegram shows it.

        Every message in a topic carries the topic's creation message as its
        ``reply_to_message``. A topic the bot opened is skipped: its name is a
        placeholder or the session's own title already.
        """
        root = getattr(message, "reply_to_message", None)
        created = getattr(root, "forum_topic_created", None)
        if created is None:
            return None
        author = getattr(root, "from_user", None)
        if author is not None and author.id == self._bot_id():
            return None
        return getattr(created, "name", None) or None

    def _creates_topics(self, chat: Any) -> bool:
        """Whether a new session started in *chat* gets a topic of its own."""
        return (
            self.config.telegram.topics.new_session_creates_topic
            and self._chat_may_have_topics(chat)
        )

    async def _open_topic(self, chat_id: int, name: str | None) -> tuple[int | None, str]:
        """Create a topic; ``(thread_id, "")``, or ``(None, why Telegram refused)``.

        A rate limit is raised instead, for a caller that can wait it out.
        """
        try:
            topic = await self._app.bot.create_forum_topic(
                chat_id=chat_id, name=_topic_name(name),
            )
        except RetryAfter:
            raise
        except Exception as exc:
            logger.info("Telegram refused a topic in chat %s: %s", chat_id, exc)
            return None, str(exc) or type(exc).__name__
        return topic.message_thread_id, ""

    async def _start_in_new_topic(
        self, chat_id: int, title: str | None,
    ) -> tuple[_Address | None, str | None, str]:
        """Open a topic for a new session: ``(topic, session_id, "")``, or
        ``(None, None, why not)``.

        The chat's own session is left alone: the new session lives in the topic.
        """
        try:
            thread_id, error = await self._open_topic(chat_id, title)
        except RetryAfter as exc:  # the session starts in the chat instead
            thread_id, error = None, str(exc)
        if thread_id is None:
            return None, None, error
        topic = _Address(chat_id, thread_id)
        session_id = await self._bind_topic(topic, title=title)
        welcome = f"New session: {session_id}" + (f" — {title}" if title else "")
        try:
            await self._app.bot.send_message(
                chat_id=chat_id, text=welcome, message_thread_id=thread_id,
            )
        except Exception as exc:  # the topic and its session exist either way
            logger.info("Could not greet %s: %s", topic.channel_key, exc)
        return topic, session_id, ""

    async def _post_session_card(self, topic: _Address, session: dict) -> bool:
        """Post the session's catch-up card into *topic*; False if Telegram refuses.

        Telegram cannot move history into a topic, so the card shows the last
        messages and offers the whole conversation as a document. A rate
        limit is raised, since it says nothing about whether the topic exists.
        """
        sid = session["id"]
        messages = await self.router.get_session_messages(sid, limit=_TAIL_WINDOW)
        total = await self.router.count_session_messages(sid)
        text, markup = build_session_tail_view(
            session, messages, total, _TAIL_WINDOW, self.config.timezone,
        )
        rows = [list(row) for row in markup.inline_keyboard]
        if total:
            rows.append([InlineKeyboardButton(
                "📄 Send transcript", callback_data=f"sesstx:{sid}",
            )])
        try:
            await self._app.bot.send_message(
                chat_id=topic.chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(rows),
                **_in_thread(topic.thread_id),
            )
        except RetryAfter:
            raise
        except Exception as exc:
            logger.info("Could not post into %s: %s", topic.channel_key, exc)
            return False
        return True

    def _home_chat(self) -> int | None:
        """The chat a session gets its topic in when no chat is named.

        The notification chat, else the first allowed user's DM: the chat
        the notification service delivers to (``_resolve_telegram_chat_id``).
        """
        chat_id = self.config.notifications.telegram_chat_id
        if chat_id:
            return int(chat_id)
        allowed = self.config.telegram.allowed_users
        return int(allowed[0]) if allowed else None

    async def open_session_topic(
        self, session_id: str, title: str | None = None, chat_id: int | None = None,
    ) -> SessionTopic:
        """Show a session in a topic of its own, opening one if it has none there.

        The ``↗ Topic`` button, ``POST /api/sessions/{id}/telegram-topic`` and
        the ``session_open_topic`` tool all come here. The chat is *chat_id*,
        else :meth:`_home_chat`, so a session that never touched Telegram (a
        web one, or one an API call started) gets a topic too. A new topic is
        named *title*, else after the session, and starts with the session's
        catch-up card.

        Idempotent per chat: a session that has a topic there already gets
        its card posted in it again, and a topic that was deleted is
        replaced. A chat that was itself on the session lets go of it, and
        its next message starts a new session, so one session never has two
        windows in the same chat.
        """
        if self._app is None:
            return SessionTopic(None, error="Telegram is not running")
        if not self._topics_enabled():
            return SessionTopic(None, error="Telegram topics are off (telegram.topics.enabled)")
        chat = chat_id if chat_id is not None else self._home_chat()
        if chat is None:
            return SessionTopic(None, error=(
                "No chat to open a topic in: set notifications.telegram_chat_id "
                "or telegram.allowed_users"
            ))
        session = await self.router.get_session(session_id)
        if not session or session.get("status") == "archived":
            return SessionTopic(None, error="That session is no longer available")
        if session.get("source") == "external":
            # An MCP client's own session: Nerve runs no turns there, so
            # nothing could answer a message in its topic.
            return SessionTopic(None, error="An external client's session cannot live in a topic")
        lock = self._open_topic_locks.setdefault(f"{chat}:{session_id}", asyncio.Lock())
        try:
            async with lock:
                return await self._show_in_topic(chat, session, title)
        except RetryAfter as exc:
            wait = _retry_after_seconds(exc)
            return SessionTopic(
                None, error=f"Telegram is rate limiting this chat; retry in {wait:.0f} s",
                retry_after=wait,
            )

    async def _show_in_topic(
        self, chat_id: int, session: dict, title: str | None,
    ) -> SessionTopic:
        """:meth:`open_session_topic` once its checks pass; a rate limit is raised."""
        session_id = session["id"]
        for key in await self.router.channel_keys_for_session(session_id):
            topic = _address_of_key(key)
            if topic is None or topic.chat_id != chat_id or topic.thread_id is None:
                continue
            if await self._post_session_card(topic, session):
                return SessionTopic(key)
            await self.router.forget_channel(key)  # the topic is gone
        thread_id, error = await self._open_topic(
            chat_id, title or session.get("title") or f"Session {session_id}",
        )
        if thread_id is None:
            return SessionTopic(None, error=f"Couldn't open a topic: {error}")
        topic = _Address(chat_id, thread_id)
        await self.router.switch_session(topic.channel_key, session_id)
        root_key = _Address(chat_id).channel_key
        if await self.router.get_last_session(root_key) == session_id:
            await self.router.forget_channel(root_key)
        try:
            await self._post_session_card(topic, session)
        except RetryAfter:  # the topic is open and bound; only its card is missing
            logger.info("Rate limited: %s opened without its card", topic.channel_key)
        return SessionTopic(topic.channel_key, opened=True)

    async def _open_in_topic(self, query: Any, session_id: str) -> None:
        """The ``↗ Topic`` button: :meth:`open_session_topic` in the chat it was tapped in."""
        result = await self.open_session_topic(session_id, chat_id=query.message.chat.id)
        if result.channel_key is None:
            await query.answer(_fit_text(result.error, _TOAST_MAX_LEN), show_alert=True)
        elif result.opened:
            await query.answer("Opened in a new topic")
        else:
            await query.answer("It is open in a topic already. Posted it there.")

    async def close_session_topic(
        self, session_id: str, note: str | None = None,
    ) -> tuple[list[ClosedTopic], str]:
        """Let go of a session's topics: post *note* in each, unbind it, close it.

        For ``DELETE /api/sessions/{id}/telegram-topic`` and the
        ``session_close_topic`` tool. The session itself goes on: it is not
        stopped or archived. Each topic is unbound before it is closed, so
        the ``forum_topic_closed`` update that may follow finds no session
        to stop. A topic closed already counts as closed, and one deleted
        already is only unbound.

        Telegram closes topics in forums only. In a private chat the topic
        stays open and unbound, and a message in it starts a new session.

        Returns the topics let go of, and what went wrong with the others
        (``""`` if nothing): a note that did not post, or a close Telegram
        refused. Such a topic keeps its session.
        """
        if self._app is None:
            return [], "Telegram is not running"
        done: list[ClosedTopic] = []
        errors: list[str] = []
        for key in await self.router.channel_keys_for_session(session_id):
            topic = _address_of_key(key)
            if topic is None or topic.thread_id is None:
                continue  # the chat itself, or another channel
            # The binding lock: a message arriving meanwhile waits rather
            # than binding the topic to a new session.
            async with self._topic_locks.setdefault(key, asyncio.Lock()):
                outcome, error = await self._close_topic(topic, session_id, note)
            if error:
                errors.append(error)
            else:
                done.append(ClosedTopic(key, outcome))
        return done, "; ".join(errors)

    async def _close_topic(
        self, topic: _Address, session_id: str, note: str | None,
    ) -> tuple[str, str]:
        """Post *note* in *topic*, unbind it, close it: ``(outcome, "")`` or ``("", why not)``."""
        key = topic.channel_key
        if note and note.strip():
            try:
                await self.send(OutboundMessage(
                    target=topic.target, text=note, session_id=session_id,
                ))
            except Exception as exc:
                if not _error_says(exc, _TOPIC_GONE_ERRORS):
                    return "", f"Couldn't post the note in {key}: {exc}"
                await self.router.forget_channel(key)
                return "gone", ""
        await self.router.forget_channel(key)
        if topic.chat_id > 0:  # a private chat: Telegram has no closing there
            return "left_open", ""
        try:
            await self._app.bot.close_forum_topic(
                chat_id=topic.chat_id, message_thread_id=topic.thread_id,
            )
        except Exception as exc:
            if _error_says(exc, _TOPIC_UNCHANGED_ERRORS):
                return "closed", ""
            if _error_says(exc, _TOPIC_GONE_ERRORS):
                return "gone", ""
            await self.router.switch_session(key, session_id)  # still open: keep it
            return "", f"Couldn't close {key}: {exc}"
        return "closed", ""

    async def _send_transcript(self, query: Any, session_id: str) -> None:
        """The ``📄 Send transcript`` button: the whole conversation as a ``.md`` file."""
        session = await self.router.get_session(session_id)
        if not session:
            await query.answer("That session is no longer available", show_alert=True)
            return
        messages = await self.router.get_session_messages(
            session_id, limit=_TRANSCRIPT_MAX_MESSAGES,
        )
        document = build_session_transcript(session, messages, self.config.timezone)
        address = self._address_of(query.message.chat.id, query.message)
        await query.answer("Sending the transcript")
        try:
            await self._app.bot.send_document(
                chat_id=address.chat_id,
                document=document.encode("utf-8"),
                filename=_transcript_filename(session),
                **_in_thread(address.thread_id),
            )
        except Exception as exc:
            logger.warning("Could not send the transcript of %s: %s", session_id, exc)

    async def _handle_topic_service(self, update: Update, context: Any) -> None:
        """Keep sessions in step with the topics people create, rename and close.

        Created: the topic gets a session at once, titled with the topic's
        name. Renamed: the session takes the new name. Closed: the session is
        stopped. A change the bot made itself is skipped, because the code
        that made it has done the binding already.
        """
        self._touch()
        message, chat, user = update.message, update.effective_chat, update.effective_user
        if message is None or chat is None or user is None:
            return
        if user.id == self._bot_id() or not self._is_authorized(user.id):
            return
        thread_id = message.message_thread_id
        if type(thread_id) is not int or thread_id == GENERAL_TOPIC_ID:
            return
        if self._is_delivery_only_sink(chat, thread_id) or not self._topics_enabled():
            return
        topic = _Address(chat.id, thread_id)
        if message.forum_topic_created is not None:
            await self._bind_topic(topic, title=message.forum_topic_created.name or None)
            return
        session_id = await self.router.get_last_session(topic.channel_key)
        if session_id is None:
            return
        if message.forum_topic_edited is not None:
            if message.forum_topic_edited.name:
                await self.router.set_session_title(
                    session_id, message.forum_topic_edited.name,
                )
        elif message.forum_topic_closed is not None:
            await self.router.stop_session(session_id)

    async def on_session_titled(self, session_id: str, title: str) -> None:
        """Name the session's topics after its generated title.

        A session gets a generated title only when it had none, and every
        topic a person named gave its session that name, so this renames
        topics the bot opened as "New session" and leaves names people chose.
        """
        topics = self.config.telegram.topics
        if self._app is None or not topics.enabled or not topics.auto_rename:
            return
        for key in await self.router.channel_keys_for_session(session_id):
            topic = _address_of_key(key)
            if topic is None or topic.thread_id is None:
                continue
            try:
                await self._app.bot.edit_forum_topic(
                    chat_id=topic.chat_id,
                    message_thread_id=topic.thread_id,
                    name=_topic_name(title),
                )
            except Exception as exc:
                logger.info("Could not rename topic %s: %s", key, exc)

    # ------------------------------------------------------------------ #
    #  Auth                                                                #
    # ------------------------------------------------------------------ #

    def _is_authorized(self, user_id: int) -> bool:
        """Check if a user may talk to the bot.

        dm_policy "open" authorizes everyone (a startup warning is logged).
        Otherwise the allowed_users allowlist applies; an empty allowlist
        blocks everyone (fail closed) until a user pairs via /pair.
        """
        if self.config.telegram.dm_policy == "open":
            return True
        if not self._allowed_users:
            # No whitelist configured — block everyone (fail closed)
            logger.warning("No allowed_users configured — rejecting user %d", user_id)
            return False
        return user_id in self._allowed_users

    def _should_reply_unauthorized(self, user_id: int) -> bool:
        """Rate-limit replies to unauthorized users (once per 10 min each)."""
        now = time.monotonic()
        if now - self._unauth_reply_times.get(user_id, 0.0) < 600:
            return False
        self._unauth_reply_times[user_id] = now
        # Bound the dict so strangers can't grow it unboundedly
        if len(self._unauth_reply_times) > 500:
            for uid, _ in sorted(
                self._unauth_reply_times.items(), key=lambda kv: kv[1],
            )[:250]:
                self._unauth_reply_times.pop(uid, None)
        return True

    # ------------------------------------------------------------------ #
    #  Command handlers — delegate to router for session management        #
    # ------------------------------------------------------------------ #

    def _touch(self) -> None:
        """Record that we received an update from Telegram."""
        self._last_update_time = time.monotonic()

    def _source_routed_command(
        self,
        callback: Callable[[Update, Any], Awaitable[None]],
        *,
        collect: bool = True,
    ) -> Callable[[Update, Any], Awaitable[None]]:
        """Add source routing to a command without changing PTB precedence."""

        async def wrapped(update: Update, context: Any) -> None:
            chat = update.effective_chat
            user = update.effective_user
            message = update.message
            if chat is None or user is None or message is None:
                return

            source = self.config.telegram.source
            if collect and source.enabled and source.allow_conversations:
                await self._observe(
                    update,
                    handled=self._is_authorized(user.id),
                )
            await callback(update, context)

        return wrapped

    async def _handle_start(self, update: Update, context: Any) -> None:
        """Handle /start command."""
        self._touch()
        user_id = update.effective_user.id
        if not self._is_authorized(user_id):
            logger.warning("Unauthorized /start from user %d", user_id)
            # In pairing mode, tell the user how to pair instead of going
            # silent — surfacing their ID is the single most useful hint.
            if (
                self.config.telegram.dm_policy == "pairing"
                and self._should_reply_unauthorized(user_id)
            ):
                await update.message.reply_text(
                    "This Nerve instance isn't paired with your account.\n"
                    f"Your Telegram ID: {user_id}\n\n"
                    "To pair, run `nerve pair` on the server, then open a "
                    "private chat with me and send:\n"
                    "/pair <code>"
                )
            return
        await update.message.reply_text(
            "Connected to Nerve. Send me a message to start chatting.\n\n"
            "Replying to a message continues the session it belongs to. "
            "/new [message] starts a fresh session, with the message as its "
            "first turn if given. /sessions lists and switches sessions."
        )

    async def _handle_pair(self, update: Update, context: Any) -> None:
        """Authorize a user who sends /pair <code> in a private chat."""
        self._touch()
        chat = update.effective_chat
        user = update.effective_user
        message = update.message
        if chat is None or user is None or message is None:
            return
        if (
            chat.type != "private"
            or user.is_bot
            or getattr(message, "sender_chat", None) is not None
        ):
            logger.warning("Ignoring /pair outside a private human chat")
            return
        user_id = user.id

        if self._is_authorized(user_id):
            await update.message.reply_text("Already paired — you're authorized.")
            return

        if self.config.telegram.dm_policy != "pairing":
            logger.warning("Ignoring /pair from user %d (dm_policy != pairing)", user_id)
            return

        from nerve import pairing
        from nerve.config import append_telegram_allowed_user

        args = getattr(context, "args", None) or []
        code = args[0].strip() if args else ""
        if not code:
            if self._should_reply_unauthorized(user_id):
                await update.message.reply_text(
                    "Usage: /pair <code>\n"
                    "Get the code by running `nerve pair` on the server."
                )
            return

        if not pairing.verify_pairing_code(code):
            logger.warning("Failed /pair attempt from user %d", user_id)
            if self._should_reply_unauthorized(user_id):
                await update.message.reply_text(
                    "Invalid or expired code. Run `nerve pair` on the server "
                    "to get a fresh one."
                )
            return

        # Refuse to pair under lockdown — allowed users must come from the
        # tracked remote config, not a local runtime edit. Check BEFORE the
        # in-memory add so lockdown isn't bypassed for the current run.
        from nerve.config import LockdownError, is_locked

        if is_locked():
            await update.message.reply_text(
                "This instance is locked (remote-only). Add your Telegram user "
                "to telegram.allowed_users in the workspace config repo instead."
            )
            return

        # Success: authorize in memory and persist to config.local.yaml
        self._allowed_users.add(user_id)
        try:
            append_telegram_allowed_user(self.config.config_dir, user_id)
            # The running bot honours the new id already, so the next config
            # reload must not report telegram.allowed_users as waiting on a
            # restart: tell the restart bookkeeping this value is live.
            #
            # Recorded exactly as it landed on disk — the startup list with
            # the id appended — because the bookkeeping compares lists with
            # ``!=``: a sorted set would read as a change whenever the file's
            # order is not ascending (a hand-written [333, 111], or pairing an
            # id smaller than one already present).
            from nerve import config_reload

            started = config_reload.startup_config()
            base = list(
                started.telegram.allowed_users if started is not None
                else self.config.telegram.allowed_users
            )
            config_reload.note_applied(
                "telegram.allowed_users",
                base if user_id in base else [*base, user_id],
            )
        except LockdownError:
            # Belt-and-suspenders: config was locked between the check and here.
            self._allowed_users.discard(user_id)
            await update.message.reply_text(
                "This instance is locked (remote-only) — pairing is disabled."
            )
            return
        except Exception:
            # Includes paths.InsecureFileError (nothing was saved). The user is
            # paired only until restart, and the reply says so.
            logger.exception("Paired user %d but failed to persist to config", user_id)
            await update.message.reply_text(
                "Paired for this run, but saving to config failed — check "
                "server logs. Add your ID to telegram.allowed_users manually "
                "to make it permanent."
            )
            return
        logger.info("Telegram user %d paired successfully", user_id)
        await update.message.reply_text(
            "✓ Paired. You're now authorized — send me a message to start chatting."
        )
        logger.info("Telegram user authorized: %d", user_id)

    async def _handle_session(self, update: Update, context: Any) -> None:
        """Handle /session <id> — switch active session."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        address = self._address(update)
        channel_key = address.channel_key

        args = context.args
        if not args:
            # Show current session. A topic shows the session it is bound to
            # (binding one if it has none); the chat applies the idle rollover.
            current = (
                await self._session_for(address, update.message)
                or await self.router.get_active_session(channel_key, source="telegram")
            )
            await update.message.reply_text(
                f"Current session: `{current}`", parse_mode=ParseMode.MARKDOWN,
            )
            return

        session_id = args[0]
        try:
            await self.router.switch_session(channel_key, session_id)
            await update.message.reply_text(
                f"Switched to session: `{session_id}`", parse_mode=ParseMode.MARKDOWN,
            )
        except ValueError as e:
            await update.message.reply_text(str(e))

    async def _sessions_view_for(
        self, channel_key: str, offset: int = 0, *, chat: Any = None,
    ) -> "tuple[str, InlineKeyboardMarkup]":
        """Build the inline-keyboard sessions view for a channel.

        Shared by /sessions and the button-press handler so both render the
        same page. Only interactive (telegram/web) sessions that have history
        are switch targets — cron/automation sessions are never listed — and the
        store pins the current session first with starred (kept-alive) ones
        ahead of the rest, so an idle starred session is never buried. Filtering
        and ordering happen in SQL; we fetch one extra row to learn whether a
        further page exists. Asked for in a *chat* with topics, outside any
        topic, each session can also be opened in a topic of its own.
        """
        offset = max(0, offset)
        current = await self.router.get_last_session(channel_key)
        page = await self.router.list_interactive_sessions(
            limit=_SESSIONS_PAGE_SIZE + 1, offset=offset, current_id=current,
        )
        has_next = len(page) > _SESSIONS_PAGE_SIZE
        address = _address_of_key(channel_key)
        topic_buttons = (
            chat is not None
            and address is not None
            and address.thread_id is None
            and self._chat_may_have_topics(chat)
        )
        return build_sessions_view(
            page[:_SESSIONS_PAGE_SIZE], current,
            offset=offset, has_prev=offset > 0, has_next=has_next,
            topic_buttons=topic_buttons,
        )

    async def _handle_sessions(self, update: Update, context: Any) -> None:
        """Handle /sessions — native inline keyboard to switch session routing.

        Each recent session is a tap-to-switch button (current marked ✓); the
        id rides in the button's callback_data, so no id copy-paste is needed.
        """
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        channel_key = self._address(update).channel_key
        text, markup = await self._sessions_view_for(
            channel_key, chat=update.effective_chat,
        )
        await update.message.reply_text(text, reply_markup=markup)

    # ------------------------------------------------------------------ #
    #  /plans — review queue (approve / decline / revise from chat)        #
    # ------------------------------------------------------------------ #

    async def _plans_view_for(
        self, offset: int = 0,
    ) -> "tuple[str, InlineKeyboardMarkup]":
        """Build the /plans review-queue view for the given page.

        Plans are global (not chat-scoped), matching the WebUI: pending ones
        (the actionable state) are listed ahead of implementing ones, each
        newest-first from the store. Rows are fetched up to the end of the
        requested page plus one more, which tells whether a further page
        exists, so every page is reachable however long the queue grows.
        """
        offset = max(0, offset)
        need = offset + _PLANS_PAGE_SIZE + 1
        plans = await self.router.list_plans(status="pending", limit=need)
        if len(plans) < need:
            plans = plans + await self.router.list_plans(
                status="implementing", limit=need - len(plans),
            )
        page = plans[offset:offset + _PLANS_PAGE_SIZE]
        has_next = len(plans) > offset + _PLANS_PAGE_SIZE
        return build_plans_view(
            page, offset=offset, has_prev=offset > 0, has_next=has_next,
        )

    async def _handle_plans(self, update: Update, context: Any) -> None:
        """Handle /plans — native inline keyboard to review pending plans.

        Each plan is a tap-to-open button; the plan id rides in the button's
        callback_data, so approving/declining never needs a copy-pasted id.
        """
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        text, markup = await self._plans_view_for()
        await update.message.reply_text(text, reply_markup=markup)

    async def _edit_plans_list(self, query: Any, offset: int = 0) -> None:
        """Replace the current card with the plans list at ``offset``."""
        text, markup = await self._plans_view_for(offset)
        await self._safe_edit(query, text, markup)

    async def _edit_plan_detail(self, query: Any, plan_id: str) -> None:
        """Render a plan's detail (HTML) into the card in place."""
        plan = await self.router.get_plan(plan_id)
        if not plan:
            await self._edit_plans_list(query)
            return
        text, markup = build_plan_detail_view(plan, tzname=self.config.timezone)
        await self._safe_edit(query, text, markup, parse_mode=ParseMode.HTML)

    async def _handle_plan_button(self, query: Any) -> None:
        """Handle /plans inline-keyboard presses.

        callback_data forms:
          ``plan:list:<offset>``   — (re)show the review queue at a page
          ``plan:view:<id>``       — show a plan's detail + action buttons
          ``plan:approve:<id>``    — confirm-approve card
          ``plan:approveok:<id>``  — approve (spawns an implementation session)
          ``plan:decline:<id>``    — confirm-decline card
          ``plan:declineok:<id>``  — decline (closes the task as done)
          ``plan:revise:<id>``     — prompt (ForceReply) for revision feedback
        """
        if not query.message:
            await query.answer()
            return
        parts = query.data.split(":", 2)
        action = parts[1] if len(parts) > 1 else ""
        arg = parts[2] if len(parts) > 2 else ""

        if action == "list":
            try:
                off = max(0, int(arg))
            except ValueError:
                off = 0
            await query.answer()
            await self._edit_plans_list(query, off)
            return

        if action == "view":
            await query.answer()
            await self._edit_plan_detail(query, arg)
            return

        if action in ("approve", "decline"):
            plan = await self.router.get_plan(arg)
            if not plan:
                await query.answer("That plan is no longer available", show_alert=True)
                await self._edit_plans_list(query)
                return
            if plan.get("status") != "pending":
                await query.answer(
                    f"Plan is {plan.get('status')} — no longer actionable",
                    show_alert=True,
                )
                await self._edit_plan_detail(query, arg)
                return
            await query.answer()
            text, markup = build_plan_confirm_view(plan, action)
            await self._safe_edit(query, text, markup)
            return

        if action == "approveok":
            await self._do_plan_approve(query, arg)
            return
        if action == "declineok":
            await self._do_plan_decline(query, arg)
            return
        if action == "revise":
            await self._start_plan_revise(query, arg)
            return

        await query.answer()

    async def _do_plan_approve(self, query: Any, plan_id: str) -> None:
        """Approve via the shared service, then confirm on the card."""
        from nerve.agent.plan_service import (
            PlanNotFound,
            PlanNotPending,
            TaskNotFound,
            approve_plan,
        )
        try:
            result = await approve_plan(
                db=self.router.engine.db, engine=self.router.engine, plan_id=plan_id,
                # Telegram users are not mapped to local identities yet, so
                # the approval stays unattributed, like other Telegram input.
                actor=None, cause=None,
            )
        except (PlanNotFound, TaskNotFound):
            await query.answer("That plan is no longer available", show_alert=True)
            await self._edit_plans_list(query)
            return
        except PlanNotPending as exc:
            await query.answer(str(exc), show_alert=True)
            await self._edit_plan_detail(query, plan_id)
            return
        except NotAcceptingTurnsError as exc:
            await query.answer(str(exc), show_alert=True)
            return
        await query.answer("Approved — implementation started")
        impl = _html.escape(str(result.get("impl_session_id") or "?"))
        text = (
            "✅ <b>Plan approved.</b>\n"
            f"Implementation session <code>{impl}</code> started."
        )
        markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("🗒 Plans", callback_data="plan:list:0")]]
        )
        await self._safe_edit(query, text, markup, parse_mode=ParseMode.HTML)

    async def _do_plan_decline(self, query: Any, plan_id: str) -> None:
        """Decline via the shared service, then confirm on the card."""
        from nerve.agent.plan_service import (
            PlanNotFound,
            PlanNotPending,
            TaskNotFound,
            decline_plan,
        )
        try:
            await decline_plan(
                db=self.router.engine.db, engine=self.router.engine, plan_id=plan_id,
                # Telegram users have no local identity yet: unattributed.
                actor=None, cause=None,
            )
        except (PlanNotFound, TaskNotFound):
            await query.answer("That plan is no longer available", show_alert=True)
            await self._edit_plans_list(query)
            return
        except PlanNotPending as exc:
            await query.answer(str(exc), show_alert=True)
            await self._edit_plan_detail(query, plan_id)
            return
        await query.answer("Declined — task closed")
        text = "🗑 <b>Plan declined.</b>\nThe related task was closed as done."
        markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("🗒 Plans", callback_data="plan:list:0")]]
        )
        await self._safe_edit(query, text, markup, parse_mode=ParseMode.HTML)

    async def _start_plan_revise(self, query: Any, plan_id: str) -> None:
        """Ask for revision feedback via a ForceReply prompt.

        A message that *replies to* this prompt, from the user who tapped
        Revise and within ``_PLAN_REVISE_TTL``, is captured as feedback in
        ``_handle_message`` and dispatched to the planner — Telegram's native
        way to collect a text argument after a button press.
        """
        plan = await self.router.get_plan(plan_id)
        if not plan or plan.get("status") != "pending":
            await query.answer(
                "Only pending plans can be revised", show_alert=True,
            )
            await self._edit_plan_detail(query, plan_id)
            return
        await query.answer()
        title = plan.get("task_title") or plan.get("task_id") or plan_id
        prompt = await query.message.reply_text(
            f"✍️ Reply to this message with revision feedback for the plan "
            f"“{title}”.\nThe planner will produce a new version from your notes.",
            reply_markup=ForceReply(input_field_placeholder="What should change?"),
        )
        self._prune_plan_revise_prompts()
        self._plan_revise_prompts[(query.message.chat.id, prompt.message_id)] = (
            plan_id, query.from_user.id, time.monotonic(),
        )

    async def _submit_plan_revision(
        self, update: Update, plan_id: str, feedback: str,
    ) -> None:
        """Dispatch captured revision feedback to the planner session."""
        from nerve.agent.plan_service import (
            PlanNotFound,
            PlanNotPending,
            TaskNotFound,
            request_plan_revision,
        )
        try:
            result = await request_plan_revision(
                db=self.router.engine.db, engine=self.router.engine,
                plan_id=plan_id, feedback=feedback,
                # Telegram users have no local identity yet: unattributed.
                actor=None, cause=None,
            )
        except (PlanNotFound, TaskNotFound):
            await update.message.reply_text("That plan no longer exists.")
            return
        except (PlanNotPending, ValueError, NotAcceptingTurnsError) as exc:
            await update.message.reply_text(str(exc))
            return
        await update.message.reply_text(
            f"✍️ Revision requested for plan {plan_id}. Feedback sent to the "
            f"planner ({result['session_id']}); a new version will appear in /plans."
        )

    def _prune_plan_revise_prompts(self) -> None:
        """Forget Revise prompts left unanswered for longer than the TTL."""
        cutoff = time.monotonic() - _PLAN_REVISE_TTL
        for key, (_plan_id, _user_id, started) in list(self._plan_revise_prompts.items()):
            if started < cutoff:
                del self._plan_revise_prompts[key]

    async def _maybe_consume_plan_revision(self, update: Update) -> bool:
        """Consume a reply to a /plans revise prompt as revision feedback.

        Returns True (message handled — do not route to the agent) only when
        the message replies to an open Revise prompt in this chat and comes
        from the user who tapped Revise. Several prompts can be open at once;
        each reply goes to the plan of the prompt it answers. Any other
        message falls through to normal routing.
        """
        chat = update.effective_chat
        msg = update.message
        user = update.effective_user
        reply_to = getattr(msg, "reply_to_message", None) if msg else None
        if not chat or not user or not reply_to:
            return False
        self._prune_plan_revise_prompts()
        key = (chat.id, reply_to.message_id)
        pending = self._plan_revise_prompts.get(key)
        if not pending or pending[1] != user.id:
            return False
        del self._plan_revise_prompts[key]
        plan_id = pending[0]
        feedback = (msg.text or "").strip()
        if not feedback:
            await msg.reply_text("No feedback given — revision cancelled.")
            return True
        await self._submit_plan_revision(update, plan_id, feedback)
        return True

    async def _handle_star(self, update: Update, context: Any) -> None:
        """Handle /star — star the current session so it never auto-closes."""
        await self._set_current_starred(update, True)

    async def _handle_unstar(self, update: Update, context: Any) -> None:
        """Handle /unstar — clear the current session's kept-alive flag."""
        await self._set_current_starred(update, False)

    async def _set_current_starred(self, update: Update, starred: bool) -> None:
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        channel_key = self._address(update).channel_key
        current = await self.router.get_last_session(channel_key)
        if not current:
            await update.message.reply_text("No active session to star.")
            return
        try:
            await self.router.set_session_starred(current, starred)
        except ValueError as e:
            await update.message.reply_text(str(e))
            return
        if starred:
            await update.message.reply_text(
                "⭐ Session starred — it won't auto-close when idle."
            )
        else:
            await update.message.reply_text(
                "☆ Session unstarred — normal auto-close applies."
            )

    async def _handle_new_session(self, update: Update, context: Any) -> None:
        """Handle /new [message] — stop the current session, switch to a new one.

        Text after the command is the new session's first message, delivered
        through the same pipeline as a plain message but with the new session
        forced as the target. A reply's routing is bypassed, so this is how to
        answer a message without continuing the session it came from.

        Inside a topic this rebinds that topic only; the chat and every other
        topic keep their sessions. In the chat itself, when it has topics, the
        new session opens in a new topic instead, and the chat's session is
        left running.
        """
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        address = self._address(update)
        channel_key = address.channel_key
        # Taken from the raw text rather than ``context.args``, which is
        # whitespace-split and would flatten a multi-line message. No title:
        # the engine names the session from its first exchange.
        parts = (update.message.text or "").split(maxsplit=1)
        first_message = parts[1] if len(parts) > 1 else ""

        if address.thread_id is None and self._creates_topics(update.effective_chat):
            topic, session_id, error = await self._start_in_new_topic(address.chat_id, None)
            if topic is not None and session_id:
                await update.message.reply_text(
                    f"New session `{session_id}` is in the topic "
                    f"“{_topic_name(None)}”.",
                    parse_mode=ParseMode.MARKDOWN,
                )
                if first_message:
                    await self._deliver_message(
                        update, first_message, session_id=session_id, address=topic,
                    )
                return
            await update.message.reply_text(
                f"Couldn't open a topic ({error}), so the new session is here."
            )

        # Stop the current session before creating a new one
        prev = await self.router.get_last_session(channel_key)
        if prev:
            stopped = await self.router.stop_session(prev)
            if stopped:
                await update.message.reply_text(
                    f"Stopped session `{prev}`.",
                    parse_mode=ParseMode.MARKDOWN,
                )

        session_id = await self.router.create_session(channel_key, source="telegram")
        await update.message.reply_text(
            f"New session: `{session_id}`", parse_mode=ParseMode.MARKDOWN,
        )
        if first_message:
            await self._deliver_message(update, first_message, session_id=session_id)

    async def _handle_stop(self, update: Update, context: Any) -> None:
        """Handle /stop — stop the currently running session."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        channel_key = self._address(update).channel_key

        session_id = await self.router.get_last_session(channel_key)
        if not session_id:
            await update.message.reply_text("No active session.")
            return

        # The turn only: background agents keep running. A runtime that
        # cannot spare them is not stopped, and the answer says why.
        result = await self.router.stop_turn(session_id)
        note = turn_stop_note(result, session_id)
        if note:
            await update.message.reply_text(note, parse_mode=ParseMode.MARKDOWN)
        elif result.get("turn") in ("stopped", "killed"):
            await update.message.reply_text(
                f"Stopped session `{session_id}`.",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await update.message.reply_text("Nothing running to stop.")

    async def _handle_restart(self, update: Update, context: Any) -> None:
        """Handle /restart — run ``nerve restart`` to restart the daemon."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        await update.message.reply_text("Restarting Nerve...")
        logger.info("Restart requested by Telegram user %d", update.effective_user.id)
        subprocess.Popen(
            [sys.executable, "-m", "nerve", "restart"],
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    async def _handle_doctor(self, update: Update, context: Any) -> None:
        """Handle /doctor — run health checks and return the report."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        from nerve.cli import doctor_report
        report = doctor_report(self.config)
        await update.message.reply_text(
            f"```\n{report}\n```",
            parse_mode=ParseMode.MARKDOWN,
        )

    # ------------------------------------------------------------------ #
    #  Message handler — construct InboundMessage and delegate             #
    # ------------------------------------------------------------------ #

    async def _extract_image(self, message: Any) -> dict[str, str] | None:
        """Download and base64-encode an image from a Telegram message."""
        if message.photo:
            # Telegram provides multiple resolutions; pick the largest
            photo = message.photo[-1]
            tg_file = await photo.get_file()
            data = await tg_file.download_as_bytearray()
            return {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": base64.b64encode(bytes(data)).decode("utf-8"),
            }
        return None

    async def _extract_sticker(
        self, message: Any,
    ) -> tuple[dict[str, str] | None, str]:
        """Extract sticker image and context text from a Telegram message.

        Returns (image_dict_or_None, context_text).
        Static stickers (.webp) are downloaded as full images.
        Animated/video stickers use their thumbnail instead.
        """
        sticker = getattr(message, "sticker", None)
        if not sticker:
            return None, ""

        emoji = sticker.emoji or ""
        set_name = sticker.set_name or ""
        file_id = sticker.file_id

        # Build human-readable context
        parts = []
        if emoji:
            parts.append(emoji)
        if set_name:
            parts.append(f'set "{set_name}"')
        context = f'[Sticker: {", ".join(parts)}, file_id: {file_id}]'

        # Download image — static stickers are WEBP; animated/video use thumbnail
        image: dict[str, str] | None = None
        try:
            if sticker.is_animated or sticker.is_video:
                thumb = sticker.thumbnail
                if thumb:
                    tg_file = await thumb.get_file()
                    data = await tg_file.download_as_bytearray()
                    image = {
                        "type": "base64",
                        "media_type": "image/webp",
                        "data": base64.b64encode(bytes(data)).decode("utf-8"),
                    }
            else:
                tg_file = await sticker.get_file()
                data = await tg_file.download_as_bytearray()
                image = {
                    "type": "base64",
                    "media_type": "image/webp",
                    "data": base64.b64encode(bytes(data)).decode("utf-8"),
                }
        except Exception as e:
            logger.warning("Failed to download sticker image: %s", e)

        return image, context

    # Known text-like MIME types (beyond text/*)
    _TEXT_MIME_EXTRAS: set[str] = {
        "application/json",
        "application/xml",
        "application/javascript",
        "application/x-yaml",
        "application/yaml",
        "application/toml",
        "application/x-sh",
        "application/x-python",
        "application/sql",
        "application/x-httpd-php",
        "application/xhtml+xml",
        "application/csv",
    }

    # Extensions treated as text when MIME type is missing or generic. Shared
    # with the Slack channel and with the archive unpacker, so a file type
    # read inline here is read inline everywhere.
    _TEXT_EXTENSIONS = TEXT_EXTENSIONS

    _IMAGE_MIMES: set[str] = {"image/jpeg", "image/png", "image/gif", "image/webp"}

    _ARCHIVE_MIMES: set[str] = {"application/zip", "application/x-zip-compressed"}

    _IMAGE_EXT_TO_MIME = IMAGE_EXT_TO_MIME

    _MAX_TEXT_SIZE: int = MAX_TEXT_SIZE    # 512 KB — inline text cap
    _MAX_DOWNLOAD_SIZE: int = 20_000_000   # ~20 MB — Telegram Bot API limit

    async def _extract_document(
        self, message: Any,
    ) -> tuple[list[dict[str, str]], str]:
        """Extract document content and context from a Telegram message.

        Returns (content_blocks, context_text).
        - Text files: blocks=[], context has file content.
        - Images sent as documents: blocks=[image_dict], context has metadata.
        - PDFs: blocks=[pdf_dict], context has metadata.
        - ZIP archives: blocks=[images/pdfs inside], context has metadata + text.
        - Other: blocks=[], context has metadata only.
        """
        doc = getattr(message, "document", None)
        if not doc:
            return [], ""

        file_name = doc.file_name or "unnamed"
        mime = doc.mime_type or ""
        size = doc.file_size or 0
        ext = ""
        if "." in file_name:
            ext = "." + file_name.rsplit(".", 1)[-1].lower()

        size_str = (
            f"{size / 1024:.0f} KB" if size < 1_000_000
            else f"{size / 1_000_000:.1f} MB"
        )
        meta_line = f"[Document: {file_name} ({size_str}, {mime or 'unknown type'})]"

        # -- Too large to download via Bot API --
        if size > self._MAX_DOWNLOAD_SIZE:
            return [], f"{meta_line}\n(File too large to download)"

        # -- Text-like files --
        is_text = (
            mime.startswith("text/")
            or mime in self._TEXT_MIME_EXTRAS
            or ext in self._TEXT_EXTENSIONS
        )
        if is_text:
            if size > self._MAX_TEXT_SIZE:
                return [], f"{meta_line}\n(Text file too large to display — {size_str})"
            try:
                tg_file = await doc.get_file()
                data = await tg_file.download_as_bytearray()
                content = bytes(data).decode("utf-8", errors="replace")
                return [], f"{meta_line}\n```\n{content}\n```"
            except Exception as e:
                logger.warning("Failed to download text document %s: %s", file_name, e)
                return [], meta_line

        # -- Image files sent as documents --
        if mime in self._IMAGE_MIMES:
            try:
                tg_file = await doc.get_file()
                data = await tg_file.download_as_bytearray()
                image = {
                    "type": "base64",
                    "media_type": mime,
                    "data": base64.b64encode(bytes(data)).decode("utf-8"),
                }
                return [image], meta_line
            except Exception as e:
                logger.warning("Failed to download image document %s: %s", file_name, e)
                return [], meta_line

        # -- PDF --
        if mime == "application/pdf" or ext == ".pdf":
            try:
                tg_file = await doc.get_file()
                data = await tg_file.download_as_bytearray()
                pdf_block = {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": base64.b64encode(bytes(data)).decode("utf-8"),
                }
                return [pdf_block], meta_line
            except Exception as e:
                logger.warning("Failed to download PDF %s: %s", file_name, e)
                return [], meta_line

        # -- ZIP archives --
        if mime in self._ARCHIVE_MIMES or ext == ".zip":
            return await self._extract_zip(doc, file_name, meta_line)

        # -- Other binary files — metadata only --
        return [], meta_line

    async def _extract_zip(
        self, doc: Any, file_name: str, meta_line: str,
    ) -> tuple[list[dict[str, str]], str]:
        """Extract ZIP archive contents — text inline, images/PDFs as blocks.

        The 20 MB download cap is on the compressed archive, so the bounds on
        what it expands to live in :mod:`nerve.channels.archives`, shared with
        the Slack channel.
        """
        try:
            tg_file = await doc.get_file()
            data = await tg_file.download_as_bytearray()
        except Exception as e:
            logger.warning("Failed to download ZIP %s: %s", file_name, e)
            return [], meta_line

        return extract_zip(bytes(data), meta_line)

    def _is_delivery_only_sink(self, chat: Any, thread_id: int | None = None) -> bool:
        """True when ``chat`` is an opted-in group/supergroup notification sink.

        ``notifications.telegram_chat_id`` can be pointed at a dedicated group
        so pushes (the notify tool, async questions/approvals) land there
        instead of the owner's DM. When ``notifications.delivery_only_sink`` is
        enabled, such a group is delivery-only: an inbound update must not start
        an agent turn.

        Gated behind the opt-in flag (default off) so an existing install that
        used a group as the sink and still chatted there is unaffected. Only
        groups and supergroups qualify: a DM sink (chat id == the user's own
        private chat) stays interactive regardless. Inline-button callbacks are
        handled separately, so questions/approvals delivered here stay
        answerable.

        With ``notifications.telegram_thread_id`` the sink is one topic, and
        only that topic is one-way: the rest of the group stays interactive.
        ``thread_id`` is the topic the update came from.
        """
        notif = self.config.notifications
        if not getattr(notif, "delivery_only_sink", False):
            return False
        notif_chat = notif.telegram_chat_id
        if not (
            bool(notif_chat)
            and chat.id == notif_chat
            and chat.type in ("group", "supergroup")
        ):
            return False
        sink_thread = getattr(notif, "telegram_thread_id", None)
        return sink_thread is None or sink_thread == thread_id
    @property
    def observation(self) -> ObservationPolicy:
        """The source grant, rebuilt per read so reloads apply at once."""
        source = self.config.telegram.source
        return ObservationPolicy(
            enabled=source.enabled,
            conversations=PatternGate(
                "chat",
                allow=list(source.allow_conversations),
                deny=list(source.deny_conversations),
            ),
            senders=PatternGate(
                "sender",
                allow=list(source.allow_senders),
                deny=list(source.deny_senders),
            ),
        )

    async def _observe(self, update: Update, handled: bool = False) -> None:
        """Buffer a group message for the source inbox, if the grant allows.

        Asked of every group message, whatever ``telegram.allowed_users``
        decided: the source is its own grant, so a sender being authorized
        neither earns nor forfeits a place in the inbox. ``handled`` says the
        live route accepted this message, and by default that is where it
        stops — one message should not arrive twice.
        ``include_handled_messages`` turns the copy back on for a source
        meant as a record.

        Private chats are never a source. A stranger's DM is a refusal, and
        filing it away is not what the silence led them to expect; a group an
        operator listed is a different matter.

        The bot must be a group administrator, or have privacy mode disabled
        via BotFather, for Telegram to deliver ordinary group traffic at all.
        Without that it only sees commands and replies to itself, and this
        collects almost nothing however the grant is written.
        """
        # Cheapest gates first: this runs on every message now, not only the
        # ones from a sender the allowlist refused.
        source = self.config.telegram.source
        if not source.enabled:
            return
        if handled and not source.include_handled_messages:
            return
        policy = self.observation
        if not policy.active:
            return
        chat = update.effective_chat
        user = update.effective_user
        message = update.message
        if chat is None or user is None or message is None:
            return
        if chat.type == "private":
            return
        # Pairing codes are credentials. A command addressed to another bot
        # bypasses our CommandHandler and reaches the generic message handler,
        # so enforce this exclusion at the source gate too.
        text = message.text or message.caption or ""
        words = text.split(maxsplit=1)
        command = words[0].partition("@")[0].casefold() if words else ""
        if command == "/pair":
            return
        # Telegram delivers other bots' messages to group handlers under
        # bot-to-bot mode. Slack drops them via _is_another_app_talking, and
        # two agents feeding each other's inboxes is no better than two
        # agents answering each other.
        if user.is_bot:
            return

        # Only the numeric id is stable. A chat title is set by whoever runs
        # the group, so treating it as allow-eligible would let anyone create
        # a group called "ops-room" and walk into a grant meant for someone
        # else's; a @username is claimable and movable for the same reason.
        # Both stay deny-eligible, where a spoofable name can only ever
        # subtract access.
        conversation = Identity(
            id=str(chat.id),
            self_set_names=tuple(n for n in (chat.username, chat.title) if n),
        )
        sender = Identity(
            id=str(user.id),
            self_set_names=tuple(
                n for n in (
                    user.username, user.full_name, user.first_name, user.last_name,
                ) if n
            ),
        )

        verdict = policy.check(conversation, sender)
        if not verdict.allowed:
            logger.debug("Telegram did not observe a message: %s", verdict.reason)
            return

        text = message.text or message.caption or ""
        attachment = _describe_telegram_attachment(message)
        if attachment:
            text = f"{attachment}\n\n{text}" if text else attachment
        # Neither text nor an attachment is nothing worth a row.
        if not text:
            return

        sent_at = message.date or datetime.now(timezone.utc)
        metadata = {
            "chat_type": chat.type or "",
            "reply_to_message_id": (
                str(_reply_of(message).message_id)
                if _reply_of(message)
                else ""
            ),
        }
        # The inbox groups a forum by chat, as before; the topic is a detail.
        thread_id = self._topic_of(message)
        if thread_id is not None:
            metadata["thread_id"] = str(thread_id)
        observed = ObservedMessage(
            channel_name="telegram",
            channel_key=f"telegram:{chat.id}",
            conversation_id=str(chat.id),
            sender_id=str(user.id),
            text=text,
            message_id=str(message.message_id),
            timestamp=sent_at.isoformat(),
            conversation_title=chat.title or chat.username or "",
            sender_name=user.username or user.full_name or "",
            metadata=metadata,
        )
        await self.router.observe(
            observed,
            ttl_days=self.config.sync.message_ttl_days,
            max_stored_messages=source.max_stored_messages,
        )

    async def _handle_message(self, update: Update, context: Any) -> None:
        """Handle incoming text and photo messages — delegate to router."""
        self._touch()
        chat = update.effective_chat
        user = update.effective_user
        message = update.message
        if chat is None or user is None or message is None:
            return
        # Two independent routes, as on Slack. Authorization decides the live
        # one; the source has its own grant and is asked either way, so a
        # group can feed the inbox whether or not its members may also drive
        # the agent.
        handled = self._is_authorized(user.id)
        await self._observe(update, handled=handled)
        if not handled:
            return

        # A reply to a pending ask_user card is its answer — like a button tap
        # or /reply — not a message for the agent. Checked before the sink
        # gate so a card in the notifications group can be answered there too.
        # An album is a message, not an answer.
        if not update.message.media_group_id and await self._maybe_answer_question_reply(
            update, update.message.text or update.message.caption or "",
        ):
            return

        # Delivery-only notification sink: never start an agent turn on a
        # message that arrives in the notifications group (a one-way channel).
        if self._is_delivery_only_sink(
            update.effective_chat, self._raw_topic(update.message),
        ):
            logger.info(
                "Ignoring inbound message in delivery-only notification chat %s",
                update.effective_chat.id,
            )
            return

        # A reply to a /plans "Revise" ForceReply prompt is revision feedback,
        # not a message for the agent. Intercept before any routing.
        if await self._maybe_consume_plan_revision(update):
            return

        # Media group (album) — collect all parts before processing
        if update.message.media_group_id:
            await self._collect_media_group(update)
            return

        await self._deliver_message(
            update, update.message.text or update.message.caption or "",
        )

    async def _deliver_message(
        self, update: Update, text: str, *, session_id: str | None = None,
        address: _Address | None = None,
    ) -> None:
        """Turn one inbound message into an agent turn.

        Builds the agent-facing text (forward origin, reply context and quote,
        attachments on this message and on the replied-to one), picks the
        session and delivers. ``session_id`` forces the target: ``/new <text>``
        passes the session it just created, so the message gets there even as
        a reply — no routing lookup, no rejection. Otherwise a reply routes to
        the session that produced the replied-to message when routing is on,
        and anything else lands in the chat's active session. ``address``
        overrides where the message is taken to come from: ``/new <text>`` in a
        chat with topics passes the topic it just opened.
        """
        address = address or self._address(update)
        chat_id = address.chat_id

        # Cache for reaction lookups (raw text, before reply-context prefix)
        self._cache_message(
            update.message.message_id, chat_id, text, address.thread_id,
        )

        # Prepend forward origin (if forwarded message)
        forward_context = _format_forward_context(update.message)
        if forward_context:
            text = f"{forward_context}\n\n{text}" if text else forward_context

        # Prepend reply-to context and quote (if replying to a message)
        reply_context = _format_reply_context(update.message)
        if reply_context:
            text = f"{reply_context}\n\n{text}" if text else reply_context

        # Extract image if present
        images: list[dict[str, str]] = []
        image = await self._extract_image(update.message)
        if image:
            images.append(image)

        # Extract sticker if present
        sticker_image, sticker_context = await self._extract_sticker(update.message)
        if sticker_context:
            text = f"{sticker_context}\n\n{text}" if text else sticker_context
        if sticker_image:
            images.append(sticker_image)

        # Extract document/file if present
        doc_contents, doc_context = await self._extract_document(update.message)
        if doc_context:
            text = f"{doc_context}\n\n{text}" if text else doc_context
        if doc_contents:
            images.extend(doc_contents)

        # Extract attachments from replied-to message (images, documents, PDFs)
        reply_msg = _reply_of(update.message)
        if reply_msg:
            reply_image = await self._extract_image(reply_msg)
            if reply_image:
                images.append(reply_image)
            reply_doc_contents, reply_doc_ctx = await self._extract_document(reply_msg)
            if reply_doc_ctx:
                text = f"{reply_doc_ctx}\n\n{text}" if text else reply_doc_ctx
            if reply_doc_contents:
                images.extend(reply_doc_contents)

        if not text and not images:
            return

        logger.info(
            "Telegram message from %s: %s%s",
            chat_id,
            (text[:80] + ("..." if len(text) > 80 else "")) if text else "(no text)",
            f" [{len(images)} image(s)]" if images else "",
        )

        metadata: dict[str, Any] = {"message_id": update.message.message_id}
        if images:
            metadata["images"] = images

        # Reply routing (telegram.reply_routes_to_origin_session, on by default).
        # When off, a reply is context-only and lands in the active session
        # (the target stays None), exactly like a plain message. A forced
        # target skips the lookup altogether: a reply the user sent as
        # /new <text> must never be refused on its way to the session they
        # just asked for.
        channel_key = address.channel_key
        target_session: str | None = session_id
        if target_session is None and address.thread_id is not None:
            # A topic is bound to one session: every message in it, a reply
            # included, goes there (see _session_for).
            target_session = await self._session_for(address, update.message)
        elif target_session is None and self._reply_routing_enabled():
            # Route a reply to the session that produced the replied-to message
            # (and make it active). An unresolvable reply is rejected with a
            # no-LLM nudge rather than mis-routed into the active session.
            action, routed_session = await self._resolve_reply(
                reply_msg.message_id if reply_msg else None, chat_id,
            )
            if action == "reject":
                logger.info(
                    "Telegram reply from chat %s not tied to a live session; nudging",
                    chat_id,
                )
                try:
                    await update.message.reply_text(REPLY_UNRESOLVED_MSG)
                except Exception:
                    logger.error("Failed to send reply-unresolved nudge to chat %s", chat_id)
                return
            if routed_session:
                logger.info("Telegram reply routed to session %s", routed_session)
            target_session = await self._delivery_session(routed_session, channel_key)
        # Record this inbound message against the session it lands in, so a
        # later reply to it — including the user replying to their own
        # message — routes back to the same session. Recorded with routing
        # off too, so the map is already complete when the flag is turned on.
        await self._record_reply_route(
            update.message.message_id, chat_id,
            target_session or await self._delivery_session(None, channel_key),
        )

        msg = InboundMessage(
            channel_name="telegram",
            channel_key=channel_key,
            sender_id=address.target,
            text=text,
            session_id=target_session,
            metadata=metadata,
        )

        try:
            await self.router.handle_message(msg)
        except NotAcceptingTurnsError:
            # The stream adapter already sent the engine's error event.
            logger.info("Draining: refused a message from chat %s", chat_id)
        except Exception as e:
            logger.error("Agent error for chat %s: %s", chat_id, e, exc_info=True)
            try:
                await update.message.reply_text(f"Error: {e}")
            except Exception:
                logger.error("Failed to send error reply to chat %s", chat_id)

    # ------------------------------------------------------------------ #
    #  Reaction handler                                                    #
    # ------------------------------------------------------------------ #

    async def _handle_reaction(self, update: Update, context: Any) -> None:
        """Handle message reaction updates — forward as text to router."""
        self._touch()
        reaction_update = update.message_reaction
        if reaction_update is None:
            return

        user = reaction_update.user
        if user is None or not self._is_authorized(user.id):
            return

        # Delivery-only notification sink: never start an agent turn on a
        # reaction that arrives in the notifications group (a one-way channel).
        if self._is_delivery_only_sink(reaction_update.chat):
            logger.info(
                "Ignoring reaction in delivery-only notification chat %s",
                reaction_update.chat.id,
            )
            return

        chat_id = reaction_update.chat.id
        message_id = reaction_update.message_id

        # Extract new emoji reactions (standard + premium custom emoji)
        emojis = []
        custom_ids = []
        for r in reaction_update.new_reaction:
            emoji = getattr(r, "emoji", None)
            if emoji:
                emojis.append(emoji)
            else:
                custom_id = getattr(r, "custom_emoji_id", None)
                if custom_id:
                    custom_ids.append(custom_id)

        # Resolve premium custom emoji IDs to their base emoji
        if custom_ids:
            try:
                stickers = await self._app.bot.get_custom_emoji_stickers(custom_ids)
                for sticker in stickers:
                    emojis.append(sticker.emoji or f"[premium:{sticker.custom_emoji_id}]")
            except Exception as e:
                logger.warning("Failed to resolve custom emoji: %s", e)
                emojis.extend(f"[premium:{cid}]" for cid in custom_ids)

        if not emojis:
            # Reaction removed — ignore for now
            return

        emoji_str = " ".join(emojis)

        # Look up original message text from cache. A reaction update names
        # no topic, so the cache is also where the reaction's topic comes from.
        thread_id: int | None = None
        cached = self._message_cache.get((chat_id, message_id))
        if cached:
            thread_id, original_text = cached
            # A one-way sink topic takes no reactions either; the check above
            # covered a sink that is the whole chat.
            if self._is_delivery_only_sink(reaction_update.chat, thread_id):
                logger.info(
                    "Ignoring reaction in the delivery-only notification topic "
                    "%s of chat %s", thread_id, chat_id,
                )
                return
            if thread_id is not None and not self._topics_enabled():
                thread_id = None
            text = f'[Reaction: {emoji_str} on message: "{original_text}"]'
        elif self._chat_may_have_topics(reaction_update.chat):
            # The message may sit in any topic. Routing the reaction to the
            # chat would hand it to a session that never saw the message.
            logger.info(
                "Ignoring a reaction in chat %s: message %d is not cached, "
                "so its topic is unknown", chat_id, message_id,
            )
            return
        else:
            text = f"[Reaction: {emoji_str}]"

        logger.info("Telegram reaction from %s: %s (msg %d)", chat_id, emoji_str, message_id)

        address = _Address(chat_id, thread_id)
        msg = InboundMessage(
            channel_name="telegram",
            channel_key=address.channel_key,
            sender_id=address.target,
            text=text,
            metadata={},
        )

        try:
            msg.session_id = await self._session_for(address)
            await self.router.handle_message(msg)
        except NotAcceptingTurnsError:
            # The stream adapter already sent the engine's error event.
            logger.info("Draining: refused a reaction from chat %s", chat_id)
        except Exception as e:
            logger.error("Agent error for reaction in chat %s: %s", chat_id, e, exc_info=True)

    # ------------------------------------------------------------------ #
    #  Media group (album) collection                                     #
    # ------------------------------------------------------------------ #

    async def _collect_media_group(self, update: Update) -> None:
        """Buffer a media-group message; schedule processing after a short delay."""
        group_id = update.message.media_group_id
        if group_id not in self._media_groups:
            self._media_groups[group_id] = []
        self._media_groups[group_id].append(update)

        # Reset the timer — wait for remaining album parts
        task = self._media_group_tasks.get(group_id)
        if task:
            task.cancel()
        self._media_group_tasks[group_id] = asyncio.create_task(
            self._process_media_group(group_id),
        )

    async def _process_media_group(self, group_id: str) -> None:
        """Wait briefly for all album parts, then send as one message."""
        await asyncio.sleep(0.5)

        updates = self._media_groups.pop(group_id, [])
        self._media_group_tasks.pop(group_id, None)
        if not updates:
            return

        address = self._address(updates[0])
        chat_id = address.chat_id

        # Caption is usually on the first message only
        text = ""
        for u in updates:
            caption = u.message.text or u.message.caption or ""
            if caption:
                text = caption
                break

        # Prepend forward origin (album forward info is on the first message)
        forward_context = _format_forward_context(updates[0].message)
        if forward_context:
            text = f"{forward_context}\n\n{text}" if text else forward_context

        # Prepend reply-to context (album reply info is on the first message)
        reply_context = _format_reply_context(updates[0].message)
        if reply_context:
            text = f"{reply_context}\n\n{text}" if text else reply_context

        # Download all images
        images: list[dict[str, str]] = []
        for u in updates:
            image = await self._extract_image(u.message)
            if image:
                images.append(image)

        # Extract attachments from replied-to message (images, documents, PDFs)
        reply_msg = _reply_of(updates[0].message)
        if reply_msg:
            reply_image = await self._extract_image(reply_msg)
            if reply_image:
                images.append(reply_image)
            reply_doc_contents, reply_doc_ctx = await self._extract_document(reply_msg)
            if reply_doc_ctx:
                text = f"{reply_doc_ctx}\n\n{text}" if text else reply_doc_ctx
            if reply_doc_contents:
                images.extend(reply_doc_contents)

        if not text and not images:
            return

        logger.info(
            "Telegram media group from %s: %d image(s), caption: %s",
            chat_id, len(images),
            (text[:80] + "..." if len(text) > 80 else text) if text else "(none)",
        )

        metadata: dict[str, Any] = {"message_id": updates[0].message.message_id}
        if images:
            metadata["images"] = images

        # Reply routing (telegram.reply_routes_to_origin_session, on by default).
        # When off, a reply is context-only and lands in the active session
        # (session_id stays None), exactly like a plain message.
        channel_key = address.channel_key
        target_session: str | None = None
        if address.thread_id is not None:
            # A topic is bound to one session: every message in it, a reply
            # included, goes there (see _session_for).
            target_session = await self._session_for(address, updates[0].message)
        elif self._reply_routing_enabled():
            # Route a reply to the session that produced the replied-to message
            # (and make it active). An unresolvable reply is rejected with a
            # no-LLM nudge rather than mis-routed into the active session.
            action, routed_session = await self._resolve_reply(
                reply_msg.message_id if reply_msg else None, chat_id,
            )
            if action == "reject":
                logger.info(
                    "Telegram reply from chat %s not tied to a live session; nudging",
                    chat_id,
                )
                try:
                    await updates[0].message.reply_text(REPLY_UNRESOLVED_MSG)
                except Exception:
                    logger.error("Failed to send reply-unresolved nudge to chat %s", chat_id)
                return
            if routed_session:
                logger.info("Telegram reply routed to session %s", routed_session)
            target_session = await self._delivery_session(routed_session, channel_key)
        # Record this inbound message against the session it lands in, so a
        # later reply to it — including the user replying to their own
        # message — routes back to the same session. Recorded with routing
        # off too, so the map is already complete when the flag is turned on.
        await self._record_reply_route(
            updates[0].message.message_id, chat_id,
            target_session or await self._delivery_session(None, channel_key),
        )

        msg = InboundMessage(
            channel_name="telegram",
            channel_key=channel_key,
            sender_id=address.target,
            text=text,
            session_id=target_session,
            metadata=metadata,
        )

        try:
            await self.router.handle_message(msg)
        except NotAcceptingTurnsError:
            # The stream adapter already sent the engine's error event.
            logger.info("Draining: refused an album from chat %s", chat_id)
        except Exception as e:
            logger.error("Agent error for chat %s: %s", chat_id, e, exc_info=True)
            try:
                await updates[0].message.reply_text(f"Error: {e}")
            except Exception:
                logger.error("Failed to send error reply to chat %s", chat_id)

    # ------------------------------------------------------------------ #
    #  Error handler                                                       #
    # ------------------------------------------------------------------ #

    async def _handle_error(self, update: object, context: Any) -> None:
        """Log errors from the Telegram bot polling/handler pipeline."""
        self._touch()
        logger.error(
            "Telegram update error: %s (update=%s)",
            context.error, update, exc_info=context.error,
        )

    # ------------------------------------------------------------------ #
    #  Notification callback handlers                                      #
    # ------------------------------------------------------------------ #

    async def _safe_edit(
        self, query: Any, text: str, markup: Any, parse_mode: Any = None,
    ) -> None:
        """edit_message_text that never raises.

        "Message is not modified" (re-showing the same view) is expected and
        ignored; any other failure is logged, since the card then stays stale.
        """
        try:
            await query.edit_message_text(
                text=text, reply_markup=markup, parse_mode=parse_mode,
            )
        except Exception as exc:
            if "not modified" not in str(exc).lower():
                logger.warning("Telegram message edit failed: %s", exc)

    async def _edit_session_tail(
        self, query: Any, session_id: str, window: int,
    ) -> None:
        """Render a session's recent history (catch-up view) into the card in place."""
        window = max(1, min(window, _TAIL_MAX))
        session = await self.router.get_session(session_id)
        if not session:
            channel_key = self._address_of(
                query.message.chat.id, query.message,
            ).channel_key
            text, markup = await self._sessions_view_for(
                channel_key, chat=query.message.chat,
            )
            await self._safe_edit(query, text, markup)
            return
        messages = await self.router.get_session_messages(session_id, limit=window)
        total = await self.router.count_session_messages(session_id)
        text, markup = build_session_tail_view(
            session, messages, total, window, self.config.timezone,
        )
        await self._safe_edit(query, text, markup, parse_mode=ParseMode.HTML)

    async def _handle_session_button(self, query: Any) -> None:
        """Handle /sessions inline-keyboard presses.

        callback_data forms:
          ``sess:list``           — (re)show the session switch list (first page)
          ``sess:page:<offset>``  — show the switch list at a pagination offset
          ``sess:new``            — create a fresh session (current keeps running);
                                    in a chat with topics, in a new topic
          ``sess:<id>``           — switch routing to <id>, then show its history
          ``sesstail:<id>:<win>`` — widen the catch-up window (informational only)
          ``sesstopic:<id>``      — open <id> in a topic of its own
          ``sesstx:<id>``         — send <id>'s whole conversation as a document

        After a switch/create the card is replaced by that session's recent
        history (native order, oldest→newest) so the user can catch up — which
        matters most when switching into a background session they weren't
        following.
        """
        if not query.message:
            await query.answer()
            return
        # The card sits in the topic it was asked for in, so a tap there
        # switches that topic only.
        chat = query.message.chat
        address = self._address_of(chat.id, query.message)
        channel_key = address.channel_key
        data = query.data

        if data.startswith("sesstopic:"):
            await self._open_in_topic(query, data.split(":", 1)[1])
            return

        if data.startswith("sesstx:"):
            await self._send_transcript(query, data.split(":", 1)[1])
            return

        if data == "sess:list":
            await query.answer()
            text, markup = await self._sessions_view_for(channel_key, chat=chat)
            await self._safe_edit(query, text, markup)
            return

        if data.startswith("sess:page:"):
            try:
                page_offset = max(0, int(data.rsplit(":", 1)[1]))
            except (IndexError, ValueError):
                page_offset = 0
            await query.answer()
            text, markup = await self._sessions_view_for(
                channel_key, page_offset, chat=chat,
            )
            await self._safe_edit(query, text, markup)
            return

        topic_error = ""
        if (
            data == "sess:new"
            and address.thread_id is None
            and self._creates_topics(chat)
        ):
            _, session_id, topic_error = await self._start_in_new_topic(chat.id, None)
            if session_id:
                await query.answer("Started a new session in a new topic")
                return
            # Telegram refused the topic: start the session here instead.

        if data.startswith("sesstail:"):
            parts = data.split(":")
            sid = parts[1] if len(parts) > 1 else ""
            try:
                window = int(parts[2])
            except (IndexError, ValueError):
                window = _TAIL_WINDOW
            await query.answer()
            await self._edit_session_tail(query, sid, window)
            return

        # sess:new / sess:<id>
        target = data.split(":", 1)[1]
        sid = target
        try:
            if target == "new":
                sid = await self.router.create_session(
                    channel_key, source="telegram",
                )
                if topic_error:
                    await query.answer(_fit_text(
                        f"Couldn't open a topic ({topic_error}), "
                        "so the new session is here.", _TOAST_MAX_LEN,
                    ), show_alert=True)
                else:
                    await query.answer("Started a new session")
            else:
                await self.router.switch_session(channel_key, target)
                await query.answer("Switched")
        except ValueError:
            await query.answer(
                "That session is no longer available", show_alert=True,
            )
            text, markup = await self._sessions_view_for(channel_key, chat=chat)
            await self._safe_edit(query, text, markup)
            return
        await self._edit_session_tail(query, sid, _TAIL_WINDOW)

    async def _handle_callback_query(self, update: Update, context: Any) -> None:
        """Handle inline keyboard button presses for notification questions."""
        self._touch()
        query = update.callback_query
        if not query or not query.data:
            return

        if not self._is_authorized(query.from_user.id):
            await query.answer("Unauthorized", show_alert=True)
            return

        # /sessions inline keyboard: switch/create/list + history buttons.
        if query.data.startswith(("sess:", "sesstail:", "sesstopic:", "sesstx:")):
            await self._handle_session_button(query)
            return

        # /plans inline keyboard: list/view/approve/decline/revise.
        if query.data.startswith("plan:"):
            await self._handle_plan_button(query)
            return

        # Parse callback_data: "notif:{notification_id}:{answer}"
        # or, from a question button, "notifopt:{notification_id}:{option index}".
        parts = query.data.split(":", 2)
        if len(parts) < 3 or parts[0] not in ("notif", "notifopt"):
            await query.answer()
            return

        notification_id = parts[1]

        if not self._notification_service:
            await query.answer("Service unavailable", show_alert=True)
            return

        answer = await self._notification_service.resolve_telegram_answer(
            notification_id, parts[2], by_index=parts[0] == "notifopt",
        )
        if answer is None:
            await query.answer("Already answered or expired", show_alert=True)
            return

        try:
            result = await self._notification_service.answer_delivered_notification(
                notification_id,
                answer,
                channel="telegram",
                target=self._answer_target(update.effective_chat.id, query.message),
                actor=str(query.from_user.id),
            )
        except NotAcceptingTurnsError as exc:
            # The question stays pending, and the card keeps its buttons.
            await query.answer(_fit_text(str(exc), _TOAST_MAX_LEN), show_alert=True)
            return

        if result:
            status_line = f"\u2705 Answered: {answer}"
            toast = f"Answered: {answer}"
            # A snooze keeps the row pending with redeliver_at stamped \u2014
            # confirm on the card that it will come back, instead of the
            # generic answered state (which read as "handled, gone").
            snoozed_until = self._snoozed_until(result)
            if snoozed_until:
                status_line = (
                    f"\U0001F4A4 Snoozed until {snoozed_until} \u2014 will resurface"
                )
                toast = f"Snoozed until {snoozed_until}"
            await query.answer(_fit_text(toast, _TOAST_MAX_LEN))
            try:
                original = query.message.text or ""
                text = _fit_text(f"{original}\n\n{status_line}", MAX_MSG_LEN)
                # The status line goes after the card's text, so the card's
                # formatting and links keep their places. A text that had to
                # be cut goes out plain, since a place can lie past the cut.
                # The card's link preview choice stays too.
                kept: dict[str, Any] = {}
                entities = getattr(query.message, "entities", None)
                if entities and text.startswith(original):
                    kept["entities"] = entities
                link_preview = getattr(query.message, "link_preview_options", None)
                if link_preview is not None:
                    kept["link_preview_options"] = link_preview
                await query.edit_message_text(text=text, reply_markup=None, **kept)
            except Exception:
                # Remove the buttons anyway: the tap has been handled.
                try:
                    await query.edit_message_reply_markup(reply_markup=None)
                except Exception:
                    pass
        else:
            await query.answer("Already answered or expired", show_alert=True)

    @staticmethod
    def _snoozed_until(notification: dict[str, Any]) -> str | None:
        """Render the next delivery time when an answer snoozed the row."""
        try:
            if (
                notification.get("status") != "pending"
                or not notification.get("redeliver_at")
            ):
                return None
            from datetime import datetime
            dt = datetime.fromisoformat(notification["redeliver_at"])
            return dt.astimezone().strftime("%Y-%m-%d %H:%M %Z")
        except Exception:
            return None

    async def _handle_reply(self, update: Update, context: Any) -> None:
        """Handle /reply <text> — answer the most recent pending question."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        if not context.args:
            await update.message.reply_text("Usage: /reply <your answer>")
            return
        if not self._notification_service:
            await update.message.reply_text("Notification service not available")
            return

        answer_text = " ".join(context.args)

        try:
            result = await self._notification_service.answer_latest_question(
                answer_text,
                channel="telegram",
                target=self._answer_target(update.effective_chat.id, update.message),
                actor=str(update.effective_user.id),
            )
        except NotAcceptingTurnsError as exc:
            await update.message.reply_text(str(exc))
            return

        if result:
            await update.message.reply_text(
                f"Answer recorded for: {result['title']}",
            )
        else:
            await update.message.reply_text("No pending questions in this chat.")

    async def _maybe_answer_question_reply(self, update: Update, text: str) -> bool:
        """Consume a reply to a pending ``ask_user`` card as its answer.

        Returns True (message handled — do not route to the agent) when the
        replied-to message is a pending question delivered to this chat and
        the answer was recorded, or refused because the engine is draining
        (the question stays pending, as with /reply). Anything else falls
        through to normal routing: not a reply, no text, no service, a card
        that is not a pending question, or an answer not accepted because the
        question was answered or expired meanwhile. Works whether or not reply
        routing is enabled, and in a delivery-only notifications group.
        """
        message = update.message
        reply_to = getattr(message, "reply_to_message", None)
        if reply_to is None or not text or self._notification_service is None:
            return False
        # A card in a topic is recorded under the topic's target.
        target = self._address(update).target
        notif = await self.router.engine.db.find_notification_for_delivery(
            "telegram", target, str(reply_to.message_id),
        )
        if not notif or notif.get("status") != "pending" or notif.get("type") != "question":
            return False
        try:
            result = await self._notification_service.answer_delivered_notification(
                notif["id"],
                text,
                channel="telegram",
                target=target,
                actor=str(update.effective_user.id),
            )
        except NotAcceptingTurnsError as exc:
            await message.reply_text(str(exc))
            return True
        if not result:
            return False
        await message.reply_text(f"Answer recorded for: {result['title']}")
        return True
