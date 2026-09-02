"""Drain the channel observation spool into the source inbox.

Chat arrives by push; sources are pull, cursor, and cron. Rather than teach
the sources layer about sockets — or poll a chat API that already delivered
the same messages, paying twice in latency and rate limit for a second
cursor to disagree with — the channel spools what it saw and this source
drains the spool.

Everything past that is inherited. :class:`~nerve.sources.runner.SourceRunner`
supplies filtering, condensing, TTL, health, and cursor advance, and the
existing ``poll_source`` / ``read_source`` tools and ``MessagesGate`` work
against the result with no channel-specific code anywhere in this layer.

The cursor is the spool's autoincrement id, which is why the spool exists:
a monotonic integer that survives pruning makes ``WHERE id > cursor``
trivially correct, where a chat timestamp would not be.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from nerve.sources.base import Source
from nerve.sources.models import FetchResult, SourceRecord

if TYPE_CHECKING:
    from nerve.db import Database

logger = logging.getLogger(__name__)

_SUMMARY_PREVIEW = 80


class ChannelSource(Source):
    """A source over one channel's observation spool.

    ``channel`` is the transport name (``"slack"``), which is also
    :attr:`source_name` — so the inbox shows ``slack`` beside ``gmail`` and
    ``github``, and a cron gate reads ``sources: [slack]``.
    """

    def __init__(self, channel: str, db: Database):
        self.source_name = channel
        self.channel = channel
        self._db = db

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Read spooled observations past *cursor*.

        A malformed cursor is treated as "start from the beginning" rather
        than an error: the spool is TTL-bounded, so the worst case is
        re-reading a bounded backlog, and ``source_messages`` is keyed
        ``(source, id)``, which makes that a no-op instead of a duplicate.
        """
        after_id = _as_id(cursor)
        try:
            rows = await self._db.read_channel_observations(
                self.channel, after_id=after_id, limit=limit,
            )
        except Exception as e:
            logger.error(
                "Channel source %s: reading the spool failed: %s",
                self.channel, e, exc_info=True,
            )
            return FetchResult(records=[], next_cursor=cursor)

        if not rows:
            return FetchResult(records=[], next_cursor=cursor, has_more=False)

        # The cursor tracks what was *scanned*, not what parsed. An
        # unreadable row still has to move it, or the drain re-reads it
        # every run and never reaches what is behind it.
        return FetchResult(
            records=[
                _to_record(self.channel, payload)
                for _, payload in rows
                if payload is not None
            ],
            next_cursor=str(rows[-1][0]),
            has_more=len(rows) >= limit,
        )


def _as_id(cursor: str | None) -> int:
    """The spool id a cursor names, or 0."""
    if not cursor:
        return 0
    try:
        return int(cursor)
    except (TypeError, ValueError):
        logger.warning("Ignoring unreadable channel cursor %r", cursor)
        return 0


def _to_record(channel: str, payload: dict[str, Any]) -> SourceRecord:
    """Turn one spooled :class:`ObservedMessage` payload into a record."""
    conversation_id = payload.get("conversation_id") or ""
    message_id = payload.get("message_id") or ""
    text = payload.get("text") or ""
    title = payload.get("conversation_title") or conversation_id
    sender = payload.get("sender_name") or payload.get("sender_id") or "unknown"

    preview = text[:_SUMMARY_PREVIEW].replace("\n", " ")
    if len(text) > _SUMMARY_PREVIEW:
        preview += "..."

    # The record id is the transport's own address, not the spool id, so a
    # message observed twice collapses on the inbox's (source, id) key
    # instead of arriving twice.
    return SourceRecord(
        id=f"{conversation_id}:{message_id}",
        source=channel,
        record_type=f"{channel}_message",
        summary=f"[{title}] {sender}: {preview}",
        content=text,
        timestamp=payload.get("timestamp") or "",
        metadata={
            "conversation_id": conversation_id,
            "conversation_title": payload.get("conversation_title") or "",
            "sender_id": payload.get("sender_id") or "",
            "sender_name": payload.get("sender_name") or "",
            "message_id": message_id,
            "channel_key": payload.get("channel_key") or "",
            **(payload.get("metadata") or {}),
        },
    )


__all__ = ["ChannelSource"]
