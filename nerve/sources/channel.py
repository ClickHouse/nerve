"""Drain the channel observation buffer into the source inbox.

Chat arrives by push; sources are pull, cursor, and cron. Rather than teach
the sources layer about sockets — or poll a chat API that already delivered
the same messages, paying twice in latency and rate limit for a second
cursor to disagree with — the channel buffers what it saw and this source
drains the buffer.

Everything past that is inherited. :class:`~nerve.sources.runner.SourceRunner`
supplies filtering, condensing, TTL, health, and cursor advance, and the
existing ``poll_source`` / ``read_source`` tools and ``MessagesGate`` work
against the result with no channel-specific code anywhere in this layer.

The cursor is the buffer's autoincrement id, which is why the buffer exists:
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
    """A source over one channel's observation buffer.

    The source name is ``<channel>:observed`` — compound like
    ``gmail:<account>``, and distinct on purpose. ``telegram`` is already a
    pull source, and sharing a name would mean sharing a job id
    (``source:telegram``) and a cursor key: the two runners would evict each
    other from the scheduler and then read each other's cursor, one an
    integer and the other Telethon's JSON state. So a cron gate reads
    ``sources: [slack:observed]``, and observed traffic stays legible as its
    own stream rather than blending into a pull source's.

    Only this process writes the buffer: the channel transport, or the
    gateway stream that stands in for it, delivers a message here and the
    router buffers it. Nothing is added while the machine is parked, and
    what arrives meanwhile is delivered after the next wake. So the
    scheduled drain is not the instance's next scheduled task while nothing
    waits in the buffer (``push_fed``), and a platform that parks an idle
    machine is not kept up, or woken, for each drain; a drain with messages
    waiting since the last one counts from the next forecast on
    (:meth:`has_undrained`), so the machine stays up, or is woken, to take
    them. While the instance runs, the schedule drains the buffer as it
    always did.
    """

    push_fed = True

    def __init__(self, channel: str, db: Database):
        self.channel = channel
        self.source_name = f"{channel}:observed"
        self._db = db

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Read buffered observations past *cursor*.

        A malformed cursor is treated as "start from the beginning" rather
        than an error: the buffer is TTL-bounded, so the worst case is
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
                "Channel source %s: reading the buffer failed: %s",
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
                _to_record(self.channel, self.source_name, payload)
                for _, payload in rows
                if isinstance(payload, dict)
            ],
            next_cursor=str(rows[-1][0]),
            has_more=len(rows) >= limit,
        )

    async def has_undrained(self, cursor: str | None) -> bool:
        """Whether a message waits in the buffer past *cursor*.

        One row is enough to know; a failed read answers no, so a forecast
        never fails for it.
        """
        try:
            rows = await self._db.read_channel_observations(
                self.channel, after_id=_as_id(cursor, quiet=True), limit=1,
            )
        except Exception:
            logger.debug(
                "Channel source %s: checking the buffer failed", self.channel, exc_info=True,
            )
            return False
        return bool(rows)


def _as_id(cursor: str | None, *, quiet: bool = False) -> int:
    """The buffer id a cursor names, or 0."""
    if not cursor:
        return 0
    try:
        return int(cursor)
    except (TypeError, ValueError):
        if not quiet:
            logger.warning("Ignoring unreadable channel cursor %r", cursor)
        return 0


def _to_record(
    channel: str, source_name: str, payload: dict[str, Any],
) -> SourceRecord:
    """Turn one buffered :class:`ObservedMessage` payload into a record.

    When a channel gateway linked a person to the sender, the summary shows
    the person's name after the sender, and the metadata names the person.
    The drain's sender rule matches only ``sender_id``.
    """
    conversation_id = payload.get("conversation_id") or ""
    message_id = payload.get("message_id") or ""
    text = payload.get("text") or ""
    title = payload.get("conversation_title") or conversation_id
    sender = payload.get("sender_name") or payload.get("sender_id") or "unknown"
    principal_id = payload.get("principal_id") or ""
    principal_name = (payload.get("principal_name") or "") if principal_id else ""
    if principal_name and principal_name != sender:
        sender = f"{sender} ({principal_name})"

    preview = text[:_SUMMARY_PREVIEW].replace("\n", " ")
    if len(text) > _SUMMARY_PREVIEW:
        preview += "..."

    # The record id is the transport's own address, not the buffer id, so a
    # message observed twice collapses on the inbox's (source, id) key
    # instead of arriving twice.
    return SourceRecord(
        id=f"{conversation_id}:{message_id}",
        source=source_name,
        record_type=f"{channel}_message",
        summary=f"[{title}] {sender}: {preview}",
        content=text,
        timestamp=payload.get("timestamp") or "",
        metadata={
            "conversation_id": conversation_id,
            "conversation_title": payload.get("conversation_title") or "",
            "sender_id": payload.get("sender_id") or "",
            "sender_name": payload.get("sender_name") or "",
            **({"principal_id": principal_id} if principal_id else {}),
            **({"principal_name": principal_name} if principal_name else {}),
            "message_id": message_id,
            "channel_key": payload.get("channel_key") or "",
            **(payload.get("metadata") or {}),
        },
    )


__all__ = ["ChannelSource"]
