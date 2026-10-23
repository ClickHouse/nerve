"""GitHub source in hosted mode: drains the records that the gateway delivered.

With ``sync.github.mode: hosted`` the channel gateway polls GitHub for the
agent, builds each record the way :class:`~nerve.sources.github.GitHubSource`
builds it (enrichment included), and streams it to Nerve. The hosted source
consumer writes every record to the ``channel_observations`` buffer under the
channel ``github`` (see :mod:`nerve.channels.hosted.sources`), and this
source turns the buffered payloads back into :class:`SourceRecord` objects. It
makes no GitHub calls.

The source keeps the name ``github``, so it runs as ``source:github`` with the
GitHub guardrail, drop hook, condensing, health checks, consumer cursors and
``messages`` gates of the polling source.

The cursor is the buffer's row ID, as for the chat channel drains. A cursor
left by the polling source is a timestamp; it reads as 0, so the first drain
starts at the beginning of the buffer.

Buffered payload (one row per record)::

    {"event_id": ..., "connection_id": ..., "conversation_id": "owner/repo",
     "record": {"id", "record_type", "summary", "content",
                "timestamp": RFC 3339,
                "metadata": [{"key", "value", "values"}, ...]}}

Metadata values arrive as strings. ``actors`` and ``changes`` are lists, and
``unread`` becomes the boolean the polling source stores.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from nerve.sources.base import Source
from nerve.sources.models import FetchResult, SourceRecord

if TYPE_CHECKING:
    from nerve.db import Database

logger = logging.getLogger(__name__)

SOURCE_NAME = "github"
# The buffer channel that the hosted source consumer writes for this source.
BUFFER_CHANNEL = "github"
# Metadata keys whose value is a list of strings, and those that are flags.
_LIST_KEYS = frozenset({"actors", "changes"})
_FLAG_KEYS = frozenset({"unread"})


class HostedGitHubSource(Source):
    """The GitHub source over the hosted records buffer."""

    source_name = SOURCE_NAME
    push_fed = True

    def __init__(self, db: Database):
        self._db = db

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Read buffered records past *cursor*, oldest first.

        A failed read raises, so the runner reports it and its health checks
        count it.
        """
        rows = await self._db.read_channel_observations(
            BUFFER_CHANNEL, after_id=_buffer_position(cursor), limit=limit,
        )
        if not rows:
            return FetchResult(records=[], next_cursor=cursor, has_more=False)

        records: list[SourceRecord] = []
        for row_id, payload in rows:
            record = record_from_buffer(payload) if isinstance(payload, dict) else None
            if record is None:
                logger.warning("Hosted GitHub source: skipping unreadable buffer row %s", row_id)
                continue
            records.append(record)
        # The cursor tracks what was scanned, so an unreadable row is passed
        # and does not stop the drain.
        return FetchResult(
            records=records,
            next_cursor=str(rows[-1][0]),
            has_more=len(rows) >= limit,
        )


def _buffer_position(cursor: str | None) -> int:
    """The buffer row ID that *cursor* names, or 0."""
    if not cursor:
        return 0
    try:
        return int(cursor)
    except ValueError:
        logger.info(
            "Hosted GitHub source: cursor %r is not a buffer position (a cursor of "
            "the polling source); draining from the start of the buffer", cursor,
        )
        return 0


def record_from_buffer(payload: dict[str, Any]) -> SourceRecord | None:
    """The inbox record of one buffered payload, or ``None`` when it is unusable."""
    record = payload.get("record")
    if not isinstance(record, dict):
        return None
    record_id = record.get("id")
    record_type = record.get("record_type")
    summary = record.get("summary")
    if not all(isinstance(value, str) and value for value in (record_id, record_type, summary)):
        return None
    content = record.get("content")
    timestamp = record.get("timestamp")
    return SourceRecord(
        id=record_id,
        source=SOURCE_NAME,
        record_type=record_type,
        summary=summary,
        content=content if isinstance(content, str) else "",
        timestamp=timestamp if isinstance(timestamp, str) else "",
        metadata=_metadata(record.get("metadata")),
    )


def _metadata(entries: object) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    if not isinstance(entries, list):
        return metadata
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        key = entry.get("key")
        if not isinstance(key, str) or not key:
            continue
        value = entry.get("value")
        value = value if isinstance(value, str) else ""
        values = entry.get("values")
        values = [str(item) for item in values] if isinstance(values, list) else []
        if values or key in _LIST_KEYS:
            metadata[key] = values or ([value] if value else [])
        elif key in _FLAG_KEYS:
            metadata[key] = value.strip().lower() == "true"
        else:
            metadata[key] = value
    return metadata


__all__ = ["BUFFER_CHANNEL", "SOURCE_NAME", "HostedGitHubSource", "record_from_buffer"]
