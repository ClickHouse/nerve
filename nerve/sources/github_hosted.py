"""GitHub sources in hosted mode: drain the records that the gateway delivered.

With ``sync.github.mode: hosted`` the channel gateway polls GitHub for the
agent and streams the records to Nerve, for two providers: ``github``, the
notifications, each built the way :class:`~nerve.sources.github.GitHubSource`
builds it (enrichment included), and ``github_repos``, the repository watch,
each built the way :class:`~nerve.sources.github_repos.GitHubReposSource`
builds it. The hosted source consumer writes every record to the
``channel_observations`` buffer under the provider's name (see
:mod:`nerve.channels.hosted.sources`), and this source turns the buffered
payloads back into :class:`SourceRecord` objects. It makes no GitHub calls.

The source keeps the provider's name, so it runs as ``source:github`` or
``source:github_repos`` with the guardrail, drop hook, condensing, health
checks, consumer cursors and ``messages`` gates of the polling source.

The cursor is the buffer's row ID, as for the chat channel drains. A cursor
left by the polling source (a timestamp, or the repository watch's JSON) reads
as 0, so the first drain starts at the beginning of the buffer.

Buffered payload (one row per record)::

    {"event_id": ..., "connection_id": ..., "conversation_id": "owner/repo",
     "record": {"id", "record_type", "summary", "content",
                "timestamp": RFC 3339,
                "metadata": [{"key", "value", "values"}, ...]}}

Metadata values arrive as strings. ``actors`` and ``changes`` are lists, and
``unread`` becomes a boolean.
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
# The providers that have a hosted GitHub source. Each one's buffer channel
# and source name is the provider's name.
PROVIDERS = ("github", "github_repos")
# Metadata keys whose value is a list of strings, and those that are flags.
_LIST_KEYS = frozenset({"actors", "changes"})
_FLAG_KEYS = frozenset({"unread"})


class HostedGitHubSource(Source):
    """A GitHub source over the hosted records buffer of its provider.

    *provider* is ``github`` (the default) or ``github_repos``: the buffer
    channel that the hosted source consumer writes, and the source name.
    """

    source_name = SOURCE_NAME
    push_fed = True

    def __init__(self, db: Database, provider: str = SOURCE_NAME):
        if provider not in PROVIDERS:
            raise ValueError(f"no hosted GitHub source for provider {provider!r}")
        self._db = db
        self.source_name = provider
        self._buffer = provider

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Read buffered records past *cursor*, oldest first.

        A failed read raises, so the runner reports it and its health checks
        count it.
        """
        rows = await self._db.read_channel_observations(
            self._buffer, after_id=self._position(cursor), limit=limit,
        )
        if not rows:
            return FetchResult(records=[], next_cursor=cursor, has_more=False)

        records: list[SourceRecord] = []
        for row_id, payload in rows:
            record = (
                record_from_buffer(payload, source=self.source_name)
                if isinstance(payload, dict) else None
            )
            if record is None:
                logger.warning(
                    "Hosted %s source: skipping unreadable buffer row %s", self.source_name, row_id,
                )
                continue
            records.append(record)
        # The cursor tracks what was scanned, so an unreadable row is passed
        # and does not stop the drain.
        return FetchResult(
            records=records,
            next_cursor=str(rows[-1][0]),
            has_more=len(rows) >= limit,
        )

    async def has_undrained(self, cursor: str | None) -> bool:
        """Whether a record waits in the buffer past *cursor*.

        Normally none does: the cron service drains the buffer as records are
        delivered. One row is enough to know; a failed read answers no, so a
        forecast never fails for it.
        """
        try:
            rows = await self._db.read_channel_observations(
                self._buffer, after_id=self._position(cursor, quiet=True), limit=1,
            )
        except Exception:
            logger.debug("Hosted %s source: checking the buffer failed", self.source_name, exc_info=True)
            return False
        return bool(rows)

    def _position(self, cursor: str | None, *, quiet: bool = False) -> int:
        """The buffer row ID that *cursor* names, or 0."""
        if not cursor:
            return 0
        try:
            return int(cursor)
        except ValueError:
            if not quiet:
                logger.info(
                    "Hosted %s source: cursor %.80r is not a buffer position (a cursor of "
                    "the polling source); draining from the start of the buffer",
                    self.source_name, cursor,
                )
            return 0


def record_from_buffer(payload: dict[str, Any], source: str = SOURCE_NAME) -> SourceRecord | None:
    """The inbox record of one buffered payload, or ``None`` when it is unusable.

    *source* is the provider whose buffer held it, and the record's source.
    """
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
        source=source,
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


__all__ = ["BUFFER_CHANNEL", "PROVIDERS", "SOURCE_NAME", "HostedGitHubSource", "record_from_buffer"]
