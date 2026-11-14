"""Hosted sources: drain the records that the channel gateway delivered.

In hosted mode the channel gateway polls a provider for the agent and
streams the records to Nerve. The hosted source consumer writes every record
to the ``channel_observations`` buffer under the provider's name (see
:mod:`nerve.channels.hosted.sources`), and a :class:`HostedSource` turns the
buffered payloads back into :class:`SourceRecord` objects. It makes no
provider calls.

The source keeps the provider's name, so it runs as ``source:<provider>``
with the guardrail, condensing, health checks, consumer cursors and
``messages`` gates of every source. The cursor is the buffer's row ID, as for
the chat channel drains.

:class:`HostedSource` drains a generic source, such as Linear: the record's
metadata holds its record ``schema`` and its ``data``, the JSON object that
the provider defines, with every member. GitHub has a closed vocabulary of its
own (:class:`~nerve.sources.github_hosted.HostedGitHubSource`).

Buffered payload of a generic record (one row per record)::

    {"event_id": ..., "connection_id": ..., "conversation_id": ...,
     "record": {"id", "record_type", "schema", "summary", "content",
                "timestamp": RFC 3339, "metadata": [], "data": {...}}}
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from nerve.sources.base import Source
from nerve.sources.models import FetchResult, SourceRecord
from nerve.sources.source_json import record_metadata

if TYPE_CHECKING:
    from nerve.db import Database

logger = logging.getLogger(__name__)


class HostedSource(Source):
    """A source over the hosted records buffer of one provider.

    *provider* is the buffer channel that the hosted source consumer writes,
    and the source name.
    """

    push_fed = True

    def __init__(self, db: Database, provider: str):
        self._db = db
        self.source_name = provider
        self._buffer = provider

    def record(self, payload: dict[str, Any]) -> SourceRecord | None:
        """The inbox record of one buffered payload, or ``None`` when it is unusable."""
        return record_from_buffer(payload, source=self.source_name)

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
            record = self.record(payload) if isinstance(payload, dict) else None
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


def record_fields(payload: dict[str, Any]) -> dict[str, Any] | None:
    """The record of a buffered payload, or ``None`` without its ID, type and summary."""
    record = payload.get("record")
    if not isinstance(record, dict):
        return None
    if not all(isinstance(record.get(key), str) and record[key] for key in ("id", "record_type", "summary")):
        return None
    return record


def record_from_buffer(payload: dict[str, Any], source: str) -> SourceRecord | None:
    """The inbox record of one buffered generic payload, or ``None`` when it is unusable.

    *source* is the provider whose buffer held it, and the record's source.
    The record's metadata holds its ``schema`` and its ``data``, with every
    member (:func:`~nerve.sources.source_json.record_metadata`); a record
    without data has no metadata.
    """
    record = record_fields(payload)
    if record is None:
        return None
    content = record.get("content")
    timestamp = record.get("timestamp")
    data = record.get("data")
    schema = record.get("schema")
    return SourceRecord(
        id=record["id"],
        source=source,
        record_type=record["record_type"],
        summary=record["summary"],
        content=content if isinstance(content, str) else "",
        timestamp=timestamp if isinstance(timestamp, str) else "",
        metadata=(
            record_metadata(schema if isinstance(schema, str) else "", dict(data))
            if isinstance(data, dict) else {}
        ),
    )


__all__ = ["HostedSource", "record_fields", "record_from_buffer"]
