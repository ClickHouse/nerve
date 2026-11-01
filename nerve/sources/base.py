"""Abstract base class for all data sources."""

from __future__ import annotations

import abc

from nerve.sources.models import FetchResult, SourceRecord


class Source(abc.ABC):
    """Abstract base for all data sources.

    Each source implements fetch() which returns records since a cursor.
    The cursor is an opaque string — each source defines its own semantics
    (message ID, timestamp, page token, etc.).
    """

    source_name: str = ""

    # True for a source that reads a buffer only this process writes, as
    # records are delivered to it: the hosted sources, which the cron service
    # also runs as soon as records arrive, and the channel drains, whose buffer
    # the channel transport fills. Nothing is added to such a buffer while the
    # machine is parked, so the cron schedule does not count the source's
    # scheduled run as work the instance will start on its own — unless
    # delivered records wait in the buffer (see has_undrained) — and a platform
    # that parks the machine is not told to wake it for the run. A source that
    # polls a remote service on its schedule keeps the default: its run is the
    # only way its records come in.
    push_fed: bool = False

    # True for a source whose first records the consumers must read: the
    # hosted email source, which a person switches on to send mail to the
    # agent. When the cron service registers its runner, it starts a cursor on
    # the source at the source's newest message for each consumer that a
    # ``messages`` gate of a cron job names and for each consumer that holds a
    # cursor on another source, unless the consumer has a cursor on this
    # source already (Database.start_consumer_cursors). Without it, a
    # consumer's cursor on a source starts at the newest message when the
    # consumer first reads the source, so the records that came before it are
    # never read, and a ``messages`` gate for any source does not see them.
    starts_consumer_cursors: bool = False

    async def has_undrained(self, cursor: str | None) -> bool:
        """Whether a run from *cursor* would find delivered records waiting.

        Asked by the cron schedule about a ``push_fed`` source. Delivered
        records that wait in the buffer are work the scheduled run will do, so
        that run counts as the next scheduled task, and a platform that parks
        the machine keeps it up, or wakes it, for them; with nothing waiting
        the run is not counted. The default answers no; a source that can look
        at its buffer cheaply overrides it. Must not raise.
        """
        return False

    @abc.abstractmethod
    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Fetch new records since cursor.

        Args:
            cursor: Opaque cursor from the previous fetch. None on first run.
            limit: Maximum number of records to return.

        Returns:
            FetchResult with records, next_cursor, and has_more flag.
        """
        ...

    async def preprocess(self, records: list[SourceRecord]) -> list[SourceRecord]:
        """Source-specific preprocessing of fetched records.

        Called by SourceRunner after fetch() and before processing.
        Override to add source-specific content cleanup (e.g., stripping
        email boilerplate, normalizing markdown, etc.).

        Default: no-op (returns records unchanged).
        """
        return records

    async def close(self) -> None:
        """Optional cleanup — e.g. disconnect clients."""
        pass
