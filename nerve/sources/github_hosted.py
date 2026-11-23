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
``unread`` and ``mentions_agent`` become booleans.

A ``github_thread_item`` record (one comment, review or description of a
thread, with its author) gets a few fixed lines before its text that say
where it was written and by whom (:func:`thread_item_content`), and its
author as its ``actors``, so the actor guardrail applies to the author.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from nerve.sources.base import Source
from nerve.sources.github import THREAD_ITEM_RECORD_TYPE
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
_FLAG_KEYS = frozenset({"unread", "mentions_agent"})

# How a thread item's message names its thread, its kind and its author's
# relation to the agent (the gateway's ``author_relation``).
_SUBJECT_WORDS = {"Issue": "issue", "PullRequest": "pull request"}
_ITEM_WORDS = {
    "body": "description", "comment": "comment", "review": "review",
    "review_comment": "review comment",
}
_RELATION_WORDS = {
    "self": "the agent itself",
    "member": "member of the organization",
    "team": "member of a team the agent listens to",
    "collaborator": "collaborator on the repository",
    "app": "a GitHub App",
    "agent": "another agent of the organization",
    "listed": "listed by the organization's administrators",
    "outside": "outside the organization",
}


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
    content = content if isinstance(content, str) else ""
    timestamp = record.get("timestamp")
    metadata = _metadata(record.get("metadata"))
    if record_type == THREAD_ITEM_RECORD_TYPE:
        author = metadata.get("author")
        metadata["actors"] = [author] if isinstance(author, str) and author else []
        content = thread_item_content(metadata, content)
    return SourceRecord(
        id=record_id,
        source=source,
        record_type=record_type,
        summary=summary,
        content=content,
        timestamp=timestamp if isinstance(timestamp, str) else "",
        metadata=metadata,
    )


def thread_item_content(metadata: dict[str, Any], text: str) -> str:
    """The message of a thread item: fixed lines about the item, then its *text*.

    The lines show what the gateway sent: the repository, the thread, its
    state, the item's kind, the author with their relation to the agent,
    whether the item mentions the agent, and the links. A thread whose title
    the gateway withheld says so.
    """
    subject = _SUBJECT_WORDS.get(metadata.get("subject_type", ""), "thread")
    kind = _ITEM_WORDS.get(metadata.get("item_kind", ""), "item")
    if metadata.get("review_state"):
        kind = f"{kind} ({metadata['review_state']})"
    author = metadata.get("author", "") or "unknown"
    if metadata.get("author_kind") == "bot":
        author = f"{author} (bot)"
    relation = _RELATION_WORDS.get(metadata.get("author_relation", ""))
    title = metadata.get("title", "")
    lines = [
        f"Repository: {metadata.get('repo_name', '')}",
        f"Thread: {subject} #{metadata.get('number', '')}",
        f"Title: {title}" if title else "Title: (not shown)",
        f"State: {metadata.get('state', '')}",
        f"Item: {kind}",
        f"Author: {author}, {relation}" if relation else f"Author: {author}",
        f"Mentions the agent: {'yes' if metadata.get('mentions_agent') else 'no'}",
        f"Item URL: {metadata.get('item_url', '')}",
        f"Thread URL: {metadata.get('subject_url', '')}",
    ]
    return "\n".join(lines) + (f"\n\n{text}" if text else "")


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
