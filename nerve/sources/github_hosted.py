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
The drain and the buffered payload are those of every hosted source
(:mod:`nerve.sources.hosted`).

The source keeps the provider's name, so it runs as ``source:github`` or
``source:github_repos`` with the guardrail, drop hook, condensing, health
checks, consumer cursors and ``messages`` gates of the polling source.

The cursor is the buffer's row ID, as for the chat channel drains. A cursor
left by the polling source (a timestamp, or the repository watch's JSON) reads
as 0, so the first drain starts at the beginning of the buffer.

Metadata values arrive as strings. ``actors`` and ``changes`` are lists, and
``unread`` becomes a boolean.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from nerve.sources import hosted
from nerve.sources.hosted import HostedRecordSource
from nerve.sources.models import SourceRecord

if TYPE_CHECKING:
    from nerve.db import Database

SOURCE_NAME = "github"
# The buffer channel that the hosted source consumer writes for this source.
BUFFER_CHANNEL = "github"
# The providers that have a hosted GitHub source. Each one's buffer channel
# and source name is the provider's name.
PROVIDERS = ("github", "github_repos")
# Metadata keys whose value is a list of strings, and those that are flags.
_LIST_KEYS = frozenset({"actors", "changes"})
_FLAG_KEYS = frozenset({"unread"})


class HostedGitHubSource(HostedRecordSource):
    """A GitHub source over the hosted records buffer of its provider.

    *provider* is ``github`` (the default) or ``github_repos``: the buffer
    channel that the hosted source consumer writes, and the source name.
    """

    source_name = SOURCE_NAME
    list_keys = _LIST_KEYS
    flag_keys = _FLAG_KEYS

    def __init__(self, db: Database, provider: str = SOURCE_NAME):
        if provider not in PROVIDERS:
            raise ValueError(f"no hosted GitHub source for provider {provider!r}")
        super().__init__(db, provider)


def record_from_buffer(payload: dict[str, Any], source: str = SOURCE_NAME) -> SourceRecord | None:
    """The inbox record of one buffered GitHub payload, or ``None`` when it is unusable.

    *source* is the provider whose buffer held it, and the record's source.
    """
    return hosted.record_from_buffer(payload, source, list_keys=_LIST_KEYS, flag_keys=_FLAG_KEYS)


__all__ = ["BUFFER_CHANNEL", "PROVIDERS", "SOURCE_NAME", "HostedGitHubSource", "record_from_buffer"]
