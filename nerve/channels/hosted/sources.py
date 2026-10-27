"""Hosted sources: records that the shared gateway polls for this agent.

In hosted mode the channel gateway polls a source instead of this process.
It applies the filters that Nerve publishes before it stores a record, keeps
the records in the agent's inbox until Nerve reads them, also while Nerve is
not running, and Nerve reads them over the same streams that carry hosted
channel traffic. GitHub is the hosted source, as a whole: with
``sync.github.mode: hosted`` both the notifications (provider ``github``) and
the repository watch (provider ``github_repos``) come from the gateway.

:class:`HostedSourceConsumer` takes a provider's records from the inbox reader
and writes each one to the local ``channel_observations`` buffer under the
provider's name. The provider's source runner drains that buffer into the
inbox with the guardrail, drop hook, and condensing of the polling source
(:mod:`nerve.sources.github_hosted`). A record is acknowledged only after its
buffer row is written.

:func:`subscriptions_for` builds the ``subscriptions`` frame from the same
settings that build the inbox filter, so the gateway and Nerve drop the same
records.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

from nerve.channels.hosted.contract import (
    EVENT_KIND_SOURCE_RECORD,
    SUBSCRIPTIONS_VERSION,
    Event,
    RepoWatch,
    SourceFilters,
    SourceSubscription,
    Subscriptions,
)
from nerve.channels.hosted.contract.model import (
    MAX_REPO_WATCHES,
    MAX_SUBSCRIPTION_PATTERN_BYTES,
    MAX_SUBSCRIPTION_PATTERNS,
    REPO_WATCH_PROVIDER,
    REPO_WATCH_TYPES,
    ZERO_TIME,
    valid_pattern,
    valid_repo_name,
)
from nerve.channels.hosted.contract.wire import format_time
from nerve.channels.hosted.intake import Disposition

if TYPE_CHECKING:
    from nerve.config import GitHubReposSyncConfig, GitHubSyncConfig, NerveConfig
    from nerve.db import Database

logger = logging.getLogger(__name__)

GITHUB = "github"
# The repository watch: the repositories that sync.github_repos names.
GITHUB_REPOS = REPO_WATCH_PROVIDER

# Row target of a provider's buffer. The gateway keeps at most this many
# source records for an agent, and the runner drains the buffer after every
# delivery, so it is reached only while the drain keeps failing.
BUFFER_MAX_ROWS = 2_000


def hosted_sources(config: NerveConfig) -> list[str]:
    """The source providers whose records come from the gateway.

    GitHub is hosted as a whole: ``sync.github.mode: hosted`` makes the
    repository watch (``github_repos``) hosted too.
    """
    return [GITHUB, GITHUB_REPOS] if config.sync.github.mode == "hosted" else []


# ---------------------------------------------------------------------- #
#  Subscriptions                                                          #
# ---------------------------------------------------------------------- #


def subscriptions_for(config: NerveConfig, providers: Iterable[str]) -> Subscriptions:
    """What the agent subscribes to, among the hosted *providers*.

    A provider that is switched off is left out, so the gateway stops polling
    it for this agent. So is a repository watch that names no repository:
    the ``github_repos`` subscription is sent only while the watch is on and
    lists a repository, and an agent without one sends the same frame as
    before the watch existed.
    """
    providers = set(providers)
    sources: list[SourceSubscription] = []
    if GITHUB in providers and config.sync.github.enabled:
        sources.append(github_subscription(config.sync.github))
    if GITHUB_REPOS in providers and config.sync.github_repos.enabled:
        watch = github_repos_subscription(config.sync.github_repos)
        if watch is not None:
            sources.append(watch)
    return Subscriptions(version=SUBSCRIPTIONS_VERSION, sources=tuple(sources))


def github_subscription(settings: GitHubSyncConfig) -> SourceSubscription:
    """The GitHub subscription, with the inbox guardrail's patterns as filters.

    A list the gateway cannot take is published wider, never narrower: an
    allow list goes empty and a deny list keeps the patterns that fit. The
    gateway then stores records that Nerve's inbox filter still drops, which
    costs a wake but loses nothing.

    ``sync.github.inbox`` is published as ``inbox`` only when it is ``all``.
    The gateway reads a missing one as ``participating``, so the frame of an
    agent that keeps the default stays exactly as it was.
    """
    return SourceSubscription(
        provider=GITHUB,
        filters=SourceFilters(
            allow_repos=_allow_list("sync.github.allow_repos", settings.allow_repos),
            deny_repos=_deny_list("sync.github.deny_repos", settings.deny_repos),
            allow_actors=_allow_list("sync.github.allow_actors", settings.allow_actors),
            deny_actors=_deny_list("sync.github.deny_actors", settings.deny_actors),
            allow_reasons=_allow_list("sync.github.allow_reasons", settings.allow_reasons),
            deny_reasons=_deny_list("sync.github.deny_reasons", settings.deny_reasons),
            deny_ci_branches=_deny_list("sync.github.deny_ci_branches", settings.deny_ci_branches),
        ),
        pr_watch=bool(settings.pr_watch),
        inbox="all" if settings.inbox == "all" else "",
    )


def github_repos_subscription(settings: GitHubReposSyncConfig) -> SourceSubscription | None:
    """The repository watch subscription, or ``None`` when it watches nothing.

    Every repository of ``sync.github_repos.repos`` carries the same
    settings, all of them sent. The config refuses to load names the gateway
    would not take; a list built some other way loses those names, past
    :data:`MAX_REPO_WATCHES` too, with a warning, so that the gateway never
    closes the stream over them. ``deny_authors`` goes as a deny list does:
    the patterns that fit, while the inbox filter applies them all.
    """
    names: list[str] = []
    seen: set[str] = set()
    for value in settings.repos or ():
        name = str(value or "").strip()
        if not name:
            continue
        # The gateway compares names ignoring case, as GitHub does.
        if not valid_repo_name(name) or name.lower() in seen:
            logger.warning("sync.github_repos.repos: %r is not published to the gateway", name)
            continue
        seen.add(name.lower())
        names.append(name)
    if len(names) > MAX_REPO_WATCHES:
        logger.warning(
            "sync.github_repos.repos names %d repositories; the gateway watches the first %d",
            len(names), MAX_REPO_WATCHES,
        )
        names = names[:MAX_REPO_WATCHES]
    chosen = {str(kind).strip().lower() for kind in settings.types or ()}
    types = tuple(kind for kind in REPO_WATCH_TYPES if kind in chosen)
    if not names or not types:
        return None
    deny_authors = _deny_list("sync.github_repos.deny_authors", settings.deny_authors)
    return SourceSubscription(
        provider=GITHUB_REPOS,
        repos=tuple(
            RepoWatch(
                name=name,
                types=types,
                include_bots=bool(settings.include_bots),
                include_drafts=bool(settings.include_drafts),
                updates=bool(settings.updates),
                deny_authors=deny_authors,
            )
            for name in names
        ),
    )


def _patterns(values: object) -> list[str]:
    """The non-blank patterns of a setting, stripped as the inbox filter strips them."""
    if not isinstance(values, (list, tuple)):
        return []
    return [text for text in (str(value).strip() for value in values if value is not None) if text]


def _allow_list(setting: str, values: object) -> tuple[str, ...]:
    patterns = _patterns(values)
    if len(patterns) <= MAX_SUBSCRIPTION_PATTERNS and all(valid_pattern(p) for p in patterns):
        return tuple(patterns)
    logger.warning(
        "%s has more than %d patterns, or one longer than %d bytes or with a "
        "control character; the gateway is told to keep every record and the inbox "
        "filter applies the list alone",
        setting, MAX_SUBSCRIPTION_PATTERNS, MAX_SUBSCRIPTION_PATTERN_BYTES,
    )
    return ()


def _deny_list(setting: str, values: object) -> tuple[str, ...]:
    patterns = _patterns(values)
    kept = [p for p in patterns if valid_pattern(p)][:MAX_SUBSCRIPTION_PATTERNS]
    if len(kept) < len(patterns):
        logger.warning(
            "%s has more than %d patterns, or one longer than %d bytes or with "
            "a control character; the gateway gets the first %d usable ones and the inbox "
            "filter applies the rest",
            setting, MAX_SUBSCRIPTION_PATTERNS, MAX_SUBSCRIPTION_PATTERN_BYTES, len(kept),
        )
    return tuple(kept)


# ---------------------------------------------------------------------- #
#  Consumer                                                               #
# ---------------------------------------------------------------------- #


def buffered_record(event: Event) -> dict[str, Any]:
    """The buffer payload of one source record event.

    ``record.metadata`` keeps the wire entries (``key`` with ``value`` or
    ``values``); the source that drains the buffer knows which keys hold
    lists and which hold flags.
    """
    record = event.record
    timestamp = record.timestamp if record.timestamp != ZERO_TIME else event.occurred_at
    return {
        "event_id": event.event_id,
        "connection_id": str(event.connection_id),
        "conversation_id": event.conversation.id,
        "record": {
            "id": record.id,
            "record_type": record.record_type,
            "summary": record.summary,
            "content": record.content,
            "timestamp": format_time(timestamp) if timestamp != ZERO_TIME else "",
            "metadata": [
                {"key": entry.key, "value": entry.value, "values": list(entry.values)}
                for entry in record.metadata
            ],
        },
    }


class HostedSourceConsumer:
    """The inbox reader's consumer for one provider's hosted source records.

    It holds no stream state and never refuses capacity: the reader pauses
    every provider while any consumer refuses, and a write to the local
    buffer needs no room. A switched-off source still buffers the records
    already on their way; its runner drains them once it is switched on.
    """

    def __init__(
        self,
        provider: str,
        db: Database,
        config_getter: Callable[[], NerveConfig],
    ) -> None:
        self._provider = provider
        self._db = db
        self._config = config_getter
        self._listener: Callable[[str], Any] | None = None
        self._tasks: set[asyncio.Future] = set()

    @property
    def name(self) -> str:
        return self._provider

    def can_accept(self) -> bool:
        return True

    def set_delivery_listener(self, listener: Callable[[str], Any] | None) -> None:
        """Call *listener* with the provider name after each stored record.

        A listener that returns an awaitable runs as its own task, so the
        reader never waits for it.
        """
        self._listener = listener

    async def deliver(self, event: Event) -> Disposition:
        purpose = event.admission.purpose
        if event.kind != EVENT_KIND_SOURCE_RECORD or purpose != "observe" or event.record is None:
            return Disposition.rejected(
                "admission_rejected", f"{event.kind} ({purpose}) is not a {self._provider} source record",
            )
        config = self._config()
        try:
            await self._db.insert_channel_observation(
                channel=self._provider,
                channel_key=f"{self._provider}:{event.conversation.id}",
                payload=buffered_record(event),
                ttl_days=config.sync.message_ttl_days,
                max_rows=BUFFER_MAX_ROWS,
            )
        except Exception as error:  # noqa: BLE001 - the row comes back on a later read
            logger.warning(
                "Could not buffer the %s record %s: %s", self._provider, event.record.id, error,
            )
            return Disposition.deferred("the source record could not be stored")
        self._notify()
        return Disposition.accepted()

    def _notify(self) -> None:
        listener = self._listener
        if listener is None:
            return
        try:
            outcome = listener(self._provider)
        except Exception:  # the record is stored; the runner's schedule still drains it
            logger.exception("The %s delivery listener failed", self._provider)
            return
        if inspect.isawaitable(outcome):
            task = asyncio.ensure_future(outcome)
            self._tasks.add(task)
            task.add_done_callback(self._finished)

    def _finished(self, task: asyncio.Future) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error(
                "The %s delivery listener failed: %s", self._provider, task.exception(),
                exc_info=task.exception(),
            )

    async def stop(self) -> None:
        """Cancel the listener calls still running."""
        tasks, self._tasks = list(self._tasks), set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


__all__ = [
    "BUFFER_MAX_ROWS",
    "GITHUB",
    "GITHUB_REPOS",
    "HostedSourceConsumer",
    "buffered_record",
    "github_repos_subscription",
    "github_subscription",
    "hosted_sources",
    "subscriptions_for",
]
