"""Hosted sources: records that the shared gateway polls for this agent.

In hosted mode the channel gateway polls a source instead of this process.
It applies the filters that Nerve publishes before it stores a record, keeps
the records in the agent's inbox while the agent is parked, and Nerve reads
them over the same streams that carry hosted channel traffic. GitHub is the
only hosted source.

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
    SourceFilters,
    SourceSubscription,
    Subscriptions,
)
from nerve.channels.hosted.contract.model import (
    MAX_SUBSCRIPTION_PATTERN_BYTES,
    MAX_SUBSCRIPTION_PATTERNS,
    ZERO_TIME,
    valid_pattern,
)
from nerve.channels.hosted.contract.wire import format_time
from nerve.channels.hosted.intake import Disposition

if TYPE_CHECKING:
    from nerve.config import GitHubSyncConfig, NerveConfig
    from nerve.db import Database

logger = logging.getLogger(__name__)

GITHUB = "github"

# Row target of a provider's buffer. The gateway keeps at most this many
# source records for an agent, and the runner drains the buffer after every
# delivery, so it is reached only while the drain keeps failing.
BUFFER_MAX_ROWS = 2_000


def hosted_sources(config: NerveConfig) -> list[str]:
    """The source providers whose records come from the gateway."""
    return [GITHUB] if config.sync.github.mode == "hosted" else []


# ---------------------------------------------------------------------- #
#  Subscriptions                                                          #
# ---------------------------------------------------------------------- #


def subscriptions_for(config: NerveConfig, providers: Iterable[str]) -> Subscriptions:
    """What the agent subscribes to, among the hosted *providers*.

    A provider that is switched off is left out, so the gateway stops polling
    it for this agent.
    """
    sources: list[SourceSubscription] = []
    if GITHUB in providers and config.sync.github.enabled:
        sources.append(github_subscription(config.sync.github))
    return Subscriptions(version=SUBSCRIPTIONS_VERSION, sources=tuple(sources))


def github_subscription(settings: GitHubSyncConfig) -> SourceSubscription:
    """The GitHub subscription, with the inbox guardrail's patterns as filters.

    A list the gateway cannot take is published wider, never narrower: an
    allow list goes empty and a deny list keeps the patterns that fit. The
    gateway then stores records that Nerve's inbox filter still drops, which
    costs a wake but loses nothing.
    """
    return SourceSubscription(
        provider=GITHUB,
        filters=SourceFilters(
            allow_repos=_allow_list("allow_repos", settings.allow_repos),
            deny_repos=_deny_list("deny_repos", settings.deny_repos),
            allow_actors=_allow_list("allow_actors", settings.allow_actors),
            deny_actors=_deny_list("deny_actors", settings.deny_actors),
            allow_reasons=_allow_list("allow_reasons", settings.allow_reasons),
            deny_reasons=_deny_list("deny_reasons", settings.deny_reasons),
            deny_ci_branches=_deny_list("deny_ci_branches", settings.deny_ci_branches),
        ),
        pr_watch=bool(settings.pr_watch),
    )


def _patterns(values: object) -> list[str]:
    """The non-blank patterns of a setting, stripped as the inbox filter strips them."""
    if not isinstance(values, (list, tuple)):
        return []
    return [text for text in (str(value).strip() for value in values if value is not None) if text]


def _allow_list(key: str, values: object) -> tuple[str, ...]:
    patterns = _patterns(values)
    if len(patterns) <= MAX_SUBSCRIPTION_PATTERNS and all(valid_pattern(p) for p in patterns):
        return tuple(patterns)
    logger.warning(
        "sync.github.%s has more than %d patterns, or one longer than %d bytes or with a "
        "control character; the gateway is told to keep every record and the inbox "
        "filter applies the list alone",
        key, MAX_SUBSCRIPTION_PATTERNS, MAX_SUBSCRIPTION_PATTERN_BYTES,
    )
    return ()


def _deny_list(key: str, values: object) -> tuple[str, ...]:
    patterns = _patterns(values)
    kept = [p for p in patterns if valid_pattern(p)][:MAX_SUBSCRIPTION_PATTERNS]
    if len(kept) < len(patterns):
        logger.warning(
            "sync.github.%s has more than %d patterns, or one longer than %d bytes or with "
            "a control character; the gateway gets the first %d usable ones and the inbox "
            "filter applies the rest",
            key, MAX_SUBSCRIPTION_PATTERNS, MAX_SUBSCRIPTION_PATTERN_BYTES, len(kept),
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
        except Exception:  # noqa: BLE001 - the record is stored; the runner's schedule still drains it
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
    "HostedSourceConsumer",
    "buffered_record",
    "github_subscription",
    "hosted_sources",
    "subscriptions_for",
]
