"""SourceRunner — pure ingestion pipeline.

Reads cursor from DB, calls source.fetch(), persists records to the
source_messages inbox, optionally condenses long content via LLM,
and advances the cursor. No processing (agent/memorize/notify) —
consumption is handled separately by consumer tools.

Pipeline: fetch → source.preprocess → guardrail → persist → LLM condense
→ drop hook → advance cursor

A run repeats that pipeline while the source reports ``has_more``, so a backlog
deeper than ``batch_size`` drains in one run rather than one batch per tick.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import tempfile
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable

from nerve.cron.jobs import RESERVED_JOB_ID_PREFIX
from nerve.sources.models import IngestResult, SourceRecord, cursor_text
from nerve.utils.llm import response_text

if TYPE_CHECKING:
    from nerve.db import Database
    from nerve.sources.base import Source
    from nerve.sources.filters import InboxFilter

logger = logging.getLogger(__name__)


@dataclass
class SourceHealth:
    """Tracks circuit breaker state for a source runner."""

    consecutive_failures: int = 0
    last_error: str | None = None
    last_error_at: datetime | None = None
    last_success_at: datetime | None = None
    state: str = "healthy"  # healthy | degraded | open
    backoff_until: datetime | None = None

    # Thresholds
    DEGRADED_AFTER: int = 2
    OPEN_AFTER: int = 5
    BASE_BACKOFF_SECS: int = 30
    MAX_BACKOFF_SECS: int = 3600

    def record_success(self) -> str | None:
        """Record a successful run. Returns previous state if it changed."""
        prev = self.state
        self.consecutive_failures = 0
        self.last_error = None
        self.last_success_at = datetime.now(timezone.utc)
        self.state = "healthy"
        self.backoff_until = None
        return prev if prev != "healthy" else None

    def record_failure(self, error: str) -> str | None:
        """Record a failed run. Returns new state if it transitioned."""
        prev = self.state
        self.consecutive_failures += 1
        self.last_error = error
        self.last_error_at = datetime.now(timezone.utc)

        # State transitions
        if self.consecutive_failures >= self.OPEN_AFTER:
            self.state = "open"
        elif self.consecutive_failures >= self.DEGRADED_AFTER:
            self.state = "degraded"

        # Compute backoff: min(30 * 2^(failures-1), 3600) seconds
        backoff_secs = min(
            self.BASE_BACKOFF_SECS * (2 ** (self.consecutive_failures - 1)),
            self.MAX_BACKOFF_SECS,
        )
        self.backoff_until = datetime.now(timezone.utc) + timedelta(seconds=backoff_secs)

        return self.state if self.state != prev else None

    @property
    def is_backed_off(self) -> bool:
        """Check if the runner should skip due to active backoff."""
        if self.backoff_until is None:
            return False
        return datetime.now(timezone.utc) < self.backoff_until

# Records with content longer than this (after source.preprocess) are sent
# to a fast LLM for extraction/condensation.
_CONDENSE_THRESHOLD = 800  # chars

# Per-record guardrail drop lines emitted per run (a noisy source could
# otherwise drop hundreds at once).
_MAX_DROP_LOG_LINES = 10

# Wall-clock cap on one drop-hook run. The hook runs inside the sync, under
# the runner's lock, so a hung command must not stall the source.
_DROP_HOOK_TIMEOUT_SECS = 60

# After the timeout kill: how long to wait for the hook to be reaped. Bounds
# the case of a process that left the hook's process group (setsid) and so
# survived the group kill.
_DROP_HOOK_REAP_SECS = 5

# Tail of a drop hook's stderr kept in the log line.
_DROP_HOOK_STDERR_CHARS = 500

# (id, timestamp) pairs of dropped records the hook already received, kept per
# runner so that a record listed again is not passed again. A source with an
# inclusive cursor (GitHub) lists the records in the cursor's own second on
# every run until a newer one arrives. The oldest pairs are forgotten first.
_DROP_HOOK_SEEN_MAX = 4096

# Batches one run drains before leaving the rest for the next tick. Bounds a
# very deep backlog, and a source whose has_more never goes false.
_MAX_PASSES = 20

_CONDENSE_PROMPT = (
    "Extract the essential information from this source record content.\n"
    "Rules:\n"
    "- Keep: key facts, amounts, dates, names, identifiers, action items, deadlines\n"
    "- Keep ALL actionable URLs: booking links, messaging threads, payment links, "
    "reply links, tracking pages, confirmation links\n"
    "- Remove: legal disclaimers, marketing copy, boilerplate, footer text, "
    "unsubscribe links, social media links, tracking pixels\n"
    "- Preserve the original structure and key details\n"
    "- Return ONLY the cleaned content, no preamble or commentary\n"
    "- If the message is mostly noise, return just the 1-2 core facts"
)


def _file_tail(f: Any, max_chars: int) -> str:
    """The last *max_chars* of a binary file, decoded and stripped."""
    try:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(max(0, size - max_chars * 4))
        return f.read().decode(errors="replace").strip()[-max_chars:]
    except (OSError, ValueError):
        return ""


@dataclass
class _BatchResult:
    """One batch's outcome, plus what the drain loop needs to page onward."""

    records_ingested: int
    records_dropped: int = 0
    next_cursor: str | None = None
    has_more: bool = False
    error: str | None = None


class SourceRunner:
    """Fetches records from a source and persists them to the inbox.

    Pure ingestion — no agent processing. Consumption happens via
    consumer tools (poll_source, read_source).

    Args:
        source: The data source to fetch from.
        db: Database for cursor persistence and logging.
        batch_size: Max records per fetch.
        condense: Enable LLM condensation for long records.
        condense_model: Model name for condensation (the configured ``memory.fast_model``).
        condense_prompt: Custom system prompt for condensation (uses default if empty).
        condense_client_factory: Callable that creates an async Anthropic client.
            Provided by NerveConfig.create_async_anthropic_client — handles provider
            detection, credentials, timeout, and proxy routing automatically.
        ttl_days: TTL for persisted source messages.
        inbox_filter: Optional guardrail applied after preprocess and before
            persist. Records that don't pass are dropped — never written to
            the inbox, never seen by the agent. An inactive/None filter is a
            no-op. See :mod:`nerve.sources.filters`.
        drop_hook: Optional argv of a command run when the guardrail drops
            records. It reads one JSON object per dropped record on stdin.
            Best effort: a failure is logged and never fails the sync. See
            :meth:`_run_drop_hook`.
        schedule: Crontab or interval this runner asks to be scheduled on.
            Empty means "look me up in ``config.sync.<name>``", which is how
            every pull source works. A source configured somewhere else —
            a channel's ``source`` block, say — carries its own cadence
            here, because the alternative is a phantom ``config.sync``
            section that duplicates it, or a runner that is silently never
            scheduled. See :meth:`CronService._source_schedule`.
    """

    def __init__(
        self,
        source: Source,
        db: Database,
        batch_size: int = 50,
        condense: bool = False,
        condense_model: str = "",
        condense_prompt: str = "",
        condense_client_factory: Callable[[], Any] | None = None,
        ttl_days: int = 7,
        inbox_filter: InboxFilter | None = None,
        drop_hook: list[str] | None = None,
        schedule: str = "",
    ):
        self.source = source
        self.db = db
        self.batch_size = batch_size
        # Always derived, never passed in: the scheduler reserves the whole
        # `source:` namespace for these runners (see nerve/cron/jobs.py), and a
        # runner sitting outside it could be displaced by a user cron job.
        self.job_id = f"{RESERVED_JOB_ID_PREFIX}{source.source_name}"
        self.condense = condense
        self.condense_model = condense_model
        self.condense_prompt = condense_prompt
        self._client_factory = condense_client_factory
        self.ttl_days = ttl_days
        self.inbox_filter = inbox_filter
        self.drop_hook = self._valid_drop_hook(drop_hook)
        self._drop_hook_seen: OrderedDict[tuple[str, str], None] = OrderedDict()
        self.schedule = schedule
        self._lock = asyncio.Lock()
        self._condense_client: Any | None = None
        self.health = SourceHealth()
        self._notification_service: Any | None = None

    def set_notification_service(self, service: Any) -> None:
        """Set the notification service for degradation alerts."""
        self._notification_service = service

    async def wait_until_idle(self) -> None:
        """Return once no run of this runner is in progress; at once when idle.

        A run in progress may have fetched before newer records arrived, and
        :meth:`run` returns without fetching while one is, so a caller that
        must see those records waits for it first.
        """
        if self._lock.locked():
            async with self._lock:
                pass

    async def _notify_state_change(self, new_state: str) -> None:
        """Send a notification when health state transitions."""
        svc = self._notification_service
        if not svc:
            return
        priority = "urgent" if new_state == "open" else "high"
        title = f"Source {self.source.source_name}: {new_state}"
        body = (
            f"Circuit breaker is now **{new_state}** after "
            f"{self.health.consecutive_failures} consecutive failures.\n"
            f"Last error: {self.health.last_error or 'unknown'}"
        )
        try:
            await svc.send_notification(
                session_id="system",
                title=title,
                body=body,
                priority=priority,
            )
        except Exception as e:
            logger.warning("Failed to send health notification: %s", e)

    async def run(self) -> IngestResult:
        """Full ingestion cycle: fetch → preprocess → guardrail → persist → condense → drop hook → advance cursor.

        Uses a per-runner lock to prevent concurrent execution (e.g. cron
        firing while a manual sync is in progress).  If the lock is already
        held, the call returns immediately with 0 records.

        Includes circuit breaker: skips execution when in backoff period.
        """
        # Circuit breaker: skip if backed off
        if self.health.is_backed_off:
            remaining = (self.health.backoff_until - datetime.now(timezone.utc)).total_seconds()
            logger.info(
                "Source %s: circuit breaker active (%s), backoff %.0fs remaining — skipping",
                self.source.source_name, self.health.state, remaining,
            )
            return IngestResult(records_ingested=0)

        if self._lock.locked():
            logger.info("Source %s: sync already in progress, skipping", self.source.source_name)
            return IngestResult(records_ingested=0)

        async with self._lock:
            result = await self._run_locked()

        # Update health state based on result
        if result.error:
            new_state = self.health.record_failure(result.error)
            if new_state:
                logger.warning(
                    "Source %s: health transitioned to %s (%d consecutive failures)",
                    self.source.source_name, new_state, self.health.consecutive_failures,
                )
                await self._notify_state_change(new_state)
        else:
            prev_state = self.health.record_success()
            if prev_state:
                logger.info(
                    "Source %s: recovered from %s → healthy",
                    self.source.source_name, prev_state,
                )

        return result

    async def _run_locked(self) -> IngestResult:
        """Drain batches until the source reports no more, or _MAX_PASSES.

        A source arriving faster than one ``batch_size`` per tick never catches
        up on one batch per run, and a buffered one is then trimmed from the
        old end while the drain is still behind it.

        Each batch persists and advances the cursor before the next starts, so
        stopping at the bound leaves progress durable and the next run resumes
        there.
        """
        cursor = await self.db.get_sync_cursor(self.source.source_name)
        if cursor is None:
            await self._seed_consumer_cursors()
        total_ingested = 0
        total_dropped = 0

        for pass_number in range(1, _MAX_PASSES + 1):
            result = await self._ingest_batch(cursor)
            if result.error:
                # Earlier batches are persisted and their cursor moved. Report
                # the failure, keep the progress.
                return IngestResult(
                    records_ingested=total_ingested,
                    records_dropped=total_dropped,
                    error=result.error,
                )
            total_ingested += result.records_ingested
            total_dropped += result.records_dropped

            if not result.has_more:
                break
            # More to read but a standing cursor would re-read the same batch.
            if result.next_cursor == cursor:
                logger.warning(
                    "Source %s: reports more to read but did not advance its "
                    "cursor (%s). Stopping this run.",
                    self.source.source_name, cursor_text(cursor),
                )
                break
            cursor = result.next_cursor
            if pass_number == _MAX_PASSES:
                logger.warning(
                    "Source %s: still has a backlog after %d batches of %d. "
                    "Stopping until the next run. Raise batch_size or the "
                    "schedule if this repeats.",
                    self.source.source_name, _MAX_PASSES, self.batch_size,
                )

        return IngestResult(
            records_ingested=total_ingested, records_dropped=total_dropped,
        )

    async def _seed_consumer_cursors(self) -> None:
        """Before a source's first run, start the existing consumers' cursors on it.

        Before it stores anything, so that its first records are unread for
        them, and a ``messages`` gate on any source fires for them. Best
        effort: a failure is logged and the run goes on.
        """
        from nerve.cron.gates import _consumer_cursor_ttl_days

        try:
            seeded = await self.db.seed_consumer_cursors(
                self.source.source_name, ttl_days=_consumer_cursor_ttl_days(),
            )
        except Exception:  # noqa: BLE001 — the run itself must not fail on this
            logger.warning(
                "Source %s: could not start the consumers' cursors",
                self.source.source_name, exc_info=True,
            )
            return
        if seeded:
            logger.info(
                "Source %s: first run, started %d consumer cursor(s) on it",
                self.source.source_name, seeded,
            )

    async def _ingest_batch(self, cursor: str | None) -> _BatchResult:
        """One fetch → preprocess → guardrail → persist → condense → drop hook → advance cycle."""
        logger.info(
            "Source %s: fetching (cursor=%s, batch_size=%d)",
            self.source.source_name, cursor_text(cursor), self.batch_size,
        )

        try:
            result = await self.source.fetch(cursor, limit=self.batch_size)
        except Exception as e:
            logger.error("Source %s fetch failed: %s", self.source.source_name, e, exc_info=True)
            return _BatchResult(records_ingested=0, error=str(e))

        if not result.records:
            # Even with 0 records, advance cursor if it changed
            # (e.g., Telegram baseline establishing a cursor with no records)
            if result.next_cursor and result.next_cursor != cursor:
                await self.db.set_sync_cursor(self.source.source_name, result.next_cursor)
                logger.info(
                    "Source %s: no records, cursor advanced to %s",
                    self.source.source_name, cursor_text(result.next_cursor),
                )
            else:
                logger.info("Source %s: no new records", self.source.source_name)
            # No records means nothing to page past, whatever has_more says.
            return _BatchResult(records_ingested=0, next_cursor=result.next_cursor)

        # Ingestion pipeline:
        # 1. Source-specific cleanup (e.g., Gmail boilerplate stripping)
        records = await self.source.preprocess(result.records)

        # 1b. Guardrail: drop records that don't pass the inbox filter. This is
        # the choke point — dropped records are never persisted, so the agent
        # can never see them (worker-mode safety). The cursor still advances
        # below. A source with an inclusive cursor can list a dropped record
        # again; the drop hook skips one it already received (step 3b).
        dropped: list[SourceRecord] = []
        if self.inbox_filter is not None and self.inbox_filter.active:
            kept, dropped = self.inbox_filter.partition(records)
            if dropped:
                logger.info(
                    "Source %s: guardrail dropped %d/%d records (e.g. %r)",
                    self.source.source_name, len(dropped), len(records),
                    dropped[0].summary,
                )
                # Name the rule that fired — the summary alone can't tell a
                # repo drop from an actor or CI-branch drop, and the record
                # itself is gone after this point. Capped to keep logs sane.
                for record in dropped[:_MAX_DROP_LOG_LINES]:
                    rule = self.inbox_filter.rejects(record)
                    field = rule.field if rule else "?"
                    logger.info(
                        "Source %s: dropped %s on %s=%r — %s",
                        self.source.source_name, record.id, field,
                        (record.metadata or {}).get(field), record.summary,
                    )
            records = kept
        dropped_count = len(dropped)

        # 2. Persist to inbox (post-preprocess, pre-condense — human-readable)
        try:
            stored_ids = set(await self._persist_to_inbox(records))
        except Exception as e:
            logger.error(
                "Source %s persistence failed: %s",
                self.source.source_name, e, exc_info=True,
            )
            return IngestResult(
                records_ingested=0,
                records_dropped=dropped_count,
                error=str(e),
            )
        # Storage skips an unchanged record and keeps the last copy of a repeated id;
        # condense and count only what it stored, once per id.
        records = list({r.id: r for r in records if r.id in stored_ids}.values())

        # 3. LLM-based condensation for still-long records (configurable per source)
        if self.condense:
            pre_condense = {r.id: r.content for r in records}
            records = await self._condense_long_content(records)
            # Track which records were actually condensed (for debug view)
            processed_map = {
                r.id: r.content for r in records
                if r.content != pre_condense.get(r.id)
            }
            if processed_map:
                await self._update_processed_content(processed_map)

        # 3b. Hand the dropped records to the drop hook. After persist, so a
        # failed persist (which returns above without advancing the cursor)
        # re-fetches the batch and the hook sees its drops then. Before the
        # cursor advances, so a crash in between runs the hook again rather
        # than never: at-least-once, which is why a hook must be idempotent.
        # A record the hook already received is not passed again, whether or
        # not that attempt succeeded: no retry.
        if dropped and self.drop_hook:
            unseen = self._unseen_drops(dropped)
            if len(unseen) < len(dropped):
                logger.debug(
                    "Source %s: %d dropped record(s) already passed to the drop hook",
                    self.source.source_name, len(dropped) - len(unseen),
                )
            if unseen:
                try:
                    await self._run_drop_hook(unseen)
                except Exception:  # noqa: BLE001 — a hook must never hold back the cursor
                    logger.warning(
                        "Source %s: drop hook failed", self.source.source_name, exc_info=True,
                    )
                self._remember_drops(unseen)

        # 4. Advance source cursor (ingestion complete)
        if result.next_cursor is not None:
            await self.db.set_sync_cursor(self.source.source_name, result.next_cursor)
            logger.info(
                "Source %s: %d records ingested, cursor -> %s",
                self.source.source_name, len(records), cursor_text(result.next_cursor),
            )

        return _BatchResult(
            records_ingested=len(records),
            records_dropped=dropped_count,
            next_cursor=result.next_cursor,
            has_more=result.has_more,
        )

    # ------------------------------------------------------------------
    # Drop hook
    # ------------------------------------------------------------------

    def _valid_drop_hook(self, drop_hook: object) -> list[str]:
        """The hook argv, or ``[]`` (off) when the configured value is unusable.

        Config coercion turns list entries into strings, but leaves a value of
        the wrong shape (a mapping, say) as it is.
        """
        if not drop_hook:
            return []
        # Types only in these messages: the value can carry arguments such as
        # tokens, and only argv[0] is ever logged.
        if not isinstance(drop_hook, (list, tuple)):
            logger.warning(
                "Source %s: drop hook must be a list of strings, got a %s — hook disabled",
                self.source.source_name, type(drop_hook).__name__,
            )
            return []
        bad = sorted({type(arg).__name__ for arg in drop_hook if not isinstance(arg, str)})
        if bad:
            logger.warning(
                "Source %s: drop hook entries must be strings, got %s — hook disabled",
                self.source.source_name, ", ".join(bad),
            )
            return []
        if not drop_hook[0].strip():
            logger.warning(
                "Source %s: drop hook has an empty program name — hook disabled",
                self.source.source_name,
            )
            return []
        return list(drop_hook)

    def _unseen_drops(self, dropped: list[SourceRecord]) -> list[SourceRecord]:
        """The dropped records the hook has not received, once per (id, timestamp).

        For GitHub, ``id`` is the thread and ``timestamp`` its ``updated_at``, so
        new activity on a thread is a new pair and reaches the hook again.
        """
        unseen: dict[tuple[str, str], SourceRecord] = {}
        for record in dropped:
            key = (record.id, record.timestamp)
            if key not in self._drop_hook_seen:
                unseen[key] = record
        return list(unseen.values())

    def _remember_drops(self, delivered: list[SourceRecord]) -> None:
        """Record what the hook received. Bounded: the oldest pairs go first."""
        for record in delivered:
            self._drop_hook_seen[(record.id, record.timestamp)] = None
        while len(self._drop_hook_seen) > _DROP_HOOK_SEEN_MAX:
            self._drop_hook_seen.popitem(last=False)

    @staticmethod
    def _drop_hook_payload(
        dropped: list[SourceRecord], inbox_filter: InboxFilter | None,
    ) -> bytes:
        """One JSON line per dropped record — identifiers and metadata, no content."""
        lines = []
        for record in dropped:
            rule = inbox_filter.rejects(record) if inbox_filter else None
            lines.append(json.dumps({
                "source": record.source,
                "id": record.id,
                "record_type": record.record_type,
                "summary": record.summary,
                "timestamp": record.timestamp,
                "metadata": record.metadata or {},
                "dropped_on": rule.field if rule else None,
            }, default=str))
        return ("\n".join(lines) + "\n").encode()

    async def _run_drop_hook(self, dropped: list[SourceRecord]) -> None:
        """Pipe the records the guardrail dropped to the ``drop_hook`` command.

        Dropped records are never persisted, so this is the only point where
        anything can act on a drop — for example, acknowledge the person whose
        message the agent will never see.

        stdin carries one JSON object per line: ``source``, ``id``,
        ``record_type``, ``summary``, ``timestamp``, ``metadata``, and
        ``dropped_on`` (the field of the rule that fired). The content is left
        out on purpose. The guardrail exists to keep it away from automation,
        and identifiers plus metadata are enough to act on a drop. The other
        fields are untrusted too (a GitHub summary carries the issue title).

        The command runs with the daemon's environment and privileges, in its
        own process group. stdout is discarded; stderr goes to a temporary
        file, so a background job the hook leaves behind cannot hold a pipe
        open and stall the sync. Past ``_DROP_HOOK_TIMEOUT_SECS`` the whole
        group is killed. Best effort: a hook that is missing, fails, or times
        out is logged, never raised.
        """
        name = self.source.source_name
        program = self.drop_hook[0]
        payload = self._drop_hook_payload(dropped, self.inbox_filter)

        with tempfile.TemporaryFile() as stderr_file:
            try:
                proc = await asyncio.create_subprocess_exec(
                    *self.drop_hook,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=stderr_file,
                    start_new_session=True,
                )
            except Exception as e:  # noqa: BLE001 — OSError, or ValueError on a NUL in argv
                logger.warning("Source %s: drop hook %r did not start: %s", name, program, e)
                return

            completed = timed_out = False
            try:
                await asyncio.wait_for(
                    proc.communicate(payload), timeout=_DROP_HOOK_TIMEOUT_SECS,
                )
                completed = True
            except asyncio.TimeoutError:
                timed_out = True
            finally:
                # Keyed on communicate() finishing, not on the hook's exit: a
                # hook can exit while a child it started still holds stdin.
                # Also runs on cancellation (daemon shutdown), so no part of
                # the hook is left running unattended.
                if not completed:
                    await self._kill_drop_hook(proc)

            tail = _file_tail(stderr_file, _DROP_HOOK_STDERR_CHARS)

        if timed_out:
            logger.warning(
                "Source %s: drop hook %r did not finish within %ss; its process "
                "group was killed. stderr: %r",
                name, program, _DROP_HOOK_TIMEOUT_SECS, tail,
            )
        elif proc.returncode != 0:
            logger.warning(
                "Source %s: drop hook %r exited %d for %d dropped record(s). stderr: %r",
                name, program, proc.returncode, len(dropped), tail,
            )
        else:
            logger.info(
                "Source %s: drop hook %r exited 0 for %d dropped record(s)",
                name, program, len(dropped),
            )

    async def _kill_drop_hook(self, proc: asyncio.subprocess.Process) -> None:
        """Kill the hook's process group and reap it, waiting a bounded time."""
        # The group id outlives the leader while any member is alive, so this
        # also reaches children of a hook that already exited.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(proc.pid, signal.SIGKILL)
        # wait() also waits for the stdin pipe to close. Abort rather than
        # close: close() waits for buffered data, which a process outside the
        # group (setsid) may never read.
        if proc.stdin is not None:
            with contextlib.suppress(Exception):
                proc.stdin.transport.abort()
        try:
            await asyncio.wait_for(proc.wait(), timeout=_DROP_HOOK_REAP_SECS)
        except asyncio.TimeoutError:
            logger.warning(
                "Source %s: drop hook %r (pid %d) not reaped within %ss after "
                "SIGKILL; a process outside its group may hold its stdin",
                self.source.source_name, self.drop_hook[0], proc.pid, _DROP_HOOK_REAP_SECS,
            )

    # ------------------------------------------------------------------
    # Inbox persistence
    # ------------------------------------------------------------------

    async def _persist_to_inbox(self, records: list[SourceRecord]) -> list[str]:
        """Save records to the source_messages table for inbox display."""
        return await self.db.insert_source_messages(
            records, source=self.source.source_name, ttl_days=self.ttl_days,
        )

    async def _update_processed_content(self, processed_map: dict[str, str]) -> None:
        """Update processed_content on inbox messages after condensation."""
        try:
            await self.db.update_source_messages_processed(
                self.source.source_name, list(processed_map.keys()), processed_map,
            )
        except Exception as e:
            logger.warning("Failed to update processed content: %s", e)

    # ------------------------------------------------------------------
    # Condensation
    # ------------------------------------------------------------------

    async def _get_condense_client(self) -> Any | None:
        """Get or create the async Anthropic client for condensation.

        Uses the config-provided factory which handles provider detection,
        credentials, timeout, and Bedrock/Anthropic switching automatically.
        """
        if self._condense_client is not None:
            return self._condense_client
        if self._client_factory is None:
            return None
        self._condense_client = self._client_factory()
        return self._condense_client

    async def _condense_long_content(
        self, records: list[SourceRecord],
    ) -> list[SourceRecord]:
        """Use a fast LLM to condense records with long content.

        Applies to ALL source types — any record whose content exceeds
        _CONDENSE_THRESHOLD after source.preprocess() gets sent to Haiku
        for extraction of essential information.

        On failure (import error, API error, timeout), the original content
        is preserved — this step never blocks the pipeline.
        """
        model = self.condense_model
        if not model:
            return records

        long_records = [r for r in records if len(r.content) > _CONDENSE_THRESHOLD]
        if not long_records:
            return records

        client = await self._get_condense_client()
        if not client:
            return records

        logger.info(
            "Source %s: condensing %d/%d records with %s",
            self.source.source_name, len(long_records), len(records), model,
        )
        sem = asyncio.Semaphore(5)

        async def condense_one(record: SourceRecord) -> None:
            async with sem:
                original_len = len(record.content)
                try:
                    prompt = self.condense_prompt or _CONDENSE_PROMPT
                    prompt_content = (
                        f"{prompt}\n\n"
                        f"---\n\n"
                        f"{record.content}"
                    )
                    response = await asyncio.wait_for(
                        client.messages.create(
                            model=model,
                            # Thinking counts toward max_tokens, and the 5.5
                            # models think by default.
                            max_tokens=2048,
                            messages=[{
                                "role": "user",
                                "content": prompt_content,
                            }],
                        ),
                        timeout=30,
                    )
                    text = response_text(response)
                    stop_reason = getattr(response, "stop_reason", None)
                    if not text or stop_reason == "max_tokens":
                        raise ValueError(
                            f"no complete text (stop_reason={stop_reason})"
                        )
                    record.content = text
                    logger.debug(
                        "Condensed %s: %d → %d chars",
                        record.id, original_len, len(record.content),
                    )
                except Exception as e:
                    logger.warning(
                        "LLM condense failed for record %s (%d chars), keeping original: %s",
                        record.id, original_len, e,
                    )

        await asyncio.gather(*[condense_one(r) for r in long_records])
        return records
