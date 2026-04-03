"""SourceRunner — pure ingestion pipeline.

Reads cursor from DB, calls source.fetch(), persists records to the
source_messages inbox, optionally condenses long content via LLM,
and advances the cursor. No processing (agent/memorize/notify) —
consumption is handled separately by consumer tools.

Pipeline: fetch → source.preprocess → persist → LLM condense → advance cursor
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from nerve.sources.models import IngestResult, SourceRecord

if TYPE_CHECKING:
    from nerve.db import Database
    from nerve.sources.base import Source

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


class SourceRunner:
    """Fetches records from a source and persists them to the inbox.

    Pure ingestion — no agent processing. Consumption happens via
    consumer tools (poll_source, read_source).

    Args:
        source: The data source to fetch from.
        db: Database for cursor persistence and logging.
        batch_size: Max records per fetch.
        job_id: Cron job ID for logging (default: "source:<name>").
        condense: Enable LLM condensation for long records.
        condense_config: Dict with 'api_key' and 'model' for Haiku condensation.
        ttl_days: TTL for persisted source messages.
    """

    def __init__(
        self,
        source: Source,
        db: Database,
        batch_size: int = 50,
        job_id: str = "",
        condense: bool = False,
        condense_config: dict[str, str] | None = None,
        ttl_days: int = 7,
    ):
        self.source = source
        self.db = db
        self.batch_size = batch_size
        self.job_id = job_id or f"source:{source.source_name}"
        self.condense = condense
        self.condense_config = condense_config or {}
        self.ttl_days = ttl_days
        self._lock = asyncio.Lock()
        self._condense_client: Any | None = None  # Lazy AsyncAnthropic
        self.health = SourceHealth()
        self._notification_service: Any | None = None

    def set_notification_service(self, service: Any) -> None:
        """Set the notification service for degradation alerts."""
        self._notification_service = service

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
        """Full ingestion cycle: fetch → preprocess → persist → condense → advance cursor.

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
        """Actual run logic, called under lock."""
        cursor = await self.db.get_sync_cursor(self.source.source_name)
        logger.info(
            "Source %s: fetching (cursor=%s, batch_size=%d)",
            self.source.source_name, cursor, self.batch_size,
        )

        try:
            result = await self.source.fetch(cursor, limit=self.batch_size)
        except Exception as e:
            logger.error("Source %s fetch failed: %s", self.source.source_name, e, exc_info=True)
            return IngestResult(records_ingested=0, error=str(e))

        if not result.records:
            # Even with 0 records, advance cursor if it changed
            # (e.g., Telegram baseline establishing a cursor with no records)
            if result.next_cursor and result.next_cursor != cursor:
                await self.db.set_sync_cursor(self.source.source_name, result.next_cursor)
                logger.info(
                    "Source %s: no records, cursor advanced to %s",
                    self.source.source_name, result.next_cursor,
                )
            else:
                logger.info("Source %s: no new records", self.source.source_name)
            return IngestResult(records_ingested=0)

        # Ingestion pipeline:
        # 1. Source-specific cleanup (e.g., Gmail boilerplate stripping)
        records = await self.source.preprocess(result.records)

        # 2. Persist to inbox (post-preprocess, pre-condense — human-readable)
        await self._persist_to_inbox(records)

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

        # 4. Advance source cursor (ingestion complete)
        if result.next_cursor is not None:
            await self.db.set_sync_cursor(self.source.source_name, result.next_cursor)
            logger.info(
                "Source %s: %d records ingested, cursor -> %s",
                self.source.source_name, len(records), result.next_cursor,
            )

        return IngestResult(records_ingested=len(records))

    # ------------------------------------------------------------------
    # Inbox persistence
    # ------------------------------------------------------------------

    async def _persist_to_inbox(self, records: list[SourceRecord]) -> None:
        """Save records to the source_messages table for inbox display."""
        try:
            await self.db.insert_source_messages(
                records, source=self.source.source_name, ttl_days=self.ttl_days,
            )
        except Exception as e:
            logger.warning(
                "Failed to persist %d records to inbox: %s", len(records), e,
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
        """Get or create the shared AsyncAnthropic client for condensation.

        Returns None when proxy mode is active (uses httpx directly).
        """
        if self.condense_config.get("use_proxy"):
            return None  # Proxy uses OpenAI-compatible httpx calls
        if self._condense_client is not None:
            return self._condense_client
        api_key = self.condense_config.get("api_key")
        if not api_key:
            return None
        try:
            import anthropic
        except ImportError:
            logger.warning("anthropic package not available for content condensation")
            return None
        base_url = self.condense_config.get("base_url")
        kwargs: dict[str, Any] = {"api_key": api_key}
        if base_url:
            # Strip /v1/ suffix — Anthropic SDK prepends it internally,
            # so including it here causes /v1/v1/messages (404).
            url = base_url.rstrip("/")
            if url.endswith("/v1"):
                url = url[:-3]
            kwargs["base_url"] = url
        self._condense_client = anthropic.AsyncAnthropic(**kwargs)
        return self._condense_client

    async def _condense_via_proxy(
        self, content: str, model: str,
    ) -> str:
        """Condense content via the OpenAI-compatible proxy endpoint."""
        import httpx

        base_url = self.condense_config.get("base_url", "")
        if not base_url.endswith("/"):
            base_url += "/"
        api_key = self.condense_config.get("api_key", "")

        async with httpx.AsyncClient() as http_client:
            resp = await http_client.post(
                f"{base_url}chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "max_tokens": 1024,
                    "messages": [{"role": "user", "content": content}],
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"]

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
        model = self.condense_config.get("model")
        if not model:
            return records

        long_records = [r for r in records if len(r.content) > _CONDENSE_THRESHOLD]
        if not long_records:
            return records

        use_proxy = self.condense_config.get("use_proxy", False)

        if not use_proxy:
            client = await self._get_condense_client()
            if not client:
                return records
        else:
            client = None
            if not self.condense_config.get("api_key"):
                return records

        logger.info(
            "Source %s: condensing %d/%d records with %s%s",
            self.source.source_name, len(long_records), len(records), model,
            " (via proxy)" if use_proxy else "",
        )
        sem = asyncio.Semaphore(5)

        async def condense_one(record: SourceRecord) -> None:
            async with sem:
                original_len = len(record.content)
                try:
                    condense_prompt = self.condense_config.get("prompt") or _CONDENSE_PROMPT
                    prompt_content = (
                        f"{condense_prompt}\n\n"
                        f"---\n\n"
                        f"{record.content}"
                    )
                    if use_proxy:
                        condensed = await self._condense_via_proxy(
                            prompt_content, model,
                        )
                    else:
                        response = await asyncio.wait_for(
                            client.messages.create(
                                model=model,
                                max_tokens=1024,
                                messages=[{
                                    "role": "user",
                                    "content": prompt_content,
                                }],
                            ),
                            timeout=30,
                        )
                        condensed = response.content[0].text
                    record.content = condensed
                    logger.debug(
                        "Condensed %s: %d → %d chars",
                        record.id, original_len, len(condensed),
                    )
                except Exception as e:
                    logger.warning(
                        "LLM condense failed for record %s (%d chars), keeping original: %s",
                        record.id, original_len, e,
                    )

        await asyncio.gather(*[condense_one(r) for r in long_records])
        return records
