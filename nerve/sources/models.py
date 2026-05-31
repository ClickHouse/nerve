"""Core data structures for the sources layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SourceRecord:
    """A single record fetched from a source."""

    id: str                         # Opaque, source-defined (message_id, notification_id, etc.)
    source: str                     # "telegram", "gmail", "github"
    record_type: str                # "telegram_message", "gmail_message", "github_notification"
    summary: str                    # Single-line human-readable description
    content: str                    # Full text content for agent consumption
    timestamp: str                  # ISO 8601
    metadata: dict[str, Any] = field(default_factory=dict)
    raw_content: str | None = None  # Original unprocessed content (e.g., HTML email body)


@dataclass
class FetchResult:
    """Result of a source fetch operation."""

    records: list[SourceRecord]
    next_cursor: str | None         # Opaque — passed back verbatim on next call
    has_more: bool = False


@dataclass
class IngestResult:
    """Result of source ingestion (fetch + persist, no processing)."""

    records_ingested: int
    records_dropped: int = 0        # Dropped by the inbox guardrail before persist
    error: str | None = None


@dataclass
class ProcessResult:
    """Result of processing a batch of records (legacy, kept for compat)."""

    records_processed: int
    records_skipped: int
    actions_taken: list[str] = field(default_factory=list)
    error: str | None = None
    session_id: str | None = None       # Cron session that processed this batch (for linking)
