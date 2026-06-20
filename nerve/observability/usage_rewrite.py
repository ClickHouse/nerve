"""Cache-aware usage rewriting for LangSmith agent spans exported to Langfuse.

Why this exists
---------------
The LangSmith ``claude-agent-sdk`` integration reports token usage in
LangSmith's canonical format: ``input_tokens`` is the **sum** of uncached
input plus prompt-cache reads and writes, with the breakdown kept in
``input_token_details``. LangSmith's own backend understands that format
and applies cache discounts from the details.

Langfuse does not. Its OTEL ingestion reads the flat
``gen_ai.usage.input_tokens`` attribute, ignores
``input_token_details`` (which the LangSmith exporter also stringifies
as a Python repr rather than JSON), and prices every input token at the
model's uncached input rate. Agent sessions are typically >95%
cache-read tokens billed at 10% of the input price, so Langfuse
overstates agent costs by roughly 5-10x.

What this module does
---------------------
A delegating :class:`opentelemetry.sdk.trace.export.SpanExporter` that
sits in front of Langfuse's OTLP exporter. For every span that carries a
``langsmith.metadata.usage_metadata`` attribute (the accurate,
transcript-reconciled per-message usage), it rewrites the
``gen_ai.usage.*`` attributes into the same shape that
``opentelemetry-instrumentation-anthropic`` emits — uncached input plus
explicit ``gen_ai.usage.cache_read.input_tokens`` /
``gen_ai.usage.cache_creation.input_tokens`` — which Langfuse maps to
its ``input_cached_tokens`` / ``input_cache_creation`` usage keys and
prices with the cache rates from its managed model definitions.

Known approximation: 1-hour cache writes are folded into the single
``cache_creation`` attribute and priced at the 5-minute rate (Langfuse's
OTEL mapping has no separate 1h key). The 1h share of agent traffic is
small; the error is a few percent of the cache-write component.

Failure mode
------------
Best-effort, like the rest of the observability package. Any error while
rewriting a span exports the original span unchanged; any error while
installing leaves the exporter unwrapped. Tracing must never take down
or distort the host beyond what it already does.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Mapping, Optional, Sequence

logger = logging.getLogger(__name__)

# LangSmith attribute carrying the canonical usage metadata JSON. This is
# set by the claude-agent-sdk integration from live AssistantMessages and
# later overwritten with accurate counts reconciled from the JSONL
# transcripts.
USAGE_METADATA_ATTR = "langsmith.metadata.usage_metadata"

# OTEL GenAI attributes Langfuse maps into usage_details. The cache keys
# follow the opentelemetry-instrumentation-anthropic (traceloop)
# convention, which Langfuse maps to ``input_cached_tokens`` and
# ``input_cache_creation`` — both priced in Langfuse-managed Anthropic
# model definitions.
GEN_AI_INPUT_TOKENS = "gen_ai.usage.input_tokens"
GEN_AI_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"
GEN_AI_TOTAL_TOKENS = "gen_ai.usage.total_tokens"
GEN_AI_CACHE_READ_TOKENS = "gen_ai.usage.cache_read.input_tokens"
GEN_AI_CACHE_CREATION_TOKENS = "gen_ai.usage.cache_creation.input_tokens"


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def rewrite_usage_attributes(
    attributes: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    """Compute corrected ``gen_ai.usage.*`` attributes for a span.

    Returns a dict of attribute updates, or ``None`` when the span has no
    parseable LangSmith usage metadata (in which case it must be exported
    unchanged).
    """
    raw = attributes.get(USAGE_METADATA_ATTR)
    if not raw or not isinstance(raw, str):
        return None

    try:
        meta = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(meta, dict):
        return None

    # LangSmith canonical: input_tokens already includes cache tokens.
    input_total = _to_int(meta.get("input_tokens"))
    output_tokens = _to_int(meta.get("output_tokens"))

    details = meta.get("input_token_details")
    if not isinstance(details, dict):
        details = {}

    cache_read = _to_int(details.get("cache_read"))
    cache_creation = _to_int(
        details.get("ephemeral_5m_input_tokens")
    ) + _to_int(
        # The integration writes "1hr"; tolerate "1h" too.
        details.get("ephemeral_1hr_input_tokens")
        or details.get("ephemeral_1h_input_tokens")
    )

    uncached_input = max(input_total - cache_read - cache_creation, 0)

    updates: dict[str, Any] = {
        GEN_AI_INPUT_TOKENS: uncached_input,
        GEN_AI_OUTPUT_TOKENS: output_tokens,
        GEN_AI_TOTAL_TOKENS: input_total + output_tokens,
    }
    if cache_read:
        updates[GEN_AI_CACHE_READ_TOKENS] = cache_read
    if cache_creation:
        updates[GEN_AI_CACHE_CREATION_TOKENS] = cache_creation
    return updates


class UsageRewritingSpanExporter:
    """SpanExporter decorator that fixes LangSmith usage before export.

    Implements the ``SpanExporter`` interface by delegation so it can wrap
    whatever exporter Langfuse configured (OTLP by default, anything in
    tests). Spans without LangSmith usage metadata pass through untouched
    — including ``opentelemetry-instrumentation-anthropic`` generations,
    which already report cache usage correctly.
    """

    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate

    def export(self, spans: Sequence[Any]) -> Any:
        try:
            spans = [self._rewrite(span) for span in spans]
        except Exception:
            # Never lose a batch to rewriting problems.
            logger.debug("Usage rewrite failed for batch", exc_info=True)
        return self._delegate.export(spans)

    def shutdown(self) -> None:
        return self._delegate.shutdown()

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        force_flush = getattr(self._delegate, "force_flush", None)
        if force_flush is None:
            return True
        return force_flush(timeout_millis)

    # -- internals ----------------------------------------------------

    def _rewrite(self, span: Any) -> Any:
        try:
            attributes = span.attributes or {}
            updates = rewrite_usage_attributes(attributes)
            if not updates:
                return span
            merged = dict(attributes)
            merged.update(updates)
            return self._clone_with_attributes(span, merged)
        except Exception:
            logger.debug(
                "Usage rewrite failed for span %r — exporting unchanged",
                getattr(span, "name", "?"),
                exc_info=True,
            )
            return span

    @staticmethod
    def _clone_with_attributes(span: Any, attributes: dict[str, Any]) -> Any:
        """Build a copy of a ReadableSpan with replaced attributes.

        Ended spans are immutable, so the only clean way to adjust
        attributes at export time is to construct a new ReadableSpan that
        shares every other field.
        """
        from opentelemetry.sdk.trace import ReadableSpan

        context = getattr(span, "context", None) or span.get_span_context()
        return ReadableSpan(
            name=span.name,
            context=context,
            parent=span.parent,
            resource=span.resource,
            attributes=attributes,
            events=span.events,
            links=span.links,
            kind=span.kind,
            instrumentation_scope=span.instrumentation_scope,
            status=span.status,
            start_time=span.start_time,
            end_time=span.end_time,
        )


def install_usage_rewriter(client: Any) -> bool:
    """Wrap the Langfuse span processors' exporters on a live client.

    Walks ``client._resources.tracer_provider`` for
    ``LangfuseSpanProcessor`` instances and wraps each underlying span
    exporter in :class:`UsageRewritingSpanExporter`. Idempotent — already
    wrapped exporters are left alone.

    Returns True when at least one exporter is wrapped (or was already
    wrapped), False when the structure doesn't match (SDK layout change,
    tracing disabled, etc.).
    """
    try:
        from langfuse._client.span_processor import LangfuseSpanProcessor
    except ImportError:
        return False

    resources = getattr(client, "_resources", None)
    provider = getattr(resources, "tracer_provider", None)
    if provider is None:
        return False

    multi = getattr(provider, "_active_span_processor", None)
    processors = getattr(multi, "_span_processors", None) or ()

    installed = False
    for processor in processors:
        if not isinstance(processor, LangfuseSpanProcessor):
            continue
        if _wrap_processor_exporter(processor):
            installed = True
    return installed


def _wrap_processor_exporter(processor: Any) -> bool:
    """Replace a BatchSpanProcessor's exporter with the rewriting wrapper.

    Handles both OTEL SDK layouts:
    - >= 1.39: ``processor._batch_processor._exporter``
    - older:   ``processor.span_exporter``
    """
    holders = []
    batch_processor = getattr(processor, "_batch_processor", None)
    if batch_processor is not None and hasattr(batch_processor, "_exporter"):
        holders.append((batch_processor, "_exporter"))
    if hasattr(processor, "span_exporter"):
        holders.append((processor, "span_exporter"))

    for holder, attr in holders:
        current = getattr(holder, attr, None)
        if current is None:
            continue
        if isinstance(current, UsageRewritingSpanExporter):
            return True  # already installed
        try:
            setattr(holder, attr, UsageRewritingSpanExporter(current))
            return True
        except AttributeError:
            # Read-only property (e.g. a delegating accessor) — try the
            # next holder.
            continue
    return False
