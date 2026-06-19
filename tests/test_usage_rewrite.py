"""Tests for nerve.observability.usage_rewrite.

Covers the pure attribute-rewrite logic, the delegating span exporter
(including pass-through of non-LangSmith spans), and installation onto a
real OTEL TracerProvider + LangfuseSpanProcessor pipeline.

The OTEL SDK is a hard dependency of the langfuse extra; tests that need
it are skipped when it isn't installed so the suite still passes on
minimal environments.
"""

from __future__ import annotations

import json

import pytest

from nerve.observability.usage_rewrite import (
    GEN_AI_CACHE_CREATION_TOKENS,
    GEN_AI_CACHE_READ_TOKENS,
    GEN_AI_INPUT_TOKENS,
    GEN_AI_OUTPUT_TOKENS,
    GEN_AI_TOTAL_TOKENS,
    USAGE_METADATA_ATTR,
    UsageRewritingSpanExporter,
    install_usage_rewriter,
    rewrite_usage_attributes,
)

otel_sdk = pytest.importorskip("opentelemetry.sdk.trace", reason="OTEL SDK not installed")


def _usage_attr(
    input_tokens: int,
    output_tokens: int,
    cache_read: int = 0,
    ephemeral_5m: int = 0,
    ephemeral_1h: int = 0,
) -> str:
    """Build a LangSmith-canonical usage_metadata JSON string.

    Mirrors langsmith's ``extract_usage_metadata``: ``input_tokens``
    already includes the cache tokens.
    """
    details = {}
    if cache_read:
        details["cache_read"] = cache_read
    if ephemeral_5m:
        details["ephemeral_5m_input_tokens"] = ephemeral_5m
    if ephemeral_1h:
        details["ephemeral_1hr_input_tokens"] = ephemeral_1h
    meta = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
    }
    if details:
        meta["input_token_details"] = details
    return json.dumps(meta)


# ---------------------------------------------------------------------------
# rewrite_usage_attributes — pure logic
# ---------------------------------------------------------------------------


class TestRewriteUsageAttributes:
    def test_cache_heavy_turn_is_split(self):
        # Shape observed on a real agent span: 237,073 "input" tokens of
        # which 236,384 were cache reads and 687 cache writes.
        attrs = {
            USAGE_METADATA_ATTR: _usage_attr(
                input_tokens=237_073,
                output_tokens=5,
                cache_read=236_384,
                ephemeral_5m=687,
            )
        }
        updates = rewrite_usage_attributes(attrs)
        assert updates == {
            GEN_AI_INPUT_TOKENS: 2,
            GEN_AI_OUTPUT_TOKENS: 5,
            GEN_AI_TOTAL_TOKENS: 237_078,
            GEN_AI_CACHE_READ_TOKENS: 236_384,
            GEN_AI_CACHE_CREATION_TOKENS: 687,
        }

    def test_uncached_call_keeps_input(self):
        attrs = {USAGE_METADATA_ATTR: _usage_attr(811, 146)}
        updates = rewrite_usage_attributes(attrs)
        assert updates == {
            GEN_AI_INPUT_TOKENS: 811,
            GEN_AI_OUTPUT_TOKENS: 146,
            GEN_AI_TOTAL_TOKENS: 957,
        }

    def test_one_hour_cache_writes_counted(self):
        attrs = {
            USAGE_METADATA_ATTR: _usage_attr(
                input_tokens=1_000, output_tokens=10,
                cache_read=500, ephemeral_5m=100, ephemeral_1h=300,
            )
        }
        updates = rewrite_usage_attributes(attrs)
        assert updates[GEN_AI_INPUT_TOKENS] == 100
        assert updates[GEN_AI_CACHE_READ_TOKENS] == 500
        assert updates[GEN_AI_CACHE_CREATION_TOKENS] == 400

    def test_inconsistent_details_clamped_to_zero(self):
        # Cache details larger than input_tokens must not go negative.
        attrs = {
            USAGE_METADATA_ATTR: json.dumps(
                {
                    "input_tokens": 100,
                    "output_tokens": 1,
                    "input_token_details": {"cache_read": 500},
                }
            )
        }
        updates = rewrite_usage_attributes(attrs)
        assert updates[GEN_AI_INPUT_TOKENS] == 0
        assert updates[GEN_AI_CACHE_READ_TOKENS] == 500

    def test_missing_attribute_returns_none(self):
        assert rewrite_usage_attributes({}) is None
        assert rewrite_usage_attributes({"other": "x"}) is None

    def test_malformed_json_returns_none(self):
        assert rewrite_usage_attributes({USAGE_METADATA_ATTR: "{not json"}) is None
        assert rewrite_usage_attributes({USAGE_METADATA_ATTR: "[1, 2]"}) is None

    def test_non_string_attribute_returns_none(self):
        assert rewrite_usage_attributes({USAGE_METADATA_ATTR: 42}) is None

    def test_garbage_token_values_coerced(self):
        attrs = {
            USAGE_METADATA_ATTR: json.dumps(
                {"input_tokens": "abc", "output_tokens": None}
            )
        }
        updates = rewrite_usage_attributes(attrs)
        assert updates == {
            GEN_AI_INPUT_TOKENS: 0,
            GEN_AI_OUTPUT_TOKENS: 0,
            GEN_AI_TOTAL_TOKENS: 0,
        }


# ---------------------------------------------------------------------------
# UsageRewritingSpanExporter — delegation + span rebuild
# ---------------------------------------------------------------------------


class FakeExporter:
    def __init__(self):
        self.batches = []
        self.shutdown_called = False
        self.flushed = False

    def export(self, spans):
        self.batches.append(list(spans))
        return "exported"

    def shutdown(self):
        self.shutdown_called = True

    def force_flush(self, timeout_millis=30_000):
        self.flushed = True
        return True


def _make_spans(tracer_attrs_pairs):
    """End real SDK spans with given attributes and return ReadableSpans."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    collected = []

    class Collector:
        def export(self, spans):
            collected.extend(spans)
            return None

        def shutdown(self):
            return None

        def force_flush(self, timeout_millis=30_000):
            return True

    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(Collector()))
    tracer = provider.get_tracer("langsmith")
    for name, attrs in tracer_attrs_pairs:
        with tracer.start_as_current_span(name) as span:
            for key, value in attrs.items():
                span.set_attribute(key, value)
    provider.shutdown()
    return collected


class TestUsageRewritingSpanExporter:
    def test_rewrites_langsmith_span_and_keeps_others(self):
        usage_json = _usage_attr(
            input_tokens=237_073, output_tokens=5,
            cache_read=236_384, ephemeral_5m=687,
        )
        spans = _make_spans(
            [
                (
                    "claude.assistant.turn",
                    {
                        USAGE_METADATA_ATTR: usage_json,
                        GEN_AI_INPUT_TOKENS: 956_331,  # inflated
                        GEN_AI_OUTPUT_TOKENS: 572,
                        "langsmith.span.kind": "llm",
                    },
                ),
                (
                    "anthropic.chat",
                    {GEN_AI_INPUT_TOKENS: 811, GEN_AI_OUTPUT_TOKENS: 146},
                ),
            ]
        )
        delegate = FakeExporter()
        exporter = UsageRewritingSpanExporter(delegate)

        result = exporter.export(spans)
        assert result == "exported"
        assert len(delegate.batches) == 1
        exported = {s.name: s for s in delegate.batches[0]}

        turn = exported["claude.assistant.turn"]
        assert turn.attributes[GEN_AI_INPUT_TOKENS] == 2
        assert turn.attributes[GEN_AI_OUTPUT_TOKENS] == 5
        assert turn.attributes[GEN_AI_CACHE_READ_TOKENS] == 236_384
        assert turn.attributes[GEN_AI_CACHE_CREATION_TOKENS] == 687
        # Original metadata preserved for debugging.
        assert turn.attributes[USAGE_METADATA_ATTR] == usage_json
        assert turn.attributes["langsmith.span.kind"] == "llm"

        # Non-LangSmith span passes through as the same object, untouched.
        chat = exported["anthropic.chat"]
        assert chat is spans[1]
        assert chat.attributes[GEN_AI_INPUT_TOKENS] == 811

    def test_clone_preserves_span_identity_fields(self):
        spans = _make_spans(
            [
                (
                    "claude.assistant.turn",
                    {USAGE_METADATA_ATTR: _usage_attr(100, 10, cache_read=90)},
                )
            ]
        )
        original = spans[0]
        delegate = FakeExporter()
        UsageRewritingSpanExporter(delegate).export(spans)
        clone = delegate.batches[0][0]

        assert clone is not original
        assert clone.context.span_id == original.context.span_id
        assert clone.context.trace_id == original.context.trace_id
        assert clone.start_time == original.start_time
        assert clone.end_time == original.end_time
        assert clone.instrumentation_scope == original.instrumentation_scope
        assert clone.resource is original.resource

    def test_broken_span_exported_unchanged(self):
        class BrokenSpan:
            name = "broken"

            @property
            def attributes(self):
                raise RuntimeError("boom")

        delegate = FakeExporter()
        exporter = UsageRewritingSpanExporter(delegate)
        broken = BrokenSpan()
        exporter.export([broken])
        assert delegate.batches == [[broken]]

    def test_shutdown_and_flush_delegate(self):
        delegate = FakeExporter()
        exporter = UsageRewritingSpanExporter(delegate)
        exporter.shutdown()
        assert delegate.shutdown_called
        assert exporter.force_flush() is True
        assert delegate.flushed


# ---------------------------------------------------------------------------
# install_usage_rewriter — wiring onto a live provider
# ---------------------------------------------------------------------------


class TestInstallUsageRewriter:
    def test_installs_on_langfuse_processor(self):
        langfuse_sp = pytest.importorskip(
            "langfuse._client.span_processor",
            reason="langfuse package not installed",
        )
        from types import SimpleNamespace

        from opentelemetry.sdk.trace import TracerProvider

        delegate = FakeExporter()
        processor = langfuse_sp.LangfuseSpanProcessor(
            public_key="pk-test",
            secret_key="sk-test",
            base_url="http://localhost:9",
            span_exporter=delegate,
        )
        provider = TracerProvider()
        provider.add_span_processor(processor)
        client = SimpleNamespace(
            _resources=SimpleNamespace(tracer_provider=provider)
        )

        try:
            assert install_usage_rewriter(client) is True
            # Idempotent on second call.
            assert install_usage_rewriter(client) is True

            # End a span through the real pipeline and verify the
            # delegate receives rewritten attributes.
            tracer = provider.get_tracer("langsmith")
            with tracer.start_as_current_span("claude.assistant.turn") as span:
                span.set_attribute(
                    USAGE_METADATA_ATTR,
                    _usage_attr(1_000, 7, cache_read=900, ephemeral_5m=50),
                )
                span.set_attribute(GEN_AI_INPUT_TOKENS, 1_000)
            processor.force_flush()

            assert delegate.batches, "span never reached the delegate"
            exported = delegate.batches[0][0]
            assert exported.attributes[GEN_AI_INPUT_TOKENS] == 50
            assert exported.attributes[GEN_AI_CACHE_READ_TOKENS] == 900
            assert exported.attributes[GEN_AI_CACHE_CREATION_TOKENS] == 50
        finally:
            provider.shutdown()

    def test_returns_false_without_resources(self):
        from types import SimpleNamespace

        pytest.importorskip(
            "langfuse._client.span_processor",
            reason="langfuse package not installed",
        )
        assert install_usage_rewriter(SimpleNamespace()) is False
        assert (
            install_usage_rewriter(
                SimpleNamespace(_resources=SimpleNamespace(tracer_provider=None))
            )
            is False
        )
