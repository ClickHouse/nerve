"""Tests for nerve.observability.langfuse — covers no-op behavior when
keys are absent, redaction, init wiring with mocked SDK, and flush.

The ``init_langfuse`` tests patch ``sys.modules`` with a fake SDK before
``init_langfuse`` runs, so they never touch the real packages or the
network. The span redaction tests use the real ``langfuse`` types and span
processor (skipped when the package is missing), because the hook contract
belongs to the SDK.
"""

from __future__ import annotations

import base64
import json
import os
import re
import sys
from queue import Queue
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from nerve.observability import langfuse as lf


@pytest.fixture(autouse=True)
def _reset_lf_state(monkeypatch):
    """Reset module state and clear LANGFUSE_* env vars between tests."""
    lf._enabled = False
    lf._host = ""
    lf._project_id = None
    lf._redact_patterns = []
    lf._last_flush_at = None
    lf._auth_ok = False
    lf._usage_rewriter = False
    lf._client = None
    for var in (
        "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY", "LANGFUSE_HOST",
        "LANGFUSE_BASE_URL",
        "LANGSMITH_OTEL_ENABLED", "LANGSMITH_OTEL_ONLY", "LANGSMITH_TRACING",
    ):
        monkeypatch.delenv(var, raising=False)
    yield
    lf._enabled = False
    lf._redact_patterns = []
    lf._client = None


def _config_with(**kwargs):
    """Build a minimal config-shaped object with a ``langfuse`` attr."""
    from nerve.config import LangfuseConfig
    cfg = SimpleNamespace()
    cfg.langfuse = LangfuseConfig(**kwargs)
    return cfg


# ---------------------------------------------------------------------------
# init_langfuse — disabled paths
# ---------------------------------------------------------------------------


def test_init_noop_without_keys():
    assert lf.init_langfuse(_config_with()) is False
    assert lf.is_enabled() is False
    status = lf.get_status()
    assert status["enabled"] is False
    assert status["auth_ok"] is False
    assert status["host"] is None


def test_init_noop_when_only_public_key():
    assert lf.init_langfuse(_config_with(public_key="pk-lf-x")) is False


def test_init_noop_when_only_secret_key():
    assert lf.init_langfuse(_config_with(secret_key="sk-lf-x")) is False


def test_init_noop_when_config_lacks_langfuse_attr():
    """If the config object has no ``langfuse`` field at all, init bails."""
    bare = SimpleNamespace()
    assert lf.init_langfuse(bare) is False


# ---------------------------------------------------------------------------
# init_langfuse — happy path with mocked SDK
# ---------------------------------------------------------------------------


class _FakeApiError(Exception):
    """Stands in for langfuse.api.core.api_error.ApiError."""

    def __init__(self, status_code: int | None) -> None:
        super().__init__(f"status_code: {status_code}")
        self.status_code = status_code


def _install_fake_langfuse(monkeypatch, *, auth_ok: bool = True):
    """Install a fake langfuse + langsmith + opentelemetry-instrumentation
    into sys.modules so ``init_langfuse`` doesn't touch the real packages.

    ``auth_ok=True`` makes the key check return one project; ``False`` makes
    it raise an HTTP 401 error. Returns the client, the ``Langfuse``
    constructor, and the configure_claude_agent_sdk and AnthropicInstrumentor
    mocks so tests can assert they were called.
    """
    fake_client = MagicMock()
    if auth_ok:
        fake_client.api.projects.get.return_value = SimpleNamespace(
            data=[SimpleNamespace(id="proj-test", name="test project")],
        )
    else:
        fake_client.api.projects.get.side_effect = _FakeApiError(401)
    fake_client.flush = MagicMock()

    fake_lf = MagicMock()
    fake_lf.Langfuse = MagicMock(return_value=fake_client)
    fake_filter = MagicMock()
    fake_filter.is_default_export_span.return_value = True
    fake_install = MagicMock()
    monkeypatch.setattr(lf, "_install_redaction", fake_install)
    # propagate_attributes returns a context manager — use MagicMock's
    # __enter__/__exit__ defaults.
    fake_lf.propagate_attributes = MagicMock(return_value=MagicMock(
        __enter__=MagicMock(return_value=None),
        __exit__=MagicMock(return_value=False),
    ))

    fake_configure = MagicMock()
    fake_langsmith_int = MagicMock()
    fake_langsmith_int.configure_claude_agent_sdk = fake_configure

    fake_instrumentor_inst = MagicMock()
    fake_instrumentor_cls = MagicMock(return_value=fake_instrumentor_inst)
    fake_anthropic_instr = MagicMock()
    fake_anthropic_instr.AnthropicInstrumentor = fake_instrumentor_cls

    monkeypatch.setitem(sys.modules, "langfuse", fake_lf)
    monkeypatch.setitem(sys.modules, "langfuse._client.span_filter", fake_filter)
    monkeypatch.setitem(sys.modules, "langsmith", MagicMock())
    monkeypatch.setitem(sys.modules, "langsmith.integrations", MagicMock())
    monkeypatch.setitem(
        sys.modules, "langsmith.integrations.claude_agent_sdk",
        fake_langsmith_int,
    )
    monkeypatch.setitem(sys.modules, "opentelemetry", MagicMock())
    monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation", MagicMock())
    monkeypatch.setitem(
        sys.modules, "opentelemetry.instrumentation.anthropic",
        fake_anthropic_instr,
    )
    return SimpleNamespace(
        client=fake_client,
        constructor=fake_lf.Langfuse,
        install=fake_install,
        span_filter=fake_filter.is_default_export_span,
        configure=fake_configure,
        instrumentor_inst=fake_instrumentor_inst,
        instrumentor_cls=fake_instrumentor_cls,
    )


def test_init_with_keys_passes_keys_to_client_and_calls_instrumentors(monkeypatch):
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)

    cfg = _config_with(
        public_key="pk-lf-test",
        secret_key="sk-lf-test",
        host="https://cloud.langfuse.com/",
    )
    assert lf.init_langfuse(cfg) is True
    assert lf.is_enabled() is True

    # The keys go to the client constructor.
    fakes.constructor.assert_called_once()
    kwargs = fakes.constructor.call_args.kwargs
    assert kwargs["public_key"] == "pk-lf-test"
    assert kwargs["secret_key"] == "sk-lf-test"
    assert kwargs["base_url"] == "https://cloud.langfuse.com"
    fakes.install.assert_called_once_with(fakes.client)
    assert kwargs["should_export_span"](object()) is True
    fakes.span_filter.return_value = False
    assert kwargs["should_export_span"](object()) is False

    # LangSmith flags are set; they hold no credentials.
    assert os.environ["LANGSMITH_OTEL_ENABLED"] == "true"
    assert os.environ["LANGSMITH_OTEL_ONLY"] == "true"
    assert os.environ["LANGSMITH_TRACING"] == "true"

    # Key check ran once and its project id reaches the status block.
    fakes.client.api.projects.get.assert_called_once()
    status = lf.get_status()
    assert status["project_id"] == "proj-test"
    assert status["auth_ok"] is True
    assert status["host"] == "https://cloud.langfuse.com"

    # Instrumentors called
    fakes.configure.assert_called_once()
    fakes.instrumentor_cls.assert_called_once()
    fakes.instrumentor_inst.instrument.assert_called_once()


def test_init_keeps_keys_out_of_the_process_environment(monkeypatch):
    """Child processes (the Claude CLI, shell tools, Codex) copy
    ``os.environ``, so the keys must never be written there."""
    _install_fake_langfuse(monkeypatch, auth_ok=True)
    cfg = _config_with(public_key="pk-lf-test", secret_key="sk-lf-test")
    assert lf.init_langfuse(cfg) is True

    assert "LANGFUSE_PUBLIC_KEY" not in os.environ
    assert "LANGFUSE_SECRET_KEY" not in os.environ
    assert "LANGFUSE_HOST" not in os.environ
    assert not any("sk-lf-test" in value for value in os.environ.values())


@pytest.mark.parametrize("status_code", [401, 403])
def test_init_disabled_when_host_rejects_keys(monkeypatch, status_code):
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)
    fakes.client.api.projects.get.side_effect = _FakeApiError(status_code)
    cfg = _config_with(public_key="pk-lf-bad", secret_key="sk-lf-bad")

    assert lf.init_langfuse(cfg) is False
    assert lf.is_enabled() is False
    fakes.client.shutdown.assert_called_once()
    fakes.configure.assert_not_called()
    fakes.instrumentor_cls.assert_not_called()


def test_init_disabled_when_auth_check_fails(monkeypatch):
    _install_fake_langfuse(monkeypatch, auth_ok=False)
    cfg = _config_with(public_key="pk-lf-bad", secret_key="sk-lf-bad")
    assert lf.init_langfuse(cfg) is False
    assert lf.is_enabled() is False


def test_init_disabled_when_no_project_matches_keys(monkeypatch):
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)
    fakes.client.api.projects.get.return_value = SimpleNamespace(data=[])
    cfg = _config_with(public_key="pk-lf-x", secret_key="sk-lf-x")

    assert lf.init_langfuse(cfg) is False
    assert lf.is_enabled() is False
    fakes.client.shutdown.assert_called_once()


@pytest.mark.parametrize(
    "error", [ConnectionError("network down"), _FakeApiError(503)],
)
def test_init_stays_enabled_when_key_check_cannot_reach_host(monkeypatch, error):
    """A Langfuse outage at startup must not turn tracing off until the
    next restart; only a rejected key pair does that."""
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)
    fakes.client.api.projects.get.side_effect = error
    cfg = _config_with(public_key="pk-lf-x", secret_key="sk-lf-x")

    assert lf.init_langfuse(cfg) is True
    assert lf.is_enabled() is True
    status = lf.get_status()
    assert status["auth_ok"] is False
    assert status["project_id"] is None
    fakes.client.shutdown.assert_not_called()
    fakes.configure.assert_called_once()
    fakes.instrumentor_inst.instrument.assert_called_once()


def test_init_disabled_when_client_constructor_raises(monkeypatch):
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)
    fakes.constructor.side_effect = ValueError("bad base_url")
    cfg = _config_with(public_key="pk-lf-x", secret_key="sk-lf-x")

    assert lf.init_langfuse(cfg) is False
    assert lf.is_enabled() is False
    fakes.configure.assert_not_called()


def test_init_keeps_export_closed_when_redaction_setup_fails(monkeypatch):
    fakes = _install_fake_langfuse(monkeypatch)

    def fail_install(client):
        assert fakes.constructor.call_args.kwargs["should_export_span"](object()) is False
        raise AttributeError("unsupported exporter layout")

    fakes.install.side_effect = fail_install
    assert lf.init_langfuse(_config_with(public_key="pk-test", secret_key="sk-test")) is False
    assert fakes.constructor.call_args.kwargs["should_export_span"](object()) is False
    fakes.client.shutdown.assert_called_once()
    fakes.configure.assert_not_called()
    fakes.instrumentor_cls.assert_not_called()


def test_init_continues_when_anthropic_instrumentor_fails(monkeypatch):
    """Failing to instrument Anthropic SDK shouldn't disable the rest."""
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)
    fakes.instrumentor_inst.instrument.side_effect = RuntimeError("boom")

    cfg = _config_with(public_key="pk-lf-x", secret_key="sk-lf-x")
    assert lf.init_langfuse(cfg) is True  # still enabled overall
    assert lf.is_enabled() is True


# ---------------------------------------------------------------------------
# attributes()
# ---------------------------------------------------------------------------


def test_attributes_nullcontext_when_disabled():
    """When disabled, attributes() must yield without touching the SDK."""
    with lf.attributes(session_id="s1", tags=["a"]):
        pass  # Just verify no crash and the block runs.


def test_attributes_calls_propagate_when_enabled(monkeypatch):
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)
    cfg = _config_with(public_key="pk-lf", secret_key="sk-lf")
    assert lf.init_langfuse(cfg) is True

    with lf.attributes(
        session_id="s1",
        user_id="u1",
        tags=["source:web", "model:opus-4-8"],
        metadata={"k": "v", "ignored": None},
    ):
        pass

    # Called once with the expected kwargs (None values stripped).
    sys.modules["langfuse"].propagate_attributes.assert_called_once()
    call_kwargs = sys.modules["langfuse"].propagate_attributes.call_args.kwargs
    assert call_kwargs["session_id"] == "s1"
    assert call_kwargs["user_id"] == "u1"
    assert call_kwargs["tags"] == ["source:web", "model:opus-4-8"]
    assert call_kwargs["metadata"] == {"k": "v"}


def test_attributes_propagates_exception_thrown_into_yield(monkeypatch):
    """Regression: an exception raised inside the with-block must propagate
    as itself, not get swallowed and rethrown as RuntimeError from a
    double-yield.

    Pre-fix the ``except Exception`` branch caught the thrown-in exception
    and ``yield``ed a second time, which violated the
    @contextmanager generator protocol and produced
    ``RuntimeError("generator didn't stop after throw()")``. The engine's
    idle-timeout path throws ``asyncio.TimeoutError`` into this context
    manager and relies on the same type coming back out so its retry
    logic catches it.
    """
    import asyncio

    _install_fake_langfuse(monkeypatch, auth_ok=True)
    cfg = _config_with(public_key="pk-lf", secret_key="sk-lf")
    assert lf.init_langfuse(cfg) is True

    with pytest.raises(asyncio.TimeoutError, match="idle"):
        with lf.attributes(session_id="s1", tags=["a"]):
            raise asyncio.TimeoutError("idle")


def test_attributes_propagates_exception_when_setup_fails(monkeypatch):
    """If ``propagate_attributes()`` itself raises during setup,
    ``attributes()`` falls back to a bare yield AND must still propagate
    exceptions raised inside the with-block.
    """
    import asyncio

    _install_fake_langfuse(monkeypatch, auth_ok=True)
    cfg = _config_with(public_key="pk-lf", secret_key="sk-lf")
    assert lf.init_langfuse(cfg) is True
    sys.modules["langfuse"].propagate_attributes = MagicMock(
        side_effect=RuntimeError("bad kwargs"),
    )

    with pytest.raises(asyncio.TimeoutError, match="idle"):
        with lf.attributes(session_id="s1"):
            raise asyncio.TimeoutError("idle")


def test_attributes_propagates_non_timeout_exceptions(monkeypatch):
    """Generalize the regression: ANY exception type raised inside the
    block must propagate unchanged (not just TimeoutError)."""
    _install_fake_langfuse(monkeypatch, auth_ok=True)
    cfg = _config_with(public_key="pk-lf", secret_key="sk-lf")
    assert lf.init_langfuse(cfg) is True

    with pytest.raises(ValueError, match="boom"):
        with lf.attributes(session_id="s1"):
            raise ValueError("boom")


def test_attributes_normal_exit_when_block_succeeds(monkeypatch):
    """Sanity: a clean exit through the block still works after the fix
    (regression coverage doesn't paper over normal use)."""
    _install_fake_langfuse(monkeypatch, auth_ok=True)
    cfg = _config_with(public_key="pk-lf", secret_key="sk-lf")
    assert lf.init_langfuse(cfg) is True

    ran = False
    with lf.attributes(session_id="s1", tags=["normal"]):
        ran = True
    assert ran is True


# ---------------------------------------------------------------------------
# redact()
# ---------------------------------------------------------------------------


def test_redact_noop_when_disabled():
    # Even with patterns set, when not enabled redact returns input unchanged.
    lf._redact_patterns = [re.compile(r"sk-ant-\w+")]
    assert lf.redact("hello sk-ant-AAAA") == "hello sk-ant-AAAA"


def test_redact_strips_anthropic_key_when_enabled():
    lf._enabled = True
    lf._redact_patterns = [re.compile(r"sk-ant-[A-Za-z0-9_\-]+")]
    assert lf.redact("API key: sk-ant-AAAABBBBCC123") == "API key: [REDACTED]"


def test_redact_handles_empty_input():
    lf._enabled = True
    lf._redact_patterns = [re.compile(r"x+")]
    assert lf.redact("") == ""


def test_redact_handles_no_patterns():
    lf._enabled = True
    lf._redact_patterns = []
    assert lf.redact("anything") == "anything"


# ---------------------------------------------------------------------------
# Span redaction on export (mask_otel_spans)
# ---------------------------------------------------------------------------

# Synthetic values in the shape of the default patterns. Not real keys.
_FAKE_ANTHROPIC_KEY = "sk-ant-" + "A1b2C3d4" * 4
_FAKE_LANGFUSE_SECRET = "sk-lf-" + "0f0f0f0f" * 4


def _default_patterns():
    from nerve.config import LangfuseConfig
    return lf._compile_patterns(LangfuseConfig().redact_patterns)


def test_compile_patterns_skips_invalid_regex():
    compiled = lf._compile_patterns(["(", r"ok-\d+"])
    assert [p.pattern for p in compiled] == [r"ok-\d+"]


def test_redact_attributes_returns_only_changed_values():
    patterns = _default_patterns()
    attributes = {
        "gen_ai.prompt.0.content": f"use {_FAKE_ANTHROPIC_KEY} for the call",
        "gen_ai.prompt.1.content": "nothing secret here",
        "gen_ai.usage.input_tokens": 12,
        "langfuse.trace.tags": ("source:web", _FAKE_LANGFUSE_SECRET),
        "flags": (True, False),
    }
    changed = lf.redact_attributes(attributes, patterns)

    assert changed == {
        "gen_ai.prompt.0.content": "use [REDACTED] for the call",
        "langfuse.trace.tags": ("source:web", "[REDACTED]"),
    }


def test_redact_attributes_without_patterns_changes_nothing():
    assert lf.redact_attributes({"a": _FAKE_ANTHROPIC_KEY}, []) == {}


def test_redact_attributes_handles_nested_values_and_bytes():
    value = {
        "nested": [{"token": _FAKE_ANTHROPIC_KEY.encode(), "count": 12}],
        "binary": b"\xff" + _FAKE_ANTHROPIC_KEY.encode(),
        "flags": (True, False),
    }
    changed = lf.redact_attributes({"input": value}, _default_patterns())
    assert changed == {"input": {
        "nested": ({"token": b"[REDACTED]", "count": 12},),
        "binary": b"[REDACTED]",
        "flags": (True, False),
    }}
    assert value["nested"][0]["token"] == _FAKE_ANTHROPIC_KEY.encode()


@pytest.mark.parametrize("as_bytes", [False, True])
def test_redact_attributes_decodes_json_text(as_bytes):
    value = json.dumps({"input": "private-π-token line1\nline2"})
    if as_bytes:
        value = value.encode()
    patterns = lf._compile_patterns(["private-π-token", "line1\nline2"])
    changed = lf.redact_attributes({"input": value}, patterns)
    assert json.loads(changed["input"]) == {"input": "[REDACTED] [REDACTED]"}
    assert isinstance(changed["input"], type(value))


def test_redact_attributes_preserves_unmatched_values():
    attributes = {
        "json": b'{"input": "plain text"}',
        "nested": {"items": ("plain text", 1, None)},
        "invalid_json": '{"input":',
    }
    assert lf.redact_attributes(attributes, _default_patterns()) == {}


def test_mask_hook_patches_only_spans_with_secrets():
    types = pytest.importorskip("langfuse.types", reason="langfuse not installed")
    lf._redact_patterns = _default_patterns()

    def span(span_id: str, attributes: dict):
        return types.OtelSpanData(
            trace_id="0" * 32, span_id=span_id, parent_span_id=None,
            name="claude.assistant.turn", instrumentation_scope_name="langsmith",
            instrumentation_scope_version=None, attributes=attributes,
            resource_attributes={},
        )

    secret_id = types.OtelSpanIdentifier(trace_id="0" * 32, span_id="1" * 16)
    clean_id = types.OtelSpanIdentifier(trace_id="0" * 32, span_id="2" * 16)
    params = types.MaskOtelSpansParams(spans={
        secret_id: span("1" * 16, {"output": f"key={_FAKE_LANGFUSE_SECRET}"}),
        clean_id: span("2" * 16, {"output": "plain text"}),
    })

    result = lf._mask_otel_spans(params=params)

    assert isinstance(result, types.MaskOtelSpansResult)
    assert set(result.span_patches) == {secret_id}
    assert dict(result.span_patches[secret_id].set_attributes) == {
        "output": "key=[REDACTED]",
    }


def test_mask_hook_returns_none_without_patterns_or_matches():
    types = pytest.importorskip("langfuse.types", reason="langfuse not installed")
    identifier = types.OtelSpanIdentifier(trace_id="0" * 32, span_id="1" * 16)
    params = types.MaskOtelSpansParams(spans={
        identifier: types.OtelSpanData(
            trace_id="0" * 32, span_id="1" * 16, parent_span_id=None, name="s",
            instrumentation_scope_name="langsmith",
            instrumentation_scope_version=None,
            attributes={"output": "plain text"}, resource_attributes={},
        ),
    })

    lf._redact_patterns = []
    assert lf._mask_otel_spans(params=params) is None
    lf._redact_patterns = _default_patterns()
    assert lf._mask_otel_spans(params=params) is None


class _RecordingExporter:
    """Minimal SpanExporter that keeps every exported batch."""

    def __init__(self):
        self.batches = []

    def export(self, spans):
        from opentelemetry.sdk.trace.export import SpanExportResult
        self.batches.append(list(spans))
        return SpanExportResult.SUCCESS

    def shutdown(self):
        return None

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        return True


def test_real_pipeline_redacts_spans_and_keeps_usage_rewrite():
    """End a span through a real LangfuseSpanProcessor that has the hook
    and the usage rewriter installed, the way init_langfuse wires them."""
    span_processor = pytest.importorskip(
        "langfuse._client.span_processor", reason="langfuse not installed",
    )
    from opentelemetry.sdk.trace import TracerProvider

    from nerve.observability.usage_rewrite import (
        GEN_AI_CACHE_READ_TOKENS,
        GEN_AI_INPUT_TOKENS,
        USAGE_METADATA_ATTR,
        install_usage_rewriter,
    )

    lf._redact_patterns = _default_patterns()
    delegate = _RecordingExporter()
    processor = span_processor.LangfuseSpanProcessor(
        public_key="pk-test",
        secret_key="sk-test",
        base_url="http://localhost:9",
        span_exporter=delegate,
    )
    provider = TracerProvider()
    provider.add_span_processor(processor)
    client = SimpleNamespace(_resources=SimpleNamespace(
        tracer_provider=provider, public_key="pk-test",
    ))

    try:
        lf._install_redaction(client)
        assert install_usage_rewriter(client) is True
        tracer = provider.get_tracer("langsmith")
        with tracer.start_as_current_span("claude.assistant.turn") as span:
            span.set_attribute(
                "gen_ai.prompt.0.content", f"token {_FAKE_ANTHROPIC_KEY} here",
            )
            span.set_attribute(
                USAGE_METADATA_ATTR,
                '{"input_tokens": 100, "output_tokens": 5, '
                '"input_token_details": {"cache_read": 90}}',
            )
            span.set_attribute(GEN_AI_INPUT_TOKENS, 100)
        processor.force_flush()

        exported = [s for batch in delegate.batches for s in batch]
        assert len(exported) == 1
        attributes = exported[0].attributes
        assert attributes["gen_ai.prompt.0.content"] == "token [REDACTED] here"
        assert attributes[GEN_AI_INPUT_TOKENS] == 10
        assert attributes[GEN_AI_CACHE_READ_TOKENS] == 90
        assert not any(
            isinstance(v, str) and _FAKE_ANTHROPIC_KEY in v
            for v in attributes.values()
        )
    finally:
        provider.shutdown()


@pytest.fixture
def real_export_pipeline():
    from langfuse._client.span_processor import LangfuseSpanProcessor
    from langfuse._task_manager.media_manager import MediaManager
    from opentelemetry.sdk.trace import TracerProvider

    uploads = Queue()
    media = MediaManager(
        api_client=MagicMock(), httpx_client=MagicMock(), media_upload_queue=uploads,
    )
    delegate = _RecordingExporter()
    processor = LangfuseSpanProcessor(
        public_key="pk-test", secret_key="sk-test", base_url="http://localhost:9",
        span_exporter=delegate, media_manager=media,
    )
    provider = TracerProvider()
    provider.add_span_processor(processor)
    client = SimpleNamespace(_resources=SimpleNamespace(
        tracer_provider=provider, public_key="pk-test",
    ))
    lf._install_redaction(client)
    try:
        yield provider, delegate, uploads
    finally:
        provider.shutdown()


def test_langsmith_byte_payloads_are_redacted_on_export(real_export_pipeline):
    from langsmith import Client, traceable, tracing_context
    from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans

    provider, delegate, _ = real_export_pipeline
    lf._redact_patterns = _default_patterns()
    client = Client(tracing_mode="otel", otel_tracer_provider=provider)

    @traceable(run_type="tool", client=client)
    def sample_tool(text):
        return text

    try:
        with tracing_context(enabled=True):
            sample_tool(_FAKE_ANTHROPIC_KEY)
        client.flush()
        provider.force_flush()
        exported = [span for batch in delegate.batches for span in batch]
        assert len(exported) == 1
        assert json.loads(exported[0].attributes["gen_ai.prompt"]) == {"text": "[REDACTED]"}
        assert json.loads(exported[0].attributes["gen_ai.completion"]) == {"output": "[REDACTED]"}
        assert _FAKE_ANTHROPIC_KEY.encode() not in encode_spans(exported).SerializeToString()
    finally:
        client.close()


@pytest.mark.asyncio
async def test_anthropic_json_text_is_redacted_on_export(real_export_pipeline):
    from opentelemetry.instrumentation.anthropic.span_utils import aset_input_attributes

    provider, delegate, _ = real_export_pipeline
    lf._redact_patterns = lf._compile_patterns(["private-π-token", "line1\nline2"])
    tracer = provider.get_tracer("opentelemetry.instrumentation.anthropic")
    with tracer.start_as_current_span("anthropic.chat") as span:
        await aset_input_attributes(span, {
            "messages": [{"role": "user", "content": "private-π-token line1\nline2"}],
        })
    provider.force_flush()
    exported = [span for batch in delegate.batches for span in batch]
    assert len(exported) == 1
    messages = json.loads(exported[0].attributes["gen_ai.input.messages"])
    assert messages[0]["parts"][0]["content"] == "[REDACTED] [REDACTED]"


@pytest.mark.asyncio
@pytest.mark.parametrize("patterns", [[], [r"(?s).+"]])
async def test_media_queue_obeys_redaction(real_export_pipeline, patterns):
    from opentelemetry.instrumentation.anthropic.span_utils import aset_input_attributes

    provider, delegate, uploads = real_export_pipeline
    lf._redact_patterns = lf._compile_patterns(patterns)
    content = "data:text/plain;base64," + base64.b64encode(b"private content").decode()
    tracer = provider.get_tracer("opentelemetry.instrumentation.anthropic")
    with tracer.start_as_current_span("anthropic.chat") as span:
        await aset_input_attributes(span, {
            "messages": [{"role": "user", "content": content}],
        })
    provider.force_flush()
    exported = [span for batch in delegate.batches for span in batch]
    assert len(exported) == 1
    if patterns:
        assert uploads.empty()
        assert "[REDACTED]" in exported[0].attributes["gen_ai.input.messages"]
        assert content not in exported[0].attributes["gen_ai.input.messages"]
    else:
        assert uploads.get_nowait()["content_bytes"] == b"private content"
        assert "@@@langfuseMedia:" in exported[0].attributes["gen_ai.input.messages"]


def test_masking_failure_drops_batch_before_media_extraction(real_export_pipeline, monkeypatch):
    provider, delegate, uploads = real_export_pipeline
    lf._redact_patterns = _default_patterns()
    monkeypatch.setattr(lf, "redact_attributes", MagicMock(side_effect=RuntimeError("test")))
    tracer = provider.get_tracer("langsmith")
    with tracer.start_as_current_span("media") as span:
        span.set_attribute("input", "data:text/plain;base64,cHJpdmF0ZQ==")
    provider.force_flush()
    assert delegate.batches == []
    assert uploads.empty()


# ---------------------------------------------------------------------------
# flush()
# ---------------------------------------------------------------------------


def test_flush_noop_when_disabled():
    # Should not raise even though no SDK is loaded.
    lf.flush()
    assert lf._last_flush_at is None


def test_flush_calls_client_flush_when_enabled(monkeypatch):
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)
    cfg = _config_with(public_key="pk-lf", secret_key="sk-lf")
    assert lf.init_langfuse(cfg) is True

    lf.flush()
    fakes.client.flush.assert_called_once()
    assert lf._last_flush_at is not None
    # ISO timestamp shape
    assert "T" in lf._last_flush_at


def test_flush_swallows_errors(monkeypatch):
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)
    cfg = _config_with(public_key="pk-lf", secret_key="sk-lf")
    assert lf.init_langfuse(cfg) is True

    fakes.client.flush.side_effect = RuntimeError("network gone")
    # Must not raise.
    lf.flush()
