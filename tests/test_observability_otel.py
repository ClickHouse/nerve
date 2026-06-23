"""Tests for nerve.observability.otel — the generic OTLP export layer.

Hermetic: no network, no real OTLP endpoint. Exporters are swapped for the
SDK's in-memory variants by monkeypatching the module's exporter factories,
so spans/metrics/logs are captured in-process. Also covers the no-op path
(disabled) and the Langfuse-coexistence ordering invariant.
"""

from __future__ import annotations

import logging
import os
from types import SimpleNamespace

import pytest

from nerve.config import TelemetryConfig
from nerve.observability import otel


@pytest.fixture(autouse=True)
def _reset_otel_state(monkeypatch):
    """Reset module state + clear OTEL_* env between tests."""
    otel._enabled = False
    otel._endpoint = ""
    otel._traces = otel._metrics = otel._logs = False
    otel._tracer_provider = None
    otel._meter_provider = None
    otel._logger_provider = None
    # Defensively detach any OTLP log handler a prior test attached to root.
    if otel._log_handler is not None:
        logging.getLogger().removeHandler(otel._log_handler)
    otel._log_handler = None
    otel._last_flush_at = None
    otel.memorize_runs = otel._NOOP
    otel.memorize_errors = otel._NOOP
    otel.notifications_sent = otel._NOOP
    otel.agent_turns = otel._NOOP
    for var in (
        "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_HEADERS", "OTEL_SERVICE_NAME",
    ):
        monkeypatch.delenv(var, raising=False)
    yield
    otel._enabled = False


def _config(**kwargs) -> SimpleNamespace:
    cfg = SimpleNamespace()
    cfg.telemetry = TelemetryConfig(**kwargs)
    return cfg


# --------------------------------------------------------------------------- #
#  Disabled / no-op                                                            #
# --------------------------------------------------------------------------- #


def test_disabled_when_no_endpoint():
    assert otel.init_otel(_config()) is False
    assert otel.is_enabled() is False
    # No env seeded.
    assert "OTEL_EXPORTER_OTLP_ENDPOINT" not in os.environ
    # Counters remain no-ops and are safe to call.
    otel.memorize_runs.add(1)
    otel.notifications_sent.add(5)


def test_get_status_disabled():
    s = otel.get_status()
    assert s["enabled"] is False and s["endpoint"] is None
    assert s["traces"] is False and s["metrics"] is False and s["logs"] is False


def test_no_config_attr():
    assert otel.init_otel(SimpleNamespace()) is False


# --------------------------------------------------------------------------- #
#  Traces                                                                      #
# --------------------------------------------------------------------------- #


def test_traces_exported(monkeypatch):
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    exporter = InMemorySpanExporter()
    # Use a SimpleSpanProcessor wrapping the in-memory exporter so spans are
    # captured synchronously (no batch flush needed).
    monkeypatch.setattr(otel, "_make_span_exporter", lambda headers=None: exporter)
    monkeypatch.setattr(
        "opentelemetry.sdk.trace.export.BatchSpanProcessor", SimpleSpanProcessor,
    )

    assert otel.init_otel(_config(endpoint="http://localhost:4318",
                                  metrics=False, logs=False)) is True
    assert otel.is_enabled() and otel._traces

    tracer = otel._tracer_provider.get_tracer("test")
    with tracer.start_as_current_span("unit-span"):
        pass

    spans = exporter.get_finished_spans()
    assert any(s.name == "unit-span" for s in spans)


# --------------------------------------------------------------------------- #
#  Metrics                                                                     #
# --------------------------------------------------------------------------- #


def test_metrics_counter_recorded(monkeypatch):
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    reader = InMemoryMetricReader()
    monkeypatch.setattr(otel, "_make_metric_reader", lambda interval_ms, headers=None: reader)

    assert otel.init_otel(_config(endpoint="http://localhost:4318",
                                  traces=False, logs=False,
                                  system_metrics=False)) is True
    assert otel._metrics

    otel.memorize_runs.add(3)
    otel.notifications_sent.add(1)

    data = reader.get_metrics_data()
    # Flatten metric names that recorded data points.
    names = {
        m.name
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
    }
    assert "nerve.memorize.runs" in names
    assert "nerve.notifications.sent" in names


# --------------------------------------------------------------------------- #
#  Logs                                                                        #
# --------------------------------------------------------------------------- #


def test_logs_exported_with_trace_context(monkeypatch):
    from opentelemetry.sdk._logs.export import (
        InMemoryLogRecordExporter,
        SimpleLogRecordProcessor,
    )

    exporter = InMemoryLogRecordExporter()
    monkeypatch.setattr(otel, "_make_log_exporter", lambda headers=None: exporter)
    monkeypatch.setattr(
        "opentelemetry.sdk._logs.export.BatchLogRecordProcessor",
        SimpleLogRecordProcessor,
    )
    # Need a tracer too so a span context exists for correlation.
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )
    monkeypatch.setattr(otel, "_make_span_exporter", lambda headers=None: InMemorySpanExporter())
    monkeypatch.setattr(
        "opentelemetry.sdk.trace.export.BatchSpanProcessor", SimpleSpanProcessor,
    )

    assert otel.init_otel(_config(endpoint="http://localhost:4318",
                                  logs=True, metrics=False,
                                  system_metrics=False)) is True
    assert otel._logs

    try:
        tracer = otel._tracer_provider.get_tracer("test")
        with tracer.start_as_current_span("log-span"):
            logging.getLogger("nerve.test").warning("hello otel logs")

        records = exporter.get_finished_logs()
        assert any("hello otel logs" in str(r.log_record.body) for r in records)
        # The record emitted inside the span carries a valid trace id.
        assert any(r.log_record.trace_id for r in records)
    finally:
        otel.shutdown()


# --------------------------------------------------------------------------- #
#  Env passthrough + coexistence + status                                      #
# --------------------------------------------------------------------------- #


def test_env_setdefault_does_not_override(monkeypatch):
    monkeypatch.setattr(otel, "_make_span_exporter",
                        lambda headers=None: _silent_span_exporter())
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://preset:9999")

    otel.init_otel(_config(endpoint="http://localhost:4318",
                           metrics=False, logs=False))
    # Operator-set env wins; init must not clobber it.
    assert os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://preset:9999"


def test_coexistence_provider_is_not_proxy(monkeypatch):
    from opentelemetry import trace
    from opentelemetry.trace import ProxyTracerProvider

    monkeypatch.setattr(otel, "_make_span_exporter",
                        lambda headers=None: _silent_span_exporter())
    otel.init_otel(_config(endpoint="http://localhost:4318",
                           metrics=False, logs=False))

    # After init the global provider is a real SDK provider — so Langfuse v3
    # would take its "reuse existing provider" branch rather than replace it.
    assert not isinstance(trace.get_tracer_provider(), ProxyTracerProvider)
    from opentelemetry.sdk.trace import TracerProvider
    assert isinstance(otel._tracer_provider, TracerProvider)


def test_status_enabled(monkeypatch):
    monkeypatch.setattr(otel, "_make_span_exporter",
                        lambda headers=None: _silent_span_exporter())
    otel.init_otel(_config(endpoint="http://localhost:4318",
                           metrics=False, logs=False))
    s = otel.get_status()
    assert s["enabled"] is True
    assert s["endpoint"] == "http://localhost:4318"
    assert s["traces"] is True


def test_idempotent_reinit(monkeypatch):
    monkeypatch.setattr(otel, "_make_span_exporter",
                        lambda headers=None: _silent_span_exporter())
    cfg = _config(endpoint="http://localhost:4318", metrics=False, logs=False)
    assert otel.init_otel(cfg) is True
    first_provider = otel._tracer_provider
    # Second call short-circuits (already enabled) — no new provider.
    assert otel.init_otel(cfg) is True
    assert otel._tracer_provider is first_provider


# --------------------------------------------------------------------------- #
#  Protocol + headers (regression: review H1, H2)                              #
# --------------------------------------------------------------------------- #


def test_grpc_fallback_to_http_when_unavailable(monkeypatch):
    import importlib.util

    if importlib.util.find_spec("opentelemetry.exporter.otlp.proto.grpc"):
        pytest.skip("grpc exporter installed — fallback path not exercised")

    monkeypatch.setattr(otel, "_make_span_exporter",
                        lambda headers=None: _silent_span_exporter())
    otel.init_otel(_config(endpoint="http://localhost:4318", protocol="grpc",
                           metrics=False, logs=False))
    # Effective protocol must agree with what the factories read from env.
    assert os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] == "http/protobuf"


def test_env_protocol_grpc_also_triggers_fallback(monkeypatch):
    import importlib.util

    if importlib.util.find_spec("opentelemetry.exporter.otlp.proto.grpc"):
        pytest.skip("grpc exporter installed — fallback path not exercised")

    # Operator sets grpc via env while config stays default http — the check
    # must read the effective env value, not config.protocol.
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    monkeypatch.setattr(otel, "_make_span_exporter",
                        lambda headers=None: _silent_span_exporter())
    otel.init_otel(_config(endpoint="http://localhost:4318",
                           metrics=False, logs=False))
    assert os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] == "http/protobuf"


def test_headers_passed_to_exporter_not_env(monkeypatch):
    captured: dict = {}

    def fake_span_exporter(headers=None):
        captured["headers"] = headers
        return _silent_span_exporter()

    monkeypatch.setattr(otel, "_make_span_exporter", fake_span_exporter)
    # A value with a comma and '=' padding — exactly what the env-string
    # encoding would mangle.
    hdrs = {"authorization": "Bearer a,b=="}
    otel.init_otel(_config(endpoint="http://localhost:4318", headers=hdrs,
                           metrics=False, logs=False))
    assert captured["headers"] == hdrs
    # Headers are passed via constructor, NOT serialized into the env var.
    assert "OTEL_EXPORTER_OTLP_HEADERS" not in os.environ


def test_headers_defer_to_operator_env(monkeypatch):
    captured: dict = {}

    def fake_span_exporter(headers=None):
        captured["headers"] = headers
        return _silent_span_exporter()

    monkeypatch.setattr(otel, "_make_span_exporter", fake_span_exporter)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=Bearer%20x")
    otel.init_otel(_config(endpoint="http://localhost:4318", headers={"x": "y"},
                           metrics=False, logs=False))
    # Operator-set env present → defer to SDK env parsing (pass None).
    assert captured["headers"] is None


def test_shutdown_resets_state(monkeypatch):
    monkeypatch.setattr(otel, "_make_span_exporter",
                        lambda headers=None: _silent_span_exporter())
    otel.init_otel(_config(endpoint="http://localhost:4318",
                           metrics=False, logs=False))
    assert otel.is_enabled() and otel._tracer_provider is not None

    otel.shutdown()
    # State fully reset — no dangling references to shut-down providers.
    assert otel.is_enabled() is False
    assert otel._tracer_provider is None
    assert otel._meter_provider is None and otel._logger_provider is None
    assert otel.memorize_runs is otel._NOOP
    assert otel.agent_turns is otel._NOOP
    # A second shutdown is a no-op (doesn't raise).
    otel.shutdown()


def _silent_span_exporter():
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )
    return InMemorySpanExporter()
