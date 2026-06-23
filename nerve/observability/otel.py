"""Generic OpenTelemetry (OTLP) export.

Vendor-neutral counterpart to :mod:`nerve.observability.langfuse`. When
``telemetry.endpoint`` is configured, this module sets up global OTel
Tracer / Meter / Logger providers with OTLP exporters pointed at an
endpoint the operator runs (a collector, or any OTLP backend). Nerve does
not run a collector — bring your own.

Activation is config-driven: empty ``endpoint`` → every entry point is a
no-op and Nerve runs identically (the FastAPI request middleware is only
attached when enabled, so there is zero per-request overhead).

Coexistence with Langfuse
-------------------------
Langfuse v3 *reuses* an existing global ``TracerProvider`` if one is
already set, only creating its own when the global is still a
``ProxyTracerProvider``. So :func:`init_otel` MUST run before
``init_langfuse``: Nerve installs the global provider, then Langfuse
attaches its span processor onto it, and every span fans out to both the
OTLP exporter and Langfuse.

Failure mode
------------
Best-effort, like the Langfuse integration. Missing optional packages,
a bad endpoint, or exporter errors log a warning and leave Nerve running
with telemetry disabled — they never raise.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module state — set once by ``init_otel``. When ``_enabled`` is False every
# public entry point short-circuits.
# ---------------------------------------------------------------------------
_enabled: bool = False
_endpoint: str = ""
_traces: bool = False
_metrics: bool = False
_logs: bool = False
_tracer_provider: Any = None      # opentelemetry.sdk.trace.TracerProvider
_meter_provider: Any = None       # opentelemetry.sdk.metrics.MeterProvider
_logger_provider: Any = None      # opentelemetry.sdk._logs.LoggerProvider
_log_handler: Any = None          # LoggingHandler attached to the root logger
_last_flush_at: str | None = None


# ---------------------------------------------------------------------------
# No-op instrument — lets call sites do ``otel.memorize_runs.add(1)``
# unconditionally. Replaced with real counters in ``init_otel`` when metrics
# are enabled.
# ---------------------------------------------------------------------------
class _NoopInstrument:
    def add(self, *args: Any, **kwargs: Any) -> None:  # counters
        pass

    def record(self, *args: Any, **kwargs: Any) -> None:  # histograms
        pass


_NOOP = _NoopInstrument()

# nerve-specific instruments (reassigned to real counters in init_otel)
memorize_runs: Any = _NOOP
memorize_errors: Any = _NOOP
notifications_sent: Any = _NOOP
agent_turns: Any = _NOOP


def is_enabled() -> bool:
    """Return True when OTLP export is active."""
    return _enabled


def get_status() -> dict[str, Any]:
    """Status block for ``/api/diagnostics`` and the UI."""
    return {
        "enabled": _enabled,
        "endpoint": _endpoint or None,
        "traces": _traces,
        "metrics": _metrics,
        "logs": _logs,
        "last_flush_at": _last_flush_at,
    }


# ---------------------------------------------------------------------------
# Exporter / reader factories — overridable so tests can inject in-memory
# variants. Endpoint/protocol come from the standard OTEL_EXPORTER_OTLP_* env
# vars (seeded by init_otel from config, operator env winning). ``headers``
# is passed to the exporter constructor directly rather than via the
# OTEL_EXPORTER_OTLP_HEADERS env var, whose comma/percent-encoded string
# format would mangle values containing commas — see init_otel.
# ---------------------------------------------------------------------------
def _make_span_exporter(headers: dict | None = None) -> Any:
    if _proto_is_grpc():
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
    else:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
    return OTLPSpanExporter(headers=headers) if headers else OTLPSpanExporter()


def _make_metric_reader(interval_ms: int, headers: dict | None = None) -> Any:
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

    if _proto_is_grpc():
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
            OTLPMetricExporter,
        )
    else:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )
    exporter = OTLPMetricExporter(headers=headers) if headers else OTLPMetricExporter()
    return PeriodicExportingMetricReader(
        exporter, export_interval_millis=interval_ms,
    )


def _make_log_exporter(headers: dict | None = None) -> Any:
    if _proto_is_grpc():
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
            OTLPLogExporter,
        )
    else:
        from opentelemetry.exporter.otlp.proto.http._log_exporter import (
            OTLPLogExporter,
        )
    return OTLPLogExporter(headers=headers) if headers else OTLPLogExporter()


def _proto_is_grpc() -> bool:
    """Effective protocol, resolved from the (already-seeded) env var."""
    proto = os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
    return proto == "grpc"


def _service_version() -> str:
    try:
        from importlib.metadata import version
        return version("nerve")
    except Exception:
        return "0.1.0"


def init_otel(config: Any) -> bool:
    """Set up global Trace/Meter/Logger providers with OTLP exporters.

    Returns True when active, False when disabled (no endpoint) or on a
    setup failure. Never raises. MUST be called before ``init_langfuse``
    and before the agent engine / SDK clients are created.
    """
    global _enabled, _endpoint, _traces, _metrics, _logs
    global _tracer_provider, _meter_provider, _logger_provider, _log_handler
    global memorize_runs, memorize_errors, notifications_sent, agent_turns

    if _enabled:
        return True  # idempotent — providers are set-once globally

    tel = getattr(config, "telemetry", None)
    if tel is None or not getattr(tel, "enabled", False):
        logger.info("OTel: disabled (no telemetry.endpoint configured)")
        return False

    endpoint = tel.endpoint.strip()

    # Seed standard env vars (operator-set env always wins via setdefault).
    # Headers are NOT seeded here — they're passed to exporter constructors
    # below to avoid the OTEL_EXPORTER_OTLP_HEADERS string-encoding pitfalls.
    os.environ.setdefault("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
    os.environ.setdefault("OTEL_EXPORTER_OTLP_PROTOCOL", tel.protocol)
    os.environ.setdefault("OTEL_SERVICE_NAME", tel.service_name)

    # Resolve the EFFECTIVE protocol (env wins) and verify the exporter is
    # available; fall back to http/protobuf so a grpc request without the
    # grpc package degrades gracefully instead of silently dropping spans.
    # This MUST read the env var (not tel.protocol) so the factories — which
    # also read env via _proto_is_grpc() — and this check always agree.
    if os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] == "grpc":
        try:
            import opentelemetry.exporter.otlp.proto.grpc  # noqa: F401
        except ImportError:
            logger.warning(
                "OTel: protocol 'grpc' requested but opentelemetry-exporter-"
                "otlp-proto-grpc is not installed — falling back to "
                "'http/protobuf'.",
            )
            os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf"

    # Headers go to the exporter constructor directly. If the operator set
    # OTEL_EXPORTER_OTLP_HEADERS themselves, defer to the SDK's env parsing
    # (pass None) so we don't double-apply.
    headers: dict | None = (
        None
        if "OTEL_EXPORTER_OTLP_HEADERS" in os.environ
        else (tel.headers or None)
    )

    try:
        from opentelemetry.sdk.resources import Resource

        resource = Resource.create({
            "service.name": tel.service_name,
            "service.version": _service_version(),
        })
    except Exception as e:
        logger.warning("OTel: failed to build resource (%s) — disabled", e)
        return False

    # --- Traces -----------------------------------------------------------
    if tel.traces:
        try:
            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor

            tp = TracerProvider(resource=resource)
            tp.add_span_processor(BatchSpanProcessor(_make_span_exporter(headers)))
            # First-call-wins globally. We run before init_langfuse, so
            # Langfuse will reuse this provider rather than replace it.
            trace.set_tracer_provider(tp)
            _tracer_provider = tp
            _traces = True
        except Exception as e:
            logger.warning("OTel: trace setup failed (%s)", e)

    # --- Metrics ----------------------------------------------------------
    if tel.metrics:
        try:
            from opentelemetry import metrics
            from opentelemetry.sdk.metrics import MeterProvider

            reader = _make_metric_reader(tel.metric_interval_ms, headers)
            mp = MeterProvider(resource=resource, metric_readers=[reader])
            metrics.set_meter_provider(mp)
            _meter_provider = mp

            # Bind counters to our provider directly (not the global) so they
            # are correct even if another provider won the set-once global.
            meter = mp.get_meter("nerve")
            memorize_runs = meter.create_counter(
                "nerve.memorize.runs", description="Memorization sweep runs",
            )
            memorize_errors = meter.create_counter(
                "nerve.memorize.errors", description="Memorization sweep errors",
            )
            notifications_sent = meter.create_counter(
                "nerve.notifications.sent", description="Notifications dispatched",
            )
            agent_turns = meter.create_counter(
                "nerve.agent.turns", description="Agent run turns started",
            )
            # Only mark metrics live once all counters are bound, so a failure
            # mid-setup doesn't leave a mix of real + no-op instruments.
            _metrics = True

            if tel.system_metrics:
                try:
                    from opentelemetry.instrumentation.system_metrics import (
                        SystemMetricsInstrumentor,
                    )
                    SystemMetricsInstrumentor().instrument(meter_provider=mp)
                except Exception as e:
                    logger.warning("OTel: system metrics setup failed (%s)", e)
        except Exception as e:
            logger.warning("OTel: metric setup failed (%s)", e)

    # --- Logs (opt-in) ----------------------------------------------------
    if tel.logs:
        try:
            from opentelemetry._logs import set_logger_provider
            from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

            lp = LoggerProvider(resource=resource)
            lp.add_log_record_processor(
                BatchLogRecordProcessor(_make_log_exporter(headers)),
            )
            set_logger_provider(lp)
            _logger_provider = lp

            # Route stdlib logs to OTLP as well. setup_logging() owns console
            # formatting; this only adds an export sink.
            handler = LoggingHandler(level=logging.NOTSET, logger_provider=lp)
            logging.getLogger().addHandler(handler)
            _log_handler = handler
            _logs = True
        except Exception as e:
            logger.warning("OTel: log export setup failed (%s)", e)

    # --- Outbound HTTP instrumentation ------------------------------------
    if _traces:
        try:
            from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
            HTTPXClientInstrumentor().instrument()
        except Exception as e:
            logger.warning("OTel: httpx instrumentation failed (%s)", e)

    if not (_traces or _metrics or _logs):
        logger.warning("OTel: endpoint set but no signals enabled — disabled")
        return False

    _enabled = True
    _endpoint = endpoint
    logger.info(
        "OTel: enabled (endpoint=%s, protocol=%s, traces=%s metrics=%s logs=%s)",
        endpoint, os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL"),
        _traces, _metrics, _logs,
    )
    return True


def instrument_app(app: "FastAPI") -> None:
    """Attach FastAPI server instrumentation (HTTP spans + metrics).

    Called from ``create_app()`` — before the providers exist — so it relies
    on OTel's proxy tracer/meter resolving to our providers once ``init_otel``
    sets them in the lifespan. The caller decides whether to call this
    (gated on ``config.telemetry.enabled``) so disabled installs pay nothing.
    """
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor.instrument_app(app)
    except Exception as e:
        logger.warning("OTel: FastAPI instrumentation failed (%s)", e)


def flush(timeout_ms: int = 5000) -> None:
    """Force-flush pending spans/metrics/logs. Safe when disabled."""
    global _last_flush_at
    if not _enabled:
        return
    ok = False
    for provider in (_tracer_provider, _meter_provider, _logger_provider):
        if provider is None:
            continue
        try:
            provider.force_flush(timeout_millis=timeout_ms)
            ok = True
        except Exception as e:
            logger.debug("OTel: force_flush failed (%s)", e)
    if ok:
        _last_flush_at = datetime.now(timezone.utc).isoformat()


def shutdown(timeout_ms: int = 5000) -> None:
    """Flush and shut down all providers. Safe when disabled. Idempotent."""
    global _enabled, _tracer_provider, _meter_provider, _logger_provider
    global _log_handler, memorize_runs, memorize_errors
    global notifications_sent, agent_turns
    if not _enabled:
        return
    flush(timeout_ms)
    if _log_handler is not None:
        try:
            logging.getLogger().removeHandler(_log_handler)
        except Exception:
            pass
    for provider in (_tracer_provider, _meter_provider, _logger_provider):
        if provider is None:
            continue
        try:
            provider.shutdown()
        except Exception as e:
            logger.debug("OTel: provider shutdown failed (%s)", e)
    # Drop references to the now-dead providers and rebind counters to no-ops
    # so nothing (flush, diagnostics, call sites) touches a shut-down provider.
    # (Note: OTel's global tracer/meter providers are set-once per process, so
    # re-initializing in the same process is not supported regardless.)
    _tracer_provider = _meter_provider = _logger_provider = None
    _log_handler = None
    memorize_runs = memorize_errors = notifications_sent = agent_turns = _NOOP
    # Flip the flag last so a second shutdown() is a no-op.
    _enabled = False
