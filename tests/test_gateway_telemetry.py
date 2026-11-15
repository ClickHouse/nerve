"""FastAPI's own OpenTelemetry spans stay off on the gateway app.

Langfuse installs the global tracer provider. With FastAPI's default, the
gateway would record a span for each request into that provider, and the span
of a /ws connection would become the parent of each agent turn that the
connection starts.
"""

from __future__ import annotations

from fastapi.testclient import TestClient
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


def test_gateway_records_no_request_spans(tmp_path, monkeypatch):
    import nerve.config as cfgmod
    from nerve.config import NerveConfig
    from nerve.gateway.server import create_app

    config = NerveConfig()
    config.workspace = tmp_path / "ws"
    config.workspace.mkdir()
    monkeypatch.setattr(cfgmod, "_config", config, raising=False)

    # A configured global provider, as Langfuse leaves it. The patch keeps it
    # out of the process-wide provider, which OpenTelemetry sets only once.
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)

    # Without the context manager, TestClient does not run the lifespan.
    response = TestClient(create_app()).get("/health")

    assert response.status_code == 200
    assert exporter.get_finished_spans() == ()
