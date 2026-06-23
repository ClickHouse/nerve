"""Observability integrations.

Houses the Langfuse adapter (``langfuse``) and a vendor-neutral OpenTelemetry
OTLP exporter (``otel``). Both are optional and config-driven; they can run
together (Langfuse reuses the OTel-provided global tracer provider).
"""
