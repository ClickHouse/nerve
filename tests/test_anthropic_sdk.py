"""Nerve's Anthropic SDK clients against a stand-in Messages API.

The stand-in records each request. The tests check what the SDK sends for
Nerve's own model calls: API version 2026-10-21, the API key as
``Authorization: Bearer``, no sampling parameters and no trace context. With
Langfuse on, ``AnthropicInstrumentor`` makes the one span of each call, also
for the Bedrock clients.
"""

from __future__ import annotations

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from nerve import setup_checks
from nerve.config import NerveConfig

API_VERSION = "2026-10-21"
_KEY = "sk-ant-test-key-not-real"
_OK = {
    "id": "msg_test", "type": "message", "role": "assistant",
    "model": "claude-test", "content": [{"type": "text", "text": "ok"}],
    "stop_reason": "end_turn", "stop_sequence": None,
    "usage": {"input_tokens": 3, "output_tokens": 1},
}


class _StandIn:
    """A local HTTP server for ``POST /v1/messages`` and Bedrock's
    ``POST /model/<id>/invoke``. It records each request and answers with
    ``reply`` (status, JSON body)."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.reply: tuple[int, dict] = (200, _OK)
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: object) -> None:
                pass

            def do_POST(self) -> None:  # noqa: N802 - http.server naming
                raw = self.rfile.read(int(self.headers.get("content-length") or 0))
                stand_in.requests.append({
                    "path": self.path,
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                    "body": json.loads(raw or b"{}"),
                })
                status, body = stand_in.reply
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._server.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True,
        ).start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def stand_in(monkeypatch):
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy",
                 "https_proxy", "all_proxy", "ANTHROPIC_CUSTOM_HEADERS"):
        monkeypatch.delenv(name, raising=False)
    server = _StandIn()
    # The Bedrock clients send their requests here.
    monkeypatch.setenv("ANTHROPIC_BEDROCK_BASE_URL", server.url)
    try:
        yield server
    finally:
        server.close()


@pytest.fixture
def spans(monkeypatch):
    """A recording tracer provider as the global one, for this test only."""
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace, "_TRACER_PROVIDER", provider)
    yield exporter
    provider.shutdown()


@pytest.fixture
def instrumented(spans):
    """``AnthropicInstrumentor``, as Langfuse installs it, on the recording provider."""
    from opentelemetry import trace
    from opentelemetry.instrumentation.anthropic import AnthropicInstrumentor

    instrumentor = AnthropicInstrumentor()
    if instrumentor.is_instrumented_by_opentelemetry:
        instrumentor.uninstrument()
    instrumentor.instrument(tracer_provider=trace.get_tracer_provider())
    try:
        yield spans
    finally:
        instrumentor.uninstrument()


def _api_config(url: str) -> NerveConfig:
    return NerveConfig.from_dict({"anthropic_api_key": _KEY, "anthropic_base_url": url})


def _proxy_config(port: int) -> NerveConfig:
    return NerveConfig.from_dict({
        "proxy": {"enabled": True, "port": port, "api_key": "sk-local-proxy-key"},
    })


def _bedrock_config() -> NerveConfig:
    return NerveConfig.from_dict({"provider": {
        "type": "bedrock", "aws_region": "us-east-1",
        "aws_access_key_id": "test", "aws_secret_access_key": "test",
    }})


def _call(config: NerveConfig, *, use_async: bool, model: str = "claude-haiku-5-5") -> None:
    """One ``messages.create`` through Nerve's sync or async client factory."""
    kwargs = {"model": model, "max_tokens": 5, "messages": [{"role": "user", "content": "hi"}]}
    if use_async:
        async def run() -> None:
            async with config.create_async_anthropic_client(max_retries=0) as client:
                await client.messages.create(**kwargs)
        asyncio.run(run())
    else:
        config.create_anthropic_client(max_retries=0).messages.create(**kwargs)


# --------------------------------------------------------------------------- #
#  What a request carries                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("route", ["api_key", "proxy"])
def test_the_key_goes_out_as_a_bearer_token(stand_in, route, use_async):
    """The API key, or the proxy's local key, is in ``Authorization``. A
    proxy or gateway at the base URL must read it there."""
    if route == "api_key":
        config, key = _api_config(stand_in.url), _KEY
    else:
        config, key = _proxy_config(stand_in.port), "sk-local-proxy-key"

    _call(config, use_async=use_async)

    (request,) = stand_in.requests
    assert request["path"] == "/v1/messages"
    assert request["headers"]["authorization"] == f"Bearer {key}"
    assert "x-api-key" not in request["headers"]
    assert request["headers"]["anthropic-version"] == API_VERSION


@pytest.mark.parametrize("use_async", [False, True])
def test_bedrock_requests_use_the_new_api_version(stand_in, use_async):
    _call(_bedrock_config(), use_async=use_async, model="us.anthropic.claude-haiku-5-5")

    (request,) = stand_in.requests
    assert request["path"] == "/model/us.anthropic.claude-haiku-5-5/invoke"
    assert request["headers"]["authorization"].startswith("AWS4-HMAC-SHA256 ")
    assert request["body"]["anthropic_version"] == API_VERSION


@pytest.mark.asyncio
@pytest.mark.parametrize("model", [
    "us.anthropic.claude-haiku-5-5",
    "us.anthropic.claude-haiku-4-5-20251001-v1:0",
    "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/abc123",
])
async def test_the_memory_bedrock_client_sends_no_temperature(stand_in, model):
    """memU gives a temperature; the real SDK client gets none for any model."""
    from nerve.memory.memu_bridge import MemUBridge

    client = MemUBridge(_bedrock_config())._make_bedrock_client(model)
    try:
        text, _ = await client.chat("hello", temperature=0.3)
    finally:
        await client.close()

    assert text == "ok"
    (request,) = stand_in.requests
    assert request["path"].startswith("/model/")
    assert "temperature" not in request["body"]
    assert request["body"]["anthropic_version"] == API_VERSION


# --------------------------------------------------------------------------- #
#  The key check                                                               #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [
    # What the Claude API answered on 2026-11-13 for a key that it does not
    # know, sent as a bearer token.
    {"type": "error", "error": {"type": "authentication_error", "message": "invalid x-api-key"}},
    # A body that names no header and no error type: the status decides.
    {"error": "Invalid API key"},
])
@pytest.mark.parametrize("route", ["api_key", "proxy"])
async def test_a_401_for_the_bearer_key_is_an_invalid_key(stand_in, body, route):
    stand_in.reply = (401, body)
    config = _api_config(stand_in.url) if route == "api_key" else _proxy_config(stand_in.port)

    result = await setup_checks.provider_test(config)

    assert result["code"] == "invalid_key", result
    assert result["ok"] is False
    assert stand_in.requests[0]["headers"]["authorization"].startswith("Bearer ")


# --------------------------------------------------------------------------- #
#  Spans                                                                       #
# --------------------------------------------------------------------------- #


def test_nerve_clients_make_no_sdk_span_and_send_no_trace_context(stand_in, spans):
    import anthropic

    # Control: a client with the SDK defaults makes its own span and sends
    # the trace context.
    anthropic.Anthropic(api_key=_KEY, base_url=stand_in.url, max_retries=0).messages.create(
        model="claude-haiku-5-5", max_tokens=5, messages=[{"role": "user", "content": "hi"}],
    )
    assert [s.name for s in spans.get_finished_spans()] == ["anthropic.messages.create"]
    assert "traceparent" in stand_in.requests[0]["headers"]
    spans.clear()

    for config in (_api_config(stand_in.url), _bedrock_config()):
        for use_async in (False, True):
            _call(config, use_async=use_async)

    assert spans.get_finished_spans() == ()
    assert len(stand_in.requests) == 5
    assert not any("traceparent" in r["headers"] for r in stand_in.requests[1:])


@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("provider", ["anthropic", "bedrock"])
def test_the_instrumentor_wraps_each_client(stand_in, instrumented, provider, use_async):
    """One span for each call, from ``AnthropicInstrumentor``, with the model
    and the token counts."""
    if provider == "anthropic":
        config, model = _api_config(stand_in.url), "claude-haiku-5-5"
    else:
        config, model = _bedrock_config(), "us.anthropic.claude-haiku-5-5"

    _call(config, use_async=use_async, model=model)

    (span,) = instrumented.get_finished_spans()
    assert span.instrumentation_scope.name == "opentelemetry.instrumentation.anthropic"
    assert span.attributes["gen_ai.request.model"] == model
    assert span.attributes["gen_ai.usage.input_tokens"] == 3
    assert span.attributes["gen_ai.usage.output_tokens"] == 1
