"""Langfuse observability integration.

Activation is purely config-driven: when both ``langfuse.public_key`` and
``langfuse.secret_key`` are set in config, this module wires up tracing of
the agent loop and of direct Anthropic SDK calls. With keys absent, every
public function is a no-op and Nerve runs identically.

Implementation notes
--------------------
- Built on the Langfuse Python SDK (>= 5.0.1), which itself wraps
  OpenTelemetry.
- The keys go only to the ``Langfuse`` client constructor. Nerve does not
  write them to ``os.environ``, so child processes (the Claude CLI, the
  agent's shell tools, Codex) do not inherit them.
- The Claude Agent SDK is instrumented via
  ``langsmith.integrations.claude_agent_sdk.configure_claude_agent_sdk``,
  which routes its OTEL spans into Langfuse via the ``LANGSMITH_OTEL_*``
  flags.
- Direct Anthropic SDK calls (session titles, memU on Bedrock, memU date
  resolution) are instrumented via
  ``opentelemetry.instrumentation.anthropic.AnthropicInstrumentor``.
- Trace-level attributes (``session_id``, ``user_id``, ``tags``,
  ``metadata``) are propagated to every span inside a wrapped block via
  :func:`langfuse.propagate_attributes`, through the in-process
  OpenTelemetry context.
- ``langfuse.redact_patterns`` are applied to text in span attributes,
  including bytes and nested values, before the SDK extracts media. The
  patterns match each string as text and, if it is JSON text, each string
  in the decoded value too. Span names and span events are not redacted.
  The SDK writes Langfuse metadata attributes as JSON text. They are
  decoded, redacted and encoded again, so they stay JSON text.

Failure mode
------------
Initialization is best-effort. A key pair that the host rejects, or a
missing package, logs a warning and leaves Nerve running with
observability disabled. If the startup key check cannot reach the host,
tracing stays on and the exporter sends spans once the host answers.
Nothing here raises.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Mapping
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

logger = logging.getLogger(__name__)

REDACTED = "[REDACTED]"

# Return value of ``_decode_json`` for text that is not JSON.
_NOT_JSON = object()

# The Langfuse SDK writes the value of these attributes, and of each
# ``<prefix>.<key>`` attribute, as JSON text. A string value is also
# JSON-encoded.
_METADATA_ATTRIBUTE_PREFIXES = (
    "langfuse.trace.metadata",
    "langfuse.observation.metadata",
)

# HTTP statuses with which a Langfuse host rejects a key pair.
_REJECTED_KEY_STATUSES = frozenset({401, 403})

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------
# Set once by ``init_langfuse``. When ``_enabled`` is False every public
# entry point short-circuits.

_enabled: bool = False
_host: str = ""
_project_id: str | None = None
_redact_patterns: list[re.Pattern[str]] = []
_last_flush_at: str | None = None
_auth_ok: bool = False
_usage_rewriter: bool = False
_client: Any = None


def is_enabled() -> bool:
    """Return True when Langfuse tracing is active."""
    return _enabled


def get_status() -> dict[str, Any]:
    """Status block for ``/api/diagnostics`` and the UI deep-link.

    ``project_id`` is None until the startup key check reaches the host.
    The UI needs it to link to a session, because Langfuse UI routes start
    with ``/project/<id>``.
    """
    return {
        "enabled": _enabled,
        "host": _host or None,
        "project_id": _project_id,
        "auth_ok": _auth_ok,
        "last_flush_at": _last_flush_at,
        "usage_rewriter": _usage_rewriter,
    }


def init_langfuse(config: Any) -> bool:
    """Initialize Langfuse + OTEL instrumentation if keys are configured.

    Returns True when active, False when disabled (no keys, missing
    packages, or a rejected key pair). Never raises: observability must
    not be able to take down the gateway.
    """
    global _enabled, _host, _project_id, _redact_patterns, _auth_ok
    global _usage_rewriter, _client

    lf = getattr(config, "langfuse", None)
    if lf is None:
        return False

    public_key = (getattr(lf, "public_key", "") or "").strip()
    secret_key = (getattr(lf, "secret_key", "") or "").strip()
    if not public_key or not secret_key:
        logger.info("Langfuse: disabled (no public_key/secret_key in config)")
        return False

    host = (getattr(lf, "host", "") or "https://cloud.langfuse.com").rstrip("/")

    # The LangSmith integration reads these flags when it is configured.
    # They hold no credentials.
    os.environ.setdefault("LANGSMITH_OTEL_ENABLED", "true")
    os.environ.setdefault("LANGSMITH_OTEL_ONLY", "true")
    os.environ.setdefault("LANGSMITH_TRACING", "true")

    try:
        from langfuse import Langfuse
        from langfuse._client.span_filter import is_default_export_span
    except ImportError as e:
        logger.warning(
            "Langfuse: package not importable (%s) — observability disabled. "
            "Run `uv sync` to reinstall Nerve's dependencies.", e,
        )
        return False

    _redact_patterns = _compile_patterns(getattr(lf, "redact_patterns", []))
    export_ready = False

    def should_export_span(span: Any) -> bool:
        return export_ready and is_default_export_span(span)

    try:
        client = Langfuse(
            public_key=public_key,
            secret_key=secret_key,
            base_url=host,
            should_export_span=should_export_span,
        )
    except Exception as e:
        logger.warning(
            "Langfuse: client setup failed (%s) — observability disabled",
            type(e).__name__,
        )
        return False

    try:
        _install_redaction(client)
    except Exception as e:
        logger.warning(
            "Langfuse: redaction setup failed (%s) — observability disabled",
            type(e).__name__,
        )
        _shutdown_quietly(client)
        return False

    # Key check: the same request as ``Langfuse.auth_check()``, which also
    # returns the project id for the UI deep link.
    try:
        projects = client.api.projects.get()
        data = list(getattr(projects, "data", None) or [])
        if not data:
            logger.warning(
                "Langfuse: %s returned no project for this key pair — "
                "observability disabled. Verify the pk/sk pair.", host,
            )
            _shutdown_quietly(client)
            return False
        _project_id = str(data[0].id)
        _auth_ok = True
    except Exception as e:
        status = getattr(e, "status_code", None)
        if status in _REJECTED_KEY_STATUSES:
            logger.warning(
                "Langfuse: %s rejected the key pair (HTTP %s) — "
                "observability disabled. Verify the pk/sk pair.", host, status,
            )
            _shutdown_quietly(client)
            return False
        logger.warning(
            "Langfuse: could not verify the keys against %s (%s) — tracing "
            "stays on, and spans are sent when the host answers",
            host, f"HTTP {status}" if status else type(e).__name__,
        )
        _auth_ok = False

    # Wrap the Langfuse OTLP exporter so LangSmith agent spans get
    # cache-aware usage attributes. Without this, Langfuse prices every
    # cached input token at the full uncached rate (~5-10x overcount on
    # agent sessions). See nerve/observability/usage_rewrite.py.
    try:
        from nerve.observability.usage_rewrite import install_usage_rewriter
        _usage_rewriter = install_usage_rewriter(client)
        if _usage_rewriter:
            logger.info("Langfuse: cache-aware usage rewriter installed")
        else:
            logger.warning(
                "Langfuse: usage rewriter not installed (SDK layout "
                "mismatch?) — agent-loop costs in Langfuse will overcount "
                "cached input tokens"
            )
    except Exception as e:
        _usage_rewriter = False
        logger.warning(
            "Langfuse: usage rewriter installation failed (%s) — "
            "agent-loop costs in Langfuse will overcount cached input "
            "tokens", e,
        )

    export_ready = True

    # Wrap Claude Agent SDK. Failure here disables agent tracing but doesn't
    # disable the rest — direct Anthropic instrumentation still covers
    # session titles and memU's Anthropic calls.
    try:
        from langsmith.integrations.claude_agent_sdk import (
            configure_claude_agent_sdk,
        )
        configure_claude_agent_sdk()
    except Exception as e:
        logger.warning(
            "Langfuse: failed to configure Claude Agent SDK tracing (%s) — "
            "agent loop will not be traced", e,
        )

    # Wrap direct Anthropic SDK calls (session titles, memU on Bedrock,
    # memU date resolution).
    try:
        from opentelemetry.instrumentation.anthropic import AnthropicInstrumentor
        AnthropicInstrumentor().instrument()
    except Exception as e:
        logger.warning(
            "Langfuse: failed to instrument Anthropic SDK (%s) — "
            "direct Anthropic calls will not be traced", e,
        )

    _client = client
    _enabled = True
    _host = host
    logger.info(
        "Langfuse: enabled (host=%s, project=%s)", host, _project_id or "unverified",
    )
    return True


def _compile_patterns(raw_patterns: Any) -> list[re.Pattern[str]]:
    """Compile redact patterns. Bad regexes are skipped, not fatal."""
    compiled: list[re.Pattern[str]] = []
    for pat in list(raw_patterns or []):
        try:
            compiled.append(re.compile(pat))
        except (re.error, TypeError) as e:
            logger.warning(
                "Langfuse: invalid redact pattern %r (%s) — skipped", pat, e,
            )
    return compiled


def _shutdown_quietly(client: Any) -> None:
    """Stop a client that will not be used. Errors are logged, never raised."""
    try:
        client.shutdown()
    except Exception as e:
        logger.debug("Langfuse: client shutdown failed (%s)", e)


def _redact_text(text: str, patterns: list[re.Pattern[str]]) -> str:
    out = text
    for pat in patterns:
        out = pat.sub(REDACTED, out)
    return out


def _decode_json(text: str) -> Any:
    """Return the decoded value of JSON object, array or string text.

    Returns ``_NOT_JSON`` for any other text.
    """
    if not text.lstrip().startswith(("{", "[", '"')):
        return _NOT_JSON
    try:
        return json.loads(text)
    except ValueError:
        return _NOT_JSON


def _encode_json(value: Any, like: str) -> str:
    """Encode ``value`` as JSON text with the separators that ``like`` uses."""
    spaced = ": " in like or ", " in like
    separators = (", ", ": ") if spaced else (",", ":")
    return json.dumps(value, ensure_ascii=False, separators=separators)


def _redact_string(value: str, patterns: list[re.Pattern[str]]) -> str:
    """Return a redacted copy of a string, or ``value`` itself.

    The patterns match the string as text first. If the result is JSON
    text, they also match each string, key and scalar in the decoded
    value. If a decoded part changes, the value is encoded again with the
    same separators and the patterns match the new text. If the text
    match makes JSON text invalid, the decoded original value gets the
    same steps. Thus a match in the text and a match that only the
    decoded value shows (for example an escaped character) are both
    redacted.
    """
    text = _redact_text(value, patterns)
    decoded = _decode_json(text)
    if decoded is _NOT_JSON:
        if text == value:
            return value
        decoded = _decode_json(value)
        if decoded is _NOT_JSON:
            return text
        redacted = _redact_json(decoded, patterns)
        if redacted is decoded:
            return text
        return _redact_text(_encode_json(redacted, value), patterns)
    redacted = _redact_json(decoded, patterns)
    if redacted is not decoded:
        text = _redact_text(_encode_json(redacted, text), patterns)
    return value if text == value else text


def _redact_value(value: Any, patterns: list[re.Pattern[str]]) -> Any:
    """Return a redacted copy of an OTEL attribute value.

    Returns ``value`` itself when nothing matched, so callers can detect a
    change with ``is``. Text in bytes, mappings, sequences and serialized
    JSON is included. Non-UTF-8 bytes are replaced with the redaction marker.
    """
    if isinstance(value, bytes):
        try:
            text = value.decode("utf-8")
        except UnicodeDecodeError:
            return REDACTED.encode("utf-8")
        redacted = _redact_string(text, patterns)
        return value if redacted is text else redacted.encode("utf-8")
    if isinstance(value, str):
        return _redact_string(value, patterns)
    if isinstance(value, Mapping):
        items = {
            _redact_text(key, patterns): _redact_value(item, patterns)
            for key, item in value.items()
        }
        return value if items == value else items
    if isinstance(value, (list, tuple)):
        items = tuple(_redact_value(item, patterns) for item in value)
        return value if all(a is b for a, b in zip(value, items)) else items
    return value


def _is_metadata_attribute(key: Any) -> bool:
    return isinstance(key, str) and any(
        key == prefix or key.startswith(prefix + ".")
        for prefix in _METADATA_ATTRIBUTE_PREFIXES
    )


def _redact_json(value: Any, patterns: list[re.Pattern[str]]) -> Any:
    """Return a redacted copy of a decoded JSON value.

    Returns ``value`` itself when nothing matched. Strings go through
    :func:`_redact_string`. A number, boolean or null whose JSON text a
    pattern matches becomes the redacted text, as a string.
    """
    if isinstance(value, str):
        return _redact_string(value, patterns)
    if isinstance(value, dict):
        items: dict[str, Any] = {}
        changed = False
        for key, item in value.items():
            new_key = _redact_text(key, patterns)
            new_item = _redact_json(item, patterns)
            changed = changed or new_key != key or new_item is not item
            items[new_key] = new_item
        return items if changed else value
    if isinstance(value, list):
        items_list = [_redact_json(item, patterns) for item in value]
        if any(a is not b for a, b in zip(value, items_list)):
            return items_list
        return value
    text = json.dumps(value)
    redacted = _redact_text(text, patterns)
    return value if redacted == text else redacted


def _redact_metadata_value(value: Any, patterns: list[re.Pattern[str]]) -> Any:
    """Return a redacted copy of a Langfuse metadata attribute value.

    Returns ``value`` itself when nothing matched. The SDK encodes each
    metadata value as JSON text, so this text is decoded, the decoded
    value is redacted, and the result is encoded again as JSON text. The
    patterns do not match the quotes that the SDK adds. A value that is
    not JSON text is redacted as plain text.
    """
    if not isinstance(value, str):
        return _redact_value(value, patterns)
    try:
        decoded = json.loads(value)
    except ValueError:
        return _redact_value(value, patterns)
    redacted = _redact_json(decoded, patterns)
    if redacted is decoded:
        return value
    return json.dumps(redacted, ensure_ascii=False, separators=(",", ":"))


def redact_attributes(
    attributes: Any, patterns: list[re.Pattern[str]],
) -> dict[str, Any]:
    """Return only the attributes that the patterns change, redacted."""
    changed: dict[str, Any] = {}
    if not patterns:
        return changed
    for key, value in attributes.items():
        if _is_metadata_attribute(key):
            redacted = _redact_metadata_value(value, patterns)
        else:
            redacted = _redact_value(value, patterns)
        if redacted is not value:
            changed[key] = redacted
    return changed


def _mask_otel_spans(*, params: Any) -> Any:
    """Redact span attributes before media extraction and export.

    Runs on the span processor's export thread for every batch this client
    sends. If it raises, Langfuse drops the whole batch, so a bug here
    loses spans and never sends them unredacted.
    """
    patterns = _redact_patterns
    if not patterns:
        return None

    from langfuse.types import MaskOtelSpansResult, OtelSpanPatch

    patches = {}
    for identifier, span in params.spans.items():
        changed = redact_attributes(span.attributes, patterns)
        if changed:
            patches[identifier] = OtelSpanPatch(set_attributes=changed)
    if not patches:
        return None
    return MaskOtelSpansResult(span_patches=patches)


def _install_redaction(client: Any) -> None:
    """Install masking ahead of the client's media-processing exporter.

    Requires the Langfuse 5 / OpenTelemetry 1.45 exporter layout. The
    client's span filter stays closed until this setup succeeds.
    """
    from langfuse._client.span_exporter import LangfuseTransformingSpanExporter
    from langfuse._client.span_processor import LangfuseSpanProcessor

    resources = client._resources
    processors = resources.tracer_provider._active_span_processor._span_processors
    matching = [
        processor for processor in processors
        if isinstance(processor, LangfuseSpanProcessor)
        and processor.public_key == resources.public_key
    ]
    if len(matching) != 1:
        raise RuntimeError("Expected one Langfuse span processor")
    batch = matching[0]._batch_processor
    batch._exporter = LangfuseTransformingSpanExporter(
        exporter=batch._exporter,
        media_manager=None,
        mask_otel_spans=_mask_otel_spans,
    )


@contextmanager
def attributes(
    *,
    session_id: str | None = None,
    user_id: str | None = None,
    tags: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> Iterator[None]:
    """Wrap a block so OTEL spans inside it carry these Langfuse attributes.

    No-op when Langfuse is disabled. Setup failures (missing optional
    package, ``propagate_attributes`` raising on bad kwargs) are logged
    and the block runs without propagation. Exceptions thrown into the
    yielded block (e.g. ``asyncio.TimeoutError`` from the engine's idle-
    timeout path) propagate normally — they are never swallowed and never
    converted into a spurious ``RuntimeError`` from a double-yield.
    """
    if not _enabled:
        yield
        return

    try:
        from langfuse import propagate_attributes
    except Exception:
        yield
        return

    kwargs: dict[str, Any] = {}
    if session_id:
        kwargs["session_id"] = session_id
    if user_id:
        kwargs["user_id"] = user_id
    if tags:
        kwargs["tags"] = list(tags)
    if metadata:
        # Filter out None values — Langfuse rejects them in some versions.
        clean_meta = {k: v for k, v in metadata.items() if v is not None}
        if clean_meta:
            kwargs["metadata"] = clean_meta

    # Guard ENTER only — never wrap the yield. ExitStack closes the inner
    # context on the way out even when the yielded block raises, so spans
    # are still finalized correctly. A previous version wrapped the yield
    # in a try/except Exception and yielded a second time on failure,
    # which produced "RuntimeError: generator didn't stop after throw()"
    # whenever an exception was thrown into the yield, masking the real
    # exception (e.g. the engine's asyncio.TimeoutError).
    with ExitStack() as stack:
        try:
            stack.enter_context(propagate_attributes(**kwargs))
        except Exception as e:
            logger.debug(
                "Langfuse propagate_attributes failed (%s) — continuing", e,
            )
        yield


def redact(text: str) -> str:
    """Apply configured redact patterns to a string. No-op when disabled."""
    if not _enabled or not text or not _redact_patterns:
        return text
    return _redact_text(text, _redact_patterns)


def flush(timeout: float = 5.0) -> None:
    """Flush pending spans. Safe to call when disabled. Swallows errors.

    The ``timeout`` arg is accepted for forward compatibility — the current
    Langfuse Python SDK doesn't expose a per-call timeout on ``flush()``.
    """
    global _last_flush_at
    if not _enabled or _client is None:
        return
    try:
        _client.flush()
        _last_flush_at = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        logger.debug("Langfuse flush failed (%s)", e)
