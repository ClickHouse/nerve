"""Langfuse observability integration.

Activation is purely config-driven: when both ``langfuse.public_key`` and
``langfuse.secret_key`` are set in config, this module wires up tracing of
the agent loop and memU LLM calls. With keys absent, every public function
is a no-op and Nerve runs identically.

Implementation notes
--------------------
- Built on the Langfuse Python SDK (>= 3.0), which itself wraps OpenTelemetry.
- The Claude Agent SDK is instrumented via
  ``langsmith.integrations.claude_agent_sdk.configure_claude_agent_sdk``,
  which routes its OTEL spans into Langfuse via the ``LANGSMITH_OTEL_*``
  flags.
- Direct Anthropic SDK calls (used in :mod:`nerve.memory.memu_bridge`) are
  instrumented via ``opentelemetry.instrumentation.anthropic.AnthropicInstrumentor``.
- Trace-level attributes (``session_id``, ``user_id``, ``tags``, ``metadata``)
  are propagated to every span inside a wrapped block via OpenTelemetry
  Baggage using :func:`langfuse.propagate_attributes`.

Failure mode
------------
Initialization is best-effort. Bad keys, network failures, or missing
optional packages log a warning and leave Nerve running with observability
disabled — they never raise.
"""

from __future__ import annotations

import logging
import os
import re
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------
# Set once by ``init_langfuse``. When ``_enabled`` is False every public
# entry point short-circuits.

_enabled: bool = False
_host: str = ""
_redact_patterns: list[re.Pattern[str]] = []
_last_flush_at: str | None = None
_auth_ok: bool = False
_usage_rewriter: bool = False


def is_enabled() -> bool:
    """Return True when Langfuse tracing is active."""
    return _enabled


def get_status() -> dict[str, Any]:
    """Status block for ``/api/diagnostics`` and the UI deep-link."""
    return {
        "enabled": _enabled,
        "host": _host or None,
        "auth_ok": _auth_ok,
        "last_flush_at": _last_flush_at,
        "usage_rewriter": _usage_rewriter,
    }


def init_langfuse(config: Any) -> bool:
    """Initialize Langfuse + OTEL instrumentation if keys are configured.

    Returns True when active, False when disabled (no keys, missing
    packages, or auth failure). Never raises — observability must not be
    able to take down the gateway.
    """
    global _enabled, _host, _redact_patterns, _auth_ok, _usage_rewriter

    lf = getattr(config, "langfuse", None)
    if lf is None:
        return False

    public_key = (getattr(lf, "public_key", "") or "").strip()
    secret_key = (getattr(lf, "secret_key", "") or "").strip()
    if not public_key or not secret_key:
        logger.info("Langfuse: disabled (no public_key/secret_key in config)")
        return False

    host = (getattr(lf, "host", "") or "https://cloud.langfuse.com").rstrip("/")

    # Set env vars before any import — both the Langfuse SDK and the
    # LangSmith integration read these at import / client-init time.
    os.environ["LANGFUSE_PUBLIC_KEY"] = public_key
    os.environ["LANGFUSE_SECRET_KEY"] = secret_key
    os.environ["LANGFUSE_HOST"] = host
    os.environ.setdefault("LANGSMITH_OTEL_ENABLED", "true")
    os.environ.setdefault("LANGSMITH_OTEL_ONLY", "true")
    os.environ.setdefault("LANGSMITH_TRACING", "true")

    try:
        from langfuse import get_client
    except ImportError as e:
        logger.warning(
            "Langfuse: package not installed (%s) — observability disabled. "
            "Install with: uv pip install -e .[observability]", e,
        )
        return False

    try:
        client = get_client()
        _auth_ok = bool(client.auth_check())
        if not _auth_ok:
            logger.warning(
                "Langfuse: auth_check failed against %s — observability disabled. "
                "Verify pk/sk pair.", host,
            )
            return False
    except Exception as e:
        logger.warning(
            "Langfuse: auth_check raised (%s) — observability disabled", e,
        )
        return False

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

    # Wrap Claude Agent SDK. Failure here disables agent tracing but doesn't
    # disable the rest — direct Anthropic instrumentation can still cover
    # the memU pipeline.
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

    # Wrap direct Anthropic SDK (memU embeddings/condensation/classification).
    try:
        from opentelemetry.instrumentation.anthropic import AnthropicInstrumentor
        AnthropicInstrumentor().instrument()
    except Exception as e:
        logger.warning(
            "Langfuse: failed to instrument Anthropic SDK (%s) — "
            "memU LLM calls will not be traced", e,
        )

    # Compile redact patterns once. Bad regexes are skipped, not fatal.
    raw_patterns = list(getattr(lf, "redact_patterns", []) or [])
    compiled: list[re.Pattern[str]] = []
    for pat in raw_patterns:
        try:
            compiled.append(re.compile(pat))
        except re.error as e:
            logger.warning(
                "Langfuse: invalid redact pattern %r (%s) — skipped", pat, e,
            )
    _redact_patterns = compiled

    _enabled = True
    _host = host
    logger.info("Langfuse: enabled (host=%s)", host)
    return True


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
    out = text
    for pat in _redact_patterns:
        out = pat.sub("[REDACTED]", out)
    return out


def flush(timeout: float = 5.0) -> None:
    """Flush pending spans. Safe to call when disabled. Swallows errors.

    The ``timeout`` arg is accepted for forward compatibility — the current
    Langfuse Python SDK doesn't expose a per-call timeout on ``flush()``.
    """
    global _last_flush_at
    if not _enabled:
        return
    try:
        from langfuse import get_client
        client = get_client()
        client.flush()
        _last_flush_at = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        logger.debug("Langfuse flush failed (%s)", e)
