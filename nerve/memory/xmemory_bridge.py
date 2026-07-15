"""xmemory.ai bridge — optional structured-memory layer alongside memU.

xmemory (https://xmemory.ai) is a schema-backed memory service. Unlike
memU's free-form semantic store, an xmemory *instance* holds structured
objects defined by a schema; you ``write`` free text (an LLM extracts it
into typed objects) and ``read`` in natural language (it answers from the
knowledge graph).

In Nerve, xmemory runs *next to* memU, never replacing it:

* ``memorize`` tool  → dual-writes: memU (as today) **and** xmemory
  (async ``write_async``, fire-and-forget).
* ``memory_recall`` tool → memU returns its N items/breadcrumbs **and**
  this bridge appends xmemory's read result (serialized as JSON) for the
  query. The read mode is configurable (``xmemory.read_mode``): a synthesized
  natural-language answer by default (``single-answer``), or the structured
  ``raw-tables`` / ``xresponse`` payloads.
* The memorization *sweep* (session-close, cron) stays memU-only — it
  never goes through the ``memorize`` tool handler, so it's untouched.

The bridge is inert unless ``config.xmemory.enabled`` (both an API token
and an ``instance_id`` are set). Every xmemory call is wrapped so a slow
or failing xmemory can never break memU recall or the memorize tool.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from nerve.config import XmemoryConfig

logger = logging.getLogger(__name__)


class XmemoryBridge:
    """Thin async wrapper around the ``xmemory-ai`` SDK.

    Holds a long-lived :class:`AsyncXmemoryClient` bound to a single
    instance. Constructed cheaply; the network client and instance handle
    are created in :meth:`initialize`. All public data methods degrade to a
    no-op (returning ``None`` / ``False``) when the bridge is unavailable.
    """

    def __init__(self, config: "XmemoryConfig") -> None:
        self._config = config
        self._client: Any = None
        self._instance: Any = None
        self._available = False
        # SDK enum/type handles, populated on successful import.
        self._ReadMode: Any = None
        self._ExtractionLogic: Any = None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def initialize(self) -> None:
        """Construct the async client and bind the instance.

        No-op (stays unavailable) when xmemory is not configured or the
        ``xmemory-ai`` package is not importable. Never raises.
        """
        if not self._config.enabled:
            logger.debug(
                "xmemory: not configured (need api_key + instance_id) — disabled",
            )
            return

        try:
            from xmemory import (  # type: ignore[import-not-found]
                AsyncXmemoryClient,
                ExtractionLogic,
                ReadMode,
            )
        except ImportError as e:
            logger.warning(
                "xmemory: configured but `xmemory-ai` package not installed "
                "(%s) — disabled. Run `uv pip install xmemory-ai`.",
                e,
            )
            return

        try:
            self._client = AsyncXmemoryClient(
                self._config.api_url or None,
                api_key=self._config.api_key,
                timeout=self._config.timeout,
            )
            # ``.instance()`` returns a bound handle with no network call;
            # reads/writes hit the API lazily.
            self._instance = self._client.instance(self._config.instance_id)
            self._ReadMode = ReadMode
            self._ExtractionLogic = ExtractionLogic
            self._available = True
            logger.info(
                "xmemory bridge ready (instance=%s, url=%s)",
                self._config.instance_id,
                self._config.api_url,
            )
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("xmemory: client init failed (%s) — disabled", e)
            self._client = None
            self._instance = None
            self._available = False

    async def aclose(self) -> None:
        """Close the underlying HTTP client. Idempotent, never raises."""
        client = self._client
        self._available = False
        self._client = None
        self._instance = None
        if client is None:
            return
        try:
            close = getattr(client, "aclose", None) or getattr(client, "close", None)
            if close is not None:
                result = close()
                if hasattr(result, "__await__"):
                    await result
        except Exception as e:  # pragma: no cover - defensive
            logger.debug("xmemory: error closing client: %s", e)

    @property
    def available(self) -> bool:
        """True when xmemory is configured, imported, and bound."""
        return self._available and self._instance is not None

    # ------------------------------------------------------------------ #
    # Data ops
    # ------------------------------------------------------------------ #
    def _extraction_logic(self) -> Any:
        """Map the configured ``extraction_logic`` string to the SDK enum."""
        fast = (self._config.extraction_logic or "deep").strip().lower() == "fast"
        return self._ExtractionLogic.FAST if fast else self._ExtractionLogic.DEEP

    def _read_mode(self) -> Any:
        """Map the configured ``read_mode`` to the SDK enum.

        Config values mirror the SDK's own wire values (``single-answer``,
        ``raw-tables``, ``xresponse``), so the enum resolves them directly and
        any mode the SDK adds later needs no change here. Underscores are
        accepted as an alias; unknown values fall back to ``single-answer``,
        the configured default.
        """
        mode = (self._config.read_mode or "").strip().lower().replace("_", "-")
        try:
            return self._ReadMode(mode)
        except ValueError:
            logger.warning(
                "xmemory: unknown read_mode=%r, falling back to single-answer",
                self._config.read_mode,
            )
            return self._ReadMode.SINGLE_ANSWER

    async def memorize(self, text: str) -> bool:
        """Async-write ``text`` to xmemory (fire-and-forget).

        Returns True if the write was enqueued, False otherwise. Failures
        are swallowed (logged) so the memorize tool never fails on xmemory.
        """
        if not self.available or not text:
            return False
        try:
            await self._instance.write_async(
                text, extraction_logic=self._extraction_logic(),
            )
            return True
        except Exception as e:
            logger.warning("xmemory write_async failed: %s", e)
            return False

    async def recall_answer(self, query: str) -> str | None:
        """Query xmemory and return its read result serialized as JSON.

        The read mode comes from ``xmemory.read_mode``. The result shape is
        mode-dependent — an answer envelope (``single-answer``), table
        ``columns``/``rows`` (``raw-tables``), or ``objects``/``relations``
        (``xresponse``) — with no field common to all three. So the bridge does
        not parse it: it serializes the whole read payload as JSON and hands
        that to recall, letting the model read the structure. The payload is the
        per-sub-query ``reader_results`` when the server decomposed the query
        (xmemory-ai 0.10.0+), else the combined ``reader_result``.

        Returns ``None`` when unavailable, empty, or on any error (so recall
        always falls back to memU alone).
        """
        if not self.available or not query:
            return None
        try:
            result = await self._instance.read(query, read_mode=self._read_mode())
        except Exception as e:
            logger.warning("xmemory read failed: %s", e)
            return None
        return _serialize_read_payload(result)


def _serialize_read_payload(result: Any) -> str | None:
    """Serialize an SDK ReadResult's payload as JSON.

    Prefers ``reader_results`` (one entry per sub-query when the server
    decomposed a composite query) and falls back to the combined
    ``reader_result`` when it is absent or empty — a query the server did not
    decompose, or a server predating decomposition. The payload shape is
    mode-dependent and left intact; only Pydantic models are unwrapped to plain
    dicts (via :func:`_json_default`) so ``json`` can render them. A bare-string
    payload passes through unquoted. Returns ``None`` for an empty payload.
    """
    payload = getattr(result, "reader_results", None)
    if not payload:  # None or empty list → fall back to the combined result
        payload = getattr(result, "reader_result", result)
    if payload is None:
        return None
    if isinstance(payload, str):
        return payload.strip() or None
    try:
        text = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, default=_json_default,
        )
    except Exception:
        text = str(payload)
    return text.strip() or None


def _json_default(value: Any) -> Any:
    """``json.dumps`` hook for values it cannot render natively — chiefly the
    SDK's Pydantic models (e.g. ``TaggedReaderResult``), unwrapped to dicts."""
    for attr in ("model_dump", "dict"):
        method = getattr(value, attr, None)
        if callable(method):
            return method()
    if hasattr(value, "__dict__"):
        return value.__dict__
    return str(value)
