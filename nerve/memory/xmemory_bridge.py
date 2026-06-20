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
  this bridge appends xmemory's single synthesized answer to the query.
* The memorization *sweep* (session-close, cron) stays memU-only — it
  never goes through the ``memorize`` tool handler, so it's untouched.

The bridge is inert unless ``config.xmemory.enabled`` (both an API token
and an ``instance_id`` are set). Every xmemory call is wrapped so a slow
or failing xmemory can never break memU recall or the memorize tool.
"""

from __future__ import annotations

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
        """Query xmemory and return its single synthesized answer.

        Uses ``SINGLE_ANSWER`` read mode — xmemory translates the question
        to SQL over its knowledge graph and returns a natural-language
        answer. Returns the answer string, or ``None`` when unavailable,
        empty, or on any error (so recall always falls back to memU alone).
        """
        if not self.available or not query:
            return None
        try:
            result = await self._instance.read(
                query, read_mode=self._ReadMode.SINGLE_ANSWER,
            )
            return _extract_answer(result)
        except Exception as e:
            logger.warning("xmemory read failed: %s", e)
            return None


def _extract_answer(result: Any) -> str | None:
    """Pull the natural-language answer out of an SDK ReadResult.

    SINGLE_ANSWER mode yields ``reader_result == {"answer": "..."}``; we
    stay defensive about shape (dict, object, or bare string).
    """
    reader = getattr(result, "reader_result", result)
    answer: Any = None
    if isinstance(reader, dict):
        answer = reader.get("answer")
    elif hasattr(reader, "answer"):
        answer = reader.answer
    elif isinstance(reader, str):
        answer = reader
    if answer is None:
        return None
    text = str(answer).strip()
    return text or None
