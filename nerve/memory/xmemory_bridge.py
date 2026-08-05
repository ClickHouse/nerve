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
* The memorization *sweep* (session-close, cron) is memU-only by default.
  With ``xmemory.index_conversations`` set, every message window the sweep
  indexes into memU is also mirrored here as a **text-only** transcript
  (:meth:`XmemoryBridge.memorize_conversation`): role + content only —
  thinking and tool blocks/results never leave the box. Transcripts are
  chunked and written with FAST extraction (they are high-volume; the
  configured ``extraction_logic`` still governs the memorize tool).

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

# Soft byte budget per transcript write job (headers may push a chunk a few
# dozen bytes over). Sized so each xmemory extraction sees a coherent slice
# of conversation (~16K tokens) while staying well under request-size caps.
_TRANSCRIPT_CHUNK_BYTES = 64_000


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

    @property
    def indexes_conversations(self) -> bool:
        """True when the bridge is available AND transcript mirroring is
        opted in via ``xmemory.index_conversations``."""
        return self.available and self._config.index_conversations

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

    async def memorize_conversation(self, session_id: str, messages: list[dict]) -> int:
        """Mirror a session-transcript window to xmemory as free-text writes.

        Same text-only contract as the memU sweep: each message contributes
        ``role`` + ``content`` (+ ``created_at`` when present) — ``thinking``
        and ``blocks`` (tool calls/results, images) are never sent. Long
        transcripts are split at message boundaries into
        ~``_TRANSCRIPT_CHUNK_BYTES`` chunks, each enqueued via ``write_async``
        with FAST extraction (transcripts are high-volume; the configured
        ``extraction_logic`` still governs the memorize tool's writes).

        Opt-in via ``xmemory.index_conversations`` and best-effort by design:
        a failed chunk is logged, the remaining chunks are abandoned, and the
        window is never retried for xmemory (the sweep watermark is memU's).
        Returns the number of chunks successfully enqueued (0 when disabled,
        empty, or on an immediate failure).
        """
        if not self.indexes_conversations or not messages:
            return 0
        chunks = _transcript_chunks(
            session_id, messages, chunk_bytes=_TRANSCRIPT_CHUNK_BYTES,
        )
        if not chunks:
            return 0
        sent = 0
        for chunk in chunks:
            try:
                await self._instance.write_async(
                    chunk, extraction_logic=self._ExtractionLogic.FAST,
                )
                sent += 1
            except Exception as e:
                logger.warning(
                    "xmemory transcript write failed for session %s "
                    "(chunk %d/%d): %s — abandoning remaining chunks",
                    session_id, sent + 1, len(chunks), e,
                )
                break
        if sent:
            logger.info(
                "xmemory: enqueued transcript for session %s "
                "(%d message(s), %d/%d chunk(s))",
                session_id, len(messages), sent, len(chunks),
            )
        return sent

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


def _transcript_lines(messages: list[dict]) -> list[str]:
    """Flatten message rows into text-only transcript lines.

    Mirrors the memU sweep's payload contract (see
    ``MemUBridge.memorize_conversation``): only ``role`` + ``content``
    (+ ``created_at`` when present) survive. ``thinking`` and ``blocks``
    (tool calls/results, images) are deliberately dropped, and messages
    with empty content are skipped.
    """
    lines: list[str] = []
    for msg in messages:
        content = str(msg.get("content") or "").strip()
        if not content:
            continue
        role = msg.get("role") or "unknown"
        created_at = msg.get("created_at")
        prefix = f"[{created_at}] {role}" if created_at else str(role)
        lines.append(f"{prefix}: {content}")
    return lines


def _transcript_chunks(
    session_id: str,
    messages: list[dict],
    chunk_bytes: int = _TRANSCRIPT_CHUNK_BYTES,
) -> list[str]:
    """Split a transcript into write-sized chunks of ~``chunk_bytes`` each.

    Splits at message boundaries so each extraction sees whole messages; a
    single message larger than the budget is hard-split on byte boundaries
    (multibyte characters straddling a cut are dropped, matching the recall
    handler's clipping). Each chunk opens with a one-line header carrying
    the session id and, for multi-chunk transcripts, its position — enough
    context for xmemory's extraction to relate the parts.
    """
    lines = _transcript_lines(messages)
    if not lines:
        return []

    # Message-boundary pieces, hard-splitting any single oversized line.
    parts: list[str] = []
    for line in lines:
        data = line.encode("utf-8")
        if len(data) <= chunk_bytes:
            parts.append(line)
        else:
            parts.extend(
                data[i : i + chunk_bytes].decode("utf-8", "ignore")
                for i in range(0, len(data), chunk_bytes)
            )

    groups: list[list[str]] = [[]]
    size = 0
    for part in parts:
        n = len(part.encode("utf-8")) + 1  # +1 for the joining newline
        if size and size + n > chunk_bytes:
            groups.append([])
            size = 0
        groups[-1].append(part)
        size += n

    total = len(groups)
    chunks: list[str] = []
    for i, group in enumerate(groups, start=1):
        position = f", part {i}/{total}" if total > 1 else ""
        header = f"Conversation transcript (session {session_id}{position}):\n"
        chunks.append(header + "\n".join(group))
    return chunks
