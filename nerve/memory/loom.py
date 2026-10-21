"""Loom memory backend — memory kept in a Loom service, reached through the
MCP gateway.

Selected with ``memory.backend: loom``. Agents use Loom's own tools: the MCP
gateway's catalog lists Loom as the server ``loom``, and every catalog
server reaches the agent as an MCP server (its tools are
``mcp__loom__<tool>``). Nerve's nine memory tools are not offered then
(:func:`nerve.agent.backends.base.config_excluded_tools`).

Nerve itself makes only the lifecycle calls, through the same gateway, as
the same agent, so Loom attributes them to that agent's credential:

* **startup recall** — the memories a new session starts with: Loom's
  ``context.pack`` in startup mode, memories section only (tool
  ``loom_context_pack``). A pack that brings none gets ``loom_search``
  instead: Loom builds each section on its own and answers without one that
  failed, while the search fails as long as Loom does, so a failed section
  freezes nothing into the session.
* **transcript extraction** — at session close, rotation and in the
  indexing sweep: ``memory.set_from_messages`` with ``defer`` (tool
  ``loom_memory_set_from_messages``). Loom extracts a queued batch in the
  background and retries it there; how a batch ended is not on MCP
  (``memory.extraction_status`` is kept for people and operators), so the
  memory audit log names each batch's ``queue_id`` for a lookup in Loom's
  dashboard.
* **identity and health** — ``namespace.summary`` (tool
  ``loom_namespace_summary``) with the namespace Loom resolves for the
  agent: its answer names the org and namespace, and carries the counts the
  Diagnostics page shows. Loom's MCP has no ``whoami``; this is the probe.

Loom fills ``org``, ``namespace`` and ``agent`` from the agent's credential
when a call leaves them out; ``memory.loom.org`` and ``memory.loom.namespace``
make Nerve's calls name them instead.

Failure contract, the part the rest of Nerve depends on: a gateway or Loom
that cannot answer — no gateway URL, no ``loom`` server in the catalog, a
policy refusal (``-32001``), a condition that may pass (``-32002``), a
server fault, a timeout, a rate limit — raises
:class:`~nerve.memory.backend.MemoryBackendUnavailable`, so a session starts
without priors instead of crashing and the sweep keeps its watermark. A
request Loom refused as invalid raises :class:`LoomRequestError`.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, unquote

from nerve.config import NerveConfig
from nerve.mcp_gateway import (
    POLICY_REFUSED,
    GatewayCallError,
    McpGatewayCatalog,
    McpGatewayServer,
)
from nerve.memory.backend import MemoryBackendUnavailable

logger = logging.getLogger(__name__)

# The catalog server that is Loom.
LOOM_SERVER_ID = "loom"
# The synthetic tool that runs an op the roster does not list.
LOOM_OP = "loom_op"


@dataclass(frozen=True)
class LoomCall:
    """One Loom operation Nerve calls: the MCP tool that runs it, the
    arguments Nerve may send, and the result fields it reads.
    ``tests/test_loom_contract.py`` checks them against Loom's own op schemas
    (``tests/fixtures/loom_lifecycle_ops.json``)."""

    tool: str
    fields: frozenset[str]
    reads: frozenset[str]


LOOM_CALLS: dict[str, LoomCall] = {
    "context.pack": LoomCall(
        "loom_context_pack",
        frozenset({
            "org", "namespace", "query", "mode", "only_sections", "top_k",
            "section_token_budgets", "include_decisions",
        }),
        frozenset({"rendered", "sections"}),
    ),
    "search.query": LoomCall("loom_search", frozenset({"query", "top_k"}), frozenset({"memory"})),
    "memory.set_from_messages": LoomCall(
        "loom_memory_set_from_messages",
        frozenset({
            "org", "namespace", "session_id", "messages", "last_messages", "defer",
            "extra_tags", "observation_date",
        }),
        frozenset({"queue_id"}),
    ),
    "namespace.summary": LoomCall(
        "loom_namespace_summary",
        frozenset({"org", "namespace", "top_entities_limit"}),
        frozenset({"org", "namespace", "total_memories", "type_counts"}),
    ),
}

# Ops safe to send again after a failure: reads only. A transcript queued for
# extraction is never resent, since the lost answer may have queued it.
_RETRIED_OPS = frozenset({"context.pack", "search.query", "namespace.summary"})
_MAX_ATTEMPTS = 2
_BACKOFF_S = 0.5
# Seconds a lifecycle call may take through the gateway (which gives the
# upstream call up to two minutes); extraction carries a large body.
_READ_TIMEOUT_S = 20.0
_EXTRACT_TIMEOUT_S = 60.0
# After the gateway did not answer at all, fail fast for this long instead of
# making every session start wait out the timeout again.
_BREAKER_S = 15.0
# The longest Retry-After honoured as a hold on one op.
_HOLD_MAX_S = 60.0
# How long a failed identity probe is remembered before the next one.
_IDENTITY_RETRY_S = 30.0
# A catalog without Loom is read again at most this often.
_CATALOG_RETRY_S = 30.0
# The health note: probed this often in the background, and probed again on
# a read when it is older than the second figure.
_HEALTH_INTERVAL_S = 300.0
_HEALTH_FRESH_S = 60.0
# Codes in a Loom refusal that mean "not now" rather than "never".
_TRANSIENT_CODES = frozenset({
    "RATE_LIMITED", "OVERLOADED", "BACKEND_TIMEOUT", "EMBED_TIMEOUT", "SCHEMA_PENDING",
    "AUTH_UNAVAILABLE",
})

# What the startup recall may freeze into a session: items of this many
# characters, this many bytes in all, from a memories section of this many
# tokens.
_ITEM_MAX_CHARS = 500
_STARTUP_MAX_BYTES = 4000
_STARTUP_SECTION_TOKENS = 1000

# memory.set_from_messages takes at most 400 messages. A window is cut into
# batches of that size whose messages stay within this many bytes of JSON:
# the gateway refuses a request body over 1 MiB, and the envelope and the
# context turns need the rest.
_BATCH_MAX_MESSAGES = 400
_BATCH_MAX_BYTES = 768 * 1024
# Turns of the previous batch handed to the next as context (Loom's cap is
# 20), each clipped: they only resolve references, they are not extracted.
_CONTEXT_MESSAGES = 10
_CONTEXT_MAX_BYTES = 2000
# Batches Loom accepted, remembered so a retried window does not queue the
# same extraction twice (in this process).
_ACCEPTED_TTL_S = 24 * 3600
_ACCEPTED_MAX = 4096
# Every fact Loom extracts from a Nerve conversation carries this tag.
EXTRACTION_TAG = "source:nerve"

# One item of a context pack's memories section:
# `- [<citation> <labels>] <text>`, later lines indented by two spaces, the
# citation percent-encoded. A section cut to its budget ends in a line of its
# own that says so.
_PACK_LINE_RE = re.compile(r"- \[(?P<cite>[^\s\]]+)[^\]]*\] ?(?P<text>.*)")
_PACK_TRUNCATED = "...[truncated]"
_SECTION_OPEN = "<memories>\n"
_SECTION_CLOSE = "\n</memories>"

# A memory with no Nerve type is shown in the words of memU's types.
_LOOM_TYPE_NAMES = {
    "preference": "profile",
    "procedural": "behavior",
    "semantic": "knowledge",
    "episodic": "event",
}


class LoomRequestError(RuntimeError):
    """Loom understood a request and refused it; the same request gets the
    same answer. ``code`` is Loom's code for the refusal."""

    def __init__(self, op: str, status: int | None, code: str, message: str) -> None:
        status_text = f"HTTP {status} " if status else ""
        super().__init__(
            f"Loom refused {op} ({status_text}{code})" + (f": {message}" if message else "")
        )
        self.op = op
        self.status = status
        self.code = code
        self.message = message


@dataclass(frozen=True)
class LoomIdentity:
    """The org and namespace Nerve's calls land in. ``source`` is ``config``
    (memory.loom.org and memory.loom.namespace) or ``loom`` (what Loom
    resolved for the agent's credential)."""

    org: str
    namespace: str
    source: str


# --------------------------------------------------------------------------- #
# Metrics — the shape the diagnostics page renders (see MemUMetrics)
# --------------------------------------------------------------------------- #


@dataclass
class _OpStats:
    call_count: int = 0
    error_count: int = 0
    total_duration_s: float = 0.0
    last_duration_s: float = 0.0
    last_error: str = ""
    last_success_at: str | None = None


class LoomMetrics:
    """Per-operation counters, serialized like memU's for the same page."""

    OPERATIONS = ("startup_recall", "memorize_conversation", "health")

    def __init__(self) -> None:
        self.initialized_at = ""
        self.service_available = False
        self.ops: dict[str, _OpStats] = {name: _OpStats() for name in self.OPERATIONS}
        self.in_flight: dict[int, tuple[str, str, float]] = {}
        self._counter = 0

    def begin_op(self, operation: str, description: str) -> int:
        self._counter += 1
        self.in_flight[self._counter] = (operation, description, time.monotonic())
        return self._counter

    def end_op(self, op_id: int, *, success: bool, error: str = "") -> None:
        entry = self.in_flight.pop(op_id, None)
        if entry is None:
            return
        operation, _description, started = entry
        stats = self.ops.get(operation)
        if stats is None:
            return
        duration = time.monotonic() - started
        stats.call_count += 1
        stats.total_duration_s += duration
        stats.last_duration_s = duration
        if success:
            stats.last_success_at = datetime.now(UTC).isoformat()
        else:
            stats.error_count += 1
            stats.last_error = error[:200]

    def to_dict(self) -> dict[str, Any]:
        now = time.monotonic()
        return {
            "initialized_at": self.initialized_at,
            "service_available": self.service_available,
            "operations": {
                name: {
                    "call_count": s.call_count,
                    "error_count": s.error_count,
                    "avg_duration_s": round(s.total_duration_s / s.call_count, 2) if s.call_count else 0,
                    "last_duration_s": round(s.last_duration_s, 2),
                    "last_error": s.last_error,
                    "last_success_at": s.last_success_at,
                }
                for name, s in self.ops.items()
            },
            "in_flight": [
                {"operation": op, "description": desc, "elapsed_s": round(now - started, 1)}
                for op, desc, started in self.in_flight.values()
            ],
        }


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _clip(text: str, limit: int = _ITEM_MAX_CHARS) -> str:
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return json.dumps(value, ensure_ascii=False)


def _json_size(value: Any) -> int:
    """Bytes of ``value`` in a request body (compact UTF-8 JSON, as httpx sends it)."""
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _fit_turn(turn: dict[str, Any], max_bytes: int) -> dict[str, Any]:
    """``turn`` with its content cut until its JSON is at most ``max_bytes``."""
    size = _json_size(turn)
    if size <= max_bytes:
        return turn
    content = str(turn.get("content") or "")
    marker = " […truncated]"
    for _ in range(8):
        keep = max(0, int(len(content) * (max_bytes - 64) / max(size, 1)))
        content = content[:keep]
        candidate = {**turn, "content": content + marker}
        size = _json_size(candidate)
        if size <= max_bytes:
            return candidate
    return {**turn, "content": marker.strip()}


def _within_bytes(items: list[dict[str, str]], max_bytes: int) -> list[dict[str, str]]:
    """The leading items whose summaries fit in ``max_bytes`` together."""
    kept: list[dict[str, str]] = []
    used = 0
    for item in items:
        size = len(item["summary"].encode("utf-8"))
        if kept and used + size > max_bytes:
            break
        kept.append(item)
        used += size
    return kept


def _parse_dt(value: Any) -> datetime | None:
    """A timestamp as a naive UTC datetime; None if unreadable."""
    if not value:
        return None
    text = str(value).strip().replace(" ", "T", 1)
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC).replace(tzinfo=None)
    return dt


def _iso_utc(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _type_name(loom_type: Any) -> str:
    native = str(loom_type or "")
    return _LOOM_TYPE_NAMES.get(native, native)


def _memories_section(structured: Any) -> str:
    """The memories section of a context pack.

    The full output names it (``sections``); over MCP, Loom answers with the
    rendered pack only, where the section is tagged ``<memories>``. A pack cut
    to its budget may have lost the closing tag: what is left ends in the cut
    marker.
    """
    if not isinstance(structured, dict):
        return ""
    sections = structured.get("sections")
    if isinstance(sections, dict) and isinstance(sections.get("memories"), str):
        return sections["memories"]
    rendered = str(structured.get("rendered") or "")
    start = rendered.find(_SECTION_OPEN)
    if start < 0:
        return ""
    body = rendered[start + len(_SECTION_OPEN):]
    end = body.find(_SECTION_CLOSE)
    return body[:end] if end >= 0 else body


# --------------------------------------------------------------------------- #
# The backend
# --------------------------------------------------------------------------- #


class LoomMemory:
    """The lifecycle side of the Loom memory backend (see the module docstring)."""

    backend = "loom"

    def __init__(
        self,
        config: NerveConfig,
        audit_db: Any = None,
        *,
        catalog: McpGatewayCatalog | None = None,
        transport: Any = None,
        health_interval: float | None = _HEALTH_INTERVAL_S,
    ) -> None:
        self.config = config
        self._cfg = config.memory.loom
        # nerve.db, for the memory audit log.
        self._db = audit_db
        self._gateway_url = config.mcp_gateway_url or ""
        # Tests hand in an httpx.MockTransport for both the catalog and the calls.
        self._transport = transport
        self._catalog = catalog
        self._server: McpGatewayServer | None = None
        self._available = False
        self._error = ""
        self._metrics = LoomMetrics()
        self._ident: LoomIdentity | None = None
        self._ident_lock = asyncio.Lock()
        self._ident_retry_at = 0.0
        self._ident_error = ""
        self._down_until = 0.0
        self._down_reason = ""
        self._held: dict[str, tuple[float, str]] = {}
        self._accepted: dict[str, float] = {}
        self._backoff_s = _BACKOFF_S
        self._catalog_read_at = 0.0
        # Set when a call says the catalog may have changed (a policy refusal).
        self._catalog_stale = False
        # The last health probe: what the Diagnostics page shows.
        self._health: dict[str, Any] = {}
        self._health_at = 0.0
        self._health_lock = asyncio.Lock()
        self._health_task: asyncio.Task | None = None
        # None: no background probe (tests); the note is then probed on read.
        self._health_interval = health_interval

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def initialize(self) -> bool:
        """Get ready to reach Loom through the gateway. Never raises, and
        makes no call: the health probe starts in the background.

        Without ``mcp_gateway_url`` nothing can reach Loom: the backend
        stays unavailable and says why. A gateway that is not up yet, or a
        catalog without ``loom``, does not fail initialization: every call
        raises :class:`MemoryBackendUnavailable` until Loom answers.
        """
        if not self._gateway_url:
            self._error = "mcp_gateway_url is not set, so Loom cannot be reached"
            logger.error("Loom memory backend not started: %s", self._error)
            return False
        if self._catalog is None:
            self._catalog = McpGatewayCatalog(self._gateway_url, transport=self._transport)
        self._available = True
        self._metrics.initialized_at = datetime.now(UTC).isoformat()
        self._metrics.service_available = True
        if self._cfg.org and self._cfg.namespace:
            self._ident = LoomIdentity(self._cfg.org, self._cfg.namespace, "config")
        logger.info("Loom memory backend via the MCP gateway at %s", self._gateway_url)
        if self._health_interval is not None:
            self._health_task = asyncio.create_task(self._health_loop(), name="loom-health")
        return True

    async def shutdown(self) -> None:
        """Stop the health probe and close the HTTP clients. Idempotent."""
        task, self._health_task = self._health_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        server, self._server = self._server, None
        if server is not None:
            await server.close()
        if self._catalog is not None:
            await self._catalog.close()
        self._available = False
        self._metrics.service_available = False

    @property
    def available(self) -> bool:
        return self._available

    @property
    def metrics(self) -> LoomMetrics:
        return self._metrics

    @property
    def dashboard_url(self) -> str:
        """Where a person opens this memory in Loom's dashboard, or "".

        ``memory.loom.dashboard_url``, at the home namespace when it is
        known; no link without the setting.
        """
        base = (self._cfg.dashboard_url or "").strip().rstrip("/")
        if not base:
            return ""
        namespace = self._ident.namespace if self._ident else ""
        return f"{base}/memories?namespace={quote(namespace, safe='')}" if namespace else base

    @staticmethod
    def _release_memory() -> None:
        """No-op: nothing is held in process (the sweep calls this after memU)."""

    async def apply_categories(self, categories: list[Any]) -> bool:
        """Nothing to apply: Loom keeps its own categories, and memory.categories
        is memU's. True, so setup does not wait for a restart for them."""
        return True

    # ------------------------------------------------------------------ #
    # Transport
    # ------------------------------------------------------------------ #

    async def _loom_server(self) -> McpGatewayServer:
        """The gateway server that is Loom, from the catalog. A catalog
        without it, or one a policy refusal may have outdated, is read
        again, at most every 30 seconds."""
        if self._server is not None:
            return self._server
        assert self._catalog is not None
        catalog = self._catalog.latest
        server = next((s for s in (catalog.servers if catalog else ()) if s.id == LOOM_SERVER_ID), None)
        now = time.monotonic()
        outdated = server is None or self._catalog_stale
        if outdated and (catalog is None or now - self._catalog_read_at >= _CATALOG_RETRY_S):
            self._catalog_read_at, self._catalog_stale = now, False
            catalog = await self._catalog.refresh()
            server = next((s for s in (catalog.servers if catalog else ()) if s.id == LOOM_SERVER_ID), None)
        if server is None:
            raise MemoryBackendUnavailable(
                "memory backend unavailable: "
                + ("the MCP gateway's catalog lists no 'loom' server for this agent"
                   if catalog is not None else
                   f"the MCP gateway at {self._gateway_url} gave no catalog")
            )
        self._server = McpGatewayServer(server.url, transport=self._transport)
        return self._server

    async def _forget_server(self) -> None:
        self._catalog_stale = True
        server, self._server = self._server, None
        if server is not None:
            await server.close()

    async def _call(self, op: str, arguments: dict[str, Any], *, timeout: float = _READ_TIMEOUT_S) -> Any:
        """Run the Loom op ``op`` through the gateway; the op's structured result.

        ``memory.loom.org`` and ``memory.loom.namespace`` are added when the
        op takes them and the call names none.
        """
        if not self._available:
            raise MemoryBackendUnavailable(f"memory backend unavailable: {self._error or 'not running'}")
        spec = LOOM_CALLS[op]
        unknown = set(arguments) - spec.fields
        if unknown:  # a programming error, caught by the contract test
            raise ValueError(f"{op} does not take {sorted(unknown)} from Nerve")
        args = dict(arguments)
        for key, value in (("org", self._cfg.org), ("namespace", self._cfg.namespace)):
            if value and key in spec.fields:
                args.setdefault(key, value)
        name, call_args = (LOOM_OP, {"op": op, "arguments": args}) if spec.tool == LOOM_OP else (spec.tool, args)
        now = time.monotonic()
        if now < self._down_until:
            raise MemoryBackendUnavailable(
                f"memory backend unavailable: {self._down_reason} "
                f"(not retried for another {self._down_until - now:.0f}s)"
            )
        held = self._held.get(op)
        if held and now < held[0]:
            raise MemoryBackendUnavailable(
                f"memory backend unavailable: {held[1]} (held for another {held[0] - now:.0f}s)"
            )
        attempts = _MAX_ATTEMPTS if op in _RETRIED_OPS else 1
        for attempt in range(1, attempts + 1):
            server = await self._loom_server()
            try:
                result = await server.call_tool(name, call_args, timeout=timeout)
            except GatewayCallError as e:
                failure = self._gateway_failure(op, e)
                if e.status is None and e.code is None:
                    # No answer at all: do not make the next caller wait too.
                    self._down_until = time.monotonic() + _BREAKER_S
                    self._down_reason = str(e)
                if e.code == POLICY_REFUSED or e.status == 404:
                    # Loom left the agent's catalog, or the policy changed:
                    # the next call reads the catalog again.
                    await self._forget_server()
                if e.retry_after:
                    self._held[op] = (time.monotonic() + min(e.retry_after, _HOLD_MAX_S), str(e))
                if not e.retryable or attempt >= attempts or e.status is None or e.retry_after:
                    raise failure from None
                await asyncio.sleep(self._backoff_s * attempt)
                continue
            self._down_until = 0.0
            if result.is_error:
                raise self._refusal(op, result.structured, result.text)
            return result.structured
        raise AssertionError("unreachable")  # pragma: no cover

    @staticmethod
    def _gateway_failure(op: str, e: GatewayCallError) -> Exception:
        """What a failed gateway call means to the caller: Loom unavailable,
        or (for a call Loom's MCP server rejected as malformed) a refusal."""
        if e.code is not None and e.code != POLICY_REFUSED and not e.retryable:
            # A JSON-RPC error the same call gets again (invalid params,
            # method not served): a bug, not an outage.
            return LoomRequestError(op, e.status, f"JSON-RPC {e.code}", str(e))
        return MemoryBackendUnavailable(f"memory backend unavailable: {e}")

    def _refusal(self, op: str, body: Any, text: str) -> Exception:
        """A tool result with ``isError``: Loom ran the call and refused it."""
        data = body if isinstance(body, dict) else {}
        detail = data.get("detail") if isinstance(data.get("detail"), dict) else data
        code = str(detail.get("code") or data.get("code") or "") or "TOOL_ERROR"
        message = str(detail.get("message") or data.get("message") or text or "")[:300]
        status = data.get("status")
        status = status if isinstance(status, int) else None
        retry_after = data.get("retry_after_s")
        if code in _TRANSIENT_CODES or status in (401, 429, 503):
            if isinstance(retry_after, int | float) and retry_after > 0:
                self._held[op] = (time.monotonic() + min(float(retry_after), _HOLD_MAX_S), code)
            return MemoryBackendUnavailable(
                f"memory backend unavailable: Loom refused {op} for now ({code})"
                + (f": {message}" if message else "")
            )
        return LoomRequestError(op, status, code, message)

    async def _tracked(self, operation: str, description: str, work: Awaitable[Any]) -> Any:
        op_id = self._metrics.begin_op(operation, description)
        try:
            result = await work
        except BaseException as e:
            self._metrics.end_op(op_id, success=False, error=str(e) or type(e).__name__)
            raise
        self._metrics.end_op(op_id, success=True)
        return result

    async def _audit(self, action: str, target_type: str, target_id: str | None, details: dict) -> None:
        """Best-effort row in the memory audit log (the web Log tab)."""
        if not self._db or not hasattr(self._db, "log_audit"):
            return
        try:
            await self._db.log_audit(action, target_type, target_id, "bridge", details)
        except Exception as e:  # noqa: BLE001 — the audit log never fails a call
            logger.warning("Audit log failed: %s", e)

    # ------------------------------------------------------------------ #
    # Identity
    # ------------------------------------------------------------------ #

    async def _identity(self) -> LoomIdentity:
        """The org and namespace Nerve's calls land in: the settings, else
        what Loom resolves for the agent's credential (``namespace.summary``
        echoes it). Raises :class:`MemoryBackendUnavailable` when neither
        can say."""
        if self._ident is not None:
            return self._ident
        async with self._ident_lock:
            if self._ident is not None:
                return self._ident
            if self._cfg.org and self._cfg.namespace:
                self._ident = LoomIdentity(self._cfg.org, self._cfg.namespace, "config")
                return self._ident
            if self._ident_error and time.monotonic() < self._ident_retry_at:
                raise MemoryBackendUnavailable(f"memory backend unavailable: {self._ident_error}")
            try:
                summary = await self._call("namespace.summary", {"top_entities_limit": 0})
            except (MemoryBackendUnavailable, LoomRequestError) as e:
                self._ident_error = str(e).removeprefix("memory backend unavailable: ")
                self._ident_retry_at = time.monotonic() + _IDENTITY_RETRY_S
                raise MemoryBackendUnavailable(
                    f"memory backend unavailable: Loom could not name this agent's "
                    f"namespace ({self._ident_error})"
                ) from None
            data = summary if isinstance(summary, dict) else {}
            org = str(data.get("org") or "").strip()
            namespace = str(data.get("namespace") or "").strip()
            if not org or not namespace:
                self._ident_error = "Loom's namespace.summary named no org or namespace"
                self._ident_retry_at = time.monotonic() + _IDENTITY_RETRY_S
                raise MemoryBackendUnavailable(f"memory backend unavailable: {self._ident_error}")
            self._ident, self._ident_error = LoomIdentity(org, namespace, "loom"), ""
            return self._ident

    # ------------------------------------------------------------------ #
    # Startup recall
    # ------------------------------------------------------------------ #

    async def startup_recall(self, query: str, limit: int = 8) -> list[dict[str, str]]:
        """The memories a new session starts with; the engine freezes them for
        the whole session. ``{id, type, summary}`` items, at most ``limit``
        of 500 characters and 4,000 bytes in all.

        ``context.pack`` in startup mode, memories section only: Loom's fast
        search, whose hits it serves even where a search abstains, and no
        category blurbs. A pack that brings no memories gets ``loom_search``
        (see the module docstring). Raises when Loom cannot answer, so
        nothing is frozen.
        """
        if not self._available:
            return []
        return await self._tracked(
            "startup_recall", query[:80], self._startup_recall(query, int(limit)),
        )

    async def _startup_recall(self, query: str, limit: int) -> list[dict[str, str]]:
        if limit <= 0:
            return []
        try:
            pack = await self._call("context.pack", {
                "query": query,
                "mode": "startup",
                "only_sections": ["memories"],
                "top_k": max(1, min(limit, 20)),
                "section_token_budgets": {"memories": _STARTUP_SECTION_TOKENS},
                "include_decisions": False,
            })
        except LoomRequestError as e:
            logger.info("Loom refused context.pack (%s); the startup recall searches instead", e)
            items: list[dict[str, str]] = []
        else:
            items = self._pack_items(_memories_section(pack))
        if not items:
            items = await self._search_items(query, limit)
        return _within_bytes(items[:limit], _STARTUP_MAX_BYTES)

    def _display_id(self, key: str, namespace: str) -> str:
        home = self._ident.namespace if self._ident else self._cfg.namespace
        return f"{namespace}/{key}" if namespace and namespace != home else key

    def _pack_items(self, rendered: str) -> list[dict[str, str]]:
        """The items of a pack's memories section, in its order.

        Loom escapes ``<`` and ``>`` in recalled text. A section cut to its
        budget ends in a ``...[truncated]`` line; the item the cut went
        through is left out, and a cut inside the next item's label costs
        nothing, since that line is no item.
        """
        lines = rendered.rstrip().splitlines()
        cut = bool(lines) and lines[-1].strip() == _PACK_TRUNCATED
        if cut:
            lines = lines[:-1]
        items: list[dict[str, str]] = []
        current: dict[str, str] | None = None
        for line in lines:
            if line.startswith("- ["):
                current = None
                match = _PACK_LINE_RE.fullmatch(line)
                cite = unquote(match["cite"]).removeprefix("memory:").split("#", 1)[0] if match else ""
                namespace, _, key = cite.partition("/")
                if match and key:
                    current = {"id": self._display_id(key, namespace), "type": "", "summary": match["text"]}
                    items.append(current)
            elif line.startswith("  ") and current is not None:
                current["summary"] += "\n" + line[2:]
            else:
                current = None
        if cut and current is not None and items and items[-1] is current:
            items.pop()  # the cut went through it
        out = []
        for item in items:
            item["summary"] = _clip(item["summary"].replace("&lt;", "<").replace("&gt;", ">"))
            if item["summary"]:
                out.append(item)
        return out

    async def _search_items(self, query: str, limit: int) -> list[dict[str, str]]:
        """The memory hits of ``loom_search``, abstaining or not: a startup
        prior is context, not an answer."""
        result = await self._call("search.query", {"query": query, "top_k": max(1, min(limit, 20))})
        section = result.get("memory") if isinstance(result, dict) else None
        hits = section.get("results") if isinstance(section, dict) else None
        items: list[dict[str, str]] = []
        seen: set[str] = set()
        for hit in hits if isinstance(hits, list) else []:
            if not isinstance(hit, dict):
                continue
            key = str(hit.get("parent_memory_key") or hit.get("memory_key") or hit.get("key") or "")
            summary = _clip(_text(hit.get("content_excerpt") or hit.get("content") or hit.get("value")))
            if not key or not summary:
                continue
            item_id = self._display_id(key, str(hit.get("namespace") or ""))
            if item_id in seen:
                continue
            seen.add(item_id)
            items.append({"id": item_id, "type": _type_name(hit.get("memory_type")), "summary": summary})
        return items

    # ------------------------------------------------------------------ #
    # Transcript extraction
    # ------------------------------------------------------------------ #

    async def memorize_conversation(self, session_id: str, messages: list[dict]) -> bool:
        """Queue a session window for extraction (``memory.set_from_messages``
        with ``defer``): Loom extracts the facts in the background.

        Cut into batches of at most 400 messages within the byte budget.
        RAISES on any failure, the backend not running included: the sweep
        advances its watermark unless this raises, and a window that was not
        queued must be offered again. False only when there is nothing to
        send.
        """
        if not messages:
            return False
        if not self._available:
            raise MemoryBackendUnavailable(
                f"memory backend unavailable: {self._error or 'the Loom backend is not running'}"
            )
        turns = [turn for turn in map(self._turn, messages) if turn is not None]
        if not turns:
            return False
        return await self._tracked(
            "memorize_conversation", f"session {session_id}",
            self._memorize_conversation(session_id or "-", turns),
        )

    def _turn(self, message: dict) -> dict[str, Any] | None:
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            return None
        turn: dict[str, Any] = {"role": str(message.get("role") or "user"), "content": content}
        stamp = _parse_dt(message.get("created_at"))
        if stamp is not None:
            # Loom anchors what a turn says ("yesterday") to when it was said.
            turn["timestamp"] = _iso_utc(stamp)
        return _fit_turn(turn, _BATCH_MAX_BYTES - 1024)

    @staticmethod
    def _batches(turns: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
        batches: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        size = 0
        for turn in turns:
            turn_size = _json_size(turn) + 1
            if current and (len(current) >= _BATCH_MAX_MESSAGES or size + turn_size > _BATCH_MAX_BYTES):
                batches.append(current)
                current, size = [], 0
            current.append(turn)
            size += turn_size
        if current:
            batches.append(current)
        return batches

    @staticmethod
    def _context(turns: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """The context a following batch gets: the last turns, kept short."""
        return [_fit_turn(turn, _CONTEXT_MAX_BYTES) for turn in turns[-_CONTEXT_MESSAGES:]]

    @staticmethod
    def _observation_date(turns: list[dict[str, Any]]) -> str:
        """The day a batch was said (its last turn), for relative dates."""
        for turn in reversed(turns):
            stamp = turn.get("timestamp")
            if stamp:
                return str(stamp)[:10]
        return datetime.now(UTC).strftime("%Y-%m-%d")

    async def _send_window(
        self, session_id: str, turns: list[dict[str, Any]], context: list[dict[str, Any]],
    ) -> dict[str, Any]:
        arguments: dict[str, Any] = {
            "session_id": session_id,
            "messages": turns,
            "defer": True,
            "extra_tags": [EXTRACTION_TAG],
            "observation_date": self._observation_date(turns),
        }
        if context:
            arguments["last_messages"] = context
        result = await self._call("memory.set_from_messages", arguments, timeout=_EXTRACT_TIMEOUT_S)
        return result if isinstance(result, dict) else {}

    async def _memorize_conversation(self, session_id: str, all_turns: list[dict[str, Any]]) -> bool:
        queued = 0
        queue_ids: list[str] = []
        context: list[dict[str, Any]] = []
        batches = self._batches(all_turns)
        for turns in batches:
            digest = hashlib.sha256(
                json.dumps([session_id, turns], ensure_ascii=False, sort_keys=True).encode("utf-8"),
            ).hexdigest()
            if not self._was_accepted(digest):
                data = await self._send_window(session_id, turns, context)
                self._remember_accepted(digest)
                queued += 1
                if data.get("queue_id"):  # none when Loom extracted it right away
                    queue_ids.append(str(data["queue_id"]))
            context = self._context(turns)
        await self._audit("conversation_indexed", "resource", session_id, {
            "message_count": len(all_turns), "batches": len(batches), "queued": queued,
            "queue_ids": queue_ids,
        })
        return True

    def _was_accepted(self, digest: str) -> bool:
        accepted_at = self._accepted.get(digest)
        return accepted_at is not None and time.monotonic() - accepted_at < _ACCEPTED_TTL_S

    def _remember_accepted(self, digest: str) -> None:
        now = time.monotonic()
        if len(self._accepted) >= _ACCEPTED_MAX:
            cutoff = now - _ACCEPTED_TTL_S
            self._accepted = {k: t for k, t in self._accepted.items() if t > cutoff}
            if len(self._accepted) >= _ACCEPTED_MAX:
                self._accepted.pop(next(iter(self._accepted)))
        self._accepted[digest] = now

    # ------------------------------------------------------------------ #
    # Health
    # ------------------------------------------------------------------ #

    async def get_health(self) -> dict[str, Any]:
        """The diagnostics snapshot, in memU's shape plus ``backend``,
        ``dashboard_url`` (a link for people, or "") and ``loom`` (the
        gateway, the identity, the last error and when it was checked).

        Served from the health note, probed again first when it is older
        than a minute.
        """
        if time.monotonic() - self._health_at >= _HEALTH_FRESH_S:
            await self._probe_health()
        result = self._metrics.to_dict()
        note = self._health
        ident = self._ident
        result["database"] = dict(note.get("database") or {
            "total_items": 0, "total_categories": 0, "total_resources": 0,
            "type_distribution": {}, "events_missing_happened_at": 0, "db_size_mb": 0,
        })
        result["service_available"] = bool(self._available and note.get("reachable"))
        result["backend"] = self.backend
        result["dashboard_url"] = self.dashboard_url
        result["loom"] = {
            "gateway_url": self._gateway_url,
            "server": LOOM_SERVER_ID,
            "org": ident.org if ident else self._cfg.org,
            "namespace": ident.namespace if ident else self._cfg.namespace,
            "identity_source": ident.source if ident else "",
            "error": self._error if not self._available else str(note.get("error") or ""),
            "checked_at": note.get("checked_at"),
        }
        return result

    async def _health_loop(self) -> None:
        """Probe every five minutes, so the Diagnostics page shows a recent
        note without making a call of its own each time."""
        while True:
            try:
                await self._probe_health()
            except Exception:  # the next round tries again
                logger.exception("Loom health probe failed")
            await asyncio.sleep(self._health_interval or _HEALTH_INTERVAL_S)

    async def _probe_health(self) -> None:
        """One ``namespace.summary``: whether Loom answers through the
        gateway, the counts, and the namespace it resolved. Never raises."""
        async with self._health_lock:
            if time.monotonic() - self._health_at < 1.0:
                return  # another caller just probed
            database: dict[str, Any] = {
                "total_items": 0, "total_categories": 0, "total_resources": 0,
                "type_distribution": {}, "events_missing_happened_at": 0, "db_size_mb": 0,
            }
            note: dict[str, Any] = {"reachable": False, "error": "", "database": database}
            if not self._available:
                note["error"] = self._error or "not running"
            else:
                try:
                    summary = await self._tracked(
                        "health", "namespace.summary",
                        self._call("namespace.summary", {"top_entities_limit": 0}),
                    )
                except (MemoryBackendUnavailable, LoomRequestError) as e:
                    note["error"] = str(e).removeprefix("memory backend unavailable: ")
                else:
                    note["reachable"] = True
                    data = summary if isinstance(summary, dict) else {}
                    database["total_items"] = int(data.get("total_memories") or 0)
                    database["type_distribution"] = {
                        str(row.get("memory_type")): int(row.get("n") or 0)
                        for row in data.get("type_counts") or []
                        if isinstance(row, dict)
                    }
                    self._note_identity(str(data.get("org") or ""), str(data.get("namespace") or ""))
            note["checked_at"] = datetime.now(UTC).isoformat()
            was = self._health.get("reachable") if self._health else None
            if note["reachable"] != was:
                if note["reachable"]:
                    ident = self._ident
                    logger.info(
                        "Loom answers through the MCP gateway: org %s, namespace %s",
                        ident.org if ident else "?", ident.namespace if ident else "?",
                    )
                else:
                    logger.warning("Loom is not usable through the MCP gateway: %s", note["error"])
            self._health, self._health_at = note, time.monotonic()

    def _note_identity(self, org: str, namespace: str) -> None:
        """Keep the identity Loom resolved current (a settings identity stays)."""
        if not org or not namespace or (self._ident and self._ident.source == "config"):
            return
        fresh = LoomIdentity(org, namespace, "loom")
        if self._ident and self._ident != fresh:
            logger.warning(
                "Loom now resolves this agent to org %s, namespace %s (was %s, %s)",
                org, namespace, self._ident.org, self._ident.namespace,
            )
        self._ident = fresh
