"""Loom memory backend — the memory tools served from a Loom service.

Selected with ``memory.backend: loom``. Loom is a memory service with an HTTP
API in which every operation is ``POST /v1/<operation>`` with a JSON body;
this bridge calls it with plain ``httpx`` and exposes the same public surface
as :class:`nerve.memory.memu_bridge.MemUBridge`, so the tool handlers, the
engine's pre-recall and the indexing sweep work unchanged.

How the tools map onto Loom operations:

* ``memorize`` → ``memory.set`` under a key minted here, ``memory_type``
  ``semantic`` (``procedural`` for behaviour), tags ``nerve`` and
  ``nerve:<type>``, and ``valid_at`` for events.
* ``memory_recall`` → ``memory.search`` (``recipe="fast"``, every namespace
  the token reads) plus ``namespace.search_categories``, returned in memU's
  shape: items, then ``cat:<id>`` category breadcrumbs.
* ``memory_expand_category`` → ``namespace.list_categories`` to name the
  category, then ``memory.list`` of the rows tagged with that name.
* ``conversation_history`` → ``memory.timeline`` (``valid_at`` is the date a
  memory happened); ``memory_records_by_date`` → ``memory.list`` newest
  first, paged.
* ``memory_update`` → ``memory.update_by_id`` (no-clobber via
  ``if_match_sha256``) and ``memory.assign_category``; ``memory_delete`` →
  ``memory.delete``; ``category_update`` → ``namespace.register_category``.
* the indexing sweep → ``memory.set_from_messages`` with ``defer=true``.

Failure contract, the part the rest of Nerve depends on: an unreachable
service, a timeout, HTTP 5xx, 429 or a refused credential raises
:class:`~nerve.memory.backend.MemoryBackendUnavailable` after bounded retries,
so recall reports memory as DOWN instead of empty and the sweep keeps its
watermark. An empty or abstaining search is ``[]``, never an error. A request
Loom understood and refused (another 4xx) raises :class:`LoomRequestError`.

Every request carries ``consumer="nerve"``; writes carry the configured
``agent`` and, when known, the Nerve session id as ``session_id``. Reads use
``agent="*"``: in Loom the agent is authorship, and reading only one's own
rows would hide a namespace shared with other agents.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import random
import re
import time
import uuid
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from nerve.config import (
    LOOM_NAMESPACE_ENV,
    LOOM_ORG_ENV,
    NerveConfig,
    normalize_loom_base_url,
)
from nerve.memory.backend import MemoryBackendUnavailable, category_breadcrumb

logger = logging.getLogger(__name__)

# Stamped on every request: Loom's ledgers group calls by it.
CONSUMER = "nerve"
# Tags on Nerve's own writes, and on every fact Loom extracts from a Nerve
# conversation. The date tools read rows carrying either.
NERVE_TAG = "nerve"
EXTRACTION_TAG = "source:nerve"
_TYPE_TAG_PREFIX = "nerve:"
_NERVE_TAGS = [NERVE_TAG, EXTRACTION_TAG]

# Nerve's memory types live in a `nerve:<type>` tag. Loom's own `memory_type`
# only decides how a row is searched: behaviour is a procedure, the rest are
# plain facts. Never `episodic`, which Loom keeps out of search.
_PROCEDURAL_TYPES = frozenset({"behavior"})
_TYPE_RE = re.compile(r"[a-z][a-z0-9_-]{0,31}")

# Keys this bridge mints: `nerve_<type>_<20 hex>`. The type is in the key
# because memory.timeline answers without tags; a tag still wins wherever one
# is read, so a later memory_update of the type is what recall shows.
_MINTED_KEY_RE = re.compile(r"nerve_([a-z][a-z0-9_-]{0,31})_[0-9a-f]{20}")
_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
# Loom's namespace names: what may precede the `/` of a qualified id.
_NAMESPACE_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,127}")

# Per-call timeouts: reads take memory.loom.timeout_s, writes this multiple of
# it, and conversation extraction (a large body, queued server-side) this one.
_WRITE_TIMEOUT_FACTOR = 3
_EXTRACT_TIMEOUT_FACTOR = 6
# Connecting never needs the read budget; an unreachable host should fail fast.
_CONNECT_TIMEOUT_S = 5.0
_MAX_ATTEMPTS = 3
_BACKOFF_BASE_S = 0.25
_MAX_RETRY_AFTER_S = 5.0
# After a call fails on the connection or a 5xx, fail fast for this long
# instead of making every turn wait out the timeouts again.
_BREAKER_S = 15.0
# How long an identity that could not be resolved is remembered before the
# next attempt, so a recall storm does not repeat the lookup on every call.
_IDENTITY_RETRY_S = 30.0
# Wait before registering memory.categories again after an attempt failed.
_SEED_RETRY_S = 300.0

# Operations safe to send again after a timeout: reads, and the writes Loom
# marks idempotent (sent with an Idempotency-Key, so a resend replays).
_IDEMPOTENT_OPS = frozenset({
    "memory.search",
    "memory.list",
    "memory.timeline",
    "memory.get",
    "memory.get_by_id",
    "memory.set",
    "memory.update_by_id",
    "memory.delete",
    "memory.assign_category",
    "namespace.list_categories",
    "namespace.search_categories",
    "namespace.summary",
})
# 503 codes Loom answers before running the operation: any op may resend.
_NEVER_RAN_CODES = frozenset({"AUTH_UNAVAILABLE", "OVERLOADED", "IDEMPOTENT_IN_PROGRESS"})

# memory.set_from_messages takes at most 400 messages; a window is cut into
# batches of that size, and smaller when the bytes add up.
_BATCH_MAX_MESSAGES = 400
_BATCH_MAX_BYTES = 4 * 1024 * 1024
_MESSAGE_MAX_CHARS = 200_000
# Turns of the previous batch handed to the next as context (Loom's cap is 20),
# each clipped: they only resolve references, they are not extracted again.
_CONTEXT_MESSAGES = 10
_CONTEXT_MAX_CHARS = 2000
# Batches Loom accepted, remembered so a retried window does not queue the
# same extraction twice (in this process).
_ACCEPTED_TTL_S = 24 * 3600
_ACCEPTED_MAX = 4096

# memory_records_by_date scans memory.list newest first, at most this far.
_RECORDS_PAGE = 500
_RECORDS_MAX_PAGES = 5
# memory.timeline answers at most 500 rows.
_TIMELINE_MAX = 500
# namespace.search_categories shows the first 280 characters of a summary.
_SUMMARY_EXCERPT_CHARS = 280


class LoomRequestError(RuntimeError):
    """Loom understood a request and refused it (a 4xx other than auth).

    Not an outage — resending the same request gets the same answer — so it
    is kept apart from :class:`MemoryBackendUnavailable`. ``code`` is Loom's
    machine-readable ``detail.code``.
    """

    def __init__(self, op: str, status: int, code: str, message: str) -> None:
        super().__init__(
            f"Loom refused {op} (HTTP {status} {code})" + (f": {message}" if message else "")
        )
        self.op = op
        self.status = status
        self.code = code
        self.message = message


class _AuthRefused(Exception):
    """A 401/403: the identity is re-resolved once before it counts as an outage."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(f"HTTP {status} {code}")
        self.status = status
        self.code = code
        self.message = message


@dataclass(frozen=True)
class LoomIdentity:
    """The ``org`` and home ``namespace`` every request is addressed to.

    ``namespaces`` is everything the token reads (home first) when Loom said
    so; ``source`` is the step that produced it: ``config``, ``whoami`` or
    ``token``.
    """

    org: str
    namespace: str
    namespaces: tuple[str, ...] = ()
    source: str = "config"


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

    OPERATIONS = (
        "recall",
        "memorize",
        "memorize_conversation",
        "expand_category",
        "conversation_history",
        "records_by_date",
        "update_item",
        "delete_item",
        "categories",
    )

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


def _usable(value: Any) -> str:
    """An org or namespace a request can name: not blank, not a wildcard."""
    text = str(value or "").strip()
    return "" if text in ("", "-", "*") else text


def _jwt_claims(token: str) -> dict[str, Any] | None:
    """The payload of a JWT, decoded but NOT verified; None for anything else.

    Read only to learn which org and namespace the token names, so requests
    can address them. Loom verifies the token on every call.
    """
    parts = (token or "").split(".")
    if len(parts) != 3 or not parts[1]:
        return None
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        data = json.loads(base64.urlsafe_b64decode(payload))
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


def _error_detail(resp: httpx.Response) -> tuple[str, str]:
    """Loom's ``detail.code`` and ``detail.message`` for an error response.

    A body that is not Loom's envelope (a proxy's page) still yields a code.
    """
    try:
        body: Any = resp.json()
    except ValueError:
        body = None
    detail = body.get("detail") if isinstance(body, dict) else None
    if isinstance(detail, dict):
        code = str(detail.get("code") or "")
        message = str(detail.get("message") or "")
    elif isinstance(detail, str):
        code, message = "", detail
    else:
        code, message = "", resp.text[:200].strip()
    return code or f"HTTP_{resp.status_code}", message[:500]


def _retry_after(resp: httpx.Response) -> float | None:
    """Seconds the response asks to wait (``Retry-After``), if it says."""
    raw = resp.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None


def _parse_dt(value: Any) -> datetime | None:
    """A Loom or Nerve timestamp as a naive UTC datetime; None if unreadable."""
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


def _day_range(start: str, end: str) -> tuple[datetime, datetime]:
    """``[start 00:00, end + 1 day)`` in UTC for two ``YYYY-MM-DD`` dates."""
    try:
        first = date.fromisoformat(start.strip())
        last = date.fromisoformat((end or start).strip())
    except ValueError as e:
        raise ValueError(f"dates must be YYYY-MM-DD: {e}") from None
    return (
        datetime(first.year, first.month, first.day),
        datetime(last.year, last.month, last.day) + timedelta(days=1),
    )


def _clean_type(memory_type: str | None) -> str:
    """A Nerve memory type as it goes into a tag; ``knowledge`` when unusable."""
    text = (memory_type or "").strip().lower()
    return text if _TYPE_RE.fullmatch(text) else "knowledge"


def _type_tag(tag: str) -> str | None:
    """The type a ``nerve:<type>`` tag names, else None."""
    text = str(tag).strip().lower()
    if text.startswith(_TYPE_TAG_PREFIX):
        kind = text[len(_TYPE_TAG_PREFIX):]
        if _TYPE_RE.fullmatch(kind):
            return kind
    return None


def _nerve_type(tags: Any, memory_key: str, loom_type: Any) -> str:
    """The type the tools show: the ``nerve:<type>`` tag, else the one in a
    key this bridge minted, else Loom's own ``memory_type``."""
    for tag in tags or ():
        kind = _type_tag(tag)
        if kind:
            return kind
    minted = _MINTED_KEY_RE.fullmatch(memory_key or "")
    if minted:
        return minted.group(1)
    return str(loom_type or "")


def _text(value: Any) -> str:
    """A stored value as the one line of text the tools print."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return json.dumps(value, ensure_ascii=False)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        key = value.lower()
        if key not in seen:
            seen.add(key)
            out.append(value)
    return out


# --------------------------------------------------------------------------- #
# The bridge
# --------------------------------------------------------------------------- #


class LoomBridge:
    """The memory tools' backend when memory lives in a Loom service."""

    backend = "loom"

    def __init__(
        self,
        config: NerveConfig,
        audit_db: Any = None,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.config = config
        self._cfg = config.memory.loom
        self._audit_db = audit_db
        # Tests hand in an httpx.MockTransport; production uses the default.
        self._transport = transport
        self._client: httpx.AsyncClient | None = None
        self._base_url = ""
        self._available = False
        self._init_error = ""
        self._metrics = LoomMetrics()
        self._ident: LoomIdentity | None = None
        self._ident_lock = asyncio.Lock()
        self._ident_error = ""
        self._ident_retry_at = 0.0
        self._down_until = 0.0
        self._down_reason = ""
        self._accepted: dict[str, float] = {}
        self._ignored_noted: set[str] = set()
        self._background: set[asyncio.Task] = set()
        self._seeded = False
        self._seed_retry_at = 0.0
        read = float(self._cfg.timeout_s)
        self._timeouts = {
            "read": read,
            "write": read * _WRITE_TIMEOUT_FACTOR,
            "extract": read * _EXTRACT_TIMEOUT_FACTOR,
        }
        self._backoff_base_s = _BACKOFF_BASE_S

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def initialize(self) -> bool:
        """Open the HTTP client and learn the org and namespace. Never raises.

        An unreachable Loom does not fail initialization: the bridge reports
        itself available, every call raises ``MemoryBackendUnavailable``
        until Loom answers, and the identity is looked up again then. Only a
        configuration that can never work (a malformed URL) returns False.
        """
        if self._client is not None:
            return self._available
        try:
            base_url = normalize_loom_base_url(self._cfg.base_url)
        except ValueError as e:
            self._init_error = str(e)
            logger.error("Loom memory backend not started: %s", e)
            return False
        headers = {"Accept": "application/json", "User-Agent": "nerve-loom-bridge"}
        if self._cfg.token:
            headers["Authorization"] = f"Bearer {self._cfg.token}"
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            timeout=httpx.Timeout(self._timeouts["read"], connect=_CONNECT_TIMEOUT_S),
            verify=self._cfg.verify_tls,
            transport=self._transport,
        )
        self._base_url = base_url
        self._available = True
        self._metrics.initialized_at = datetime.now(UTC).isoformat()
        self._metrics.service_available = True
        try:
            ident = await self._identity()
        except MemoryBackendUnavailable as e:
            logger.warning("Loom memory backend at %s: %s; retrying on first use", base_url, e)
        else:
            logger.info(
                "Loom memory backend at %s: org %s, namespace %s (from %s), agent %s",
                base_url, ident.org, ident.namespace, ident.source, self._cfg.agent_name,
            )
        return True

    async def shutdown(self) -> None:
        """Close the HTTP client. Idempotent."""
        for task in list(self._background):
            task.cancel()
        if self._background:
            await asyncio.gather(*self._background, return_exceptions=True)
        self._background.clear()
        client, self._client = self._client, None
        if client is not None:
            try:
                await client.aclose()
            except Exception as e:  # noqa: BLE001 — shutdown must finish  # pragma: no cover
                logger.debug("Closing the Loom client: %s", e)
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
        """Loom's base URL, which also serves its dashboard, for the UI to link to."""
        return self._base_url or self._cfg.base_url

    @staticmethod
    def _release_memory() -> None:
        """No-op: nothing is held in process (the sweep calls this after memU)."""

    # ------------------------------------------------------------------ #
    # Identity: which org and namespace the requests address
    # ------------------------------------------------------------------ #

    async def _identity(self) -> LoomIdentity:
        """The identity, resolved once per process and again after a 401/403."""
        ident = self._ident
        if ident is None:
            async with self._ident_lock:
                ident = self._ident
                if ident is None:
                    if self._ident_error and time.monotonic() < self._ident_retry_at:
                        raise MemoryBackendUnavailable(
                            f"memory backend unavailable: {self._ident_error}"
                        )
                    try:
                        ident = await self._resolve_identity()
                    except MemoryBackendUnavailable as e:
                        self._ident_error = str(e).removeprefix("memory backend unavailable: ")
                        self._ident_retry_at = time.monotonic() + _IDENTITY_RETRY_S
                        raise
                    self._ident, self._ident_error = ident, ""
        self._seed_categories_later()
        return ident

    def _forget_identity(self) -> None:
        self._ident = None
        self._ident_error = ""
        self._ident_retry_at = 0.0

    async def _resolve_identity(self) -> LoomIdentity:
        """Explicit settings first, then Loom's ``GET /v1/whoami``, then the
        token's own claims. Raises naming every step that came up empty.

        For an agent token Loom lists the agent's own namespace first, which
        is where its memory lives unless ``memory.loom.namespace`` says
        otherwise. Nothing here assumes how namespaces are named.
        """
        org = _usable(self._cfg.org)
        namespace = _usable(self._cfg.namespace)
        if org and namespace:
            return LoomIdentity(org, namespace, (namespace,), "config")
        tried: list[str] = []
        for source in ("whoami", "token"):
            claims = await self._whoami(tried) if source == "whoami" else self._token_claims(tried)
            if claims is None:
                continue
            names = tuple(n for n in (_usable(x) for x in claims.get("namespaces") or ()) if n)
            found_org = org or _usable(claims.get("org"))
            found_ns = namespace or (names[0] if names else "")
            if found_org and found_ns:
                return LoomIdentity(found_org, found_ns, names or (found_ns,), source)
            tried.append(
                "GET /v1/whoami named no usable org or namespace"
                if source == "whoami" else "the token names no usable org or namespace"
            )
        raise MemoryBackendUnavailable(
            "memory backend unavailable: cannot tell which Loom org and namespace to use: "
            + "; ".join(tried)
            + f". Set memory.loom.org and memory.loom.namespace (or {LOOM_ORG_ENV} "
            f"and {LOOM_NAMESPACE_ENV})."
        )

    async def _whoami(self, tried: list[str]) -> dict[str, Any] | None:
        client = self._client
        if client is None:
            tried.append("the HTTP client is not running")
            return None
        timeout = httpx.Timeout(self._timeouts["read"], connect=_CONNECT_TIMEOUT_S)
        try:
            resp = await client.get(
                "/v1/whoami", headers={"X-Request-Id": str(uuid.uuid4())}, timeout=timeout,
            )
        except httpx.RequestError as e:
            tried.append(f"GET /v1/whoami failed ({type(e).__name__})")
            return None
        if resp.status_code != 200:
            tried.append(f"GET /v1/whoami answered HTTP {resp.status_code}")
            return None
        try:
            data = resp.json()
        except ValueError:
            data = None
        if not isinstance(data, dict):
            tried.append("GET /v1/whoami did not answer a JSON object")
            return None
        if not data.get("authenticated"):
            reason = str(data.get("reason") or "").strip()
            tried.append(
                "GET /v1/whoami: Loom did not accept the token" + (f" ({reason})" if reason else "")
            )
            return None
        return data

    def _token_claims(self, tried: list[str]) -> dict[str, Any] | None:
        claims = _jwt_claims(self._cfg.token)
        if claims is None:
            tried.append("the token is not a JWT" if self._cfg.token else "no token is configured")
        return claims

    # ------------------------------------------------------------------ #
    # Transport
    # ------------------------------------------------------------------ #

    async def _op(
        self,
        op: str,
        fields: dict[str, Any],
        *,
        kind: str = "read",
        org_only: bool = False,
        idempotency_key: str | None = None,
        attempts: int = _MAX_ATTEMPTS,
    ) -> dict[str, Any]:
        """``POST /v1/<op>`` with ``org`` and ``namespace`` filled in.

        ``fields`` may name another ``namespace`` (a row that lives in one);
        ``org_only`` leaves the namespace out for ops addressed by id.
        """
        if self._client is None or not self._available:
            raise MemoryBackendUnavailable("memory backend unavailable: the Loom bridge is not running")
        if time.monotonic() < self._down_until:
            wait = self._down_until - time.monotonic()
            raise MemoryBackendUnavailable(
                f"memory backend unavailable: {self._down_reason} (not retried for another {wait:.0f}s)"
            )
        for refused_once in (False, True):
            ident = await self._identity()
            body: dict[str, Any] = {"org": ident.org}
            if not org_only:
                body["namespace"] = ident.namespace
            body.update(fields)
            body.setdefault("consumer", CONSUMER)
            try:
                return await self._send(
                    op, body, kind=kind, idempotency_key=idempotency_key, attempts=attempts,
                )
            except _AuthRefused as e:
                if not refused_once:
                    # The token may have moved on (a renewed agent token, a
                    # revoked grant): learn the identity again, once.
                    self._forget_identity()
                    continue
                raise MemoryBackendUnavailable(
                    f"memory backend unavailable: Loom refused the credential on {op} "
                    f"(HTTP {e.status} {e.code})" + (f": {e.message}" if e.message else "")
                ) from None
        raise AssertionError("unreachable")  # pragma: no cover

    async def _send(
        self,
        op: str,
        body: dict[str, Any],
        *,
        kind: str,
        idempotency_key: str | None,
        attempts: int,
    ) -> dict[str, Any]:
        """One operation with bounded retries.

        A request the server never received (no connection) is resent for
        any op, and so is one Loom refused before running it (429, a 503
        AUTH_UNAVAILABLE/OVERLOADED). A timeout, a cut-off response or
        another 5xx is resent only for an idempotent op, since the server may
        have acted. Whatever is still failing then raises
        ``MemoryBackendUnavailable``.
        """
        client = self._client
        assert client is not None
        idempotent = op in _IDEMPOTENT_OPS
        timeout = self._timeouts[kind]
        http_timeout = httpx.Timeout(timeout, connect=min(timeout, _CONNECT_TIMEOUT_S))
        failure = f"{op} failed"
        # Only an unreachable or silent Loom trips the breaker: those are the
        # failures that cost every caller a timeout. An HTTP error comes back
        # fast and may concern this one operation (a slow embedder, say).
        unreachable = False
        for attempt in range(1, attempts + 1):
            headers = {"X-Request-Id": str(uuid.uuid4())}
            if idempotency_key and idempotent:
                headers["Idempotency-Key"] = idempotency_key
            hint: float | None = None
            try:
                resp = await client.post(f"/v1/{op}", json=body, headers=headers, timeout=http_timeout)
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as e:
                retry, unreachable = True, True
                failure = f"cannot reach Loom at {self._base_url} ({type(e).__name__})"
            except httpx.TimeoutException:
                retry, unreachable = idempotent, True
                failure = f"Loom did not answer {op} within {timeout:g}s"
            except httpx.RequestError as e:
                retry, unreachable = idempotent, True
                failure = f"the connection to Loom broke during {op} ({type(e).__name__})"
            else:
                status = resp.status_code
                if status < 300:
                    self._down_until = 0.0
                    self._note_ignored_args(op, resp)
                    return self._json_object(op, resp)
                code, message = _error_detail(resp)
                if status in (401, 403):
                    raise _AuthRefused(status, code, message)
                if status in (408, 429) or (status == 503 and code in _NEVER_RAN_CODES):
                    retry, unreachable = True, False
                elif status >= 500:
                    retry, unreachable = idempotent, False
                else:
                    raise LoomRequestError(op, status, code, message)
                failure = f"Loom answered {op} with HTTP {status} {code}" + (
                    f": {message}" if message else ""
                )
                hint = _retry_after(resp)
            if not retry or attempt >= attempts:
                break
            await asyncio.sleep(self._backoff(attempt, hint))
        if unreachable:
            self._down_until = time.monotonic() + _BREAKER_S
            self._down_reason = failure
        raise MemoryBackendUnavailable(f"memory backend unavailable: {failure}")

    def _backoff(self, attempt: int, hint: float | None) -> float:
        if hint is not None:
            return min(hint, _MAX_RETRY_AFTER_S)
        delay = self._backoff_base_s * (2 ** (attempt - 1))
        return min(delay * random.uniform(0.75, 1.25), _MAX_RETRY_AFTER_S)

    @staticmethod
    def _json_object(op: str, resp: httpx.Response) -> dict[str, Any]:
        try:
            data = resp.json()
        except ValueError:
            data = None
        if not isinstance(data, dict):
            raise MemoryBackendUnavailable(
                f"memory backend unavailable: Loom answered {op} with a body that is not "
                f"a JSON object (HTTP {resp.status_code})"
            )
        return data

    def _note_ignored_args(self, op: str, resp: httpx.Response) -> None:
        """Log, once per op, fields Loom says it did not read: API drift."""
        ignored = resp.headers.get("X-Loom-Ignored-Args")
        if ignored and op not in self._ignored_noted:
            self._ignored_noted.add(op)
            logger.warning("Loom ignored fields of %s: %s", op, ignored)

    async def _tracked(self, operation: str, description: str, work: Awaitable[Any]) -> Any:
        op_id = self._metrics.begin_op(operation, description)
        try:
            result = await work
        except BaseException as e:
            self._metrics.end_op(op_id, success=False, error=str(e) or type(e).__name__)
            raise
        self._metrics.end_op(op_id, success=True)
        return result

    async def _audit(
        self,
        action: str,
        target_type: str,
        target_id: str | None = None,
        source: str = "bridge",
        details: dict | None = None,
    ) -> None:
        """Best-effort row in the memory audit log (the web Log tab)."""
        if not self._audit_db:
            return
        try:
            await self._audit_db.log_audit(action, target_type, target_id, source, details)
        except Exception as e:  # noqa: BLE001 — the audit log never fails a write
            logger.warning("Audit log failed: %s", e)

    def _display_id(self, memory_key: str, namespace: str) -> str:
        """The id the tools print: the key, qualified as ``<namespace>/<key>``
        when the row lives outside the home namespace."""
        home = self._ident.namespace if self._ident else ""
        if namespace and home and namespace != home:
            return f"{namespace}/{memory_key}"
        return memory_key

    def _key_candidates(self, ref: str) -> list[tuple[str | None, str]]:
        """Where an id from the tools may live: the whole id as a key in the
        home namespace, then ``<namespace>/<key>``. ``None`` = home."""
        out: list[tuple[str | None, str]] = [(None, ref)]
        namespace, sep, key = ref.partition("/")
        home = self._ident.namespace if self._ident else ""
        if sep and key and _NAMESPACE_RE.fullmatch(namespace) and namespace != home:
            out.append((namespace, key))
        return out

    # ------------------------------------------------------------------ #
    # Recall
    # ------------------------------------------------------------------ #

    async def recall(
        self, query: str, limit: int = 10, category_limit: int = 5,
    ) -> list[dict[str, str]]:
        """Items (``{id, type, summary}``) first, then category breadcrumbs
        (``{id: "cat:<id>", type: "category", name, summary}``) — memU's shape.

        An abstaining search is no items. A failing category lookup costs
        the breadcrumbs only; a failing search raises.
        """
        if not self._available:
            return []
        try:
            return await self._tracked(
                "recall", query[:80], self._recall(query, int(limit), int(category_limit)),
            )
        except LoomRequestError as e:
            logger.error("Loom recall refused: %s", e)
            return []

    async def _recall(self, query: str, limit: int, category_limit: int) -> list[dict[str, str]]:
        items, cats = await asyncio.gather(
            self._search_items(query, limit),
            self._search_categories(query, category_limit),
            return_exceptions=True,
        )
        if isinstance(items, BaseException):
            raise items
        if isinstance(cats, BaseException):
            logger.warning("Loom category lookup failed during recall: %s", cats)
            cats = []
        return items[:limit] + cats[:category_limit]

    async def _search_items(self, query: str, limit: int) -> list[dict[str, str]]:
        if limit <= 0:
            return []
        data = await self._op("memory.search", {
            "agent": "*",
            "query": query,
            "recipe": "fast",
            "namespace_scope": "all_mine",
            "top_k": min(limit, 500),
            "include_tags": True,
            "bodies": "full",
            "explain": False,
        })
        if data.get("abstain"):
            return []
        items: list[dict[str, str]] = []
        seen: set[str] = set()
        for hit in data.get("results") or []:
            if not isinstance(hit, dict):
                continue
            # A verbatim chunk of a long value answers for the fact it came from.
            key = str(hit.get("parent_memory_key") or hit.get("memory_key") or "")
            summary = _text(hit.get("content_excerpt"))
            if not key or not summary:
                continue
            item_id = self._display_id(key, str(hit.get("namespace") or ""))
            if item_id in seen:
                continue
            seen.add(item_id)
            items.append({
                "id": item_id,
                "type": _nerve_type(hit.get("tags"), key, hit.get("memory_type")),
                "summary": summary,
            })
        return items

    async def _search_categories(self, query: str, category_limit: int) -> list[dict[str, str]]:
        if category_limit <= 0:
            return []
        data = await self._op("namespace.search_categories", {
            "query": query,
            "top_k": min(category_limit, 20),
        })
        cats: list[dict[str, str]] = []
        for hit in data.get("results") or []:
            if not isinstance(hit, dict):
                continue
            cat_id = str(hit.get("category_id") or "")
            name = str(hit.get("name") or "")
            crumb = category_breadcrumb(
                name, str(hit.get("description") or ""), str(hit.get("summary_excerpt") or ""),
            )
            if cat_id and (crumb or name):
                cats.append({"id": f"cat:{cat_id}", "type": "category", "name": name, "summary": crumb})
        return cats

    # ------------------------------------------------------------------ #
    # Categories
    # ------------------------------------------------------------------ #

    async def list_categories(self, namespace: str | None = None) -> list[dict[str, Any]]:
        """The categories registered in a namespace (home by default)."""
        fields = {"namespace": namespace} if namespace else {}
        data = await self._op("namespace.list_categories", fields)
        return [
            {
                "id": str(c.get("category_id") or ""),
                "name": str(c.get("name") or ""),
                "description": str(c.get("description") or ""),
                "item_count": int(c.get("item_count") or 0),
            }
            for c in data.get("categories") or []
            if isinstance(c, dict)
        ]

    async def _find_category(self, ref: str) -> dict[str, Any] | None:
        """A category by id, else by exact name (case-insensitive)."""
        cats = await self.list_categories()
        for cat in cats:
            if cat["id"] == ref:
                return cat
        lowered = ref.lower()
        for cat in cats:
            if cat["name"].lower() == lowered:
                return cat
        return None

    async def expand_category(
        self, category_id: str, query: str = "", limit: int = 20,
    ) -> dict[str, Any]:
        """``{name, total, items}`` for a ``cat:<id>`` breadcrumb, newest first.

        Members are the rows tagged with the category's name: Loom's
        extractor tags facts with the registered category names, and
        ``update_item`` tags what it assigns. ``name`` is None for an unknown
        category; an outage raises.
        """
        empty: dict[str, Any] = {"name": None, "total": 0, "items": []}
        if not self._available:
            return empty
        ref = category_id.removeprefix("cat:")
        ref = ref.strip()
        if not ref:
            return empty
        try:
            return await self._tracked(
                "expand_category", ref, self._expand_category(ref, query, int(limit)),
            )
        except LoomRequestError as e:
            logger.error("Loom expand_category refused for %s: %s", ref, e)
            return empty

    async def _expand_category(self, ref: str, query: str, limit: int) -> dict[str, Any]:
        cat = await self._find_category(ref)
        if cat is None:
            return {"name": None, "total": 0, "items": []}
        fields: dict[str, Any] = {
            "agent": "*",
            "user_id": "*",
            "session_id": "*",
            "tags": [cat["name"]],
            "sort_by": "recency",
            "limit": max(1, min(limit, 1000)),
        }
        if query:
            fields["contains"] = query
        data = await self._op("memory.list", fields)
        items = []
        for entry in data.get("entries") or []:
            row = self._row_from_entry(entry)
            if row:
                items.append({"id": row["id"], "type": row["type"], "summary": row["summary"]})
        return {"name": cat["name"], "total": int(data.get("total") or len(items)), "items": items}

    def _row_from_entry(self, entry: Any) -> dict[str, Any] | None:
        """A memory.list entry as the dict the date tools print."""
        if not isinstance(entry, dict):
            return None
        key = str(entry.get("memory_key") or "")
        summary = _text(entry.get("value"))
        if not key or not summary:
            return None
        return {
            "id": self._display_id(key, ""),
            "type": _nerve_type(entry.get("tags"), key, entry.get("memory_type")),
            "summary": summary,
            "created_at": entry.get("created_at"),
            "updated_at": entry.get("updated_at"),
        }

    async def create_category(self, name: str, description: str, source: str = "bridge") -> bool:
        """Register a category. An existing one is left as it is (its summary
        too), as memU's create does."""
        name = (name or "").strip()
        if not self._available or not name:
            return False
        return await self._tracked(
            "categories", f"create {name}", self._create_category(name, description, source),
        )

    async def _create_category(self, name: str, description: str, source: str) -> bool:
        if any(c["name"].lower() == name.lower() for c in await self.list_categories()):
            return True
        await self._register_category(name, description or "", "")
        await self._audit("category_created", "category", name, source, {"description": description})
        return True

    async def _register_category(self, name: str, description: str, summary: str) -> str:
        data = await self._op("namespace.register_category", {
            "name": name, "description": description, "summary": summary,
        }, kind="write")
        if not data.get("embedded", True):
            logger.warning(
                "Loom stored category %r without an embedding; recall will not show it "
                "until it is registered again",
                name,
            )
        return str(data.get("category_id") or "")

    async def update_category(
        self,
        category_id: str,
        summary: str | None = None,
        description: str | None = None,
        source: str = "bridge",
    ) -> bool:
        """Replace a category's summary and/or description (Loom re-embeds).

        Loom stores a category whole, so the field not being changed is sent
        back as it is. Its summary can only be read back up to 280
        characters: a description-only update of a longer summary is refused
        rather than truncating it.
        """
        if not self._available:
            return False
        ref = category_id.removeprefix("cat:")
        ref = ref.strip()
        if not ref or (summary is None and description is None):
            return False
        return await self._tracked(
            "categories", f"update {ref}",
            self._update_category(ref, summary, description, source),
        )

    async def _update_category(
        self, ref: str, summary: str | None, description: str | None, source: str,
    ) -> bool:
        cat = await self._find_category(ref)
        if cat is None:
            return False
        new_description = description if description is not None else cat["description"]
        if summary is None:
            summary = await self._current_summary(cat)
        await self._register_category(cat["name"], new_description, summary)
        await self._audit("category_updated", "category", cat["id"], source, {
            "summary_changed": summary is not None,
            "description_changed": description is not None,
        })
        return True

    async def _current_summary(self, cat: dict[str, Any]) -> str:
        data = await self._op("namespace.search_categories", {"query": cat["name"], "top_k": 20})
        for hit in data.get("results") or []:
            if isinstance(hit, dict) and hit.get("category_id") == cat["id"]:
                excerpt = str(hit.get("summary_excerpt") or "")
                if len(excerpt) < _SUMMARY_EXCERPT_CHARS:
                    return excerpt
                raise ValueError(
                    f"category {cat['name']!r} has a summary longer than Loom returns "
                    f"({_SUMMARY_EXCERPT_CHARS} characters), so updating only its "
                    "description would cut the summary short; pass the summary as well"
                )
        # Not embedded (search never returns it) or no summary: nothing to keep.
        return ""

    async def apply_categories(self, categories: list[Any]) -> bool:
        """Register the configured categories that Loom does not have yet.

        What a restart does with ``memory.categories``, without one (the setup
        page's Mode step). Existing categories are left alone.
        """
        if not self._available:
            return False
        return await self._tracked(
            "categories", "apply", self._ensure_categories(list(categories)),
        )

    async def _ensure_categories(self, categories: list[Any]) -> bool:
        if not categories:
            return True
        existing = {c["name"].lower() for c in await self.list_categories()}
        ok = True
        for cat in categories:
            name = str(getattr(cat, "name", "") or "").strip()
            if not name or name.lower() in existing:
                continue
            try:
                await self._register_category(name, str(getattr(cat, "description", "") or ""), "")
            except (MemoryBackendUnavailable, LoomRequestError) as e:
                logger.warning("Could not register memory category %s in Loom: %s", name, e)
                ok = False
            else:
                existing.add(name.lower())
        return ok

    def _seed_categories_later(self) -> None:
        """Register ``memory.categories`` in the background once the identity
        is known, as memU seeds them at startup; a failed attempt is made
        again on a later call, at most every few minutes."""
        if (
            self._seeded
            or not self.config.memory.categories
            or time.monotonic() < self._seed_retry_at
        ):
            return
        self._seeded = True
        task = asyncio.ensure_future(self._seed_categories())
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _seed_categories(self) -> None:
        try:
            ok = await self._ensure_categories(list(self.config.memory.categories))
        except (MemoryBackendUnavailable, LoomRequestError) as e:
            logger.warning("Seeding memory categories in Loom failed: %s", e)
            ok = False
        if not ok:
            self._seeded = False
            self._seed_retry_at = time.monotonic() + _SEED_RETRY_S

    # ------------------------------------------------------------------ #
    # Writes
    # ------------------------------------------------------------------ #

    async def memorize_fact(
        self,
        content: str,
        memory_type: str = "knowledge",
        *,
        session_id: str | None = None,
        source: str = "agent_tool",
    ) -> bool:
        """Store one fact with ``memory.set`` — the ``memorize`` tool.

        True when Loom took it (a restatement Loom merged into an existing
        row counts). Raises ``MemoryBackendUnavailable`` on an outage and
        ``LoomRequestError`` when Loom refuses the value.
        """
        if not self._available:
            return False
        kind = _clean_type(memory_type)
        key = f"nerve_{kind}_{uuid.uuid4().hex[:20]}"
        return await self._tracked(
            "memorize", key, self._memorize_fact(key, kind, content, session_id, source),
        )

    async def _memorize_fact(
        self, key: str, kind: str, content: str, session_id: str | None, source: str,
    ) -> bool:
        fields: dict[str, Any] = {
            "agent": self._cfg.agent_name,
            "memory_key": key,
            "memory_type": "procedural" if kind in _PROCEDURAL_TYPES else "semantic",
            "value": content,
            "tags": [NERVE_TAG, f"{_TYPE_TAG_PREFIX}{kind}"],
        }
        if session_id:
            fields["session_id"] = session_id
        if kind == "event":
            # The tool has no date argument: an event is dated when it is saved.
            fields["valid_at"] = _iso_utc(datetime.now(UTC))
        data = await self._op(
            "memory.set", fields, kind="write", idempotency_key=str(uuid.uuid4()),
        )
        stored = str(data.get("merged_into_key") or data.get("memory_key") or key)
        await self._audit("item_created", "item", stored, source, {
            "memory_type": kind, "verdict": str(data.get("conflict_verdict") or ""),
        })
        return True

    async def memorize_file(
        self, file_path: str, modality: str = "document", source: str = "bridge",
    ) -> bool:
        """memU's file entry point, for callers that have a file in hand.

        A ``conversation`` file is the JSON array memU's sweep writes; a
        document is one fact, optionally ``"<type>: <content>"`` as the
        ``memorize`` tool used to write it.
        """
        if not self._available:
            return False
        path = Path(file_path)
        text = await asyncio.to_thread(path.read_text, encoding="utf-8")
        if modality == "conversation":
            messages = json.loads(text)
            if not isinstance(messages, list):
                raise ValueError(f"{file_path}: a conversation file holds a JSON array")
            match = re.fullmatch(r"session-(.+)-\d+", path.stem)
            return await self.memorize_conversation(match.group(1) if match else "-", messages)
        kind, sep, rest = text.partition(":")
        if sep and _TYPE_RE.fullmatch(kind.strip().lower()) and rest.strip():
            return await self.memorize_fact(rest.strip(), kind.strip(), source=source)
        return await self.memorize_fact(text.strip(), "knowledge", source=source)

    async def memorize_conversation(self, session_id: str, messages: list[dict]) -> bool:
        """Queue a session window for extraction (``memory.set_from_messages``,
        ``defer=true``): Loom extracts the facts in the background.

        Cut into batches of at most 400 messages. RAISES on any failure — the
        sweep advances its watermark unless this raises, and a window that
        was not queued must be offered again. False only when there is
        nothing to send.
        """
        if not messages or not self._available:
            return False
        turns = [t for t in (self._turn(m) for m in messages) if t]
        if not turns:
            return False
        return await self._tracked(
            "memorize_conversation", f"session {session_id}",
            self._memorize_conversation(session_id or "-", turns),
        )

    @staticmethod
    def _turn(message: dict) -> dict[str, Any] | None:
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            return None
        if len(content) > _MESSAGE_MAX_CHARS:
            content = content[:_MESSAGE_MAX_CHARS] + " […truncated]"
        turn: dict[str, Any] = {"role": str(message.get("role") or "user"), "content": content}
        stamp = _parse_dt(message.get("created_at"))
        if stamp is not None:
            # Loom anchors what a turn says ("yesterday") to when it was said.
            turn["timestamp"] = _iso_utc(stamp)
        return turn

    @staticmethod
    def _batches(turns: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
        batches: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        size = 0
        for turn in turns:
            turn_size = len(turn["content"].encode("utf-8")) + 128
            if current and (len(current) >= _BATCH_MAX_MESSAGES or size + turn_size > _BATCH_MAX_BYTES):
                batches.append(current)
                current, size = [], 0
            current.append(turn)
            size += turn_size
        if current:
            batches.append(current)
        return batches

    async def _memorize_conversation(self, session_id: str, turns: list[dict[str, Any]]) -> bool:
        queued: list[str] = []
        previous: list[dict[str, Any]] = []
        batches = self._batches(turns)
        for batch in batches:
            digest = hashlib.sha256(
                json.dumps([session_id, batch], ensure_ascii=False, sort_keys=True).encode("utf-8"),
            ).hexdigest()
            if not self._was_accepted(digest):
                fields: dict[str, Any] = {
                    "agent": self._cfg.agent_name,
                    "session_id": session_id,
                    "messages": batch,
                    "defer": True,
                    "extra_tags": [EXTRACTION_TAG],
                    "observation_date": self._observation_date(batch),
                }
                if previous:
                    fields["last_messages"] = previous
                data = await self._op("memory.set_from_messages", fields, kind="extract")
                self._remember_accepted(digest)
                queued.append(str(data.get("queue_id") or ""))
            # Context for the next batch only (not extracted again): kept short.
            previous = [
                {**turn, "content": turn["content"][:_CONTEXT_MAX_CHARS]}
                for turn in batch[-_CONTEXT_MESSAGES:]
            ]
        await self._audit("conversation_indexed", "resource", session_id, "bridge", {
            "message_count": len(turns),
            "batches": len(batches),
            "queued": len(queued),
        })
        return True

    @staticmethod
    def _observation_date(batch: list[dict[str, Any]]) -> str:
        """The day a batch was said (its last turn), for relative dates."""
        for turn in reversed(batch):
            stamp = turn.get("timestamp")
            if stamp:
                return str(stamp)[:10]
        return datetime.now(UTC).strftime("%Y-%m-%d")

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
    # Update and delete
    # ------------------------------------------------------------------ #

    async def _read_row(self, ref: str) -> dict[str, Any] | None:
        """The live row an id from the tools names: a Loom memory id (UUID),
        a key in the home namespace, or ``<namespace>/<key>``."""
        if _UUID_RE.fullmatch(ref):
            hit = await self._op("memory.get_by_id", {"memory_id": ref}, org_only=True)
            if not hit.get("found"):
                return None
            located: list[tuple[dict[str, Any], str | None]] = [({
                "agent": hit.get("agent") or "*",
                "user_id": hit.get("user_id") or "-",
                "session_id": hit.get("session_id") or "-",
                "memory_key": hit.get("memory_key") or "",
            }, str(hit.get("namespace") or "") or None)]
        else:
            await self._identity()  # the home namespace decides the candidates
            located = [
                ({"agent": self._cfg.agent_name, "memory_key": key}, namespace)
                for namespace, key in self._key_candidates(ref)
            ]
        for fields, namespace in located:
            if namespace:
                fields["namespace"] = namespace
            got = await self._op("memory.get", fields)
            if got.get("found"):
                return {
                    "memory_id": str(got.get("memory_id") or ""),
                    "memory_key": fields["memory_key"],
                    "namespace": namespace,
                    "tags": [str(t) for t in got.get("tags") or []],
                    "sha256": str(got.get("content_sha256") or ""),
                }
        return None

    async def update_item(
        self,
        memory_id: str,
        content: str | None = None,
        memory_type: str | None = None,
        categories: list[str] | None = None,
        source: str = "bridge",
    ) -> bool:
        """Change a memory's content, type or categories.

        The content goes through ``memory.update_by_id`` against the hash
        just read, so a concurrent write is not clobbered (one re-read and
        retry). The type is the ``nerve:<type>`` tag. ``categories`` replaces
        the row's categories by name, as memU does — unknown names are
        ignored — through both a tag and ``memory.assign_category``. False
        when no such memory exists.
        """
        if not self._available or (content is None and memory_type is None and categories is None):
            return False
        return await self._tracked(
            "update_item", memory_id,
            self._update_item(memory_id.strip(), content, memory_type, categories, source),
        )

    async def _update_item(
        self,
        ref: str,
        content: str | None,
        memory_type: str | None,
        categories: list[str] | None,
        source: str,
    ) -> bool:
        for attempt in (1, 2):
            row = await self._read_row(ref)
            if row is None or not row["memory_id"]:
                return False
            tags = list(row["tags"])
            if memory_type is not None:
                tags = [t for t in tags if _type_tag(t) is None]
                tags += [NERVE_TAG, f"{_TYPE_TAG_PREFIX}{_clean_type(memory_type)}"]
            wanted: list[str] = []
            dropped: list[str] = []
            if categories is not None:
                registered = {
                    c["name"].lower(): c["name"] for c in await self.list_categories(row["namespace"])
                }
                wanted = _dedupe([registered[n.strip().lower()] for n in categories
                                  if n.strip().lower() in registered])
                keep = {w.lower() for w in wanted}
                dropped = [t for t in tags if t.lower() in registered and t.lower() not in keep]
                tags = [t for t in tags if t.lower() not in registered] + [w.lower() for w in wanted]
            tags = _dedupe(tags)
            fields: dict[str, Any] = {"memory_id": row["memory_id"]}
            if content is not None:
                fields["value"] = content
            if {t.lower() for t in tags} != {t.lower() for t in row["tags"]}:
                fields["tags"] = tags
            if len(fields) > 1:
                if row["sha256"]:
                    fields["if_match_sha256"] = row["sha256"]
                try:
                    data = await self._op(
                        "memory.update_by_id", fields, kind="write", org_only=True,
                        idempotency_key=str(uuid.uuid4()),
                    )
                except LoomRequestError as e:
                    if e.code == "PRECONDITION_FAILED" and attempt == 1:
                        continue  # written meanwhile: read it again
                    raise
                if not data.get("updated"):
                    return False
            for name, tombstone in [(w, False) for w in wanted] + [(d, True) for d in dropped]:
                link: dict[str, Any] = {
                    "memory_key": row["memory_key"], "category_name": name, "tombstone": tombstone,
                }
                if row["namespace"]:
                    link["namespace"] = row["namespace"]
                await self._op(
                    "memory.assign_category", link, kind="write", idempotency_key=str(uuid.uuid4()),
                )
            await self._audit("item_updated", "item", ref, source, {
                "content_changed": content is not None,
                "type_changed": memory_type is not None,
                "categories_changed": categories is not None,
            })
            return True
        return False

    async def delete_item(self, memory_id: str, source: str = "bridge") -> bool:
        """Delete a memory (Loom keeps a tombstone it can restore for 14
        days). False when no such memory exists."""
        if not self._available:
            return False
        return await self._tracked(
            "delete_item", memory_id, self._delete_item(memory_id.strip(), source),
        )

    async def _delete_item(self, ref: str, source: str) -> bool:
        if _UUID_RE.fullmatch(ref):
            hit = await self._op("memory.get_by_id", {"memory_id": ref}, org_only=True)
            if not hit.get("found"):
                return False
            targets: list[dict[str, Any]] = [{
                "namespace": str(hit.get("namespace") or ""),
                "agent": hit.get("agent") or self._cfg.agent_name,
                "user_id": hit.get("user_id") or "-",
                "session_id": hit.get("session_id") or "-",
                "memory_key": hit.get("memory_key") or "",
            }]
        else:
            # Loom resolves a key under any agent of the namespace.
            await self._identity()  # the home namespace decides the candidates
            targets = []
            for namespace, key in self._key_candidates(ref):
                target: dict[str, Any] = {"agent": self._cfg.agent_name, "memory_key": key}
                if namespace:
                    target["namespace"] = namespace
                targets.append(target)
        for target in targets:
            if not target.get("namespace"):
                target.pop("namespace", None)
            data = await self._op(
                "memory.delete", target, kind="write", idempotency_key=str(uuid.uuid4()),
            )
            if data.get("tombstoned"):
                await self._audit("item_deleted", "item", ref, source)
                return True
        return False

    # ------------------------------------------------------------------ #
    # Date tools
    # ------------------------------------------------------------------ #

    async def conversation_history(
        self, date_from: str, date_to: str = "", limit: int = 30,
    ) -> list[dict[str, Any]]:
        """Memories that happened in ``[date_from, date_to]`` (UTC days), newest
        first: ``memory.timeline`` over Nerve's rows, keeping those with a
        ``valid_at`` (Loom's "when it happened"; the extractor dates facts by
        the turn they came from)."""
        if not self._available:
            return []
        start, end = _day_range(date_from, date_to)
        return await self._tracked(
            "conversation_history", f"{date_from}..{date_to or date_from}",
            self._conversation_history(start, end, int(limit)),
        )

    async def _conversation_history(
        self, start: datetime, end: datetime, limit: int,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        data = await self._op("memory.timeline", {
            "agent": "*",
            "tags": list(_NERVE_TAGS),
            "since": _iso_utc(start),
            "until": _iso_utc(end - timedelta(seconds=1)),
            "top_k": min(_TIMELINE_MAX, max(limit * 3, 50)),
            "order": "desc",
        })
        rows: list[dict[str, Any]] = []
        for event in data.get("events") or []:
            if not isinstance(event, dict) or not event.get("valid_at"):
                continue
            key = str(event.get("memory_key") or "")
            summary = _text(event.get("content_excerpt"))
            if not key or not summary:
                continue
            rows.append({
                "id": self._display_id(key, str(event.get("namespace") or "")),
                "type": _nerve_type((), key, event.get("memory_type")),
                "summary": summary,
                "happened_at": event.get("valid_at"),
            })
            if len(rows) >= limit:
                break
        return rows

    async def records_by_date(
        self,
        date_from: str,
        date_to: str = "",
        limit: int = 100,
        include_updated: bool = False,
    ) -> list[dict[str, Any]]:
        """Memories created (and with ``include_updated``, rewritten) in
        ``[date_from, date_to]`` (UTC days), newest first.

        ``memory.list`` of Nerve's rows in the home namespace, newest first,
        at most five pages of 500. Loom cannot sort by ``updated_at``, so an
        update is caught only for rows inside the pages scanned.
        """
        if not self._available:
            return []
        start, end = _day_range(date_from, date_to)
        return await self._tracked(
            "records_by_date", f"{date_from}..{date_to or date_from}",
            self._records_by_date(start, end, int(limit), bool(include_updated)),
        )

    async def _records_by_date(
        self, start: datetime, end: datetime, limit: int, include_updated: bool,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if limit <= 0:
            return rows
        offset = 0
        for _page in range(_RECORDS_MAX_PAGES):
            data = await self._op("memory.list", {
                "agent": "*",
                "user_id": "*",
                "session_id": "*",
                "tags": list(_NERVE_TAGS),
                "sort_by": "recency",
                "limit": _RECORDS_PAGE,
                "offset": offset,
            })
            entries = data.get("entries") or []
            for entry in entries:
                row = self._row_from_entry(entry)
                created = _parse_dt(row["created_at"]) if row else None
                if row is None or created is None:
                    continue
                if created < start and not include_updated:
                    # Newest first: nothing after this was created in the window.
                    return rows
                updated = _parse_dt(row["updated_at"])
                rewritten = updated is not None and updated != created
                updated_in_window = (
                    include_updated
                    and updated is not None
                    and rewritten
                    and start <= updated < end
                    and updated.date() != created.date()
                )
                if start <= created < end or updated_in_window:
                    row["updated"] = bool(include_updated and rewritten)
                    rows.append(row)
                    if len(rows) >= limit:
                        return rows
            if not entries or not data.get("truncated"):
                break
            offset += len(entries)
        return rows

    # ------------------------------------------------------------------ #
    # Health
    # ------------------------------------------------------------------ #

    async def get_db_stats(self) -> dict[str, Any]:
        """Counts for the diagnostics page, from ``namespace.summary`` and
        ``namespace.list_categories`` (home namespace; one attempt each)."""
        stats: dict[str, Any] = {
            "total_items": 0,
            "total_categories": 0,
            "total_resources": 0,
            "type_distribution": {},
            "events_missing_happened_at": 0,
            "db_size_mb": 0,
        }
        if not self._available:
            return stats
        summary = await self._op("namespace.summary", {"top_entities_limit": 0}, attempts=1)
        stats["total_items"] = int(summary.get("total_memories") or 0)
        stats["type_distribution"] = {
            str(row.get("memory_type")): int(row.get("n") or 0)
            for row in summary.get("type_counts") or []
            if isinstance(row, dict)
        }
        data = await self._op("namespace.list_categories", {}, attempts=1)
        stats["total_categories"] = len(data.get("categories") or [])
        return stats

    async def get_health(self) -> dict[str, Any]:
        """The diagnostics snapshot, in memU's shape plus ``backend``,
        ``dashboard_url`` (Loom's URL) and ``loom`` (identity and last error)."""
        result = self._metrics.to_dict()
        ident = self._ident
        info: dict[str, Any] = {
            "base_url": self.dashboard_url,
            "org": ident.org if ident else self._cfg.org,
            "namespace": ident.namespace if ident else self._cfg.namespace,
            "agent": self._cfg.agent_name,
            "identity_source": ident.source if ident else "",
            "error": self._init_error or self._ident_error,
        }
        reachable = False
        try:
            result["database"] = await self.get_db_stats()
            reachable = self._available
        except (MemoryBackendUnavailable, LoomRequestError) as e:
            result["database"] = {
                "total_items": 0, "total_categories": 0, "total_resources": 0,
                "type_distribution": {}, "events_missing_happened_at": 0, "db_size_mb": 0,
            }
            info["error"] = str(e)
        result["service_available"] = bool(self._available and reachable)
        result["backend"] = self.backend
        result["dashboard_url"] = self.dashboard_url
        result["loom"] = info
        return result
