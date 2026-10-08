"""MCP servers from the catalog of an MCP gateway, for external mode.

In external mode (``NERVE_AUTH_MODE=external``), an MCP gateway decides which
MCP servers and tools the agent can use. Nerve reads the agent's catalog from
``GET <mcp_gateway_url>/catalog`` when it starts and before each new session.
Each catalog server becomes one HTTP MCP server of the agent clients, at
``<mcp_gateway_url>/s/<id>/mcp``. A session that is running keeps the servers
that it started with.

The gateway identifies the agent from the connection, so the request carries
no credential. The catalog holds no upstream address and no secret.

When the gateway cannot give a catalog, Nerve keeps the catalog that it
applied last (none after startup) and tries again in the background, with an
interval that doubles after each failure up to a limit.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import ssl
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import httpx

logger = logging.getLogger(__name__)

# The wire contract of GET /catalog, schema version 1.
CATALOG_PATH = "/catalog"
CATALOG_SCHEMA_VERSION = 1
SERVER_ID_RE = re.compile(r"^[a-z]([a-z0-9-]{0,30}[a-z0-9])?$")
TOOL_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

# The error envelope of a response without a catalog. Only values of these
# shapes go into a log line or the diagnostics.
_REASON_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_REQUEST_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
)

# JSON-RPC error code of a policy denial at the gateway. Its message goes to
# the model unchanged.
CODE_DENIED = -32001

# The name of Nerve's own MCP server in both clients. A catalog server with
# this ID is not used, because the name is taken.
BUILTIN_SERVER_NAME = "nerve"

# Who manages a server that comes from the catalog, in the API.
MANAGED_BY_ORGANIZATION = "organization"

# The largest catalog that Nerve reads. The gateway bounds a tenant snapshot
# to 8 MiB, and a catalog holds less than its snapshot.
MAX_CATALOG_BYTES = 8 * 1024 * 1024

# One catalog request. A new session waits for it, so it is short.
FETCH_TIMEOUT = httpx.Timeout(5.0, connect=2.0)

# Interval before the first retry after a failure, and its upper limit.
RETRY_INITIAL_SECONDS = 1.0
RETRY_MAX_SECONDS = 60.0


def server_path(server_id: str) -> str:
    """Return the path of one MCP server, relative to the MCP gateway URL."""
    return f"/s/{server_id}/mcp"


class CatalogError(Exception):
    """The gateway gave no catalog, or a catalog that breaks the contract.

    The message names the problem only. It never holds the response body.
    """


@dataclass(frozen=True)
class GatewayServer:
    """One MCP server of the agent, as the catalog names it."""

    id: str
    url: str
    tools: tuple[str, ...]
    display_name: str = ""
    description: str = ""


@dataclass(frozen=True)
class GatewayCatalog:
    """The MCP servers that the agent can use, at one policy generation."""

    generation: int
    digest: str
    servers: tuple[GatewayServer, ...]


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _text(server: dict, key: str, field: str) -> str:
    value = server.get(key, "")
    if value is None:
        return ""
    if not isinstance(value, str):
        raise CatalogError(f"{field}.{key} is not a string")
    return value


def parse_catalog(payload: Any, base_url: str) -> GatewayCatalog:
    """Check a decoded catalog against the contract and return it.

    ``base_url`` is the MCP gateway URL without a trailing ``/``. The server
    URLs are ``base_url`` followed by the server path. Fields that the
    contract does not define are ignored. Raises :class:`CatalogError`.
    """
    if not isinstance(payload, dict):
        raise CatalogError("catalog is not a JSON object")
    schema = payload.get("schemaVersion")
    if not _is_int(schema) or schema != CATALOG_SCHEMA_VERSION:
        raise CatalogError("catalog has an unsupported schemaVersion")
    generation = payload.get("generation")
    if not _is_int(generation) or generation < 0:
        raise CatalogError("catalog generation is not a non-negative integer")
    digest = payload.get("digest")
    if not isinstance(digest, str) or not DIGEST_RE.fullmatch(digest):
        raise CatalogError("catalog digest is not a sha256 digest")
    raw_servers = payload.get("servers")
    if not isinstance(raw_servers, list):
        raise CatalogError("catalog servers is not a list")

    servers: list[GatewayServer] = []
    for index, server in enumerate(raw_servers):
        field = f"servers[{index}]"
        if not isinstance(server, dict):
            raise CatalogError(f"{field} is not a JSON object")
        server_id = server.get("id")
        if not isinstance(server_id, str) or not SERVER_ID_RE.fullmatch(server_id):
            raise CatalogError(f"{field}.id is not a valid server ID")
        if servers and servers[-1].id >= server_id:
            raise CatalogError(f"{field}.id is not in ascending order")
        path = server.get("path")
        if path != server_path(server_id):
            raise CatalogError(f"{field}.path does not match its ID")
        tools = server.get("tools")
        if not isinstance(tools, list) or not tools:
            raise CatalogError(f"{field}.tools is not a non-empty list")
        for tool_index, tool in enumerate(tools):
            if not isinstance(tool, str) or not TOOL_NAME_RE.fullmatch(tool):
                raise CatalogError(f"{field}.tools has an invalid tool name")
            if tool_index and tools[tool_index - 1] >= tool:
                raise CatalogError(f"{field}.tools is not sorted without duplicates")
        servers.append(GatewayServer(
            id=server_id,
            url=base_url + path,
            tools=tuple(tools),
            display_name=_text(server, "displayName", field),
            description=_text(server, "description", field),
        ))
    return GatewayCatalog(
        generation=generation, digest=digest, servers=tuple(servers),
    )


def _error_envelope(body: bytes) -> str:
    """Return the reason and request ID of an error body, when it has them."""
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return ""
    if not isinstance(data, dict):
        return ""
    parts = []
    reason = data.get("reason")
    if isinstance(reason, str) and _REASON_RE.fullmatch(reason):
        parts.append(reason)
    request_id = data.get("requestId")
    if isinstance(request_id, str) and _REQUEST_ID_RE.fullmatch(request_id):
        parts.append(f"request {request_id}")
    return ", ".join(parts)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class McpGatewayCatalog:
    """Read the agent's catalog from the MCP gateway and keep the last one.

    :meth:`start` reads the catalog once. :meth:`refresh` reads it again
    before a new session. Concurrent calls share one request. A changed
    catalog replaces the applied one and goes to ``on_applied``. A failure
    keeps the applied catalog and starts the background retry, which stops at
    the first success.
    """

    def __init__(
        self,
        base_url: str,
        *,
        on_applied: Callable[[GatewayCatalog], Awaitable[None]] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
        retry_initial: float = RETRY_INITIAL_SECONDS,
        retry_max: float = RETRY_MAX_SECONDS,
    ):
        self._base_url = base_url.rstrip("/")
        self._on_applied = on_applied
        self._transport = transport
        self._sleep = sleep
        self._retry_initial = retry_initial
        self._retry_max = retry_max
        self._client: httpx.AsyncClient | None = None
        self._applied: GatewayCatalog | None = None
        self._applied_at: str | None = None
        self._checked_at: str | None = None
        self._error: str | None = None
        self._inflight: asyncio.Task | None = None
        self._retry_task: asyncio.Task | None = None
        self._closed = False

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def applied(self) -> GatewayCatalog | None:
        """The catalog that new sessions use, or ``None`` before the first one."""
        return self._applied

    @property
    def servers(self) -> tuple[GatewayServer, ...]:
        """The servers of the applied catalog. Empty before the first one."""
        return self._applied.servers if self._applied is not None else ()

    @property
    def retrying(self) -> bool:
        return self._retry_task is not None and not self._retry_task.done()

    def status(self) -> dict[str, Any]:
        """The applied catalog and the result of the last request."""
        applied = self._applied
        return {
            "url": self._base_url,
            "generation": applied.generation if applied else None,
            "digest": applied.digest if applied else None,
            "servers": [server.id for server in applied.servers] if applied else [],
            "applied_at": self._applied_at,
            "checked_at": self._checked_at,
            "error": self._error,
            "retrying": self.retrying,
        }

    async def start(self) -> None:
        """Read the catalog once. On a failure, start the background retry."""
        await self.refresh()

    async def refresh(self) -> GatewayCatalog | None:
        """Read the catalog now and return the applied one. Never raises."""
        if self._closed:
            return self._applied
        await self._attempt()
        return self._applied

    async def close(self) -> None:
        """Stop the background retry and close the HTTP client."""
        self._closed = True
        for task in (self._retry_task, self._inflight):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except BaseException:  # noqa: BLE001 - the task is cancelled
                    pass
        self._retry_task = None
        self._inflight = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _attempt(self) -> bool:
        """Read and apply the catalog once, shared by concurrent callers."""
        task = self._inflight
        if task is None or task.done():
            task = asyncio.ensure_future(self._fetch_and_apply())
            self._inflight = task
        try:
            ok = await asyncio.shield(task)
        except asyncio.CancelledError:
            if not task.cancelled():
                raise
            return False
        if ok:
            self._stop_retry()
        else:
            self._start_retry()
        return ok

    async def _fetch_and_apply(self) -> bool:
        self._checked_at = _now()
        try:
            catalog = await self._fetch()
        except Exception as e:  # noqa: BLE001 - a failure keeps the applied catalog
            error = str(e) if isinstance(e, CatalogError) else (
                f"MCP gateway catalog request failed ({type(e).__name__})"
            )
            # One warning for each new problem; a retry that fails in the
            # same way is logged at debug level.
            log = logger.debug if error == self._error else logger.warning
            log("MCP gateway catalog not read: %s", error)
            self._error = error
            return False
        if self._error is not None:
            logger.info("MCP gateway catalog read again. Earlier problem: %s", self._error)
        self._error = None
        if catalog != self._applied:
            self._applied = catalog
            self._applied_at = _now()
            logger.info(
                "MCP gateway catalog generation %d applied for new sessions: "
                "%d server(s) (%s)",
                catalog.generation, len(catalog.servers),
                ", ".join(server.id for server in catalog.servers) or "none",
            )
            if self._on_applied is not None:
                try:
                    await self._on_applied(catalog)
                except Exception:  # noqa: BLE001 - the catalog is applied
                    logger.exception("MCP gateway catalog handler failed")
        return True

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            # The gateway is at the link end of the machine and is never
            # reached through a proxy, so the proxy environment is not read.
            # TLS uses the system trust store.
            self._client = httpx.AsyncClient(
                transport=self._transport,
                timeout=FETCH_TIMEOUT,
                follow_redirects=False,
                trust_env=False,
                verify=ssl.create_default_context(),
            )
        return self._client

    async def _fetch(self) -> GatewayCatalog:
        url = self._base_url + CATALOG_PATH
        try:
            async with self._http().stream(
                "GET", url, headers={"Accept": "application/json"},
            ) as response:
                body = await self._read_limited(response)
        except httpx.HTTPError as e:
            raise CatalogError(
                f"cannot reach the MCP gateway ({type(e).__name__})",
            ) from e
        if response.status_code != 200:
            detail = _error_envelope(body)
            suffix = f" ({detail})" if detail else ""
            raise CatalogError(
                f"MCP gateway answered HTTP {response.status_code}{suffix}",
            )
        content_type = response.headers.get("content-type", "")
        if content_type.split(";", 1)[0].strip().lower() != "application/json":
            raise CatalogError("MCP gateway catalog is not application/json")
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeDecodeError) as e:
            raise CatalogError("MCP gateway catalog is not valid JSON") from e
        return parse_catalog(payload, self._base_url)

    @staticmethod
    async def _read_limited(response: httpx.Response) -> bytes:
        chunks: list[bytes] = []
        size = 0
        async for chunk in response.aiter_bytes():
            size += len(chunk)
            if size > MAX_CATALOG_BYTES:
                raise CatalogError(
                    f"MCP gateway catalog is larger than {MAX_CATALOG_BYTES} bytes",
                )
            chunks.append(chunk)
        return b"".join(chunks)

    def _start_retry(self) -> None:
        if self._closed or self.retrying:
            return
        self._retry_task = asyncio.ensure_future(self._retry_loop())

    def _stop_retry(self) -> None:
        task = self._retry_task
        self._retry_task = None
        if task is not None and not task.done():
            task.cancel()

    async def _retry_loop(self) -> None:
        delay = self._retry_initial
        while not self._closed:
            await self._sleep(delay)
            task = self._inflight
            if task is None or task.done():
                task = asyncio.ensure_future(self._fetch_and_apply())
                self._inflight = task
            if await asyncio.shield(task):
                self._retry_task = None
                return
            delay = min(delay * 2, self._retry_max)
