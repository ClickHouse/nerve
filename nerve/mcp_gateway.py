"""MCP servers from the catalog of an MCP gateway, for external mode.

In external mode (``NERVE_AUTH_MODE=external``), an MCP gateway decides which
MCP servers the agent can use. Nerve reads ``GET <mcp_gateway_url>/catalog``
when it starts, before each new session, and at a fixed interval, and gives
each server of the catalog to the agent clients as one HTTP MCP server. A
running session gets a changed catalog at its next turn. The gateway
identifies the agent from the connection, so the request carries no
credential. The catalog holds no upstream address and no secret.

Nerve reads the members ``revision`` and ``servers`` of the catalog document,
and ``id``, ``url`` and ``revision`` of each server. It ignores other members,
for example the ``displayName`` and ``description`` of a server. A revision
is opaque: Nerve compares it for equality only. ``url`` is a URL reference,
which Nerve resolves against the URL of the catalog request (RFC 3986,
section 5).

Nerve refuses a document without these members, or with a member that is
not valid, as a whole and keeps the catalog that it read last (none after
startup). The gateway checks the policy on each call, so a catalog that is
older than the policy gives no access that the policy does not allow.

Nerve also calls tools of a catalog server for its own use
(:class:`McpGatewayServer`; the Loom memory backend's lifecycle calls). The
gateway serves each server as stateless streamable HTTP: one JSON-RPC
request per ``POST``, answered with JSON, no session to open first.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlsplit

import httpx

logger = logging.getLogger(__name__)

CATALOG_PATH = "/catalog"

# A server ID: 1 to 32 lowercase letters, digits and hyphens. It starts with a
# letter and does not end with a hyphen. The ID is the server name in both
# clients and a key in the Codex configuration overrides.
SERVER_ID_RE = re.compile(r"^[a-z]([a-z0-9-]{0,30}[a-z0-9])?$")

_CATALOG_MEMBERS = frozenset({"revision", "servers"})
_SERVER_MEMBERS = frozenset({"id", "url", "revision"})

# The time limit of one catalog request, from the connection to the last byte
# of the response. A new session waits for the request.
FETCH_TIMEOUT_SECONDS = 5.0


class CatalogError(Exception):
    """The gateway gave no catalog, or a document of another shape.

    The message names the problem only. It never holds the response body.
    """


@dataclass(frozen=True)
class GatewayServer:
    """One MCP server of the agent. ``url`` is absolute."""

    id: str
    url: str
    revision: str


@dataclass(frozen=True)
class GatewayCatalog:
    """The MCP servers that the agent can use, in ID order."""

    revision: str
    servers: tuple[GatewayServer, ...]


def _object(value: Any, members: frozenset[str], field: str) -> dict:
    if not isinstance(value, dict) or not members <= value.keys():
        raise CatalogError(
            f"catalog {field} is not an object with the members "
            + ", ".join(sorted(members))
        )
    return value


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise CatalogError(f"catalog {field} is not a non-empty string")
    return value


def _server_url(reference: str, catalog_url: str, field: str) -> str:
    """Resolve ``reference`` against the catalog URL.

    The result must have the scheme, the host and the port of the catalog
    URL, and no user information.
    """
    try:
        url = urljoin(catalog_url, reference)
        same_origin = urlsplit(url)[:2] == urlsplit(catalog_url)[:2]
    except ValueError:
        same_origin = False
    if not same_origin:
        raise CatalogError(f"catalog {field} is not a URL of the MCP gateway")
    return url


def parse_catalog(payload: Any, catalog_url: str) -> GatewayCatalog:
    """Check a decoded catalog document and return it.

    ``catalog_url`` is the URL of the catalog request. Other members are
    ignored. Raises :class:`CatalogError` for a document without the members
    that Nerve reads, or with a member that is not valid.
    """
    data = _object(payload, _CATALOG_MEMBERS, "document")
    revision = _string(data["revision"], "revision")
    if not isinstance(data["servers"], list):
        raise CatalogError("catalog servers is not a list")
    servers: list[GatewayServer] = []
    for index, raw in enumerate(data["servers"]):
        field = f"servers[{index}]"
        server = _object(raw, _SERVER_MEMBERS, field)
        server_id = server["id"]
        if not isinstance(server_id, str) or not SERVER_ID_RE.fullmatch(server_id):
            raise CatalogError(f"catalog {field}.id is not a valid server ID")
        # Strict order also refuses two servers with the same ID.
        if servers and servers[-1].id >= server_id:
            raise CatalogError(f"catalog {field}.id is not in ascending order")
        url = _string(server["url"], f"{field}.url")
        servers.append(GatewayServer(
            id=server_id,
            url=_server_url(url, catalog_url, f"{field}.url"),
            revision=_string(server["revision"], f"{field}.revision"),
        ))
    return GatewayCatalog(revision=revision, servers=tuple(servers))


class McpGatewayCatalog:
    """Read the agent's catalog from the MCP gateway and keep the last one.

    ``latest`` is the last catalog read, ``None`` before the first one.
    """

    def __init__(
        self, base_url: str, *, transport: httpx.AsyncBaseTransport | None = None,
    ):
        self.catalog_url = base_url.rstrip("/") + CATALOG_PATH
        self.latest: GatewayCatalog | None = None
        self._transport = transport
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()
        self._reads = 0  # completed or failed reads
        self._error: str | None = None

    async def refresh(self) -> GatewayCatalog | None:
        """Read the catalog now and return the last catalog read.

        A failure keeps the last catalog. One read runs at a time. A call that
        waits while another call reads returns the result of that read, so a
        call waits for at most one read. Never raises.
        """
        reads = self._reads
        async with self._lock:
            if self._reads != reads:
                return self.latest
            try:
                async with asyncio.timeout(FETCH_TIMEOUT_SECONDS):
                    catalog = await self._fetch()
            except Exception as e:  # noqa: BLE001 - a failure keeps the last catalog
                error = str(e) if isinstance(e, CatalogError) else (
                    f"MCP gateway catalog request failed ({type(e).__name__})"
                )
                # One warning for each new problem.
                log = logger.debug if error == self._error else logger.warning
                log("MCP gateway catalog not read: %s", error)
                self._error = error
                return self.latest
            finally:
                self._reads += 1
            if self._error is not None:
                logger.info("MCP gateway catalog read again")
                self._error = None
            self.latest = catalog
            return catalog

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _fetch(self) -> GatewayCatalog:
        if self._client is None:
            # The gateway is at the link end of the machine. Nerve does not
            # reach it through a proxy, so the client ignores the proxy
            # environment variables.
            self._client = httpx.AsyncClient(
                transport=self._transport,
                timeout=FETCH_TIMEOUT_SECONDS,
                trust_env=False,
            )
        response = await self._client.get(
            self.catalog_url, headers={"Accept": "application/json"},
        )
        if response.status_code != 200:
            raise CatalogError(f"MCP gateway answered HTTP {response.status_code}")
        try:
            payload = response.json()
        except ValueError as e:
            raise CatalogError("MCP gateway catalog is not JSON") from e
        return parse_catalog(payload, self.catalog_url)


# --- Tool calls -------------------------------------------------------------

# The protocol version of Nerve's tool calls. The gateway serves 2025-06-18,
# 2025-11-25 and 2026-07-28; from 2026-07-28 a call must also carry its
# arguments as Mcp-Param headers, which this client does not send.
TOOL_CALL_PROTOCOL_VERSION = "2025-06-18"
# JSON-RPC errors the gateway documents: -32001 is a policy decision (the
# server or tool is not enabled for the agent, or no credential works: do not
# retry); -32002 is a condition that may pass (an unreachable or slow
# upstream, a limit, an open circuit: retry later). -32000 is a fault the
# upstream server reported.
POLICY_REFUSED = -32001
RETRY_LATER = -32002
SERVER_FAULT = -32000


class GatewayCallError(Exception):
    """A tool call that produced no tool result.

    ``code`` is the JSON-RPC error code, ``None`` when the call failed below
    JSON-RPC; ``status`` the HTTP status, ``None`` when no response came;
    ``reason`` the gateway's reason code for a response without a JSON-RPC
    message. ``retryable`` says whether the same call may succeed later.
    The message never holds an argument or a credential.
    """

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        status: int | None = None,
        reason: str = "",
        retryable: bool = False,
        retry_after: float | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.status = status
        self.reason = reason
        self.retryable = retryable
        self.retry_after = retry_after


@dataclass(frozen=True)
class GatewayToolResult:
    """The result of a tool call: ``is_error`` for a tool that ran and
    refused, ``structured`` its ``structuredContent`` (else the JSON of its
    text, else ``None``) and ``text`` its text blocks."""

    is_error: bool
    structured: Any
    text: str


def _retry_after(response: httpx.Response) -> float | None:
    try:
        return max(0.0, float(response.headers.get("Retry-After", "")))
    except ValueError:
        return None


def _jsonrpc_message(response: httpx.Response) -> Any:
    """The JSON-RPC message of a response: the body, or the last ``data``
    event of an event stream. ``None`` when there is none."""
    content_type = response.headers.get("Content-Type", "")
    if content_type.startswith("text/event-stream"):
        message = None
        for line in response.text.splitlines():
            if line.startswith("data:"):
                try:
                    message = json.loads(line[5:].strip())
                except ValueError:
                    continue
        return message
    try:
        return response.json()
    except ValueError:
        return None


def _tool_result(result: Any) -> GatewayToolResult:
    if not isinstance(result, dict):
        raise GatewayCallError("MCP server answered a tool call without a result object")
    texts = [
        str(block.get("text") or "")
        for block in result.get("content") or []
        if isinstance(block, dict) and block.get("type") == "text"
    ]
    text = "\n".join(texts)
    structured = result.get("structuredContent")
    if structured is None and text:
        try:
            structured = json.loads(text)
        except ValueError:
            structured = None
    return GatewayToolResult(
        is_error=bool(result.get("isError")), structured=structured, text=text,
    )


# The gateway is at the link end of the machine: a connection that does not
# open in this time will not open.
CONNECT_TIMEOUT_SECONDS = 5.0


def _call_timeout(seconds: float) -> httpx.Timeout:
    return httpx.Timeout(seconds, connect=min(seconds, CONNECT_TIMEOUT_SECONDS))


class McpGatewayServer:
    """Call the tools of one server of the MCP gateway.

    ``url`` is the server's absolute URL from the catalog. The gateway
    identifies the agent from the connection, so no request carries a
    credential, and Nerve reaches it directly, not through a proxy.
    """

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self.url = url
        self._timeout = timeout
        self._transport = transport
        self._client: httpx.AsyncClient | None = None
        self._ids = itertools.count(1)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def call_tool(
        self, name: str, arguments: dict[str, Any], *, timeout: float | None = None,
    ) -> GatewayToolResult:
        """Run the tool ``name`` once. Raises :class:`GatewayCallError` when
        the call produced no tool result; a tool that ran and refused is a
        result with ``is_error``."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                transport=self._transport, timeout=_call_timeout(self._timeout), trust_env=False,
            )
        request_id = next(self._ids)
        body = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
        try:
            response = await self._client.post(
                self.url,
                json=body,
                headers={
                    "Accept": "application/json, text/event-stream",
                    "MCP-Protocol-Version": TOOL_CALL_PROTOCOL_VERSION,
                },
                timeout=_call_timeout(self._timeout if timeout is None else timeout),
            )
        except httpx.TimeoutException as e:
            raise GatewayCallError(
                f"MCP gateway did not answer {name} in time ({type(e).__name__})",
                retryable=True,
            ) from None
        except httpx.RequestError as e:
            raise GatewayCallError(
                f"MCP gateway request for {name} failed ({type(e).__name__})",
                retryable=True,
            ) from None
        message = _jsonrpc_message(response)
        error = message.get("error") if isinstance(message, dict) else None
        if isinstance(error, dict):
            code = error.get("code")
            code = code if isinstance(code, int) else None
            text = str(error.get("message") or "error")[:300]
            raise GatewayCallError(
                f"MCP gateway refused {name}: {text} ({code})",
                code=code,
                status=response.status_code,
                retryable=code in (RETRY_LATER, SERVER_FAULT) or response.status_code == 429,
                retry_after=_retry_after(response),
            )
        if response.status_code != 200 or not isinstance(message, dict):
            reason = ""
            if isinstance(message, dict) and isinstance(message.get("reason"), str):
                reason = message["reason"][:64]
            raise GatewayCallError(
                f"MCP gateway answered {name} with HTTP {response.status_code}"
                + (f" ({reason})" if reason else ""),
                status=response.status_code,
                reason=reason,
                retryable=response.status_code == 429 or response.status_code >= 500,
                retry_after=_retry_after(response),
            )
        return _tool_result(message.get("result"))
