"""MCP servers from the catalog of an MCP gateway, for external mode.

In external mode (``NERVE_AUTH_MODE=external``), an MCP gateway decides which
MCP servers the agent can use. Nerve reads ``GET <mcp_gateway_url>/catalog``
when it starts and before each new session, and gives each server of the
catalog to the agent clients as one HTTP MCP server. The gateway identifies
the agent from the connection, so the request carries no credential. The
catalog holds no upstream address and no secret.

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
"""

from __future__ import annotations

import asyncio
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
