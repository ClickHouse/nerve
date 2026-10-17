"""MCP servers from the catalog of an MCP gateway, for external mode.

In external mode (``NERVE_AUTH_MODE=external``), an MCP gateway decides which
MCP servers and tools the agent can use. Nerve reads the agent's catalog from
``GET <mcp_gateway_url>/catalog`` when it starts and before each new session.
Each catalog server becomes one HTTP MCP server of the agent clients, at
``<mcp_gateway_url>/s/<id>/mcp``, also when the gateway has not observed the
server's tools yet: a client lists the tools through MCP when it connects, and
the gateway checks every call. A session that is running keeps the servers
that it started with.

The catalog has schema version 2. For each server it has the desired part
(path, tool policy, desired revision), which comes from the policy only, and
what the gateway observed: the last tool list that it served to the agent
(``inventory``) and the result of its last read (``health``). Revisions are
content hashes. Nerve acts on a changed desired revision or inventory
revision, never on a time, a health value, or the tenant generation.

The gateway identifies the agent from the connection, so the request carries
no credential. The catalog holds no upstream address and no secret.

When the gateway cannot give a catalog, Nerve keeps the catalog that it read
last (none after startup) and tries again in the background, with an interval
that doubles after each failure up to a limit. A catalog that Nerve read is
the configuration of new sessions; it does not show that a running client
loaded it.
"""

from __future__ import annotations

import asyncio
import calendar
import hashlib
import json
import logging
import re
import ssl
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Iterable

import httpx

logger = logging.getLogger(__name__)

# The wire contract of GET /catalog, schema version 2.
CATALOG_PATH = "/catalog"
CATALOG_SCHEMA_VERSION = 2
SERVER_ID_RE = re.compile(r"^[a-z]([a-z0-9-]{0,30}[a-z0-9])?$")
TOOL_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
# The tenant digest and every revision: "sha256:" and 64 lowercase hex digits.
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
# A tool mode, a tool change behavior and a check failure reason: lowercase
# ASCII letters, digits and underscores that start with a letter, at most 32,
# 32 and 64 characters.
TOOL_MODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
TOOL_CHANGES_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
CHECK_FAILURE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
# A time of the catalog: UTC, whole seconds, the suffix Z.
TIMESTAMP_RE = re.compile(
    r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z$",
)
# The largest value of a JSON integer member (a 64-bit signed integer).
_MAX_JSON_INT = 2**63 - 1

# The tool modes of schema 2. ``selected`` names the tools that the agent can
# call. ``all`` names no tool: the agent can call every tool of the server,
# also a tool that the server adds later. A server with another mode is
# configured all the same, and its tool names are not read.
TOOL_MODE_SELECTED = "selected"
TOOL_MODE_ALL = "all"

# Which definition of each tool the gateway serves when the server changes it.
# An absent value means ``accept``. Nerve configures a server with any value
# in the same way: the gateway applies it on every request.
TOOL_CHANGES_ACCEPT = "accept"

# The state of a server's inventory.
INVENTORY_UNKNOWN = "unknown"
INVENTORY_CURRENT = "current"
INVENTORY_STALE = "stale"
INVENTORY_STATES = (INVENTORY_UNKNOWN, INVENTORY_CURRENT, INVENTORY_STALE)

# The schema string at the start of the aggregate desired revision's encoding.
DESIRED_REVISION_SCHEMA = "nerve.mcp-gateway-desired-revision/2"

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

# Why a reload of the MCP configuration does nothing in external mode.
MANAGED_RELOAD_DETAIL = (
    "The organization manages the MCP servers of this agent through the MCP "
    "gateway. Nerve reads the gateway's catalog when it starts and before each "
    "new session, so there is no configuration to reload."
)

# The largest catalog that the gateway sends (4 MiB, with the final newline).
# It answers an error instead of a larger catalog.
CONTRACT_MAX_CATALOG_BYTES = 4 * 1024 * 1024

# The largest catalog that Nerve reads. It is larger than the contract's
# maximum, so every catalog that the gateway can send is read in full.
MAX_CATALOG_BYTES = 8 * 1024 * 1024

# One catalog request. A new session waits for it, so it is short.
# FETCH_TIMEOUT bounds each network operation; FETCH_DEADLINE_SECONDS bounds
# the whole request, from the connection to the parsed catalog. A gateway that
# sends its answer too slowly is unavailable for that request.
FETCH_TIMEOUT = httpx.Timeout(5.0, connect=2.0)
FETCH_DEADLINE_SECONDS = 5.0

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
class GatewayInventory:
    """The last tool list that the gateway served to the agent from a server.

    ``revision`` is a content hash of the served definitions; a list without a
    tool has a revision too. ``desired_revision`` is the server's desired
    revision when the gateway served the list: when it differs from the
    server's current one, the list belongs to an older policy.
    """

    revision: str
    tool_count: int
    observed_at: str
    desired_revision: str


@dataclass(frozen=True)
class GatewayHealth:
    """The gateway's last tool list read for the agent from one server.

    ``inventory_state`` is ``unknown`` (no inventory), ``current`` or
    ``stale``. ``failure`` is the fixed reason of a failed last read; a reason
    that Nerve does not know is a failure without detail.
    """

    inventory_state: str = INVENTORY_UNKNOWN
    checked_at: str | None = None
    failure: str | None = None


@dataclass(frozen=True)
class GatewayServer:
    """One MCP server of the agent, as the catalog names it.

    ``tools`` holds the tool names of the ``selected`` tool policy, possibly
    none. It is ``None`` for the mode ``all``, which names no tool, and for a
    tool mode that Nerve does not know; the server is configured all the
    same. ``tool_changes`` is the tool change behavior of the server
    (``accept`` when the catalog has none), also a value that Nerve does not
    know. ``inventory`` is ``None`` when the gateway has observed no tool
    list of the server for the agent, which is not the same as an empty list.
    """

    id: str
    url: str
    tools: tuple[str, ...] | None
    display_name: str = ""
    description: str = ""
    tool_mode: str = TOOL_MODE_SELECTED
    tool_changes: str = TOOL_CHANGES_ACCEPT
    desired_revision: str = ""
    inventory: GatewayInventory | None = None
    health: GatewayHealth = GatewayHealth()


@dataclass(frozen=True)
class GatewayCatalog:
    """The MCP servers that the agent can use, and what the gateway observed.

    ``desired_revision`` is the agent's aggregate desired revision, checked
    against the servers. ``generation`` and ``digest`` identify the tenant's
    policy and are for diagnostics only: they also change for a change that
    has no effect for this agent.
    """

    generation: int
    digest: str
    desired_revision: str
    servers: tuple[GatewayServer, ...]


def desired_revision(servers: Iterable[tuple[str, str]]) -> str:
    """The aggregate desired revision of ``(server ID, desired revision)`` pairs.

    The pairs are in catalog order. The encoding is the contract's: a string
    is its UTF-8 byte length as a 4-byte unsigned big-endian integer and its
    bytes, and a list is its item count in the same form and its items. The
    encoding is the schema string, then the list of servers, each as its ID
    and its desired revision.
    """
    pairs = list(servers)
    digest = hashlib.sha256()

    def put_string(value: str) -> None:
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)

    put_string(DESIRED_REVISION_SCHEMA)
    digest.update(len(pairs).to_bytes(4, "big"))
    for server_id, revision in pairs:
        put_string(server_id)
        put_string(revision)
    return "sha256:" + digest.hexdigest()


def follow_inventory(
    known: str | None, inventory: GatewayInventory | None,
) -> tuple[str | None, bool]:
    """Apply the contract's inventory rule for one server.

    ``known`` is the inventory revision that the reader knows for the server,
    or ``None``. Returns the revision that the reader knows after this
    catalog, and whether the server's tools must be listed again:

    - a revision that differs from the known one: list again;
    - a revision when none is known: know it, list nothing;
    - no inventory after a known revision: list again once, know none, so a
      later catalog without an inventory asks for nothing;
    - no inventory when none is known: nothing.

    A gateway replica keeps its observations in memory, so an inventory can be
    absent after a restart or from another replica; this rule lists the tools
    once and not at every such catalog.
    """
    if inventory is None:
        return None, known is not None
    return inventory.revision, known is not None and known != inventory.revision


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _member(data: dict, key: str, field: str) -> Any:
    """A required member. Raises :class:`CatalogError` when it is absent."""
    if key not in data:
        raise CatalogError(f"catalog {field} is missing")
    return data[key]


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise CatalogError(f"catalog {field} is not a string")
    return value


def _integer(value: Any, field: str) -> int:
    """A non-negative JSON integer member."""
    if not _is_int(value) or not 0 <= value <= _MAX_JSON_INT:
        raise CatalogError(f"catalog {field} is not a non-negative integer")
    return value


def _revision(value: Any, field: str) -> str:
    if not isinstance(value, str) or not DIGEST_RE.fullmatch(value):
        raise CatalogError(f"catalog {field} is not a sha256 revision")
    return value


def _timestamp(value: Any, field: str) -> str:
    """A time of the form ``YYYY-MM-DDTHH:MM:SSZ`` that exists."""
    match = TIMESTAMP_RE.fullmatch(value) if isinstance(value, str) else None
    if match is not None:
        year, month, day, hour, minute, second = (int(part) for part in match.groups())
        if (
            1 <= month <= 12
            and 1 <= day <= calendar.monthrange(year, month)[1]
            and hour <= 23 and minute <= 59 and second <= 59
        ):
            return value
    raise CatalogError(f"catalog {field} is not a UTC time with whole seconds")


def _object(value: Any, field: str) -> dict:
    """A member that holds an object. ``null`` counts as an empty object."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise CatalogError(f"catalog {field} is not a JSON object")
    return value


def _text(server: dict, key: str, field: str) -> str:
    """Display text. Absent or ``null`` is empty; a lone surrogate becomes
    U+FFFD, so the text can be encoded as UTF-8."""
    value = server.get(key, "")
    if value is None:
        return ""
    if not isinstance(value, str):
        raise CatalogError(f"catalog {field}.{key} is not a string")
    return value.encode("utf-16", "surrogatepass").decode("utf-16", "replace")


def _tool_policy(
    server: dict, field: str,
) -> tuple[str, tuple[str, ...] | None, str]:
    """The tool mode, the tool names when the mode ``selected`` names them,
    and the tool change behavior."""
    policy = _object(server.get("toolPolicy"), f"{field}.toolPolicy")
    mode = policy.get("mode")
    if not isinstance(mode, str) or not TOOL_MODE_RE.fullmatch(mode):
        raise CatalogError(f"catalog {field}.toolPolicy.mode is not a valid tool mode")
    tools = policy.get("tools")
    tools_field = f"{field}.toolPolicy.tools"
    if tools is None:
        if mode == TOOL_MODE_SELECTED:
            raise CatalogError(f"catalog {tools_field} is null for mode {mode}")
    elif not isinstance(tools, list):
        raise CatalogError(f"catalog {tools_field} is not a list")
    else:
        for index, tool in enumerate(tools):
            if not isinstance(tool, str) or not TOOL_NAME_RE.fullmatch(tool):
                raise CatalogError(f"catalog {tools_field} has an invalid tool name")
            if index and tools[index - 1] >= tool:
                raise CatalogError(f"catalog {tools_field} is not sorted without duplicates")
        if mode == TOOL_MODE_ALL:
            raise CatalogError(f"catalog {tools_field} is not null for mode {mode}")
    # Absent, null and empty mean accept.
    changes = policy.get("changes")
    if changes is None or changes == "":
        changes = TOOL_CHANGES_ACCEPT
    elif not isinstance(changes, str) or not TOOL_CHANGES_RE.fullmatch(changes):
        raise CatalogError(
            f"catalog {field}.toolPolicy.changes is not a valid tool change behavior",
        )
    # A consumer that does not know the mode does not read the tool names.
    names = tuple(tools) if mode == TOOL_MODE_SELECTED else None
    return mode, names, changes


def _inventory(server: dict, field: str) -> GatewayInventory | None:
    raw = server.get("inventory")
    if raw is None:
        return None
    field = f"{field}.inventory"
    if not isinstance(raw, dict):
        raise CatalogError(f"catalog {field} is not a JSON object")
    return GatewayInventory(
        revision=_revision(raw.get("revision"), f"{field}.revision"),
        tool_count=_integer(_member(raw, "toolCount", f"{field}.toolCount"), f"{field}.toolCount"),
        observed_at=_timestamp(raw.get("observedAt"), f"{field}.observedAt"),
        desired_revision=_revision(raw.get("desiredRevision"), f"{field}.desiredRevision"),
    )


def _health(
    server: dict, field: str, revision: str, inventory: GatewayInventory | None,
) -> GatewayHealth:
    """The health of a server, checked against its inventory."""
    raw = _object(server.get("health"), f"{field}.health")
    field = f"{field}.health"
    state = raw.get("inventoryState")
    if state not in INVENTORY_STATES:
        raise CatalogError(f"catalog {field}.inventoryState is not a known state")
    checked_at = raw.get("checkedAt")
    if checked_at is not None:
        checked_at = _timestamp(checked_at, f"{field}.checkedAt")
    failure = raw.get("failure")
    if failure is not None and (
        not isinstance(failure, str) or not CHECK_FAILURE_RE.fullmatch(failure)
    ):
        raise CatalogError(f"catalog {field}.failure is not a valid reason")

    if (inventory is None) != (state == INVENTORY_UNKNOWN):
        raise CatalogError(f"catalog {field}.inventoryState does not match the inventory")
    if state == INVENTORY_CURRENT and (
        failure is not None or inventory.desired_revision != revision
    ):
        raise CatalogError(
            f"catalog {field}.inventoryState is current without a current read",
        )
    if failure is not None and checked_at is None:
        raise CatalogError(f"catalog {field}.checkedAt is missing for a failure")
    # Times of one format compare in time order as strings.
    if inventory is not None and (checked_at is None or checked_at < inventory.observed_at):
        raise CatalogError(
            f"catalog {field}.checkedAt is missing or earlier than the observation",
        )
    return GatewayHealth(inventory_state=state, checked_at=checked_at, failure=failure)


def _server(server: Any, field: str, base_url: str, previous: str | None) -> GatewayServer:
    """One server, checked in the order of the contract's validation."""
    server = _object(server, field)
    server_id = server.get("id")
    if not isinstance(server_id, str) or not SERVER_ID_RE.fullmatch(server_id):
        raise CatalogError(f"catalog {field}.id is not a valid server ID")
    if previous is not None and previous >= server_id:
        raise CatalogError(f"catalog {field}.id order is not strictly ascending")
    display_name = _text(server, "displayName", field)
    description = _text(server, "description", field)
    path = server.get("path")
    if path != server_path(server_id):
        raise CatalogError(f"catalog {field}.path does not match its ID")
    mode, tools, changes = _tool_policy(server, field)
    revision = _revision(server.get("desiredRevision"), f"{field}.desiredRevision")
    inventory = _inventory(server, field)
    health = _health(server, field, revision, inventory)
    return GatewayServer(
        id=server_id,
        url=base_url + path,
        tools=tools,
        display_name=display_name,
        description=description,
        tool_mode=mode,
        tool_changes=changes,
        desired_revision=revision,
        inventory=inventory,
        health=health,
    )


def parse_catalog(payload: Any, base_url: str) -> GatewayCatalog:
    """Check a decoded catalog against the contract and return it.

    ``base_url`` is the MCP gateway URL without a trailing ``/``. The server
    URLs are ``base_url`` followed by the server path. Members that the
    contract does not define are ignored. The aggregate desired revision must
    match the servers. Raises :class:`CatalogError`, whose message names the
    member as the contract's validation names it.
    """
    if not isinstance(payload, dict):
        raise CatalogError("catalog is not a JSON object")
    schema = payload.get("schemaVersion")
    if not _is_int(schema) or schema != CATALOG_SCHEMA_VERSION:
        raise CatalogError("catalog schemaVersion is not supported")
    generation = _integer(_member(payload, "generation", "generation"), "generation")
    digest = payload.get("digest")
    if not isinstance(digest, str) or not DIGEST_RE.fullmatch(digest):
        raise CatalogError("catalog digest is not a sha256 digest")
    raw_servers = payload.get("servers")
    if not isinstance(raw_servers, list):
        raise CatalogError("catalog servers is not a list")

    servers: list[GatewayServer] = []
    for index, server in enumerate(raw_servers):
        previous = servers[-1].id if servers else None
        servers.append(_server(server, f"servers[{index}]", base_url, previous))

    revision = _revision(payload.get("desiredRevision"), "desiredRevision")
    if revision != desired_revision((s.id, s.desired_revision) for s in servers):
        raise CatalogError("catalog desiredRevision does not match its servers")
    return GatewayCatalog(
        generation=generation, digest=digest, desired_revision=revision,
        servers=tuple(servers),
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


def _not_json(constant: str) -> Any:
    """Refuse ``NaN`` and ``Infinity``, which Python reads and JSON lacks."""
    raise ValueError(f"{constant} is not JSON")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def server_status(server: GatewayServer) -> dict[str, Any]:
    """One catalog server for diagnostics: its desired state, and separately
    what the gateway observed of it."""
    inventory = server.inventory
    return {
        "id": server.id,
        "desired_revision": server.desired_revision,
        "tool_mode": server.tool_mode,
        "tool_changes": server.tool_changes,
        "inventory": None if inventory is None else {
            "revision": inventory.revision,
            "tool_count": inventory.tool_count,
            "observed_at": inventory.observed_at,
            "desired_revision": inventory.desired_revision,
        },
        "health": {
            "inventory_state": server.health.inventory_state,
            "checked_at": server.health.checked_at,
            "failure": server.health.failure,
        },
    }


class McpGatewayCatalog:
    """Read the agent's catalog from the MCP gateway and keep the last one.

    :meth:`start` reads the catalog once. :meth:`refresh` reads it again
    before a new session. Concurrent calls share one request, which takes at
    most ``deadline`` seconds. Each catalog read replaces the kept one. A
    changed catalog also goes to ``on_change``: one with another desired
    revision, or one where a server's inventory asks for a new tool list
    (:func:`follow_inventory`). Times, health and the tenant generation do not
    make a change. When ``on_change`` fails, the next successful request calls
    it again with the kept catalog. A failure keeps the catalog and starts the
    background retry, which stops at the first success.
    """

    def __init__(
        self,
        base_url: str,
        *,
        on_change: Callable[[GatewayCatalog], Awaitable[None]] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
        retry_initial: float = RETRY_INITIAL_SECONDS,
        retry_max: float = RETRY_MAX_SECONDS,
        deadline: float = FETCH_DEADLINE_SECONDS,
    ):
        self._base_url = base_url.rstrip("/")
        self._on_change = on_change
        self._transport = transport
        self._sleep = sleep
        self._retry_initial = retry_initial
        self._retry_max = retry_max
        self._deadline = deadline
        self._client: httpx.AsyncClient | None = None
        self._latest: GatewayCatalog | None = None
        # The inventory revision that the reader knows for each server.
        self._known_inventory: dict[str, str] = {}
        # Whether on_change completed for the last change.
        self._handled = True
        self._fetched_at: str | None = None
        self._checked_at: str | None = None
        self._error: str | None = None
        self._inflight: asyncio.Task | None = None
        self._retry_task: asyncio.Task | None = None
        self._closed = False

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def latest(self) -> GatewayCatalog | None:
        """The last catalog read, which new sessions use; ``None`` before the
        first one. A running session can have an older one."""
        return self._latest

    @property
    def servers(self) -> tuple[GatewayServer, ...]:
        """The servers of the last catalog read. Empty before the first one."""
        return self._latest.servers if self._latest is not None else ()

    @property
    def retrying(self) -> bool:
        return self._retry_task is not None and not self._retry_task.done()

    def status(self) -> dict[str, Any]:
        """The last catalog read and the result of the last request.

        For each server: its desired revision, and separately what the
        gateway observed (``inventory``, ``health``). None of it tells what a
        running session loaded.
        """
        latest = self._latest
        return {
            "url": self._base_url,
            "generation": latest.generation if latest else None,
            "digest": latest.digest if latest else None,
            "desired_revision": latest.desired_revision if latest else None,
            "servers": [server_status(server) for server in latest.servers] if latest else [],
            "fetched_at": self._fetched_at,
            "checked_at": self._checked_at,
            "error": self._error,
            "retrying": self.retrying,
        }

    async def start(self) -> None:
        """Read the catalog once. On a failure, start the background retry."""
        await self.refresh()

    async def refresh(self) -> GatewayCatalog | None:
        """Read the catalog now and return the last one read. Never raises."""
        if self._closed:
            return self._latest
        await self._attempt()
        return self._latest

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
        """Read the catalog once, shared by concurrent callers."""
        task = self._inflight
        if task is None or task.done():
            task = asyncio.ensure_future(self._fetch_and_keep())
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

    async def _fetch_and_keep(self) -> bool:
        self._checked_at = _now()
        try:
            async with asyncio.timeout(self._deadline):
                catalog = await self._fetch()
        except Exception as e:  # noqa: BLE001 - a failure keeps the catalog
            if isinstance(e, CatalogError):
                error = str(e)
            elif isinstance(e, TimeoutError):
                error = (
                    "MCP gateway catalog not received within "
                    f"{self._deadline:g} seconds"
                )
            else:
                error = f"MCP gateway catalog request failed ({type(e).__name__})"
            # One warning for each new problem; a retry that fails in the
            # same way is logged at debug level.
            log = logger.debug if error == self._error else logger.warning
            log("MCP gateway catalog not read: %s", error)
            self._error = error
            return False
        if self._error is not None:
            logger.info("MCP gateway catalog read again. Earlier problem: %s", self._error)
        self._error = None
        previous = self._latest
        self._latest = catalog
        self._fetched_at = _now()
        relist = self._follow_inventory(catalog)
        if previous is None or previous.desired_revision != catalog.desired_revision:
            self._handled = False
            logger.info(
                "MCP gateway catalog read: desired revision %s (generation %d), "
                "%d server(s) (%s). New sessions get these servers.",
                catalog.desired_revision, catalog.generation, len(catalog.servers),
                ", ".join(server.id for server in catalog.servers) or "none",
            )
        if relist:
            self._handled = False
            logger.info(
                "MCP gateway catalog: the observed tools of %s changed",
                ", ".join(relist),
            )
        if not self._handled and self._on_change is not None:
            try:
                await self._on_change(catalog)
                self._handled = True
            except Exception:  # noqa: BLE001 - the catalog is kept
                logger.exception(
                    "MCP gateway catalog handler failed; the next catalog "
                    "request calls it again",
                )
        return True

    def _follow_inventory(self, catalog: GatewayCatalog) -> tuple[str, ...]:
        """Update the known inventory revisions; return the servers whose
        tools must be listed again (:func:`follow_inventory`)."""
        known: dict[str, str] = {}
        relist: list[str] = []
        for server in catalog.servers:
            revision, again = follow_inventory(
                self._known_inventory.get(server.id), server.inventory,
            )
            if revision is not None:
                known[server.id] = revision
            if again:
                relist.append(server.id)
        self._known_inventory = known
        return tuple(relist)

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
            payload = json.loads(body, parse_constant=_not_json)
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
                task = asyncio.ensure_future(self._fetch_and_keep())
                self._inflight = task
            if await asyncio.shield(task):
                self._retry_task = None
                return
            delay = min(delay * 2, self._retry_max)
