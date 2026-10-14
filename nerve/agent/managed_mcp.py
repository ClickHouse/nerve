"""The MCP gateway catalog of each running session (external mode).

A client gets the gateway servers of the applied catalog when it is built
(:class:`nerve.mcp_gateway.CatalogServers`). When a newer catalog is applied
later, a running session follows it at the start of its next turn, as far as
its backend can change MCP servers in a running client:

- A backend with ``updates_mcp_servers`` (Claude) turns off the servers that
  the catalog no longer names, turns on again the servers that it names
  again, and reconnects the servers whose allowed tools changed, so that the
  client lists their tools again. It cannot add a server: a catalog server
  that the client was not built with reaches new sessions only.
- Another backend (Codex) keeps the servers it was built with. The changed
  servers reach new sessions only.

The gateway refuses a server or a tool that the current policy does not
allow on every call, so a session that is behind loses no protection; it
shows the model a stale list. Diagnostics show each session's generation and
the servers that it does not follow.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from nerve.mcp_gateway import CatalogServers, plan_catalog_update

logger = logging.getLogger(__name__)

# The longest wait for one change of a running client (turn a server off or
# on, or reconnect it). The client then lists the server's tools at the
# gateway, which answers from the link end in milliseconds. A change that
# takes longer leaves the server pending and the turn goes on.
UPDATE_TIMEOUT_SECONDS = 10.0


@dataclass
class _SessionCatalog:
    client: Any
    backend: str
    built_generation: int | None
    generation: int | None
    # The (generation, digest) of the last catalog that the session was
    # compared with, so an unchanged catalog costs one comparison per turn.
    checked: tuple[int | None, str | None]
    configured: frozenset[str]
    enabled: dict[str, tuple[str, ...]] = field(default_factory=dict)
    pending: tuple[str, ...] = ()


class SessionCatalogs:
    """The catalog generation of each running client, and its updates."""

    def __init__(self) -> None:
        self._sessions: dict[str, _SessionCatalog] = {}

    def record(self, session_id: str, backend: str, client: Any) -> None:
        """Remember the catalog that a new client was built with.

        A client without a :class:`CatalogServers` snapshot (local mode, or a
        test double) is not tracked.
        """
        servers = getattr(client, "managed_mcp_servers", None)
        if not isinstance(servers, CatalogServers):
            self._sessions.pop(session_id, None)
            return
        self._sessions[session_id] = _SessionCatalog(
            client=client,
            backend=backend,
            built_generation=servers.generation,
            generation=servers.generation,
            checked=(servers.generation, servers.digest),
            configured=frozenset(server.id for server in servers),
            enabled={server.id: server.tools for server in servers},
        )

    async def update(
        self,
        session_id: str,
        client: Any,
        catalog: CatalogServers | None,
        *,
        can_update: bool,
    ) -> None:
        """Bring a running client to ``catalog`` as far as its backend can.

        Called at the start of a turn, under the session lock, so no turn of
        the session runs at the same time. Never raises: a change that fails
        leaves its server pending, and the next turn tries it again.
        """
        state = self._sessions.get(session_id)
        if state is None or state.client is not client or catalog is None:
            return
        key = (catalog.generation, catalog.digest)
        if key == state.checked:
            return
        plan = plan_catalog_update(state.configured, state.enabled, catalog)
        wanted = {server.id: server.tools for server in catalog}
        if not can_update:
            state.checked = key
            if plan.empty:
                state.generation = catalog.generation
                state.pending = ()
            else:
                state.pending = plan.servers()
                logger.info(
                    "Session %s (%s) keeps MCP gateway catalog generation %s; "
                    "new sessions get generation %s. Changed server(s): %s",
                    session_id, state.backend, state.generation,
                    catalog.generation, ", ".join(state.pending),
                )
            return

        failed: list[str] = []

        async def change(server_id: str, action: str, call: Callable[[], Any]) -> bool:
            try:
                await asyncio.wait_for(call(), UPDATE_TIMEOUT_SECONDS)
                return True
            except Exception as e:  # noqa: BLE001 - the turn must go on
                logger.warning(
                    "Session %s: could not %s MCP gateway server %r for "
                    "catalog generation %s (%s); new sessions get it",
                    session_id, action, server_id, catalog.generation,
                    type(e).__name__,
                )
                failed.append(server_id)
                return False

        for server_id in plan.disable:
            if await change(
                server_id, "turn off",
                lambda sid=server_id: client.set_mcp_server_enabled(sid, False),
            ):
                state.enabled.pop(server_id, None)
        for server_id in plan.reconnect:
            if await change(
                server_id, "reconnect",
                lambda sid=server_id: client.reconnect_mcp_server(sid),
            ):
                state.enabled[server_id] = wanted[server_id]
        for server_id in plan.enable:
            if await change(
                server_id, "turn on",
                lambda sid=server_id: client.set_mcp_server_enabled(sid, True),
            ):
                state.enabled[server_id] = wanted[server_id]

        state.generation = catalog.generation
        state.pending = tuple(sorted({*plan.missing, *failed}))
        # A failed change is tried again at the next turn.
        if not failed:
            state.checked = key
        if not plan.empty:
            logger.info(
                "Session %s (%s) follows MCP gateway catalog generation %s: "
                "turned off %s, reconnected %s, turned on %s; for new "
                "sessions only: %s",
                session_id, state.backend, catalog.generation,
                ", ".join(plan.disable) or "none",
                ", ".join(plan.reconnect) or "none",
                ", ".join(plan.enable) or "none",
                ", ".join(state.pending) or "none",
            )

    def status(self, live_client: Callable[[str], Any]) -> list[dict[str, Any]]:
        """One entry for each running client, sorted by session ID.

        ``live_client`` returns a session's client now. An entry whose client
        is gone (disconnected or rebuilt) is dropped.
        """
        entries: list[dict[str, Any]] = []
        for session_id in sorted(self._sessions):
            state = self._sessions[session_id]
            if live_client(session_id) is not state.client:
                del self._sessions[session_id]
                continue
            entries.append({
                "session_id": session_id,
                "backend": state.backend,
                "built_generation": state.built_generation,
                "generation": state.generation,
                "pending": list(state.pending),
            })
        return entries
