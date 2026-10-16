"""The MCP gateway catalog of each running session (external mode).

A client gets the gateway servers of the applied catalog when it is built
(:class:`nerve.mcp_gateway.CatalogServers`). When a newer catalog is applied
later, a running session follows it before its next user turn, as far as
its backend can change MCP servers in a running client:

- A backend with ``updates_mcp_servers`` (Claude) turns off the servers that
  the catalog no longer names, turns on again the servers that it names
  again, and reconnects the servers whose allowed tools changed, so that the
  client lists their tools again. It cannot add a server: a catalog server
  that the client was not built with reaches new sessions only.
- Another backend (Codex) keeps the servers it was built with. The changed
  servers reach new sessions only.

The engine asks for an update only when the client is between turns: after
the drain of buffered autonomous output found nothing more, and while the
session runs no background task, whose end is what makes the CLI start a
turn on its own. All changes of
one update share one time budget, and the disables of removed servers start
first. A change whose result is unknown when the budget ends (the client may
still apply it) makes the engine replace the client before the user turn
starts, so no change of the old client can land in that turn.

The gateway refuses a server or a tool that the current policy does not
allow on every call, so a session that is behind loses no protection; it
shows the model a stale list. Diagnostics show each session's generation and
the servers that it does not follow.
"""

from __future__ import annotations

import asyncio
import enum
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from nerve.mcp_gateway import CatalogServers, plan_catalog_update

logger = logging.getLogger(__name__)

# The time budget of one update of a running client, for all its changes
# together (turn servers off, reconnect them, turn them on). The client lists
# a server's tools at the gateway, which answers from the link end in
# milliseconds, so the budget is far above the normal time. It bounds the
# delay that an update adds to a user turn.
UPDATE_BUDGET_SECONDS = 10.0


class UpdateOutcome(enum.Enum):
    """The result of :meth:`SessionCatalogs.update` for the engine."""

    # Nothing to do, or only bookkeeping: the client stays as it is.
    UNCHANGED = "unchanged"
    # The changes that the backend can make were made or refused.
    APPLIED = "applied"
    # The client is not provably between turns; a later turn tries again.
    DEFERRED = "deferred"
    # The result of a change is unknown: replace the client before the turn.
    REBUILD = "rebuild"


@dataclass
class _SessionCatalog:
    client: Any
    backend: str
    built_generation: int | None
    # The generation of the last catalog that the session follows in full,
    # apart from servers that it cannot add.
    generation: int | None
    # The (generation, digest) of the last catalog that the session took in,
    # so an unchanged catalog costs one comparison per turn.
    checked: tuple[int | None, str | None]
    configured: frozenset[str]
    enabled: dict[str, tuple[str, ...]] = field(default_factory=dict)
    pending: tuple[str, ...] = ()
    # A change was sent and its result is not known (the update ran out of
    # time or was cancelled). The client must be replaced.
    uncertain: bool = False


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
        between_turns: Callable[[], bool],
    ) -> UpdateOutcome:
        """Bring a running client to ``catalog`` as far as its backend can.

        The caller holds the session's run lock and starts no turn until this
        returns. ``between_turns`` tells whether the client is provably
        between turns now; it is asked before the first change and again
        before the second group of changes. A refused change leaves its
        server pending, and the next turn tries it again.
        """
        state = self._sessions.get(session_id)
        if state is None or state.client is not client:
            return UpdateOutcome.UNCHANGED
        if state.uncertain:
            return UpdateOutcome.REBUILD
        if catalog is None:
            return UpdateOutcome.UNCHANGED
        key = (catalog.generation, catalog.digest)
        if key == state.checked:
            return UpdateOutcome.UNCHANGED
        plan = plan_catalog_update(state.configured, state.enabled, catalog)
        wanted = {server.id: server.tools for server in catalog}
        changes = (*plan.disable, *plan.reconnect, *plan.enable)

        if not changes or not can_update:
            # Bookkeeping only: there is nothing that this client can change.
            state.checked = key
            if not changes:
                state.generation = catalog.generation
                state.pending = plan.missing
            else:
                state.pending = plan.servers()
                logger.info(
                    "Session %s (%s) keeps MCP gateway catalog generation %s; "
                    "new sessions get generation %s. Changed server(s): %s",
                    session_id, state.backend, state.generation,
                    catalog.generation, ", ".join(state.pending),
                )
            return UpdateOutcome.UNCHANGED

        if not between_turns():
            state.pending = plan.servers()
            logger.info(
                "Session %s: MCP gateway catalog generation %s waits for a "
                "later turn; the client is not between turns",
                session_id, catalog.generation,
            )
            return UpdateOutcome.DEFERRED

        loop = asyncio.get_running_loop()
        budget = UPDATE_BUDGET_SECONDS
        deadline = loop.time() + budget
        failed: set[str] = set()
        deferred: set[str] = set()
        # Until every sent change has a known result, the client counts as
        # uncertain; a cancellation keeps it so.
        state.uncertain = True
        state.pending = plan.servers()

        async def run(
            calls: dict[str, Callable[[], Awaitable[Any]]],
        ) -> dict[str, bool] | None:
            """Run changes at the same time. ``None`` when the budget ended
            before each had a result."""
            if not calls:
                return {}
            tasks = {
                asyncio.ensure_future(call()): server_id
                for server_id, call in calls.items()
            }
            remaining = max(0.0, deadline - loop.time())
            try:
                done, not_done = await asyncio.wait(tasks, timeout=remaining)
            except asyncio.CancelledError:
                # The client stays uncertain; its next turn replaces it.
                for task in tasks:
                    task.cancel()
                raise
            for task in not_done:
                task.cancel()
            if not_done:
                logger.warning(
                    "Session %s: MCP gateway server change(s) for %s did not "
                    "finish within %.0f s; the client is replaced",
                    session_id, ", ".join(sorted(tasks[t] for t in not_done)),
                    budget,
                )
                return None
            results: dict[str, bool] = {}
            for task in done:
                server_id = tasks[task]
                error = task.exception()
                results[server_id] = error is None
                if error is not None:
                    logger.warning(
                        "Session %s: the client refused a change of MCP "
                        "gateway server %r for catalog generation %s (%s); "
                        "the next turn tries again",
                        session_id, server_id, catalog.generation,
                        type(error).__name__,
                    )
            return results

        # Removed servers first, so a revocation reaches the client first.
        disabled = await run({
            server_id: (lambda sid=server_id: client.set_mcp_server_enabled(sid, False))
            for server_id in plan.disable
        })
        if disabled is None:
            return UpdateOutcome.REBUILD
        for server_id, ok in disabled.items():
            if ok:
                state.enabled.pop(server_id, None)
            else:
                failed.add(server_id)

        refreshed: dict[str, bool] = {}
        second = {
            **{
                server_id: (lambda sid=server_id: client.reconnect_mcp_server(sid))
                for server_id in plan.reconnect
            },
            **{
                server_id: (lambda sid=server_id: client.set_mcp_server_enabled(sid, True))
                for server_id in plan.enable
            },
        }
        if second and between_turns():
            result = await run(second)
            if result is None:
                return UpdateOutcome.REBUILD
            refreshed = result
        else:
            deferred.update(second)
        for server_id, ok in refreshed.items():
            if ok:
                state.enabled[server_id] = wanted[server_id]
            else:
                failed.add(server_id)

        state.uncertain = False
        state.pending = tuple(sorted({*plan.missing, *failed, *deferred}))
        if not failed and not deferred:
            state.generation = catalog.generation
            state.checked = key
        logger.info(
            "Session %s (%s) MCP gateway catalog generation %s: turned off %s, "
            "reconnected %s, turned on %s; pending: %s",
            session_id, state.backend, catalog.generation,
            ", ".join(sorted(s for s, ok in disabled.items() if ok)) or "none",
            ", ".join(sorted(s for s in plan.reconnect if refreshed.get(s))) or "none",
            ", ".join(sorted(s for s in plan.enable if refreshed.get(s))) or "none",
            ", ".join(state.pending) or "none",
        )
        return UpdateOutcome.APPLIED

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
