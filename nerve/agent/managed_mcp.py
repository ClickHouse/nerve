"""The MCP gateway catalog of each running session (external mode).

A client gets the gateway servers of the last catalog read when it is built
(:class:`nerve.mcp_gateway.CatalogServers`). When Nerve reads a newer catalog
later, the engine brings a running client to it before the client's next
user turn, at a safe boundary:

- The target is the complete key of the last catalog read: the agent's
  desired revision, and for each server its URL, desired revision and
  inventory revision under the contract's rule
  (:func:`nerve.mcp_gateway.follow_inventory`: a revision when the client
  knew none is adopted, because the client listed the tools when it
  connected). The tenant generation, times and health are not in it, so
  they never change a client. Catalogs that arrive while a client cannot
  change are coalesced: only the last one is applied.
- A client changes only when the change disrupts nothing: the drain of
  buffered autonomous output ended with no turn open, nothing is buffered,
  and neither the engine nor the backend reports live work (the engine's
  background-task registry; for Codex also running commands, child agents
  and active threads, see ``AgentClient.replacement_blockers``). Otherwise
  the change waits (``waiting_for_safe_boundary``). Nothing is cancelled to
  apply a catalog.
- A backend with ``updates_mcp_servers`` (Claude) changes the servers that
  its client was built with in place: it turns removed servers off and
  returned servers on, and reconnects the servers whose desired revision
  changed or whose tools must be listed again. All changes of one update
  share one time budget, and the removed servers are turned off first.
- An added server, a changed server URL, a backend without in-place changes
  (Codex), or an in-place change whose result is unknown at the end of the
  budget needs a new client. The engine replaces the client and the new one
  continues the same native conversation (Claude session, Codex thread). For
  a catalog change it never starts a new native conversation: when the new
  client cannot resume, the session keeps its conversation and shows an
  apply error, and a later user turn tries again.

The applied revision of a client is the desired revision of the last
catalog that it took in full: its build, a complete in-place update, or the
build of its replacement. A newer revision is never written on a client
that was built from an older catalog.

The gateway refuses a server or a tool that the current policy does not
allow on every call, so a session that is behind loses no protection; it
shows the model a stale list. Gateway errors in tool results (a denial, a
reset upstream session, an unknown call outcome) reach the model unchanged
and never change a client.
"""

from __future__ import annotations

import asyncio
import enum
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from nerve.mcp_gateway import (
    CatalogServers,
    CatalogUpdate,
    follow_inventory,
    plan_catalog_update,
)

logger = logging.getLogger(__name__)

# The time budget of one in-place update of a running client, for all its
# changes together (turn servers off, reconnect them, turn them on). The
# client lists a server's tools at the gateway, which answers from the link
# end in milliseconds, so the budget is far above the normal time. It bounds
# the delay that an update adds to a user turn.
UPDATE_BUDGET_SECONDS = 10.0

# Why a running client does not have the last catalog read (``pending``).
# A newer catalog was read; the client changes before its next user turn.
PENDING_NEXT_TURN = "waiting_for_turn"
# The client runs work that a change would disrupt; a later turn tries again.
PENDING_SAFE_BOUNDARY = "waiting_for_safe_boundary"
# A change was refused, or the replacement could not resume the
# conversation; a later turn tries again.
PENDING_APPLY_ERROR = "apply_error"

# How the last update of a client ended (``last_update.result``).
RESULT_APPLIED = "applied"
RESULT_REPLACED = "replaced"
RESULT_PARTIAL = "partial"
RESULT_FAILED = "failed"

# How the last update changed the client (``last_update.method``).
METHOD_IN_PLACE = "in_place"
METHOD_REPLACEMENT = "replacement"


class UpdateOutcome(enum.Enum):
    """The result of :meth:`SessionCatalogs.update` for the engine."""

    # The client has the target, or cannot be compared with it.
    UNCHANGED = "unchanged"
    # In-place changes ran; refused or waiting ones stay pending.
    APPLIED = "applied"
    # The client runs work that a change would disrupt; nothing changed.
    DEFERRED = "deferred"
    # The engine must replace the client, resuming its conversation.
    REPLACE = "replace"


def _catalog_key(servers: CatalogServers) -> tuple:
    """What decides whether a running client must change: the desired
    revision, and the URL and inventory revision of each server. The
    tenant generation, times and health are not in it."""
    return (
        servers.desired_revision,
        tuple(
            (
                server.id, server.url,
                server.inventory.revision if server.inventory else None,
            )
            for server in servers
        ),
    )


def _follow_inventories(
    known: dict[str, str], catalog: CatalogServers,
) -> tuple[dict[str, str], tuple[str, ...]]:
    """The inventory revisions known after ``catalog``, and the servers whose
    tools must be listed again (:func:`follow_inventory`)."""
    after: dict[str, str] = {}
    relist: list[str] = []
    for server in catalog:
        revision, again = follow_inventory(known.get(server.id), server.inventory)
        if revision is not None:
            after[server.id] = revision
        if again:
            relist.append(server.id)
    return after, tuple(relist)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class _Pending:
    reason: str
    blocked_by: tuple[str, ...] = ()
    error: str | None = None


@dataclass
class _Plan:
    """What a client must change to reach one target."""

    key: tuple
    target: CatalogServers
    # The inventory revisions that the client knows after the change.
    inventory: dict[str, str]
    update: CatalogUpdate
    # Servers of the client whose URL in the target differs.
    moved: tuple[str, ...]

    @property
    def servers(self) -> tuple[str, ...]:
        return tuple(sorted({*self.update.servers(), *self.moved}))


@dataclass
class _SessionState:
    # None after the engine stopped the client and its replacement failed.
    client: Any
    backend: str
    built_revision: str | None
    applied_revision: str | None
    # The key (_catalog_key) of the last target that the client took in
    # full, so an unchanged catalog costs one comparison per turn.
    applied_key: tuple
    # The servers that the client was built with, and their URLs.
    urls: dict[str, str]
    # The servers that are on, each with the desired revision that it was
    # last listed with.
    enabled: dict[str, str] = field(default_factory=dict)
    # The inventory revision that the client knows for each server.
    inventory: dict[str, str] = field(default_factory=dict)
    pending: _Pending | None = None
    last_update: dict[str, Any] | None = None
    # A change was sent and its result is not known (the update ran out of
    # time or was cancelled). The client must be replaced.
    uncertain: bool = False


class SessionCatalogs:
    """The applied catalog of each running client, and its updates."""

    def __init__(self) -> None:
        self._sessions: dict[str, _SessionState] = {}

    def record(self, session_id: str, backend: str, client: Any) -> None:
        """Remember the catalog that a new client was built with.

        A client without a :class:`CatalogServers` snapshot (local mode, or a
        test double) is not tracked. The last update of the session's
        earlier client is kept.
        """
        servers = getattr(client, "managed_mcp_servers", None)
        if not isinstance(servers, CatalogServers):
            self._sessions.pop(session_id, None)
            return
        previous = self._sessions.get(session_id)
        self._sessions[session_id] = _SessionState(
            client=client,
            backend=backend,
            built_revision=servers.desired_revision,
            applied_revision=servers.desired_revision,
            applied_key=_catalog_key(servers),
            urls={server.id: server.url for server in servers},
            enabled={server.id: server.desired_revision for server in servers},
            inventory={
                server.id: server.inventory.revision
                for server in servers if server.inventory is not None
            },
            last_update=previous.last_update if previous is not None else None,
        )

    def replaced(self, session_id: str, client: Any) -> None:
        """Note that ``client`` replaced the session's client for a catalog
        change (after :meth:`record`)."""
        state = self._sessions.get(session_id)
        if state is None or state.client is not client:
            return
        state.last_update = {
            "target_revision": state.applied_revision,
            "result": RESULT_REPLACED,
            "method": METHOD_REPLACEMENT,
            "at": _now(),
            "error": None,
        }

    def apply_failed(
        self, session_id: str, client: Any, error: str, *, stopped: bool,
    ) -> None:
        """Note that a replacement of ``client`` failed.

        ``stopped`` tells whether the engine stopped the old client before
        the failure; the session then has no client until a later turn.
        """
        state = self._sessions.get(session_id)
        if state is None or state.client is not client:
            return
        if stopped:
            state.client = None
        state.pending = _Pending(PENDING_APPLY_ERROR, error=error)
        state.last_update = {
            "target_revision": None,
            "result": RESULT_FAILED,
            "method": METHOD_REPLACEMENT,
            "at": _now(),
            "error": error,
        }

    def _plan(self, state: _SessionState, target: CatalogServers) -> _Plan:
        known, relist = _follow_inventories(state.inventory, target)
        return _Plan(
            key=_catalog_key(target),
            target=target,
            inventory=known,
            update=plan_catalog_update(state.urls, state.enabled, target, relist),
            moved=tuple(sorted(
                server.id for server in target
                if server.id in state.urls and state.urls[server.id] != server.url
            )),
        )

    async def update(
        self,
        session_id: str,
        client: Any,
        target: CatalogServers | None,
        *,
        in_place: bool,
        blockers: Callable[[], Awaitable[tuple[str, ...]]],
    ) -> UpdateOutcome:
        """Bring a running client to ``target``, or tell the engine to
        replace it.

        The caller holds the session's run lock and starts no turn until
        this returns. ``in_place`` tells whether the backend changes MCP
        servers of a running client. ``blockers`` returns the live work
        that a change would disrupt; it is asked before the first change and
        again before the second group of in-place changes.
        """
        state = self._sessions.get(session_id)
        if state is None or state.client is not client or target is None:
            return UpdateOutcome.UNCHANGED
        plan = self._plan(state, target)
        if not state.uncertain:
            if plan.key == state.applied_key:
                state.pending = None
                return UpdateOutcome.UNCHANGED
            if not plan.servers:
                # Bookkeeping only: the client has each server of the target
                # with its desired revision (an adopted inventory revision).
                self._took(state, plan)
                return UpdateOutcome.UNCHANGED

        found = tuple(await blockers())
        if found:
            state.pending = _Pending(PENDING_SAFE_BOUNDARY, blocked_by=found)
            logger.info(
                "Session %s (%s): MCP gateway catalog %s waits for a safe "
                "boundary (%s); servers: %s",
                session_id, state.backend, target.desired_revision,
                ", ".join(found), ", ".join(plan.servers) or "none",
            )
            return UpdateOutcome.DEFERRED

        if state.uncertain or not in_place or plan.update.missing or plan.moved:
            logger.info(
                "Session %s (%s): MCP gateway catalog %s needs a new client; "
                "servers: %s",
                session_id, state.backend, target.desired_revision,
                ", ".join(plan.servers) or "none",
            )
            return UpdateOutcome.REPLACE
        return await self._update_in_place(session_id, state, plan, blockers)

    def _took(self, state: _SessionState, plan: _Plan) -> None:
        """The client now has ``plan.target`` in full."""
        state.inventory = plan.inventory
        state.applied_key = plan.key
        state.applied_revision = plan.target.desired_revision
        state.pending = None

    async def _update_in_place(
        self,
        session_id: str,
        state: _SessionState,
        plan: _Plan,
        blockers: Callable[[], Awaitable[tuple[str, ...]]],
    ) -> UpdateOutcome:
        client = state.client
        update = plan.update
        wanted = {server.id: server.desired_revision for server in plan.target}
        loop = asyncio.get_running_loop()
        budget = UPDATE_BUDGET_SECONDS
        deadline = loop.time() + budget
        failed: set[str] = set()
        deferred: set[str] = set()
        later: tuple[str, ...] = ()
        # Until every sent change has a known result, the client counts as
        # uncertain; a cancellation keeps it so.
        state.uncertain = True

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
                        "gateway server %r (%s); a later turn tries again",
                        session_id, server_id, type(error).__name__,
                    )
            return results

        # Removed servers first, so a revocation reaches the client first.
        disabled = await run({
            server_id: (lambda sid=server_id: client.set_mcp_server_enabled(sid, False))
            for server_id in update.disable
        })
        if disabled is None:
            return UpdateOutcome.REPLACE
        for server_id, ok in disabled.items():
            if ok:
                state.enabled.pop(server_id, None)
            else:
                failed.add(server_id)

        refreshed: dict[str, bool] = {}
        second = {
            **{
                server_id: (lambda sid=server_id: client.reconnect_mcp_server(sid))
                for server_id in update.reconnect
            },
            **{
                server_id: (lambda sid=server_id: client.set_mcp_server_enabled(sid, True))
                for server_id in update.enable
            },
        }
        if second:
            later = tuple(await blockers())
            if later:
                deferred.update(second)
            else:
                result = await run(second)
                if result is None:
                    return UpdateOutcome.REPLACE
                refreshed = result
        for server_id, ok in refreshed.items():
            if ok:
                state.enabled[server_id] = wanted[server_id]
            else:
                failed.add(server_id)

        state.uncertain = False
        error: str | None = None
        if not failed and not deferred:
            self._took(state, plan)
            result_name = RESULT_APPLIED
        else:
            # A server whose change failed or waits lists its tools later.
            unsettled = failed | deferred
            state.inventory = {
                **{
                    sid: rev for sid, rev in plan.inventory.items()
                    if sid not in unsettled
                },
                **{
                    sid: state.inventory[sid] for sid in unsettled
                    if sid in state.inventory
                },
            }
            if failed:
                error = "the client refused the change of " + ", ".join(sorted(failed))
                state.pending = _Pending(PENDING_APPLY_ERROR, error=error)
                result_name = RESULT_FAILED
            else:
                state.pending = _Pending(PENDING_SAFE_BOUNDARY, blocked_by=later)
                result_name = RESULT_PARTIAL
        state.last_update = {
            "target_revision": plan.target.desired_revision,
            "result": result_name,
            "method": METHOD_IN_PLACE,
            "at": _now(),
            "error": error,
        }
        logger.info(
            "Session %s (%s) MCP gateway catalog %s: turned off %s, "
            "reconnected %s, turned on %s; not changed: %s",
            session_id, state.backend, plan.target.desired_revision,
            ", ".join(sorted(s for s, ok in disabled.items() if ok)) or "none",
            ", ".join(sorted(s for s in update.reconnect if refreshed.get(s))) or "none",
            ", ".join(sorted(s for s in update.enable if refreshed.get(s))) or "none",
            ", ".join(sorted(failed | deferred)) or "none",
        )
        return UpdateOutcome.APPLIED

    def status(
        self,
        live_client: Callable[[str], Any],
        target: CatalogServers | None,
    ) -> list[dict[str, Any]]:
        """One entry for each running client, sorted by session ID.

        ``live_client`` returns a session's client now; an entry whose client
        is gone (disconnected or rebuilt) is dropped. ``target`` is the last
        catalog read; ``pending`` compares each client with it.
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
                "built_revision": state.built_revision,
                "applied_revision": state.applied_revision,
                "inventory": dict(sorted(state.inventory.items())),
                "last_update": state.last_update,
                "pending": self._pending_status(state, target),
            })
        return entries

    def _pending_status(
        self, state: _SessionState, target: CatalogServers | None,
    ) -> dict[str, Any] | None:
        plan = self._plan(state, target) if target is not None else None
        current = plan is None or plan.key == state.applied_key or not plan.servers
        if state.client is not None and current and not state.uncertain:
            return None
        pending = state.pending or _Pending(PENDING_NEXT_TURN)
        return {
            "reason": pending.reason,
            "target_revision": target.desired_revision if target is not None else None,
            "servers": list(plan.servers) if plan is not None else [],
            "blocked_by": list(pending.blocked_by),
            "error": pending.error,
        }
