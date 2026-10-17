"""The work in a Codex app-server that an end of the process would stop.

Nerve replaces a running Codex client (a new app-server process and
``thread/resume``) to apply an MCP gateway catalog change. The old process
and its process group stop, and with them every command it runs and every
child agent thread it hosts. So the engine replaces a client only when the
app-server runs none of these.

An empty notification queue proves nothing: notifications that arrive
between turns stay in the queue until the next turn reads and drops them.
:class:`LiveWork` therefore sees every notification when it arrives (the
transport's observer) and keeps the last evidence for each thread and each
open item. A state that it cannot classify counts as live work.
"""

from __future__ import annotations

from nerve.agent.backends.base import (
    BLOCKER_ACTIVE_TURN,
    BLOCKER_CHILD_AGENT,
    BLOCKER_COMMAND,
    BLOCKER_STATE_UNKNOWN,
)

# Thread states that LiveWork records.
ACTIVE = "active"
IDLE = "idle"
UNKNOWN = "unknown"

# ThreadStatus.type values of the app-server schema.
THREAD_STATUS_ACTIVE = "active"
THREAD_STATUSES_IDLE = frozenset({"idle", "notLoaded", "systemError"})

# CollabAgentStatus values of the app-server schema.
AGENT_STATES_LIVE = frozenset({"pendingInit", "running"})
AGENT_STATES_ENDED = frozenset({
    "interrupted", "completed", "errored", "shutdown", "notFound",
})

ITEM_COMMAND = "commandExecution"
ITEM_COLLAB = "collabAgentToolCall"


def thread_state(status: object) -> str:
    """LiveWork's state for a ThreadStatus object of the app-server."""
    kind = status.get("type") if isinstance(status, dict) else None
    if kind == THREAD_STATUS_ACTIVE:
        return ACTIVE
    if kind in THREAD_STATUSES_IDLE:
        return IDLE
    return UNKNOWN


class LiveWork:
    """Running turns, open items and child agents of one app-server."""

    def __init__(self) -> None:
        # Thread ID -> ACTIVE, IDLE or UNKNOWN: the last evidence.
        self._threads: dict[str, str] = {}
        # (thread ID, item ID) -> item type, from item/started until
        # item/completed. A command can outlive its turn (a process that
        # keeps running after the tool call returned).
        self._items: dict[tuple[str, str], str] = {}

    def observe(self, method: str, params: dict) -> None:
        """Take in one notification of the app-server."""
        thread_id = str(params.get("threadId") or "")
        if method == "turn/started":
            if thread_id:
                self._threads[thread_id] = ACTIVE
        elif method == "turn/completed":
            if thread_id:
                self._threads[thread_id] = IDLE
                # Other items of the thread ended with its turn.
                for key, kind in list(self._items.items()):
                    if key[0] == thread_id and kind != ITEM_COMMAND:
                        del self._items[key]
        elif method == "thread/status/changed":
            if thread_id:
                self._threads[thread_id] = thread_state(params.get("status"))
        elif method == "thread/closed":
            if thread_id:
                self._threads[thread_id] = IDLE
        elif method in ("item/started", "item/completed"):
            item = params.get("item")
            if not isinstance(item, dict):
                return
            item_id = str(item.get("id") or "")
            kind = str(item.get("type") or "")
            if item_id and thread_id:
                if method == "item/started":
                    self._items[(thread_id, item_id)] = kind
                else:
                    self._items.pop((thread_id, item_id), None)
            if kind == ITEM_COLLAB:
                self._follow_agents(item)

    def _follow_agents(self, item: dict) -> None:
        """Take in the agent states that a collaboration tool call reports."""
        states = item.get("agentsStates")
        states = states if isinstance(states, dict) else {}
        for thread_id, state in states.items():
            status = state.get("status") if isinstance(state, dict) else None
            if status in AGENT_STATES_LIVE:
                self._threads[str(thread_id)] = ACTIVE
            elif status in AGENT_STATES_ENDED:
                self._threads[str(thread_id)] = IDLE
            else:
                self._threads[str(thread_id)] = UNKNOWN
        receivers = item.get("receiverThreadIds")
        for thread_id in receivers if isinstance(receivers, list) else ():
            # A receiver without a reported state (for example an agent
            # that a spawn starts) may run: unknown until evidence comes.
            self._threads.setdefault(str(thread_id), UNKNOWN)

    def take_status(self, loaded: dict[str, str]) -> None:
        """Take in the app-server's own report of its threads.

        ``loaded`` maps each thread that the app-server holds in memory to
        its state (``thread/loaded/list`` and ``thread/read``). A thread
        that it does not hold runs nothing. Items other than commands end
        with their thread's turn; a command can outlive it, so commands
        stay until their ``item/completed``.
        """
        for thread_id in list(self._threads):
            if thread_id not in loaded:
                self._threads[thread_id] = IDLE
        self._threads.update(loaded)
        for key, kind in list(self._items.items()):
            if kind != ITEM_COMMAND and self._threads.get(key[0]) == IDLE:
                del self._items[key]

    def blockers(self, main_thread: str | None) -> set[str]:
        """``BLOCKER_*`` values for the live work that LiveWork knows of."""
        found: set[str] = set()
        for (thread_id, _item_id), kind in self._items.items():
            if kind == ITEM_COMMAND:
                found.add(BLOCKER_COMMAND)
            elif thread_id == main_thread:
                found.add(BLOCKER_ACTIVE_TURN)
            else:
                found.add(BLOCKER_CHILD_AGENT)
        for thread_id, state in self._threads.items():
            if state == ACTIVE:
                found.add(
                    BLOCKER_ACTIVE_TURN if thread_id == main_thread
                    else BLOCKER_CHILD_AGENT
                )
            elif state == UNKNOWN:
                found.add(BLOCKER_STATE_UNKNOWN)
        return found
