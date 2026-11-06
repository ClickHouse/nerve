"""Agent registry: the sub-agents and background tasks of every session.

The engine never lets a sub-agent's traffic into the orchestrator's turn.
Everything a sub-agent produces (events tagged ``parent_tool_use_id`` and
:class:`~nerve.agent.backends.events.SubagentUsage`), every spawning tool call
and every CLI task frame lands here instead, and becomes an
:class:`~nerve.agent.agents.model.AgentRecord`: a durable roster entry with an
accurate status, its brief, live progress, a per-agent transcript and a cost
estimate.

Memory model, per session: ``records`` by public id, plus routing maps
``by_task`` (CLI task id), ``by_route`` (``parent_tool_use_id`` values that
route to a record: the spawn and its resumes), ``by_spawn`` (the spawning
``tool_use_id``) and ``tool_owner`` (an LRU of sub-agent tool calls to their
agent). A session is loaded from the database on first touch; the sweeper
evicts sessions with nothing live that were not touched for 30 minutes.

Every mutation bumps the record's ``rev``, queues it for the write-behind
writer and tells the emitter; transcript items get a per-agent ``seq`` (from 1,
no gaps) and go to the writer and to subscribed sockets. Ingestion never
awaits a socket and never raises.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Iterable

from nerve.agent.agents import model as m
from nerve.agent.agents.emitter import AgentEmitter, RecordingEmitter
from nerve.agent.agents.model import AgentRecord
from nerve.agent.agents.writer import AgentWriter
from nerve.agent.backends.events import (
    SubagentStarted,
    SubagentUsage,
    SystemEvent,
    TextDelta,
    ThinkingDelta,
    ToolOutputDelta,
    ToolResult,
    ToolUse,
)

logger = logging.getLogger(__name__)

# (session_id, tool_use_id, is_error, result_content, tool_calls_log)
SideEffectsFn = Callable[[str, "str | None", bool, str, list], Awaitable[None]]

TASK_SUBTYPES = frozenset({"task_started", "task_progress", "task_updated", "task_notification"})
TOOL_OWNER_LRU = 5000            # sidechain tool calls remembered per session
OPEN_USAGE_IDS = 32              # message ids kept open for last-frame-wins dedupe
SPAWN_LOST_SECONDS = 300.0       # starting with no task_started and no spawn result
# A CLI task seen before the receipt of the spawn it belongs to waits this
# long for that receipt, then gets a record of its own.
PENDING_TASK_SECONDS = 10.0
# Foreground shells remembered (no record) in case they move to the background.
MAX_FOREGROUND_TASKS = 256
TURN_STOP_WINDOW_SECONDS = 30.0  # a turn stop explains a kill this long after
SWEEP_SECONDS = 5.0
EVICT_IDLE_SECONDS = 30 * 60.0
LOAD_LIMIT = 500
MAX_TRIGGERS = 5
MAX_NOTES = 10
NOTE_END_REASONS = frozenset({"gateway_restart", "process_ended", "force_stop", "model_switch"})
# remove_client reason -> end reason of the records it interrupts.
CLIENT_GONE_END_REASONS = {
    "process_exit": "process_ended",
    "process_ended": "process_ended",
    "hung_runtime": "process_ended",
    "idle_reap": "process_ended",
    "deleted": "process_ended",
    "model_switch": "model_switch",
    "force_stop": "force_stop",
    "archived": "archived",
    "gateway_restart": "gateway_restart",
}
# Where a spawn's receipt names the CLI task it started: the structured result
# (Agent: agentId; Workflow, Monitor, remote Agent: taskId; Bash:
# backgroundTaskId), else the result text.
_RECEIPT_TASK_KEYS = ("agentId", "taskId", "backgroundTaskId")
_RECEIPT_TASK_TEXT = (
    re.compile(r"agentId:\s*([\w-]+)"),
    re.compile(r"running in background with ID:\s*([\w-]+)"),
    re.compile(r"Monitor started \(task\s+([\w-]+)"),
    re.compile(r"Task ID:\s*([\w-]+)"),
)
_RECEIPT_OUTPUT_TEXT = re.compile(r"Output is being written to:\s*(\S+?\.output)\b")
_NOTE_REASON_TEXT = {
    "gateway_restart": "interrupted by a gateway restart",
    "process_ended": "interrupted when its runtime ended",
    "force_stop": "ended by a force stop",
    "model_switch": "ended by a model switch",
}
_SUBAGENT_DEFAULTS = SimpleNamespace(
    per_task_stop_affordance=True,
    progress_summaries=True,
    forward_subagent_text=True,
    quiet_after_minutes=10,
    lost_grace_seconds=30,
    stop_confirm_seconds=10,
    transcript_item_max_bytes=m.DEFAULT_MAX_TOOL_PAYLOAD,
)
# Input keys the shared tool-result side effects read.
_SIDE_EFFECT_INPUT_KEYS = ("file_path", "notebook_path", "command", "pattern")
_SIDE_EFFECT_VALUE_CAP = 512


def trim_tool_input(tool_input: Any) -> dict[str, Any]:
    """The part of a tool input the shared side effects read: ``file_path``,
    ``notebook_path``, ``command`` and ``pattern``, strings cut to 512."""
    if not isinstance(tool_input, dict):
        return {}
    out: dict[str, Any] = {}
    for key in _SIDE_EFFECT_INPUT_KEYS:
        if key in tool_input:
            value = tool_input[key]
            if isinstance(value, str):
                value = value[:_SIDE_EFFECT_VALUE_CAP]
            out[key] = value
    return out


def result_text(content: Any) -> str:
    """A tool result's content as sanitized text."""
    if isinstance(content, str):
        text = content
    elif content is None:
        text = ""
    else:
        try:
            text = json.dumps(content, default=str)
        except (TypeError, ValueError):
            text = str(content)
    return m.sanitize(text)


def _content_text(content: Any) -> str | None:
    """The text of a structured ``content`` list (``[{"type": "text", ...}]``)."""
    if isinstance(content, str):
        return content or None
    if isinstance(content, list):
        parts = [
            str(c.get("text") or "")
            for c in content
            if isinstance(c, dict) and c.get("type") == "text"
        ]
        text = "\n".join(p for p in parts if p)
        return text or None
    return None


def _note_text(value: str | None) -> str:
    """Agent-controlled text for the orchestrator reminder: one line, with
    angle brackets replaced so it cannot open or close a tag."""
    if not value:
        return ""
    text = " ".join(str(value).split())
    return text.replace("<", "‹").replace(">", "›")


def _is_foreground_shell(data: dict) -> bool:
    """A foreground Bash call's task: part of the turn, not background work
    (the CLI registers every foreground Bash that runs a few seconds)."""
    return _str(data.get("task_type")) == "local_bash" and data.get("is_backgrounded") is False


def _is_final_frame(subtype: str, data: dict, patch: dict) -> bool:
    if subtype == "task_notification":
        return True
    return subtype == "task_updated" and _str(patch.get("status")) in (
        "completed", "failed", "killed",
    )


def _receipt_task_id(structured: dict | None, content: str) -> str | None:
    """The CLI task id a spawn's launch receipt names, if any."""
    for key in _RECEIPT_TASK_KEYS:
        value = _str((structured or {}).get(key))
        if value:
            return value
    for pattern in _RECEIPT_TASK_TEXT:
        found = pattern.search(content or "")
        if found:
            return found.group(1)
    return None


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    return None


def _str(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _norm(value: Any) -> str:
    """A description or command, comparable: whitespace collapsed, capped."""
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())[: m.MAX_SUMMARY].casefold()


@dataclass
class _PendingTask:
    """A CLI task with no record yet.

    Either the receipt of the spawn it belongs to has not arrived (the CLI
    reports a background task before the spawning call's result, often
    without the call's id), or it is a foreground shell: every foreground
    Bash call that runs a few seconds is a CLI task too, but it is part of
    the turn, not background work, and gets a record only if it moves to
    the background."""
    task_id: str
    data: dict
    since: float
    started: bool = False          # task_started seen (data holds its fields)
    in_live_set: bool = False      # listed by background_tasks_changed
    missing_since: float | None = None
    foreground: bool = False


@dataclass
class _Session:
    sid: str
    records: dict[str, AgentRecord] = field(default_factory=dict)
    by_task: dict[str, str] = field(default_factory=dict)
    by_route: dict[str, str] = field(default_factory=dict)
    by_spawn: dict[str, str] = field(default_factory=dict)
    tool_owner: "OrderedDict[str, str]" = field(default_factory=OrderedDict)
    # CLI tasks without a record yet (see _PendingTask), oldest first.
    pending: "OrderedDict[str, _PendingTask]" = field(default_factory=OrderedDict)
    # Records folded into another one; their last view (hidden: ambient)
    # goes out once so open tabs drop them.
    tombstones: dict[str, AgentRecord] = field(default_factory=dict)
    client_gen: int | None = None
    backend: str = "claude"
    loaded: bool = False
    touched: float = 0.0
    turn_stop_at: float | None = None
    turn_stop_kills_bg: bool = False
    # Terminal records that ended before this stay out of the legacy chips.
    legacy_cutoff: str | None = None
    anon_usage: int = 0


class AgentRegistry:
    """Per-session registry of sub-agents and background tasks."""

    def __init__(
        self,
        db: Any = None,
        *,
        config_fn: Callable[[], Any] | None = None,
        side_effects: SideEffectsFn | None = None,
        stop_task_fn: Callable[[str, str], Awaitable[None]] | None = None,
        capabilities_fn: Callable[[str], dict] | None = None,
        emitter: AgentEmitter | None = None,
        clock: Callable[[], float] = time.time,
        writer: AgentWriter | None = None,
    ) -> None:
        self._db = db
        self._config_fn = config_fn
        self._side_effects = side_effects
        self._stop_task_fn = stop_task_fn
        self._capabilities_fn = capabilities_fn
        self._clock = clock
        self.writer = writer or AgentWriter(db, wall_clock=clock)
        self.emitter = emitter or AgentEmitter()
        self.emitter.bind(self)
        self._sessions: dict[str, _Session] = {}
        self._loading: dict[str, asyncio.Future] = {}
        self._sweeper: asyncio.Task | None = None
        self._background: set[asyncio.Task] = set()
        # Called without arguments whenever a session's live count may have
        # changed (see add_live_listener).
        self._live_listeners: tuple[Callable[[], None], ...] = ()

    @classmethod
    def for_tests(cls, **kw: Any) -> "AgentRegistry":
        """A registry with no database (records and items live in memory) whose
        emitter records into ``.emitted`` (a list of ``(channel, message)``)."""
        kw.setdefault("emitter", RecordingEmitter(
            has_listeners=kw.pop("has_listeners", None),
        ))
        return cls(None, **kw)

    @property
    def emitted(self) -> list[tuple[str, dict]]:
        """What a test registry's emitter sent (``[]`` for a broadcasting one)."""
        return getattr(self.emitter, "emitted", [])

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                          #
    # ------------------------------------------------------------------ #

    async def start(self, *, reconcile: bool = True) -> None:
        """Settle rows a crash left live, then start the writer, the emitter
        and the sweeper (every 5 s).

        ``reconcile=False`` skips the settling: a one-shot CLI engine started
        while the gateway runs must not end the gateway's live agents."""
        if reconcile:
            try:
                await self.reconcile_startup()
            except Exception:  # noqa: BLE001 — never block startup
                logger.exception("agents: startup reconcile failed")
        self.writer.start()
        self.emitter.start()
        if self._sweeper is None or self._sweeper.done():
            self._sweeper = asyncio.create_task(self._sweep_loop(), name="agent-sweeper")

    async def close(self) -> None:
        """Final flush; stop the tasks."""
        sweeper, self._sweeper = self._sweeper, None
        if sweeper is not None and not sweeper.done():
            sweeper.cancel()
            try:
                await sweeper
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                logger.exception("agents: sweeper failed")
        await self.emitter.close()
        await self.writer.close()

    async def reconcile_startup(self) -> int:
        """Mark records a previous process left live as
        ``interrupted(gateway_restart)``: their runtime died with it."""
        if self._db is None:
            return 0
        now = self._clock()
        n = await self._db.mark_live_agents_interrupted(
            reason="gateway_restart", now=m.iso_z(now), rev=m.next_rev(0),
        )
        if n:
            logger.info("agents: %d record(s) left live by the last run marked interrupted", n)
        return n

    async def _sweep_loop(self) -> None:
        while True:
            await asyncio.sleep(SWEEP_SECONDS)
            try:
                self.sweep()
            except Exception:  # noqa: BLE001 — the sweeper must survive
                logger.exception("agents: sweep failed")

    def sweep(self) -> None:
        """Time-based transitions: a CLI task whose spawn receipt never came
        gets its own record after ``PENDING_TASK_SECONDS``; a spawn that never
        started becomes lost after 300 s; a live-set member missing for
        ``lost_grace_seconds`` becomes lost; a stop with no final frame after
        ``stop_confirm_seconds`` is flagged unconfirmed. Idle sessions are
        evicted."""
        now = self._clock()
        sub = self._sub()
        lost_grace = float(sub.lost_grace_seconds)
        confirm = float(sub.stop_confirm_seconds)
        for sid, st in list(self._sessions.items()):
            for pend in list(st.pending.values()):
                if not pend.foreground and now - pend.since >= PENDING_TASK_SECONDS:
                    self._materialize(st, pend)
            for rec in list(st.records.values()):
                if (
                    rec.status == "starting"
                    and rec.created_mono is not None
                    and now - rec.created_mono >= SPAWN_LOST_SECONDS
                ):
                    self._terminal(st, rec, "lost", "vanished")
                elif (
                    rec.live
                    and rec.missing_since is not None
                    and now - rec.missing_since >= lost_grace
                ):
                    self._terminal(st, rec, "lost", "vanished")
                elif (
                    rec.status == "stopping"
                    and not rec.stop_unconfirmed
                    and rec.stop_requested_mono is not None
                    and now - rec.stop_requested_mono >= confirm
                ):
                    rec.stop_unconfirmed = True
                    self._changed(rec, immediate=True)
            if (
                self._db is not None
                and st.loaded
                and not any(r.live for r in st.records.values())
                and not any(not p.foreground for p in st.pending.values())
                and now - st.touched >= EVICT_IDLE_SECONDS
                and not self.writer.has_pending(sid)
            ):
                del self._sessions[sid]

    # ------------------------------------------------------------------ #
    #  Session state                                                      #
    # ------------------------------------------------------------------ #

    def _state_sync(self, sid: str) -> _Session:
        st = self._sessions.get(sid)
        if st is None:
            st = _Session(sid=sid, loaded=self._db is None)
            self._sessions[sid] = st
        st.touched = self._clock()
        return st

    async def _state(self, sid: str) -> _Session:
        st = self._state_sync(sid)
        if not st.loaded:
            await self._load(st)
        return st

    async def ensure_loaded(self, sid: str) -> None:
        """Load a session's records from the database (first touch only)."""
        await self._state(sid)

    async def _load(self, st: _Session) -> None:
        pending = self._loading.get(st.sid)
        if pending is not None:
            await asyncio.shield(pending)
            return
        fut = asyncio.get_running_loop().create_future()
        self._loading[st.sid] = fut
        done = False
        try:
            rows = await self._db.list_session_agents(st.sid, limit=LOAD_LIMIT)
            seqs = await self._db.max_agent_event_seqs([r["id"] for r in rows])
            for row in rows:
                if row["id"] in st.records:
                    continue  # memory wins
                rec = m.from_row(row)
                rec.next_seq = seqs.get(rec.id, 0) + 1
                for item in self.writer.pending_items(rec.id):
                    rec.next_seq = max(rec.next_seq, int(item.get("seq", 0)) + 1)
                self._index(st, rec)
            done = True
        except asyncio.CancelledError:
            raise  # not loaded: the next touch loads again
        except Exception:  # noqa: BLE001 — a failed load must not break ingestion
            logger.exception("agents[%s] load failed", st.sid[:8])
            done = True
        finally:
            if done:
                st.loaded = True
            self._loading.pop(st.sid, None)
            if not fut.done():
                fut.set_result(None)

    @staticmethod
    def _index(st: _Session, rec: AgentRecord) -> None:
        st.records[rec.id] = rec
        if rec.task_id:
            st.by_task.setdefault(rec.task_id, rec.id)
        if rec.spawn_tool_use_id:
            st.by_spawn.setdefault(rec.spawn_tool_use_id, rec.id)
        for route in rec.route_ids:
            st.by_route.setdefault(route, rec.id)

    def _sub(self) -> Any:
        config = self._config_fn() if self._config_fn else None
        return getattr(getattr(config, "agent", None), "subagents", None) or _SUBAGENT_DEFAULTS

    def _agent_teams(self) -> bool:
        config = self._config_fn() if self._config_fn else None
        return bool(getattr(getattr(config, "agent", None), "agent_teams", True))

    def _max_tool_payload(self) -> int:
        value = getattr(self._sub(), "transcript_item_max_bytes", m.DEFAULT_MAX_TOOL_PAYLOAD)
        try:
            return max(256, int(value))
        except (TypeError, ValueError):
            return m.DEFAULT_MAX_TOOL_PAYLOAD

    def _now_iso(self) -> str:
        return m.iso_z(self._clock())

    # ------------------------------------------------------------------ #
    #  Mutation helpers                                                   #
    # ------------------------------------------------------------------ #

    def _changed(self, rec: AgentRecord, *, immediate: bool, counts: bool = False) -> None:
        """A view-changing mutation: new rev, queued write, coalesced emit."""
        rec.rev = m.next_rev(rec.rev)
        self.writer.mark_dirty(rec)
        if not rec.ambient:
            self.emitter.upsert(rec.session_id, rec.id, immediate)
        if counts:
            self._counts_changed(rec.session_id)

    def _counts_changed(self, sid: str) -> None:
        """The session's live count may have changed: tell the emitter and
        the live listeners."""
        self.emitter.counts(sid)
        for listener in self._live_listeners:
            try:
                listener()
            except Exception:  # ingestion never raises
                logger.exception("agents: live listener failed")

    def _append_item(self, rec: AgentRecord, item: dict) -> dict:
        full = {"seq": rec.next_seq, "at": self._now_iso(), **item}
        rec.next_seq += 1
        self.writer.append(rec, full)
        if not rec.ambient:
            self.emitter.event(rec.session_id, rec.id, full)
        return full

    def _lifecycle(
        self, rec: AgentRecord, event: str, *, reason: str | None = None,
        actor_id: str | None = None,
    ) -> None:
        self._append_item(rec, {
            "kind": "lifecycle", "event": event, "run": int(rec.runs),
            "reason": reason, "actor_id": actor_id,
        })

    def _new_record(
        self, st: _Session, *, kind: str, status: str,
        spawn_tool_use_id: str | None = None, parent: AgentRecord | None = None,
    ) -> AgentRecord:
        now = self._clock()
        rec = AgentRecord(
            id=m.new_agent_id(), session_id=st.sid, backend=st.backend,
            kind=kind, status=status, created_at=m.iso_z(now),
        )
        rec.created_mono = now
        rec.client_gen = st.client_gen
        if spawn_tool_use_id:
            rec.spawn_tool_use_id = spawn_tool_use_id
            rec.route_ids.add(spawn_tool_use_id)
            st.by_spawn[spawn_tool_use_id] = rec.id
            st.by_route.setdefault(spawn_tool_use_id, rec.id)
        if parent is not None:
            rec.parent_agent_id = parent.id
            rec.depth = int(parent.depth) + 1
        st.records[rec.id] = rec
        return rec

    def _bind_task(self, st: _Session, rec: AgentRecord, task_id: str) -> bool:
        if rec.task_id == task_id:
            return True
        other = st.by_task.get(task_id)
        if other is not None and other != rec.id and other in st.records:
            logger.warning(
                "agents[%s] task %s already belongs to %s; not rebinding to %s",
                st.sid[:8], task_id, other, rec.id,
            )
            return False
        if rec.task_id:
            st.by_task.pop(rec.task_id, None)
        rec.task_id = task_id
        st.by_task[task_id] = rec.id
        return True

    def _set_running(self, rec: AgentRecord) -> bool:
        if rec.status == "starting":
            rec.status = "running"
            return True
        return False

    def _terminal(
        self, st: _Session, rec: AgentRecord, status: str, end_reason: str,
    ) -> bool:
        """Settle a record. The first terminal frame wins; later ones only
        fill empty fields (see the callers)."""
        if rec.terminal:
            return False
        actor_id = rec.stopped_by_actor_id if status == "stopped" else None
        rec.status = status
        rec.end_reason = end_reason
        rec.ended_at = self._now_iso()
        if not rec.result_text and rec.last_text:
            rec.result_text = m.cap_text(rec.last_text, m.MAX_RESULT)[0] or None
        self._shed_memory(rec)
        rec.settled_since_turn = True
        rec.stop_unconfirmed = False
        rec.stop_pending = False
        rec.pending_interaction = False
        rec.in_live_set = False
        rec.missing_since = None
        self._lifecycle(rec, status, reason=end_reason, actor_id=actor_id)
        self.writer.kick()
        self._changed(rec, immediate=True, counts=True)
        return True

    @staticmethod
    def _shed_memory(rec: AgentRecord) -> None:
        """A settled record keeps no working state in memory: the last text
        (already the result), the recent tool calls and the open usage
        frames (folded into the totals, which do not change)."""
        from nerve.db.usage import estimate_turn_cost

        rec.last_text = ""
        rec.recent_tool_calls.clear()
        if rec.usage_by_msg:
            for model, usage in rec.usage_by_msg.values():
                _add_usage(rec.usage_folded, usage)
                rec.cost_folded += _estimate(estimate_turn_cost, usage, model)
            rec.usage_by_msg.clear()

    def _stop_reason(self, st: _Session, rec: AgentRecord) -> str:
        if rec.stop_scope == "user":
            return "user"
        if rec.stop_scope == "stop_all":
            return "stop_all"
        if rec.orchestrator_stop_pending:
            return "orchestrator"
        if (
            st.turn_stop_at is not None
            and self._clock() - st.turn_stop_at <= TURN_STOP_WINDOW_SECONDS
            and (rec.background is False or st.turn_stop_kills_bg)
        ):
            return "turn_stop"
        return "unknown_stop"

    def _remember_owner(self, st: _Session, tool_use_id: str, agent_id: str) -> None:
        st.tool_owner[tool_use_id] = agent_id
        st.tool_owner.move_to_end(tool_use_id)
        while len(st.tool_owner) > TOOL_OWNER_LRU:
            st.tool_owner.popitem(last=False)

    def _owner(self, st: _Session, parent_tool_use_id: str) -> AgentRecord:
        """The agent a sidechain event belongs to; a placeholder record when
        the route is unknown (its spawn was never seen)."""
        aid = st.by_route.get(parent_tool_use_id)
        rec = st.records.get(aid) if aid else None
        if rec is not None:
            return rec
        rec = self._new_record(
            st, kind="agent", status="running", spawn_tool_use_id=parent_tool_use_id,
        )
        rec.started_at = rec.run_started_at = rec.created_at
        logger.debug(
            "agents[%s] placeholder %s for unknown route %s",
            st.sid[:8], rec.id, parent_tool_use_id,
        )
        self._changed(rec, immediate=True, counts=True)
        return rec

    def _touch(self, rec: AgentRecord) -> None:
        """Sidechain activity: running, and the activity clock moves."""
        rec.last_activity_at = self._now_iso()
        promoted = self._set_running(rec)
        self._changed(rec, immediate=promoted, counts=promoted)

    def _find(
        self, st: _Session, *, task_id: str | None = None, tool_use_id: str | None = None,
    ) -> AgentRecord | None:
        if task_id:
            aid = st.by_task.get(task_id)
            if aid and aid in st.records:
                return st.records[aid]
        if tool_use_id:
            aid = st.by_spawn.get(tool_use_id) or st.by_route.get(tool_use_id)
            if aid and aid in st.records:
                return st.records[aid]
        return None

    def _resolve_task_ref(self, st: _Session, ref: Any) -> AgentRecord | None:
        """A record from a TaskStop target: a CLI task id, an agent name or a
        public agent id."""
        ref = _str(ref)
        if not ref:
            return None
        rec = self._find(st, task_id=ref)
        if rec is not None:
            return rec
        if ref in st.records:
            return st.records[ref]
        named = [r for r in st.records.values() if r.name == ref]
        live = [r for r in named if r.live]
        return (live or named or [None])[-1]

    async def _deferred_stop(self, rec: AgentRecord) -> None:
        """A stop requested before the task id was known, now that it is."""
        if not rec.stop_pending or not rec.task_id or self._stop_task_fn is None:
            return
        rec.stop_pending = False
        try:
            await self._stop_task_fn(rec.session_id, rec.task_id)
        except Exception as e:  # noqa: BLE001 — the sweeper flags it unconfirmed
            logger.warning(
                "agents[%s] deferred stop of %s failed: %s",
                rec.session_id[:8], rec.task_id, e,
            )

    # ------------------------------------------------------------------ #
    #  Ingestion                                                          #
    # ------------------------------------------------------------------ #

    def _note_spawn(
        self, st: _Session, event: ToolUse, owner: AgentRecord | None,
    ) -> AgentRecord | None:
        """A spawning tool call (main or sidechain): a ``starting`` record."""
        kind = m.kind_for_spawn(event.name, event.input)
        if kind is None or not event.tool_use_id:
            return None
        inp = event.input if isinstance(event.input, dict) else {}
        existing = st.by_spawn.get(event.tool_use_id)
        if existing and existing in st.records:
            # A record made before the call was seen (a placeholder for its
            # route, or a task frame that named the call): give it the brief.
            rec = st.records[existing]
            if self._fill_brief(rec, kind, inp):
                self._changed(rec, immediate=True)
            return rec
        rec = self._new_record(
            st, kind=kind, status="starting",
            spawn_tool_use_id=event.tool_use_id, parent=owner,
        )
        self._fill_brief(rec, kind, inp)
        if kind == "agent":
            # The CLI backgrounds agents unless told otherwise.
            rec.background = inp.get("run_in_background") is not False
        else:
            rec.background = True
        self._changed(rec, immediate=True, counts=True)
        return rec

    @staticmethod
    def _fill_brief(rec: AgentRecord, kind: str, inp: dict) -> bool:
        """Fill the brief fields a spawn's input carries and the record lacks.
        Returns whether anything changed."""
        before = (rec.description, rec.name, rec.agent_type, rec.requested_model,
                  rec.prompt, rec.command)
        if not rec.description:
            rec.description = m.cap_chars(inp.get("description"), m.MAX_SUMMARY) or ""
        if kind == "agent":
            rec.name = rec.name or m.cap_chars(_str(inp.get("name")), 200)
            rec.agent_type = rec.agent_type or m.cap_chars(_str(inp.get("subagent_type")), 200)
            rec.requested_model = rec.requested_model or m.cap_chars(_str(inp.get("model")), 200)
            prompt = inp.get("prompt")
            if not rec.prompt and isinstance(prompt, str) and prompt:
                rec.prompt = m.cap_text(prompt, m.MAX_PROMPT)[0]
        elif kind == "workflow":
            rec.name = rec.name or m.cap_chars(_workflow_name(inp), 200)
        else:  # shell / monitor
            command = inp.get("command")
            if not rec.command and isinstance(command, str) and command:
                rec.command = m.cap_text(command, m.MAX_COMMAND)[0]
        return before != (rec.description, rec.name, rec.agent_type,
                          rec.requested_model, rec.prompt, rec.command)

    def _note_task_stop(self, st: _Session, tool_input: Any) -> None:
        inp = tool_input if isinstance(tool_input, dict) else {}
        rec = self._resolve_task_ref(st, inp.get("task_id") or inp.get("shell_id"))
        if rec is not None and rec.live:
            rec.orchestrator_stop_pending = True

    async def note_main_tool_use(self, sid: str, event: ToolUse) -> None:
        """An orchestrator tool call: spawns create records; ``TaskStop``
        marks its target as stopped by the orchestrator."""
        try:
            if m.kind_for_spawn(event.name, event.input) is None and event.name != "TaskStop":
                return
            st = await self._state(sid)
            if event.name == "TaskStop":
                self._note_task_stop(st, event.input)
            else:
                self._note_spawn(st, event, owner=None)
        except Exception:  # noqa: BLE001 — ingestion never breaks a turn
            logger.exception("agents[%s] main tool call ingestion failed", sid[:8])

    async def on_main_tool_result(self, sid: str, event: ToolResult) -> None:
        """An orchestrator tool result: settles or launches its spawn."""
        try:
            if not event.tool_use_id:
                return
            st = await self._state(sid)
            rec = self._find_spawn(st, event.tool_use_id)
            if rec is not None:
                await self._spawn_result(st, rec, event)
        except Exception:  # noqa: BLE001
            logger.exception("agents[%s] main tool result ingestion failed", sid[:8])

    @staticmethod
    def _find_spawn(st: _Session, tool_use_id: str) -> AgentRecord | None:
        aid = st.by_spawn.get(tool_use_id)
        return st.records.get(aid) if aid else None

    async def on_subagent_started(self, sid: str, event: SubagentStarted) -> None:
        """A main-chain sub-agent start. Claude: the spawning ``ToolUse``
        already created the record. Codex collab agents (no ``ToolUse``) get a
        read-only, foreground record that their tool result settles."""
        try:
            st = await self._state(sid)
            rec = self._find_spawn(st, event.tool_use_id) if event.tool_use_id else None
            if rec is not None:
                if st.backend == "codex" and self._set_running(rec):
                    rec.started_at = rec.started_at or self._now_iso()
                    rec.run_started_at = rec.started_at
                    self._lifecycle(rec, "started")
                    self._changed(rec, immediate=True, counts=True)
                return
            if not event.tool_use_id:
                return
            rec = self._new_record(
                st, kind="agent", status="running", spawn_tool_use_id=event.tool_use_id,
            )
            rec.agent_type = m.cap_chars(_str(event.subagent_type), 200)
            rec.description = m.cap_chars(event.description, m.MAX_SUMMARY) or ""
            rec.requested_model = m.cap_chars(_str(event.model), 200)
            rec.background = False  # lives inside the turn; its result settles it
            rec.started_at = rec.run_started_at = self._now_iso()
            self._lifecycle(rec, "started")
            self._changed(rec, immediate=True, counts=True)
        except Exception:  # noqa: BLE001
            logger.exception("agents[%s] sub-agent start ingestion failed", sid[:8])

    async def ingest_sidechain(self, sid: str, event: Any) -> None:
        """One event a sub-agent produced. Never raises."""
        try:
            if isinstance(event, (SubagentStarted, ToolOutputDelta)):
                return  # the ToolUse covers the spawn; live output is Codex-only
            parent = getattr(event, "parent_tool_use_id", None)
            if not parent:
                return
            st = await self._state(sid)
            rec = self._owner(st, parent)
            if isinstance(event, SubagentUsage):
                self._usage(st, rec, event)
            elif isinstance(event, TextDelta):
                self._text_item(rec, "text", event.text)
                rec.last_text = (event.text or "")[: m.MAX_RESULT]
                self._touch(rec)
            elif isinstance(event, ThinkingDelta):
                self._text_item(rec, "thinking", event.text)
                self._touch(rec)
            elif isinstance(event, ToolUse):
                await self._sidechain_tool_use(st, rec, event)
            elif isinstance(event, ToolResult):
                await self._sidechain_tool_result(st, rec, event)
        except Exception:  # noqa: BLE001 — ingestion never breaks a turn
            logger.exception("agents[%s] sidechain ingestion failed", sid[:8])

    def _text_item(self, rec: AgentRecord, kind: str, text: str) -> None:
        capped, truncated, size = m.cap_text(text, m.MAX_TEXT_ITEM)
        item: dict[str, Any] = {"kind": kind, "text": capped}
        if truncated:
            item.update(truncated=True, full_size=size)
        self._append_item(rec, item)

    async def _sidechain_tool_use(
        self, st: _Session, rec: AgentRecord, event: ToolUse,
    ) -> None:
        child = None
        if m.kind_for_spawn(event.name, event.input) is not None:
            child = self._note_spawn(st, event, owner=rec)
        elif event.name == "TaskStop":
            self._note_task_stop(st, event.input)
        capped, truncated, size = m.cap_tool_input(event.input, self._max_tool_payload())
        item: dict[str, Any] = {
            "kind": "tool_call",
            "tool_use_id": event.tool_use_id,
            "tool": event.name,
            "input": capped,
            "input_truncated": truncated,
            "child_agent_id": child.id if child is not None else None,
        }
        if truncated:
            item["full_size"] = size
        self._append_item(rec, item)
        rec.activity = m.summarize_activity(event.name, event.input)
        if not rec.has_progress_meter:
            rec.tool_uses += 1
        rec.recent_tool_calls.append({
            "tool": event.name,
            "tool_use_id": event.tool_use_id,
            "input": trim_tool_input(event.input),
        })
        if event.tool_use_id:
            self._remember_owner(st, event.tool_use_id, rec.id)
        self._touch(rec)

    async def _sidechain_tool_result(
        self, st: _Session, rec: AgentRecord, event: ToolResult,
    ) -> None:
        content = result_text(event.content)
        capped, truncated, size = m.cap_text(content, self._max_tool_payload())
        self._append_item(rec, {
            "kind": "tool_result",
            "tool_use_id": event.tool_use_id,
            "result": capped,
            "is_error": bool(event.is_error),
            "truncated": truncated,
            "full_size": size,
        })
        self._touch(rec)
        if self._side_effects is not None and event.tool_use_id:
            try:
                await self._side_effects(
                    st.sid, event.tool_use_id, bool(event.is_error), content,
                    list(rec.recent_tool_calls),
                )
            except Exception:  # noqa: BLE001
                logger.exception("agents[%s] tool-result side effects failed", st.sid[:8])
        if event.tool_use_id:
            child = self._find_spawn(st, event.tool_use_id)
            if child is not None:
                await self._spawn_result(st, child, event)

    async def _spawn_result(
        self, st: _Session, rec: AgentRecord, event: ToolResult,
    ) -> None:
        """The spawning call's result: failed spawn, background launch, or
        the foreground agent's completion.

        A launch receipt names the CLI task it started (Agent ``agentId``,
        Workflow/Monitor ``taskId``, Bash ``backgroundTaskId``, or the same
        in the result text). It is authoritative: the task's frames that
        arrived first (the CLI sends them before the receipt) are applied to
        this record, and a record made from them alone is folded into it. A
        record whose stop was requested before the receipt still binds its
        task id (so the deferred stop can fire) and may still settle (the
        work raced the stop)."""
        if not rec.live:
            return
        content = result_text(event.content)
        structured = event.structured if isinstance(event.structured, dict) else None
        status = (structured or {}).get("status")
        if event.is_error and rec.started_at is None:
            rec.error = m.cap_text(content, m.MAX_ERROR)[0] or None
            self._terminal(st, rec, "failed", "spawn_failed")
            return
        if status == "completed":
            # A foreground agent's completion; its task (if the CLI reported
            # one) is this record's, so its frames stop waiting for a home.
            agent_id = _str((structured or {}).get("agentId"))
            if agent_id:
                self._bind_receipt(st, rec, agent_id)
        task_id = None if status == "completed" else _receipt_task_id(structured, content)
        launched = status in ("async_launched", "remote_launched") or (
            not event.is_error and (
                "Async agent launched" in content
                or (task_id is not None and rec.kind in ("shell", "monitor", "workflow"))
            )
        )
        if launched:
            if task_id and not self._bind_receipt(st, rec, task_id):
                return  # never promote a record whose task belongs elsewhere
            output_file = (
                _str((structured or {}).get("outputFile"))
                or _str((structured or {}).get("outputPath"))
            )
            if output_file is None:
                found = _RECEIPT_OUTPUT_TEXT.search(content)
                output_file = found.group(1) if found else None
            if output_file and not rec.output_file:
                rec.output_file = output_file
            rec.background = True
            promoted = self._set_running(rec)
            if rec.started_at is None:
                rec.started_at = rec.run_started_at = self._now_iso()
                rec.client_gen = st.client_gen
                self._lifecycle(rec, "started")
            self._changed(rec, immediate=True, counts=promoted)
            await self._deferred_stop(rec)
            return
        if status == "completed":
            text = _content_text((structured or {}).get("content")) or content
            rec.result_text = m.cap_text(text, m.MAX_RESULT)[0] or None
            tokens = _int((structured or {}).get("totalTokens"))
            if tokens is not None:
                rec.tokens = tokens
            tool_uses = _int((structured or {}).get("totalToolUseCount"))
            if tool_uses is not None:
                rec.tool_uses = tool_uses
            self._terminal(st, rec, "completed", "completed")
            return
        if rec.background is False:
            if event.is_error:
                rec.error = m.cap_text(content, m.MAX_ERROR)[0] or None
                self._terminal(st, rec, "failed", "error")
            else:
                if not rec.result_text:
                    rec.result_text = m.cap_text(content, m.MAX_RESULT)[0] or None
                self._terminal(st, rec, "completed", "completed")

    def _usage(self, st: _Session, rec: AgentRecord, event: SubagentUsage) -> None:
        from nerve.db.usage import estimate_turn_cost

        usage = event.usage if isinstance(event.usage, dict) else {}
        mid = event.message_id
        if not mid:
            st.anon_usage += 1
            mid = f"_anon{st.anon_usage}"
        rec.usage_by_msg[mid] = (event.model, dict(usage))
        rec.usage_by_msg.move_to_end(mid)
        while len(rec.usage_by_msg) > OPEN_USAGE_IDS:
            _old, (model, old_usage) = rec.usage_by_msg.popitem(last=False)
            _add_usage(rec.usage_folded, old_usage)
            rec.cost_folded += _estimate(estimate_turn_cost, old_usage, model)
        totals = dict(rec.usage_folded)
        cost = rec.cost_folded
        for model, open_usage in rec.usage_by_msg.values():
            _add_usage(totals, open_usage)
            cost += _estimate(estimate_turn_cost, open_usage, model)
        rec.usage = totals
        rec.cost_usd = cost
        if event.model and event.model not in rec.models:
            rec.models.append(event.model)
            if len(set(rec.models)) > 1 and rec.model_changed_at is None:
                rec.model_changed_at = self._now_iso()
        if not rec.has_progress_meter:
            rec.tokens = sum(totals.get(k, 0) for k in (
                "input_tokens", "output_tokens", "cache_read_input_tokens",
                "cache_creation_input_tokens",
            ))
        self._changed(rec, immediate=False)

    async def on_system_event(self, sid: str, event: SystemEvent) -> bool:
        """A CLI system frame (``task_*``, ``background_tasks_changed``).
        Returns True when the legacy background-task chip list changed."""
        try:
            subtype = event.subtype
            data = event.data if isinstance(event.data, dict) else {}
            if subtype == "background_tasks_changed":
                st = await self._state(sid)
                for rec in self._live_set(st, data):
                    await self._deferred_stop(rec)
                return False
            if subtype not in TASK_SUBTYPES:
                return False
            task_id = _str(data.get("task_id"))
            if not task_id:
                return False
            st = await self._state(sid)
            patch = data.get("patch") if isinstance(data.get("patch"), dict) else {}
            logger.debug(
                "agents[%s] %s task=%s tool_use=%s type=%s bg=%s status=%s depth=%s",
                sid[:8], subtype, task_id, data.get("tool_use_id"), data.get("task_type"),
                data.get("is_backgrounded", patch.get("is_backgrounded")),
                data.get("status", patch.get("status")), data.get("spawn_depth"),
            )
            rec = self._find(st, task_id=task_id)
            before = self._legacy_entry(rec) if rec is not None else None
            if subtype == "task_started":
                rec = self._task_started(st, task_id, data)
            elif rec is None:
                rec = self._unbound_task_frame(st, task_id, subtype, data, patch)
            if rec is None:
                return False
            if subtype == "task_progress":
                self._task_progress(rec, data)
            elif subtype == "task_updated":
                self._task_updated(st, rec, data, patch)
            elif subtype == "task_notification":
                self._task_notification(st, rec, data)
            await self._deferred_stop(rec)
            return before != self._legacy_entry(rec)
        except Exception:  # noqa: BLE001 — ingestion never breaks a turn
            logger.exception("agents[%s] system event ingestion failed", sid[:8])
            return False

    def _task_started(
        self, st: _Session, task_id: str, data: dict,
    ) -> AgentRecord | None:
        """``task_started``: bind the task to its record and start it.

        The record is the one already bound to the task (a resume), else the
        spawn the frame's ``tool_use_id`` names. A task with neither waits
        (no record) while a background spawn of its kind still awaits its
        receipt (the CLI reports the task first, often without the call's
        id; the receipt names the task) and when it is a foreground shell
        (part of the turn); anything else gets a record of its own."""
        rec = self._find(st, task_id=task_id)
        pend = st.pending.get(task_id)
        merged = {
            **(pend.data if pend is not None else {}),
            **{k: v for k, v in data.items() if v is not None},
        }
        if rec is None:
            rec, _guessed = self._claim(st, task_id, merged, relaxed=False)
        if rec is None:
            foreground = _is_foreground_shell(merged)
            if foreground or self._spawn_awaited(st, merged):
                self._pend(st, task_id, merged, started=True, foreground=foreground)
                return None
            rec = self._new_task_record(st, task_id, merged)
        if rec.task_id != task_id and not self._bind_task(st, rec, task_id):
            return None
        if pend is not None:
            st.pending.pop(task_id, None)
            self._join_live_set(rec, pend)
        self._start_task(st, rec, merged)
        return rec

    def _unbound_task_frame(
        self, st: _Session, task_id: str, subtype: str, data: dict, patch: dict,
    ) -> AgentRecord | None:
        """The record a ``task_progress``/``task_updated``/``task_notification``
        frame of a task with no record yet belongs to, or None (dropped)."""
        pend = st.pending.get(task_id)
        if pend is not None and pend.foreground:
            if subtype == "task_updated" and patch.get("is_backgrounded") is True:
                return self._materialize(st, pend, background=True)
            if _is_final_frame(subtype, data, patch):
                st.pending.pop(task_id, None)  # the foreground shell ended with its call
            return None
        probe = dict(pend.data) if pend is not None else {}
        if _str(data.get("tool_use_id")):
            probe["tool_use_id"] = data["tool_use_id"]
        rec, _guessed = self._claim(st, task_id, probe, relaxed=False)
        if rec is not None:
            if not self._bind_task(st, rec, task_id):
                return None
            if pend is not None:
                st.pending.pop(task_id, None)
                self._apply_pending(st, rec, pend)
            return rec
        if pend is not None:
            # Its receipt is late: it gets a record now (the frame needs one).
            return self._materialize(st, pend)
        return None

    def _start_task(self, st: _Session, rec: AgentRecord, data: dict) -> None:
        """Apply a task's start frame (``task_started`` fields, or the live
        set's entry) to its bound record: kind, brief, status, lifecycle. A
        record that had ended is resumed (SendMessage)."""
        tool_use_id = _str(data.get("tool_use_id"))
        task_type = _str(data.get("task_type"))
        resume = rec.terminal
        if task_type:
            rec.task_type = task_type
            kind, ambient = m.kind_for_task_type(
                task_type, "Monitor" if rec.kind == "monitor" else None,
            )
            if kind != "other" or ambient:
                rec.kind = kind
            rec.ambient = rec.ambient or ambient
        if data.get("ambient"):
            rec.ambient = True
        if not rec.description and _str(data.get("description")):
            rec.description = m.cap_chars(data.get("description"), m.MAX_SUMMARY) or ""
        if rec.kind == "workflow" and _str(data.get("workflow_name")):
            # The CLI's name for the workflow is authoritative.
            rec.name = m.cap_chars(data.get("workflow_name"), 200)
        if _str(data.get("subagent_type")):
            rec.agent_type = m.cap_chars(data.get("subagent_type"), 200)
        if isinstance(data.get("is_backgrounded"), bool):
            rec.background = data["is_backgrounded"]
        depth = _int(data.get("spawn_depth"))
        if depth:
            rec.depth = depth
        prompt = data.get("prompt")
        if not rec.prompt and isinstance(prompt, str) and prompt:
            rec.prompt = m.cap_text(prompt, m.MAX_PROMPT)[0]
        now = self._now_iso()
        first_start = rec.started_at is None
        rec.started_at = rec.started_at or now
        rec.run_started_at = now
        rec.last_activity_at = now
        rec.client_gen = st.client_gen
        if resume:
            rec.runs += 1
            rec.status = "running"
            rec.end_reason = rec.ended_at = rec.error = None
            rec.stop_requested_at = rec.stop_scope = None
            rec.stopped_by_actor_id = rec.stopped_caused_by_actor_id = None
            rec.stop_requested_mono = None
            rec.orchestrator_stop_pending = False
            rec.settled_since_turn = False
            # The result is the latest run's: the new run's last text replaces it.
            rec.result_text = None
            rec.last_text = ""
            if tool_use_id:
                rec.route_ids.add(tool_use_id)
                st.by_route.setdefault(tool_use_id, rec.id)
            self._lifecycle(rec, "resumed")
        else:
            self._set_running(rec)
            if first_start:
                self._lifecycle(rec, "started")
        self._changed(rec, immediate=True, counts=True)

    # ------------------------------------------------------------------ #
    #  Tasks without a record yet, receipts, merges                       #
    # ------------------------------------------------------------------ #

    def _unbound_spawns(
        self, st: _Session, data: dict, *, starting_only: bool,
    ) -> list[AgentRecord]:
        """Live spawn records with no task id that a CLI task of this type
        could belong to, oldest first."""
        task_type = _str(data.get("task_type"))
        kind, _ambient = m.kind_for_task_type(task_type)
        allowed = m.SPAWN_KINDS_FOR_TASK_KIND.get(kind)
        if not allowed:
            return []
        bg = data.get("is_backgrounded")
        out = []
        for rec in st.records.values():
            if (
                rec.task_id
                or not rec.spawn_tool_use_id
                or not rec.live
                or rec.backend != "claude"
                or rec.kind not in allowed
                or (starting_only and rec.status != "starting")
            ):
                continue
            if task_type == "local_bash" and isinstance(bg, bool) and rec.background != bg:
                continue  # a foreground shell never belongs to a background Bash spawn
            out.append(rec)
        return out

    def _spawn_awaited(self, st: _Session, data: dict) -> bool:
        """Whether a spawn this task could belong to still awaits its
        receipt (which names its task)."""
        return bool(self._unbound_spawns(st, data, starting_only=True))

    def _claim(
        self, st: _Session, task_id: str, data: dict, *, relaxed: bool,
    ) -> tuple[AgentRecord | None, bool]:
        """The record a task with no binding belongs to, and whether that is
        a guess. The spawn its ``tool_use_id`` names is certain. ``relaxed``
        (a task whose receipt never came) also guesses: the oldest unbound
        spawn of its kind with the same description, else the only one."""
        tool_use_id = _str(data.get("tool_use_id"))
        if tool_use_id:
            aid = st.by_spawn.get(tool_use_id) or st.by_route.get(tool_use_id)
            rec = st.records.get(aid) if aid else None
            if rec is not None and rec.task_id in (None, task_id):
                return rec, False
        if not relaxed:
            return None, False
        candidates = self._unbound_spawns(st, data, starting_only=False)
        desc = _norm(data.get("description"))
        if desc:
            for rec in candidates:
                if desc in (_norm(rec.description), _norm(rec.command)):
                    return rec, True
        if len(candidates) == 1:
            return candidates[0], True
        return None, False

    def _pend(
        self, st: _Session, task_id: str, data: dict, *, started: bool = False,
        live: bool = False, foreground: bool = False,
    ) -> _PendingTask:
        """Remember a task that gets no record yet."""
        pend = st.pending.get(task_id)
        if pend is None:
            pend = _PendingTask(task_id=task_id, data={}, since=self._clock())
            st.pending[task_id] = pend
        pend.data.update({k: v for k, v in data.items() if v is not None})
        pend.started = pend.started or started
        if started:
            pend.foreground = foreground
        if live:
            pend.in_live_set = True
            pend.missing_since = None
        if pend.foreground:
            foreground_ids = [p.task_id for p in st.pending.values() if p.foreground]
            for old in foreground_ids[: max(0, len(foreground_ids) - MAX_FOREGROUND_TASKS)]:
                st.pending.pop(old, None)
        return pend

    def _new_task_record(self, st: _Session, task_id: str, data: dict) -> AgentRecord:
        """A record for a task no spawn we saw started (kind from its type,
        parent from the agent that made the call), bound to the task."""
        tool_use_id = _str(data.get("tool_use_id"))
        kind, _ambient = m.kind_for_task_type(_str(data.get("task_type")))
        owner_id = st.tool_owner.get(tool_use_id) if tool_use_id else None
        rec = self._new_record(
            st, kind=kind, status="starting",
            spawn_tool_use_id=(
                tool_use_id if tool_use_id and tool_use_id not in st.by_spawn else None
            ),
            parent=st.records.get(owner_id) if owner_id else None,
        )
        rec.task_only = True
        rec.task_id = task_id
        st.by_task[task_id] = rec.id
        if owner_id and kind in ("shell", "monitor"):
            owner = st.records.get(owner_id)
            for call in (owner.recent_tool_calls if owner else ()):
                command = (call.get("input") or {}).get("command")
                if call.get("tool_use_id") == tool_use_id and isinstance(command, str):
                    rec.command = m.cap_text(command, m.MAX_COMMAND)[0] or None
        return rec

    def _materialize(
        self, st: _Session, pend: _PendingTask, *, background: bool | None = None,
    ) -> AgentRecord:
        """Give a waiting task its record: a guessed spawn of its kind, else
        a record of its own. ``background`` overrides what its start frame
        said (a foreground shell that moved to the background)."""
        st.pending.pop(pend.task_id, None)
        data = dict(pend.data)
        if background is not None:
            data["is_backgrounded"] = background
        rec, guessed = self._claim(st, pend.task_id, data, relaxed=not pend.foreground)
        if rec is not None and rec.task_id in (None, pend.task_id) and (
            rec.task_id == pend.task_id or self._bind_task(st, rec, pend.task_id)
        ):
            rec.task_guessed = rec.task_guessed or guessed
        else:
            rec = self._new_task_record(st, pend.task_id, data)
        if background:
            rec.background = True
        self._join_live_set(rec, pend)
        self._start_task(st, rec, data)
        if rec.stop_pending and rec.task_id:
            self._spawn_task(self._deferred_stop(rec))
        return rec

    @staticmethod
    def _join_live_set(rec: AgentRecord, pend: _PendingTask) -> None:
        if pend.in_live_set:
            rec.in_live_set = True
            rec.missing_since = pend.missing_since
            rec.background = True

    def _apply_pending(self, st: _Session, rec: AgentRecord, pend: _PendingTask) -> None:
        """A waiting task's frames, applied to the record it turned out to
        belong to."""
        self._join_live_set(rec, pend)
        if pend.started or pend.in_live_set:
            self._start_task(st, rec, pend.data)

    def _bind_receipt(self, st: _Session, rec: AgentRecord, task_id: str) -> bool:
        """Bind the task a spawn's receipt names to the spawn's record.

        The receipt is authoritative: the task's waiting frames are applied;
        a record made from the task's frames alone is folded into this one;
        a task guessed for another spawn moves here (and a task guessed for
        this one goes back to waiting). Returns False (nothing bound) only
        when the task belongs to another spawn for certain."""
        if rec.task_id == task_id:
            rec.task_guessed = False
            return True
        other_id = st.by_task.get(task_id)
        other = st.records.get(other_id) if other_id else None
        if other is not None and other is not rec:
            if other.task_guessed:
                self._release_task(st, other)
            elif other.task_only or not other.spawn_tool_use_id:
                if rec.task_id is not None:
                    self._release_task(st, rec)
                self._merge_twin(st, rec, other)
                return True
            else:
                logger.warning(
                    "agents[%s] task %s already belongs to %s; not rebinding to %s",
                    st.sid[:8], task_id, other.id, rec.id,
                )
                return False
        if rec.task_id is not None:
            self._release_task(st, rec)
        rec.task_id = task_id
        rec.task_guessed = False
        st.by_task[task_id] = rec.id
        pend = st.pending.pop(task_id, None)
        if pend is not None:
            self._apply_pending(st, rec, pend)
        return True

    def _release_task(self, st: _Session, rec: AgentRecord) -> None:
        """A guessed task binding was wrong: the task goes back to waiting
        (its own spawn's receipt or the grace gives it a home)."""
        task_id = rec.task_id
        if not task_id:
            return
        if st.by_task.get(task_id) == rec.id:
            st.by_task.pop(task_id, None)
        pend = self._pend(
            st, task_id,
            {"task_type": rec.task_type, "description": rec.description},
            started=rec.started_at is not None, live=rec.in_live_set,
        )
        pend.missing_since = rec.missing_since
        rec.task_id = None
        rec.task_guessed = False
        rec.in_live_set = False
        rec.missing_since = None
        if rec.has_progress_meter:
            # The meter was the other task's: fall back to the record's own usage.
            rec.has_progress_meter = False
            rec.tokens = sum(rec.usage.get(k, 0) for k in (
                "input_tokens", "output_tokens", "cache_read_input_tokens",
                "cache_creation_input_tokens",
            ))
        self._changed(rec, immediate=True)

    def _merge_twin(self, st: _Session, rec: AgentRecord, twin: AgentRecord) -> None:
        """Fold a record made from a task's frames alone into the spawn
        record the task belongs to, then drop it (its last view goes out
        hidden, so open tabs drop it too)."""
        task_id = twin.task_id
        if st.by_task.get(task_id) == twin.id:
            st.by_task.pop(task_id, None)
        twin.task_id = None
        rec.task_id = task_id
        rec.task_guessed = False
        st.by_task[task_id] = rec.id
        rec.task_type = rec.task_type or twin.task_type
        rec.agent_type = rec.agent_type or twin.agent_type
        rec.prompt = rec.prompt or twin.prompt
        rec.description = rec.description or twin.description
        rec.summary = rec.summary or twin.summary
        rec.output_file = rec.output_file or twin.output_file
        rec.activity = rec.activity or twin.activity
        if twin.has_progress_meter:
            rec.tokens, rec.tool_uses = twin.tokens, twin.tool_uses
            rec.has_progress_meter = True
        if twin.in_live_set:
            rec.in_live_set = True
            rec.missing_since = twin.missing_since
            rec.background = True
        starts = [t for t in (rec.started_at, twin.started_at) if t]
        first_start = rec.started_at is None
        rec.started_at = min(starts) if starts else None
        rec.run_started_at = rec.run_started_at or twin.run_started_at
        rec.last_activity_at = max(
            [t for t in (rec.last_activity_at, twin.last_activity_at) if t], default=None,
        )
        rec.client_gen = twin.client_gen if twin.client_gen is not None else rec.client_gen
        rec.runs = max(rec.runs, twin.runs)
        if twin.status == "stopping" and rec.status != "stopping" and rec.live:
            rec.status = "stopping"
            rec.stop_requested_at = twin.stop_requested_at
            rec.stop_requested_mono = twin.stop_requested_mono
            rec.stop_scope = twin.stop_scope
            rec.stopped_by_actor_id = twin.stopped_by_actor_id
            rec.stopped_caused_by_actor_id = twin.stopped_caused_by_actor_id
            rec.stop_unconfirmed = twin.stop_unconfirmed
        if rec.live and twin.terminal:
            rec.error = rec.error or twin.error
            self._terminal(st, rec, twin.status, twin.end_reason or "completed")
        elif rec.live and twin.live:
            if self._set_running(rec) or first_start:
                self._lifecycle(rec, "started")
        logger.info(
            "agents[%s] task %s: record %s folded into %s",
            st.sid[:8], task_id, twin.id, rec.id,
        )
        self._drop_record(st, twin, into=rec)
        self._changed(rec, immediate=True, counts=True)

    def _drop_record(self, st: _Session, rec: AgentRecord, *, into: AgentRecord) -> None:
        """Remove a folded record: memory, routing maps, queued and stored
        rows. Its last view goes out once, hidden (``ambient``)."""
        st.records.pop(rec.id, None)
        for routes in (st.by_task, st.by_spawn, st.by_route):
            for key in [k for k, v in routes.items() if v == rec.id]:
                routes.pop(key, None)
        for key in [k for k, v in st.tool_owner.items() if v == rec.id]:
            st.tool_owner[key] = into.id
        for child in st.records.values():
            if child.parent_agent_id == rec.id:
                child.parent_agent_id = into.id
        self.writer.forget_record(rec)
        rec.ambient = True
        rec.rev = m.next_rev(rec.rev)
        st.tombstones[rec.id] = rec
        self.emitter.upsert(st.sid, rec.id, True)
        self._counts_changed(st.sid)

    def _spawn_task(self, coro: Awaitable[None]) -> None:
        """Run ``coro`` in the background (from sync code), keeping a reference."""
        try:
            task = asyncio.get_running_loop().create_task(coro)
        except RuntimeError:
            coro.close()  # type: ignore[attr-defined]
            return
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    def _task_progress(self, rec: AgentRecord, data: dict) -> None:
        usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
        total = _int(usage.get("total_tokens"))
        tool_uses = _int(usage.get("tool_uses"))
        summary = m.cap_chars(_str(data.get("summary")), m.MAX_SUMMARY)
        if rec.terminal:
            # Late frame: only fill what is still empty.
            if summary and not rec.summary:
                rec.summary = summary
            if total is not None and not rec.tokens:
                rec.tokens = total
            self._changed(rec, immediate=False)
            return
        description = m.cap_chars(_str(data.get("description")), m.MAX_ACTIVITY)
        if description:
            rec.activity = description
        if summary:
            rec.summary = summary
        if total is not None:
            rec.tokens = total
            rec.has_progress_meter = True
        if tool_uses is not None:
            rec.tool_uses = tool_uses
            rec.has_progress_meter = True
        rec.last_activity_at = self._now_iso()
        promoted = self._set_running(rec)
        self._changed(rec, immediate=promoted, counts=promoted)

    def _task_updated(self, st: _Session, rec: AgentRecord, data: dict, patch: dict) -> None:
        if isinstance(patch.get("is_backgrounded"), bool) and rec.live:
            rec.background = patch["is_backgrounded"]
            self._changed(rec, immediate=False)
        status = _str(patch.get("status"))
        if status == "completed":
            self._terminal(st, rec, "completed", "completed")
        elif status == "failed":
            if not rec.terminal and patch.get("error"):
                rec.error = m.cap_text(patch.get("error"), m.MAX_ERROR)[0] or None
            self._terminal(st, rec, "failed", "error")
        elif status == "killed":
            self._terminal(st, rec, "stopped", self._stop_reason(st, rec))

    def _task_notification(self, st: _Session, rec: AgentRecord, data: dict) -> None:
        was_terminal = rec.terminal
        summary = m.cap_chars(_str(data.get("summary")), m.MAX_SUMMARY)
        output_file = _str(data.get("output_file"))
        usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
        total = _int(usage.get("total_tokens"))
        if summary and (not was_terminal or not rec.summary):
            rec.summary = summary
        if output_file and (not was_terminal or not rec.output_file):
            rec.output_file = output_file
        if total is not None and total > rec.tokens:
            rec.tokens = total
        status = _str(data.get("status"))
        if data.get("reason") == "worker_restart":
            settled = self._terminal(st, rec, "interrupted", "process_ended")
        elif status == "completed":
            settled = self._terminal(st, rec, "completed", "completed")
        elif status == "failed":
            settled = self._terminal(st, rec, "failed", "error")
        elif status == "stopped":
            settled = self._terminal(st, rec, "stopped", self._stop_reason(st, rec))
        else:
            settled = False
        if not settled:
            self._changed(rec, immediate=False)

    def _live_set(self, st: _Session, data: dict) -> list[AgentRecord]:
        """``background_tasks_changed``: the full live background set
        (REPLACE). Members are marked; a member that disappears is lost after
        the grace unless a final frame arrives first; records that were never
        members (foreground work) are never narrowed.

        The CLI sends this frame before the task's ``task_started`` and
        before the spawning call's receipt. An unknown member therefore waits
        (no record) while a spawn of its kind awaits its receipt, a listed
        foreground shell has moved to the background (it gets its record
        now), and anything else gets a record of its own. Returns the records
        that got a task id here (their deferred stops can fire)."""
        listed: dict[str, dict] = {}
        for task in data.get("tasks") or []:
            if isinstance(task, dict) and _str(task.get("task_id")):
                listed[str(task["task_id"]).strip()] = task
        bound: list[AgentRecord] = []
        for task_id, task in listed.items():
            task_type = _str(task.get("task_type"))
            kind, ambient = m.kind_for_task_type(task_type)
            rec = self._find(st, task_id=task_id)
            if rec is None:
                entry = {
                    "task_type": task_type,
                    "description": _str(task.get("description")),
                    "ambient": True if task.get("ambient") else None,
                }
                pend = st.pending.get(task_id)
                if pend is not None and pend.foreground:
                    pend.in_live_set = True
                    bound.append(self._materialize(st, pend, background=True))
                    continue
                probe = {**(pend.data if pend is not None else {}),
                         **{k: v for k, v in entry.items() if v is not None}}
                if pend is not None or self._spawn_awaited(st, probe):
                    self._pend(st, task_id, entry, live=True)
                    continue
                rec = self._new_task_record(st, task_id, entry)
                rec.in_live_set = True
                rec.background = True
                self._start_task(st, rec, entry)
                continue
            if rec.terminal:
                continue  # a final frame already settled it
            new_ambient = rec.ambient or bool(task.get("ambient")) or ambient
            changed = (
                not rec.in_live_set
                or rec.missing_since is not None
                or rec.background is not True
                or rec.ambient != new_ambient
            )
            rec.in_live_set = True
            rec.missing_since = None
            rec.background = True
            rec.ambient = new_ambient
            promoted = self._set_running(rec)
            if changed or promoted:
                self._changed(rec, immediate=promoted, counts=promoted)
        now = self._clock()
        for rec in st.records.values():
            if (
                rec.live and rec.in_live_set and rec.task_id not in listed
                and rec.missing_since is None
            ):
                rec.missing_since = now
        for pend in st.pending.values():
            if pend.in_live_set and pend.task_id not in listed and pend.missing_since is None:
                pend.missing_since = now
        return bound

    def set_workflow_snapshot(self, sid: str, tool_use_id: str, snapshot: dict) -> None:
        """The latest workflow-progress snapshot of a ``Workflow`` call."""
        st = self._sessions.get(sid)
        if st is None or not tool_use_id or not isinstance(snapshot, dict):
            return
        rec = self._find(st, tool_use_id=tool_use_id)
        if rec is None:
            return
        rec.workflow = dict(snapshot)
        if not rec.has_progress_meter:
            tokens = _int(snapshot.get("totalTokens"))
            if tokens is not None:
                rec.tokens = tokens
        self._changed(rec, immediate=False)

    def on_interaction(self, sid: str, task_id: str | None, pending: bool) -> None:
        """A foreground agent's interactive prompt opened or closed."""
        st = self._sessions.get(sid)
        if st is None or not task_id:
            return
        rec = self._find(st, task_id=task_id)
        if rec is None or rec.pending_interaction == bool(pending):
            return
        rec.pending_interaction = bool(pending) and rec.live
        self._changed(rec, immediate=True)

    def describe_task(self, sid: str, task_id: str) -> dict | None:
        st = self._sessions.get(sid)
        rec = self._find(st, task_id=task_id) if st else None
        if rec is None:
            return None
        return {"agent_id": rec.id, "task_id": rec.task_id, "label": m.label_of(rec)}

    # ------------------------------------------------------------------ #
    #  Runtime lifecycle                                                  #
    # ------------------------------------------------------------------ #

    def on_client_connected(self, sid: str, client_gen: int, *, backend: str) -> None:
        """A new runtime client for the session: new records take its backend;
        ``resumable`` compares against its generation."""
        st = self._state_sync(sid)
        st.client_gen = client_gen
        st.backend = backend or "claude"

    def on_client_gone(self, sid: str, reason: str) -> None:
        """The session's runtime went away: every live record is interrupted
        (with the reason's end reason). Synchronous; never raises."""
        try:
            st = self._sessions.get(sid)
            if st is None:
                return
            end_reason = CLIENT_GONE_END_REASONS.get(reason, "process_ended")
            for rec in list(st.records.values()):
                if rec.live:
                    self._terminal(st, rec, "interrupted", end_reason)
                rec.in_live_set = False
                rec.missing_since = None
            st.pending.clear()  # the runtime's tasks went with it
            st.client_gen = None
        except Exception:  # noqa: BLE001
            logger.exception("agents[%s] client teardown reconcile failed", sid[:8])

    async def forget_session(self, sid: str) -> None:
        """The session was deleted: drop its records, pending writes and
        pending emits."""
        self._sessions.pop(sid, None)
        self.writer.forget_session(sid)
        self.emitter.forget(sid)

    def settle_turn_scoped(self, sid: str) -> int:
        """The orchestrator's turn ended. Codex collab agents live inside the
        turn, and their result settles them; one still live now got no final
        status and never will (``lost``). Claude's foreground agents are left
        alone: one can move to the background mid-run without a new start
        frame. Returns how many were settled."""
        st = self._sessions.get(sid)
        if st is None:
            return 0
        settled = 0
        for rec in list(st.records.values()):
            if rec.live and rec.backend == "codex" and rec.background is False:
                if self._terminal(st, rec, "lost", "vanished"):
                    settled += 1
        return settled

    def note_turn_stop(self, sid: str, *, kills_background: bool = False) -> None:
        """Stop interrupted the orchestrator's turn: a kill reported within
        30 s is explained by it (end reason ``turn_stop``) for foreground
        records, or for all when the stop was confirmed to kill background
        agents."""
        st = self._state_sync(sid)
        st.turn_stop_at = self._clock()
        st.turn_stop_kills_bg = bool(kills_background)

    # ------------------------------------------------------------------ #
    #  Stop bookkeeping                                                   #
    # ------------------------------------------------------------------ #

    def plan_stop(self, sid: str, agent_id: str, *, cascade: bool) -> list[AgentRecord]:
        """The live records a stop of ``agent_id`` targets: its live
        descendants deepest first (with ``cascade``), then the record."""
        st = self._sessions.get(sid)
        rec = st.records.get(agent_id) if st else None
        if rec is None or not rec.live:
            return []
        targets: list[AgentRecord] = []
        if cascade:
            children: dict[str, list[AgentRecord]] = {}
            for r in st.records.values():
                if r.parent_agent_id:
                    children.setdefault(r.parent_agent_id, []).append(r)
            seen: set[str] = {rec.id}
            stack = list(children.get(rec.id, ()))
            while stack:
                r = stack.pop()
                if r.id in seen:
                    continue
                seen.add(r.id)
                if r.live:
                    targets.append(r)
                stack.extend(children.get(r.id, ()))
            targets.sort(key=lambda r: (-int(r.depth), r.created_at))
        targets.append(rec)
        return targets

    def mark_stopping(
        self, sid: str, records: list[AgentRecord], *, scope: str, attribution: Any,
    ) -> None:
        """Nerve asked the runtime to stop these records."""
        now_iso = self._now_iso()
        now = self._clock()
        actor_id = getattr(attribution, "actor_id", None)
        caused_by = getattr(attribution, "caused_by_actor_id", None)
        for rec in records:
            if not rec.live:
                continue
            rec.status = "stopping"
            rec.stop_requested_at = now_iso
            rec.stop_requested_mono = now
            rec.stop_scope = scope
            rec.stopped_by_actor_id = actor_id
            rec.stopped_caused_by_actor_id = caused_by
            rec.stop_unconfirmed = False
            if not rec.task_id:
                rec.stop_pending = True
            self._lifecycle(rec, "stop_requested", reason=scope, actor_id=actor_id)
            self._changed(rec, immediate=True)
        self.writer.kick()

    # ------------------------------------------------------------------ #
    #  Queries                                                            #
    # ------------------------------------------------------------------ #

    def get(self, sid: str, agent_id: str) -> AgentRecord | None:
        st = self._sessions.get(sid)
        return st.records.get(agent_id) if st else None

    def session_backend(self, sid: str) -> str | None:
        """The backend of the session's last client, when one connected."""
        st = self._sessions.get(sid)
        return st.backend if st is not None and st.client_gen is not None else None

    def live(self, sid: str) -> list[AgentRecord]:
        """Live, non-ambient records, oldest first."""
        st = self._sessions.get(sid)
        if st is None:
            return []
        return [r for r in st.records.values() if r.live and not r.ambient]

    def has_live(self, sid: str) -> bool:
        return self.live_count(sid) > 0

    def live_count(self, sid: str) -> int:
        st = self._sessions.get(sid)
        if st is None:
            return 0
        return sum(1 for r in st.records.values() if r.live and not r.ambient)

    def any_live(self) -> bool:
        """Whether any session has a live, non-ambient agent.

        Every live record is in memory: the sweeper never evicts a session
        with one, and startup marks the live rows of a previous process
        interrupted.
        """
        return any(self.live_count(sid) > 0 for sid in list(self._sessions))

    def add_live_listener(self, listener: Callable[[], None]) -> None:
        """Call ``listener()`` whenever a session's live count may have
        changed. It runs inside ingestion, so it must only hand off; an
        exception it raises is logged."""
        if listener not in self._live_listeners:
            self._live_listeners = (*self._live_listeners, listener)

    def remove_live_listener(self, listener: Callable[[], None]) -> None:
        """Stop calling a listener added with :meth:`add_live_listener`."""
        self._live_listeners = tuple(
            item for item in self._live_listeners if item != listener
        )

    def agent_id_for_spawn(self, sid: str, tool_use_id: str) -> str | None:
        st = self._sessions.get(sid)
        return st.by_spawn.get(tool_use_id) if st and tool_use_id else None

    def take_turn_triggers(self, sid: str) -> tuple[list[dict], int]:
        """Pop the agents that settled since the last turn: up to five
        triggers (most recent last) and how many more there were."""
        st = self._sessions.get(sid)
        if st is None:
            return [], 0
        settled = [r for r in st.records.values() if r.settled_since_turn and not r.ambient]
        for r in st.records.values():
            r.settled_since_turn = False
        settled.sort(key=lambda r: (r.ended_at or "", r.created_at))
        shown = settled[-MAX_TRIGGERS:]
        return [m.trigger_of(r) for r in shown], len(settled) - len(shown)

    def clear_turn_triggers(self, sid: str) -> None:
        """A user turn starts: forget what settled since the last turn (and
        drop settled records from the legacy chip list)."""
        st = self._sessions.get(sid)
        if st is None:
            return
        for r in st.records.values():
            r.settled_since_turn = False
        st.legacy_cutoff = self._now_iso()

    async def take_orchestrator_notes(self, sid: str) -> str | None:
        """A one-off reminder about background work that ended outside the
        orchestrator's control (restart, runtime end, force stop, model
        switch) since its last turn, or ``None``. Marks them notified."""
        st = await self._state(sid)
        due = [
            r for r in st.records.values()
            if r.end_reason in NOTE_END_REASONS
            and r.orchestrator_notified_at is None
            and not r.ambient
        ]
        if not due:
            return None
        due.sort(key=lambda r: r.ended_at or "")
        shown = due[-MAX_NOTES:]
        lines = []
        for rec in shown:
            started = m.parse_iso(rec.run_started_at or rec.started_at or rec.created_at)
            ended = m.parse_iso(rec.ended_at)
            ran = ""
            if started is not None and ended is not None and ended >= started:
                seconds = int(ended - started)
                ran = f"ran {seconds // 60}m" if seconds >= 60 else f"ran {seconds}s"
            when = (rec.ended_at or "")[11:16]
            what = _NOTE_REASON_TEXT.get(rec.end_reason or "", "ended")
            # The label and the activity come from the agent's own input and
            # tool calls: no markup of theirs may close this reminder.
            activity = _note_text(rec.activity)
            detail = "; ".join(p for p in (ran, f"last: {activity}" if activity else "") if p)
            ref = rec.task_id or rec.id
            lines.append(
                f"- {_note_text(m.label_of(rec))} ({ref}): {what}"
                + (f" at {when} UTC" if when else "")
                + (f" ({detail})" if detail else "")
            )
        now = self._now_iso()
        for rec in due:
            rec.orchestrator_notified_at = now
            self.writer.mark_dirty(rec)
        if self._db is not None:
            try:
                await self._db.mark_agents_notified([r.id for r in due], now)
            except Exception:  # noqa: BLE001
                logger.debug("agents[%s] notified mark failed", sid[:8], exc_info=True)
        return (
            "<system-reminder>Background work ended outside your control since "
            "your last turn:\n" + "\n".join(lines) + "\nPartial work may remain in "
            "their working directories. Relaunch them if still needed.</system-reminder>"
        )

    def capabilities(self, sid: str) -> dict[str, Any]:
        """The session's ``AgentCapabilities``."""
        caps: dict[str, Any] = {}
        if self._capabilities_fn is not None:
            try:
                caps = dict(self._capabilities_fn(sid) or {})
            except Exception:  # noqa: BLE001
                logger.debug("agents[%s] capabilities failed", sid[:8], exc_info=True)
        try:
            quiet = int(self._sub().quiet_after_minutes)
        except (TypeError, ValueError):
            quiet = 10
        return {
            "stop_task": bool(caps.get("stop_task")),
            "transcript": bool(caps.get("transcript")),
            "output": bool(caps.get("output")),
            "interrupt_spares_background": bool(caps.get("interrupt_spares_background")),
            "quiet_after_minutes": quiet,
        }

    def _view_ctx(self, sid: str) -> dict[str, Any]:
        st = self._sessions.get(sid)
        return {
            "stop_supported": self.capabilities(sid)["stop_task"],
            "agent_teams": self._agent_teams(),
            "live_client_gen": st.client_gen if st else None,
        }

    def to_view(self, rec: AgentRecord, ctx: dict | None = None) -> dict:
        """The record's ``AgentView``."""
        return m.to_view(rec, **(ctx or self._view_ctx(rec.session_id)))

    def summary(self, sid: str) -> dict[str, Any]:
        """The session's ``AgentSummary`` (ambient records excluded)."""
        counts = {s: 0 for s in (
            "starting", "running", "stopping", "completed", "failed", "stopped",
            "interrupted", "lost",
        )}
        st = self._sessions.get(sid)
        records = [r for r in st.records.values() if not r.ambient] if st else []
        attention = shells_live = tokens = 0
        cost = 0.0
        oldest: str | None = None
        for rec in records:
            counts[rec.status] = counts.get(rec.status, 0) + 1
            if m.attention_of(rec) is not None:
                attention += 1
            tokens += int(rec.tokens)
            cost += float(rec.cost_usd)
            if rec.live:
                if rec.kind in ("shell", "monitor"):
                    shells_live += 1
                started = rec.started_at or rec.created_at
                if started and (oldest is None or started < oldest):
                    oldest = started
        live = counts["starting"] + counts["running"] + counts["stopping"]
        return {
            "total": len(records), "live": live, **counts,
            "attention": attention, "shells_live": shells_live, "tokens": tokens,
            "cost_usd": round(cost, 6), "oldest_live_started_at": oldest,
        }

    def agent_update_message(self, sid: str, agent_ids: Iterable[str]) -> dict | None:
        """The coalesced ``agent_update`` for these ids, built from the
        current records (the emitter calls this at send time)."""
        st = self._sessions.get(sid)
        if st is None:
            return None
        ctx = self._view_ctx(sid)
        ids = list(agent_ids)
        recs = [st.records[a] for a in ids if a in st.records]
        recs = [r for r in recs if not r.ambient]
        # Folded records go out once more, hidden, so open tabs drop them.
        recs += [st.tombstones.pop(a) for a in ids if a in st.tombstones]
        if not recs:
            return None
        recs.sort(key=lambda r: r.created_at)
        return {
            "type": "agent_update",
            "session_id": sid,
            "agents": [self.to_view(r, ctx) for r in recs],
            "summary": self.summary(sid),
        }

    async def snapshot(self, sid: str, *, limit: int = 200) -> dict[str, Any]:
        """The ``AgentSnapshot`` minus ``queue``: every live non-ambient record
        plus the most recent terminal ones up to ``limit``, oldest first."""
        st = await self._state(sid)
        records = [r for r in st.records.values() if not r.ambient]
        live = [r for r in records if r.live]
        ended = sorted((r for r in records if not r.live), key=lambda r: r.created_at)
        room = max(0, int(limit) - len(live))
        keep = live + (ended[-room:] if room else [])
        keep.sort(key=lambda r: r.created_at)
        ctx = self._view_ctx(sid)
        return {
            "session_id": sid,
            "agents": [self.to_view(r, ctx) for r in keep],
            "summary": self.summary(sid),
            "capabilities": self.capabilities(sid),
            "generated_at": self._now_iso(),
        }

    async def detail(self, sid: str, agent_id: str) -> dict | None:
        """``{"agent", "prompt", "result_text"}`` for one record, or None."""
        await self.ensure_loaded(sid)
        rec = self.get(sid, agent_id)
        if rec is None:
            return None
        return {"agent": self.to_view(rec), "prompt": rec.prompt, "result_text": rec.result_text}

    async def transcript(
        self, sid: str, agent_id: str, *, before_seq: int | None = None,
        after_seq: int | None = None, limit: int = 200,
    ) -> dict[str, Any]:
        """A page of one agent's transcript: persisted items merged with the
        writer's unflushed ones (pending copy first, then the database,
        deduplicated by ``seq``), ascending."""
        if before_seq is not None and after_seq is not None:
            raise ValueError("at most one cursor")
        limit = max(0, int(limit))
        pending = self.writer.pending_items(agent_id)  # first: nothing can fall in a gap
        stored: list[dict] = []
        if self._db is not None:
            stored = await self._db.list_agent_events(
                agent_id, before_seq=before_seq, after_seq=after_seq, limit=limit,
            )
        merged: dict[int, dict] = {int(i["seq"]): i for i in stored}
        for item in pending:
            merged.setdefault(int(item["seq"]), item)
        seqs = sorted(merged)
        if after_seq is not None:
            seqs = [s for s in seqs if s > after_seq][:limit]
        else:
            if before_seq is not None:
                seqs = [s for s in seqs if s < before_seq]
            seqs = seqs[-limit:] if limit else []
        items = [merged[s] for s in seqs]
        first = items[0]["seq"] if items else None
        has_more = False
        if first is not None and first > 1:
            has_more = any(int(i["seq"]) < first for i in pending)
            if not has_more and self._db is not None:
                has_more = bool(await self._db.list_agent_events(
                    agent_id, before_seq=first, limit=1,
                ))
        return {
            "agent_id": agent_id,
            "items": items,
            "first_seq": first,
            "last_seq": items[-1]["seq"] if items else None,
            "has_more_before": has_more,
        }

    async def owner_session(self, agent_id: str) -> str | None:
        """The session a record belongs to (also for unloaded sessions)."""
        for sid, st in self._sessions.items():
            if agent_id in st.records:
                return sid
        if self._db is None:
            return None
        row = await self._db.get_session_agent(agent_id)
        return row["session_id"] if row else None

    @staticmethod
    def _legacy_entry(rec: AgentRecord) -> dict | None:
        if rec.ambient or not rec.task_id:
            return None
        if rec.kind == "workflow":
            tool = "Workflow"
        elif rec.kind in ("agent", "teammate", "remote_agent"):
            tool = "Agent"
        else:
            tool = "Bash"
        if rec.live:
            status = "running"
        elif rec.status in ("completed", "stopped"):
            status = "done"
        else:
            status = "failed"
        return {"task_id": rec.task_id, "label": m.label_of(rec), "tool": tool, "status": status}

    def legacy_task_list(self, sid: str) -> list[dict]:
        """The ``background_tasks_update`` chips old tabs render (kept for
        one release): live tasks plus those settled since the last user
        turn."""
        st = self._sessions.get(sid)
        if st is None:
            return []
        out = []
        for rec in st.records.values():
            entry = self._legacy_entry(rec)
            if entry is None:
                continue
            if not rec.live and st.legacy_cutoff and (rec.ended_at or "") <= st.legacy_cutoff:
                continue
            out.append(entry)
        return out


def empty_snapshot(session_id: str, *, quiet_after_minutes: int = 10) -> dict[str, Any]:
    """An ``AgentSnapshot`` with nothing in it (every key present): what a
    client gets when no engine can answer."""
    return {
        "session_id": session_id,
        "agents": [],
        "summary": {
            "total": 0, "live": 0, "starting": 0, "running": 0, "stopping": 0,
            "completed": 0, "failed": 0, "stopped": 0, "interrupted": 0, "lost": 0,
            "attention": 0, "shells_live": 0, "tokens": 0, "cost_usd": 0.0,
            "oldest_live_started_at": None,
        },
        "capabilities": {
            "stop_task": False, "transcript": False, "output": False,
            "interrupt_spares_background": False,
            "quiet_after_minutes": int(quiet_after_minutes),
        },
        "queue": [],
        "generated_at": m.iso_z(time.time()),
    }


def _workflow_name(tool_input: dict) -> str | None:
    """A workflow's name: the ``name`` argument, or ``meta.name`` in an inline
    script."""
    name = tool_input.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    script = tool_input.get("script")
    if isinstance(script, str):
        found = re.search(r"name\s*:\s*['\"]([^'\"]+)['\"]", script)
        if found:
            return found.group(1)
    return None


def _add_usage(totals: dict[str, int], usage: dict) -> None:
    for key, value in usage.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        totals[key] = totals.get(key, 0) + int(value)


def _estimate(fn: Callable[..., float], usage: dict, model: str | None) -> float:
    try:
        return float(fn(usage, model))
    except Exception:  # noqa: BLE001 — an unpriceable frame costs nothing
        return 0.0
