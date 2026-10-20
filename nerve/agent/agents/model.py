"""The agent record: one sub-agent or background task of a session.

Pure data and derivations, no I/O: the record dataclass, the status and kind
vocabularies, the kind mapping from CLI task types and spawning tools, the
public view (:func:`to_view`, the ``AgentView`` JSON of the API contract),
payload caps, and the row shape persisted in ``session_agents``.

A record is created at the spawning ``ToolUse`` (status ``starting``), binds
the CLI ``task_id`` when the CLI reports it, and settles on the first terminal
frame. Its public id (``ag_`` + 12 hex) never changes.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from urllib.parse import urlparse

AgentKind = Literal[
    "agent", "workflow", "shell", "monitor", "teammate", "remote_agent",
    "mcp_task", "other",
]
AgentStatus = Literal[
    "starting", "running", "stopping", "completed", "failed", "stopped",
    "interrupted", "lost",
]
LIVE_STATUSES: frozenset[str] = frozenset({"starting", "running", "stopping"})
TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"completed", "failed", "stopped", "interrupted", "lost"},
)
EndReason = Literal[
    "completed", "error", "spawn_failed",                      # how the work went
    "user", "stop_all", "turn_stop", "orchestrator", "unknown_stop",   # who stopped it
    "process_ended", "gateway_restart", "force_stop", "model_switch", "archived",  # runtime went away
    "vanished",                                                 # left the CLI's live set with no final frame
]
END_REASONS: frozenset[str] = frozenset({
    "completed", "error", "spawn_failed",
    "user", "stop_all", "turn_stop", "orchestrator", "unknown_stop",
    "process_ended", "gateway_restart", "force_stop", "model_switch", "archived",
    "vanished",
})
Attention = Literal["needs_input", "failed", "interrupted", "lost", "stop_unconfirmed"]
LifecycleEvent = Literal[
    "started", "resumed", "stop_requested", "completed", "failed", "stopped",
    "interrupted", "lost",
]

# Tools whose call spawns an agent record (plus Bash with
# input.run_in_background is True).
SPAWN_TOOLS: frozenset[str] = frozenset({"Agent", "Task", "Workflow", "Monitor"})
AGENT_SPAWN_TOOLS: frozenset[str] = frozenset({"Agent", "Task"})
# A user stop ends an agent for good: the CLI refuses a later SendMessage
# resume of an agent the user stopped.
USER_STOP_REASONS: frozenset[str] = frozenset({"user", "stop_all"})

# CLI task_type -> record kind (local_bash depends on the spawning tool).
_KIND_BY_TASK_TYPE: dict[str, str] = {
    "local_agent": "agent",
    "local_workflow": "workflow",
    "in_process_teammate": "teammate",
    "remote_agent": "remote_agent",
    "mcp_task": "mcp_task",
    "monitor_mcp": "monitor",
    "monitor_ws": "monitor",
}
# CLI housekeeping tasks: hidden everywhere, never counted.
AMBIENT_TASK_TYPES: frozenset[str] = frozenset({"dream", "auto_mode_scan"})
# The spawn record kinds a CLI task of each kind can belong to (local_bash
# tasks come from Bash or Monitor; remote agents and teammates from Agent).
SPAWN_KINDS_FOR_TASK_KIND: dict[str, frozenset[str]] = {
    "agent": frozenset({"agent"}),
    "remote_agent": frozenset({"agent"}),
    "teammate": frozenset({"agent"}),
    "workflow": frozenset({"workflow"}),
    "shell": frozenset({"shell", "monitor"}),
    "monitor": frozenset({"monitor"}),
}

# Payload caps (bytes of UTF-8 unless noted).
MAX_PROMPT = 32_768
MAX_RESULT = 32_768
MAX_TEXT_ITEM = 32_768
# The tool-payload cap is configurable (agent.subagents.transcript_item_max_bytes).
DEFAULT_MAX_TOOL_PAYLOAD = 16_384
MAX_COMMAND = 2048
MAX_ERROR = 2048
MAX_ACTIVITY = 160            # characters
MAX_SUMMARY = 400             # characters
MAX_RESULT_PREVIEW = 200      # characters
LABEL_COMMAND_CHARS = 60
RECENT_TOOL_CALLS = 64

_SURROGATE_RE = re.compile(r"[\ud800-\udfff]")


# ---------------------------------------------------------------------- #
#  Small helpers                                                          #
# ---------------------------------------------------------------------- #

def new_agent_id() -> str:
    """A fresh public agent id: ``ag_`` + 12 hex digits."""
    return "ag_" + secrets.token_hex(6)


def iso_z(ts: float) -> str:
    """A UTC ISO-8601 timestamp ending in ``Z`` (millisecond precision,
    fixed width, so string order is time order)."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def parse_iso(value: str | None) -> float | None:
    """Seconds since the epoch for an ISO-8601 timestamp, or ``None``."""
    if not value:
        return None
    try:
        text = value.replace("Z", "+00:00") if value.endswith("Z") else value
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def next_rev(rev: int) -> int:
    """The revision after ``rev``: strictly larger, and at least the current
    time in microseconds, so it keeps increasing across restarts."""
    return max(rev + 1, time.time_ns() // 1000)


def sanitize(text: str) -> str:
    """Replace orphaned UTF-16 surrogates (output cut mid-emoji), which cannot
    be encoded as UTF-8."""
    return _SURROGATE_RE.sub("�", text) if _SURROGATE_RE.search(text) else text


def cap_text(text: Any, max_bytes: int) -> tuple[str, bool, int]:
    """Cut ``text`` to ``max_bytes`` of UTF-8 on a character boundary.

    Returns ``(text, truncated, full_size)`` where ``full_size`` is the byte
    length of the whole text.
    """
    if text is None:
        return "", False, 0
    if not isinstance(text, str):
        text = str(text)
    text = sanitize(text)
    raw = text.encode("utf-8")
    size = len(raw)
    if size <= max_bytes:
        return text, False, size
    return raw[: max(0, max_bytes)].decode("utf-8", "ignore"), True, size


def cap_chars(text: Any, max_chars: int) -> str | None:
    """``text`` as a string of at most ``max_chars`` characters (``None`` stays
    ``None``; empty becomes ``None``)."""
    if text is None:
        return None
    if not isinstance(text, str):
        text = str(text)
    text = sanitize(text)
    return text[:max_chars] if text else None


def _json_size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"))


def _shrink(value: Any, limit: int, depth: int = 0) -> Any:
    """Truncate every string in a JSON-like value to ``limit`` bytes, and
    long lists to 50 items."""
    if isinstance(value, str):
        return cap_text(value, limit)[0]
    if isinstance(value, dict):
        if depth > 8:
            return "…"
        return {str(k): _shrink(v, limit, depth + 1) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        if depth > 8:
            return "…"
        items = [_shrink(v, limit, depth + 1) for v in list(value)[:50]]
        if len(value) > 50:
            items.append(f"… {len(value) - 50} more")
        return items
    return value


def cap_tool_input(tool_input: Any, max_bytes: int) -> tuple[dict, bool, int]:
    """A tool input that fits ``max_bytes`` of JSON.

    Keeps the structure (keys) and truncates string values until it fits;
    as a last resort the input becomes ``{"_preview": <JSON prefix>}``.
    Returns ``(input, truncated, full_size)``.
    """
    if not isinstance(tool_input, dict):
        tool_input = {"value": tool_input} if tool_input is not None else {}
    try:
        size = _json_size(tool_input)
    except (TypeError, ValueError):
        tool_input = {"value": str(tool_input)}
        size = _json_size(tool_input)
    if size <= max_bytes:
        return sanitize_json(tool_input), False, size
    limit = max(64, max_bytes // 4)
    while limit >= 64:
        shrunk = _shrink(tool_input, limit)
        if _json_size(shrunk) <= max_bytes:
            return shrunk, True, size
        limit //= 2
    # Last resort: a prefix of the JSON text. Escaping can grow it again
    # when it is stored as a string, so shrink until the whole thing fits.
    raw = json.dumps(tool_input, ensure_ascii=False, default=str)
    budget = max(0, max_bytes - 16)
    while True:
        capped = {"_preview": cap_text(raw, budget)[0]}
        capped_size = _json_size(capped)
        if capped_size <= max_bytes or budget == 0:
            return capped, True, size
        budget = max(0, min(budget - 1, budget * max_bytes // capped_size - 1))


def sanitize_json(value: Any) -> Any:
    """``value`` with every string's orphaned surrogates replaced."""
    if isinstance(value, str):
        return sanitize(value)
    if isinstance(value, dict):
        return {str(k): sanitize_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_json(v) for v in value]
    return value


def kind_for_spawn(tool: str, tool_input: Any) -> str | None:
    """The record kind a main- or sidechain ``ToolUse`` spawns, or ``None``
    when the call spawns nothing."""
    if tool in AGENT_SPAWN_TOOLS:
        return "agent"
    if tool == "Workflow":
        return "workflow"
    if tool == "Monitor":
        return "monitor"
    if (
        tool == "Bash"
        and isinstance(tool_input, dict)
        and tool_input.get("run_in_background") is True
    ):
        return "shell"
    return None


def kind_for_task_type(
    task_type: str | None, spawn_tool: str | None = None,
) -> tuple[str, bool]:
    """``(kind, ambient)`` for a CLI ``task_type``.

    ``local_bash`` is a shell unless the spawning tool was ``Monitor``
    (the CLI reports both as ``local_bash``).
    """
    tt = (task_type or "").strip()
    if tt == "local_bash":
        return ("monitor" if spawn_tool == "Monitor" else "shell"), False
    if tt in AMBIENT_TASK_TYPES:
        return "other", True
    return _KIND_BY_TASK_TYPE.get(tt, "other"), False


def summarize_activity(tool: str, tool_input: Any) -> str:
    """One line naming what a tool call does, at most 160 characters."""
    inp = tool_input if isinstance(tool_input, dict) else {}
    tool = tool or "tool"

    def _s(key: str) -> str:
        value = inp.get(key)
        return value.strip() if isinstance(value, str) else ""

    detail = ""
    if tool == "Bash":
        command = _s("command")
        detail = command.splitlines()[0].strip() if command else ""
    elif tool in ("Read", "Write", "Edit", "NotebookEdit"):
        path = _s("file_path") or _s("notebook_path")
        detail = os.path.basename(path.rstrip("/")) or path
    elif tool in ("Grep", "Glob"):
        detail = _s("pattern")
    elif tool == "WebFetch":
        url = _s("url")
        try:
            detail = (urlparse(url).hostname or url) if url else ""
        except ValueError:
            detail = url
    elif tool == "WebSearch":
        detail = _s("query")
    elif tool in AGENT_SPAWN_TOOLS:
        detail = _s("description")
    elif tool.startswith("mcp__"):
        parts = tool.split("__", 2)
        if len(parts) == 3 and parts[1] and parts[2]:
            return sanitize(f"{parts[1]}/{parts[2]}")[:MAX_ACTIVITY]
    text = f"{tool}: {detail}" if detail else tool
    return sanitize(" ".join(text.split()))[:MAX_ACTIVITY]


def first_line(text: str | None, max_chars: int) -> str | None:
    """The first non-empty line of ``text``, at most ``max_chars``."""
    if not text:
        return None
    for line in str(text).splitlines():
        line = line.strip()
        if line:
            return line[:max_chars]
    return None


# ---------------------------------------------------------------------- #
#  The record                                                             #
# ---------------------------------------------------------------------- #

@dataclass
class AgentRecord:
    # identity (persisted)
    id: str                                  # "ag_" + secrets.token_hex(6); immutable public key
    session_id: str
    backend: str = "claude"                  # "claude" | "codex"
    kind: str = "agent"                      # AgentKind
    task_id: str | None = None               # CLI task id (== agentId for agents); bound when known
    task_type: str | None = None             # raw CLI task_type
    spawn_tool_use_id: str | None = None     # the spawning ToolUse id (main or sidechain)
    route_ids: set[str] = field(default_factory=set)   # parent_tool_use_ids that route here (spawn + resumes)
    parent_agent_id: str | None = None       # None = spawned by the orchestrator
    depth: int = 1
    # brief (persisted)
    name: str | None = None                  # Agent input.name
    description: str = ""
    agent_type: str | None = None            # subagent_type
    prompt: str | None = None                # <= 32 KiB
    command: str | None = None               # shell/monitor command, <= 2 KiB
    requested_model: str | None = None       # Agent input.model alias
    models: list[str] = field(default_factory=list)    # effective models in first-seen order
    model_changed_at: str | None = None      # when a second model was first observed
    background: bool = True
    ambient: bool = False                    # CLI housekeeping: hidden everywhere, never counted
    # lifecycle (persisted)
    status: str = "starting"                 # AgentStatus
    end_reason: str | None = None            # EndReason
    error: str | None = None                 # <= 2 KiB
    created_at: str = ""                     # server UTC ISO-8601 with "Z"
    started_at: str | None = None            # first task_started
    run_started_at: str | None = None        # latest start or resume
    last_activity_at: str | None = None
    ended_at: str | None = None
    runs: int = 1                            # +1 per SendMessage resume
    stop_requested_at: str | None = None
    stop_scope: str | None = None            # "user" | "stop_all"
    stopped_by_actor_id: str | None = None
    stopped_caused_by_actor_id: str | None = None
    orchestrator_notified_at: str | None = None
    # progress (persisted)
    activity: str | None = None              # "Bash: pytest -q", <= 160 chars
    summary: str | None = None               # task_progress/task_notification summary, <= 400 chars
    result_text: str | None = None           # final report, <= 32 KiB
    output_file: str | None = None
    tokens: int = 0
    tool_uses: int = 0
    usage: dict[str, int] = field(default_factory=dict)  # deduped anthropic-shape totals
    cost_usd: float = 0.0                    # live estimate
    workflow: dict | None = None             # latest snapshot for kind == "workflow"
    rev: int = 0
    # memory only
    next_seq: int = 1
    client_gen: int | None = None            # generation of the client that ran it
    in_live_set: bool = False
    missing_since: float | None = None
    stop_pending: bool = False               # a stop was requested before task_id was known
    orchestrator_stop_pending: bool = False  # a TaskStop tool call targeted this record
    pending_interaction: bool = False
    stop_unconfirmed: bool = False
    has_progress_meter: bool = False         # task_progress usage seen: tokens/tool_uses come from the CLI meter
    usage_by_msg: "OrderedDict[str, tuple[str | None, dict]]" = field(default_factory=OrderedDict)
    last_text: str = ""
    recent_tool_calls: "deque[dict]" = field(
        default_factory=lambda: deque(maxlen=RECENT_TOOL_CALLS),
    )
    settled_since_turn: bool = False
    # memory only (implementation): usage and cost already folded out of
    # usage_by_msg (and everything loaded from the database); the shown
    # totals are these plus the open message ids.
    usage_folded: dict[str, int] = field(default_factory=dict)
    cost_folded: float = 0.0
    stop_requested_mono: float | None = None  # clock() of the last Nerve stop request
    created_mono: float | None = None         # clock() at creation (for the lost-spawn rule)
    # The task id was matched to this spawn by a guess (no receipt named it
    # yet): a receipt that names it for another spawn moves it there.
    task_guessed: bool = False
    # Made from a CLI task's frames alone (no spawning call seen yet): the
    # receipt of the spawn the task belongs to folds it into that record.
    task_only: bool = False

    @property
    def live(self) -> bool:
        return self.status in LIVE_STATUSES

    @property
    def terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES


# ---------------------------------------------------------------------- #
#  Derived view fields                                                    #
# ---------------------------------------------------------------------- #

def label_of(rec: AgentRecord) -> str:
    """First non-empty of name, description, the command's first 60 chars,
    task_type, kind."""
    for value in (
        rec.name,
        rec.description,
        (rec.command or "")[:LABEL_COMMAND_CHARS],
        rec.task_type,
        rec.kind,
    ):
        if value and str(value).strip():
            return str(value).strip()
    return rec.kind


def attention_of(rec: AgentRecord) -> str | None:
    """Server-derived attention: the first that applies."""
    if rec.pending_interaction and rec.live:
        return "needs_input"
    if rec.status == "failed":
        return "failed"
    if rec.status == "interrupted":
        return "interrupted"
    if rec.status == "lost":
        return "lost"
    if rec.status == "stopping" and rec.stop_unconfirmed:
        return "stop_unconfirmed"
    return None


def model_changed_of(rec: AgentRecord) -> dict | None:
    if len(set(rec.models)) > 1:
        return {"from": rec.models[0], "to": rec.models[-1], "at": rec.model_changed_at}
    return None


def resumable_of(rec: AgentRecord, live_client_gen: int | None) -> bool:
    """A finished agent the orchestrator can still resume with SendMessage:
    completed or failed on the session's current client, not user-stopped."""
    return (
        rec.status in ("completed", "failed")
        and rec.client_gen is not None
        and live_client_gen is not None
        and rec.client_gen == live_client_gen
        and rec.end_reason not in USER_STOP_REASONS
    )


def controls_of(
    rec: AgentRecord, *, stop_supported: bool, agent_teams: bool,
    live_client_gen: int | None,
) -> dict[str, bool]:
    resumable = resumable_of(rec, live_client_gen)
    return {
        "stop": bool(rec.live and stop_supported and not rec.ambient),
        "transcript": rec.backend == "claude"
        and rec.kind in ("agent", "teammate", "remote_agent"),
        "output": rec.kind in ("shell", "monitor") and bool(rec.output_file),
        "message": bool(
            rec.backend == "claude"
            and rec.kind == "agent"
            and agent_teams
            and (rec.live or resumable)
            and rec.end_reason not in USER_STOP_REASONS
        ),
    }


def to_view(
    rec: AgentRecord, *, stop_supported: bool = False, agent_teams: bool = False,
    live_client_gen: int | None = None,
) -> dict[str, Any]:
    """The ``AgentView`` of the API contract (every key always present)."""
    return {
        "id": rec.id,
        "session_id": rec.session_id,
        "backend": rec.backend,
        "kind": rec.kind,
        "task_id": rec.task_id,
        "task_type": rec.task_type,
        "spawn_tool_use_id": rec.spawn_tool_use_id,
        "parent_agent_id": rec.parent_agent_id,
        "depth": int(rec.depth),
        "name": rec.name,
        "label": label_of(rec),
        "description": rec.description or "",
        "agent_type": rec.agent_type,
        "command": rec.command,
        "background": bool(rec.background),
        "ambient": bool(rec.ambient),
        "status": rec.status,
        "end_reason": rec.end_reason,
        "error": rec.error,
        "attention": attention_of(rec),
        "activity": rec.activity,
        "summary": rec.summary,
        "result_preview": (
            first_line(rec.result_text, MAX_RESULT_PREVIEW)
            or first_line(rec.summary, MAX_RESULT_PREVIEW)
        ),
        "requested_model": rec.requested_model,
        "model": rec.models[-1] if rec.models else None,
        "model_changed": model_changed_of(rec),
        "tokens": int(rec.tokens),
        "tool_uses": int(rec.tool_uses),
        "cost_usd": round(float(rec.cost_usd), 6),
        "runs": int(rec.runs),
        "created_at": rec.created_at,
        "started_at": rec.started_at,
        "run_started_at": rec.run_started_at,
        "last_activity_at": rec.last_activity_at,
        "ended_at": rec.ended_at,
        "stop_requested_at": rec.stop_requested_at,
        "stopped_by_actor_id": rec.stopped_by_actor_id,
        "resumable": resumable_of(rec, live_client_gen),
        "controls": controls_of(
            rec, stop_supported=stop_supported, agent_teams=agent_teams,
            live_client_gen=live_client_gen,
        ),
        "workflow": rec.workflow if rec.kind == "workflow" else None,
        "rev": int(rec.rev),
    }


def brief(rec: AgentRecord) -> dict[str, Any]:
    """The ``AgentBrief`` of the API contract."""
    return {
        "id": rec.id,
        "label": label_of(rec),
        "kind": rec.kind,
        "status": rec.status,
        "started_at": rec.started_at,
        "tokens": int(rec.tokens),
    }


def trigger_of(rec: AgentRecord) -> dict[str, Any]:
    """The ``AutoTurnTrigger`` of the API contract."""
    return {
        "agent_id": rec.id,
        "label": label_of(rec),
        "kind": rec.kind,
        "status": rec.status,
    }


# ---------------------------------------------------------------------- #
#  Persistence shape (session_agents)                                     #
# ---------------------------------------------------------------------- #

# Every session_agents column, in table order.
ROW_COLUMNS: tuple[str, ...] = (
    "id", "session_id", "backend", "kind", "task_id", "task_type",
    "spawn_tool_use_id", "route_ids", "parent_agent_id", "depth", "name",
    "description", "agent_type", "prompt", "command", "requested_model",
    "models", "model_changed_at", "background", "ambient", "status",
    "end_reason", "error", "activity", "summary", "result_text", "output_file",
    "tokens", "tool_uses", "usage", "cost_usd", "runs", "workflow",
    "created_at", "started_at", "run_started_at", "last_activity_at",
    "ended_at", "stop_requested_at", "stop_scope", "stopped_by_actor_id",
    "stopped_caused_by_actor_id", "orchestrator_notified_at", "rev",
    "updated_at",
)
# Columns stored as JSON text.
JSON_COLUMNS: frozenset[str] = frozenset({"route_ids", "models", "usage", "workflow"})


def to_row(rec: AgentRecord, *, updated_at: str) -> dict[str, Any]:
    """The ``session_agents`` row for a record (JSON columns as Python
    values; the store encodes them)."""
    return {
        "id": rec.id,
        "session_id": rec.session_id,
        "backend": rec.backend,
        "kind": rec.kind,
        "task_id": rec.task_id,
        "task_type": rec.task_type,
        "spawn_tool_use_id": rec.spawn_tool_use_id,
        "route_ids": sorted(rec.route_ids),
        "parent_agent_id": rec.parent_agent_id,
        "depth": int(rec.depth),
        "name": rec.name,
        "description": rec.description or "",
        "agent_type": rec.agent_type,
        "prompt": rec.prompt,
        "command": rec.command,
        "requested_model": rec.requested_model,
        "models": list(rec.models),
        "model_changed_at": rec.model_changed_at,
        "background": 1 if rec.background else 0,
        "ambient": 1 if rec.ambient else 0,
        "status": rec.status,
        "end_reason": rec.end_reason,
        "error": rec.error,
        "activity": rec.activity,
        "summary": rec.summary,
        "result_text": rec.result_text,
        "output_file": rec.output_file,
        "tokens": int(rec.tokens),
        "tool_uses": int(rec.tool_uses),
        "usage": dict(rec.usage),
        "cost_usd": float(rec.cost_usd),
        "runs": int(rec.runs),
        "workflow": rec.workflow,
        "created_at": rec.created_at,
        "started_at": rec.started_at,
        "run_started_at": rec.run_started_at,
        "last_activity_at": rec.last_activity_at,
        "ended_at": rec.ended_at,
        "stop_requested_at": rec.stop_requested_at,
        "stop_scope": rec.stop_scope,
        "stopped_by_actor_id": rec.stopped_by_actor_id,
        "stopped_caused_by_actor_id": rec.stopped_caused_by_actor_id,
        "orchestrator_notified_at": rec.orchestrator_notified_at,
        "rev": int(rec.rev),
        "updated_at": updated_at,
    }


def _decoded(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (bytes, str)):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return default
    return value


def from_row(row: dict[str, Any]) -> AgentRecord:
    """A record from a ``session_agents`` row (JSON columns may be text or
    already decoded)."""
    usage = _decoded(row.get("usage"), {}) or {}
    if not isinstance(usage, dict):
        usage = {}
    usage = {str(k): int(v) for k, v in usage.items() if isinstance(v, (int, float))}
    workflow = _decoded(row.get("workflow"), None)
    models = _decoded(row.get("models"), []) or []
    route_ids = _decoded(row.get("route_ids"), []) or []
    cost = float(row.get("cost_usd") or 0.0)
    rec = AgentRecord(
        id=str(row["id"]),
        session_id=str(row["session_id"]),
        backend=row.get("backend") or "claude",
        kind=row.get("kind") or "other",
        task_id=row.get("task_id"),
        task_type=row.get("task_type"),
        spawn_tool_use_id=row.get("spawn_tool_use_id"),
        route_ids={str(r) for r in route_ids if r},
        parent_agent_id=row.get("parent_agent_id"),
        depth=int(row.get("depth") or 1),
        name=row.get("name"),
        description=row.get("description") or "",
        agent_type=row.get("agent_type"),
        prompt=row.get("prompt"),
        command=row.get("command"),
        requested_model=row.get("requested_model"),
        models=[str(m) for m in models if m],
        model_changed_at=row.get("model_changed_at"),
        background=bool(row.get("background", 1)),
        ambient=bool(row.get("ambient", 0)),
        status=row.get("status") or "running",
        end_reason=row.get("end_reason"),
        error=row.get("error"),
        created_at=row.get("created_at") or "",
        started_at=row.get("started_at"),
        run_started_at=row.get("run_started_at"),
        last_activity_at=row.get("last_activity_at"),
        ended_at=row.get("ended_at"),
        runs=int(row.get("runs") or 1),
        stop_requested_at=row.get("stop_requested_at"),
        stop_scope=row.get("stop_scope"),
        stopped_by_actor_id=row.get("stopped_by_actor_id"),
        stopped_caused_by_actor_id=row.get("stopped_caused_by_actor_id"),
        orchestrator_notified_at=row.get("orchestrator_notified_at"),
        activity=row.get("activity"),
        summary=row.get("summary"),
        result_text=row.get("result_text"),
        output_file=row.get("output_file"),
        tokens=int(row.get("tokens") or 0),
        tool_uses=int(row.get("tool_uses") or 0),
        usage=dict(usage),
        cost_usd=cost,
        workflow=workflow if isinstance(workflow, dict) else None,
        rev=int(row.get("rev") or 0),
    )
    # Everything persisted is settled history: new usage adds on top.
    rec.usage_folded = dict(usage)
    rec.cost_folded = cost
    return rec
