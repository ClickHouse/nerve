"""Where a session comes from: its trigger and the details of its origin.

Each session row has a ``trigger``: the kind of event that started the
session, from the closed vocabulary :data:`TRIGGERS`. The code path that
creates the session sets it once. ``source`` is a different field: free text,
partly set by REST callers, and one value can belong to different triggers
(a plan implementation session has the source ``web``).

``origin_detail`` is a JSON object with the details that the UI shows, for
example the Slack channel or the cron job. Its keys are the ``DETAIL_*``
constants below. Each key is optional.

A row from before the column existed has no trigger, and neither has a row
whose creator gave no origin. :func:`derive_origin` gives the origin of such
a row at read time from ``source``, the session ID, the parent session, the
metadata and the channel mapping.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field

# A person in the web UI, also the onboarding conversation and forks.
TRIGGER_CHAT = "chat"
# ``/api/chat`` and other REST callers, also webhooks.
TRIGGER_API = "api"
TRIGGER_SLACK = "slack"
TRIGGER_TELEGRAM = "telegram"
TRIGGER_EMAIL = "email"
# Cron jobs and "Run later".
TRIGGER_CRON = "cron"
# Plan implementation and planner sessions.
TRIGGER_PLAN = "plan"
# Workflow runs and review loop legs.
TRIGGER_WORKFLOW = "workflow"
# MCP satellite sessions and imported threads.
TRIGGER_EXTERNAL = "external"
# Nerve's own model calls outside a session turn. No session has it.
TRIGGER_SYSTEM = "system"
# No other value applies.
TRIGGER_UNKNOWN = "unknown"

TRIGGERS = frozenset({
    TRIGGER_CHAT, TRIGGER_API, TRIGGER_SLACK, TRIGGER_TELEGRAM, TRIGGER_EMAIL,
    TRIGGER_CRON, TRIGGER_PLAN, TRIGGER_WORKFLOW, TRIGGER_EXTERNAL,
    TRIGGER_SYSTEM, TRIGGER_UNKNOWN,
})

DETAIL_CHANNEL = "channel"            # conversation ID, for example a Slack channel ID
DETAIL_CHANNEL_NAME = "channel_name"  # its display name, when the channel knows it
DETAIL_THREAD = "thread"              # thread ID in the conversation
DETAIL_CRON_JOB = "cron_job"
DETAIL_RUN_LATER = "run_later"        # the delay that the person chose
DETAIL_PLAN_ID = "plan_id"
DETAIL_TASK_ID = "task_id"
DETAIL_FORK_OF = "fork_of"            # the session that the fork copies
DETAIL_WORKFLOW_RUN = "workflow_run"
DETAIL_CLIENT = "client"              # name of the external client
DETAIL_HOOK = "hook"

# Channel name (and ``source``) of a channel session, to its trigger.
_CHANNEL_TRIGGERS = {
    "web": TRIGGER_CHAT,
    "api": TRIGGER_API,
    "slack": TRIGGER_SLACK,
    "telegram": TRIGGER_TELEGRAM,
    "email": TRIGGER_EMAIL,
}

# Other ``source`` values of a row without a better clue, to its trigger.
_SOURCE_TRIGGERS = {
    "cron": TRIGGER_CRON,
    "hook": TRIGGER_API,
    "workflow": TRIGGER_WORKFLOW,
    "external": TRIGGER_EXTERNAL,
}

# Session ID prefixes that Nerve gives, to the trigger and the detail key of
# the second ID part (``cron:<job>:<run>``, ``hook:<name>:<id>``).
_ID_PREFIXES: dict[str, tuple[str, str | None]] = {
    "cron:": (TRIGGER_CRON, DETAIL_CRON_JOB),
    "hook:": (TRIGGER_API, DETAIL_HOOK),
    "workflow:": (TRIGGER_WORKFLOW, DETAIL_WORKFLOW_RUN),
    "impl-": (TRIGGER_PLAN, None),
    "onboarding-": (TRIGGER_CHAT, None),
}

# The label of each trigger when the details give no better one.
_LABELS = {
    TRIGGER_CHAT: "Started in the web chat",
    TRIGGER_API: "Started through the API",
    TRIGGER_SLACK: "Started from Slack",
    TRIGGER_TELEGRAM: "Started from Telegram",
    TRIGGER_EMAIL: "Started from email",
    TRIGGER_CRON: "Started by a scheduled run",
    TRIGGER_PLAN: "Started by a plan",
    TRIGGER_WORKFLOW: "Started by a workflow run",
    TRIGGER_EXTERNAL: "Started in an external client",
    TRIGGER_SYSTEM: "Started by Nerve",
    TRIGGER_UNKNOWN: "Started from an unknown origin",
}


@dataclass(frozen=True)
class Origin:
    """The trigger of a session and the details for display."""

    trigger: str
    detail: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.trigger not in TRIGGERS:
            raise ValueError(f"unknown session trigger {self.trigger!r}")
        # Empty values are left out, so a detail is present or absent.
        object.__setattr__(
            self, "detail", {k: str(v) for k, v in self.detail.items() if v},
        )

    def detail_json(self) -> str | None:
        """The stored ``origin_detail`` value: JSON, or ``None`` when empty."""
        return json.dumps(self.detail) if self.detail else None


def channel_origin(
    channel_name: str, channel_key: str, conversation_title: str = "",
) -> Origin:
    """The origin of a session that a channel message or command creates.

    ``channel_key`` is ``<channel>:<conversation>[:<thread>]``, for example
    ``slack:C0123ABCD:1700000000.000100`` or ``telegram:12345``.
    ``conversation_title`` is the display name of the conversation, when the
    channel knows it. The web and API channel keys carry no details.
    """
    trigger = _CHANNEL_TRIGGERS.get(channel_name, TRIGGER_UNKNOWN)
    if trigger in (TRIGGER_CHAT, TRIGGER_API):
        return Origin(trigger)
    _, _, rest = channel_key.partition(":")
    conversation, _, thread = rest.partition(":")
    return Origin(trigger, {
        DETAIL_CHANNEL: conversation,
        DETAIL_THREAD: thread,
        DETAIL_CHANNEL_NAME: conversation_title,
    })


def cron_origin(job_id: str) -> Origin:
    """The origin of a session that a run of cron job ``job_id`` creates."""
    return Origin(TRIGGER_CRON, {DETAIL_CRON_JOB: job_id})


def plan_origin(plan_id: str, task_id: str) -> Origin:
    """The origin of a session that implements or revises a plan."""
    return Origin(TRIGGER_PLAN, {DETAIL_PLAN_ID: plan_id, DETAIL_TASK_ID: task_id})


def external_origin(client: str) -> Origin:
    """The origin of a session of an agent that runs outside Nerve."""
    return Origin(TRIGGER_EXTERNAL, {DETAIL_CLIENT: client})


def _metadata(row: Mapping) -> dict:
    raw = row.get("metadata")
    if isinstance(raw, dict):
        return raw
    try:
        value = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def derive_origin(row: Mapping, channel_key: str | None = None) -> Origin:
    """The origin of a session row that has no stored trigger.

    Uses, in this order: the session ID prefixes that Nerve gives, a fork's
    parent (a fork copies the ``source`` of its parent), the channel
    ``source`` values with the channel mapping ``channel_key`` (the latest
    mapped channel of the session, if any), and then ``source`` alone. An
    unknown ``source`` gives ``unknown``.
    """
    source = row.get("source") or ""
    session_id = row.get("id") or ""
    for prefix, (trigger, key) in _ID_PREFIXES.items():
        if session_id.startswith(prefix):
            part = session_id[len(prefix):].split(":", 1)[0]
            return Origin(trigger, {key: part} if key else {})
    if session_id.startswith("fork-") and row.get("parent_session_id"):
        return Origin(TRIGGER_CHAT, {DETAIL_FORK_OF: row["parent_session_id"]})
    if source in _CHANNEL_TRIGGERS:
        return channel_origin(source, channel_key or "")
    trigger = _SOURCE_TRIGGERS.get(source, TRIGGER_UNKNOWN)
    if trigger == TRIGGER_EXTERNAL:
        client = _metadata(row).get("client_name")
        return Origin(trigger, {DETAIL_CLIENT: client} if isinstance(client, str) else {})
    return Origin(trigger)


def session_origin(row: Mapping, channel_key: str | None = None) -> Origin:
    """The stored origin of a session row, or the derived one for an old row."""
    trigger = row.get("trigger")
    if trigger not in TRIGGERS:
        return derive_origin(row, channel_key)
    try:
        detail = json.loads(row.get("origin_detail") or "{}")
    except (TypeError, ValueError):
        detail = {}
    return Origin(trigger, detail if isinstance(detail, dict) else {})


def origin_label(origin: Origin) -> str:
    """One short sentence that says where the session comes from."""
    trigger, detail = origin.trigger, origin.detail
    if trigger == TRIGGER_SLACK:
        if name := detail.get(DETAIL_CHANNEL_NAME):
            return f"Started from Slack #{name}"
        if detail.get(DETAIL_CHANNEL, "").startswith("D"):
            return "Started from a Slack direct message"
        return "Started from Slack"
    if trigger == TRIGGER_API and (hook := detail.get(DETAIL_HOOK)):
        return f"Started by webhook {hook}"
    if trigger == TRIGGER_CRON:
        if detail.get(DETAIL_RUN_LATER):
            return "Scheduled with Run later"
        if job := detail.get(DETAIL_CRON_JOB):
            return f"Started by cron job {job}"
    if trigger == TRIGGER_EXTERNAL and (client := detail.get(DETAIL_CLIENT)):
        return f"Started in {client}"
    if trigger == TRIGGER_CHAT and detail.get(DETAIL_FORK_OF):
        return "Forked from another chat"
    return _LABELS.get(trigger, _LABELS[TRIGGER_UNKNOWN])


def origin_fields(row: Mapping, channel_key: str | None = None) -> dict:
    """The origin fields of a session in the API: trigger, label, details."""
    origin = session_origin(row, channel_key)
    return {
        "trigger": origin.trigger,
        "origin_label": origin_label(origin),
        "origin_detail": dict(origin.detail),
    }
