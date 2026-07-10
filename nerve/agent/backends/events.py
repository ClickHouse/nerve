"""Normalized agent events — the engine↔backend boundary vocabulary.

Every backend translates its native stream into these types; the engine
consumes ONLY these. No ``claude_agent_sdk`` or Codex protocol types may
cross this boundary (see docs/plans/codex-backend.md §1/§4).

Design notes:

* One native message may translate into several events (a multi-block
  Claude ``AssistantMessage`` yields one event per block).
* ``TurnCompleted`` is the terminal event of every turn — including
  interrupted and failed turns — so the engine's turn loop has exactly
  one exit shape.
* ``NormalizedUsage.to_anthropic_shape()`` defines the usage-dict
  contract for everything downstream of the engine (DB usage rows, the
  ``done`` broadcast the web UI reads, the cache-TTL split). The Claude
  backend passes its native dict through untouched (it already IS this
  shape); Codex maps its camelCase breakdown into it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class NormalizedUsage:
    """Backend-neutral token usage for a single turn.

    ``raw`` retains the backend-native payload for diagnostics (persisted
    inside the anthropic-shaped dict under ``_raw`` when it differs).
    """

    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_anthropic(cls, usage: dict[str, Any]) -> "NormalizedUsage":
        """Build from an Anthropic-shaped usage dict (Claude backend)."""
        return cls(
            input_tokens=int(usage.get("input_tokens") or 0),
            output_tokens=int(usage.get("output_tokens") or 0),
            cache_read_tokens=int(usage.get("cache_read_input_tokens") or 0),
            cache_creation_tokens=int(usage.get("cache_creation_input_tokens") or 0),
            raw=usage,
        )

    def to_anthropic_shape(self) -> dict[str, Any]:
        """The engine-facing usage dict.

        For the Claude backend ``raw`` is already Anthropic-shaped and is
        returned untouched — preserving nested fields the cache-TTL split
        reads (``cache_creation.ephemeral_*``) byte-for-byte. For other
        backends the canonical keys are synthesized and the native payload
        is kept under ``_raw``.
        """
        if "input_tokens" in self.raw:
            return self.raw
        shaped: dict[str, Any] = {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_read_input_tokens": self.cache_read_tokens,
            "cache_creation_input_tokens": self.cache_creation_tokens,
        }
        if self.raw:
            shaped["_raw"] = self.raw
        return shaped


@dataclass
class TextDelta:
    """Streamed assistant text."""

    text: str
    parent_tool_use_id: str | None = None


@dataclass
class ThinkingDelta:
    """Streamed reasoning/thinking text."""

    text: str
    parent_tool_use_id: str | None = None


@dataclass
class ToolUse:
    """The agent invoked a tool."""

    tool_use_id: str | None
    name: str
    input: dict[str, Any]
    parent_tool_use_id: str | None = None


@dataclass
class ToolResult:
    """A tool finished; ``content`` mirrors the SDK's content shape
    (string, list of content blocks, or None)."""

    tool_use_id: str | None
    content: Any
    is_error: bool = False
    parent_tool_use_id: str | None = None


@dataclass
class SubagentStarted:
    """A sub-agent (Claude ``Task``/``Agent`` tool) began. Claude-only."""

    tool_use_id: str
    subagent_type: str
    description: str
    model: str | None = None


@dataclass
class ModelObserved:
    """The serving model was observed/changed mid-stream.

    Claude: ``AssistantMessage.model`` on main-agent messages.
    Codex: resolved thread model at turn start + ``model/rerouted``.
    Feeds ``st.last_model`` / serving-model change detection.
    """

    model: str


@dataclass
class SystemEvent:
    """Backend system/meta message passthrough.

    Claude: ``SystemMessage`` subtypes (init, task_* lifecycle chips,
    workflow progress). Codex: plan deltas (``codex_plan``), retryable
    errors (``codex_error``), rate-limit updates. The engine routes by
    ``subtype``; unknown subtypes are informational only.
    """

    subtype: str
    data: dict[str, Any] = field(default_factory=dict)


TurnStatus = Literal["completed", "interrupted", "failed"]


@dataclass
class TurnCompleted:
    """Terminal event of a turn — always emitted exactly once per turn.

    ``total_cost_usd`` semantics depend on the backend's
    ``cost_is_cumulative`` capability: Claude reports a process-cumulative
    figure (the engine diffs it); Codex reports THIS turn's cost,
    pre-computed from the pricing table (``None`` when the model has no
    table entry — never estimated).
    """

    native_session_id: str | None = None
    model: str | None = None
    usage: NormalizedUsage | None = None
    total_cost_usd: float | None = None
    duration_ms: int | None = None
    duration_api_ms: int | None = None
    num_turns: int | None = None
    context_window: int | None = None
    status: TurnStatus = "completed"
    error: str | None = None


AgentEvent = (
    TextDelta
    | ThinkingDelta
    | ToolUse
    | ToolResult
    | SubagentStarted
    | ModelObserved
    | SystemEvent
    | TurnCompleted
)
