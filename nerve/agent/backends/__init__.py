"""Agent backends — pluggable runtimes behind one engine seam.

The engine constructs every configured backend once at init via
:func:`build_backends` and resolves one per session (sticky — see
docs/plans/codex-backend.md §3). Backends receive their collaborators
through :class:`BackendDeps` to avoid engine↔backend import cycles.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from nerve.agent.backends.base import (
    AgentBackend,
    AgentClient,
    BackendCapabilities,
    BackendError,
    SessionSpec,
    TransportDiedError,
    TurnInput,
)
from nerve.agent.backends.events import (
    AgentEvent,
    ModelObserved,
    NormalizedUsage,
    SubagentStarted,
    SystemEvent,
    TextDelta,
    ThinkingDelta,
    ToolResult,
    ToolOutputDelta,
    ToolUse,
    TurnCompleted,
)

if TYPE_CHECKING:
    from nerve.config import NerveConfig
    from nerve.db import Database


@dataclass
class BackendDeps:
    """Collaborators a backend may need, provided by the engine.

    Callables (rather than direct references) where the underlying value
    is hot-reloadable or wired up after engine construction.
    """

    config: "NerveConfig"
    db: "Database"
    registry: Any                                   # ToolRegistry
    tool_ctx_factory: Callable[[str], Any]          # session_id -> ToolContext
    external_mcp_servers: Callable[[], list]        # live McpServerConfig cache
    claude_plugins: Callable[[], list] = field(default=lambda: [])
    # Codex tool-bridge collaborators (None until the gateway wires them):
    gateway_port: Callable[[], int | None] = field(default=lambda: None)
    mint_session_token: Callable[[str], str] | None = None


def build_backends(deps: BackendDeps) -> dict[str, AgentBackend]:
    """Construct every known backend.

    Both backends are always constructed (construction is cheap and has
    no side effects beyond mkdir of the codex home): a session created on
    codex must stay resumable even after the operator flips the config
    default back to claude — the sticky ``sessions.backend`` column
    routes to the stored backend regardless of current config.
    """
    from nerve.agent.backends.claude import ClaudeBackend
    from nerve.agent.backends.codex import CodexBackend

    return {
        "claude": ClaudeBackend(deps),
        "codex": CodexBackend(deps),
    }


__all__ = [
    "AgentBackend",
    "AgentClient",
    "AgentEvent",
    "BackendCapabilities",
    "BackendDeps",
    "BackendError",
    "ModelObserved",
    "NormalizedUsage",
    "SessionSpec",
    "SubagentStarted",
    "SystemEvent",
    "TextDelta",
    "ThinkingDelta",
    "ToolResult",
    "ToolOutputDelta",
    "ToolUse",
    "TransportDiedError",
    "TurnCompleted",
    "TurnInput",
    "build_backends",
]
