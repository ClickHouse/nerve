"""Runtime-agnostic tool registry — core types.

This module defines the data structures every tool handler operates on,
independent of any specific runtime (Claude Agent SDK, external MCP server,
JSON-RPC, etc.). Handlers receive a :class:`ToolContext` (carrying the
session and all collaborator references) and return a :class:`ToolResult`.

The previous design relied on module-level globals in ``nerve/agent/tools.py``
to pass workspace/db/engine references to handlers, which was unsafe under
concurrent sessions (notably the ``_current_session_id`` race). Routing all
state through ``ToolContext`` makes handlers pure functions of their inputs
and lets the same set of tools be served by multiple runtimes side-by-side.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine
    from nerve.config import NerveConfig
    from nerve.db import Database
    from nerve.memory.memu_bridge import MemUBridge
    from nerve.memory.xmemory_bridge import XmemoryBridge
    from nerve.notifications.service import NotificationService
    from nerve.skills.manager import SkillManager


@dataclass
class ToolContext:
    """Per-invocation context passed to every tool handler.

    Constructed once per session inside ``engine.run()`` (or once per MCP
    connection for the future external server) and threaded through every
    handler call. Handlers MUST NOT read state from anywhere else.

    Fields are concrete types rather than Protocols to keep call sites
    readable; tests construct ``ToolContext`` with mocks/None for fields a
    given handler doesn't need.
    """

    session_id: str
    workspace: "Path | None" = None
    db: "Database | None" = None
    memory_bridge: "MemUBridge | None" = None
    xmemory_bridge: "XmemoryBridge | None" = None
    config: "NerveConfig | None" = None
    skill_manager: "SkillManager | None" = None
    engine: "AgentEngine | None" = None
    notification_service: "NotificationService | None" = None


@dataclass
class ToolResult:
    """Normalized return value from any handler.

    Adapters translate this to whichever wire format the runtime expects:
    the Claude Agent SDK takes ``{"content": [...], "is_error": bool}``,
    a future stdio MCP server emits ``CallToolResult``, and so on.

    ``content`` is a list of MCP content blocks — typically
    ``[{"type": "text", "text": "..."}]``.

    ``structured`` is reserved for future use; the current adapters ignore
    it. Tools that want to return JSON payloads should still serialize them
    into a text block for now so behavior is identical across runtimes.
    """

    content: list[dict]
    is_error: bool = False
    structured: dict | None = None

    @classmethod
    def text(cls, message: str, *, is_error: bool = False) -> "ToolResult":
        """Convenience: build a ToolResult wrapping a single text block."""
        return cls(content=[{"type": "text", "text": message}], is_error=is_error)

    def to_dict(self) -> dict:
        """Serialize to the dict shape Claude Agent SDK tools return."""
        return {"content": self.content, "is_error": self.is_error}


ToolHandler = Callable[[ToolContext, dict], Awaitable[ToolResult]]


@dataclass(frozen=True)
class ToolSpec:
    """Static metadata for a registered tool.

    A spec is a value object — no runtime state. Adapters consume specs to
    register tools with whichever protocol they speak.

    ``input_schema`` is a JSON Schema object stored at module level (not
    rebuilt per call) so we avoid the closure-allocation overhead the old
    decorator suffered from.
    """

    name: str
    description: str
    input_schema: dict
    handler: ToolHandler


class ToolRegistry:
    """Catalog of registered tool specs.

    The registry is built once at engine startup via :func:`build_default_registry`
    and queried per session when an adapter constructs a runtime-specific
    server. It is intentionally minimal — no lifecycle, no per-call state.
    """

    def __init__(self) -> None:
        self._specs: dict[str, ToolSpec] = {}

    def register(self, spec: ToolSpec) -> None:
        if spec.name in self._specs:
            raise ValueError(f"Tool {spec.name!r} already registered")
        self._specs[spec.name] = spec

    def get(self, name: str) -> ToolSpec | None:
        return self._specs.get(name)

    def list(self, *, include_hoa: bool = False) -> list[ToolSpec]:
        """Return all registered specs, optionally filtering HoA tools.

        HoA tools are only useful when ``config.houseofagents.enabled`` is
        true. The caller decides whether to include them so the SDK adapter
        can drop them from sessions where HoA is off (saves context tokens).
        """
        return [
            spec for spec in self._specs.values()
            if include_hoa or not spec.name.startswith("hoa_")
        ]

    def __contains__(self, name: str) -> bool:
        return name in self._specs

    def __len__(self) -> int:
        return len(self._specs)

    async def invoke(
        self, name: str, ctx: ToolContext, args: dict,
    ) -> ToolResult:
        """Invoke a tool by name. Raises KeyError if not registered.

        This is the canonical entry point for callers outside the agent
        loop — HTTP routes, the future external MCP server, internal
        cross-tool dispatch — so the handler signature stays uniform.
        """
        spec = self._specs.get(name)
        if spec is None:
            raise KeyError(f"Unknown tool: {name!r}")
        return await spec.handler(ctx, args)
