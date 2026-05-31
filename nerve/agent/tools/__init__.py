"""Public API for Nerve agent tools.

This package replaces the previous monolithic ``nerve/agent/tools.py``
module. Tool handlers are organized by domain under :mod:`handlers/`.
Schemas are module-level constants in :mod:`schemas`. The Claude Agent
SDK adapter is in :mod:`claude_sdk_adapter`. Other runtime adapters
(stdio MCP, Streamable HTTP, JSON-RPC) will live alongside the SDK one
and consume the same registry.

This module re-exports the new :class:`ToolRegistry` API and a thin
back-compat surface (``init_tools``, ``ALL_TOOLS``, individual SdkMcpTool
re-exports, ``_*_impl`` helpers) so existing callers and tests don't have
to migrate all at once. The shim is the only place that holds onto
process-wide globals; the new runtime path is fully ctx-driven.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from claude_agent_sdk import SdkMcpTool

# --- New public API ---
from nerve.agent.tools.registry import (
    ToolContext,
    ToolHandler,
    ToolRegistry,
    ToolResult,
    ToolSpec,
)
from nerve.agent.tools.claude_sdk_adapter import (
    _shim_schema,
    _wrap_for_sdk,
    build_nerve_mcp_server,
    build_session_mcp_server,
    tool,
)
from nerve.agent.tools.handlers import build_default_registry

logger = logging.getLogger(__name__)


# ----- Legacy module globals (back-compat for tests + lifecycle wiring) -----
#
# These globals exist purely so test fixtures (test_send_file.py patches
# ``tools._workspace`` etc.) and the legacy initialization flow keep
# working without modification. The production runtime path builds a
# ``ToolContext`` per session inside ``engine.run()`` and never touches
# these globals.
#
# ``_notification_service`` is set by ``gateway/server.py`` after engine
# init returns; the others are set by ``init_tools()`` during
# ``engine.initialize()``.

_workspace: Path | None = None
_db: Any = None
_memory_bridge: Any = None
_config: Any = None
_skill_manager: Any = None
_engine: Any = None
_notification_service: Any = None

# DEPRECATED — kept only so the legacy fallback path in
# ``_legacy_ctx_for_session(None)`` doesn't raise. Real session_ids flow
# through the new ToolContext on every call.
_current_session_id: str = "unknown"


def init_tools(
    workspace: Path,
    db: Any,
    memory_bridge: Any = None,
    config: Any = None,
    skill_manager: Any = None,
    engine: Any = None,
) -> None:
    """Back-compat: set legacy module globals.

    The new runtime path constructs :class:`ToolContext` per-session
    inside ``engine.run()``. This shim survives only to keep the legacy
    ``_*_impl`` helpers (used by tests) and the SdkMcpTool re-exports
    (used by HTTP routes pre-migration) working.
    """
    global _workspace, _db, _memory_bridge, _config, _skill_manager, _engine
    _workspace = workspace
    _db = db
    _memory_bridge = memory_bridge
    _config = config
    _skill_manager = skill_manager
    _engine = engine


def _legacy_ctx(session_id: str | None = None) -> ToolContext:
    """Build a :class:`ToolContext` from the legacy module globals.

    Used by legacy ``_*_impl`` helpers (test back-compat) and by the
    auto-generated SdkMcpTool re-exports that callers pre-refactor still
    import directly.
    """
    return ToolContext(
        session_id=session_id or _current_session_id,
        workspace=_workspace,
        db=_db,
        memory_bridge=_memory_bridge,
        config=_config,
        skill_manager=_skill_manager,
        engine=_engine,
        notification_service=_notification_service,
    )


# --- Legacy _*_impl re-exports (test_send_file.py, test_session_mcp.py) ---

async def _notify_impl(args: dict, session_id: str) -> dict:
    """Back-compat. Build ToolContext from legacy globals, dispatch."""
    from nerve.agent.tools.handlers.notifications import notify_handler
    ctx = _legacy_ctx(session_id)
    result = await notify_handler(ctx, args)
    return result.to_dict()


async def _ask_user_impl(args: dict, session_id: str) -> dict:
    """Back-compat for tests."""
    from nerve.agent.tools.handlers.notifications import ask_user_handler
    ctx = _legacy_ctx(session_id)
    result = await ask_user_handler(ctx, args)
    return result.to_dict()


async def _react_impl(args: dict, session_id: str) -> dict:
    """Back-compat for tests."""
    from nerve.agent.tools.handlers.notifications import react_handler
    ctx = _legacy_ctx(session_id)
    result = await react_handler(ctx, args)
    return result.to_dict()


async def _send_sticker_impl(args: dict, session_id: str) -> dict:
    """Back-compat for tests."""
    from nerve.agent.tools.handlers.notifications import send_sticker_handler
    ctx = _legacy_ctx(session_id)
    result = await send_sticker_handler(ctx, args)
    return result.to_dict()


async def _send_file_impl(args: dict, session_id: str) -> dict:
    """Back-compat for tests."""
    from nerve.agent.tools.handlers.notifications import send_file_handler
    ctx = _legacy_ctx(session_id)
    result = await send_file_handler(ctx, args)
    return result.to_dict()


# --- Legacy SdkMcpTool re-exports ---
#
# The default registry is built once at module import. Each named tool
# (task_create, task_update, etc.) resolves via ``__getattr__`` to an
# SdkMcpTool bound to the legacy singleton context, so callers like
# ``from nerve.agent.tools import task_create`` still work and
# ``task_create.handler({...})`` still returns the dict shape the
# Claude Agent SDK produces.

_DEFAULT_REGISTRY: ToolRegistry = build_default_registry()


def create_session_mcp_server(session_id: str):
    """Legacy entry point used by engine.py and tests.

    Builds a per-session MCP server backed by the default registry and
    the legacy module globals (workspace, db, etc.). New code should
    prefer ``build_session_mcp_server(registry, ctx, include_hoa=...)``
    with a freshly-constructed ``ToolContext`` directly.
    """
    ctx = _legacy_ctx(session_id)
    include_hoa = bool(_config and _config.houseofagents.enabled)
    return build_session_mcp_server(_DEFAULT_REGISTRY, ctx, include_hoa=include_hoa)


def create_nerve_mcp_server():
    """DEPRECATED legacy shared server. Kept only for tests.

    Notification tools in this server have no session_id bound, so they
    fall back to whatever session_id was last written to
    ``_current_session_id`` (i.e. they're racy under concurrent sessions
    — which is precisely the bug ``create_session_mcp_server`` fixes).
    """
    return build_nerve_mcp_server(_DEFAULT_REGISTRY)


# Eagerly build SdkMcpTool list for ``ALL_TOOLS`` — used by prompts.py
# (system-prompt tool listing) and tests that parameterize over every
# registered tool. The wrapped SdkMcpTool objects are bound to a
# legacy singleton ctx (``session_id="unknown"``); production sessions
# never see these (they're shadowed by the per-session server).
_LEGACY_SINGLETON_CTX = ToolContext(session_id="system")


def _build_all_tools() -> list[SdkMcpTool]:
    """Build the legacy ``ALL_TOOLS`` list from the default registry.

    HoA tools are excluded (matching pre-refactor behavior, where they
    were filtered out of ``ALL_TOOLS`` via ``_HOA_TOOL_NAMES``).
    """
    return [
        _wrap_for_sdk(spec, _LEGACY_SINGLETON_CTX)
        for spec in _DEFAULT_REGISTRY.list(include_hoa=False)
    ]


ALL_TOOLS: list[SdkMcpTool] = _build_all_tools()


def _wrap_legacy(spec: ToolSpec) -> SdkMcpTool:
    """Wrap a spec for legacy ``task_foo.handler({...})`` callers.

    Differs from :func:`_wrap_for_sdk` in that the ``ToolContext`` is
    rebuilt on every call from the live module globals (workspace, db,
    etc.) rather than captured once. Tests like ``test_plan_revise``
    call :func:`init_tools` AFTER importing the symbol and expect that
    the next call sees the freshly-set globals; an eagerly-captured ctx
    would bind everything to ``None``.
    """
    from claude_agent_sdk import tool as _sdk_tool_decorator

    schema = _shim_schema(spec.input_schema)

    @_sdk_tool_decorator(spec.name, spec.description, schema)
    async def _sdk_handler(args: dict) -> dict:
        ctx = _legacy_ctx()
        result = await spec.handler(ctx, args)
        return result.to_dict()

    return _sdk_handler


# Per-tool SdkMcpTool re-exports for legacy callers. Resolved via
# ``__getattr__`` so we don't pay the cost of materializing every wrapped
# tool as a module attribute unless someone actually imports it.
def __getattr__(name: str) -> SdkMcpTool:
    spec = _DEFAULT_REGISTRY.get(name)
    if spec is not None:
        return _wrap_legacy(spec)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # New API
    "ToolContext",
    "ToolHandler",
    "ToolRegistry",
    "ToolResult",
    "ToolSpec",
    "build_default_registry",
    "build_session_mcp_server",
    "build_nerve_mcp_server",
    "tool",
    # Legacy back-compat
    "init_tools",
    "ALL_TOOLS",
    "create_session_mcp_server",
    "create_nerve_mcp_server",
    "_notify_impl",
    "_ask_user_impl",
    "_react_impl",
    "_send_sticker_impl",
    "_send_file_impl",
    "_legacy_ctx",
]
