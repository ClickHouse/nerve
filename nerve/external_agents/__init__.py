"""External agent bootstrap + sync subsystem.

Configures third-party agents (Codex, Claude Code, ...) to consume
Nerve as an MCP server, and keeps their memory files (AGENTS.md,
CLAUDE.md, ...) in sync with the workspace identity files (SOUL.md,
USER.md, MEMORY.md, ...).

Public surface:

- :class:`ExternalAgent` and :data:`AGENT_REGISTRY` — the plug-in
  registry used by the bootstrap wizard and sync service.
- :class:`ConfigWriter` — the atomic, allowlist-bound file writer
  that both the wizard's apply step and the sync service share.
- :class:`SyncService` — background coroutine that re-renders each
  configured agent's memory bundle on a timer.

Templates and renderers live in submodules.
"""

from nerve.external_agents.registry import (
    AGENT_REGISTRY,
    AgentSetupResult,
    ExternalAgent,
    FileTarget,
)
from nerve.external_agents.writer import ConfigWriter, SecurityError

__all__ = [
    "AGENT_REGISTRY",
    "AgentSetupResult",
    "ConfigWriter",
    "ExternalAgent",
    "FileTarget",
    "SecurityError",
]
