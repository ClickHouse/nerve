"""Sub-agents and background tasks of a session (the agent registry).

The engine routes every sidechain event (anything a sub-agent produces) and
every CLI task frame here, never into the parent turn. See ``docs/agents.md``.
"""

from __future__ import annotations

from nerve.agent.agents.model import (
    LIVE_STATUSES,
    SPAWN_TOOLS,
    TERMINAL_STATUSES,
    AgentRecord,
)
from nerve.agent.agents.registry import AgentRegistry

__all__ = [
    "AgentRecord",
    "AgentRegistry",
    "LIVE_STATUSES",
    "SPAWN_TOOLS",
    "TERMINAL_STATUSES",
]
