"""What every memory backend shares, and the switch between them.

``memory.backend`` picks the backend the engine builds:

* ``memu`` (the default): memU embedded in the process, see
  :class:`nerve.memory.memu_bridge.MemUBridge`. Nerve's nine memory tools
  (``memorize``, ``memory_recall`` and the rest) serve it.
* ``loom``: a Loom memory service reached through the MCP gateway, see
  :class:`nerve.memory.loom.LoomMemory`. Agents use Loom's own tools from
  the gateway's catalog; Nerve makes only the lifecycle calls.

This module holds what belongs to neither, so a module can name it without
loading memU's numpy stack: the outage exception, the helpers through which
the engine asks a backend for what only some backends do, and the factory.
"""

from __future__ import annotations

from typing import Any


class MemoryBackendUnavailable(RuntimeError):
    """Raised when a memory operation fails because the backend is
    transiently unavailable (an unreachable service, a timeout, HTTP 429/5xx,
    an auth blip) rather than because the query genuinely had no results.

    The tool layer surfaces this LOUDLY so a session never mistakes "memory
    is DOWN" for "no relevant memories", and the indexing sweep keeps its
    watermark so the window is offered again instead of skipped.
    """


async def recall_for_startup(bridge: Any, query: str, limit: int) -> list[dict]:
    """The memories a new session starts with; the engine freezes their
    summaries for the whole session.

    A backend with a startup read of its own (the Loom backend's
    ``startup_recall``) answers it; memU recalls.
    """
    if callable(getattr(type(bridge), "startup_recall", None)):
        return await bridge.startup_recall(query, limit=limit)
    return await bridge.recall(query, limit=limit)


def create_memory_bridge(config: Any, audit_db: Any = None) -> Any:
    """The memory backend ``config.memory.backend`` names, not yet initialized.

    The modules are imported here rather than at the top so a Loom instance
    never loads memU's numpy stack.
    """
    if config.memory.backend == "loom":
        from nerve.memory.loom import LoomMemory

        return LoomMemory(config, audit_db=audit_db)
    from nerve.memory.memu_bridge import MemUBridge

    return MemUBridge(config, audit_db=audit_db)
