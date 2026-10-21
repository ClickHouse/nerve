"""What every memory backend shares, and the switch between them.

The memory tools talk to one backend object (``ToolContext.memory_bridge``,
``AgentEngine.memory_bridge``), and ``memory.backend`` picks which:

* ``memu`` (the default): memU embedded in the process, see
  :class:`nerve.memory.memu_bridge.MemUBridge`.
* ``loom``: a Loom memory service over HTTP, see
  :class:`nerve.memory.loom_bridge.LoomBridge`.

Both expose the same public surface. This module holds the parts of that
contract that belong to neither: the outage exception the tool layer turns
into its loud "memory is DOWN" text, the category breadcrumb that recall
returns, and the factory that builds the configured backend.
"""

from __future__ import annotations

from typing import Any


class MemoryBackendUnavailable(RuntimeError):
    """Raised when a memory operation fails because the backend is transiently
    unavailable (an unreachable service, a timeout, HTTP 429/5xx, an auth
    blip) rather than because the query genuinely had no results. The tool
    layer surfaces this LOUDLY so a session never mistakes "memory is DOWN"
    for "no relevant memories", and the indexing sweep keeps its watermark so
    the window is retried instead of skipped."""


# Max characters for a category breadcrumb in recall() output. A category
# summary is a whole rolled-up topic document (often 5–20KB); recall must
# surface it as a short navigable pointer, not dump the document.
CATEGORY_BREADCRUMB_MAXLEN = 200


def category_breadcrumb(name: str, description: str, summary: str) -> str:
    """Build a short one-line breadcrumb for a category recall hit.

    Prefers the curated ``description`` (a 1-line purpose). Falls back to the
    first meaningful line of the rolled-up ``summary`` (with any ``[ref:ID]``
    citation markers stripped). Never returns the full summary — that is what
    ``memory_expand_category`` is for.
    """
    text = (description or "").strip()
    if not text:
        try:
            from memu.utils.references import strip_references
        except Exception:  # noqa: BLE001 — without memU there are no markers  # pragma: no cover
            def strip_references(t: str | None) -> str | None:
                return t
        for raw in (summary or "").splitlines():
            line = (strip_references(raw) or "").strip()
            # Skip markdown headers — they just repeat the category name,
            # which is returned separately. Take the first real content line.
            if not line or line.startswith("#"):
                continue
            line = line.lstrip("-*").strip()
            if line:
                text = line
                break
    if len(text) > CATEGORY_BREADCRUMB_MAXLEN:
        text = text[: CATEGORY_BREADCRUMB_MAXLEN - 1].rstrip() + "…"
    return text


async def recall_for_startup(bridge: Any, query: str, limit: int) -> list[dict]:
    """The memories a new session starts with; the engine freezes their
    summaries for the whole session.

    A backend with a read of its own for this (the Loom bridge's
    ``startup_recall``, which serves memories even where a search abstains,
    and no category blurbs) answers it; memU recalls.
    """
    if callable(getattr(type(bridge), "startup_recall", None)):
        return await bridge.startup_recall(query, limit=limit)
    return await bridge.recall(query, limit=limit)


def create_memory_bridge(config: Any, audit_db: Any = None) -> Any:
    """The memory backend ``config.memory.backend`` names, not yet initialized.

    The bridge modules are imported here rather than at the top so a Loom
    instance never loads memU's numpy stack, and a memU instance never
    needs anything Loom-specific.
    """
    if config.memory.backend == "loom":
        from nerve.memory.loom_bridge import LoomBridge

        return LoomBridge(config, audit_db=audit_db)
    from nerve.memory.memu_bridge import MemUBridge

    return MemUBridge(config, audit_db=audit_db)
