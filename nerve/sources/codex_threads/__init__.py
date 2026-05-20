"""Codex thread sync — pull Codex rollout items into Nerve as satellite sessions.

This package implements the inbound mirror of the external MCP server:
once Codex calls Nerve tools via MCP, this sync source brings the
*conversation* alongside the tool calls so downstream systems (memory
sweep, recall, UI, dedup) see the full transcript.

Public API:
  * :class:`CodexThreadSyncService` — top-level lifecycle owner (start/stop)
  * :func:`build_service` — build a service from :class:`NerveConfig`
"""

from __future__ import annotations

from nerve.sources.codex_threads.base import (
    CodexOrigin,
    ThreadEvent,
    SessionMeta,
    WorkspaceFilter,
)
from nerve.sources.codex_threads.ingester import CodexIngester
from nerve.sources.codex_threads.service import (
    CodexThreadSyncService,
    build_service,
)
from nerve.sources.codex_threads.translator import translate_event

__all__ = [
    "CodexOrigin",
    "CodexThreadSyncService",
    "CodexIngester",
    "SessionMeta",
    "ThreadEvent",
    "WorkspaceFilter",
    "build_service",
    "translate_event",
]
