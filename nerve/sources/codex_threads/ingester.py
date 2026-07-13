"""Idempotent ingestion of Codex thread events into Nerve.

Maps :class:`ThreadEvent`s to session lifecycle operations plus message
inserts. A native-thread mapping reuses the owning Nerve session when one
exists; otherwise the fallback satellite ID is ``codex:<thread_id>``.

All inserts go through ``add_message_idempotent`` keyed on
``(session_id, external_id)``: the partial unique index added in v028
drops the duplicate when both the MCP server and the rollout sync see
the same call_id.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from nerve.agent.streaming import broadcaster as default_broadcaster
from nerve.sources.codex_threads.base import (
    SessionMeta,
    ThreadEvent,
    WorkspaceFilter,
)
from nerve.sources.codex_threads.translator import (
    StoredMessage,
    translate_event,
)

if TYPE_CHECKING:
    from nerve.agent.streaming import StreamBroadcaster
    from nerve.db import Database

logger = logging.getLogger(__name__)


def codex_session_id(thread_id: str) -> str:
    """Canonical satellite session id for a Codex thread.

    Public so the external MCP server can adopt the same convention.
    """
    return f"codex:{thread_id}"


class CodexIngester:
    """Writes :class:`ThreadEvent`s to the Nerve database.

    Per-origin instances let the service track which origin owns which
    thread (stored in metadata) without complicating the session id.
    """

    def __init__(
        self,
        db: "Database",
        *,
        origin_id: str,
        workspace_filter: WorkspaceFilter,
        broadcaster: "StreamBroadcaster | None" = None,
        store_encrypted_reasoning: bool = True,
    ) -> None:
        self.db = db
        self.origin_id = origin_id
        self.filter = workspace_filter
        self.broadcaster = broadcaster or default_broadcaster
        self.store_encrypted_reasoning = store_encrypted_reasoning
        # Per-thread scope decisions cached so we don't re-evaluate the
        # filter for every event in a long-running thread.
        self._in_scope: set[str] = set()
        self._out_of_scope: set[str] = set()
        # Stats reported to diagnostics
        self.stats: dict[str, int] = {
            "messages_inserted": 0,
            "messages_skipped_duplicate": 0,
            "messages_skipped_oos": 0,
            "threads_in_scope": 0,
            "threads_out_of_scope": 0,
            "threads_archived": 0,
        }

    # ------------------------------------------------------------------
    # Scope tracking
    # ------------------------------------------------------------------

    def mark_in_scope(self, thread_id: str) -> None:
        """Pre-decide that a thread is in scope (origin-level filter)."""
        self._in_scope.add(thread_id)
        self._out_of_scope.discard(thread_id)

    def mark_out_of_scope(self, thread_id: str) -> None:
        self._out_of_scope.add(thread_id)
        self._in_scope.discard(thread_id)

    def is_in_scope(self, thread_id: str) -> bool:
        return thread_id in self._in_scope

    def is_decided(self, thread_id: str) -> bool:
        return thread_id in self._in_scope or thread_id in self._out_of_scope

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def ingest(self, event: ThreadEvent) -> None:
        """Apply one event to the database."""
        if event.type == "thread_in_scope":
            await self._ensure_session(event)
            return
        if event.type == "thread_out_of_scope":
            self.mark_out_of_scope(event.thread_id)
            self.stats["threads_out_of_scope"] += 1
            return
        if event.type == "thread_archived":
            await self._archive_session(event.thread_id)
            return

        if event.thread_id in self._out_of_scope:
            # Quiet drop — we already decided this thread isn't ours.
            self.stats["messages_skipped_oos"] += 1
            return

        if event.thread_id not in self._in_scope:
            # We saw a message before session_meta — common when an
            # origin replays a half-written file. Refuse to create a
            # session out of thin air; let the service replay from the
            # beginning of the file when session_meta arrives.
            logger.debug(
                "Ingester: dropping %s for thread %s (no session_meta yet)",
                event.type, event.thread_id,
            )
            return

        if event.type in ("turn_started", "turn_completed"):
            # Metadata only — no message row. Useful as a hook for
            # future turn-aware features (cost tracking, etc.).
            return

        messages = translate_event(
            event,
            store_encrypted_reasoning=self.store_encrypted_reasoning,
        )
        if not messages:
            return

        session_id = await self._session_id_for(event.thread_id)
        for msg in messages:
            await self._insert_message(session_id, msg)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _ensure_session(self, event: ThreadEvent) -> None:
        """Create the satellite session row on first sighting of a
        ``session_meta`` line — but only if the workspace filter agrees.
        """
        payload = event.payload
        cwd = payload.get("cwd")

        if not self.filter.matches(cwd):
            self.mark_out_of_scope(event.thread_id)
            self.stats["threads_out_of_scope"] += 1
            logger.info(
                "Codex thread %s: cwd=%r — out of scope, skipping",
                event.thread_id[:8], cwd,
            )
            return

        self.mark_in_scope(event.thread_id)
        self.stats["threads_in_scope"] += 1

        session_id = await self._session_id_for(event.thread_id)
        existing = await self.db.get_session(session_id)
        if existing is not None:
            # Already created — MCP server may have got there first.
            # Make sure the metadata reflects this origin.
            await self._merge_origin_metadata(session_id, existing, payload)
            return

        meta = SessionMeta.from_payload(payload, event.timestamp)
        metadata = {
            "client_name": "codex",
            "runtime": "codex-external",
            "codex_thread_id": meta.thread_id,
            "codex_cwd": meta.cwd,
            "codex_originator": meta.originator,
            "codex_source": meta.source,
            "codex_cli_version": meta.cli_version,
            "codex_model_provider": meta.model_provider,
            "origin_ids": [self.origin_id],
        }
        title = _build_title(meta)
        await self.db.create_session(
            session_id=session_id,
            title=title,
            source="external",
            metadata=metadata,
            status="active",
            backend="codex",
            cwd=meta.cwd,
        )
        await self.db.bind_native_thread("codex", meta.thread_id, session_id)
        logger.info(
            "Codex thread %s: synced (cwd=%s, origin=%s) → %s",
            meta.thread_id[:8], meta.cwd, self.origin_id, session_id,
        )
        await self._broadcast(session_id, {
            "type": "session_created",
            "session_id": session_id,
        })

    async def _merge_origin_metadata(
        self, session_id: str, existing: dict, payload: dict,
    ) -> None:
        """Augment the existing satellite session's metadata.

        Useful when the MCP server created the session first and the
        sync source later discovers the rollout file. We add
        ``origin_ids`` (a list) and backfill any codex_* fields the
        MCP server didn't know about.

        Also reactivates ``stopped`` sessions — orphan recovery (or an
        older Nerve version) may have flipped them off, but if we're
        seeing rollout events the Codex side is alive again.
        """
        import json as _json
        try:
            meta = _json.loads(existing.get("metadata") or "{}")
        except (TypeError, _json.JSONDecodeError):
            meta = {}
        origin_ids = meta.get("origin_ids") or []
        if self.origin_id not in origin_ids:
            origin_ids.append(self.origin_id)
        meta["origin_ids"] = origin_ids
        # Backfill — never overwrite a value the MCP server already set.
        for key, src_key in (
            ("codex_thread_id", "id"),
            ("codex_cwd", "cwd"),
            ("codex_originator", "originator"),
            ("codex_source", "source"),
            ("codex_cli_version", "cli_version"),
            ("codex_model_provider", "model_provider"),
        ):
            if not meta.get(key) and payload.get(src_key):
                meta[key] = payload[src_key]
        meta.setdefault("client_name", "codex")
        meta.setdefault("runtime", "codex-external")
        await self.db.update_session_metadata(session_id, meta)
        # Bring stopped sessions back to active when their rollout is
        # still alive. Archived stays archived.
        if existing.get("source") == "external" and existing.get("status") == "stopped":
            try:
                await self.db.update_session_fields(session_id, {"status": "active"})
            except Exception:
                logger.exception("Failed to reactivate session %s", session_id)

    async def _archive_session(self, thread_id: str) -> None:
        session_id = await self._session_id_for(thread_id)
        existing = await self.db.get_session(session_id)
        if existing is None:
            return
        try:
            # Native Nerve sessions own their own lifecycle.  A rollout being
            # archived must not hide the corresponding live chat.
            if existing.get("source") == "external":
                await self.db.update_session_fields(session_id, {"status": "archived"})
                self.stats["threads_archived"] += 1
                logger.info("Codex thread %s: archived", thread_id[:8])
        except Exception:
            logger.exception("Failed to archive Codex session %s", session_id)

    async def _session_id_for(self, thread_id: str) -> str:
        mapped = await self.db.get_session_for_native_thread("codex", thread_id)
        return mapped or codex_session_id(thread_id)

    async def _insert_message(
        self, session_id: str, msg: StoredMessage,
    ) -> None:
        # Merge intent — fold this tool_result onto the matching
        # tool_call message's block instead of creating a new row.
        if msg.merge_into_tool_use_id is not None:
            merged_id = await self.db.merge_tool_result_into_call(
                session_id=session_id,
                tool_use_id=msg.merge_into_tool_use_id,
                result=msg.merge_result,
                is_error=msg.merge_is_error,
            )
            if merged_id is not None:
                self.stats["messages_inserted"] += 0  # merge isn't a new row
                await self._broadcast(session_id, {
                    "type": "tool_result",
                    "session_id": session_id,
                    "message_id": merged_id,
                    "tool_use_id": msg.merge_into_tool_use_id,
                    "result": msg.merge_result,
                    "is_error": msg.merge_is_error,
                })
                return
            # No matching tool_call yet — happens if Codex flushed the
            # output line before its function_call (rare, but possible).
            # Fall through and insert a synthetic tool_call carrying the
            # result so the UI shows something.
            msg = StoredMessage(
                role="assistant",
                external_id=f"tool_call:{msg.merge_into_tool_use_id}",
                content=msg.content,
                blocks=[{
                    "type": "tool_call",
                    "tool": "(unknown — result arrived before call)",
                    "input": {},
                    "tool_use_id": msg.merge_into_tool_use_id,
                    "result": msg.merge_result,
                    "is_error": msg.merge_is_error,
                }],
                created_at=msg.created_at,
                channel=msg.channel,
            )

        created_at = msg.created_at.isoformat() if msg.created_at else None
        try:
            inserted = await self.db.add_message_idempotent(
                session_id=session_id,
                role=msg.role,
                content=msg.content,
                external_id=msg.external_id,
                channel=msg.channel,
                thinking=msg.thinking,
                blocks=msg.blocks,
                created_at=created_at,
            )
        except Exception:
            logger.exception(
                "Codex ingest: insert failed for %s (external_id=%s)",
                session_id, msg.external_id,
            )
            return

        if inserted is None:
            self.stats["messages_skipped_duplicate"] += 1
            logger.debug(
                "Codex ingest: skipped duplicate %s (external_id=%s)",
                session_id, msg.external_id,
            )
            return

        self.stats["messages_inserted"] += 1
        await self._broadcast(session_id, {
            "type": "message_added",
            "session_id": session_id,
            "message_id": inserted,
            "role": msg.role,
            "blocks": msg.blocks,
            "external_id": msg.external_id,
        })

    async def _broadcast(self, session_id: str, payload: dict) -> None:
        try:
            await self.broadcaster.broadcast(session_id, payload)
        except Exception:                # pragma: no cover - defensive
            logger.exception("Codex ingest: broadcast failed for %s", session_id)


def _build_title(meta: SessionMeta) -> str:
    """Short, human-readable title for the Codex session card."""
    surface = meta.source or meta.originator or "codex"
    short_id = meta.thread_id[:8] if meta.thread_id else "thread"
    return f"Codex/{surface} ({short_id})"
