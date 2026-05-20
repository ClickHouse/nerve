"""Satellite session resolver — map MCP connections to Nerve sessions.

Each MCP connection (one per ``mcp-session-id`` header in HTTP transport)
gets a corresponding row in the ``sessions`` table with ``source="external"``,
so external tool calls show up in the UI's session list alongside native
sessions. The session ID is deterministic from
``(client_name, client_session_id_or_mcp_session_id)`` so re-resolves are
idempotent — useful for tests and for clients that reconnect with the
same session id.

No DB migration is needed: the satellite is just a regular ``sessions``
row with ``source="external"`` and ``metadata`` JSON carrying the client
name, the raw mcp-session-id, and an optional client-supplied id.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nerve.db import Database

logger = logging.getLogger(__name__)


def _sanitize_client_name(name: str | None) -> str:
    """Coerce an arbitrary client identifier into a session-id-safe slug.

    Session IDs are used in URLs and filesystem paths, so we restrict
    them to ``[A-Za-z0-9_.-]`` plus colons (which we use as separators).
    Anything else becomes ``_``.
    """
    if not name:
        return "external"
    safe = "".join(c if (c.isalnum() or c in "_.-") else "_" for c in name)
    return safe or "external"


_CODEX_CLIENT_NAMES = {"codex", "codex_exec", "codex_tui"}


def _looks_like_codex_thread_id(value: str | None) -> bool:
    """Heuristic: is this a UUID-like Codex thread id?

    Codex thread ids are UUIDv7-shaped (8-4-4-4-12 hex digits with
    dashes). We accept anything 36 chars long with four dashes — cheap
    to evaluate and false positives are harmless (worst case the MCP
    server creates a ``codex:<X>`` row that just won't match a sync
    record).
    """
    if not value or len(value) != 36:
        return False
    return value.count("-") == 4


class SatelliteSessionResolver:
    """Resolve an MCP connection to a Nerve satellite session record.

    Constructed once per HTTP mount. Each ``resolve()`` call ensures a
    session row exists in the DB and returns its id. Callers should cache
    the result for the lifetime of the underlying MCP connection so we
    don't hit the DB per tool call.

    Codex convergence: when the connecting client identifies as
    ``codex`` and supplies a thread-shaped ``client_session_id``, the
    satellite is created under the canonical ``codex:<thread_id>`` id
    that the rollout sync also uses (see
    :mod:`nerve.sources.codex_threads.ingester`). That way an MCP tool
    call and a synced rollout for the same Codex thread land on a
    single session row instead of two siblings.
    """

    def __init__(self, db: "Database") -> None:
        self.db = db

    @staticmethod
    def build_session_id(client_name: str, identifier: str) -> str:
        """Build the canonical satellite session id.

        Format: ``external:<client_name>:<identifier>``.

        ``identifier`` is either a client-supplied stable id (preferred —
        survives reconnects) or the per-connection mcp-session-id
        (best-effort fallback).
        """
        return f"external:{_sanitize_client_name(client_name)}:{identifier}"

    @staticmethod
    def build_codex_session_id(thread_id: str) -> str:
        """Build the convergent Codex thread satellite id.

        Must match :func:`nerve.sources.codex_threads.ingester.codex_session_id`.
        """
        return f"codex:{thread_id}"

    async def resolve(
        self,
        *,
        client_name: str | None,
        mcp_session_id: str,
        client_session_id: str | None = None,
    ) -> str:
        """Return the satellite session id, creating the row if needed.

        Args:
            client_name: From the MCP ``initialize`` request's
                ``clientInfo.name`` field, e.g. ``"codex"``, ``"claude-code"``.
                ``None`` is tolerated and mapped to ``"external"``.
            mcp_session_id: The transport-level session id (HTTP
                ``mcp-session-id`` header). Always present.
            client_session_id: Optional stable id supplied by the client
                (e.g. a Codex thread id). When provided, the satellite
                session id is stable across reconnects.
        """
        safe_client = _sanitize_client_name(client_name)

        # Codex convergence: when we recognise the client AND a
        # thread-shaped id was supplied, use the same session id the
        # rollout sync will use. The two paths now merge transparently.
        if (
            safe_client in _CODEX_CLIENT_NAMES
            and _looks_like_codex_thread_id(client_session_id)
        ):
            assert client_session_id is not None  # type narrowing
            sid = self.build_codex_session_id(client_session_id)
            existing = await self.db.get_session(sid)
            if existing is not None:
                return sid
            metadata = {
                "client_name": safe_client,
                "mcp_session_id": mcp_session_id,
                "client_session_id": client_session_id,
                "codex_thread_id": client_session_id,
                "runtime": "codex-external",
                "origin_ids": ["nerve-mcp-detected"],
            }
            title = f"Codex/mcp ({client_session_id[:8]})"
            try:
                await self.db.create_session(
                    session_id=sid,
                    title=title,
                    source="external",
                    metadata=metadata,
                    status="active",
                )
                logger.info(
                    "Created Codex satellite session %s via MCP (mcp=%s)",
                    sid, mcp_session_id,
                )
            except Exception:
                logger.exception("Failed to create Codex satellite %s", sid)
            return sid

        identifier = client_session_id or mcp_session_id
        sid = self.build_session_id(safe_client, identifier)

        existing = await self.db.get_session(sid)
        if existing is not None:
            return sid

        metadata = {
            "client_name": safe_client,
            "mcp_session_id": mcp_session_id,
            "client_session_id": client_session_id,
            "runtime": f"{safe_client}-external",
        }
        title = f"{safe_client} ({mcp_session_id[:8]})"
        try:
            await self.db.create_session(
                session_id=sid,
                title=title,
                source="external",
                metadata=metadata,
                status="active",
            )
            logger.info(
                "Created satellite session %s (client=%s, mcp=%s)",
                sid, safe_client, mcp_session_id,
            )
        except Exception:
            # Race: another concurrent request created the row between
            # get_session() and create_session(). create_session() is
            # INSERT OR IGNORE so this is normally swallowed; the
            # broader except is belt-and-braces.
            logger.exception("Failed to create satellite session %s", sid)

        return sid
