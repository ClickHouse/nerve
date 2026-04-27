"""Session manager — full lifecycle, persistence, and SDK client management.

Single source of truth for session state. All session mutations go through here.
Persists channel mappings, tracks SDK clients, handles fork/resume/archive.

Session lifecycle:
    created -> active -> idle -> archived
                      |-> stopped -> active (resume)
                      |-> error -> active (retry)
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any

from nerve.db import Database

logger = logging.getLogger(__name__)

# Cleanup defaults
DEFAULT_ARCHIVE_AFTER_DAYS = 30
DEFAULT_MAX_SESSIONS = 500


class SessionStatus(StrEnum):
    CREATED = "created"
    ACTIVE = "active"
    IDLE = "idle"
    STOPPED = "stopped"
    ARCHIVED = "archived"
    ERROR = "error"


class SessionManager:
    """Manages agent sessions with full lifecycle tracking.

    Owns:
    - Session CRUD with explicit status transitions
    - Channel-to-session mapping (DB-persisted, survives restarts)
    - SDK client registry and health
    - Running task tracking and stop (interrupt + cancel)
    - Session fork and resume info
    - Automatic archival and cleanup
    - Orphan recovery on startup
    """

    def __init__(self, db: Database, sticky_period_minutes: int = 120):
        self.db = db
        self.sticky_period_minutes = sticky_period_minutes
        # In-memory SDK client registry (rebuilt on demand from DB)
        self._clients: dict[str, Any] = {}
        self._client_locks: dict[str, asyncio.Lock] = {}
        # Idle tracking: session_id -> monotonic time of last run() completion
        self._last_activity: dict[str, float] = {}
        # Running task tracking
        self._running_tasks: dict[str, asyncio.Task] = {}
        self._running_sessions: set[str] = set()
        # Stop-requested flag: set when /stop arrives before the SDK client
        # is registered.  _run_inner() checks this after creating the client.
        self._stop_requested: set[str] = set()
        # Serialize status transitions
        self._transition_lock = asyncio.Lock()
        # Callback for memorizing session before close/archive/delete.
        # Set by AgentEngine after init to wire up memU bridge.
        self._on_memorize: Any | None = None

    # ------------------------------------------------------------------ #
    #  Lifecycle: Create / Get                                             #
    # ------------------------------------------------------------------ #

    async def get_or_create(
        self,
        session_id: str,
        title: str | None = None,
        source: str = "web",
        metadata: dict | None = None,
    ) -> dict:
        """Get an existing session or create a new one."""
        session = await self.db.get_session(session_id)
        if not session:
            session = await self._create_session(
                session_id, title=title, source=source, metadata=metadata,
            )
        return session

    async def _create_session(
        self,
        session_id: str,
        title: str | None = None,
        source: str = "web",
        metadata: dict | None = None,
        parent_session_id: str | None = None,
        forked_from_message: str | None = None,
    ) -> dict:
        """Create a new session with status=created and log the event."""
        session = await self.db.create_session(
            session_id, title=title, source=source, metadata=metadata,
            status=SessionStatus.CREATED,
            parent_session_id=parent_session_id,
            forked_from_message=forked_from_message,
        )
        await self.db.log_session_event(session_id, "created", {
            "source": source,
            "parent": parent_session_id,
        })
        logger.info(
            "Created session: %s (parent=%s)", session_id, parent_session_id,
        )
        return session

    # ------------------------------------------------------------------ #
    #  Lifecycle: Status transitions                                       #
    # ------------------------------------------------------------------ #

    async def transition(
        self, session_id: str, new_status: SessionStatus,
        details: dict | None = None,
    ) -> None:
        """Atomically transition a session's status and log the event."""
        async with self._transition_lock:
            await self.db.update_session_fields(
                session_id, {"status": new_status.value},
            )
            await self.db.log_session_event(
                session_id, new_status.value, details or {},
            )

    async def mark_active(
        self,
        session_id: str,
        sdk_session_id: str | None = None,
        connected_at: str | None = None,
    ) -> None:
        """Mark session as active with an SDK client."""
        now = connected_at or datetime.now(timezone.utc).isoformat()
        fields: dict[str, Any] = {
            "status": SessionStatus.ACTIVE.value,
            "connected_at": now,
            "last_activity_at": now,
        }
        if sdk_session_id is not None:
            fields["sdk_session_id"] = sdk_session_id
        await self.db.update_session_fields(session_id, fields)
        await self.db.log_session_event(session_id, "started", {
            "sdk_session_id": sdk_session_id,
        })

    async def mark_idle(
        self, session_id: str, preserve_sdk_id: bool = True,
    ) -> None:
        """Mark session as idle (client disconnected but potentially resumable)."""
        fields: dict[str, Any] = {"status": SessionStatus.IDLE.value}
        if not preserve_sdk_id:
            fields["sdk_session_id"] = None
            fields["connected_at"] = None
        await self.db.update_session_fields(session_id, fields)
        await self.db.log_session_event(session_id, "idle", {
            "resumable": preserve_sdk_id,
        })

    async def mark_stopped(self, session_id: str) -> None:
        """Mark session as user-stopped."""
        await self.db.update_session_fields(
            session_id, {"status": SessionStatus.STOPPED.value},
        )
        await self.db.log_session_event(session_id, "stopped", {})

    async def mark_error(self, session_id: str, error_msg: str) -> None:
        """Mark session as errored (SDK crashed)."""
        await self.db.update_session_fields(session_id, {
            "status": SessionStatus.ERROR.value,
            "sdk_session_id": None,
            "connected_at": None,
        })
        await self.db.log_session_event(session_id, "error", {
            "error": error_msg[:500],
        })

    # ------------------------------------------------------------------ #
    #  Channel mapping (DB-persisted)                                      #
    # ------------------------------------------------------------------ #

    async def get_last_session(
        self, channel_key: str,
    ) -> str | None:
        """Get the last used session for a channel without auto-creating.

        Returns the channel's mapped session regardless of sticky period,
        or None if no mapping exists or the mapped session was archived/deleted.
        """
        row = await self.db.get_channel_session(channel_key)
        if row:
            session = await self.db.get_session(row["session_id"])
            if session and session.get("status") != SessionStatus.ARCHIVED.value:
                return row["session_id"]
        return None

    async def get_active_session(
        self, channel_key: str, source: str = "web",
    ) -> str:
        """Get or create the active session for a channel.

        If the channel has a mapped session with activity within the sticky
        period, reuse it. Otherwise, create a fresh session and map the
        channel to it.
        """
        row = await self.db.get_channel_session(channel_key)
        if row:
            session = await self.db.get_session(row["session_id"])
            if session and self._is_within_sticky_period(session):
                return row["session_id"]

        # Create a fresh session
        session_id = self._generate_session_id()
        await self._create_session(session_id, source=source)
        await self.db.set_channel_session(channel_key, session_id)
        return session_id

    def _is_within_sticky_period(self, session: dict) -> bool:
        """Check if a session had activity within the sticky period."""
        ts = session.get("last_activity_at") or session.get("updated_at")
        if not ts:
            return False
        try:
            normalized = ts if "T" in ts else ts.replace(" ", "T") + "Z"
            if not normalized.endswith(("Z", "+00:00")):
                normalized += "+00:00"
            last = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
            cutoff = datetime.now(timezone.utc) - timedelta(
                minutes=self.sticky_period_minutes,
            )
            return last >= cutoff
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _generate_session_id() -> str:
        """Generate a short unique session ID."""
        return str(uuid.uuid4())[:8]

    async def set_active_session(
        self, channel_key: str, session_id: str,
    ) -> None:
        """Switch the active session for a channel (persisted to DB)."""
        session = await self.db.get_session(session_id)
        if not session:
            raise ValueError(f"Session not found: {session_id}")
        await self.db.set_channel_session(channel_key, session_id)
        logger.info("Channel %s -> session %s", channel_key, session_id)

    # ------------------------------------------------------------------ #
    #  SDK client management                                               #
    # ------------------------------------------------------------------ #

    def get_client(self, session_id: str) -> Any | None:
        """Get the SDK client for a session (or None)."""
        return self._clients.get(session_id)

    def set_client(self, session_id: str, client: Any) -> None:
        """Register an SDK client for a session."""
        self._clients[session_id] = client

    def remove_client(self, session_id: str) -> Any | None:
        """Remove and return the SDK client for a session."""
        self._client_locks.pop(session_id, None)
        self._last_activity.pop(session_id, None)
        return self._clients.pop(session_id, None)

    def get_lock(self, session_id: str) -> asyncio.Lock:
        """Get or create a per-session lock for client creation."""
        if session_id not in self._client_locks:
            self._client_locks[session_id] = asyncio.Lock()
        return self._client_locks[session_id]

    def touch(self, session_id: str) -> None:
        """Update last activity timestamp for idle tracking."""
        self._last_activity[session_id] = asyncio.get_event_loop().time()

    def get_idle_client_ids(self, timeout_seconds: float) -> list[str]:
        """Return session IDs with a live client idle beyond *timeout_seconds*.

        Skips sessions that are currently running an agent turn.
        """
        now = asyncio.get_event_loop().time()
        idle = []
        for sid in list(self._clients):
            if sid in self._running_sessions:
                continue  # currently executing, not idle
            last = self._last_activity.get(sid)
            if last is None or (now - last) > timeout_seconds:
                idle.append(sid)
        return idle

    # ------------------------------------------------------------------ #
    #  Running task management                                             #
    # ------------------------------------------------------------------ #

    def register_task(self, session_id: str, task: asyncio.Task) -> None:
        """Register a running asyncio.Task for a session (enables stop).

        Does NOT mark the session as running — that's done by mark_running()
        inside engine.run() to avoid a race between create_task() scheduling
        and the task's actual execution.
        """
        self._running_tasks[session_id] = task
        task.add_done_callback(lambda _: self._running_tasks.pop(session_id, None))

    def mark_running(self, session_id: str) -> None:
        """Mark a session as currently running."""
        self._running_sessions.add(session_id)

    def mark_not_running(self, session_id: str) -> None:
        """Mark a session as no longer running."""
        self._running_sessions.discard(session_id)

    def is_running(self, session_id: str) -> bool:
        """Check if a session has a running agent task."""
        return session_id in self._running_sessions

    def get_running_ids(self) -> set[str]:
        """Return the set of currently running session IDs."""
        return set(self._running_sessions)

    def request_stop(self, session_id: str) -> None:
        """Set a deferred stop flag (checked by _run_inner after client init)."""
        self._stop_requested.add(session_id)

    def pop_stop_request(self, session_id: str) -> bool:
        """Return True and clear if a stop was requested for *session_id*."""
        try:
            self._stop_requested.remove(session_id)
            return True
        except KeyError:
            return False

    async def stop_session(self, session_id: str) -> bool:
        """Stop the current turn of a running session.

        Sends SDK interrupt first — this gracefully ends the current turn
        while keeping the client alive for the next message.  Falls back
        to asyncio task cancellation (which disconnects the client) only
        if the interrupt doesn't complete within a timeout.
        """
        client = self._clients.get(session_id)
        interrupted = False
        if client:
            try:
                await client.interrupt()
                interrupted = True
                logger.info("Interrupted SDK client for session %s", session_id)
            except Exception as e:
                logger.warning(
                    "SDK interrupt failed for %s: %s",
                    session_id, e,
                )

        task = self._running_tasks.get(session_id)
        if task and not task.done():
            if interrupted:
                # Give interrupt time to complete the turn gracefully.
                # When it works, receive_response() yields a ResultMessage,
                # _run_inner exits via the normal path, and the client
                # stays alive for the next message.
                done, _ = await asyncio.wait({task}, timeout=5.0)
                if done:
                    logger.info(
                        "Session %s stopped gracefully via interrupt",
                        session_id,
                    )
                    return True

            # Interrupt failed or timed out — force cancel.
            # The CancelledError handler in _run_inner disconnects the
            # client since its state is inconsistent.
            task.cancel()
            logger.info("Cancelled task for session %s (interrupt timed out)", session_id)
            return True

        # Session is running but client/task not registered yet — set a
        # deferred stop flag that _run_inner() will check after client init.
        if self.is_running(session_id):
            self.request_stop(session_id)
            logger.info("Deferred stop requested for session %s", session_id)
            return True

        return client is not None  # interrupt was sent even if no task

    # ------------------------------------------------------------------ #
    #  Fork & resume                                                       #
    # ------------------------------------------------------------------ #

    async def fork_session(
        self,
        source_session_id: str,
        at_message_id: str | None = None,
        title: str | None = None,
        source: str | None = None,
    ) -> dict:
        """Create a forked session from an existing one.

        The fork is a new session that can optionally be linked back to a
        specific message point in the source session.

        Args:
            source: Override the source field (default: inherit from parent).
        """
        parent = await self.db.get_session(source_session_id)
        if not parent:
            raise ValueError(f"Source session not found: {source_session_id}")

        fork_id = f"fork-{str(uuid.uuid4())[:8]}"
        fork_title = title or f"Fork of {parent.get('title', source_session_id)}"

        session = await self._create_session(
            fork_id,
            title=fork_title,
            source=source or parent.get("source", "web"),
            parent_session_id=source_session_id,
            forked_from_message=at_message_id,
        )
        return session

    async def get_resume_info(self, session_id: str) -> dict | None:
        """Get SDK session ID and related info for resuming a session."""
        session = await self.db.get_session(session_id)
        if not session:
            return None
        return {
            "sdk_session_id": session.get("sdk_session_id"),
            "connected_at": session.get("connected_at"),
            "status": session.get("status"),
            "parent_session_id": session.get("parent_session_id"),
        }

    # ------------------------------------------------------------------ #
    #  Cron / Hook sessions                                                #
    # ------------------------------------------------------------------ #

    async def create_cron_session(
        self, job_id: str, run_id: str | None = None,
    ) -> dict:
        """Create an isolated session for a single cron run.

        When run_id is provided, each run gets its own session to prevent
        unbounded message accumulation.
        """
        if run_id:
            session_id = f"cron:{job_id}:{run_id}"
        else:
            session_id = f"cron:{job_id}"
        return await self.get_or_create(
            session_id, title=f"Cron: {job_id}", source="cron",
        )

    async def create_hook_session(
        self, hook_name: str, hook_id: str,
    ) -> dict:
        """Create an isolated session for a webhook."""
        session_id = f"hook:{hook_name}:{hook_id}"
        return await self.get_or_create(
            session_id, title=f"Hook: {hook_name}", source="hook",
        )

    # ------------------------------------------------------------------ #
    #  Messages (delegate to DB)                                           #
    # ------------------------------------------------------------------ #

    async def add_message(
        self,
        session_id: str,
        role: str,
        content: str,
        channel: str | None = None,
        thinking: str | None = None,
        blocks: list | None = None,
    ) -> int:
        """Add a message to a session's history."""
        return await self.db.add_message(
            session_id, role, content, channel=channel,
            thinking=thinking, blocks=blocks,
        )

    async def get_conversation_history(
        self, session_id: str, limit: int = 100,
    ) -> list[dict]:
        """Get the conversation history for a session."""
        return await self.db.get_messages(session_id, limit=limit)

    # ------------------------------------------------------------------ #
    #  Listing                                                             #
    # ------------------------------------------------------------------ #

    async def list_sessions(
        self, limit: int = 50, include_archived: bool = False,
    ) -> list[dict]:
        """List sessions, most recently updated first."""
        return await self.db.list_sessions(
            limit=limit, include_archived=include_archived,
        )

    # ------------------------------------------------------------------ #
    #  Archive & cleanup                                                   #
    # ------------------------------------------------------------------ #

    async def archive_session(self, session_id: str) -> None:
        """Archive a session: memorize, disconnect client, update status."""
        # Memorize before losing the context
        if self._on_memorize:
            try:
                await self._on_memorize(session_id)
            except Exception as e:
                logger.warning("Memorize before archive failed for %s: %s", session_id, e)

        client = self.remove_client(session_id)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        now = datetime.now(timezone.utc).isoformat()
        await self.db.update_session_fields(session_id, {
            "status": SessionStatus.ARCHIVED.value,
            "archived_at": now,
            "connected_at": None,
            "sdk_session_id": None,
        })
        await self.db.log_session_event(session_id, "archived", {})
        logger.info("Archived session %s", session_id)

    async def run_cleanup(
        self,
        archive_after_days: int = DEFAULT_ARCHIVE_AFTER_DAYS,
        max_sessions: int = DEFAULT_MAX_SESSIONS,
    ) -> dict:
        """Auto-archive stale sessions and enforce limits.

        Returns dict with cleanup statistics.
        """
        now = datetime.now(timezone.utc)
        cutoff = (now - timedelta(days=archive_after_days)).isoformat()

        # Archive idle/stopped sessions older than cutoff
        stale = await self.db.get_stale_sessions(cutoff)
        for s in stale:
            await self.archive_session(s["id"])

        # Enforce max session count (archive oldest beyond limit)
        count = await self.db.count_active_sessions()
        overflow = 0
        if count > max_sessions:
            excess = await self.db.get_oldest_sessions(
                count - max_sessions,
            )
            for s in excess:
                await self.archive_session(s["id"])
            overflow = len(excess)

        stats = {
            "archived_stale": len(stale),
            "archived_overflow": overflow,
        }
        if stale or overflow:
            logger.info("Session cleanup: %s", stats)
        return stats

    # ------------------------------------------------------------------ #
    #  Orphan recovery                                                     #
    # ------------------------------------------------------------------ #

    async def recover_orphaned_sessions(self) -> int:
        """Find sessions marked active in DB that have no live client.

        Called on startup. Sessions with sdk_session_id are marked idle
        (resumable); sessions without are marked stopped.

        Returns count of sessions recovered.
        """
        orphans = await self.db.get_sessions_by_status(
            [SessionStatus.ACTIVE.value],
        )
        recovered = 0
        for session in orphans:
            sid = session["id"]
            if sid in self._clients:
                continue  # Still has a live client

            if session.get("sdk_session_id"):
                await self.transition(sid, SessionStatus.IDLE, {
                    "reason": "orphan_recovery",
                })
                logger.info(
                    "Orphan recovery: %s -> idle (resumable, sdk=%s)",
                    sid, session["sdk_session_id"][:12],
                )
            else:
                # Memorize before marking as stopped (non-resumable, context will be lost)
                if self._on_memorize:
                    try:
                        await self._on_memorize(sid)
                    except Exception as e:
                        logger.warning("Memorize during orphan recovery failed for %s: %s", sid, e)
                await self.transition(sid, SessionStatus.STOPPED, {
                    "reason": "orphan_recovery_no_sdk_id",
                })
                logger.info(
                    "Orphan recovery: %s -> stopped (not resumable)", sid,
                )
            recovered += 1

        if recovered:
            logger.info(
                "Recovered %d orphaned session(s) on startup", recovered,
            )
        return recovered
