"""Agent engine — Claude Agent SDK wrapper.

Orchestrates SDK clients and delegates all session state to SessionManager.
The SDK handles context management and compaction internally.
Sessions are resumable across server restarts via SDK's --resume flag.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ResultMessage,
    UserMessage,
    TextBlock,
    ToolUseBlock,
    ToolResultBlock,
)
from claude_agent_sdk.types import HookMatcher, HookJSONOutput, HookContext

from nerve.agent.interactive import (
    InteractiveToolHandler,
    register_handler,
    unregister_handler,
    get_handler,
)
from nerve.agent.prompts import build_system_prompt, set_skill_manager
from nerve.agent.sessions import SessionManager, SessionStatus
from nerve.agent.streaming import broadcaster
from nerve.agent.tools import ALL_TOOLS, create_session_mcp_server, init_tools
from nerve.config import NerveConfig
from nerve.db import Database
from nerve.skills.manager import SkillManager

logger = logging.getLogger(__name__)

try:
    from claude_agent_sdk import ThinkingBlock
except ImportError:
    ThinkingBlock = None


def _normalize_ts(ts: str) -> str:
    """Normalize timestamp to SQLite-compatible ``YYYY-MM-DD HH:MM:SS`` format.

    Handles ISO 8601 (``T`` separator, ``Z`` suffix, ``+00:00`` offset,
    microseconds) and SQLite's ``CURRENT_TIMESTAMP`` output (space separator,
    no timezone).  The canonical form allows consistent comparison between
    ``messages.created_at`` and ``sessions.last_memorized_at``.
    """
    if not ts:
        return ""
    s = ts.replace("T", " ")
    # Strip timezone suffixes
    for suffix in ("+00:00", "Z"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    # Strip microseconds
    dot = s.find(".")
    if dot != -1:
        s = s[:dot]
    return s.strip()


class AgentEngine:
    """Core agent engine wrapping claude-agent-sdk.

    Delegates all session state management to SessionManager.
    Focuses on SDK client creation, message streaming, and orchestration.
    """

    def __init__(self, config: NerveConfig, db: Database):
        # Prevent "cannot launch inside another Claude Code session" errors
        # when Nerve is invoked from within a Claude Code session (e.g. CLI).
        os.environ.pop("CLAUDECODE", None)

        self.config = config
        self.db = db
        self.sessions = SessionManager(
            db, sticky_period_minutes=config.sessions.sticky_period_minutes,
        )
        self._semaphore = asyncio.Semaphore(config.agent.max_concurrent)
        self._memory_bridge = None
        self._skill_manager: SkillManager | None = None
        self._memorize_lock = asyncio.Lock()
        self._router = None  # ChannelRouter — lazy-initialized via .router property

    async def initialize(self) -> None:
        """Initialize the agent engine — set up tools and main session."""
        from nerve.memory.memu_bridge import MemUBridge
        self._memory_bridge = MemUBridge(self.config, audit_db=self.db)
        await self._memory_bridge.initialize()

        # Initialize skill manager and discover skills from filesystem
        self._skill_manager = SkillManager(self.config.workspace, self.db)
        try:
            skills = await self._skill_manager.discover()
            logger.info("Skills system initialized: %d skills discovered", len(skills))
        except Exception as e:
            logger.error("Skills discovery failed: %s", e)

        # Make skill manager available to prompts and tools
        set_skill_manager(self._skill_manager)
        init_tools(
            self.config.workspace, self.db,
            memory_bridge=self._memory_bridge,
            config=self.config,
            skill_manager=self._skill_manager,
        )

        # Wire up memorize callback so SessionManager can trigger memU indexing
        self.sessions._on_memorize = self._memorize_session

        # Recover orphaned sessions from previous crash
        try:
            await self.sessions.recover_orphaned_sessions()
        except Exception as e:
            logger.error("Orphaned session recovery failed: %s", e)

        logger.info("Agent engine initialized")

    @staticmethod
    async def _safe_disconnect(client: Any, timeout: float = 5.0) -> None:
        """Disconnect an SDK client without risking an event-loop spin.

        The SDK's Query.close() cancels its anyio task group before closing
        the transport.  If any task inside that group cannot exit promptly
        (e.g. _read_messages stuck on process.wait(), _handle_control_request
        writing to a dead pipe, or _message_send buffer full), the anyio
        _deliver_cancellation callback spins at 100% CPU forever.

        Strategy:
        1. Kill the subprocess immediately (SIGKILL) so every I/O wait
           inside the task group unblocks.
        2. Attempt a clean disconnect() with a short timeout.
        3. If that times out, forcibly disarm the anyio task group so
           _deliver_cancellation has nothing left to spin on.
        """
        # --- 1. Kill subprocess immediately ---
        transport = getattr(
            getattr(client, "_query", None), "transport", None,
        )
        proc = getattr(transport, "_process", None)
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass

        # --- 2. Try a clean disconnect with a timeout ---
        try:
            await asyncio.wait_for(client.disconnect(), timeout=timeout)
            return
        except asyncio.TimeoutError:
            logger.warning(
                "SDK client disconnect timed out after %.1fs — "
                "force-clearing task group to stop _deliver_cancellation spin",
                timeout,
            )
        except Exception:
            pass

        # --- 3. Forcibly disarm the stuck task group ---
        query = getattr(client, "_query", None)
        if query is None:
            return
        tg = getattr(query, "_tg", None)
        if tg is None:
            return

        # Cancel the pending _deliver_cancellation handle so it stops
        # rescheduling itself via call_soon().
        cs = getattr(tg, "cancel_scope", None)
        handle = getattr(cs, "_cancel_handle", None)
        if handle is not None:
            handle.cancel()
            cs._cancel_handle = None

        # Clear task sets so a stray _deliver_cancellation finds nothing.
        if cs is not None:
            cs._tasks.clear()
        tg._tasks.clear()

        # Close the transport directly (kills process, closes pipes).
        try:
            await asyncio.wait_for(query.transport.close(), timeout=2.0)
        except Exception:
            pass

        client._query = None
        client._transport = None

    async def shutdown(self) -> None:
        """Disconnect all persistent clients and mark sessions as idle.

        No memorization here — the periodic sweep handles that.
        Sessions are marked idle so they can be resumed on next startup.
        """
        for sid, client in list(self.sessions._clients.items()):
            try:
                await self._safe_disconnect(client)
                logger.info("Disconnected client for session %s", sid)
            except Exception as e:
                logger.warning("Error disconnecting client %s: %s", sid, e)

            try:
                await self.sessions.mark_idle(sid, preserve_sdk_id=True)
            except Exception:
                pass

        self.sessions._clients.clear()
        self.sessions._client_locks.clear()

    # ------------------------------------------------------------------ #
    #  Channel router                                                      #
    # ------------------------------------------------------------------ #

    @property
    def router(self):
        """Get the channel router (lazy-initialized)."""
        if self._router is None:
            from nerve.channels.router import ChannelRouter
            self._router = ChannelRouter(self)
        return self._router

    def register_channel(self, channel: Any) -> None:
        """Register a channel with the router."""
        self.router.register(channel)

    # ------------------------------------------------------------------ #
    #  File snapshot for diff tracking                                     #
    # ------------------------------------------------------------------ #

    async def _save_file_snapshot(
        self, session_id: str, file_path: str, content: str | None,
    ) -> None:
        """Persist original file content before agent modification."""
        await self.db.save_file_snapshot(session_id, file_path, content)

    # ------------------------------------------------------------------ #
    #  Memory bridge                                                       #
    # ------------------------------------------------------------------ #

    async def _memorize_session(self, session_id: str) -> None:
        """Index un-memorized messages from a session into memU.

        Uses the more recent of ``connected_at`` and ``last_memorized_at`` as
        the lower bound so already-indexed messages are never re-sent to memU.
        """
        if not self._memory_bridge or not self._memory_bridge.available:
            return

        session = await self.db.get_session(session_id)
        connected_at = session.get("connected_at") if session else None
        if not connected_at:
            return

        watermark = _normalize_ts(session.get("last_memorized_at") or "")
        connected = _normalize_ts(connected_at)

        # Pick effective lower bound: watermark wins when it's more recent
        if watermark and watermark >= connected:
            lower_bound = watermark
            inclusive = False  # strict >: watermark message already indexed
        else:
            lower_bound = connected
            inclusive = True   # >=: include messages from connection time

        async with self._memorize_lock:
            try:
                messages = await self.db.get_messages(session_id, limit=10000)

                context_msgs = []
                latest_ts: str | None = None
                for msg in messages:
                    created = msg.get("created_at", "")
                    if created:
                        ts = _normalize_ts(created)
                        if (inclusive and ts >= lower_bound) or (
                            not inclusive and ts > lower_bound
                        ):
                            context_msgs.append(msg)
                            if latest_ts is None or ts > latest_ts:
                                latest_ts = ts

                if not context_msgs:
                    return

                await self._memory_bridge.memorize_conversation(
                    session_id, context_msgs,
                )
                logger.info(
                    "Indexed %d messages from session %s into memU",
                    len(context_msgs), session_id,
                )

                # Update watermark so sweep doesn't re-index
                if latest_ts:
                    await self.db.update_session_fields(
                        session_id, {"last_memorized_at": latest_ts},
                    )

            except Exception as e:
                logger.error("Failed to memorize session %s: %s", session_id, e)

    async def _memorize_incremental(self, session_id: str) -> int:
        """Index only messages newer than last_memorized_at into memU.

        Used by the periodic sweep. Returns count of messages indexed.
        Timestamps are normalised to ``YYYY-MM-DD HH:MM:SS`` so the stored
        watermark is directly comparable with SQLite's ``CURRENT_TIMESTAMP``.
        """
        if not self._memory_bridge or not self._memory_bridge.available:
            return 0

        session = await self.db.get_session(session_id)
        if not session:
            return 0

        watermark = _normalize_ts(session.get("last_memorized_at") or "")

        try:
            messages = await self.db.get_messages(session_id, limit=10000)

            new_msgs = []
            latest_ts: str | None = None
            for msg in messages:
                created = msg.get("created_at", "")
                if created:
                    ts = _normalize_ts(created)
                    if ts > watermark:
                        new_msgs.append(msg)
                        if latest_ts is None or ts > latest_ts:
                            latest_ts = ts

            if not new_msgs:
                return 0

            await self._memory_bridge.memorize_conversation(
                session_id, new_msgs,
            )

            if latest_ts:
                await self.db.update_session_fields(
                    session_id, {"last_memorized_at": latest_ts},
                )

            return len(new_msgs)

        except Exception as e:
            logger.error(
                "Incremental memorize failed for session %s: %s",
                session_id, e,
            )
            return 0

    async def run_memorization_sweep(self) -> dict:
        """Scan all sessions for un-memorized messages and index them.

        Called periodically by the background task. Returns stats.
        Skips if another memorize operation is already in progress.
        """
        if not self._memory_bridge or not self._memory_bridge.available:
            return {"skipped": "memU not available"}

        if self._memorize_lock.locked():
            logger.info("Memorization sweep skipped: another memorize is in progress")
            return {"skipped": "memorize already in progress"}

        async with self._memorize_lock:
            sessions = await self.db.get_sessions_needing_memorization()
            total_messages = 0
            sessions_indexed = 0

            for session in sessions:
                sid = session["id"]
                count = await self._memorize_incremental(sid)
                if count > 0:
                    total_messages += count
                    sessions_indexed += 1

            # Release memory after the sweep — prevents RSS ratcheting
            # from intermediate list[float]→numpy conversions and JSON parsing.
            if self._memory_bridge:
                self._memory_bridge._release_memory()

            stats = {
                "sessions_scanned": len(sessions),
                "sessions_indexed": sessions_indexed,
                "messages_indexed": total_messages,
            }
            if sessions_indexed > 0:
                logger.info("Memorization sweep: %s", stats)
            return stats

    # ------------------------------------------------------------------ #
    #  SDK options                                                         #
    # ------------------------------------------------------------------ #

    def _build_options(
        self,
        session_id: str,
        source: str = "web",
        model: str | None = None,
        recalled_memories: list[str] | None = None,
        resume: str | None = None,
        fork_session: bool = False,
        can_use_tool=None,
    ) -> ClaudeAgentOptions:
        """Build SDK client options for a session."""
        # Get skill summaries for system prompt injection
        skill_summaries = None
        if self._skill_manager:
            try:
                import asyncio
                # get_enabled_summaries is a coroutine but _build_options is sync
                # Use the running loop if available
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # We're in an async context — schedule and await later
                    # For now, use cached data from the manager
                    skill_summaries = []
                    for sid, meta in self._skill_manager._cache.items():
                        if meta.enabled and meta.model_invocable:
                            skill_summaries.append({
                                "id": meta.id,
                                "name": meta.name,
                                "description": meta.description,
                            })
                else:
                    skill_summaries = loop.run_until_complete(
                        self._skill_manager.get_enabled_summaries()
                    )
            except Exception as e:
                logger.warning("Failed to get skill summaries: %s", e)

        system_prompt = build_system_prompt(
            workspace=self.config.workspace,
            session_id=session_id,
            source=source,
            timezone_name=self.config.timezone,
            recalled_memories=recalled_memories,
            skill_summaries=skill_summaries,
        )

        thinking_config = self._parse_thinking_config(self.config.agent.thinking)
        effort = (
            self.config.agent.effort
            if self.config.agent.effort in ("low", "medium", "high", "max")
            else None
        )
        betas = (
            ["context-1m-2025-08-07"] if self.config.agent.context_1m else []
        )

        # Build PreToolUse hook for file snapshot capture
        hooks = self._build_snapshot_hooks(session_id)

        return ClaudeAgentOptions(
            model=model or self.config.agent.model,
            system_prompt=system_prompt,
            max_turns=self.config.agent.max_turns,
            # No permission_mode — can_use_tool callback handles all permissions.
            # Interactive tools pause for user input; everything else auto-approves.
            can_use_tool=can_use_tool,
            thinking=thinking_config,
            effort=effort,
            betas=betas,
            resume=resume,
            fork_session=fork_session,
            hooks=hooks,
            allowed_tools=[
                # Built-in Claude Code tools
                "Read", "Write", "Edit", "Bash", "Glob", "Grep",
                "WebSearch", "WebFetch", "NotebookEdit",
                "AskUserQuestion", "EnterPlanMode", "ExitPlanMode",
                "Task", "TaskOutput", "TaskStop", "TodoWrite",
                "EnterWorktree", "Skill",
                # Nerve MCP tools — derived from ALL_TOOLS
                *(f"mcp__nerve__{t.name}" for t in ALL_TOOLS),
            ],
            cwd=str(self.config.workspace),
            # Per-session MCP server with session_id bound in closure —
            # ensures notify/ask_user always reference the correct session.
            mcp_servers={"nerve": create_session_mcp_server(session_id)},
        )

    def _build_snapshot_hooks(self, session_id: str) -> dict:
        """Build PreToolUse hooks for capturing file snapshots before modification."""
        from nerve.agent.interactive import _read_file_safe

        captured_files: set[str] = set()

        async def _snapshot_hook(hook_input, tool_use_id, context):
            """PreToolUse hook: capture file content before Edit/Write/NotebookEdit."""
            tool_input = hook_input.get("tool_input", {})
            file_path = tool_input.get("file_path") or tool_input.get("notebook_path")

            if file_path and file_path not in captured_files:
                captured_files.add(file_path)
                content = _read_file_safe(file_path)
                try:
                    await self._save_file_snapshot(session_id, file_path, content)
                    logger.info("Captured file snapshot for %s", file_path)
                except Exception as e:
                    logger.warning("Failed to save file snapshot for %s: %s", file_path, e)

            # Allow the tool to proceed
            return {"hookSpecificOutput": {"hookEventName": "PreToolUse"}}

        return {
            "PreToolUse": [
                HookMatcher(
                    matcher="Edit|Write|NotebookEdit",
                    hooks=[_snapshot_hook],
                ),
            ],
        }

    @staticmethod
    def _parse_thinking_config(value: str) -> dict | None:
        """Parse thinking config string into SDK ThinkingConfig dict."""
        v = value.strip().lower()
        if v == "disabled":
            return {"type": "disabled"}
        if v == "adaptive":
            return {"type": "adaptive"}
        budget_map = {
            "max": 128_000,
            "high": 64_000,
            "medium": 32_000,
            "low": 8_000,
        }
        if v in budget_map:
            return {"type": "enabled", "budget_tokens": budget_map[v]}
        try:
            tokens = int(v)
            return {"type": "enabled", "budget_tokens": tokens}
        except ValueError:
            logger.warning("Unknown thinking config '%s', using adaptive", value)
            return {"type": "adaptive"}

    # ------------------------------------------------------------------ #
    #  SDK client lifecycle                                                #
    # ------------------------------------------------------------------ #

    async def _get_or_create_client(
        self, session_id: str, source: str, model: str | None,
        fork_from: str | None = None,
    ) -> ClaudeSDKClient:
        """Get an existing persistent client or create a new one.

        If the session has a stored sdk_session_id, the new client is created
        with resume=sdk_session_id so the CLI restores full conversation
        context.

        If fork_from is set, creates the client with fork_session=True to
        branch from the given SDK session.
        """
        lock = self.sessions.get_lock(session_id)
        async with lock:
            client = self.sessions.get_client(session_id)
            if client is not None:
                return client

            # Check for stored SDK session ID for resume
            session = await self.db.get_session(session_id)
            sdk_resume_id = session.get("sdk_session_id") if session else None

            # For forks, use the source session's SDK ID
            if fork_from and not sdk_resume_id:
                sdk_resume_id = fork_from

            if sdk_resume_id:
                logger.info(
                    "Resuming session %s with SDK session %s",
                    session_id, sdk_resume_id[:12],
                )

            # Pre-recall memories for new session context
            recalled_memories: list[str] = []
            if self._memory_bridge and self._memory_bridge.available:
                try:
                    raw = await self._memory_bridge.recall(
                        f"context for {source} session",
                        limit=8,
                    )
                    recalled_memories = [m["summary"] for m in raw]
                except Exception as e:
                    logger.warning("Pre-recall failed: %s", e)

            # Determine if this is a fork
            is_fork = fork_from is not None

            # Create interactive tool handler for this session
            handler = InteractiveToolHandler(
                session_id=session_id,
                broadcast_fn=broadcaster.broadcast,
                snapshot_fn=self._save_file_snapshot,
            )
            register_handler(session_id, handler)

            options = self._build_options(
                session_id, source=source, model=model,
                recalled_memories=recalled_memories or None,
                resume=sdk_resume_id,
                fork_session=is_fork,
                can_use_tool=handler.can_use_tool,
            )
            client = ClaudeSDKClient(options=options)
            await client.connect()
            self.sessions.set_client(session_id, client)

            # Record connected_at
            now = datetime.now(timezone.utc).isoformat()
            connected_at = session.get("connected_at") if session and sdk_resume_id else now
            await self.sessions.mark_active(
                session_id,
                sdk_session_id=sdk_resume_id,
                connected_at=connected_at,
            )

            logger.info(
                "Created persistent client for session %s%s",
                session_id,
                " (resumed)" if sdk_resume_id and not is_fork else
                " (forked)" if is_fork else "",
            )
            return client

    async def _discard_client(
        self, session_id: str, clear_resume: bool = False,
    ) -> None:
        """Disconnect and remove a client.

        Args:
            clear_resume: If True, clear sdk_session_id (e.g., on error).
                         If False, keep it for future resume (e.g., on stop).
        """
        await self._memorize_session(session_id)
        client = self.sessions.remove_client(session_id)

        if clear_resume:
            await self.sessions.mark_error(session_id, "client_discarded")
        else:
            await self.sessions.mark_idle(session_id, preserve_sdk_id=True)

        if client:
            await self._safe_disconnect(client)
            logger.info(
                "Discarded client for session %s (clear_resume=%s)",
                session_id, clear_resume,
            )

    # ------------------------------------------------------------------ #
    #  Public API: run, stop, fork, resume                                 #
    # ------------------------------------------------------------------ #

    def register_task(self, session_id: str, task: asyncio.Task) -> None:
        """Register a running asyncio.Task for a session (enables stop)."""
        self.sessions.register_task(session_id, task)

    async def stop_session(self, session_id: str) -> bool:
        """Stop a running session."""
        # Cancel any pending interactive tool prompts so the handler unblocks
        handler = get_handler(session_id)
        if handler:
            handler.cancel_all()
        return await self.sessions.stop_session(session_id)

    def is_session_running(self, session_id: str) -> bool:
        return self.sessions.is_running(session_id)

    async def get_client_connected_at_async(self, session_id: str) -> str | None:
        """Async version: get connected_at from DB."""
        session = await self.db.get_session(session_id)
        return session.get("connected_at") if session else None

    async def fork_session(
        self,
        source_session_id: str,
        at_message_id: str | None = None,
        title: str | None = None,
        source: str | None = None,
    ) -> dict:
        """Fork a session. Returns the new session dict.

        Args:
            source: Override the source field on the fork (default: inherit
                    from parent).
        """
        parent = await self.db.get_session(source_session_id)
        if not parent:
            raise ValueError(f"Source session not found: {source_session_id}")

        fork = await self.sessions.fork_session(
            source_session_id, at_message_id, title, source=source,
        )
        return fork

    async def resume_session(self, session_id: str) -> dict:
        """Resume a stopped/idle session."""
        info = await self.sessions.get_resume_info(session_id)
        if not info or not info.get("sdk_session_id"):
            raise ValueError(
                f"Session {session_id} cannot be resumed (no SDK session)",
            )
        # Mark as created so the next message will reconnect the client
        await self.sessions.transition(session_id, SessionStatus.CREATED)
        session = await self.db.get_session(session_id)
        return session

    # ------------------------------------------------------------------ #
    #  Run agent                                                           #
    # ------------------------------------------------------------------ #

    async def run(
        self,
        session_id: str,
        user_message: str,
        source: str = "web",
        channel: str | None = None,
        model: str | None = None,
    ) -> str:
        """Run the agent for a user message and return the final text response."""
        if self.sessions.is_running(session_id):
            raise RuntimeError(f"Session {session_id} is already running")

        broadcaster.start_buffering(session_id)
        async with self._semaphore:
            self.sessions.mark_running(session_id)
            # Notify all connected clients that this session started running
            await broadcaster.broadcast("__global__", {
                "type": "session_running",
                "session_id": session_id,
                "is_running": True,
            })
            try:
                return await self._run_inner(
                    session_id, user_message, source, channel, model,
                )
            finally:
                self.sessions.mark_not_running(session_id)
                broadcaster.stop_buffering(session_id)
                # Notify all connected clients that this session stopped
                await broadcaster.broadcast("__global__", {
                    "type": "session_running",
                    "session_id": session_id,
                    "is_running": False,
                })

    async def _run_inner(
        self,
        session_id: str,
        user_message: str,
        source: str,
        channel: str | None,
        model: str | None,
    ) -> str:
        # Ensure session exists in DB
        await self.sessions.get_or_create(session_id, source=source)

        # Auto-title: generate a meaningful name for new sessions
        session = await self.db.get_session(session_id)
        if session:
            current_title = session.get("title")
            if current_title in (None, "", session_id):
                placeholder = user_message[:40].strip()
                if len(user_message) > 40:
                    placeholder = (
                        placeholder.rsplit(' ', 1)[0] + '...'
                        if ' ' in placeholder
                        else placeholder + '...'
                    )
                await self.db.update_session_title(session_id, placeholder)
                await broadcaster.broadcast(session_id, {
                    "type": "session_updated",
                    "session_id": session_id,
                    "title": placeholder,
                })
                asyncio.create_task(
                    self._generate_session_title(session_id, user_message),
                )

        # Store user message in DB
        await self.sessions.add_message(
            session_id, "user", user_message, channel=channel,
        )

        full_response_text = ""
        thinking_text = ""
        tool_calls_log: list[dict] = []
        tool_results_map: dict[str, dict] = {}
        ordered_blocks: list[dict] = []  # preserves interleaving for DB
        last_usage: dict | None = None
        sdk_session_id: str | None = None
        active_subagents: dict[str, float] = {}  # tool_use_id -> monotonic start

        try:
            # Get or create persistent client for this session
            # Check if we need to fork from a parent
            fork_from = None
            if session:
                parent_id = session.get("parent_session_id")
                fork_msg = session.get("forked_from_message")
                if parent_id and session.get("status") == SessionStatus.CREATED.value:
                    parent = await self.db.get_session(parent_id)
                    if parent and parent.get("sdk_session_id"):
                        fork_from = parent["sdk_session_id"]

            client = await self._get_or_create_client(
                session_id, source, model, fork_from=fork_from,
            )

            # Send message — the client preserves conversation history internally
            await client.query(user_message)

            async for message in client.receive_response():
                if isinstance(message, AssistantMessage):
                    # Extract parent_tool_use_id — set when this message
                    # comes from a sub-agent (Task) rather than the main agent
                    parent_id = getattr(message, 'parent_tool_use_id', None)

                    for block in message.content:
                        if isinstance(block, TextBlock):
                            full_response_text += block.text
                            # Track ordered blocks for DB persistence
                            if ordered_blocks and ordered_blocks[-1].get("type") == "text":
                                ordered_blocks[-1]["content"] += block.text
                            else:
                                ordered_blocks.append({"type": "text", "content": block.text})
                            await broadcaster.broadcast_token(
                                session_id, block.text,
                                parent_tool_use_id=parent_id,
                            )

                        elif ThinkingBlock is not None and isinstance(
                            block, ThinkingBlock,
                        ):
                            thinking = (
                                block.thinking
                                if hasattr(block, 'thinking')
                                else str(block)
                            )
                            thinking_text += thinking
                            # Track ordered blocks for DB persistence
                            if ordered_blocks and ordered_blocks[-1].get("type") == "thinking":
                                ordered_blocks[-1]["content"] += thinking
                            else:
                                ordered_blocks.append({"type": "thinking", "content": thinking})
                            await broadcaster.broadcast_thinking(
                                session_id, thinking,
                                parent_tool_use_id=parent_id,
                            )

                        elif isinstance(block, ToolUseBlock):
                            tool_input = (
                                block.input
                                if hasattr(block, 'input')
                                else {}
                            )
                            tool_name = (
                                block.name
                                if hasattr(block, 'name')
                                else str(block)
                            )
                            tool_use_id = (
                                block.id if hasattr(block, 'id') else None
                            )
                            await broadcaster.broadcast_tool_use(
                                session_id, tool_name, tool_input,
                                tool_use_id=tool_use_id,
                                parent_tool_use_id=parent_id,
                            )
                            # Track sub-agent lifecycle
                            if tool_name == "Task" and tool_use_id:
                                active_subagents[tool_use_id] = asyncio.get_event_loop().time()
                                await broadcaster.broadcast_subagent_start(
                                    session_id,
                                    tool_use_id=tool_use_id,
                                    subagent_type=str(tool_input.get("subagent_type", tool_input.get("model", "agent"))),
                                    description=str(tool_input.get("description", "")),
                                    model=str(tool_input.get("model", "")) or None,
                                )
                            tool_calls_log.append({
                                "tool": tool_name,
                                "input": tool_input,
                                "tool_use_id": tool_use_id,
                            })
                            ordered_blocks.append({
                                "type": "tool_call",
                                "tool": tool_name,
                                "input": tool_input,
                                "tool_use_id": tool_use_id,
                            })

                        elif isinstance(block, ToolResultBlock):
                            result_content = (
                                block.content
                                if isinstance(block.content, str)
                                else json.dumps(block.content, default=str)
                            )
                            tool_use_id = (
                                block.tool_use_id
                                if hasattr(block, 'tool_use_id')
                                else None
                            )
                            is_error = (
                                block.is_error
                                if hasattr(block, 'is_error')
                                else False
                            )
                            tool_results_map[tool_use_id] = {
                                "result": result_content,
                                "is_error": is_error,
                            }
                            # Update matching tool_call in ordered_blocks
                            if tool_use_id:
                                for ob in reversed(ordered_blocks):
                                    if ob.get("type") == "tool_call" and ob.get("tool_use_id") == tool_use_id:
                                        ob["result"] = result_content
                                        ob["is_error"] = is_error
                                        break
                            await broadcaster.broadcast_tool_result(
                                session_id, result_content,
                                tool_use_id=tool_use_id,
                                is_error=is_error or False,
                                parent_tool_use_id=parent_id,
                            )
                            # Sub-agent lifecycle: emit complete event
                            if tool_use_id and tool_use_id in active_subagents:
                                start_time = active_subagents.pop(tool_use_id)
                                duration_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)
                                await broadcaster.broadcast_subagent_complete(
                                    session_id,
                                    tool_use_id=tool_use_id,
                                    duration_ms=duration_ms,
                                    is_error=is_error or False,
                                )
                            # Auto-broadcast plan updates when Write/Edit targets a plan file
                            if not is_error and tool_use_id:
                                _maybe_broadcast_plan_update(session_id, tool_use_id, tool_calls_log)
                                _maybe_broadcast_file_changed(session_id, tool_use_id, tool_calls_log)

                elif isinstance(message, UserMessage):
                    parent_id = getattr(message, 'parent_tool_use_id', None)
                    content = (
                        message.content
                        if hasattr(message, 'content')
                        else []
                    )
                    if isinstance(content, list):
                        for block in content:
                            if isinstance(block, ToolResultBlock):
                                result_content = (
                                    block.content
                                    if isinstance(block.content, str)
                                    else json.dumps(
                                        block.content, default=str,
                                    )
                                )
                                tool_use_id = (
                                    block.tool_use_id
                                    if hasattr(block, 'tool_use_id')
                                    else None
                                )
                                is_error = (
                                    block.is_error
                                    if hasattr(block, 'is_error')
                                    else False
                                )
                                tool_results_map[tool_use_id] = {
                                    "result": result_content,
                                    "is_error": is_error,
                                }
                                # Update matching tool_call in ordered_blocks
                                if tool_use_id:
                                    for ob in reversed(ordered_blocks):
                                        if ob.get("type") == "tool_call" and ob.get("tool_use_id") == tool_use_id:
                                            ob["result"] = result_content
                                            ob["is_error"] = is_error
                                            break
                                await broadcaster.broadcast_tool_result(
                                    session_id, result_content,
                                    tool_use_id=tool_use_id,
                                    is_error=is_error or False,
                                    parent_tool_use_id=parent_id,
                                )
                                # Sub-agent lifecycle: emit complete event
                                if tool_use_id and tool_use_id in active_subagents:
                                    start_time = active_subagents.pop(tool_use_id)
                                    duration_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)
                                    await broadcaster.broadcast_subagent_complete(
                                        session_id,
                                        tool_use_id=tool_use_id,
                                        duration_ms=duration_ms,
                                        is_error=is_error or False,
                                    )
                                # Auto-broadcast plan updates when Write/Edit targets a plan file
                                if not is_error and tool_use_id:
                                    _maybe_broadcast_plan_update(session_id, tool_use_id, tool_calls_log)
                                    _maybe_broadcast_file_changed(session_id, tool_use_id, tool_calls_log)

                elif isinstance(message, ResultMessage):
                    if message.usage:
                        last_usage = message.usage
                    sdk_session_id = message.session_id

        except asyncio.CancelledError:
            logger.info("Session %s cancelled by user", session_id)
            partial = full_response_text + (
                "\n\n[Stopped by user]"
                if full_response_text
                else "[Stopped by user]"
            )
            # Merge available tool results
            for tc in tool_calls_log:
                tid = tc.get("tool_use_id")
                if tid and tid in tool_results_map:
                    tc["result"] = tool_results_map[tid]["result"]
                    tc["is_error"] = tool_results_map[tid]["is_error"]
            await self.sessions.add_message(
                session_id, "assistant", partial,
                channel=channel,
                thinking=thinking_text if thinking_text else None,
                tool_calls=tool_calls_log if tool_calls_log else None,
                blocks=ordered_blocks if ordered_blocks else None,
            )
            await broadcaster.broadcast(session_id, {
                "type": "stopped", "session_id": session_id,
            })
            # Memorize before discarding client
            await self._memorize_session(session_id)
            # Keep sdk_session_id for resume — stop is user-initiated
            await self.sessions.mark_stopped(session_id)
            unregister_handler(session_id)
            client = self.sessions.remove_client(session_id)
            if client:
                await self._safe_disconnect(client)
            return partial

        except Exception as e:
            error_msg = f"Agent error: {e}"
            logger.error(error_msg, exc_info=True)
            await broadcaster.broadcast_error(session_id, str(e))
            # Memorize before discarding client
            await self._memorize_session(session_id)
            # Clear resume — CLI state may be corrupted after error
            unregister_handler(session_id)
            self.sessions.remove_client(session_id)
            await self.sessions.mark_error(session_id, str(e))
            full_response_text = error_msg

        # Merge tool results into tool_calls_log
        for tc in tool_calls_log:
            tid = tc.get("tool_use_id")
            if tid and tid in tool_results_map:
                tc["result"] = tool_results_map[tid]["result"]
                tc["is_error"] = tool_results_map[tid]["is_error"]

        # Store assistant message in DB
        await self.sessions.add_message(
            session_id, "assistant", full_response_text,
            channel=channel,
            thinking=thinking_text if thinking_text else None,
            tool_calls=tool_calls_log if tool_calls_log else None,
            blocks=ordered_blocks if ordered_blocks else None,
        )

        # Persist SDK session ID and update status
        if sdk_session_id:
            await self.sessions.mark_active(
                session_id,
                sdk_session_id=sdk_session_id,
                connected_at=await self.get_client_connected_at_async(session_id),
            )

        # Persist usage for context bar on session switch
        max_context = 1_048_576 if self.config.agent.context_1m else 200_000
        if last_usage:
            usage_data = {
                **last_usage,
                "max_context_tokens": max_context,
            }
            session_record = await self.db.get_session(session_id)
            meta = json.loads(session_record.get("metadata") or "{}") if session_record else {}
            meta["last_usage"] = usage_data
            await self.db.update_session_metadata(session_id, meta)

        await broadcaster.broadcast_done(
            session_id,
            usage=last_usage,
            max_context_tokens=max_context,
        )
        self.sessions.touch(session_id)
        return full_response_text

    # ------------------------------------------------------------------ #
    #  Cron / Hook runs                                                    #
    # ------------------------------------------------------------------ #

    async def run_cron(
        self,
        job_id: str,
        prompt: str,
        model: str | None = None,
        run_id: str | None = None,
    ) -> str:
        """Run an agent turn for a cron job in an isolated session.

        The SDK client is discarded immediately after the run completes
        to avoid leaking claude CLI subprocesses for one-shot jobs.
        """
        if run_id is None:
            run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        session = await self.sessions.create_cron_session(job_id, run_id=run_id)
        session_id = session["id"]
        try:
            return await self.run(
                session_id=session_id,
                user_message=prompt,
                source="cron",
                model=model or self.config.agent.cron_model,
            )
        finally:
            await self._discard_client(session_id)

    async def run_persistent_cron(
        self,
        job_id: str,
        prompt: str,
        model: str | None = None,
    ) -> str:
        """Run a persistent cron job that maintains context across runs.

        Uses a stable session_id (cron:{job_id}) so the SDK resumes
        conversation context on subsequent triggers.  The client is
        discarded after each run to free the subprocess, but
        sdk_session_id is preserved for the next resume.
        """
        session_id = f"cron:{job_id}"
        await self.sessions.get_or_create(
            session_id, title=f"Cron: {job_id}", source="cron",
        )
        try:
            return await self.run(
                session_id=session_id,
                user_message=prompt,
                source="cron",
                model=model or self.config.agent.cron_model,
            )
        finally:
            await self._discard_client(session_id)

    async def run_hook(
        self,
        hook_name: str,
        hook_id: str,
        prompt: str,
        model: str | None = None,
    ) -> str:
        """Run an agent turn for a webhook in an isolated session.

        The SDK client is discarded immediately after the run completes.
        """
        session = await self.sessions.create_hook_session(hook_name, hook_id)
        session_id = session["id"]
        try:
            return await self.run(
                session_id=session_id,
                user_message=prompt,
                source="hook",
                model=model or self.config.agent.cron_model,
            )
        finally:
            await self._discard_client(session_id)

    # ------------------------------------------------------------------ #
    #  Idle client sweep                                                   #
    # ------------------------------------------------------------------ #

    async def run_idle_client_sweep(self) -> int:
        """Disconnect clients that have been idle beyond the configured timeout.

        Idle clients still hold a claude CLI subprocess. Discarding them frees
        resources while preserving sdk_session_id for seamless resume later.

        Returns count of clients disconnected.
        """
        timeout_minutes = self.config.sessions.client_idle_timeout_minutes
        if timeout_minutes <= 0:
            return 0

        idle_ids = self.sessions.get_idle_client_ids(timeout_minutes * 60)
        for sid in idle_ids:
            logger.info("Auto-closing idle client for session %s", sid)
            await self._discard_client(sid)

        if idle_ids:
            logger.info(
                "Idle client sweep: disconnected %d client(s), %d still active",
                len(idle_ids),
                len(self.sessions._clients),
            )
        return len(idle_ids)

    # ------------------------------------------------------------------ #
    #  Title generation                                                    #
    # ------------------------------------------------------------------ #

    async def _generate_session_title(
        self, session_id: str, first_message: str,
    ) -> None:
        """Generate a meaningful short title for a session using Haiku."""
        try:
            import httpx
            api_key = self.config.anthropic_api_key
            if not api_key:
                return

            async with httpx.AsyncClient() as http_client:
                resp = await http_client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": "claude-haiku-4-5-20251001",
                        "max_tokens": 30,
                        "messages": [{
                            "role": "user",
                            "content": (
                                "Generate a short title (3-5 words, no quotes)"
                                " for a conversation that starts with:\n\n"
                                f"{first_message[:200]}"
                            ),
                        }],
                    },
                    timeout=10,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    title = data["content"][0]["text"].strip().strip('"\'').lstrip('#').strip()
                    if title and len(title) < 60:
                        await self.db.update_session_title(session_id, title)
                        await broadcaster.broadcast(session_id, {
                            "type": "session_updated",
                            "session_id": session_id,
                            "title": title,
                        })
                        logger.info(
                            "Generated title for session %s: %s",
                            session_id, title,
                        )
        except Exception as e:
            logger.warning("Failed to generate session title: %s", e)


def _maybe_broadcast_plan_update(
    session_id: str,
    tool_use_id: str,
    tool_calls_log: list[dict[str, Any]],
) -> None:
    """If a Write/Edit targeted a plan file, broadcast the updated content."""
    # Find the tool call that produced this result
    tool_entry = None
    for entry in reversed(tool_calls_log):
        if entry.get("tool_use_id") == tool_use_id:
            tool_entry = entry
            break
    if not tool_entry:
        return

    tool_name = tool_entry.get("tool", "")
    tool_input = tool_entry.get("input", {})

    if tool_name not in ("Write", "Edit"):
        return

    file_path = str(tool_input.get("file_path", ""))
    if "/.claude/plans/" not in file_path:
        return

    # Read the updated plan file and broadcast
    try:
        with open(file_path) as f:
            content = f.read()
        asyncio.get_event_loop().create_task(
            broadcaster.broadcast_plan_update(session_id, content),
        )
        logger.info("Broadcasted plan update for %s", file_path)
    except Exception as e:
        logger.warning("Failed to read plan file %s: %s", file_path, e)


_FILE_MODIFY_TOOLS = frozenset({"Edit", "Write", "NotebookEdit"})


def _maybe_broadcast_file_changed(
    session_id: str,
    tool_use_id: str,
    tool_calls_log: list[dict[str, Any]],
) -> None:
    """If a file-modifying tool succeeded, broadcast a file_changed event."""
    tool_entry = None
    for entry in reversed(tool_calls_log):
        if entry.get("tool_use_id") == tool_use_id:
            tool_entry = entry
            break
    if not tool_entry:
        return

    tool_name = tool_entry.get("tool", "")
    if tool_name not in _FILE_MODIFY_TOOLS:
        return

    tool_input = tool_entry.get("input", {})
    file_path = str(
        tool_input.get("file_path") or tool_input.get("notebook_path") or ""
    )
    if not file_path:
        return

    try:
        asyncio.get_event_loop().create_task(
            broadcaster.broadcast_file_changed(
                session_id,
                path=file_path,
                operation=tool_name.lower(),
                tool_use_id=tool_use_id,
            ),
        )
    except Exception as e:
        logger.debug("Failed to broadcast file_changed: %s", e)
