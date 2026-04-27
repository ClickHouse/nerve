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
import re
import time
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
from claude_agent_sdk._errors import CLIConnectionError
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
from nerve.config import NerveConfig, load_mcp_servers
from nerve.db import Database
from nerve.skills.manager import SkillManager

logger = logging.getLogger(__name__)

try:
    from claude_agent_sdk import ThinkingBlock
except ImportError:
    ThinkingBlock = None


_SURROGATE_RE = re.compile(r"[\ud800-\udfff]")

# Anthropic API image limits
_MAX_IMAGE_BYTES = 5 * 1024 * 1024  # 5 MB
_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}

# Magic byte signatures for supported image formats.
# Each format maps to a list of valid signatures.  A signature is a list
# of (magic_bytes, offset) pairs that must ALL match (AND logic).
_IMAGE_MAGIC: dict[str, list[list[tuple[bytes, int]]]] = {
    ".png":  [[(b"\x89PNG\r\n\x1a\n", 0)]],
    ".jpg":  [[(b"\xff\xd8\xff", 0)]],
    ".jpeg": [[(b"\xff\xd8\xff", 0)]],
    ".gif":  [[(b"GIF87a", 0)], [(b"GIF89a", 0)]],
    # WebP is RIFF container: must have RIFF at 0 AND WEBP at 8
    ".webp": [[(b"RIFF", 0), (b"WEBP", 8)]],
}


def _validate_image_file(file_path: str) -> str | None:
    """Validate that a file with an image extension contains actual image data.

    Returns None if valid, or an error string describing the problem.
    This prevents the CLI from base64-encoding non-image files (e.g. HTML
    redirect pages saved with a .png extension) and poisoning the
    conversation context with an unprocessable image block.
    """
    from pathlib import Path

    ext = Path(file_path).suffix.lower()
    if ext not in _IMAGE_EXTENSIONS:
        return None  # Not an image — nothing to validate

    try:
        size = os.path.getsize(file_path)
    except OSError:
        return None  # Let the Read tool handle missing files

    if size == 0:
        return f"Image file is empty (0 bytes): {file_path}"

    if size > _MAX_IMAGE_BYTES:
        size_mb = size / (1024 * 1024)
        return (
            f"Image file too large ({size_mb:.1f} MB > 5 MB API limit): {file_path}. "
            f"The Anthropic API rejects images larger than 5 MB."
        )

    # Check magic bytes
    magic_specs = _IMAGE_MAGIC.get(ext, [])
    if not magic_specs:
        return None  # No magic spec — let it through

    try:
        with open(file_path, "rb") as f:
            header = f.read(16)
    except OSError:
        return None  # Let the Read tool handle I/O errors

    # Each signature is a list of (bytes, offset) pairs — ALL must match.
    # Multiple signatures per format are OR'd (e.g. GIF87a vs GIF89a).
    for signature in magic_specs:
        if all(
            header[off: off + len(magic)] == magic
            for magic, off in signature
        ):
            return None  # Valid magic — good to go

    # None of the magic signatures matched
    # Check if it's actually HTML (common when auth fails on image URLs)
    is_html = header.lstrip()[:5].lower() in (b"<!doc", b"<html", b"<?xml")
    hint = (
        " The file appears to contain HTML — it may be a redirect or error page "
        "downloaded instead of the actual image."
        if is_html
        else " The file header does not match any supported image format "
        "(JPEG, PNG, GIF, WebP)."
    )
    return (
        f"File {file_path} has {ext} extension but does not contain valid image data.{hint} "
        f"Reading this file would poison the conversation with an unprocessable image block."
    )


def _validate_image_data(data_b64: str, media_type: str) -> str | None:
    """Validate base64-encoded image data before sending to the API.

    Returns None if valid, or an error string describing the problem.
    Used for images entering through Nerve's own pipeline (Telegram, etc).
    """
    import base64

    try:
        raw = base64.b64decode(data_b64[:64])  # Only need first bytes
    except Exception:
        return f"Invalid base64 encoding for {media_type} image"

    if len(raw) < 4:
        return f"Image data too small ({len(raw)} bytes) for {media_type}"

    # Map media_type to extension for magic check
    type_to_ext = {
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/gif": ".gif",
        "image/webp": ".webp",
    }
    ext = type_to_ext.get(media_type)
    if not ext:
        return None  # Unknown type — let the API decide

    magic_specs = _IMAGE_MAGIC.get(ext, [])
    for signature in magic_specs:
        if all(
            raw[off: off + len(magic)] == magic
            for magic, off in signature
        ):
            return None  # Valid

    return (
        f"Image data does not match declared type {media_type}. "
        f"The file header bytes do not contain a valid {ext.upper().strip('.')} signature."
    )


def _sanitize_surrogates(s: str) -> str:
    """Remove orphaned UTF-16 surrogates that break JSON serialization.

    The CLI may truncate large tool output mid-emoji, splitting a surrogate
    pair and leaving an unpaired high/low surrogate.  These are invalid in
    JSON and cause 400 errors from the Anthropic API.
    """
    return _SURROGATE_RE.sub("\ufffd", s) if _SURROGATE_RE.search(s) else s


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


def _parse_mcp_tool_name(tool_name: str) -> tuple[str, str] | None:
    """Parse 'mcp__server__tool' into (server_name, tool_name), or None."""
    if not tool_name or not tool_name.startswith("mcp__"):
        return None
    parts = tool_name.split("__", 2)
    if len(parts) == 3:
        return parts[1], parts[2]
    return None


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
        self._session_locks: dict[str, asyncio.Lock] = {}
        self._router = None  # ChannelRouter — lazy-initialized via .router property
        self._mcp_servers_cache = list(config.mcp_servers)  # hot-reloadable
        self._claude_code_plugins: list[dict[str, str]] = []  # plugin dirs

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
            engine=self,
        )

        # Load Claude Code plugin directories for SDK plugins field
        from nerve.config import load_claude_code_plugins
        self._claude_code_plugins = load_claude_code_plugins()

        # Initialize houseofagents service (optional)
        if self.config.houseofagents.enabled:
            from nerve.houseofagents import init_hoa_service
            svc = init_hoa_service(self.config)
            if svc:
                logger.info("houseofagents service initialized (available=%s)", svc.is_available())

        # Sync MCP servers to DB for frontend visibility
        await self._sync_mcp_servers_to_db()

        # Wire up memorize callback so SessionManager can trigger memU indexing
        self.sessions._on_memorize = self._memorize_session

        # Recover orphaned sessions from previous crash
        try:
            await self.sessions.recover_orphaned_sessions()
        except Exception as e:
            logger.error("Orphaned session recovery failed: %s", e)

        # Worker mode: check if first-boot onboarding is needed
        if self._needs_worker_onboarding():
            asyncio.get_event_loop().call_soon(
                lambda: asyncio.ensure_future(self._run_worker_onboarding())
            )

        logger.info("Agent engine initialized")

    async def _sync_mcp_servers_to_db(self) -> None:
        """Register all known MCP servers (built-in + external) in the DB."""
        # Built-in nerve server
        await self.db.upsert_mcp_server(
            name="nerve", server_type="sdk", enabled=True,
            tool_count=len(ALL_TOOLS),
        )
        # External servers from cache
        for srv in self._mcp_servers_cache:
            await self.db.upsert_mcp_server(
                name=srv.name, server_type=srv.type, enabled=srv.enabled,
            )

    async def reload_mcp_config(self) -> list:
        """Re-read MCP server config from YAML files and update cache + DB.

        New sessions will automatically use the updated config.
        Returns the list of McpServerConfig.
        """
        from nerve.config import load_claude_code_plugins, load_mcp_servers
        self._mcp_servers_cache = load_mcp_servers()
        self._claude_code_plugins = load_claude_code_plugins()
        await self._sync_mcp_servers_to_db()
        logger.info(
            "MCP config reloaded: %d server(s), %d Claude Code plugin(s)",
            len(self._mcp_servers_cache),
            len(self._claude_code_plugins),
        )
        return self._mcp_servers_cache

    def _needs_worker_onboarding(self) -> bool:
        """Check if this is a worker instance that needs first-boot onboarding."""
        task_md = self.config.workspace / "TASK.md"
        if not task_md.exists():
            return False
        content = task_md.read_text(encoding="utf-8").strip()
        # Raw task description from init starts with "# Task\n\n"
        # Structured TASK.md (post-onboarding) has "## Mission"
        return content.startswith("# Task\n") and "## Mission" not in content

    async def _run_worker_onboarding(self) -> None:
        """Run the worker onboarding agent session on first boot."""
        logger.info("Worker onboarding: starting first-boot setup session")

        task_md = self.config.workspace / "TASK.md"
        task_description = task_md.read_text(encoding="utf-8").strip()
        # Strip the "# Task\n\n" prefix
        if task_description.startswith("# Task\n\n"):
            task_description = task_description[len("# Task\n\n"):]

        prompt = (
            "You are running the **first-boot onboarding** for this Nerve worker instance.\n\n"
            f"The user described the task as:\n\n> {task_description}\n\n"
            "Your job is to research this task thoroughly and configure the worker.\n\n"
            "## Step 1: Research\n\n"
            "Use your tools to understand the task deeply:\n"
            "- **Fetch URLs** mentioned in the description (repos, docs, APIs)\n"
            "- **Search the web** for relevant documentation and tools\n"
            "- **Clone repos** if needed to understand their structure\n"
            "- **Explore CI systems**, databases, APIs referenced in the task\n"
            "- Take notes on what you discover — you'll need them for configuration\n\n"
            "## Step 2: Rewrite TASK.md\n\n"
            "Replace the raw description in TASK.md with a structured version:\n"
            "- **## Mission**: What this worker does (1-2 sentences)\n"
            "- **## Scope**: Repos, services, or systems to monitor\n"
            "- **## Triggers**: What events to watch for\n"
            "- **## Actions**: What to do when triggered (step by step)\n"
            "- **## Approval**: What needs human approval vs autonomous action\n"
            "- **## References**: Links to docs, APIs, tools discovered during research\n\n"
            "## Step 3: Create Skills\n\n"
            "Use `skill_create` to create domain-specific skills the worker will need.\n"
            "Each skill should have clear step-by-step instructions for a procedure\n"
            "(e.g., 'how to query the monitoring API', 'how to debug a deployment failure').\n\n"
            "## Step 4: Configure Cron Jobs\n\n"
            "Set up monitoring cron jobs by editing `~/.nerve/cron/jobs.yaml`.\n"
            "This is the Nerve cron system — NOT the Anthropic SDK or system crontab.\n\n"
            "The YAML format is:\n"
            "```yaml\n"
            "jobs:\n"
            "  - id: my-monitor\n"
            "    schedule: '*/15 * * * *'  # cron expression\n"
            "    description: What this job does\n"
            "    session_mode: persistent  # or 'isolated' for one-shot\n"
            "    context_rotate_hours: 24  # reset context daily (persistent only)\n"
            "    enabled: true\n"
            "    prompt: |\n"
            "      Instructions for what the agent should do each run.\n"
            "      Reference Nerve tools: task_create, plan_propose, notify,\n"
            "      memorize, skill_get, web_fetch, bash, etc.\n"
            "```\n\n"
            "Create cron jobs that implement the monitoring/actions described in the task.\n"
            "Use `persistent` session_mode for jobs that need context across runs.\n\n"
            "## Step 5: Create Initial Tasks\n\n"
            "Use `task_create` for any remaining manual setup work the user needs to do.\n\n"
            "## Step 6: Notify\n\n"
            "When done, use `notify` to tell the user that onboarding is complete.\n"
            "Include a summary of what was configured: TASK.md sections, skills created,\n"
            "cron jobs added, and any tasks that need manual attention.\n\n"
            "---\n\n"
            "Be thorough. You have full tool access — bash, web fetch, file read/write,\n"
            "skill_create, task_create, notify. This is a one-time setup — do it right.\n"
        )

        try:
            await self.run_cron(
                job_id="worker-onboarding",
                prompt=prompt,
            )
            logger.info("Worker onboarding: setup session completed")
        except Exception as e:
            logger.error("Worker onboarding failed: %s", e)

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

        thinking_config = self._parse_thinking_config(
            self.config.agent.thinking,
            model or self.config.agent.model,
        )
        effort = (
            self.config.agent.effort
            if self.config.agent.effort in ("low", "medium", "high", "xhigh", "max")
            else None
        )
        betas = (
            ["context-1m-2025-08-07"] if self.config.agent.context_1m else []
        )

        # Build PreToolUse hook for file snapshot capture
        hooks = self._build_snapshot_hooks(session_id)

        def _cli_stderr(line: str) -> None:
            stripped = line.rstrip()
            if not stripped:
                return
            # Filter debug-to-stderr output by severity
            if "[ERROR]" in stripped or "[FATAL]" in stripped:
                logger.error("CLI stderr [%s]: %s", session_id[:8], stripped)
            elif "[WARN]" in stripped:
                logger.warning("CLI stderr [%s]: %s", session_id[:8], stripped)
            elif "[DEBUG]" in stripped or "[INFO]" in stripped:
                logger.debug("CLI stderr [%s]: %s", session_id[:8], stripped)
            else:
                # Non-debug lines (e.g. raw warnings from the CLI)
                logger.warning("CLI stderr [%s]: %s", session_id[:8], stripped)

        extra_args: dict[str, str | None] = {"debug-to-stderr": None}
        # Opus 4.7 defaults thinking.display to "omitted", returning empty
        # thinking blocks with only a signature (for multi-turn continuity).
        # Force "summarized" so the UI actually has thinking text to render.
        # The CLI ignores this flag when thinking is disabled.
        # NOTE: --thinking-display hangs on Bedrock (multi-turn after ToolSearch
        # never returns). Disabled for Bedrock until the provider bug is fixed.
        if (
            thinking_config
            and thinking_config.get("type") != "disabled"
            and not self.config.provider.is_bedrock
        ):
            extra_args["thinking-display"] = "summarized"

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
            stderr=_cli_stderr,
            extra_args=extra_args,
            # No allowed_tools — can_use_tool callback handles permissions.
            # External MCP server tools are discovered at connection time,
            # so we can't enumerate them upfront.
            env=self._build_env(),
            cwd=str(self.config.workspace),
            mcp_servers=self._build_mcp_servers(session_id),
            # Claude Code plugins — loaded via --plugin-dir so the CLI
            # handles OAuth, credentials, and plugin lifecycle natively.
            plugins=self._claude_code_plugins,
        )


    def _build_env(self) -> dict[str, str]:
        """Build environment variables for the SDK subprocess."""
        env: dict[str, str] = {}
        if self.config.provider.is_bedrock:
            env["CLAUDE_CODE_USE_BEDROCK"] = "1"
            if self.config.provider.aws_region:
                env["AWS_REGION"] = self.config.provider.aws_region
            if self.config.provider.aws_profile:
                env["AWS_PROFILE"] = self.config.provider.aws_profile
            if self.config.provider.aws_access_key_id:
                env["AWS_ACCESS_KEY_ID"] = self.config.provider.aws_access_key_id
                env["AWS_SECRET_ACCESS_KEY"] = self.config.provider.aws_secret_access_key
        else:
            api_key = self.config.effective_api_key
            if api_key:
                env["ANTHROPIC_API_KEY"] = api_key
            if self.config.proxy.enabled:
                env["ANTHROPIC_BASE_URL"] = (
                    f"http://{self.config.proxy.host}:{self.config.proxy.port}"
                )
        return env

    def _build_mcp_servers(self, session_id: str) -> dict[str, Any]:
        """Build the mcp_servers dict: built-in nerve + external servers from config.

        Claude Code plugin MCPs are handled separately via the SDK ``plugins``
        field which lets the CLI manage OAuth and plugin lifecycle natively.
        """
        servers: dict[str, Any] = {
            # Per-session MCP server with session_id bound in closure —
            # ensures notify/ask_user always reference the correct session.
            "nerve": create_session_mcp_server(session_id),
        }
        for srv in self._mcp_servers_cache:
            if srv.enabled and srv.name != "nerve":
                try:
                    servers[srv.name] = srv.to_sdk_config()
                except ValueError as e:
                    logger.warning("Skipping MCP server %r: %s", srv.name, e)
        if len(servers) > 1:
            logger.debug(
                "Session %s: %d MCP servers (%s)",
                session_id[:8], len(servers),
                ", ".join(servers.keys()),
            )
        return servers

    def _build_snapshot_hooks(self, session_id: str) -> dict:
        """Build PreToolUse hooks for file snapshots and image validation."""
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

        async def _validate_image_hook(hook_input, tool_use_id, context):
            """PreToolUse hook: validate image files before Read to prevent
            poisoning the conversation with unprocessable image data.

            The CLI's Read tool detects images by file extension and base64-
            encodes them into image content blocks.  If the file isn't a valid
            image (e.g. an HTML redirect saved as .png), the API rejects it
            with 400 "Could not process image".  Worse, the bad block persists
            in the CLI's conversation history, causing *every* subsequent turn
            to fail — an unrecoverable poison loop.

            This hook checks magic bytes and size *before* Read executes,
            blocking invalid files with a clear error message so the agent
            can adjust.
            """
            tool_input = hook_input.get("tool_input", {})
            file_path = tool_input.get("file_path", "")

            error = _validate_image_file(file_path)
            if error:
                logger.warning(
                    "Blocked Read of invalid image for session %s: %s",
                    session_id[:8], error,
                )
                return {
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "deny",
                        "permissionDecisionReason": error,
                    },
                }

            return {"hookSpecificOutput": {"hookEventName": "PreToolUse"}}

        return {
            "PreToolUse": [
                HookMatcher(
                    matcher="Edit|Write|NotebookEdit",
                    hooks=[_snapshot_hook],
                ),
                HookMatcher(
                    matcher="Read",
                    hooks=[_validate_image_hook],
                ),
            ],
        }

    @staticmethod
    def _model_supports_legacy_enabled_thinking(model: str | None) -> bool:
        # Claude 4.5 / 4.6 accept thinking.type="enabled" with budget_tokens.
        # Newer models (4.7+) require thinking.type="adaptive" with effort.
        if not model:
            return False
        m = model.lower()
        return "4-5" in m or "4-6" in m

    @staticmethod
    def _parse_thinking_config(value: str, model: str | None = None) -> dict | None:
        """Parse thinking config string into SDK ThinkingConfig dict."""
        v = value.strip().lower()
        if v == "disabled":
            return {"type": "disabled"}
        if v == "adaptive":
            return {"type": "adaptive"}
        if not AgentEngine._model_supports_legacy_enabled_thinking(model):
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

    @staticmethod
    def _is_client_dead(client: ClaudeSDKClient) -> bool:
        """Check if the client's underlying CLI process has terminated."""
        transport = getattr(client, "_transport", None)
        if not transport:
            return True
        process = getattr(transport, "_process", None)
        if process is None:
            return True
        return process.returncode is not None

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
                # Health check: verify the underlying CLI process is still alive
                if self._is_client_dead(client):
                    logger.warning(
                        "Client process for session %s is dead, recreating",
                        session_id,
                    )
                    self.sessions.remove_client(session_id)
                    unregister_handler(session_id)
                    await self._safe_disconnect(client)
                    client = None
                else:
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

            # Create interactive tool handler for this session.
            # Non-web sessions (telegram, cron, hook) cannot handle interactive
            # tools — auto-deny them to prevent deadlocks.
            is_interactive = source in ("web",)
            handler = InteractiveToolHandler(
                session_id=session_id,
                broadcast_fn=broadcaster.broadcast,
                snapshot_fn=self._save_file_snapshot,
                interactive_capable=is_interactive,
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

            # Record connected_at and the resolved model
            resolved_model = options.model
            now = datetime.now(timezone.utc).isoformat()
            connected_at = session.get("connected_at") if session and sdk_resume_id else now
            await self.sessions.mark_active(
                session_id,
                sdk_session_id=sdk_resume_id,
                connected_at=connected_at,
            )
            await self.db.update_session_fields(session_id, {"model": resolved_model})

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
    #  Tool-result helpers                                                 #
    # ------------------------------------------------------------------ #

    async def _process_tool_result(
        self,
        block: ToolResultBlock,
        session_id: str,
        parent_tool_use_id: str | None,
        tool_results_map: dict[str, dict],
        ordered_blocks: list[dict],
        tool_calls_log: list[dict],
        active_subagents: dict[str, float],
    ) -> None:
        """Process a single ToolResultBlock (shared by AssistantMessage and UserMessage paths)."""
        result_content = (
            block.content
            if isinstance(block.content, str)
            else json.dumps(block.content, default=str)
        )
        # Sanitize orphaned surrogates — CLI may truncate output mid-emoji
        result_content = _sanitize_surrogates(result_content)
        tool_use_id = getattr(block, "tool_use_id", None)
        is_error = getattr(block, "is_error", False)

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
            parent_tool_use_id=parent_tool_use_id,
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

        # Auto-broadcast plan/file updates
        if not is_error and tool_use_id:
            _maybe_broadcast_plan_update(session_id, tool_use_id, tool_calls_log)
            _maybe_broadcast_file_changed(session_id, tool_use_id, tool_calls_log)

        # Record MCP tool usage for frontend stats
        if tool_use_id:
            for tc in reversed(tool_calls_log):
                if tc.get("tool_use_id") == tool_use_id:
                    parsed = _parse_mcp_tool_name(tc.get("tool", ""))
                    if parsed:
                        srv_name, mcp_tool = parsed
                        try:
                            duration = None
                            if tool_use_id in active_subagents:
                                # Sub-agent already popped above, but for
                                # regular MCP tools we don't track start time
                                pass
                            # Auto-register unknown MCP servers on first use
                            # (e.g. Claude Code plugins: "plugin_Notion_notion").
                            # Skip servers already registered at startup to avoid
                            # overwriting their type (nerve=sdk, grafana=stdio).
                            known = {"nerve"} | {
                                s.name for s in self._mcp_servers_cache
                            }
                            if srv_name not in known:
                                await self.db.upsert_mcp_server(
                                    name=srv_name, server_type="plugin",
                                    enabled=True,
                                )
                            await self.db.record_mcp_tool_usage(
                                server_name=srv_name,
                                tool_name=mcp_tool,
                                session_id=session_id,
                                duration_ms=duration,
                                success=not is_error,
                                error=result_content[:500] if is_error else None,
                            )
                        except Exception as e:
                            logger.debug("Failed to record MCP usage: %s", e)
                    break

    @staticmethod
    def _merge_tool_results(
        tool_calls_log: list[dict],
        tool_results_map: dict[str, dict],
    ) -> None:
        """Merge collected tool results back into tool_calls_log entries."""
        for tc in tool_calls_log:
            tid = tc.get("tool_use_id")
            if tid and tid in tool_results_map:
                tc["result"] = tool_results_map[tid]["result"]
                tc["is_error"] = tool_results_map[tid]["is_error"]

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
        internal: bool = False,
        images: list[dict[str, Any]] | None = None,
        image_refs: list[dict[str, Any]] | None = None,
    ) -> str:
        """Run the agent for a user message and return the final text response.

        Args:
            internal: If True, the user_message is a system-generated trigger
                      (e.g., background task completion) and won't be stored in
                      DB or shown in the UI.
            images: Optional list of image dicts with keys ``type``,
                    ``media_type``, and ``data`` (base64-encoded).
            image_refs: Optional metadata about uploaded files for persisting
                        in the user message blocks column (web uploads only).
        """
        # Serialize runs per session — messages for the same session wait
        # in order instead of failing with "already running".
        lock = self._session_locks.setdefault(session_id, asyncio.Lock())
        async with lock:
            broadcaster.start_buffering(session_id)
            async with self._semaphore:
                # Clear any stale deferred-stop flag left over from a *previous*
                # turn.  If /stop arrived while the old turn was still cleaning up
                # (mark_not_running hadn't run yet), the flag lingers and would
                # immediately kill this brand-new turn.  Flags set *during* this
                # turn's client init are unaffected — they're created after
                # mark_running below.
                self.sessions.pop_stop_request(session_id)
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
                        internal=internal, images=images,
                        image_refs=image_refs,
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
        internal: bool = False,
        images: list[dict[str, Any]] | None = None,
        image_refs: list[dict[str, Any]] | None = None,
    ) -> str:
        # Ensure session exists in DB
        await self.sessions.get_or_create(session_id, source=source)

        session = await self.db.get_session(session_id)

        if not internal and session:
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

            # Store user message in DB (note attached images for display)
            db_text = user_message
            if images:
                # Count only image/pdf entries, not text_file entries
                img_count = sum(1 for img in images if img.get("type") != "text_file")
                if img_count:
                    suffix = f"\n[{img_count} image(s) attached]"
                    db_text = (user_message + suffix) if user_message else suffix.strip()
            await self.sessions.add_message(
                session_id, "user", db_text, channel=channel,
                blocks=image_refs,
            )

        full_response_text = ""
        thinking_text = ""
        tool_calls_log: list[dict] = []
        tool_results_map: dict[str, dict] = {}
        ordered_blocks: list[dict] = []  # preserves interleaving for DB
        last_usage: dict | None = None
        sdk_session_id: str | None = None
        active_subagents: dict[str, float] = {}  # tool_use_id -> monotonic start
        result_meta: dict | None = None  # ResultMessage fields beyond usage
        last_model: str | None = None  # model from most recent AssistantMessage

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

            # Check for deferred /stop that arrived while we were setting up
            if self.sessions.pop_stop_request(session_id):
                logger.info("Stop requested before agent turn — aborting session %s", session_id)
                return ""

            # Send message — the client preserves conversation history internally
            # Escape slash-prefixed messages so Claude Code CLI doesn't
            # intercept them as built-in slash commands.  Registered bot
            # commands (/stop, /new, etc.) are handled upstream — anything
            # that reaches here should go straight to the LLM.
            query_text = user_message
            if query_text and query_text.startswith("/"):
                query_text = "\u200b" + query_text

            # Build multi-modal content blocks once (reused on retry)
            if images:
                content_blocks: list[dict[str, Any]] = []
                if query_text:
                    content_blocks.append({"type": "text", "text": query_text})
                for img in images:
                    # Text files are inlined as text context blocks
                    if img.get("type") == "text_file":
                        fname = img.get("filename", "file")
                        content = img.get("content", "")
                        content_blocks.append({
                            "type": "text",
                            "text": f"--- Attached: {fname} ---\n{content}",
                        })
                        continue

                    # PDFs use "document" content block; images use "image"
                    block_type = "document" if img["media_type"] == "application/pdf" else "image"

                    # Validate image data before sending — prevent poisoning
                    # the CLI's conversation with unprocessable images.
                    if block_type == "image":
                        img_error = _validate_image_data(
                            img["data"], img["media_type"],
                        )
                        if img_error:
                            logger.warning(
                                "Skipping invalid image for session %s: %s",
                                session_id[:8], img_error,
                            )
                            # Inject as text so the agent knows what happened
                            content_blocks.append({
                                "type": "text",
                                "text": f"[Image skipped: {img_error}]",
                            })
                            continue

                    content_blocks.append({
                        "type": block_type,
                        "source": {
                            "type": img["type"],
                            "media_type": img["media_type"],
                            "data": img["data"],
                        },
                    })

            # Send query + read response, with auto-retry on CLI crash.
            # The CLI may crash during query (CLIConnectionError) or during
            # response reading (generic Exception from the SDK reader task).
            # Retry once with a fresh client if no content was received yet.
            _got_response_content = False
            for _attempt in range(2):
                try:
                    if images:
                        async def _image_prompt():
                            yield {
                                "type": "user",
                                "message": {"role": "user", "content": content_blocks},
                                "parent_tool_use_id": None,
                            }

                        await client.query(_image_prompt())
                    else:
                        await client.query(query_text)
                except CLIConnectionError as _qerr:
                    if _attempt > 0:
                        raise
                    logger.warning(
                        "CLI dead for session %s (query phase): %s — retrying",
                        session_id, _qerr,
                    )
                    self.sessions.remove_client(session_id)
                    unregister_handler(session_id)
                    await self._safe_disconnect(client)
                    client = await self._get_or_create_client(
                        session_id, source, model,
                    )
                    continue  # retry the query

                # Read response — may raise if CLI crashes mid-stream
                try:
                    async for message in client.receive_response():
                        # Early-capture sdk_session_id from first message that
                        # carries it so it survives /stop cancellation (ResultMessage
                        # — the normal source — never arrives when the turn is
                        # interrupted).
                        if not sdk_session_id:
                            msg_sid = getattr(message, "session_id", None)
                            if msg_sid:
                                sdk_session_id = msg_sid

                        if isinstance(message, AssistantMessage):
                            _got_response_content = True
                            # Capture model from assistant message (more reliable than config)
                            msg_model = getattr(message, 'model', None)
                            if msg_model:
                                last_model = msg_model
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
                                    thinking = getattr(block, "thinking", "") or ""
                                    if not thinking:
                                        # Empty thinking block (e.g. Opus 4.7 with
                                        # display="omitted", or simple queries on
                                        # low effort). Nothing visible to render —
                                        # never fall back to str(block) as that
                                        # leaks the ThinkingBlock(...) repr into
                                        # the UI.
                                        continue
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
                                    tool_input = getattr(block, "input", {})
                                    tool_name = getattr(block, "name", None) or str(block)
                                    tool_use_id = getattr(block, "id", None)
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
                                    await self._process_tool_result(
                                        block, session_id, parent_id,
                                        tool_results_map, ordered_blocks,
                                        tool_calls_log, active_subagents,
                                    )

                        elif isinstance(message, UserMessage):
                            parent_id = getattr(message, 'parent_tool_use_id', None)
                            content = getattr(message, "content", [])
                            if isinstance(content, list):
                                for block in content:
                                    if isinstance(block, ToolResultBlock):
                                        await self._process_tool_result(
                                            block, session_id, parent_id,
                                            tool_results_map, ordered_blocks,
                                            tool_calls_log, active_subagents,
                                        )

                        elif isinstance(message, ResultMessage):
                            if message.usage:
                                last_usage = message.usage
                            sdk_session_id = message.session_id
                            result_meta = {
                                "total_cost_usd": getattr(message, "total_cost_usd", None),
                                "duration_ms": getattr(message, "duration_ms", None),
                                "duration_api_ms": getattr(message, "duration_api_ms", None),
                                "num_turns": getattr(message, "num_turns", None),
                            }

                except asyncio.CancelledError:
                    raise  # propagate to outer handler
                except Exception as _recv_err:
                    # CLI crashed during response reading.
                    # Retry only if we haven't received any content yet
                    # (otherwise we'd produce duplicate/garbled output).
                    if _got_response_content or _attempt > 0:
                        raise
                    logger.warning(
                        "CLI crashed for session %s during response "
                        "(no content yet): %s — retrying with fresh client",
                        session_id, _recv_err,
                    )
                    self.sessions.remove_client(session_id)
                    unregister_handler(session_id)
                    await self._safe_disconnect(client)
                    client = await self._get_or_create_client(
                        session_id, source, model,
                    )
                    continue  # retry query + response
                break  # success — exit retry loop

        except asyncio.CancelledError:
            logger.info("Session %s cancelled by user", session_id)
            partial = full_response_text + (
                "\n\n[Stopped by user]"
                if full_response_text
                else "[Stopped by user]"
            )

            # --- Critical cleanup first (must succeed for resume) ----------
            # Persist sdk_session_id so the session can be resumed later.
            # For new sessions the DB still has NULL because mark_active()
            # was called before the SDK emitted any messages.
            if sdk_session_id:
                await self.db.update_session_fields(
                    session_id, {"sdk_session_id": sdk_session_id},
                )
            await self.sessions.mark_stopped(session_id)
            unregister_handler(session_id)
            client = self.sessions.remove_client(session_id)
            if client:
                await self._safe_disconnect(client)

            # --- Non-critical: save message, broadcast, memorize -----------
            try:
                self._merge_tool_results(tool_calls_log, tool_results_map)
                await self.sessions.add_message(
                    session_id, "assistant", partial,
                    channel=channel,
                    thinking=thinking_text if thinking_text else None,
                    blocks=ordered_blocks if ordered_blocks else None,
                )
                await broadcaster.broadcast(session_id, {
                    "type": "stopped", "session_id": session_id,
                })
            except Exception as cleanup_err:
                logger.warning(
                    "Non-critical stop cleanup failed for %s: %s",
                    session_id, cleanup_err,
                )
            # Memorize in background — don't block the stop path
            asyncio.create_task(self._memorize_session(session_id))
            return partial

        except Exception as e:
            error_msg = f"Agent error: {e}"
            logger.error(error_msg, exc_info=True)

            # --- Poisoned context detection (Layer 2 safety net) ---
            # If the CLI's conversation history contains an unprocessable
            # image or document, every subsequent API call re-sends it and
            # gets 400.  The PreToolUse hook on Read (Layer 1) prevents
            # most cases, but images can also enter via MCP tools, sub-
            # agents, or the CLI's own internal processing.
            # When detected: kill the CLI, clear sdk_session_id so the
            # next turn starts a fresh conversation.
            err_str = str(e)
            is_poisoned = (
                "Could not process image" in err_str
                or "Could not process document" in err_str
            )
            if is_poisoned:
                logger.warning(
                    "Poisoned context detected for session %s: %s — "
                    "killing CLI and clearing session to prevent loop",
                    session_id[:8], err_str,
                )
                error_msg = (
                    "The conversation contained an unprocessable image or "
                    "document that caused the API to reject every request. "
                    "The session has been reset to recover. The conversation "
                    "context was lost — please re-state your request."
                )
                # Clear sdk_session_id so next turn creates a fresh CLI
                await self.db.update_session_fields(
                    session_id, {"sdk_session_id": None},
                )

            await broadcaster.broadcast_error(session_id, error_msg)
            # Memorize before discarding client
            await self._memorize_session(session_id)
            # Clear resume — CLI state may be corrupted after error
            unregister_handler(session_id)
            client = self.sessions.remove_client(session_id)
            await self.sessions.mark_error(session_id, error_msg)
            if client:
                await self._safe_disconnect(client)
            full_response_text = error_msg

        # Merge tool results into tool_calls_log
        self._merge_tool_results(tool_calls_log, tool_results_map)

        # Detect background tasks for auto-resume after turn ends.
        # When Bash/Task runs with run_in_background, the result is only
        # picked up on the NEXT engine.run() call.  We spawn a watcher that
        # polls the output file and auto-triggers a new run when it's ready.
        # NOTE: must run AFTER _merge_tool_results so tc["result"] is populated.
        bg_tasks: list[dict] = []  # {output_file, tool, description, command, task_id}
        for tc in tool_calls_log:
            inp = tc.get("input") or {}
            if not inp.get("run_in_background"):
                continue
            result_text = tc.get("result", "")
            # Extract output file from result text
            m = re.search(r"output_file:\s*(\S+)|Output is being written to:\s*(\S+)", result_text)
            if not m:
                logger.warning("Background task detected but no output file: %.200s", result_text)
                continue
            output_file = m.group(1) or m.group(2)
            # Extract task ID
            m_id = re.search(r"(?:ID|agentId):\s*(\S+)", result_text)
            task_id = m_id.group(1).rstrip(".") if m_id else os.path.basename(output_file).replace(".output", "")
            tool_name = tc.get("tool", "Bash")
            description = inp.get("description", "")
            command = inp.get("command", "")
            # Truncate long commands for display
            label = description or (command[:60] + "..." if len(command) > 60 else command) or tool_name
            bg_tasks.append({
                "output_file": output_file,
                "task_id": task_id,
                "tool": tool_name,
                "label": label,
            })
            logger.info("Tracking background task %s: %s", task_id, label)

        # Store assistant message in DB
        await self.sessions.add_message(
            session_id, "assistant", full_response_text,
            channel=channel,
            thinking=thinking_text if thinking_text else None,
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
        num_turns = (result_meta or {}).get("num_turns") or 1
        if last_usage:
            usage_data = {
                **last_usage,
                "max_context_tokens": max_context,
                "num_turns": num_turns,
            }
            session_record = await self.db.get_session(session_id)
            meta = json.loads(session_record.get("metadata") or "{}") if session_record else {}
            meta["last_usage"] = usage_data

            # Extract server_tool_use counts
            server_tool = last_usage.get("server_tool_use") or {}
            web_search = server_tool.get("web_search_requests", 0)
            web_fetch = server_tool.get("web_fetch_requests", 0)

            # Calculate per-turn cost.
            # NOTE: The SDK's total_cost_usd is *cumulative* across the
            # entire SDK session, NOT per-invocation.  We track the last
            # known cumulative value in session metadata so we can compute
            # the delta for this turn.
            from nerve.db.usage import estimate_turn_cost
            sdk_cost = (result_meta or {}).get("total_cost_usd")
            current_session_cost = (
                session_record.get("total_cost_usd", 0) if session_record else 0
            ) or 0

            if sdk_cost is not None:
                prev_cumulative = meta.get("_sdk_cumulative_cost", 0) or 0
                turn_cost = max(sdk_cost - prev_cumulative, 0)
                meta["_sdk_cumulative_cost"] = sdk_cost
            else:
                turn_cost = estimate_turn_cost(last_usage, model=last_model)

            # Save metadata (includes _sdk_cumulative_cost update)
            await self.db.update_session_metadata(session_id, meta)

            # Persist per-turn usage to session_usage table
            await self.db.record_turn_usage(
                session_id=session_id,
                input_tokens=last_usage.get("input_tokens", 0),
                output_tokens=last_usage.get("output_tokens", 0),
                cache_creation=last_usage.get("cache_creation_input_tokens", 0),
                cache_read=last_usage.get("cache_read_input_tokens", 0),
                max_context=max_context,
                model=last_model,
                cost_usd=turn_cost,
                duration_ms=(result_meta or {}).get("duration_ms"),
                duration_api_ms=(result_meta or {}).get("duration_api_ms"),
                num_turns=num_turns,
                web_search_requests=web_search,
                web_fetch_requests=web_fetch,
            )

            # Update total_cost_usd on the session
            await self.db.update_session_fields(session_id, {
                "total_cost_usd": current_session_cost + turn_cost,
            })

        await broadcaster.broadcast_done(
            session_id,
            usage=last_usage,
            max_context_tokens=max_context,
            num_turns=num_turns,
        )
        self.sessions.touch(session_id)

        # Spawn background task watcher if needed
        if bg_tasks:
            # Notify UI about running background tasks
            await broadcaster.broadcast(session_id, {
                "type": "background_tasks_update",
                "session_id": session_id,
                "tasks": [
                    {"task_id": t["task_id"], "label": t["label"], "tool": t["tool"], "status": "running"}
                    for t in bg_tasks
                ],
            })
            asyncio.create_task(
                self._watch_background_tasks(
                    session_id, bg_tasks, source, channel,
                )
            )

        return full_response_text

    async def _watch_background_tasks(
        self,
        session_id: str,
        bg_tasks: list[dict],
        source: str,
        channel: str | None,
    ) -> None:
        """Poll background task output files and auto-trigger engine.run().

        When tools run with run_in_background, the SDK subprocess exits after
        the main turn.  The background process writes to an output file.
        This watcher polls until the file is ready, then triggers a new
        engine.run() so the model processes the result immediately instead
        of waiting for the next user message.
        """
        poll_interval = 2  # seconds
        max_wait = 600  # 10 minutes max
        completed_ids: set[str] = set()

        try:
            elapsed = 0
            while elapsed < max_wait:
                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

                all_done = True
                newly_completed = False
                for task in bg_tasks:
                    if task["task_id"] in completed_ids:
                        continue
                    path = task["output_file"]
                    if not os.path.exists(path):
                        all_done = False
                        continue
                    try:
                        size = os.path.getsize(path)
                        if size == 0:
                            all_done = False
                            continue
                        # Check if file is still being written (modified in last 2s)
                        mtime = os.path.getmtime(path)
                        if (time.time() - mtime) < 2:
                            all_done = False
                            continue
                        # This task is done
                        completed_ids.add(task["task_id"])
                        newly_completed = True
                    except OSError:
                        all_done = False
                        continue

                # Broadcast progress to UI on any change
                if newly_completed:
                    await broadcaster.broadcast(session_id, {
                        "type": "background_tasks_update",
                        "session_id": session_id,
                        "tasks": [
                            {
                                "task_id": t["task_id"],
                                "label": t["label"],
                                "tool": t["tool"],
                                "status": "done" if t["task_id"] in completed_ids else "running",
                            }
                            for t in bg_tasks
                        ],
                    })

                if all_done:
                    break

            if elapsed >= max_wait:
                logger.warning(
                    "Background task watcher timed out for session %s",
                    session_id,
                )
                # Broadcast timeout status
                await broadcaster.broadcast(session_id, {
                    "type": "background_tasks_update",
                    "session_id": session_id,
                    "tasks": [
                        {
                            "task_id": t["task_id"],
                            "label": t["label"],
                            "tool": t["tool"],
                            "status": "done" if t["task_id"] in completed_ids else "timeout",
                        }
                        for t in bg_tasks
                    ],
                })
                return

            # All background tasks done — auto-resume the session
            logger.info(
                "Background tasks completed for session %s, auto-resuming",
                session_id,
            )

            if self.sessions.is_running(session_id):
                logger.info(
                    "Session %s already running, skipping auto-resume",
                    session_id,
                )
                return

            # Trigger a new engine.run() so the model picks up the
            # background task notifications from the SDK
            task = asyncio.create_task(
                self.run(
                    session_id=session_id,
                    user_message=(
                        "[Background tasks completed. "
                        "Check the results with TaskOutput and report to the user.]"
                    ),
                    source=source,
                    channel=channel,
                    internal=True,
                )
            )
            self.register_task(session_id, task)

        except Exception as e:
            logger.error(
                "Background task watcher failed for session %s: %s",
                session_id, e,
            )

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
        """Generate a meaningful short title for a session using a fast model."""
        try:
            # Skip if no credentials are configured (neither API key nor Bedrock)
            if not self.config.provider.is_bedrock and not self.config.effective_api_key:
                return

            client = self.config.create_anthropic_client(timeout=10.0)
            response = client.messages.create(
                model=self.config.agent.title_model,
                max_tokens=30,
                messages=[{
                    "role": "user",
                    "content": (
                        "Generate a short title (3-5 words, no quotes)"
                        " for a conversation that starts with:\n\n"
                        f"{first_message[:200]}"
                    ),
                }],
            )
            title = response.content[0].text.strip().strip('"\'').lstrip('#').strip()
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
