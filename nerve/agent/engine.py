"""Agent engine — Claude Agent SDK wrapper.

Orchestrates SDK clients and delegates all session state to SessionManager.
The SDK handles context management and compaction internally.
Sessions are resumable across server restarts via SDK's --resume flag.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import anyio

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ResultMessage,
    SystemMessage,
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
from nerve.agent.tools import (
    ToolContext,
    ToolRegistry,
    build_default_registry,
    build_session_mcp_server,
)
# Legacy back-compat: ``init_tools`` populates ``nerve.agent.tools``'s
# module globals so test fixtures that patch them and the shared
# ``plan_service`` helper (which builds its ctx via ``_legacy_ctx``)
# keep working. The new runtime path uses ``self.registry`` + a
# per-session ``ToolContext`` and ignores those globals.
from nerve.agent.tools import init_tools
from nerve.config import NerveConfig, load_mcp_servers
from nerve.db import Database
from nerve.observability.langfuse import attributes as lf_attrs
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

# Linux execve() limits a single argv element to MAX_ARG_STRLEN = PAGE_SIZE * 32
# = 131,072 bytes on common configurations. The Claude Agent SDK passes the
# system prompt inline as `--system-prompt <STRING>`, which makes the string a
# single argv element. When SOUL.md + TASK.md + AGENTS.md + TOOLS.md +
# MEMORY.md + recalled memU summaries cross that boundary, execve() returns
# E2BIG ("Argument list too long") and Claude Code fails to start.
#
# We sidestep the limit by writing the prompt to a file and passing
# `SystemPromptFile = {"type": "file", "path": ...}` (which the SDK converts
# to `--system-prompt-file <PATH>` — the path string is short).
#
# Threshold below which we keep passing inline (preserves prompt-cache hit
# behavior for small, stable prompts). Set conservatively well under the
# kernel limit to leave room for env/argv overhead.
_SYSTEM_PROMPT_INLINE_MAX = 100_000  # bytes

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


@dataclass
class _TurnState:
    """Accumulates one agent turn's worth of streamed content.

    Shared by the user-run path (``_run_inner``) and the autonomous-turn
    drain (``_drain_pending_messages``) so both produce identical UI
    broadcasts and DB records.
    """

    full_response_text: str = ""
    thinking_text: str = ""
    tool_calls_log: list[dict] = field(default_factory=list)
    tool_results_map: dict[str, dict] = field(default_factory=dict)
    ordered_blocks: list[dict] = field(default_factory=list)
    last_usage: dict | None = None
    sdk_session_id: str | None = None
    # tool_use_id -> monotonic start time of an in-flight sub-agent
    active_subagents: dict[str, float] = field(default_factory=dict)
    result_meta: dict | None = None
    last_model: str | None = None
    # True once any AssistantMessage was received (gates CLI-crash retry)
    got_content: bool = False


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
        self._xmemory_bridge = None
        self._skill_manager: SkillManager | None = None
        self._memorize_lock = asyncio.Lock()
        # Background memorization tasks (see schedule_memorize) — strong
        # refs so the tasks aren't GC'd mid-flight; pruned by their
        # done-callbacks and flushed in shutdown().
        self._memorize_bg_tasks: set[asyncio.Task] = set()
        self._session_locks: dict[str, asyncio.Lock] = {}
        # Idle stream watchers — one per live SDK client. Between run()
        # calls nothing reads the SDK message stream, but the CLI keeps
        # producing: background tasks (Bash/Agent run_in_background,
        # Monitor) settle with task_notification events that trigger FULL
        # autonomous agent turns inside the subprocess. The watcher drains
        # those through the normal processing pipeline so they stream to
        # the UI live instead of buffering invisibly (and then desyncing
        # the next receive_response()). See _idle_stream_watcher.
        self._idle_watchers: dict[str, asyncio.Task] = {}
        # Per-session background-task registry driven by the CLI's
        # task_started / task_updated / task_notification system messages:
        # session_id -> task_id -> {task_id, label, tool, status}.
        self._bg_task_registry: dict[str, dict[str, dict[str, Any]]] = {}
        # Per-session dynamic-workflow registry: session_id -> tool_use_id ->
        # {name, snapshot}. The tool_use_id is captured when a ``Workflow``
        # tool call streams; later task_* system messages carrying a
        # ``workflow_progress`` tree are matched back to it so the UI can
        # render a live phase/agent panel. The last snapshot is cached so the
        # terminal task_notification (which omits the tree) can still settle
        # the panel and persist the final state.
        self._workflows: dict[str, dict[str, dict[str, Any]]] = {}
        # Per-session active channel — set on run() entry, cleared on exit.
        # Read by session-scoped tools (send_file) to avoid dispatching via
        # stale router context from a prior inbound channel.
        self._active_channel: dict[str, str] = {}
        # Resolved model bound to each session's live SDK client. Used to
        # detect mid-session model switches (the CLI fixes its model at
        # connect time, so a change requires recreating the client).
        self._session_models: dict[str, str] = {}
        self._router = None  # ChannelRouter — lazy-initialized via .router property
        self._mcp_servers_cache = list(config.mcp_servers)  # hot-reloadable
        self._claude_code_plugins: list[dict[str, str]] = []  # plugin dirs

        # Tool registry — built once at construction. Per-session MCP
        # servers are built in ``_build_mcp_servers`` by binding a fresh
        # ``ToolContext`` (with the session_id) into closures.
        self.registry: ToolRegistry = build_default_registry()

        # NotificationService is wired in by ``gateway/server.py`` after
        # ``initialize()`` returns (it depends on the engine being live
        # so the channels are routable). Use ``set_notification_service``
        # to install it; ``ToolContext`` constructed per session picks
        # up the reference from here.
        self.notification_service: Any = None

    def set_notification_service(self, service: Any) -> None:
        """Install the notification service used by per-session ``ToolContext``.

        Called once during gateway startup. We accept ``Any`` to avoid
        a circular import with :mod:`nerve.notifications.service`.
        """
        self.notification_service = service

    def get_active_channel(self, session_id: str) -> str | None:
        """Return the channel name currently driving ``session_id`` (or None)."""
        return self._active_channel.get(session_id)

    async def initialize(self) -> None:
        """Initialize the agent engine — set up tools and main session."""
        from nerve.memory.memu_bridge import MemUBridge
        self._memory_bridge = MemUBridge(self.config, audit_db=self.db)
        await self._memory_bridge.initialize()

        # Optional xmemory.ai structured-memory layer — inert unless both a
        # token and instance_id are configured. Runs alongside memU; never
        # replaces it. ``initialize`` never raises.
        from nerve.memory.xmemory_bridge import XmemoryBridge
        self._xmemory_bridge = XmemoryBridge(self.config.xmemory)
        await self._xmemory_bridge.initialize()

        # Initialize skill manager and discover skills from filesystem
        self._skill_manager = SkillManager(self.config.workspace, self.db)
        try:
            skills = await self._skill_manager.discover()
            logger.info("Skills system initialized: %d skills discovered", len(skills))
        except Exception as e:
            logger.error("Skills discovery failed: %s", e)

        # Make skill manager available to prompts and tools
        set_skill_manager(self._skill_manager)
        # init_tools seeds ``nerve.agent.tools``'s back-compat module
        # globals so legacy callers (tests that patch ``tools._workspace``,
        # ``plan_service`` via ``_legacy_ctx``) keep working. The new
        # runtime path builds a fresh ``ToolContext`` per session inside
        # ``_build_mcp_servers`` and doesn't read these.
        init_tools(
            self.config.workspace, self.db,
            memory_bridge=self._memory_bridge,
            xmemory_bridge=self._xmemory_bridge,
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
        # Built-in nerve server. HoA tools are only exposed when enabled,
        # so the count reflects the runtime visible set rather than the
        # full registry. The frontend uses this number as a hint and is
        # not load-bearing.
        include_hoa = bool(self.config.houseofagents.enabled)
        tool_count = len(self.registry.list(include_hoa=include_hoa))
        await self.db.upsert_mcp_server(
            name="nerve", server_type="sdk", enabled=True,
            tool_count=tool_count,
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
        raw_task = await asyncio.to_thread(task_md.read_text, encoding="utf-8")
        task_description = raw_task.strip()
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
        for sid in list(self._idle_watchers):
            self._stop_idle_watcher(sid)

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

        # Cancel queued background memorizations — the periodic sweep
        # re-indexes anything they would have covered (the watermark is
        # only advanced after a successful pass).
        for task in list(self._memorize_bg_tasks):
            task.cancel()
        if self._memorize_bg_tasks:
            await asyncio.gather(
                *self._memorize_bg_tasks, return_exceptions=True,
            )
        self._memorize_bg_tasks.clear()

        # Close the optional xmemory HTTP client (no-op when disabled).
        if self._xmemory_bridge is not None:
            try:
                await self._xmemory_bridge.aclose()
            except Exception as e:  # pragma: no cover - defensive
                logger.debug("Error closing xmemory bridge: %s", e)

        # Stop the memU bridge's dedicated event-loop thread.
        if self._memory_bridge is not None:
            try:
                await self._memory_bridge.shutdown()
            except Exception as e:  # pragma: no cover - defensive
                logger.debug("Error shutting down memU bridge: %s", e)

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

    async def _memorize_session(
        self, session_id: str, connected_at_override: str | None = None,
    ) -> None:
        """Index un-memorized messages from a session into memU.

        Uses the more recent of ``connected_at`` and ``last_memorized_at`` as
        the lower bound so already-indexed messages are never re-sent to memU.

        ``connected_at_override`` replaces the live ``connected_at`` column as
        the fallback lower bound.  Background memorizations (scheduled via
        ``schedule_memorize``) pass the value frozen at scheduling time: by
        the time the task acquires the global lock, the live column may have
        been cleared (``mark_error``, context rotation) or reset by a newer
        client — either of which would silently skip or shrink the window of
        messages this memorization is meant to cover.
        """
        if not self._memory_bridge or not self._memory_bridge.available:
            return

        async with self._memorize_lock:
            # Session state (notably the last_memorized_at watermark) is
            # read inside the lock: queued memorizations for the same
            # session must each see the watermark advanced by the previous
            # one, or they would re-index the same window and regress it.
            session = await self.db.get_session(session_id)
            connected_at = connected_at_override or (
                session.get("connected_at") if session else None
            )
            if not connected_at:
                return

            watermark = _normalize_ts(
                (session or {}).get("last_memorized_at") or "",
            )
            connected = _normalize_ts(connected_at)

            # Pick effective lower bound: watermark wins when more recent
            if watermark and watermark >= connected:
                lower_bound = watermark
                inclusive = False  # strict >: watermark msg already indexed
            else:
                lower_bound = connected
                inclusive = True   # >=: include messages from connect time

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

    async def schedule_memorize(self, session_id: str) -> None:
        """Schedule memorization of ``session_id`` as a background task.

        Memorization serialises on a single global lock and one pass can
        take minutes (LLM-based indexing inside memU), so under load the
        queue wait reaches tens of minutes.  Latency-sensitive callers —
        cron-run teardown, error recovery, idle sweeps — must not block on
        it: the messages are already persisted in the DB, so indexing can
        happen whenever the queue drains.  If the process exits first, the
        periodic memorization sweep re-indexes anything still uncovered
        (the watermark is only advanced after a successful pass).

        The session's current ``connected_at`` is frozen here and handed to
        the task so the covered message window stays stable however the
        session mutates while the task is queued (see
        ``_memorize_session``).
        """
        if not self._memory_bridge or not self._memory_bridge.available:
            return

        session = await self.db.get_session(session_id)
        connected_at = session.get("connected_at") if session else None
        if not connected_at:
            return

        task = asyncio.create_task(
            self._memorize_session(
                session_id, connected_at_override=connected_at,
            ),
        )
        self._memorize_bg_tasks.add(task)

        def _done(t: asyncio.Task) -> None:
            self._memorize_bg_tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                logger.error(
                    "Background memorization failed for session %s: %s",
                    session_id, t.exception(),
                )

        task.add_done_callback(_done)

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
            # from intermediate list[float]→numpy conversions and JSON
            # parsing.  gc.collect can take 100ms+ — keep it off the loop.
            if self._memory_bridge:
                await asyncio.to_thread(self._memory_bridge._release_memory)

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

        system_prompt_str = build_system_prompt(
            workspace=self.config.workspace,
            session_id=session_id,
            source=source,
            timezone_name=self.config.timezone,
            recalled_memories=recalled_memories,
            skill_summaries=skill_summaries,
        )

        # Pass the system prompt as a file when it's large enough to risk
        # hitting Linux's MAX_ARG_STRLEN argv-element limit. See the comment
        # near _SYSTEM_PROMPT_INLINE_MAX at module scope. The SDK accepts
        # `SystemPromptFile` ({"type": "file", "path": ...}) and converts it
        # to `--system-prompt-file <PATH>` on the CLI.
        system_prompt: str | dict[str, Any]
        if len(system_prompt_str) > _SYSTEM_PROMPT_INLINE_MAX:
            sp_path = self._write_system_prompt_file(session_id, system_prompt_str)
            system_prompt = {"type": "file", "path": sp_path}
            logger.info(
                "Session %s: system prompt %d bytes (> %d), passing via file %s",
                session_id[:8],
                len(system_prompt_str),
                _SYSTEM_PROMPT_INLINE_MAX,
                sp_path,
            )
        else:
            system_prompt = system_prompt_str

        # Local Ollama models are reached through the proxy and speak the
        # OpenAI-translated API — Anthropic-only knobs (extended thinking,
        # effort, the context-1m beta) don't apply and may break translation,
        # so suppress them for non-Claude models.
        selected_model = model or self.config.agent.model
        is_ollama_model = (
            self.config.ollama.enabled and "claude" not in selected_model.lower()
        )

        thinking_config = (
            None if is_ollama_model
            else self._parse_thinking_config(self.config.agent.thinking, selected_model)
        )
        effort = (
            None if is_ollama_model
            else self._effective_effort(self.config.agent.effort, selected_model)
        )
        # Some subscriptions reject the context-1m beta for specific models
        # (e.g. claude-sonnet-4-6) — skip the beta header for those.
        betas = (
            ["context-1m-2025-08-07"]
            if not is_ollama_model and self.config.agent.context_1m_enabled_for(model)
            else []
        )

        # Build PreToolUse (file snapshots, image validation) +
        # PostToolUse (ScheduleWakeup capture) hooks.
        hooks = self._build_hooks(session_id)

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
            #
            # Remove the CLI's cron tools — Nerve has its own cron system,
            # so exposing CronCreate/CronList/CronDelete is redundant and
            # confusing. ``ScheduleWakeup`` stays available and is handled by
            # Nerve's wakeup harness (capture hook + cron-service sweep); the
            # CLI's own autonomous firing is suppressed via the
            # CLAUDE_CODE_DISABLE_CRON env var set in ``_build_env``.
            disallowed_tools=["CronCreate", "CronList", "CronDelete"],
            env=self._build_env(),
            cwd=str(self.config.workspace),
            mcp_servers=self._build_mcp_servers(session_id),
            # Claude Code plugins — loaded via --plugin-dir so the CLI
            # handles OAuth, credentials, and plugin lifecycle natively.
            plugins=self._claude_code_plugins,
        )


    def _system_prompt_dir(self) -> "os.PathLike[str]":
        """Directory where oversized system prompts are spilled to disk.

        Lives under the workspace's `.nerve/cache/system_prompts/` so it's
        per-workspace and easy to inspect / clean.
        """
        from pathlib import Path
        d = Path(self.config.workspace) / ".nerve" / "cache" / "system_prompts"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _write_system_prompt_file(self, session_id: str, content: str) -> str:
        """Write the system prompt to disk and return its absolute path.

        Uses a deterministic filename so a session that reconnects (resume)
        gets the same prompt without re-writing. Best-effort cleanup of stale
        files happens lazily on each write — anything older than 7 days is
        removed.
        """
        import time
        from pathlib import Path

        dir_path = Path(self._system_prompt_dir())

        # Lazy GC: drop files older than 7 days
        cutoff = time.time() - 7 * 24 * 3600
        try:
            for old in dir_path.iterdir():
                try:
                    if old.is_file() and old.stat().st_mtime < cutoff:
                        old.unlink()
                except OSError:
                    pass
        except OSError:
            pass

        safe_id = re.sub(r"[^A-Za-z0-9_.-]", "_", session_id)[:120]
        path = dir_path / f"{safe_id}.md"
        path.write_text(content, encoding="utf-8")
        return str(path)

    def _build_env(self) -> dict[str, str]:
        """Build environment variables for the SDK subprocess."""
        env: dict[str, str] = {}
        # Disable the CLI's built-in cron/wakeup scheduler. It fires
        # autonomously inside the subprocess, but Nerve only reads the SDK
        # stream during an active run() — so a fired turn lands in an unread
        # buffer and then desyncs the next real turn. Nerve owns wakeup
        # timing instead: a PostToolUse hook records each ScheduleWakeup and
        # the cron service fires it via run(..., source="wakeup"). The tool
        # itself stays available (this flag only gates the firing hook).
        env["CLAUDE_CODE_DISABLE_CRON"] = "1"
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
        # Construct a fresh ``ToolContext`` per session so every tool
        # handler sees the correct session_id and the live collaborator
        # references. The notification_service may still be ``None``
        # here if a session starts before gateway startup wires it; the
        # tools themselves degrade gracefully in that case.
        tool_ctx = ToolContext(
            session_id=session_id,
            workspace=self.config.workspace,
            db=self.db,
            memory_bridge=self._memory_bridge,
            xmemory_bridge=self._xmemory_bridge,
            config=self.config,
            skill_manager=self._skill_manager,
            engine=self,
            notification_service=self.notification_service,
        )
        include_hoa = bool(self.config.houseofagents.enabled)
        servers: dict[str, Any] = {
            "nerve": build_session_mcp_server(
                self.registry, tool_ctx, include_hoa=include_hoa,
            ),
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

    def _build_hooks(self, session_id: str) -> dict:
        """Build SDK hooks for this session.

        PreToolUse: file snapshots (Edit/Write/NotebookEdit) and image
        validation (Read). PostToolUse: ScheduleWakeup capture, which
        records the requested wakeup so the cron-service sweep can fire it
        through ``engine.run(..., source="wakeup")`` (the CLI's own
        autonomous firing is suppressed — see ``_build_env``).
        """
        from nerve.agent.interactive import INTERACTIVE_TOOLS, _read_file_safe

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

        async def _capture_wakeup_hook(hook_input, tool_use_id, context):
            """PostToolUse hook: record a ScheduleWakeup so Nerve can fire it.

            The CLI's own scheduler is disabled (CLAUDE_CODE_DISABLE_CRON),
            so the tool just records the request and returns. We persist it
            here and the cron-service sweep re-injects the prompt at the
            scheduled time via ``engine.run(..., source="wakeup")``.
            """
            try:
                await self._record_wakeup(
                    self.db, session_id, hook_input.get("tool_input", {}) or {},
                )
            except Exception as e:
                logger.warning(
                    "Failed to record wakeup for session %s: %s", session_id, e,
                )
            return {"hookSpecificOutput": {"hookEventName": "PostToolUse"}}

        async def _grant_permission_hook(hook_input, tool_use_id, context):
            """PreToolUse hook: pre-approve non-interactive tools.

            Background sub-agents (the Agent tool with run_in_background) run
            detached and non-blocking, so the CLI never surfaces an approval
            prompt for their nested tool calls — the ``can_use_tool`` callback
            is never invoked for them and the CLI denies their Write/Edit/Bash
            by default. A PreToolUse hook, however, DOES fire for those nested
            calls (it is a programmatic callback, not a user-facing prompt), so
            returning ``permissionDecision: "allow"`` here grants the same
            auto-approval foreground agents already get via ``can_use_tool``.

            Interactive tools and Read are left untouched: interactive tools
            defer to ``can_use_tool`` (pause / inject answers / deny), and Read
            defers to the image validator above plus the CLI's read-only
            auto-allow. This keeps the web pause-for-input flow intact while
            giving background sub-agents permission parity with the foreground.
            """
            tool_name = hook_input.get("tool_name", "")
            if tool_name in INTERACTIVE_TOOLS or tool_name == "Read":
                return {"hookSpecificOutput": {"hookEventName": "PreToolUse"}}
            return {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "allow",
                    "permissionDecisionReason": (
                        "nerve: auto-approved (background-agent permission parity)"
                    ),
                }
            }

        pre_tool_use = [
            HookMatcher(
                matcher="Edit|Write|NotebookEdit",
                hooks=[_snapshot_hook],
            ),
            HookMatcher(
                matcher="Read",
                hooks=[_validate_image_hook],
            ),
        ]
        # Catch-all permission grant so background sub-agents (whose nested
        # tool calls never reach can_use_tool) inherit foreground's tool
        # permissions. Registered last so the snapshot/validator hooks still
        # run for their tools; a deny from the validator wins over this allow.
        if self.config.agent.background_agent_permissions:
            pre_tool_use.append(
                HookMatcher(matcher=None, hooks=[_grant_permission_hook])
            )

        return {
            "PreToolUse": pre_tool_use,
            "PostToolUse": [
                HookMatcher(
                    matcher="ScheduleWakeup",
                    hooks=[_capture_wakeup_hook],
                ),
            ],
        }

    # Min/max delay the CLI's ScheduleWakeup enforces (clamped to [60, 3600]).
    _WAKEUP_MIN_DELAY = 60
    _WAKEUP_MAX_DELAY = 3600

    @classmethod
    def _wakeup_fire_at(cls, delay_seconds: Any) -> str:
        """Compute a UTC ISO fire time from a ScheduleWakeup ``delaySeconds``.

        Mirrors the CLI's clamping: non-finite or out-of-range values are
        coerced into ``[60, 3600]`` seconds from now.
        """
        try:
            delay = float(delay_seconds)
        except (TypeError, ValueError):
            delay = float(cls._WAKEUP_MIN_DELAY)
        if delay != delay:  # NaN
            delay = float(cls._WAKEUP_MIN_DELAY)
        elif delay == float("inf"):
            delay = float(cls._WAKEUP_MAX_DELAY)
        elif delay == float("-inf"):
            delay = float(cls._WAKEUP_MIN_DELAY)
        delay = max(cls._WAKEUP_MIN_DELAY, min(cls._WAKEUP_MAX_DELAY, round(delay)))
        fire_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
        return fire_at.isoformat()

    @classmethod
    async def _record_wakeup(
        cls, db: Any, session_id: str, tool_input: dict,
    ) -> int | None:
        """Persist a ScheduleWakeup request from its tool input.

        Returns the new wakeup id, or ``None`` when there's no prompt to
        re-inject (in which case nothing is scheduled).
        """
        prompt = str(tool_input.get("prompt", "")).strip()
        if not prompt:
            return None
        reason = str(tool_input.get("reason", "") or "")
        fire_at = cls._wakeup_fire_at(tool_input.get("delaySeconds"))
        wakeup_id = await db.add_wakeup(
            session_id, prompt=prompt, fire_at=fire_at, reason=reason,
        )
        logger.info(
            "Recorded wakeup %s for session %s at %s",
            wakeup_id, session_id[:8], fire_at,
        )
        return wakeup_id

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

    # Effort levels accepted per Claude model — substring-matched against the
    # full model name so dated aliases (e.g. "claude-opus-4-8-20260528") resolve.
    # Ordered most-specific to least-specific; first match wins. Mirrors the
    # pattern used by MODEL_PRICING in nerve/db/usage.py.
    _MODEL_EFFORT_LEVELS: dict[str, tuple[str, ...]] = {
        "fable-5":    ("low", "medium", "high", "xhigh", "max"),
        "opus-4-8":   ("low", "medium", "high", "xhigh", "max"),
        "opus-4-7":   ("low", "medium", "high", "xhigh", "max"),
        "opus-4-6":   ("low", "medium", "high", "max"),
        "sonnet-4-6": ("low", "medium", "high"),
    }
    _EFFORT_RANK: tuple[str, ...] = ("low", "medium", "high", "xhigh", "max")

    @staticmethod
    def _effective_effort(value: str, model: str | None = None) -> str | None:
        """Return ``value`` capped to the highest effort level ``model`` supports."""
        if value not in AgentEngine._EFFORT_RANK:
            return None
        allowed: tuple[str, ...] | None = None
        if model:
            m = model.lower()
            for key, levels in AgentEngine._MODEL_EFFORT_LEVELS.items():
                if key in m:
                    allowed = levels
                    break
        if not allowed or value in allowed:
            return value
        requested_rank = AgentEngine._EFFORT_RANK.index(value)
        for level in reversed(AgentEngine._EFFORT_RANK[: requested_rank + 1]):
            if level in allowed:
                logger.debug(
                    "Capped effort %r to %r for model %r (model caps at %r)",
                    value, level, model, allowed[-1],
                )
                return level
        return None

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

    def _sdk_resume_file_exists(self, sdk_session_id: str) -> bool:
        """Check whether Claude Code still has the conversation .jsonl
        for the given SDK session ID on this filesystem.

        The CLI stores history at::

            ~/.claude/projects/<encoded-cwd>/<sdk_session_id>.jsonl

        where <encoded-cwd> is the absolute cwd path with every '/'
        replaced by '-'.  The CLI resolves the cwd symlink before
        encoding, so when the workspace is itself a symlink (e.g. the
        Docker deployment's /root/nerve-workspace -> /Users/.../
        nerve-workspace) the history lives under the *realpath*-encoded
        directory, not the symlink-encoded one.  Check the realpath
        first and fall back to the unresolved path for non-symlinked
        layouts.

        If the file is gone (typically because the container's
        /root/.claude was not bind-mounted and got wiped on restart),
        passing --resume to the CLI fails with exit 1.

        Best-effort check: any unexpected error returns True so we still
        attempt the resume and let the CLI surface the real error,
        rather than masking unrelated bugs.
        """
        try:
            projects = os.path.expanduser("~/.claude/projects")
            workspace = str(self.config.workspace)
            bases = [os.path.realpath(workspace)]
            if workspace not in bases:
                bases.append(workspace)
            for base in bases:
                encoded = base.replace("/", "-")
                jsonl = (
                    projects + "/" + encoded
                    + "/" + sdk_session_id + ".jsonl"
                )
                if os.path.isfile(jsonl):
                    return True
            return False
        except Exception as e:
            logger.debug(
                "Could not stat resume jsonl for %s: %s, assuming present",
                sdk_session_id[:12], e,
            )
            return True

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
            requested_model = model or self.config.agent.model
            if client is not None:
                bound_model = self._session_models.get(session_id)
                # Health check: verify the underlying CLI process is still alive
                if self._is_client_dead(client):
                    logger.warning(
                        "Client process for session %s is dead, recreating",
                        session_id,
                    )
                    self._stop_idle_watcher(session_id)
                    self.sessions.remove_client(session_id)
                    unregister_handler(session_id)
                    await self._safe_disconnect(client)
                    client = None
                elif bound_model is not None and bound_model != requested_model:
                    # Model switched mid-session (e.g. the composer's picker
                    # moved from the Anthropic default to a local Ollama
                    # model). The CLI binds its model at connect time, so
                    # tear the client down and recreate it below.
                    logger.info(
                        "Session %s model changed (%s → %s), recreating client",
                        session_id, bound_model, requested_model,
                    )
                    self._stop_idle_watcher(session_id)
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

            # Defensive: verify the resume target's conversation .jsonl
            # actually exists before passing it to the CLI.  Claude Code
            # stores conversation history in /root/.claude/projects/
            # <encoded-cwd>/<sdk_session_id>.jsonl.  If that directory is
            # not bind-mounted from the host, a container restart wipes
            # the .jsonl files but the Nerve DB still holds the stale
            # sdk_session_id; the CLI dies with "No conversation
            # found with session ID" exit 1.
            #
            # When the file is missing, clear the stale id and start a
            # fresh conversation rather than crashing the turn.  Forks
            # are exempt: the source session's context lives in the
            # source's row, and a fresh fork has nothing to recover to.
            if sdk_resume_id and not fork_from:
                if not self._sdk_resume_file_exists(sdk_resume_id):
                    logger.warning(
                        "Session %s resume target %s.jsonl is missing; "
                        "starting a fresh CLI conversation.  This usually "
                        "means /root/.claude was not persisted across a "
                        "container restart.",
                        session_id, sdk_resume_id[:12],
                    )
                    await self.db.update_session_fields(
                        session_id, {"sdk_session_id": None},
                    )
                    sdk_resume_id = None

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
            # Watch the SDK stream between runs so autonomous CLI turns
            # (background task completions, Monitor events) stream to the
            # UI instead of buffering invisibly.
            self._start_idle_watcher(session_id, client, source)

            # Record connected_at and the resolved model
            resolved_model = options.model
            self._session_models[session_id] = resolved_model
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
        background_memorize: bool = False,
    ) -> None:
        """Disconnect and remove a client.

        Args:
            clear_resume: If True, clear sdk_session_id (e.g., on error).
                         If False, keep it for future resume (e.g., on stop).
            background_memorize: If True, schedule memorization as a
                background task instead of awaiting it inline.
                Memorization queues on a global lock, so awaiting it here
                blocks the caller for the whole queue wait — for cron runs
                that kept the run log "running" (and APScheduler skipping
                subsequent fires) long after the agent turn had finished.
        """
        self._stop_idle_watcher(session_id)
        if background_memorize:
            await self.schedule_memorize(session_id)
        else:
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
    #  Shared per-message processing (user runs + autonomous turns)        #
    # ------------------------------------------------------------------ #

    async def _process_sdk_message(
        self, session_id: str, message: Any, st: _TurnState,
    ) -> bool:
        """Process one SDK stream message: broadcast to the UI and
        accumulate into ``st`` for DB persistence.

        Shared by ``_run_inner`` (user-initiated turns) and
        ``_drain_pending_messages`` (autonomous CLI turns) so both paths
        produce identical events and records.

        Returns True when the message is a ResultMessage (turn complete).
        """
        # Early-capture sdk_session_id from the first message that carries
        # it so it survives /stop cancellation (ResultMessage — the normal
        # source — never arrives when the turn is interrupted).
        if not st.sdk_session_id:
            msg_sid = getattr(message, "session_id", None)
            if msg_sid:
                st.sdk_session_id = msg_sid

        if isinstance(message, AssistantMessage):
            st.got_content = True
            # Capture model from assistant message (more reliable than config)
            msg_model = getattr(message, "model", None)
            if msg_model:
                st.last_model = msg_model
            # Extract parent_tool_use_id — set when this message comes from
            # a sub-agent (Task/Agent) rather than the main agent
            parent_id = getattr(message, "parent_tool_use_id", None)

            for block in message.content:
                if isinstance(block, TextBlock):
                    st.full_response_text += block.text
                    # Track ordered blocks for DB persistence
                    if st.ordered_blocks and st.ordered_blocks[-1].get("type") == "text":
                        st.ordered_blocks[-1]["content"] += block.text
                    else:
                        st.ordered_blocks.append({"type": "text", "content": block.text})
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
                        # display="omitted", or simple queries on low
                        # effort). Nothing visible to render — never fall
                        # back to str(block) as that leaks the
                        # ThinkingBlock(...) repr into the UI.
                        continue
                    st.thinking_text += thinking
                    # Track ordered blocks for DB persistence
                    if st.ordered_blocks and st.ordered_blocks[-1].get("type") == "thinking":
                        st.ordered_blocks[-1]["content"] += thinking
                    else:
                        st.ordered_blocks.append({"type": "thinking", "content": thinking})
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
                    # Track sub-agent lifecycle.  Claude Code 2.1.x renamed
                    # the subagent-spawning tool from ``Task`` → ``Agent``
                    # (and introduced separate ``TaskCreate``/``TaskUpdate``
                    # /etc. tools for in-session todo tracking).  Match both
                    # names so old session history still opens panels on
                    # replay.
                    if tool_name in ("Task", "Agent") and tool_use_id:
                        st.active_subagents[tool_use_id] = asyncio.get_event_loop().time()
                        await broadcaster.broadcast_subagent_start(
                            session_id,
                            tool_use_id=tool_use_id,
                            subagent_type=str(tool_input.get("subagent_type", tool_input.get("model", "agent"))),
                            description=str(tool_input.get("description", "")),
                            model=str(tool_input.get("model", "")) or None,
                        )
                    # Track dynamic workflows.  A ``Workflow`` tool call spawns
                    # a background runtime; later task_* system messages carry
                    # its progress tree keyed by this tool_use_id.  Register it
                    # now so _handle_system_message can recognize those events
                    # even before the first ``workflow_progress`` payload.
                    if tool_name == "Workflow" and tool_use_id:
                        self._workflows.setdefault(session_id, {})[tool_use_id] = {
                            "name": self._derive_workflow_name(tool_input),
                            "snapshot": None,
                        }
                    st.tool_calls_log.append({
                        "tool": tool_name,
                        "input": tool_input,
                        "tool_use_id": tool_use_id,
                    })
                    st.ordered_blocks.append({
                        "type": "tool_call",
                        "tool": tool_name,
                        "input": tool_input,
                        "tool_use_id": tool_use_id,
                    })

                elif isinstance(block, ToolResultBlock):
                    await self._process_tool_result(
                        block, session_id, parent_id,
                        st.tool_results_map, st.ordered_blocks,
                        st.tool_calls_log, st.active_subagents,
                    )

        elif isinstance(message, UserMessage):
            parent_id = getattr(message, "parent_tool_use_id", None)
            content = getattr(message, "content", [])
            if isinstance(content, list):
                for block in content:
                    if isinstance(block, ToolResultBlock):
                        await self._process_tool_result(
                            block, session_id, parent_id,
                            st.tool_results_map, st.ordered_blocks,
                            st.tool_calls_log, st.active_subagents,
                        )

        elif isinstance(message, SystemMessage):
            # Task lifecycle events (task_started/task_updated/
            # task_notification) drive the background-task chips in the UI.
            # Other subtypes (init, status, ...) are informational only.
            await self._handle_system_message(session_id, message)

        elif isinstance(message, ResultMessage):
            if message.usage:
                st.last_usage = message.usage
            st.sdk_session_id = message.session_id
            st.result_meta = {
                "total_cost_usd": getattr(message, "total_cost_usd", None),
                "duration_ms": getattr(message, "duration_ms", None),
                "duration_api_ms": getattr(message, "duration_api_ms", None),
                "num_turns": getattr(message, "num_turns", None),
            }
            return True

        return False

    # CLI task statuses that mean "no longer running".
    _BG_TERMINAL_STATUSES = frozenset({"completed", "failed", "stopped", "killed"})

    async def _handle_system_message(
        self, session_id: str, message: Any,
    ) -> None:
        """Track CLI background-task lifecycle events and update the UI.

        The CLI emits ``system`` messages for background work (Bash/Agent
        ``run_in_background``, Monitor watches):

        - ``task_started``  — task spawned (description, task_type)
        - ``task_progress`` — periodic usage updates
        - ``task_updated``  — status patches
        - ``task_notification`` — task settled (completed/failed/stopped)

        These replace the old regex-based output-file watcher: the chips in
        the UI now reflect what the CLI actually tracks.
        """
        subtype = getattr(message, "subtype", "") or ""
        if subtype not in (
            "task_started", "task_progress", "task_updated", "task_notification",
        ):
            return

        data = getattr(message, "data", None) or {}
        task_id = data.get("task_id") or getattr(message, "task_id", None)
        if not task_id:
            return

        registry = self._bg_task_registry.setdefault(session_id, {})
        entry = registry.get(task_id)
        if entry is None:
            entry = {
                "task_id": task_id, "label": "", "tool": "Bash",
                "status": "running",
            }
            registry[task_id] = entry

        changed = True
        if subtype == "task_started":
            entry["label"] = (
                data.get("description")
                or getattr(message, "description", "")
                or entry["label"] or task_id
            )
            task_type = str(data.get("task_type") or "")
            entry["tool"] = "Agent" if "agent" in task_type else "Bash"
            entry["status"] = "running"
        elif subtype == "task_progress":
            # Only useful for backfilling a label if task_started was missed.
            desc = data.get("description") or getattr(message, "description", "")
            if desc and not entry["label"]:
                entry["label"] = desc
            else:
                changed = False
        elif subtype == "task_updated":
            patch = data.get("patch") or {}
            status = str(patch.get("status") or "")
            if status in self._BG_TERMINAL_STATUSES:
                entry["status"] = "done" if status in ("completed", "stopped") else "failed"
            else:
                changed = False
        elif subtype == "task_notification":
            status = str(data.get("status") or getattr(message, "status", "") or "")
            entry["status"] = (
                "done" if status in ("completed", "stopped", "") else "failed"
            )
            if not entry["label"]:
                entry["label"] = data.get("summary") or task_id

        # Dynamic-workflow progress. A workflow task is recognized either by
        # its tool_use_id (captured when the ``Workflow`` tool streamed) or by
        # the presence of a ``workflow_progress`` tree on the message. We emit
        # a dedicated event so the UI can render a live phase/agent panel —
        # independent of the coarse background-task chip above.
        tool_use_id = data.get("tool_use_id") or getattr(message, "tool_use_id", None)
        wf_reg = self._workflows.get(session_id) or {}
        wp = data.get("workflow_progress")
        task_type = str(data.get("task_type") or "")
        is_workflow = bool(tool_use_id) and (
            tool_use_id in wf_reg
            or (isinstance(wp, list) and len(wp) > 0)
            or "workflow" in task_type
        )
        if is_workflow:
            entry["tool"] = "Workflow"
            # The CLI reports the workflow name on task_started — authoritative
            # (and better than the tool-input guess for inline scripts).
            wf_name = data.get("workflow_name")
            if wf_name:
                self._workflows.setdefault(session_id, {}).setdefault(
                    tool_use_id, {"name": "Workflow", "snapshot": None},
                )["name"] = str(wf_name)
            await self._emit_workflow_progress(
                session_id, tool_use_id, subtype, data, message, wp,
            )

        if changed:
            await broadcaster.broadcast(session_id, {
                "type": "background_tasks_update",
                "session_id": session_id,
                "tasks": list(registry.values()),
            })

    async def _emit_workflow_progress(
        self,
        session_id: str,
        tool_use_id: str,
        subtype: str,
        data: dict,
        message: Any,
        wp: Any,
    ) -> None:
        """Build, cache, broadcast (and on terminal, persist) a workflow
        progress snapshot for the ``Workflow`` call ``tool_use_id``."""
        reg = self._workflows.setdefault(session_id, {})
        cached = reg.setdefault(tool_use_id, {"name": "Workflow", "snapshot": None})

        # task_progress carries the full tree; task_notification omits it, so
        # fall back to the last cached snapshot to settle the panel.
        if isinstance(wp, list) and wp:
            snapshot = self._build_workflow_snapshot(wp)
        else:
            prev = cached.get("snapshot") or {}
            snapshot = {
                "phases": prev.get("phases", []),
                "agents": prev.get("agents", []),
                "totalTokens": prev.get("totalTokens", 0),
                "totalToolCalls": prev.get("totalToolCalls", 0),
                "agentCount": prev.get("agentCount", 0),
            }

        status = self._workflow_status(subtype, data, message)
        snapshot["name"] = cached.get("name") or "Workflow"
        snapshot["status"] = status
        summary = (
            data.get("summary")
            or data.get("description")
            or getattr(message, "summary", "")
            or getattr(message, "description", "")
        )
        if summary:
            snapshot["summary"] = str(summary)[:2000]

        cached["snapshot"] = snapshot
        await broadcaster.broadcast_workflow_progress(session_id, tool_use_id, snapshot)

        if status in ("completed", "failed", "stopped"):
            try:
                await self.db.merge_workflow_into_call(session_id, tool_use_id, snapshot)
            except Exception as e:  # persistence is best-effort
                logger.debug("merge_workflow_into_call failed for %s: %s", tool_use_id, e)

    @staticmethod
    def _workflow_status(subtype: str, data: dict, message: Any) -> str:
        """Map a task_* system message to a workflow status string
        (running / completed / failed / stopped)."""
        if subtype in ("task_started", "task_progress"):
            return "running"
        if subtype == "task_updated":
            patch = data.get("patch") or {}
            s = str(patch.get("status") or "")
            if s == "killed":
                return "stopped"
            return s or "running"
        if subtype == "task_notification":
            return str(
                data.get("status") or getattr(message, "status", "") or "completed"
            )
        return "running"

    @staticmethod
    def _derive_workflow_name(tool_input: Any) -> str:
        """Best-effort workflow name: the ``name`` arg for a named workflow,
        else ``meta.name`` parsed from an inline script, else "Workflow"."""
        if not isinstance(tool_input, dict):
            return "Workflow"
        name = tool_input.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
        script = tool_input.get("script")
        if isinstance(script, str):
            m = re.search(r"name\s*:\s*['\"]([^'\"]+)['\"]", script)
            if m:
                return m.group(1)
        return "Workflow"

    @staticmethod
    def _fold_workflow_snapshots(
        ordered_blocks: list | None, wf_reg: dict | None,
    ) -> None:
        """Attach cached workflow snapshots onto their ``Workflow`` tool_call
        blocks (in place), so a settled-within-turn workflow persists its tree."""
        if not wf_reg or not ordered_blocks:
            return
        for ob in ordered_blocks:
            if not isinstance(ob, dict) or ob.get("type") != "tool_call":
                continue
            snap = (wf_reg.get(ob.get("tool_use_id")) or {}).get("snapshot")
            if snap:
                ob["workflow"] = snap

    @staticmethod
    def _build_workflow_snapshot(wp: list) -> dict:
        """Normalize the CLI's flat ``workflow_progress`` list into a
        {phases, agents, totals} snapshot for the UI."""
        phases: list[dict] = []
        agents: list[dict] = []
        for e in wp:
            if not isinstance(e, dict):
                continue
            etype = e.get("type")
            if etype == "workflow_phase":
                phases.append({"index": e.get("index"), "title": e.get("title")})
            elif etype == "workflow_agent":
                summary = e.get("lastToolSummary")
                agents.append({
                    "label": e.get("label"),
                    "phaseIndex": e.get("phaseIndex"),
                    "phaseTitle": e.get("phaseTitle"),
                    "state": e.get("state"),
                    "model": e.get("model"),
                    "tokens": e.get("tokens"),
                    "toolCalls": e.get("toolCalls"),
                    "lastToolName": e.get("lastToolName"),
                    "lastToolSummary": str(summary)[:200] if summary else None,
                    "durationMs": e.get("durationMs"),
                })
        total_tokens = sum(int(a.get("tokens") or 0) for a in agents)
        total_tool_calls = sum(int(a.get("toolCalls") or 0) for a in agents)
        return {
            "phases": phases,
            "agents": agents,
            "totalTokens": total_tokens,
            "totalToolCalls": total_tool_calls,
            "agentCount": len(agents),
        }

    def _prune_bg_tasks(self, session_id: str) -> None:
        """Drop settled background tasks from the registry.

        Called at the start of a new user turn so stale "done" chips don't
        accumulate forever. Running tasks are kept.
        """
        registry = self._bg_task_registry.get(session_id)
        if registry:
            for tid in [t for t, e in registry.items() if e.get("status") != "running"]:
                del registry[tid]
            if not registry:
                self._bg_task_registry.pop(session_id, None)

        # Drop settled workflows too (terminal snapshot already broadcast +
        # persisted); keep running ones so late progress still maps back.
        wf_reg = self._workflows.get(session_id)
        if wf_reg:
            terminal = {"completed", "failed", "stopped"}
            for tuid in [
                t for t, e in wf_reg.items()
                if (e.get("snapshot") or {}).get("status") in terminal
            ]:
                del wf_reg[tuid]
            if not wf_reg:
                self._workflows.pop(session_id, None)

    async def _finalize_turn(
        self, session_id: str, st: _TurnState, channel: str | None,
    ) -> None:
        """Persist a completed turn and emit the terminal ``done`` event.

        Shared by user runs and autonomous turns: stores the assistant
        message (with interleaved blocks), persists the SDK session id,
        records usage/cost, broadcasts ``done``, and touches the idle
        timer.
        """
        # Merge tool results into tool_calls_log
        self._merge_tool_results(st.tool_calls_log, st.tool_results_map)

        # Fold the latest dynamic-workflow snapshot onto its ``Workflow`` block
        # so the panel reconstructs after reload. This covers workflows that
        # settle *within* the launching turn — before the message row exists,
        # so the out-of-band merge_workflow_into_call has nothing to patch.
        # Longer workflows that settle after finalize are handled by that merge.
        self._fold_workflow_snapshots(st.ordered_blocks, self._workflows.get(session_id))

        # Store assistant message in DB
        await self.sessions.add_message(
            session_id, "assistant", st.full_response_text,
            channel=channel,
            thinking=st.thinking_text if st.thinking_text else None,
            blocks=st.ordered_blocks if st.ordered_blocks else None,
        )

        # Persist SDK session ID and update status
        if st.sdk_session_id:
            await self.sessions.mark_active(
                session_id,
                sdk_session_id=st.sdk_session_id,
                connected_at=await self.get_client_connected_at_async(session_id),
            )

        # Persist usage for context bar on session switch
        max_context = (
            1_048_576
            if self.config.agent.context_1m_enabled_for(st.last_model)
            else 200_000
        )
        num_turns = (st.result_meta or {}).get("num_turns") or 1
        if st.last_usage:
            usage_data = {
                **st.last_usage,
                "max_context_tokens": max_context,
                "num_turns": num_turns,
            }
            session_record = await self.db.get_session(session_id)
            meta = json.loads(session_record.get("metadata") or "{}") if session_record else {}
            meta["last_usage"] = usage_data

            # Extract server_tool_use counts
            server_tool = st.last_usage.get("server_tool_use") or {}
            web_search = server_tool.get("web_search_requests", 0)
            web_fetch = server_tool.get("web_fetch_requests", 0)

            # Calculate per-turn cost.
            # NOTE: The SDK's total_cost_usd is *cumulative* across the
            # entire SDK session, NOT per-invocation.  We track the last
            # known cumulative value in session metadata so we can compute
            # the delta for this turn.
            from nerve.db.usage import estimate_turn_cost, extract_cache_ttl_split
            sdk_cost = (st.result_meta or {}).get("total_cost_usd")
            current_session_cost = (
                session_record.get("total_cost_usd", 0) if session_record else 0
            ) or 0

            if sdk_cost is not None:
                prev_cumulative = meta.get("_sdk_cumulative_cost", 0) or 0
                turn_cost = max(sdk_cost - prev_cumulative, 0)
                meta["_sdk_cumulative_cost"] = sdk_cost
            else:
                turn_cost = estimate_turn_cost(st.last_usage, model=st.last_model)

            # Save metadata (includes _sdk_cumulative_cost update)
            await self.db.update_session_metadata(session_id, meta)

            # The Anthropic API splits cache_creation by TTL:
            #   usage.cache_creation.ephemeral_5m_input_tokens  (1.25x base)
            #   usage.cache_creation.ephemeral_1h_input_tokens  (2.00x base)
            # Older API responses omit the split; the aggregate still
            # lives in cache_creation_input_tokens.
            cache_5m, cache_1h = extract_cache_ttl_split(st.last_usage)

            # Persist per-turn usage to session_usage table
            await self.db.record_turn_usage(
                session_id=session_id,
                input_tokens=st.last_usage.get("input_tokens", 0),
                output_tokens=st.last_usage.get("output_tokens", 0),
                cache_creation=st.last_usage.get("cache_creation_input_tokens", 0),
                cache_read=st.last_usage.get("cache_read_input_tokens", 0),
                cache_creation_5m=cache_5m,
                cache_creation_1h=cache_1h,
                max_context=max_context,
                model=st.last_model,
                cost_usd=turn_cost,
                duration_ms=(st.result_meta or {}).get("duration_ms"),
                duration_api_ms=(st.result_meta or {}).get("duration_api_ms"),
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
            usage=st.last_usage,
            max_context_tokens=max_context,
            num_turns=num_turns,
        )
        self.sessions.touch(session_id)

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
                if channel is not None:
                    self._active_channel[session_id] = channel
                # Mark the turn as in flight so the finally below can
                # detect "ended without sending done/stopped/error" and
                # ship a synthetic done.  Clearing happens automatically
                # when a terminal event is broadcast.
                broadcaster.mark_turn_open(session_id)
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
                    self._active_channel.pop(session_id, None)
                    # Backstop: if _run_inner exited without broadcasting
                    # done/stopped/error (post-stream DB exception, hung
                    # CLI cancelled by an outer mechanism, etc.), the
                    # frontend never learned the turn ended and is still
                    # showing "thinking..." even though the server has
                    # cleared is_running.  Ship a synthetic done so the
                    # streaming UI exits cleanly.
                    if broadcaster.is_turn_open(session_id):
                        logger.warning(
                            "Session %s ended without a terminal event "
                            "(done/stopped/error); sending synthetic done "
                            "so the frontend exits streaming state",
                            session_id,
                        )
                        try:
                            await broadcaster.broadcast_done(session_id)
                        except Exception as e:
                            logger.warning(
                                "Synthetic done broadcast failed for %s: %s",
                                session_id, e,
                            )
                            broadcaster.clear_turn_open(session_id)
                    broadcaster.stop_buffering(session_id)
                    # Notify all connected clients that this session stopped
                    await broadcaster.broadcast("__global__", {
                        "type": "session_running",
                        "session_id": session_id,
                        "is_running": False,
                    })

    @staticmethod
    async def _iter_response_with_timeout(
        client: Any,
        session_id: str,
        idle_timeout: float,
    ):
        """Iterate ``client.receive_response()`` with a per-message idle timeout.

        The Claude Agent SDK's ``receive_response()`` async generator can
        block indefinitely if the underlying CLI subprocess hangs (stuck
        Anthropic API request, broken stdio pipe, etc.).  Without a timeout
        the engine has no way to notice — ``is_running`` stays True, the
        per-session lock stays held, queued user messages back up forever.

        Wrapping each ``__anext__()`` await in ``asyncio.wait_for`` detects
        a hung CLI when no SDK message of any kind (assistant chunk, tool
        call, tool result, ResultMessage) arrives within ``idle_timeout``
        seconds.  The iterator is closed and ``asyncio.TimeoutError`` is
        raised so the existing CLI-crash retry path in ``_run_inner`` can
        take over.

        The timeout is per-message, not per-turn, so legitimate long tool
        calls (e.g. a Bash command with ``timeout=600000`` ms) don't trip
        it as long as they emit ``tool_use``/``tool_result`` chunks
        between waits.

        ``idle_timeout <= 0`` disables the timeout entirely (kept for
        belt-and-suspenders ops who want the old behaviour back).
        """
        response_iter = client.receive_response()
        try:
            while True:
                try:
                    if idle_timeout and idle_timeout > 0:
                        message = await asyncio.wait_for(
                            response_iter.__anext__(),
                            timeout=idle_timeout,
                        )
                    else:
                        message = await response_iter.__anext__()
                except StopAsyncIteration:
                    return
                except asyncio.TimeoutError:
                    logger.warning(
                        "CLI idle timeout (%ds) for session %s — no SDK "
                        "message received; treating CLI as hung",
                        idle_timeout, session_id,
                    )
                    raise
                yield message
        finally:
            with contextlib.suppress(Exception):
                await response_iter.aclose()

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

        # Turn accumulator — shared shape with the autonomous-turn drain.
        st = _TurnState()

        # Wakeup turns (fired by the cron-service sweep) carry a leading
        # marker block so the UI shows a "scheduled wakeup" chip. Persisted
        # in ordered_blocks (survives reload) and broadcast live below.
        if source == "wakeup":
            st.ordered_blocks.append({"type": "wakeup"})

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

            # Drain autonomous-turn messages that buffered while no run was
            # active (background task settled in the race window before the
            # idle watcher claimed it).  Without this, receive_response()
            # below would consume the stale turn and terminate on ITS
            # ResultMessage — answering this message with the previous
            # turn's output (off-by-one desync).  The short first-content
            # timeout keeps a just-started autonomous turn from delaying
            # the user's message for long; if its content arrives later it
            # interleaves into this turn's stream (still rendered) and the
            # idle watcher self-heals the remainder.
            try:
                await self._drain_pending_messages(
                    session_id, client, source, channel,
                    first_content_timeout=3.0,
                )
            except asyncio.CancelledError:
                raise
            except Exception as drain_err:
                logger.warning(
                    "Pre-query drain failed for session %s: %s",
                    session_id, drain_err,
                )
            # The drain's broadcast_done (if it processed a turn) cleared
            # the open-turn flag set by run(); re-arm it so the synthetic-
            # done backstop still covers THIS turn.
            broadcaster.mark_turn_open(session_id)

            # New user turn: settled background-task chips are stale now.
            self._prune_bg_tasks(session_id)

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
            #
            # The whole turn (query + every streamed message including tool
            # calls) is wrapped in ``lf_attrs`` so all OTEL spans emitted by
            # the SDK carry our session_id / tags. The wrap is a no-op when
            # Langfuse is disabled.
            _effective_model = model or self.config.agent.model
            _lf_tags = [f"source:{source}", f"model:{_effective_model}"]
            if channel:
                _lf_tags.append(f"channel:{channel}")
            _lf_metadata = {
                "parent_session_id": session.get("parent_session_id") if session else None,
                "fork_from": fork_from,
            }
            # Live marker so the UI shows the "scheduled wakeup" chip as the
            # turn streams (the persisted block above covers reload).
            if source == "wakeup":
                await broadcaster.broadcast_wakeup(session_id)
            with lf_attrs(
                session_id=session_id,
                tags=_lf_tags,
                metadata=_lf_metadata,
            ):
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
                        self._stop_idle_watcher(session_id)
                        self.sessions.remove_client(session_id)
                        unregister_handler(session_id)
                        await self._safe_disconnect(client)
                        client = await self._get_or_create_client(
                            session_id, source, model,
                        )
                        continue  # retry the query

                    # Read response — may raise if CLI crashes mid-stream
                    # or hangs idle for longer than cli_idle_timeout_seconds
                    # (see _iter_response_with_timeout).
                    try:
                        async for message in AgentEngine._iter_response_with_timeout(
                            client, session_id,
                            self.config.agent.cli_idle_timeout_seconds,
                        ):
                            done = await self._process_sdk_message(
                                session_id, message, st,
                            )
                            if done:
                                # receive_response() also stops after the
                                # ResultMessage; the explicit break keeps
                                # the invariant local.
                                break

                    except asyncio.CancelledError:
                        raise  # propagate to outer handler
                    except Exception as _recv_err:
                        # CLI crashed during response reading.
                        # Retry only if we haven't received any content yet
                        # (otherwise we'd produce duplicate/garbled output).
                        if st.got_content or _attempt > 0:
                            raise
                        logger.warning(
                            "CLI crashed for session %s during response "
                            "(no content yet): %s — retrying with fresh client",
                            session_id, _recv_err,
                        )
                        self._stop_idle_watcher(session_id)
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
            partial = st.full_response_text + (
                "\n\n[Stopped by user]"
                if st.full_response_text
                else "[Stopped by user]"
            )

            # --- Critical cleanup first (must succeed for resume) ----------
            # Persist sdk_session_id so the session can be resumed later.
            # For new sessions the DB still has NULL because mark_active()
            # was called before the SDK emitted any messages.
            if st.sdk_session_id:
                await self.db.update_session_fields(
                    session_id, {"sdk_session_id": st.sdk_session_id},
                )
            await self.sessions.mark_stopped(session_id)
            self._stop_idle_watcher(session_id)
            unregister_handler(session_id)
            client = self.sessions.remove_client(session_id)
            if client:
                await self._safe_disconnect(client)

            # --- Non-critical: save message, broadcast, memorize -----------
            try:
                self._merge_tool_results(st.tool_calls_log, st.tool_results_map)
                await self.sessions.add_message(
                    session_id, "assistant", partial,
                    channel=channel,
                    thinking=st.thinking_text if st.thinking_text else None,
                    blocks=st.ordered_blocks if st.ordered_blocks else None,
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
            await self.schedule_memorize(session_id)
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
            # Schedule memorization BEFORE mark_error clears connected_at —
            # the frozen bound keeps coverage intact.  Scheduled, not
            # awaited: an inline memorize would hold the session lock for
            # the whole memorize-queue wait, stalling queued user messages.
            await self.schedule_memorize(session_id)
            # Clear resume — CLI state may be corrupted after error
            self._stop_idle_watcher(session_id)
            unregister_handler(session_id)
            client = self.sessions.remove_client(session_id)
            await self.sessions.mark_error(session_id, error_msg)
            if client:
                await self._safe_disconnect(client)
            st.full_response_text = error_msg

        # Persist the turn (assistant message + usage) and broadcast done.
        # Background-task continuation is handled by the CLI itself: when a
        # run_in_background task settles, the CLI runs an autonomous turn
        # which the idle stream watcher drains to the UI — no Nerve-side
        # output-file polling needed (the old regex watcher lived here).
        await self._finalize_turn(session_id, st, channel)

        return st.full_response_text

    # ------------------------------------------------------------------ #
    #  Autonomous turns — CLI activity between run() calls                 #
    # ------------------------------------------------------------------ #
    #
    # The CLI continues sessions on its own: when a background task
    # (Bash/Agent run_in_background, Monitor watch) settles, it emits
    # task_notification system messages and then runs a FULL agent turn
    # (model call + tool use + result) inside the subprocess.  Nothing
    # reads the SDK stream between run() calls, so historically those
    # turns piled up invisibly in the SDK's in-memory buffer (capacity
    # 100 — beyond that the SDK reader stalls and the control protocol
    # wedges with it) and the buffered ResultMessage then terminated the
    # NEXT receive_response() immediately, answering the next user
    # message with the previous turn's output (off-by-one desync).
    #
    # The idle stream watcher fixes both: it probes the buffer between
    # runs and drains autonomous turns through the same processing
    # pipeline as user turns — streamed live to the UI, persisted to the
    # DB, usage recorded.

    # How often the idle watcher probes the SDK buffer (seconds).
    _IDLE_STREAM_POLL_SECONDS = 0.5

    @staticmethod
    def _sdk_message_stream(client: Any) -> Any | None:
        """Return the SDK client's internal message receive stream.

        Private-API access (``client._query._message_receive``), pinned to
        the bundled SDK version. Callers degrade gracefully (drain and
        watcher become no-ops) when the attribute shape changes.
        """
        return getattr(getattr(client, "_query", None), "_message_receive", None)

    @classmethod
    def _sdk_buffer_used(cls, client: Any) -> int:
        """Number of unread messages in the SDK client's buffer (0 on error)."""
        stream = cls._sdk_message_stream(client)
        if stream is None:
            return 0
        try:
            return int(stream.statistics().current_buffer_used)
        except Exception:
            return 0

    async def _drain_pending_messages(
        self,
        session_id: str,
        client: Any,
        source: str,
        channel: str | None,
        manage_framing: bool = False,
        first_content_timeout: float = 30.0,
    ) -> int:
        """Drain SDK messages that arrived outside an active ``run()``.

        Autonomous CLI turns are routed through the same pipeline as user
        turns: blocks broadcast live, assistant message persisted with a
        leading ``{"type": "auto"}`` marker, usage recorded, ``done``
        emitted.  Standalone task lifecycle events update the background-
        task chips without opening a turn.

        Never parks while no turn is open (only consumes what's already
        buffered), so the pre-query call inside ``run()`` cannot hang on
        an idle CLI.  The CLI emits a ``system/init`` message when it
        starts processing a turn, so an ``init`` in the buffer means
        content IS coming (the model call is in flight) — the drain opens
        the turn and waits up to ``first_content_timeout`` for the first
        content message (model latency can be several seconds).  If
        nothing arrives the empty turn is dropped without persisting and
        the watcher's next poll picks the content up instead.  Once
        content flows, the wait uses the same idle timeout as a normal
        run; on that timeout the partial turn is persisted and
        ``asyncio.TimeoutError`` propagates so the caller can apply
        hung-CLI treatment.

        Caller must hold the per-session run lock.  ``manage_framing``
        controls session-level run framing (mark_running/session_running/
        buffering): the idle watcher passes True; ``run()`` passes False
        because its own framing is already open.

        Returns the number of completed autonomous turns processed.
        """
        stream = self._sdk_message_stream(client)
        if stream is None:
            return 0

        from claude_agent_sdk._errors import MessageParseError
        from claude_agent_sdk._internal.message_parser import parse_message

        idle_timeout = self.config.agent.cli_idle_timeout_seconds
        turns = 0
        st: _TurnState | None = None
        session_framing = False

        async def _open_turn() -> None:
            nonlocal st, session_framing
            if st is not None:
                return
            st = _TurnState()
            # Leading marker block → "background continuation" chip in the
            # UI, both live (auto_turn event) and after reload (persisted).
            st.ordered_blocks.append({"type": "auto"})
            if manage_framing and not session_framing:
                session_framing = True
                if not broadcaster.is_buffering(session_id):
                    broadcaster.start_buffering(session_id)
                self.sessions.mark_running(session_id)
                await broadcaster.broadcast("__global__", {
                    "type": "session_running",
                    "session_id": session_id,
                    "is_running": True,
                })
            broadcaster.mark_turn_open(session_id)
            await broadcaster.broadcast(session_id, {
                "type": "auto_turn", "session_id": session_id,
            })

        def _turn_has_content() -> bool:
            return st is not None and (
                st.got_content
                or bool(st.full_response_text)
                or len(st.ordered_blocks) > 1  # beyond the auto marker
                or st.last_usage is not None
            )

        async def _close_turn() -> None:
            nonlocal st, turns
            if st is None:
                return
            if _turn_has_content():
                await self._finalize_turn(session_id, st, channel)
                turns += 1
            # Empty turn (init arrived but content never did) — drop it;
            # the finally backstop ships a synthetic done if framing opened.
            st = None

        try:
            while True:
                if st is None:
                    # No turn open — only consume what's already buffered.
                    try:
                        data = stream.receive_nowait()
                    except anyio.WouldBlock:
                        break
                    except (anyio.EndOfStream, anyio.ClosedResourceError):
                        break
                else:
                    # Turn in flight — the CLI is producing; park for the
                    # next message.  Before the first content message the
                    # wait is capped by first_content_timeout (init arrives
                    # seconds before the model's first output); after that
                    # it matches a normal run's idle timeout.  NOTE: a
                    # timeout cancels the parked receive, which can in
                    # theory drop one in-flight message — acceptable on
                    # both timeout paths (empty turn → watcher re-drains;
                    # hung CLI → client discarded).
                    waiting_first_content = not st.got_content
                    if waiting_first_content:
                        park_timeout: float | None = first_content_timeout
                    else:
                        park_timeout = (
                            idle_timeout
                            if idle_timeout and idle_timeout > 0
                            else None
                        )
                    try:
                        data = await asyncio.wait_for(
                            stream.receive(), timeout=park_timeout,
                        )
                    except asyncio.TimeoutError:
                        if waiting_first_content:
                            logger.info(
                                "Autonomous turn for session %s produced no "
                                "content within %.0fs — deferring to the "
                                "next drain",
                                session_id, first_content_timeout,
                            )
                            await _close_turn()  # empty — dropped
                            break
                        logger.warning(
                            "Autonomous turn idle timeout (%ss) for session %s "
                            "— persisting partial turn and flagging CLI as hung",
                            idle_timeout, session_id,
                        )
                        st.full_response_text += (
                            "\n\n[Background turn interrupted: CLI went silent]"
                            if st.full_response_text
                            else "[Background turn interrupted: CLI went silent]"
                        )
                        await _close_turn()
                        raise
                    except (anyio.EndOfStream, anyio.ClosedResourceError):
                        logger.warning(
                            "SDK stream ended mid-autonomous-turn for session %s",
                            session_id,
                        )
                        await _close_turn()
                        break

                mtype = data.get("type") if isinstance(data, dict) else None
                if mtype == "end":
                    # Reader sentinel — stream is closed.
                    await _close_turn()
                    break
                if mtype == "error":
                    logger.error(
                        "SDK stream error during autonomous drain for %s: %s",
                        session_id, data.get("error"),
                    )
                    await _close_turn()
                    break

                try:
                    message = parse_message(data)
                except MessageParseError as pe:
                    logger.warning(
                        "Unparseable SDK message during drain for %s: %s",
                        session_id, pe,
                    )
                    continue
                if message is None:
                    continue

                if isinstance(message, SystemMessage) and st is None:
                    if getattr(message, "subtype", "") == "init":
                        # The CLI emits ``init`` when it starts processing
                        # a turn — an autonomous continuation is underway;
                        # open the turn and park for its content.
                        await _open_turn()
                    else:
                        # Task lifecycle events between turns — chips only.
                        await self._handle_system_message(session_id, message)
                    continue

                if isinstance(message, (AssistantMessage, UserMessage)):
                    await _open_turn()
                elif isinstance(message, ResultMessage) and st is None:
                    # Stray result with no preceding content (e.g. a prior
                    # drain timed out mid-turn).  Consume it so it can't
                    # desync the next receive_response(); nothing to render.
                    logger.info(
                        "Consumed stray ResultMessage during drain for %s",
                        session_id,
                    )
                    continue

                if st is not None:
                    turn_done = await self._process_sdk_message(
                        session_id, message, st,
                    )
                    if turn_done:
                        await _close_turn()

        except asyncio.CancelledError:
            # /stop (or teardown) cancelled the drain mid-turn — persist
            # what we have so the partial turn isn't lost.
            if st is not None and _turn_has_content():
                st.full_response_text += (
                    "\n\n[Stopped by user]"
                    if st.full_response_text
                    else "[Stopped by user]"
                )
                with contextlib.suppress(Exception):
                    self._merge_tool_results(st.tool_calls_log, st.tool_results_map)
                    await self.sessions.add_message(
                        session_id, "assistant", st.full_response_text,
                        channel=channel,
                        thinking=st.thinking_text or None,
                        blocks=st.ordered_blocks or None,
                    )
                    await broadcaster.broadcast(session_id, {
                        "type": "stopped", "session_id": session_id,
                    })
            raise
        finally:
            if manage_framing and session_framing:
                self.sessions.mark_not_running(session_id)
                # Backstop: ship a synthetic done if no terminal event was
                # broadcast (mirrors run()'s finally).
                if broadcaster.is_turn_open(session_id):
                    with contextlib.suppress(Exception):
                        await broadcaster.broadcast_done(session_id)
                    broadcaster.clear_turn_open(session_id)
                broadcaster.stop_buffering(session_id)
                with contextlib.suppress(Exception):
                    await broadcaster.broadcast("__global__", {
                        "type": "session_running",
                        "session_id": session_id,
                        "is_running": False,
                    })

        return turns

    def _start_idle_watcher(
        self, session_id: str, client: Any, source: str,
    ) -> None:
        """Spawn the idle stream watcher for a freshly connected client."""
        self._stop_idle_watcher(session_id)
        channel = self._active_channel.get(session_id)
        self._idle_watchers[session_id] = asyncio.create_task(
            self._idle_stream_watcher(session_id, client, source, channel),
            name=f"idle-watcher:{session_id}",
        )

    def _stop_idle_watcher(self, session_id: str) -> None:
        """Cancel a session's idle watcher (no-op from within the watcher)."""
        task = self._idle_watchers.pop(session_id, None)
        if task is None or task.done():
            return
        # The watcher may itself trigger client teardown (_discard_client);
        # never cancel the current task from within itself.
        if task is asyncio.current_task():
            return
        task.cancel()

    async def _idle_stream_watcher(
        self,
        session_id: str,
        client: Any,
        source: str,
        channel: str | None,
    ) -> None:
        """Drain autonomous CLI turns to the UI while no run() is active.

        Probes the SDK message buffer (non-destructively, via stream
        statistics) every ``_IDLE_STREAM_POLL_SECONDS``.  When messages
        appear and no run is active, takes the per-session run lock and
        drains them as autonomous turns.  Exits when the client is
        replaced, discarded, or its subprocess dies.
        """
        try:
            while True:
                await asyncio.sleep(self._IDLE_STREAM_POLL_SECONDS)

                if self.sessions.get_client(session_id) is not client:
                    return  # replaced/discarded — new client gets a new watcher
                if self.sessions.is_running(session_id):
                    continue  # run() owns the stream right now
                if self._sdk_buffer_used(client) <= 0:
                    if self._is_client_dead(client):
                        return
                    continue

                lock = self._session_locks.setdefault(session_id, asyncio.Lock())
                if lock.locked():
                    continue  # a run is starting; its pre-query drain covers this

                async with lock:
                    if self.sessions.get_client(session_id) is not client:
                        return

                    drain = asyncio.create_task(
                        self._drain_pending_messages(
                            session_id, client, source, channel,
                            manage_framing=True,
                        ),
                        name=f"auto-drain:{session_id}",
                    )
                    # Register so /stop reaches the drain: interrupt ends the
                    # CLI turn gracefully (drain finalizes on ResultMessage);
                    # the hard-cancel fallback cancels the drain task.
                    self.sessions.register_task(session_id, drain)
                    try:
                        await drain
                    except asyncio.TimeoutError:
                        # Hung CLI mid-autonomous-turn — same treatment as a
                        # hung run(): kill the client, next message recreates.
                        logger.warning(
                            "Discarding hung client for session %s "
                            "(autonomous turn stalled)", session_id,
                        )
                        await self._discard_client(
                            session_id, background_memorize=True,
                        )
                        return
                    except asyncio.CancelledError:
                        current = asyncio.current_task()
                        being_cancelled = bool(
                            current and current.cancelling()
                        )
                        if drain.cancelled() and not being_cancelled:
                            # /stop hard-cancelled the drain. Mid-turn CLI
                            # state is inconsistent — discard, mirroring
                            # run()'s cancel path.
                            await self.sessions.mark_stopped(session_id)
                            await self._discard_client(
                                session_id, background_memorize=True,
                            )
                            return
                        if not drain.done():
                            drain.cancel()
                            with contextlib.suppress(BaseException):
                                await drain
                        raise

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(
                "Idle stream watcher for session %s crashed: %s",
                session_id, e, exc_info=True,
            )

    # ------------------------------------------------------------------ #
    #  Cron / Hook runs                                                    #
    # ------------------------------------------------------------------ #

    async def _teardown_oneshot_client(
        self, session_id: str, *, keepalive_if_bg: bool = True,
    ) -> None:
        """Tear down a one-shot (cron / hook) run's SDK client.

        One-shot runs normally discard the client immediately to avoid leaking
        claude CLI subprocesses. The exception is a run that yields while a
        ``run_in_background`` task is still live: discarding here kills the
        subprocess and the idle-stream watcher that delivers the task's
        completion turn, so the agent would never resume to finish its work
        (the fix-worker "strand" failure). In that case keep the client alive —
        exactly as an interactive/web session does — and let
        ``run_idle_client_sweep`` reap it once the task settles (it already
        skips live-background-task sessions for the same reason).

        ``keepalive_if_bg`` MUST be False for runs whose ``session_id`` is
        reused across runs (``run_persistent_cron``'s stable ``cron:{job_id}``):
        parking such a client would let the NEXT scheduled run reuse the same
        client/conversation while the prior run's background task is still in
        flight, interleaving the two. Keep-alive is only safe for the
        unique-per-run isolated paths (``run_cron`` / ``run_hook``).
        """
        # Optimistic check: a task that settles between the watcher's last drain
        # and here still reads as live, parking a client whose work is actually
        # done — harmless, the next idle sweep reaps it.
        if keepalive_if_bg and self._has_live_background_tasks(session_id):
            logger.info(
                "One-shot session %s parked on a live background task — keeping "
                "client alive so its completion turn can resume the run; the "
                "idle sweep reaps it once the task settles.",
                session_id,
            )
            return
        # background_memorize: returning promptly closes the run log and frees
        # APScheduler to fire the next run — memorization queues on a global
        # lock and must not gate the run lifecycle.
        await self._discard_client(session_id, background_memorize=True)

    async def run_cron(
        self,
        job_id: str,
        prompt: str,
        model: str | None = None,
        run_id: str | None = None,
    ) -> str:
        """Run an agent turn for a cron job in an isolated session.

        The SDK client is normally discarded immediately after the run
        completes to avoid leaking claude CLI subprocesses for one-shot jobs —
        unless the run yielded with a live ``run_in_background`` task, in which
        case it is kept alive so the agent can resume when the task completes
        (see ``_teardown_oneshot_client``).
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
            await self._teardown_oneshot_client(session_id)

    async def run_persistent_cron(
        self,
        job_id: str,
        prompt: str,
        model: str | None = None,
    ) -> str:
        """Run a persistent cron job that maintains context across runs.

        Uses a stable session_id (cron:{job_id}) so the SDK resumes
        conversation context on subsequent triggers.  The client is discarded
        after each run to free the subprocess (sdk_session_id is preserved for
        the next resume). Unlike the isolated one-shot paths it does NOT keep
        the client alive for a live background task: the stable session is
        reused by the next run, which would collide with the parked task — so a
        persistent-cron background task that outlives its run is not resumed
        (use an isolated cron for long background work).
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
            # Stable session_id is reused by the next run, which would collide
            # with a parked background task — so persistent crons always discard
            # (no keep-alive). See _teardown_oneshot_client.
            await self._teardown_oneshot_client(session_id, keepalive_if_bg=False)

    async def run_hook(
        self,
        hook_name: str,
        hook_id: str,
        prompt: str,
        model: str | None = None,
    ) -> str:
        """Run an agent turn for a webhook in an isolated session.

        The SDK client is normally discarded immediately after the run
        completes — unless the run yielded with a live ``run_in_background``
        task, in which case it is kept alive so the agent can resume when the
        task completes (see ``_teardown_oneshot_client``).
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
            await self._teardown_oneshot_client(session_id)

    # ------------------------------------------------------------------ #
    #  Idle client sweep                                                   #
    # ------------------------------------------------------------------ #

    def _has_live_background_tasks(self, session_id: str) -> bool:
        """Whether *session_id* has a background task still running.

        The idle sweep consults this so it never discards a client that is
        parked on a live Bash/Agent ``run_in_background`` (or Monitor) task:
        discarding tears down the idle-stream watcher (``_idle_stream_watcher``)
        that delivers the task's completion turn, so the session would never
        wake when the task settles.
        """
        registry = self._bg_task_registry.get(session_id)
        return bool(registry) and any(
            entry.get("status") == "running" for entry in registry.values()
        )

    async def run_idle_client_sweep(self) -> int:
        """Disconnect clients that have been idle beyond the configured timeout.

        Idle clients still hold a claude CLI subprocess. Discarding them frees
        resources while preserving sdk_session_id for seamless resume later.

        Sessions parked on a live background task are skipped: discarding their
        client kills the idle-stream watcher that delivers the task's
        completion turn, so the session would never wake when the task settles.

        Returns count of clients disconnected.
        """
        timeout_minutes = self.config.sessions.client_idle_timeout_minutes
        if timeout_minutes <= 0:
            return 0

        idle_ids = self.sessions.get_idle_client_ids(timeout_minutes * 60)
        discarded = 0
        for sid in idle_ids:
            if self._has_live_background_tasks(sid):
                logger.info(
                    "Idle sweep: keeping session %s — background task in flight",
                    sid,
                )
                continue
            logger.info("Auto-closing idle client for session %s", sid)
            # background_memorize: free the claude subprocess now; indexing
            # follows whenever the memorize queue drains.
            await self._discard_client(sid, background_memorize=True)
            discarded += 1

        if discarded:
            logger.info(
                "Idle client sweep: disconnected %d client(s), %d still active",
                discarded,
                len(self.sessions._clients),
            )
        return discarded

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
