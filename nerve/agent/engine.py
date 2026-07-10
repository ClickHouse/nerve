"""Agent engine — backend-agnostic agent orchestration.

Orchestrates per-session agent clients (Claude Agent SDK, OpenAI Codex
app-server — see :mod:`nerve.agent.backends`) and delegates all session
state to SessionManager. The engine consumes only normalized
:mod:`nerve.agent.backends.events`; every runtime-specific type stays
inside its backend module. Sessions are resumable across server restarts
via each backend's native resume mechanism, routed by the sticky
``sessions.backend`` column (docs/plans/codex-backend.md §3).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from nerve.agent.backends import (
    AgentBackend,
    AgentClient,
    BackendDeps,
    ModelObserved,
    SessionSpec,
    SubagentStarted,
    SystemEvent,
    TextDelta,
    ThinkingDelta,
    ToolResult,
    ToolUse,
    TransportDiedError,
    TurnCompleted,
    TurnInput,
    build_backends,
)
from nerve.agent.cache_policy import resolve_cache_ttl
from nerve.agent.interactive import (
    InteractiveToolHandler,
    register_handler,
    unregister_handler,
    get_handler,
)
from nerve.agent.prompts import (
    build_system_prompt,
    current_time_str,
    set_skill_manager,
)
from nerve.agent.sessions import SessionManager, SessionStatus
from nerve.agent.streaming import broadcaster
from nerve.agent.tools import (
    ToolContext,
    ToolRegistry,
    build_default_registry,
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


_SURROGATE_RE = re.compile(r"[\ud800-\udfff]")

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


def _model_family(model: str) -> str:
    """Normalize a model identifier to a comparable family name.

    The same model family shows up under many identifiers depending on
    provider routing and release: a bare alias, a dated release id, a
    Bedrock inference-profile id, or a context-window-suffixed alias.
    Serving-model change detection compares *families*, so none of these
    synonyms may register as a change:

        example-model-2            -> example-model-2
        example-model-2-20260101   -> example-model-2
        us.provider.example-model-2-20260101-v1:0 -> example-model-2
        example-model-2[1m]        -> example-model-2
        example-model-2-latest     -> example-model-2
    """
    m = model.strip().lower()
    # Provider routing prefix: "us.anthropic.", "global.anthropic.", ...
    if "anthropic." in m:
        m = m.rsplit("anthropic.", 1)[1]
    # Context-window suffix: "...[1m]"
    m = m.split("[", 1)[0]
    # Bedrock version suffix: "-v1:0" / "-v2"
    m = re.sub(r"-v\d+(?::\d+)?$", "", m)
    # "-latest" alias suffix
    m = re.sub(r"-latest$", "", m)
    # Trailing release date: "-20260601"
    m = re.sub(r"-20\d{6}$", "", m)
    return m


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
        # Last model *observed* serving each session (from
        # AssistantMessage.model). The API may silently serve a different
        # model than requested — e.g. a capacity fallback from a frontier
        # model to the previous tier — and switch back later. Transitions
        # are surfaced as model_change blocks/events (_track_serving_model).
        # Seeded from session metadata on client creation so detection
        # survives restarts without re-firing on every resume.
        self._observed_models: dict[str, str] = {}
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

        # Agent backends (claude / codex). Constructed once; resolved per
        # session by the STICKY rule (stored sessions.backend first, then
        # config) — see _backend_for. ``_session_backends`` mirrors the
        # live client's backend for hot paths (finalize, idle watcher).
        self._backends: dict[str, AgentBackend] = build_backends(
            BackendDeps(
                config=self.config,
                db=self.db,
                registry=self.registry,
                tool_ctx_factory=self._build_tool_context,
                external_mcp_servers=lambda: self._mcp_servers_cache,
                claude_plugins=lambda: self._claude_code_plugins,
                gateway_port=self._gateway_port,
                mint_session_token=self._mint_mcp_session_token,
            )
        )
        self._session_backends: dict[str, str] = {}

    def set_notification_service(self, service: Any) -> None:
        """Install the notification service used by per-session ``ToolContext``.

        Called once during gateway startup. We accept ``Any`` to avoid
        a circular import with :mod:`nerve.notifications.service`.
        """
        self.notification_service = service

    def get_active_channel(self, session_id: str) -> str | None:
        """Return the channel name currently driving ``session_id`` (or None)."""
        return self._active_channel.get(session_id)

    # ------------------------------------------------------------------ #
    #  Backend resolution (sticky per session)                            #
    # ------------------------------------------------------------------ #

    def _build_tool_context(self, session_id: str) -> ToolContext:
        """Fresh per-session ToolContext for backend MCP servers."""
        return ToolContext(
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

    def _gateway_port(self) -> int | None:
        """Gateway port for the loopback MCP bridge (codex tool access)."""
        if not self.config.mcp_endpoint.enabled:
            return None
        return int(self.config.gateway.port)

    def _mint_mcp_session_token(self, session_id: str) -> str:
        """Session-bound bearer token for a backend-managed agent process."""
        from nerve.gateway.auth import create_mcp_session_token

        if not self.config.auth.jwt_secret:
            return ""  # dev mode — endpoint accepts unauthenticated calls
        return create_mcp_session_token(self.config.auth.jwt_secret, session_id)

    def _backend_for(self, session: dict | None, source: str) -> AgentBackend:
        """Resolve the backend for a session — STICKY on the stored column.

        1. ``sessions.backend`` set → that backend, always. Wakeup /
           internal / cron-fired turns on an existing session can never
           cross backends, no matter what the config says now (a stored
           native session id is meaningless on another runtime).
        2. New sessions: metadata ``backend_override`` (per-session A/B
           hook) → config (``agent.cron_backend`` for cron/hook sources,
           ``agent.backend`` otherwise).
        """
        stored = (session or {}).get("backend")
        name = str(stored) if stored else ""
        if not name:
            try:
                meta = json.loads((session or {}).get("metadata") or "{}")
            except (TypeError, ValueError):
                meta = {}
            override = meta.get("backend_override")
            if override:
                name = str(override).strip().lower()
        if not name:
            if source in self._CRON_EFFORT_SOURCES:
                name = self.config.agent.resolved_cron_backend
            else:
                name = self.config.agent.backend
        backend = self._backends.get(name)
        if backend is None:
            raise RuntimeError(
                f"Session requires backend {name!r} which is not available "
                f"(known: {sorted(self._backends)}). Restore the backend "
                "config or start a new session."
            )
        return backend

    def _backend_for_live_session(self, session_id: str) -> AgentBackend:
        """Backend of the session's live client (defaults to claude)."""
        name = self._session_backends.get(session_id, "claude")
        return self._backends.get(name) or self._backends["claude"]

    def _collect_skill_summaries(self) -> list[dict] | None:
        """Skill summaries for the system prompt (moved from _build_options).

        Sorted for deterministic system-prompt bytes — scan order varies
        across restarts, and a reordered skill list would silently
        invalidate every session's prompt cache after a restart.
        """
        if not self._skill_manager:
            return None
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                summaries = []
                for sid, meta in sorted(self._skill_manager._cache.items()):
                    if meta.enabled and meta.model_invocable:
                        summaries.append({
                            "id": meta.id,
                            "name": meta.name,
                            "description": meta.description,
                        })
                return summaries
            return loop.run_until_complete(
                self._skill_manager.get_enabled_summaries()
            )
        except Exception as e:
            logger.warning("Failed to get skill summaries: %s", e)
            return None

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
        """Tear a client down without letting teardown errors propagate.

        The hardened teardown logic (subprocess kill, anyio task-group
        disarm for the Claude SDK) lives in each backend's
        ``AgentClient.disconnect`` — this wrapper only guarantees the
        call can't take forever or raise into engine control flow.
        """
        try:
            await asyncio.wait_for(client.disconnect(), timeout=timeout + 5.0)
        except Exception as e:
            logger.warning("Client disconnect failed: %s", e)

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

    # Sources whose turns are sensing / triage work and use ``cron_effort``
    # instead of the interactive ``effort``. Everything else (web, telegram,
    # wakeup, ...) is treated as interactive.
    _CRON_EFFORT_SOURCES: frozenset[str] = frozenset({"cron", "hook"})

    @staticmethod
    def _base_effort_for_source(source: str, effort: str, cron_effort: str) -> str:
        """Pick the raw effort string for a turn based on its source.

        Cron and hook turns use ``cron_effort``; interactive sources keep the
        full ``effort``. The result is still passed through
        :meth:`_effective_effort` for model-cap clamping.
        """
        if source in AgentEngine._CRON_EFFORT_SOURCES:
            return cron_effort
        return effort

    # ------------------------------------------------------------------ #
    #  SDK client lifecycle                                                #
    # ------------------------------------------------------------------ #

    async def _get_or_create_client(
        self, session_id: str, source: str, model: str | None,
        fork_from: str | None = None,
    ) -> AgentClient:
        """Get an existing persistent client or create a new one.

        Backend-agnostic orchestration: resolves the session's (sticky)
        backend, validates/clears stale resume targets, freezes the
        recall priors, renders the system prompt, and hands a
        :class:`SessionSpec` to the backend. The backend owns everything
        runtime-specific (options, hooks, subprocess).
        """
        lock = self.sessions.get_lock(session_id)
        async with lock:
            client = self.sessions.get_client(session_id)

            # Session row first — backend resolution is sticky on it.
            session = await self.db.get_session(session_id)
            backend = self._backend_for(session, source)
            requested_model = model or backend.default_model(source)

            if client is not None:
                bound_model = self._session_models.get(session_id)
                # Health check: verify the underlying subprocess is alive
                if not client.is_alive():
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
                    # moved to a different model). Clients bind their model
                    # at connect time, so tear down and recreate below.
                    logger.info(
                        "Session %s model changed (%s → %s), recreating client",
                        session_id, bound_model, requested_model,
                    )
                    self._stop_idle_watcher(session_id)
                    self.sessions.remove_client(session_id)
                    unregister_handler(session_id)
                    await self._safe_disconnect(client)
                    client = None
                    # Deliberate switch — drop the observed-model baseline so
                    # the first message on the new model doesn't fire a
                    # model_change event for a change the user asked for.
                    self._observed_models.pop(session_id, None)
                else:
                    return client

            # Check for stored native session ID for resume
            sdk_resume_id = session.get("sdk_session_id") if session else None
            try:
                session_meta = json.loads(
                    (session.get("metadata") if session else None) or "{}",
                )
            except (TypeError, ValueError):
                session_meta = {}

            # Seed the serving-model baseline from the last persisted
            # observation so downgrade detection survives restarts without
            # re-firing an event on every resumed session.
            if session and session_id not in self._observed_models:
                if session_meta.get("observed_model"):
                    self._observed_models[session_id] = session_meta["observed_model"]

            # For forks, use the source session's native ID
            if fork_from and not sdk_resume_id:
                sdk_resume_id = fork_from

            # Defensive: let the backend verify the resume target is still
            # materialized (claude: the conversation .jsonl under
            # ~/.claude/projects; codex: no cheap check — create_client
            # falls back and reports resume_dropped instead).  Forks are
            # exempt: the source session's context lives in the source's
            # row, and a fresh fork has nothing to recover to.
            if sdk_resume_id and not fork_from:
                if not backend.validate_resume_target(
                    sdk_resume_id, str(self.config.workspace),
                ):
                    logger.warning(
                        "Session %s resume target %s is missing on the %s "
                        "backend; starting a fresh conversation.",
                        session_id, sdk_resume_id[:12], backend.name,
                    )
                    await self.db.update_session_fields(
                        session_id, {"sdk_session_id": None},
                    )
                    sdk_resume_id = None

            if sdk_resume_id:
                logger.info(
                    "Resuming session %s with native session %s",
                    session_id, sdk_resume_id[:12],
                )

            # Pre-recall memories for new session context. The first
            # successful recall is frozen in session metadata and reused on
            # every rebuild of the same session: byte-identical system
            # prompts across rebuilds are what let a resumed conversation
            # hit the prompt cache (see nerve/agent/cache_policy.py), and a
            # session keeping its original priors is more consistent anyway
            # — live recall stays available via the memory_recall tool.
            recalled_memories: list[str] = []
            meta_updates: dict[str, Any] = {}
            frozen_recall = session_meta.get("recalled_memories")
            if isinstance(frozen_recall, list):
                recalled_memories = [str(m) for m in frozen_recall]
            elif self._memory_bridge and self._memory_bridge.available:
                try:
                    raw = await self._memory_bridge.recall(
                        f"context for {source} session",
                        limit=8,
                    )
                    recalled_memories = [m["summary"] for m in raw]
                    meta_updates["recalled_memories"] = recalled_memories
                except Exception as e:
                    logger.warning("Pre-recall failed: %s", e)

            # Resolve the prompt-cache TTL for this client build (cadence-
            # aware; see nerve/agent/cache_policy.py). Claude-only — other
            # backends manage caching natively (capability-gated).
            cache_ttl = "5m"
            if backend.capabilities.supports_cache_ttl:
                is_claude = not (
                    self.config.ollama.enabled
                    and "claude" not in requested_model.lower()
                )
                cache_ttl = await resolve_cache_ttl(
                    self.config.agent, self.db, session_id, source,
                    requested_model,
                    session_meta=session_meta, is_claude_model=is_claude,
                )
                logger.info(
                    "Session %s: prompt-cache TTL %s (source=%s, mode=%s)",
                    session_id, cache_ttl, source,
                    session_meta.get("cache_ttl_override")
                    or self.config.agent.cache_ttl,
                )
                if session_meta.get("cache_ttl") != cache_ttl:
                    meta_updates["cache_ttl"] = cache_ttl

            if meta_updates and session:
                session_meta.update(meta_updates)
                await self.db.update_session_metadata(session_id, session_meta)

            # Determine if this is a fork
            is_fork = fork_from is not None

            # Create interactive hub for this session.
            # Non-web sessions (telegram, cron, hook) cannot handle
            # interactive pauses — auto-deny them to prevent deadlocks.
            is_interactive = source in ("web",)
            handler = InteractiveToolHandler(
                session_id=session_id,
                broadcast_fn=broadcaster.broadcast,
                snapshot_fn=self._save_file_snapshot,
                interactive_capable=is_interactive,
            )
            register_handler(session_id, handler)

            # Render the system prompt (engine-owned: identity files,
            # frozen recall, skills; the tool list respects the backend's
            # exclusions so the prompt never advertises a tool this
            # session's MCP server doesn't serve).
            system_prompt = build_system_prompt(
                workspace=self.config.workspace,
                session_id=session_id,
                source=source,
                timezone_name=self.config.timezone,
                recalled_memories=recalled_memories or None,
                skill_summaries=self._collect_skill_summaries(),
                excluded_tools=backend.excluded_tools(),
            )

            async def _record_wakeup_cb(sid: str, tool_input: dict) -> Any:
                return await self._record_wakeup(self.db, sid, tool_input)

            spec = SessionSpec(
                session_id=session_id,
                source=source,
                model=requested_model,
                effort=self._base_effort_for_source(
                    source, self.config.agent.effort,
                    self.config.agent.cron_effort,
                ),
                system_prompt=system_prompt,
                cwd=str(self.config.workspace),
                resume_native_id=sdk_resume_id,
                fork=is_fork,
                interactive=handler,
                snapshot=self._save_file_snapshot,
                record_wakeup=_record_wakeup_cb,
                cache_ttl=cache_ttl,
                max_turns=self.config.agent.max_turns,
                idle_timeout=float(self.config.agent.cli_idle_timeout_seconds),
            )

            client = await backend.create_client(spec)
            if getattr(client, "resume_dropped", False):
                # The backend had to discard the stale native id (codex
                # resume-miss recovery) — clear the persisted column AND
                # the local variable (mark_active below would otherwise
                # re-persist the stale id); the fresh id lands at turn end.
                await self.db.update_session_fields(
                    session_id, {"sdk_session_id": None},
                )
                sdk_resume_id = None
            self.sessions.set_client(session_id, client)
            self._session_backends[session_id] = backend.name

            # Stamp the sticky backend on first client build.
            if not (session or {}).get("backend"):
                await self.db.update_session_fields(
                    session_id, {"backend": backend.name},
                )

            # Watch the stream between runs so autonomous runtime turns
            # (background task completions, Monitor events) stream to the
            # UI instead of buffering invisibly. Claude-only capability.
            if backend.capabilities.supports_idle_stream:
                self._start_idle_watcher(session_id, client, source)

            # Record connected_at and the resolved model
            resolved_model = getattr(client, "model", "") or requested_model
            self._session_models[session_id] = resolved_model
            now = datetime.now(timezone.utc).isoformat()
            connected_at = session.get("connected_at") if session and sdk_resume_id else now
            await self.sessions.mark_active(
                session_id,
                sdk_session_id=sdk_resume_id,
                connected_at=connected_at,
            )
            await self.db.update_session_fields(session_id, {"model": resolved_model})

            # A brand-new runtime process starts its cumulative cost counter
            # at zero — zero the persisted baseline so the first turn's
            # delta is exact (claude-only; codex reports per-turn cost).
            if backend.capabilities.cost_is_cumulative:
                await self._reset_cost_baseline(session_id)

            logger.info(
                "Created persistent %s client for session %s%s",
                backend.name, session_id,
                " (resumed)" if sdk_resume_id and not is_fork else
                " (forked)" if is_fork else "",
            )
            return client

    async def _reset_cost_baseline(self, session_id: str) -> None:
        """Zero the persisted SDK cumulative-cost baseline for a session.

        Called right after a new CLI client is created: the fresh process
        reports ``total_cost_usd`` cumulatively from zero, so the stored
        high-water mark from the previous client must not be diffed
        against it (see compute_turn_cost in nerve.db.usage).

        Re-reads the session row so concurrent metadata writers are not
        clobbered; accounting must never break a turn, so failures are
        logged and swallowed.
        """
        try:
            session = await self.db.get_session(session_id)
            if not session:
                return
            meta = json.loads(session.get("metadata") or "{}")
            if meta.get("_sdk_cumulative_cost"):
                meta["_sdk_cumulative_cost"] = 0
                await self.db.update_session_metadata(session_id, meta)
        except Exception as e:
            logger.warning(
                "Failed to reset cost baseline for %s: %s", session_id, e,
            )

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
        event: ToolResult,
        session_id: str,
        tool_results_map: dict[str, dict],
        ordered_blocks: list[dict],
        tool_calls_log: list[dict],
        active_subagents: dict[str, float],
    ) -> None:
        """Process a normalized ToolResult event (shared by user runs and
        autonomous-turn drains)."""
        result_content = (
            event.content
            if isinstance(event.content, str)
            else json.dumps(event.content, default=str)
        )
        # Sanitize orphaned surrogates — runtimes may truncate output mid-emoji
        result_content = _sanitize_surrogates(result_content)
        tool_use_id = event.tool_use_id
        is_error = event.is_error

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
            parent_tool_use_id=event.parent_tool_use_id,
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

    async def _track_serving_model(
        self, session_id: str, model: str, st: _TurnState,
    ) -> None:
        """Detect serving-model transitions and surface them.

        The API can serve a session with a different model than the one
        configured — e.g. a capacity fallback from a frontier model to
        the previous tier — and later switch back, all without any
        explicit signal beyond ``AssistantMessage.model``. Compare each
        main-agent message's model against the last observed one (or the
        configured model for the first observation) and, when the model
        *family* changes:

        - append a ``model_change`` block to the turn (persisted with the
          message, so the transition stays visible in history),
        - broadcast a ``model_changed`` event for the live UI,
        - log it (warning when it moves away from the configured model,
          info when it returns).

        Family comparison (see ``_model_family``) keeps alias/dated/
        Bedrock spellings of the same model from registering as changes.
        """
        prev = self._observed_models.get(session_id)
        self._observed_models[session_id] = model
        configured = self._session_models.get(session_id)
        baseline = prev or configured
        if not baseline or _model_family(model) == _model_family(baseline):
            return
        downgrade = bool(
            configured and _model_family(model) != _model_family(configured),
        )
        log = logger.warning if downgrade else logger.info
        log(
            "Session %s serving model changed: %s → %s%s",
            session_id, baseline, model,
            f" (away from configured {configured})" if downgrade else "",
        )
        st.ordered_blocks.append({
            "type": "model_change",
            "from": baseline,
            "to": model,
            "downgrade": downgrade,
        })
        await broadcaster.broadcast_model_changed(
            session_id, from_model=baseline, to_model=model,
            downgrade=downgrade,
        )

    async def _process_agent_event(
        self, session_id: str, event: Any, st: _TurnState,
    ) -> bool:
        """Process one normalized agent event: broadcast to the UI and
        accumulate into ``st`` for DB persistence.

        Shared by ``_run_inner`` (user-initiated turns) and
        ``_drain_pending_messages`` (autonomous runtime turns) so both
        paths produce identical events and records. Backends translate
        their native stream into these events (see
        nerve/agent/backends/events.py).

        Returns True when the event is a TurnCompleted (turn over).
        """
        if isinstance(event, TextDelta):
            st.got_content = True
            st.full_response_text += event.text
            if st.ordered_blocks and st.ordered_blocks[-1].get("type") == "text":
                st.ordered_blocks[-1]["content"] += event.text
            else:
                st.ordered_blocks.append({"type": "text", "content": event.text})
            await broadcaster.broadcast_token(
                session_id, event.text,
                parent_tool_use_id=event.parent_tool_use_id,
            )

        elif isinstance(event, ThinkingDelta):
            st.got_content = True
            st.thinking_text += event.text
            if st.ordered_blocks and st.ordered_blocks[-1].get("type") == "thinking":
                st.ordered_blocks[-1]["content"] += event.text
            else:
                st.ordered_blocks.append({"type": "thinking", "content": event.text})
            await broadcaster.broadcast_thinking(
                session_id, event.text,
                parent_tool_use_id=event.parent_tool_use_id,
            )

        elif isinstance(event, ToolUse):
            st.got_content = True
            await broadcaster.broadcast_tool_use(
                session_id, event.name, event.input,
                tool_use_id=event.tool_use_id,
                parent_tool_use_id=event.parent_tool_use_id,
            )
            # Track dynamic workflows.  A ``Workflow`` tool call spawns
            # a background runtime; later task_* system events carry
            # its progress tree keyed by this tool_use_id.  Register it
            # now so _handle_system_event can recognize those events
            # even before the first ``workflow_progress`` payload.
            if event.name == "Workflow" and event.tool_use_id:
                self._workflows.setdefault(session_id, {})[event.tool_use_id] = {
                    "name": self._derive_workflow_name(event.input),
                    "snapshot": None,
                }
            st.tool_calls_log.append({
                "tool": event.name,
                "input": event.input,
                "tool_use_id": event.tool_use_id,
            })
            st.ordered_blocks.append({
                "type": "tool_call",
                "tool": event.name,
                "input": event.input,
                "tool_use_id": event.tool_use_id,
            })

        elif isinstance(event, SubagentStarted):
            st.active_subagents[event.tool_use_id] = asyncio.get_event_loop().time()
            await broadcaster.broadcast_subagent_start(
                session_id,
                tool_use_id=event.tool_use_id,
                subagent_type=event.subagent_type,
                description=event.description,
                model=event.model,
            )

        elif isinstance(event, ToolResult):
            await self._process_tool_result(
                event, session_id,
                st.tool_results_map, st.ordered_blocks,
                st.tool_calls_log, st.active_subagents,
            )

        elif isinstance(event, ModelObserved):
            # Main-agent serving-model observation (backends already gate
            # out sub-agent models). More reliable than config.
            st.last_model = event.model
            await self._track_serving_model(session_id, event.model, st)

        elif isinstance(event, SystemEvent):
            # Task lifecycle events (task_started/task_updated/
            # task_notification) drive the background-task chips in the UI.
            # Other subtypes (init, codex_plan, ...) are informational only.
            await self._handle_system_event(session_id, event)

        elif isinstance(event, TurnCompleted):
            if event.usage is not None:
                st.last_usage = event.usage.to_anthropic_shape()
            if event.native_session_id:
                st.sdk_session_id = event.native_session_id
            if event.model and not st.last_model:
                st.last_model = event.model
            st.result_meta = {
                "total_cost_usd": event.total_cost_usd,
                "duration_ms": event.duration_ms,
                "duration_api_ms": event.duration_api_ms,
                "num_turns": event.num_turns,
                "context_window": event.context_window,
                "status": event.status,
            }
            if event.status == "failed" and event.error:
                # Failed turns still complete: surface the error inline so
                # the conversation shows what happened (the runtime's
                # transport stays healthy — this is a model/API failure).
                note = f"⚠️ Turn failed: {event.error}"
                st.full_response_text += (
                    ("\n\n" + note) if st.full_response_text else note
                )
                st.ordered_blocks.append({"type": "text", "content": note})
                await broadcaster.broadcast_token(session_id, note)
            return True

        return False

    # CLI task statuses that mean "no longer running".
    _BG_TERMINAL_STATUSES = frozenset({"completed", "failed", "stopped", "killed"})

    async def _handle_system_event(
        self, session_id: str, event: SystemEvent,
    ) -> None:
        """Track runtime background-task lifecycle events and update the UI.

        The Claude CLI emits ``system`` messages for background work
        (Bash/Agent ``run_in_background``, Monitor watches):

        - ``task_started``  — task spawned (description, task_type)
        - ``task_progress`` — periodic usage updates
        - ``task_updated``  — status patches
        - ``task_notification`` — task settled (completed/failed/stopped)

        Backends merge any legacy top-level fields into ``event.data`` so
        this handler reads one dict.
        """
        subtype = event.subtype
        if subtype not in (
            "task_started", "task_progress", "task_updated", "task_notification",
        ):
            return

        data = event.data or {}
        task_id = data.get("task_id")
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
                data.get("description") or entry["label"] or task_id
            )
            task_type = str(data.get("task_type") or "")
            entry["tool"] = "Agent" if "agent" in task_type else "Bash"
            entry["status"] = "running"
        elif subtype == "task_progress":
            # Only useful for backfilling a label if task_started was missed.
            desc = data.get("description")
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
            status = str(data.get("status") or "")
            entry["status"] = (
                "done" if status in ("completed", "stopped", "") else "failed"
            )
            if not entry["label"]:
                entry["label"] = data.get("summary") or task_id

        # Dynamic-workflow progress. A workflow task is recognized either by
        # its tool_use_id (captured when the ``Workflow`` tool streamed) or by
        # the presence of a ``workflow_progress`` tree on the event. We emit
        # a dedicated event so the UI can render a live phase/agent panel —
        # independent of the coarse background-task chip above.
        tool_use_id = data.get("tool_use_id")
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
                session_id, tool_use_id, subtype, data, wp,
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

        status = self._workflow_status(subtype, data)
        snapshot["name"] = cached.get("name") or "Workflow"
        snapshot["status"] = status
        summary = data.get("summary") or data.get("description")
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
    def _workflow_status(subtype: str, data: dict) -> str:
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
            return str(data.get("status") or "completed")
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

        # Persist usage for context bar on session switch. Backends that
        # report the serving model's context window (codex, via
        # thread/tokenUsage/updated) override the Anthropic-derived value.
        reported_window = (st.result_meta or {}).get("context_window")
        if reported_window:
            max_context = int(reported_window)
        else:
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
            if st.last_model:
                # Baseline for serving-model change detection across
                # restarts (see _track_serving_model).
                meta["observed_model"] = st.last_model

            # Extract server_tool_use counts
            server_tool = st.last_usage.get("server_tool_use") or {}
            web_search = server_tool.get("web_search_requests", 0)
            web_fetch = server_tool.get("web_fetch_requests", 0)

            # Calculate per-turn cost. Semantics depend on the backend
            # (cost_is_cumulative capability):
            #
            # * Claude: the SDK's total_cost_usd is *cumulative* per CLI
            #   client process, NOT per-invocation.  We track the last
            #   known cumulative value in session metadata and compute the
            #   delta for this turn.  The counter resets whenever the
            #   client is recycled — compute_turn_cost detects the reset
            #   and attributes the new cumulative to this turn.
            # * Codex: the backend pre-computes THIS turn's cost from its
            #   pricing table (None when the model has no entry — recorded
            #   as $0 with tokens intact, never estimated) and the
            #   cumulative bookkeeping must stay untouched.
            from nerve.db.usage import compute_turn_cost, extract_cache_ttl_split
            sdk_cost = (st.result_meta or {}).get("total_cost_usd")
            current_session_cost = (
                session_record.get("total_cost_usd", 0) if session_record else 0
            ) or 0

            cost_is_cumulative = self._backend_for_live_session(
                session_id,
            ).capabilities.cost_is_cumulative
            if cost_is_cumulative:
                prev_cumulative = meta.get("_sdk_cumulative_cost", 0) or 0
                turn_cost, cost_source = compute_turn_cost(
                    sdk_cost, prev_cumulative, st.last_usage, model=st.last_model,
                )
                if cost_source == "sdk_reset":
                    logger.info(
                        "SDK cost counter reset for %s (%.4f < %.4f) — client "
                        "recycle; attributing new cumulative to this turn",
                        session_id, sdk_cost, prev_cumulative,
                    )
                elif cost_source == "estimate_backstop":
                    logger.warning(
                        "SDK reported no turn cost (cumulative %.4f, prev %.4f) "
                        "despite token traffic for %s — using token-based "
                        "estimate $%.4f",
                        sdk_cost, prev_cumulative, session_id, turn_cost,
                    )
                if sdk_cost is not None:
                    meta["_sdk_cumulative_cost"] = sdk_cost
            else:
                turn_cost = float(sdk_cost) if sdk_cost is not None else 0.0
                cost_source = "backend"

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

            # New user turn: settled background-task chips are stale now.
            # (moved below drain in original flow — kept semantically here)
            # Trailing wall-clock reminder. Precise time deliberately does
            # NOT live in the system prompt (only the date does): a
            # per-build timestamp there changes the prompt bytes on every
            # client rebuild, invalidating the prompt cache for the entire
            # conversation replay (see nerve/agent/cache_policy.py). As a
            # message-tail reminder it is fresher — per turn instead of per
            # client build — and costs nothing cache-wise. Not persisted to
            # the DB message (db_text above), so the UI stays clean.
            query_text = user_message
            if query_text or images:
                _time_note = (
                    "<system-reminder>Current time: "
                    f"{current_time_str(self.config.timezone)}"
                    "</system-reminder>"
                )
                query_text = (
                    f"{query_text}\n\n{_time_note}" if query_text else _time_note
                )

            # Backend-neutral turn input: attachments pass through raw;
            # each backend converts to its native shape (Anthropic content
            # blocks / codex UserInput items) and applies its own escaping
            # rules (e.g. the Claude CLI's slash-command interception).
            turn_input = TurnInput(text=query_text, images=images)

            # Send query + read response, with auto-retry on runtime crash.
            # The runtime may die during start_turn (TransportDiedError) or
            # during response reading (generic Exception from the stream).
            # Retry once with a fresh client if no content was received yet.
            #
            # The whole turn (query + every streamed event including tool
            # calls) is wrapped in ``lf_attrs`` so all OTEL spans emitted by
            # the Claude SDK carry our session_id / tags. The wrap is a
            # no-op when Langfuse is disabled (and for codex turns, which
            # are not instrumented — known v1 gap).
            _effective_model = (
                self._session_models.get(session_id) or model or ""
            )
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
                        await client.start_turn(turn_input)
                    except TransportDiedError as _qerr:
                        if _attempt > 0:
                            raise
                        logger.warning(
                            "Agent runtime dead for session %s (query phase): %s — retrying",
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

                    # Read response — may raise if the runtime crashes
                    # mid-stream or hangs idle beyond the per-message idle
                    # timeout (enforced inside the client's receive_turn).
                    try:
                        async for event in client.receive_turn():
                            done = await self._process_agent_event(
                                session_id, event, st,
                            )
                            if done:
                                # receive_turn also stops after the terminal
                                # event; the explicit break keeps the
                                # invariant local.
                                break

                    except asyncio.CancelledError:
                        raise  # propagate to outer handler
                    except Exception as _recv_err:
                        # Runtime crashed during response reading.
                        # Retry only if we haven't received any content yet
                        # (otherwise we'd produce duplicate/garbled output).
                        if st.got_content or _attempt > 0:
                            raise
                        logger.warning(
                            "Agent runtime crashed for session %s during response "
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
            # Persist the native session id so the session can be resumed
            # later. For new sessions the DB still has NULL because
            # mark_active() ran before the runtime emitted anything; the
            # normal source (TurnCompleted) never arrives on an
            # interrupted turn, so fall back to the live client's early-
            # captured id.
            if not st.sdk_session_id:
                _live = self.sessions.get_client(session_id)
                if _live is not None:
                    with contextlib.suppress(Exception):
                        st.sdk_session_id = _live.native_session_id
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
            # Preserve resumability on crashed turns (parity with the old
            # any-message early capture): the terminal event never arrived,
            # so pull the native id off the live client. _finalize_turn's
            # mark_active then restores it after mark_error's clear.
            # Poisoned contexts are the exception — they MUST start fresh
            # (the old code re-persisted the poisoned id here; fixed).
            if not is_poisoned and not st.sdk_session_id:
                _live = self.sessions.get_client(session_id)
                if _live is not None:
                    with contextlib.suppress(Exception):
                        st.sdk_session_id = _live.native_session_id

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

    async def _drain_pending_messages(
        self,
        session_id: str,
        client: Any,
        source: str,
        channel: str | None,
        manage_framing: bool = False,
        first_content_timeout: float = 30.0,
    ) -> int:
        """Drain agent events that arrived outside an active ``run()``.

        Autonomous runtime turns (Claude CLI background-task
        continuations) are routed through the same pipeline as user
        turns: blocks broadcast live, assistant message persisted with a
        leading ``{"type": "auto"}`` marker, usage recorded, ``done``
        emitted.  Standalone task lifecycle events update the background-
        task chips without opening a turn.

        Never parks while no turn is open (only consumes what's already
        buffered via ``try_receive_idle_events``), so the pre-query call
        inside ``run()`` cannot hang on an idle runtime.  A buffered
        ``init`` system event means content IS coming — the drain opens
        the turn and waits up to ``first_content_timeout`` for the first
        content event.  If nothing arrives the empty turn is dropped and
        the watcher's next poll picks the content up instead.  Once
        content flows, the wait uses the same idle timeout as a normal
        run; on that timeout the partial turn is persisted and
        ``asyncio.TimeoutError`` propagates so the caller can apply
        hung-runtime treatment.

        Backends without an idle stream (codex) return ``None`` from the
        probe immediately — the drain is a no-op for them.

        Caller must hold the per-session run lock.  ``manage_framing``
        controls session-level run framing (mark_running/session_running/
        buffering): the idle watcher passes True; ``run()`` passes False
        because its own framing is already open.

        Returns the number of completed autonomous turns processed.
        """
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
                    batch = client.try_receive_idle_events()
                    if batch is None:
                        break  # nothing buffered / stream closed
                else:
                    # Turn in flight — the runtime is producing; park for
                    # the next event batch.  Before the first content the
                    # wait is capped by first_content_timeout (init arrives
                    # seconds before the model's first output); after that
                    # it matches a normal run's idle timeout.
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
                        batch = await client.receive_idle_events(park_timeout)
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
                            "— persisting partial turn and flagging runtime as hung",
                            idle_timeout, session_id,
                        )
                        st.full_response_text += (
                            "\n\n[Background turn interrupted: runtime went silent]"
                            if st.full_response_text
                            else "[Background turn interrupted: runtime went silent]"
                        )
                        await _close_turn()
                        raise
                    if batch is None:
                        logger.warning(
                            "Agent stream ended mid-autonomous-turn for session %s",
                            session_id,
                        )
                        await _close_turn()
                        break

                if not batch:
                    continue  # unparseable / skip-and-continue payload

                # A ModelObserved that precedes content in the same batch
                # (autonomous AssistantMessage without init framing) must
                # not be lost — hold it and replay once the turn opens.
                pending_model: ModelObserved | None = None

                for event in batch:
                    if isinstance(event, SystemEvent) and st is None:
                        if event.subtype == "init":
                            # The runtime emits ``init`` when it starts
                            # processing a turn — an autonomous continuation
                            # is underway; open the turn and park for content.
                            await _open_turn()
                        else:
                            # Task lifecycle events between turns — chips only.
                            await self._handle_system_event(session_id, event)
                        continue

                    if isinstance(event, TurnCompleted) and st is None:
                        # Stray terminal event with no preceding content
                        # (e.g. a prior drain timed out mid-turn).  Consume
                        # it so it can't desync the next turn; nothing to
                        # render.
                        logger.info(
                            "Consumed stray TurnCompleted during drain for %s",
                            session_id,
                        )
                        continue

                    if st is None and isinstance(event, ModelObserved):
                        pending_model = event
                        continue

                    if st is None and isinstance(
                        event,
                        (TextDelta, ThinkingDelta, ToolUse, ToolResult,
                         SubagentStarted),
                    ):
                        await _open_turn()
                        if pending_model is not None:
                            await self._process_agent_event(
                                session_id, pending_model, st,
                            )
                            pending_model = None

                    if st is not None:
                        turn_done = await self._process_agent_event(
                            session_id, event, st,
                        )
                        if turn_done:
                            await _close_turn()
                    # A ModelObserved whose batch never produced content is
                    # dropped with it — an empty assistant message carries
                    # nothing worth persisting.

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
        """Spawn the idle stream watcher for a freshly connected client.

        No-op for backends without an idle stream (codex has no
        autonomous turns — the watcher would only burn a poll loop).
        """
        backend = self._backend_for_live_session(session_id)
        if not backend.capabilities.supports_idle_stream:
            return
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
                if client.buffer_used() <= 0:
                    if not client.is_alive():
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
        reused across runs (``run_persistent_cron``'s generation session,
        reused until context rotation): parking such a client would let the
        NEXT scheduled run reuse the same client/conversation while the
        prior run's background task is still in flight, interleaving the
        two. Keep-alive is only safe for the unique-per-run isolated paths
        (``run_cron`` / ``run_hook``).
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

    async def _stamp_cron_session_meta(
        self, session_id: str, mode: str, cache_ttl: str = "",
    ) -> None:
        """Stamp cron-session hints consumed by the cache-TTL policy.

        ``mode`` ("persistent" | "isolated") is the no-history prior for
        auto TTL resolution; ``cache_ttl`` is the per-job override from
        jobs.yaml (empty = no override).
        """
        session = await self.db.get_session(session_id)
        if not isinstance(session, dict):
            return
        try:
            meta = json.loads(session.get("metadata") or "{}")
        except (TypeError, ValueError):
            meta = {}
        updates: dict[str, Any] = {"cron_session_mode": mode}
        if cache_ttl:
            updates["cache_ttl_override"] = cache_ttl
        if all(meta.get(k) == v for k, v in updates.items()):
            return
        meta.update(updates)
        await self.db.update_session_metadata(session_id, meta)

    async def run_cron(
        self,
        job_id: str,
        prompt: str,
        model: str | None = None,
        run_id: str | None = None,
        cache_ttl: str = "",
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
        await self._stamp_cron_session_meta(session_id, "isolated", cache_ttl)
        try:
            return await self.run(
                session_id=session_id,
                user_message=prompt,
                source="cron",
                model=model,  # backend default_model(source) fills cron defaults
            )
        finally:
            await self._teardown_oneshot_client(session_id)

    async def run_persistent_cron(
        self,
        job_id: str,
        prompt: str,
        model: str | None = None,
        session_id: str | None = None,
        cache_ttl: str = "",
    ) -> str:
        """Run a persistent cron job that maintains context across runs.

        The caller (CronService) resolves which generation chat session the
        job currently owns and passes it as ``session_id`` — reusing it run
        after run so the SDK resumes conversation context, and minting a
        fresh session on context rotation (the old chat is preserved). When
        no ``session_id`` is given, falls back to the legacy stable id
        ``cron:{job_id}``. The client is discarded after each run to free
        the subprocess (sdk_session_id is preserved for the next resume).
        Unlike the isolated one-shot paths it does NOT keep the client alive
        for a live background task: the session is reused by the next run,
        which would collide with the parked task — so a persistent-cron
        background task that outlives its run is not resumed (use an
        isolated cron for long background work).
        """
        session_id = session_id or f"cron:{job_id}"
        await self.sessions.get_or_create(
            session_id, title=f"Cron: {job_id}", source="cron",
        )
        await self._stamp_cron_session_meta(session_id, "persistent", cache_ttl)
        try:
            return await self.run(
                session_id=session_id,
                user_message=prompt,
                source="cron",
                model=model,  # backend default_model(source) fills cron defaults
            )
        finally:
            # The session is reused by the next run (until rotation), which
            # would collide with a parked background task — so persistent
            # crons always discard (no keep-alive). See _teardown_oneshot_client.
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
                model=model,  # backend default_model(source) fills cron defaults
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
