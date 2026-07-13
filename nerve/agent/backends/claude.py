"""Claude backend — the Claude Agent SDK behind the backend seam.

Everything Claude-specific that used to live inline in
``nerve/agent/engine.py`` moved here unchanged in behavior:

* ``ClaudeAgentOptions`` assembly (system-prompt file spill, thinking /
  effort / betas / extra_args / env / plugins / per-session MCP servers)
* PreToolUse / PostToolUse hooks (file snapshots, image validation,
  ScheduleWakeup capture, background-agent permission parity)
* the ``can_use_tool`` permission adapter (interactive built-ins →
  :class:`~nerve.agent.interactive.InteractiveToolHandler`)
* resume-target validation (the ``~/.claude/projects`` .jsonl check)
* the hardened disconnect path (subprocess kill + anyio task-group disarm)
* SDK message → normalized :mod:`nerve.agent.backends.events` translation
* the between-turns idle stream (autonomous CLI turns)

The engine never imports ``claude_agent_sdk`` — this module is the only
place those types exist on the agent path.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
from typing import Any, AsyncIterator

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ResultMessage,
    SystemMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from claude_agent_sdk._errors import CLIConnectionError
from claude_agent_sdk.types import (
    HookMatcher,
    PermissionResult,
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)

from nerve.agent.backends import events as ev
from nerve.agent.backends.base import (
    AgentClient,
    BackendCapabilities,
    SessionSpec,
    TransportDiedError,
    TurnInput,
)
from nerve.agent.backends.images import validate_image_data, validate_image_file
from nerve.agent.cache_policy import cache_ttl_env
from nerve.agent.interactive import (
    FILE_MODIFY_TOOLS,
    INTERACTIVE_TOOLS,
    InteractiveToolHandler,
    _read_file_safe,
)

logger = logging.getLogger(__name__)

try:
    from claude_agent_sdk import ThinkingBlock
except ImportError:  # pragma: no cover - depends on SDK version
    ThinkingBlock = None

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
SYSTEM_PROMPT_INLINE_MAX = 100_000  # bytes


# ------------------------------------------------------------------ #
#  SDK message → normalized events                                     #
# ------------------------------------------------------------------ #

def translate_message(message: Any) -> list[ev.AgentEvent]:
    """Translate one SDK stream message into normalized agent events.

    One message may yield several events (a multi-block
    ``AssistantMessage``). Unknown message types yield nothing.
    """
    out: list[ev.AgentEvent] = []

    if isinstance(message, AssistantMessage):
        parent_id = getattr(message, "parent_tool_use_id", None)
        # Serving-model observation — main-agent messages only: sub-agents
        # legitimately run different models (Agent tool `model` opt,
        # built-in agent defaults), which must not pollute turn cost
        # attribution or fire serving-model change events.
        msg_model = getattr(message, "model", None)
        if msg_model and parent_id is None:
            out.append(ev.ModelObserved(model=msg_model))

        for block in message.content:
            if isinstance(block, TextBlock):
                out.append(ev.TextDelta(
                    text=block.text, parent_tool_use_id=parent_id,
                ))
            elif ThinkingBlock is not None and isinstance(block, ThinkingBlock):
                thinking = getattr(block, "thinking", "") or ""
                if not thinking:
                    # Empty thinking block (e.g. display="omitted", or
                    # simple queries on low effort). Nothing visible to
                    # render — never fall back to str(block) as that
                    # leaks the ThinkingBlock(...) repr into the UI.
                    continue
                out.append(ev.ThinkingDelta(
                    text=thinking, parent_tool_use_id=parent_id,
                ))
            elif isinstance(block, ToolUseBlock):
                tool_input = getattr(block, "input", {})
                tool_name = getattr(block, "name", None) or str(block)
                tool_use_id = getattr(block, "id", None)
                out.append(ev.ToolUse(
                    tool_use_id=tool_use_id,
                    name=tool_name,
                    input=tool_input,
                    parent_tool_use_id=parent_id,
                ))
                # Sub-agent lifecycle. Claude Code 2.1.x renamed the
                # subagent-spawning tool from ``Task`` → ``Agent``; match
                # both so old session history still opens panels on replay.
                if tool_name in ("Task", "Agent") and tool_use_id:
                    out.append(ev.SubagentStarted(
                        tool_use_id=tool_use_id,
                        subagent_type=str(
                            tool_input.get(
                                "subagent_type", tool_input.get("model", "agent"),
                            )
                        ),
                        description=str(tool_input.get("description", "")),
                        model=str(tool_input.get("model", "")) or None,
                    ))
            elif isinstance(block, ToolResultBlock):
                out.append(_translate_tool_result(block, parent_id))

    elif isinstance(message, UserMessage):
        parent_id = getattr(message, "parent_tool_use_id", None)
        content = getattr(message, "content", [])
        if isinstance(content, list):
            for block in content:
                if isinstance(block, ToolResultBlock):
                    out.append(_translate_tool_result(block, parent_id))

    elif isinstance(message, SystemMessage):
        subtype = getattr(message, "subtype", "") or ""
        data = dict(getattr(message, "data", None) or {})
        # Older SDK shapes put some fields at the message top level —
        # merge them so the engine reads one dict.
        for key in ("task_id", "description", "status", "tool_use_id", "summary"):
            if key not in data:
                value = getattr(message, key, None)
                if value is not None:
                    data[key] = value
        out.append(ev.SystemEvent(subtype=subtype, data=data))

    elif isinstance(message, ResultMessage):
        usage = (
            ev.NormalizedUsage.from_anthropic(message.usage)
            if message.usage else None
        )
        out.append(ev.TurnCompleted(
            native_session_id=message.session_id,
            model=None,  # claude reports the model per AssistantMessage
            usage=usage,
            # Cumulative per CLI process — the engine diffs it
            # (cost_is_cumulative capability).
            total_cost_usd=getattr(message, "total_cost_usd", None),
            duration_ms=getattr(message, "duration_ms", None),
            duration_api_ms=getattr(message, "duration_api_ms", None),
            num_turns=getattr(message, "num_turns", None),
            status="completed",
        ))

    return out


def _translate_tool_result(
    block: ToolResultBlock, parent_id: str | None,
) -> ev.ToolResult:
    return ev.ToolResult(
        tool_use_id=getattr(block, "tool_use_id", None),
        content=block.content,
        is_error=bool(getattr(block, "is_error", False) or False),
        parent_tool_use_id=parent_id,
    )


# ------------------------------------------------------------------ #
#  can_use_tool adapter                                                #
# ------------------------------------------------------------------ #

class ClaudeToolPermissions:
    """Adapts the backend-neutral :class:`InteractiveToolHandler` into the
    SDK's ``can_use_tool`` callback.

    * File-modifying tools trigger a pre-execution snapshot (redundant
      with the PreToolUse hook, kept as belt-and-suspenders).
    * Interactive built-ins pause via the hub and translate its outcome
      into ``PermissionResultAllow/Deny``.
    * Everything else auto-approves with zero overhead.
    """

    def __init__(self, hub: InteractiveToolHandler):
        self._hub = hub

    async def can_use_tool(
        self,
        tool_name: str,
        tool_input: dict[str, Any],
        context: ToolPermissionContext,
    ) -> PermissionResult:
        hub = self._hub
        # Capture pre-execution file snapshot for diff tracking
        # (also done via PreToolUse hook in the backend as primary path).
        if hub.snapshot_fn and tool_name in FILE_MODIFY_TOOLS:
            file_path = tool_input.get("file_path") or tool_input.get("notebook_path")
            if file_path and hub.mark_snapshotted(file_path):
                content = _read_file_safe(file_path)
                try:
                    await hub.snapshot_fn(hub.session_id, file_path, content)
                except Exception as e:
                    logger.warning(
                        "Failed to save file snapshot for %s: %s", file_path, e,
                    )

        if tool_name not in INTERACTIVE_TOOLS:
            return PermissionResultAllow()

        # Non-interactive channels: deny immediately to prevent deadlocks
        if not hub.interactive_capable:
            deny_messages = {
                "AskUserQuestion": (
                    "AskUserQuestion is not available in this channel. "
                    "Use the Nerve `ask_user` tool to ask the user questions asynchronously."
                ),
                "EnterPlanMode": (
                    "Plan mode is not available in non-web sessions. "
                    "Proceed with implementation directly."
                ),
                "ExitPlanMode": (
                    "Plan mode is not available in non-web sessions."
                ),
            }
            logger.info(
                "Session %s: auto-denying %s (non-interactive channel)",
                hub.session_id, tool_name,
            )
            return PermissionResultDeny(
                message=deny_messages.get(
                    tool_name,
                    f"{tool_name} is not available in this channel.",
                )
            )

        outcome = await hub.request_interaction(tool_name, tool_input)
        if outcome.cancelled:
            return PermissionResultDeny(
                message=outcome.message or "Session stopped by user.",
                interrupt=True,
            )
        if outcome.denied:
            return PermissionResultDeny(
                message=outcome.message or "Declined by user.",
            )
        # For AskUserQuestion: inject answers into the tool input
        if tool_name == "AskUserQuestion" and outcome.result:
            updated = {**tool_input, "answers": outcome.result}
            return PermissionResultAllow(updated_input=updated)
        # For ExitPlanMode/EnterPlanMode: just allow
        return PermissionResultAllow()


# ------------------------------------------------------------------ #
#  Backend                                                             #
# ------------------------------------------------------------------ #

class ClaudeBackend:
    """Claude Agent SDK backend."""

    name = "claude"
    capabilities = BackendCapabilities(
        cost_is_cumulative=True,
        supports_idle_stream=True,
        supports_cache_ttl=True,
        interactive_builtins=True,
        reports_context_window=False,
    )

    def __init__(self, deps: Any):
        self._deps = deps
        self.config = deps.config

    # -- policy -------------------------------------------------------- #

    def default_model(self, source: str) -> str:
        if source in ("cron", "hook"):
            return self.config.agent.cron_model
        return self.config.agent.model

    def excluded_tools(self) -> set[str]:
        # ScheduleWakeup is a Claude CLI built-in (captured via the
        # PostToolUse hook) — the registry equivalent exists for backends
        # without built-ins and would be a confusing duplicate here.
        return {"schedule_wakeup"}

    def validate_resume_target(self, native_id: str, cwd: str) -> bool:
        """Check whether Claude Code still has the conversation .jsonl
        for the given SDK session ID on this filesystem.

        The CLI stores history at::

            ~/.claude/projects/<encoded-cwd>/<sdk_session_id>.jsonl

        where <encoded-cwd> is the absolute cwd path with every '/'
        replaced by '-'.  The CLI resolves the cwd symlink before
        encoding, so when the workspace is itself a symlink the history
        lives under the *realpath*-encoded directory. Check the realpath
        first and fall back to the unresolved path.

        Best-effort: any unexpected error returns True so we still
        attempt the resume and let the CLI surface the real error.
        """
        try:
            projects = os.path.expanduser("~/.claude/projects")
            bases = [os.path.realpath(cwd)]
            if cwd not in bases:
                bases.append(cwd)
            for base in bases:
                encoded = base.replace("/", "-")
                jsonl = projects + "/" + encoded + "/" + native_id + ".jsonl"
                if os.path.isfile(jsonl):
                    return True
            return False
        except Exception as e:
            logger.debug(
                "Could not stat resume jsonl for %s: %s, assuming present",
                native_id[:12], e,
            )
            return True

    # -- client construction ------------------------------------------- #

    async def create_client(self, spec: SessionSpec) -> "ClaudeClient":
        options = self._build_options(spec)
        client = ClaudeClient(spec, options)
        await client.connect()
        return client

    def _build_options(self, spec: SessionSpec) -> ClaudeAgentOptions:
        """Build SDK client options for a session (moved from engine)."""
        config = self.config
        session_id = spec.session_id

        # Pass the system prompt as a file when it's large enough to risk
        # hitting Linux's MAX_ARG_STRLEN argv-element limit (see the
        # SYSTEM_PROMPT_INLINE_MAX comment above).
        system_prompt: str | dict[str, Any]
        if len(spec.system_prompt) > SYSTEM_PROMPT_INLINE_MAX:
            sp_path = self._write_system_prompt_file(session_id, spec.system_prompt)
            system_prompt = {"type": "file", "path": sp_path}
            logger.info(
                "Session %s: system prompt %d bytes (> %d), passing via file %s",
                session_id[:8], len(spec.system_prompt),
                SYSTEM_PROMPT_INLINE_MAX, sp_path,
            )
        else:
            system_prompt = spec.system_prompt

        # Local Ollama models are reached through the proxy and speak the
        # OpenAI-translated API — Anthropic-only knobs (extended thinking,
        # effort, the context-1m beta) don't apply and may break
        # translation, so suppress them for non-Claude models.
        selected_model = spec.model or config.agent.model
        is_ollama_model = (
            config.ollama.enabled and "claude" not in selected_model.lower()
        )

        thinking_config = (
            None if is_ollama_model
            else self._parse_thinking_config(config.agent.thinking, selected_model)
        )
        effort = (
            None if is_ollama_model
            else self._effective_effort(spec.effort, selected_model)
        )
        # Some subscriptions reject the context-1m beta for specific models
        # (e.g. claude-sonnet-4-6) — skip the beta header for those.
        betas = (
            ["context-1m-2025-08-07"]
            if not is_ollama_model
            and config.agent.context_1m_enabled_for(spec.model)
            else []
        )

        hooks = self._build_hooks(spec)

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
        # NOTE: --thinking-display hangs on Bedrock (multi-turn after
        # ToolSearch never returns). Disabled for Bedrock until the
        # provider bug is fixed.
        if (
            thinking_config
            and thinking_config.get("type") != "disabled"
            and not config.provider.is_bedrock
        ):
            extra_args["thinking-display"] = "summarized"

        can_use_tool = None
        if spec.interactive is not None:
            can_use_tool = ClaudeToolPermissions(spec.interactive).can_use_tool

        return ClaudeAgentOptions(
            model=selected_model,
            system_prompt=system_prompt,
            max_turns=spec.max_turns,
            # No permission_mode — can_use_tool callback handles all
            # permissions. Interactive tools pause for user input;
            # everything else auto-approves.
            can_use_tool=can_use_tool,
            thinking=thinking_config,
            effort=effort,
            betas=betas,
            resume=spec.resume_native_id,
            fork_session=spec.fork,
            hooks=hooks,
            stderr=_cli_stderr,
            extra_args=extra_args,
            # No allowed_tools — can_use_tool handles permissions.
            # External MCP server tools are discovered at connection time,
            # so we can't enumerate them upfront.
            #
            # Remove the CLI's cron tools — Nerve has its own cron system.
            # ``ScheduleWakeup`` stays available and is handled by Nerve's
            # wakeup harness (capture hook + cron-service sweep); the
            # CLI's own autonomous firing is suppressed via the
            # CLAUDE_CODE_DISABLE_CRON env var set in ``_build_env``.
            disallowed_tools=["CronCreate", "CronList", "CronDelete"],
            env=self._build_env(cache_ttl=spec.cache_ttl),
            cwd=spec.cwd,
            mcp_servers=self._build_mcp_servers(spec.session_id),
            # Claude Code plugins — loaded via --plugin-dir so the CLI
            # handles OAuth, credentials, and plugin lifecycle natively.
            plugins=self._deps.claude_plugins(),
        )

    def _system_prompt_dir(self) -> "os.PathLike[str]":
        """Directory where oversized system prompts are spilled to disk."""
        from pathlib import Path
        d = Path(self.config.workspace) / ".nerve" / "cache" / "system_prompts"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _write_system_prompt_file(self, session_id: str, content: str) -> str:
        """Write the system prompt to disk and return its absolute path.

        Deterministic filename so a session that reconnects (resume) gets
        the same prompt without re-writing. Lazy GC of stale files (>7d).
        """
        import time
        from pathlib import Path

        dir_path = Path(self._system_prompt_dir())

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

    def _build_env(self, cache_ttl: str = "5m") -> dict[str, str]:
        """Build environment variables for the SDK subprocess."""
        config = self.config
        env: dict[str, str] = {}
        # Prompt-cache TTL: the CLI natively supports the 1-hour TTL via
        # this env var. Resolved per client build by
        # nerve.agent.cache_policy — see that module for the policy.
        env.update(cache_ttl_env(cache_ttl, config.provider.is_bedrock))
        # Disable the CLI's built-in cron/wakeup scheduler — Nerve owns
        # wakeup timing (PostToolUse capture + cron-service sweep). The
        # tool itself stays available (this flag only gates the firing).
        env["CLAUDE_CODE_DISABLE_CRON"] = "1"
        if config.provider.is_bedrock:
            env["CLAUDE_CODE_USE_BEDROCK"] = "1"
            if config.provider.aws_region:
                env["AWS_REGION"] = config.provider.aws_region
            if config.provider.aws_profile:
                env["AWS_PROFILE"] = config.provider.aws_profile
            if config.provider.aws_access_key_id:
                env["AWS_ACCESS_KEY_ID"] = config.provider.aws_access_key_id
                env["AWS_SECRET_ACCESS_KEY"] = config.provider.aws_secret_access_key
        else:
            api_key = config.effective_api_key
            if api_key:
                env["ANTHROPIC_API_KEY"] = api_key
            if config.proxy.enabled:
                env["ANTHROPIC_BASE_URL"] = (
                    f"http://{config.proxy.host}:{config.proxy.port}"
                )
        return env

    def _build_mcp_servers(self, session_id: str) -> dict[str, Any]:
        """Build the mcp_servers dict: in-process nerve + external servers.

        Claude Code plugin MCPs are handled separately via the SDK
        ``plugins`` field which lets the CLI manage OAuth and plugin
        lifecycle natively.
        """
        from nerve.agent.tools import build_session_mcp_server

        tool_ctx = self._deps.tool_ctx_factory(session_id)
        include_hoa = bool(self.config.houseofagents.enabled)
        servers: dict[str, Any] = {
            "nerve": build_session_mcp_server(
                self._deps.registry, tool_ctx,
                include_hoa=include_hoa,
                exclude=self.excluded_tools(),
            ),
        }
        for srv in self._deps.external_mcp_servers():
            if srv.enabled and srv.name != "nerve":
                try:
                    servers[srv.name] = srv.to_sdk_config()
                except ValueError as e:
                    logger.warning("Skipping MCP server %r: %s", srv.name, e)
        if len(servers) > 1:
            logger.debug(
                "Session %s: %d MCP servers (%s)",
                session_id[:8], len(servers), ", ".join(servers.keys()),
            )
        return servers

    def _build_hooks(self, spec: SessionSpec) -> dict:
        """Build SDK hooks for this session.

        PreToolUse: file snapshots (Edit/Write/NotebookEdit) and image
        validation (Read). PostToolUse: ScheduleWakeup capture, recorded
        via ``spec.record_wakeup`` so the cron-service sweep can fire it
        through ``engine.run(..., source="wakeup")``.
        """
        session_id = spec.session_id
        captured_files: set[str] = set()

        async def _snapshot_hook(hook_input, tool_use_id, context):
            """PreToolUse: capture file content before Edit/Write/NotebookEdit."""
            tool_input = hook_input.get("tool_input", {})
            file_path = tool_input.get("file_path") or tool_input.get("notebook_path")

            if file_path and file_path not in captured_files and spec.snapshot:
                captured_files.add(file_path)
                content = _read_file_safe(file_path)
                try:
                    await spec.snapshot(session_id, file_path, content)
                    logger.info("Captured file snapshot for %s", file_path)
                except Exception as e:
                    logger.warning(
                        "Failed to save file snapshot for %s: %s", file_path, e,
                    )

            return {"hookSpecificOutput": {"hookEventName": "PreToolUse"}}

        async def _validate_image_hook(hook_input, tool_use_id, context):
            """PreToolUse: validate image files before Read.

            The CLI's Read tool detects images by extension and base64-
            encodes them into image content blocks. If the file isn't a
            valid image, the API rejects it with 400 and the bad block
            persists in the CLI's history — an unrecoverable poison loop.
            Check magic bytes and size *before* Read executes.
            """
            tool_input = hook_input.get("tool_input", {})
            file_path = tool_input.get("file_path", "")

            error = validate_image_file(file_path)
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
            """PostToolUse: record a ScheduleWakeup so Nerve can fire it."""
            if spec.record_wakeup:
                try:
                    await spec.record_wakeup(
                        session_id, hook_input.get("tool_input", {}) or {},
                    )
                except Exception as e:
                    logger.warning(
                        "Failed to record wakeup for session %s: %s",
                        session_id, e,
                    )
            return {"hookSpecificOutput": {"hookEventName": "PostToolUse"}}

        async def _grant_permission_hook(hook_input, tool_use_id, context):
            """PreToolUse: pre-approve non-interactive tools.

            Background sub-agents run detached and non-blocking, so the
            CLI never invokes ``can_use_tool`` for their nested tool
            calls and denies Write/Edit/Bash by default. A PreToolUse
            hook DOES fire for them, so returning
            ``permissionDecision: "allow"`` grants the same auto-approval
            foreground agents get. Interactive tools and Read are left
            untouched (they defer to ``can_use_tool`` / the validator).
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
            HookMatcher(matcher="Edit|Write|NotebookEdit", hooks=[_snapshot_hook]),
            HookMatcher(matcher="Read", hooks=[_validate_image_hook]),
        ]
        # Catch-all permission grant so background sub-agents inherit the
        # foreground's tool permissions. Registered last so the snapshot/
        # validator hooks still run for their tools; a deny from the
        # validator wins over this allow.
        if self.config.agent.background_agent_permissions:
            pre_tool_use.append(
                HookMatcher(matcher=None, hooks=[_grant_permission_hook])
            )

        return {
            "PreToolUse": pre_tool_use,
            "PostToolUse": [
                HookMatcher(matcher="ScheduleWakeup", hooks=[_capture_wakeup_hook]),
            ],
        }

    # -- thinking / effort helpers (moved verbatim from engine) ---------- #

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
        if not ClaudeBackend._model_supports_legacy_enabled_thinking(model):
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
        if value not in ClaudeBackend._EFFORT_RANK:
            return None
        allowed: tuple[str, ...] | None = None
        if model:
            m = model.lower()
            for key, levels in ClaudeBackend._MODEL_EFFORT_LEVELS.items():
                if key in m:
                    allowed = levels
                    break
        if not allowed or value in allowed:
            return value
        requested_rank = ClaudeBackend._EFFORT_RANK.index(value)
        for level in reversed(ClaudeBackend._EFFORT_RANK[: requested_rank + 1]):
            if level in allowed:
                logger.debug(
                    "Capped effort %r to %r for model %r (model caps at %r)",
                    value, level, model, allowed[-1],
                )
                return level
        return None


# ------------------------------------------------------------------ #
#  Client                                                              #
# ------------------------------------------------------------------ #

class ClaudeClient(AgentClient):
    """One live Claude Code CLI subprocess for one nerve session."""

    def __init__(self, spec: SessionSpec, options: ClaudeAgentOptions):
        self._spec = spec
        self._options = options
        self._sdk = ClaudeSDKClient(options=options)
        self._native_session_id: str | None = spec.resume_native_id
        # The resolved model this client was built with (engine reads it
        # to detect mid-session model switches).
        self.model: str = options.model or ""

    # -- protocol ------------------------------------------------------- #

    @property
    def native_session_id(self) -> str | None:
        return self._native_session_id

    async def connect(self) -> None:
        await self._sdk.connect()

    async def start_turn(self, turn: TurnInput) -> None:
        try:
            if turn.images or turn.documents:
                blocks = self._build_content_blocks(turn)

                async def _prompt():
                    yield {
                        "type": "user",
                        "message": {"role": "user", "content": blocks},
                        "parent_tool_use_id": None,
                    }

                await self._sdk.query(_prompt())
            else:
                await self._sdk.query(self._escape_slash(turn.text))
        except CLIConnectionError as e:
            raise TransportDiedError(str(e)) from e

    @staticmethod
    def _escape_slash(text: str) -> str:
        # Escape slash-prefixed messages so Claude Code CLI doesn't
        # intercept them as built-in slash commands. Registered bot
        # commands (/stop, /new, ...) are handled upstream — anything
        # that reaches here should go straight to the LLM.
        if text and text.startswith("/"):
            return "​" + text
        return text

    def _build_content_blocks(self, turn: TurnInput) -> list[dict[str, Any]]:
        """Build Anthropic multi-modal content blocks (moved from engine)."""
        blocks: list[dict[str, Any]] = []
        text = self._escape_slash(turn.text)
        if text:
            blocks.append({"type": "text", "text": text})
        for img in (turn.images or []) + (turn.documents or []):
            # Text files are inlined as text context blocks
            if img.get("type") == "text_file":
                fname = img.get("filename", "file")
                content = img.get("content", "")
                blocks.append({
                    "type": "text",
                    "text": f"--- Attached: {fname} ---\n{content}",
                })
                continue

            # PDFs use "document" content block; images use "image"
            block_type = (
                "document" if img.get("media_type") == "application/pdf"
                else "image"
            )

            # Validate image data before sending — prevent poisoning the
            # CLI's conversation with unprocessable images.
            if block_type == "image":
                img_error = validate_image_data(
                    img.get("data", ""), img.get("media_type", ""),
                )
                if img_error:
                    logger.warning(
                        "Skipping invalid image for session %s: %s",
                        self._spec.session_id[:8], img_error,
                    )
                    # Inject as text so the agent knows what happened
                    blocks.append({
                        "type": "text",
                        "text": f"[Image skipped: {img_error}]",
                    })
                    continue

            blocks.append({
                "type": block_type,
                "source": {
                    "type": img.get("type", "base64"),
                    "media_type": img.get("media_type"),
                    "data": img.get("data"),
                },
            })
        return blocks

    async def receive_turn(self) -> AsyncIterator[ev.AgentEvent]:
        """Iterate the SDK response with a per-message idle timeout.

        ``receive_response()`` can block indefinitely if the CLI hangs
        (stuck API request, broken stdio pipe). Each ``__anext__()`` is
        wrapped in ``asyncio.wait_for`` so a silent CLI raises
        ``asyncio.TimeoutError`` into the engine's hung-client retry
        path. The timeout is per-message: long tool calls don't trip it
        as long as tool_use/tool_result chunks keep arriving.
        ``idle_timeout <= 0`` disables the timeout.
        """
        idle_timeout = self._spec.idle_timeout
        response_iter = self._sdk.receive_response()
        try:
            while True:
                try:
                    if idle_timeout and idle_timeout > 0:
                        message = await asyncio.wait_for(
                            response_iter.__anext__(), timeout=idle_timeout,
                        )
                    else:
                        message = await response_iter.__anext__()
                except StopAsyncIteration:
                    return
                except asyncio.TimeoutError:
                    logger.warning(
                        "CLI idle timeout (%ds) for session %s — no SDK "
                        "message received; treating CLI as hung",
                        idle_timeout, self._spec.session_id,
                    )
                    raise
                done = False
                for event in self._translate_and_capture(message):
                    if isinstance(event, ev.TurnCompleted):
                        done = True
                    yield event
                if done:
                    return
        finally:
            with contextlib.suppress(Exception):
                await response_iter.aclose()

    def _translate_and_capture(self, message: Any) -> list[ev.AgentEvent]:
        # Early-capture the SDK session id from any message that carries
        # it, so /stop-mid-turn persistence works before a ResultMessage
        # ever arrives (the engine reads client.native_session_id).
        msg_sid = getattr(message, "session_id", None)
        if msg_sid:
            self._native_session_id = msg_sid
        return translate_message(message)

    async def interrupt(self) -> None:
        await self._sdk.interrupt()

    def is_alive(self) -> bool:
        transport = getattr(self._sdk, "_transport", None)
        if not transport:
            return False
        process = getattr(transport, "_process", None)
        if process is None:
            return False
        return process.returncode is None

    async def disconnect(self, timeout: float = 5.0) -> None:
        """Disconnect without risking an event-loop spin (moved from
        engine._safe_disconnect).

        The SDK's Query.close() cancels its anyio task group before
        closing the transport. If any task inside that group cannot exit
        promptly, the anyio _deliver_cancellation callback spins at 100%
        CPU forever. Strategy: kill the subprocess first so every I/O
        wait unblocks, try a clean disconnect with a timeout, then
        forcibly disarm the task group if needed.
        """
        client = self._sdk
        # --- 1. Kill subprocess immediately ---
        transport = getattr(getattr(client, "_query", None), "transport", None)
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

        cs = getattr(tg, "cancel_scope", None)
        handle = getattr(cs, "_cancel_handle", None)
        if handle is not None:
            handle.cancel()
            cs._cancel_handle = None

        if cs is not None:
            cs._tasks.clear()
        tg._tasks.clear()

        try:
            await asyncio.wait_for(query.transport.close(), timeout=2.0)
        except Exception:
            pass

        client._query = None
        client._transport = None

    # -- idle stream (autonomous CLI turns between run() calls) --------- #

    def _message_stream(self) -> Any | None:
        """The SDK client's internal receive stream.

        Private-API access (``client._query._message_receive``), pinned
        to the bundled SDK version. Callers degrade gracefully (drain and
        watcher become no-ops) when the attribute shape changes.
        """
        return getattr(getattr(self._sdk, "_query", None), "_message_receive", None)

    def buffer_used(self) -> int:
        stream = self._message_stream()
        if stream is None:
            return 0
        try:
            return int(stream.statistics().current_buffer_used)
        except Exception:
            return 0

    def try_receive_idle_events(self) -> list[ev.AgentEvent] | None:
        """Non-parking probe: translated events of one buffered message.

        Returns ``None`` when nothing is buffered or the stream is
        closed; ``[]`` for messages that parse to nothing (skip and call
        again).
        """
        import anyio

        stream = self._message_stream()
        if stream is None:
            return None
        try:
            data = stream.receive_nowait()
        except anyio.WouldBlock:
            return None
        except (anyio.EndOfStream, anyio.ClosedResourceError):
            return None
        except Exception:
            return None
        return self._parse_idle_payload(data)

    async def receive_idle_events(
        self, timeout: float | None,
    ) -> list[ev.AgentEvent] | None:
        """Park up to ``timeout`` seconds for the next idle message.

        Returns ``None`` when the stream ended/closed; raises
        ``asyncio.TimeoutError`` on timeout (caller applies hung-CLI
        treatment); ``[]`` for skip-and-continue payloads.
        """
        import anyio

        stream = self._message_stream()
        if stream is None:
            return None
        try:
            data = await asyncio.wait_for(stream.receive(), timeout=timeout)
        except (anyio.EndOfStream, anyio.ClosedResourceError):
            return None
        return self._parse_idle_payload(data)

    def _parse_idle_payload(self, data: Any) -> list[ev.AgentEvent] | None:
        """Raw stream payload → events. ``None`` = stream over."""
        from claude_agent_sdk._errors import MessageParseError
        from claude_agent_sdk._internal.message_parser import parse_message

        mtype = data.get("type") if isinstance(data, dict) else None
        if mtype == "end":
            # Reader sentinel — stream is closed.
            return None
        if mtype == "error":
            logger.error(
                "SDK stream error during idle drain for %s: %s",
                self._spec.session_id, data.get("error"),
            )
            return None

        try:
            message = parse_message(data)
        except MessageParseError as pe:
            logger.warning(
                "Unparseable SDK message during drain for %s: %s",
                self._spec.session_id, pe,
            )
            return []
        if message is None:
            return []
        return self._translate_and_capture(message)
