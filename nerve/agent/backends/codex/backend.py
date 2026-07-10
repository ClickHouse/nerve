"""Codex backend — OpenAI Codex (``codex app-server``) behind the seam.

One app-server subprocess per nerve session (mirroring the Claude
process model, so idle-sweep / kill / rebuild semantics carry over); one
Codex *thread* per nerve session; one turn at a time (the engine
serializes turns per session).

Event mapping (docs/plans/codex-backend.md §7) deliberately reuses the
Claude tool vocabulary ("Bash" / "Edit" / "WebSearch" / ``mcp__*``) so
the existing UI — tool chips, the file-diff panel keyed on
``input.file_path``, snapshot-based diffs — works unchanged. Inputs
carry codex-native fields; this is presentation, not a semantic lie.

Nerve tools reach codex sessions through the gateway's Streamable HTTP
MCP endpoint with a session-bound bearer token (env
``NERVE_MCP_TOKEN``), so ``notify`` / ``ask_user`` / ``memorize`` / ...
attribute to the real session exactly like the in-process Claude MCP.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, AsyncIterator

from nerve.agent.backends import events as ev
from nerve.agent.backends.base import (
    AgentClient,
    BackendCapabilities,
    BackendError,
    SessionSpec,
    TransportDiedError,
    TurnInput,
)
from nerve.agent.backends.codex.appserver import (
    CodexAppServerClient,
    CodexRpcError,
)
from nerve.agent.backends.codex.diffs import reverse_apply_unified_diff
from nerve.agent.backends.codex.pricing import compute_cost
from nerve.agent.backends.images import validate_image_data

logger = logging.getLogger(__name__)

# Backend notes appended to the developer instructions so the model
# knows how this runtime differs from the docs it may have absorbed.
_BACKEND_NOTES = """

<backend-notes>
You are running on Nerve's Codex backend.
- Nerve's tools (memorize, memory_recall, task_*, notify, ask_user, skills,
  schedule_wakeup, ...) are provided by the `nerve` MCP server — call them as
  `mcp__nerve__<name>` / however your harness names MCP tools.
- To schedule a future wakeup of this session, use the `schedule_wakeup` nerve
  tool (there is no ScheduleWakeup built-in here).
- AskUserQuestion and plan mode do not exist in this runtime. To ask the user
  something, use the nerve `ask_user` tool.
</backend-notes>
"""


class CodexTurnError(BackendError):
    """A codex turn failed with a non-retryable error."""


def _toml_str(value: str) -> str:
    """Quote a string as a TOML literal for ``-c key=value`` overrides."""
    return json.dumps(value, ensure_ascii=False)


class CodexBackend:
    """OpenAI Codex app-server backend."""

    name = "codex"
    capabilities = BackendCapabilities(
        cost_is_cumulative=False,
        supports_idle_stream=False,
        supports_cache_ttl=False,
        interactive_builtins=False,
        reports_context_window=True,
    )

    def __init__(self, deps: Any):
        self._deps = deps
        self.config = deps.config
        self.codex = deps.config.codex
        Path(self._home_dir()).mkdir(parents=True, exist_ok=True)

    # -- policy ---------------------------------------------------------- #

    def default_model(self, source: str) -> str:
        if source in ("cron", "hook") and self.codex.cron_model:
            return self.codex.cron_model
        return self.codex.model

    def excluded_tools(self) -> set[str]:
        return set()

    def validate_resume_target(self, native_id: str, cwd: str) -> bool:
        # No cheap filesystem check for codex threads; create_client
        # recovers from a stale id via ResumeDroppedError instead.
        return True

    def _home_dir(self) -> str:
        return os.path.expanduser(self.codex.home_dir)

    # -- client construction --------------------------------------------- #

    async def create_client(self, spec: SessionSpec) -> "CodexClient":
        client = CodexClient(self, spec)
        await client.connect()
        return client

    # -- config assembly (used by CodexClient) --------------------------- #

    def build_env(self, spec: SessionSpec) -> dict[str, str]:
        env = os.environ.copy()
        env["CODEX_HOME"] = self._home_dir()
        # Session-bound bearer token for the nerve MCP endpoint —
        # referenced from the MCP config via bearer_token_env_var so the
        # token never lands in any file.
        if self._deps.mint_session_token is not None:
            try:
                env["NERVE_MCP_TOKEN"] = self._deps.mint_session_token(
                    spec.session_id,
                )
            except Exception as e:
                logger.warning(
                    "Could not mint MCP session token for %s: %s",
                    spec.session_id, e,
                )
        return env

    def build_config_overrides(self, spec: SessionSpec) -> list[str]:
        """``-c key=value`` process-level config overrides.

        Process == session here, so spawn-level overrides ARE per-session
        config. This is the same mechanism the official SDKs use and is
        honored for every thread the process hosts.
        """
        overrides: list[str] = [
            # The workspace AGENTS.md is Nerve's identity bundle and is
            # already injected via developerInstructions — suppress
            # project-doc discovery so it isn't duplicated.
            "project_doc_max_bytes=0",
        ]

        # Nerve tools over the gateway's Streamable HTTP MCP endpoint.
        port = self._deps.gateway_port()
        if port and self._deps.mint_session_token is not None:
            base = "mcp_servers.nerve"
            url = f"http://127.0.0.1:{port}/mcp/v1"
            overrides += [
                f"{base}.url={_toml_str(url)}",
                f"{base}.bearer_token_env_var={_toml_str('NERVE_MCP_TOKEN')}",
                f"{base}.tool_timeout_sec={int(self.codex.tool_timeout_sec)}",
            ]
        else:
            logger.warning(
                "Codex session %s starts WITHOUT nerve tools: gateway port "
                "or token minter unavailable", spec.session_id,
            )

        # External MCP servers (grafana, langfuse, ...) — translate the
        # nerve config into codex's mcp_servers shape. Claude-plugin MCPs
        # have no codex equivalent and are skipped (docs plan §14).
        for srv in self._deps.external_mcp_servers():
            if not srv.enabled or srv.name == "nerve":
                continue
            try:
                overrides += self._translate_mcp_server(srv)
            except Exception as e:
                logger.warning(
                    "Skipping MCP server %r for codex: %s", srv.name, e,
                )

        if not self.codex.web_search:
            overrides.append("tools.web_search=false")

        for key, value in (self.codex.extra_config or {}).items():
            if isinstance(value, str):
                overrides.append(f"{key}={_toml_str(value)}")
            elif isinstance(value, bool):
                overrides.append(f"{key}={'true' if value else 'false'}")
            else:
                overrides.append(f"{key}={value}")
        return overrides

    @staticmethod
    def _translate_mcp_server(srv: Any) -> list[str]:
        """Translate one nerve ``McpServerConfig`` into codex overrides."""
        base = f"mcp_servers.{srv.name}"
        out: list[str] = []
        command = getattr(srv, "command", None)
        url = getattr(srv, "url", None)
        if command:
            out.append(f"{base}.command={_toml_str(command)}")
            args = getattr(srv, "args", None) or []
            if args:
                arr = ", ".join(_toml_str(str(a)) for a in args)
                out.append(f"{base}.args=[{arr}]")
            env = getattr(srv, "env", None) or {}
            for k, v in env.items():
                out.append(f"{base}.env.{k}={_toml_str(str(v))}")
        elif url:
            out.append(f"{base}.url={_toml_str(url)}")
            headers = getattr(srv, "headers", None) or {}
            for k, v in headers.items():
                out.append(f'{base}.http_headers.{_toml_str(k)}={_toml_str(str(v))}')
        else:
            raise ValueError("no command or url")
        return out

    def thread_params(self, spec: SessionSpec) -> dict[str, Any]:
        params: dict[str, Any] = {
            "cwd": spec.cwd,
            "model": spec.model or self.codex.model,
            "sandbox": self.codex.sandbox,
            "approvalPolicy": self.codex.approval_policy,
            "developerInstructions": spec.system_prompt + _BACKEND_NOTES,
        }
        return params

    def map_effort(self, effort: str) -> str | None:
        return (self.codex.effort_map or {}).get(effort)

    def auth_hint(self) -> str:
        if self.codex.auth == "api_key":
            return (
                "configure codex.api_key (or codex.api_key_env) in config"
            )
        return f"run: CODEX_HOME={self._home_dir()} codex login"

    def resolve_api_key(self) -> str | None:
        if self.codex.api_key:
            return self.codex.api_key
        # Fall back to the top-level secret (config.local.yaml) nerve
        # already keeps for OpenAI, then the configured env var.
        if getattr(self.config, "openai_api_key", ""):
            return self.config.openai_api_key
        if self.codex.api_key_env:
            return os.environ.get(self.codex.api_key_env) or None
        return None


class CodexClient(AgentClient):
    """One live ``codex app-server`` subprocess for one nerve session."""

    def __init__(self, backend: CodexBackend, spec: SessionSpec):
        self._backend = backend
        self._spec = spec
        self._transport = CodexAppServerClient(
            bin_path=backend.codex.bin_path,
            cwd=spec.cwd,
            env=backend.build_env(spec),
            server_request_handler=self._handle_server_request,
            config_overrides=backend.build_config_overrides(spec),
        )
        self._thread_id: str | None = None
        self._turn_id: str | None = None
        self._resume_dropped = False
        # Serving model as resolved for this thread; updated on
        # model/rerouted notifications.
        self.model: str = spec.model or backend.codex.model
        # item id -> item payload from item/started (approval correlation
        # + fileChange fan-out bookkeeping).
        self._items: dict[str, dict] = {}
        # Latest thread/tokenUsage/updated for the active turn.
        self._turn_usage: dict | None = None
        self._context_window: int | None = None

    # -- protocol --------------------------------------------------------- #

    @property
    def native_session_id(self) -> str | None:
        return self._thread_id

    @property
    def resume_dropped(self) -> bool:
        """True when the stored thread id could not be resumed and a
        fresh thread was started — the engine must clear the persisted
        native id (it re-persists the new one at turn end)."""
        return self._resume_dropped

    async def connect(self) -> None:
        await self._transport.start()
        await self._ensure_auth()

        spec = self._spec
        backend = self._backend
        params = backend.thread_params(spec)

        try:
            if spec.resume_native_id and spec.fork:
                response = await self._transport.request(
                    "thread/fork",
                    {**params, "threadId": spec.resume_native_id},
                )
            elif spec.resume_native_id:
                response = await self._transport.request(
                    "thread/resume",
                    {**params, "threadId": spec.resume_native_id},
                )
            else:
                response = await self._transport.request("thread/start", params)
        except CodexRpcError as e:
            # Resume-miss recovery: a wiped ~/.nerve/codex/sessions (or a
            # rollout the app-server refuses) must never brick the
            # session — fall back to a fresh thread and tell the engine
            # the old id was dropped.
            if not spec.resume_native_id:
                raise
            logger.warning(
                "Codex %s of %s failed for session %s (%s) — starting a "
                "fresh thread",
                "fork" if spec.fork else "resume",
                spec.resume_native_id[:12], spec.session_id, e,
            )
            self._resume_dropped = True
            response = await self._transport.request("thread/start", params)

        thread = response.get("thread") if isinstance(response, dict) else None
        thread_id = (thread or {}).get("id") if isinstance(thread, dict) else None
        if not thread_id:
            raise BackendError(
                f"codex thread/start returned no thread id: {response!r}"
            )
        self._thread_id = str(thread_id)

    async def _ensure_auth(self) -> None:
        """Best-effort auth check with a clear operator hint.

        An api-key config logs in automatically (persisted in
        CODEX_HOME/auth.json); ChatGPT auth must be done once manually.
        """
        backend = self._backend
        try:
            account = await self._transport.request("account/read", {})
        except CodexRpcError as e:
            logger.debug("codex account/read failed (%s) — proceeding", e)
            return
        if isinstance(account, dict) and account.get("account"):
            return

        if backend.codex.auth == "api_key":
            api_key = backend.resolve_api_key()
            if api_key:
                try:
                    await self._transport.request(
                        "account/login/start",
                        {"type": "apiKey", "apiKey": api_key},
                    )
                    logger.info("Codex: logged in with API key")
                    return
                except CodexRpcError as e:
                    raise BackendError(
                        f"Codex API-key login failed: {e}"
                    ) from e
        raise BackendError(
            "Codex is not authenticated. To fix: " + backend.auth_hint()
        )

    async def start_turn(self, turn: TurnInput) -> None:
        if not self._thread_id:
            raise BackendError("codex client has no thread")
        self._turn_usage = None
        self._items.clear()
        # Drain notifications that straggled in after the previous turn
        # (late tokenUsage updates etc.) so they can't bleed into this one.
        while not self._transport.notifications.empty():
            try:
                self._transport.notifications.get_nowait()
            except Exception:
                break

        params: dict[str, Any] = {
            "threadId": self._thread_id,
            "input": self._build_input_items(turn),
        }
        effort = self._backend.map_effort(self._spec.effort)
        if effort:
            params["effort"] = effort

        response = await self._transport.request("turn/start", params)
        turn_obj = response.get("turn") if isinstance(response, dict) else None
        self._turn_id = (
            str(turn_obj["id"])
            if isinstance(turn_obj, dict) and turn_obj.get("id")
            else None
        )

    def _build_input_items(self, turn: TurnInput) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        text = turn.text or ""
        notes: list[str] = []

        for att in (turn.images or []) + (turn.documents or []):
            if att.get("type") == "text_file":
                fname = att.get("filename", "file")
                content = att.get("content", "")
                notes.append(f"--- Attached: {fname} ---\n{content}")
                continue
            media_type = att.get("media_type") or ""
            if media_type == "application/pdf":
                # No document input type in the codex protocol — surface
                # the degradation instead of silently dropping (plan §14).
                notes.append(
                    "[A PDF attachment could not be delivered: the Codex "
                    "backend does not support document inputs. Ask the "
                    "user for the content as text if needed.]"
                )
                continue
            if att.get("path"):
                items.append({"type": "localImage", "path": str(att["path"])})
                continue
            data = att.get("data", "")
            err = validate_image_data(data, media_type)
            if err:
                logger.warning(
                    "Skipping invalid image for session %s: %s",
                    self._spec.session_id[:8], err,
                )
                notes.append(f"[Image skipped: {err}]")
                continue
            items.append({
                "type": "image",
                "url": f"data:{media_type};base64,{data}",
            })

        if notes:
            text = "\n\n".join(filter(None, [text] + notes))
        # Text goes first so the model reads the instruction before
        # attachments — insert rather than append.
        if text:
            items.insert(0, {"type": "text", "text": text})
        if not items:
            items.append({"type": "text", "text": ""})
        return items

    async def receive_turn(self) -> AsyncIterator[ev.AgentEvent]:
        """Yield normalized events until this turn completes.

        Per-notification idle timeout mirrors the Claude path: a silent
        app-server raises ``asyncio.TimeoutError`` into the engine's
        hung-client retry path. Terminates on ``turn/completed``
        regardless of status (completed / interrupted / failed) so the
        /stop flow's graceful wait works.
        """
        idle_timeout = self._spec.idle_timeout
        # The thread model is serving this turn — surface it for
        # serving-model tracking (parity with AssistantMessage.model).
        yield ev.ModelObserved(model=self.model)

        while True:
            try:
                if idle_timeout and idle_timeout > 0:
                    note = await asyncio.wait_for(
                        self._transport.notifications.get(),
                        timeout=idle_timeout,
                    )
                else:
                    note = await self._transport.notifications.get()
            except asyncio.TimeoutError:
                logger.warning(
                    "codex idle timeout (%ds) for session %s — no "
                    "notification received; treating app-server as hung",
                    idle_timeout, self._spec.session_id,
                )
                raise

            method = note.get("method", "")
            params = note.get("params", {}) or {}

            if method == "__transport_died__":
                raise TransportDiedError(
                    "codex app-server died mid-turn"
                )

            # Scope: drop notifications for other turns (stragglers from
            # an interrupted predecessor). Thread-scoped notifications
            # (no turnId) pass through.
            note_turn = params.get("turnId")
            if note_turn and self._turn_id and note_turn != self._turn_id:
                logger.debug("codex: dropping stale notification %s", method)
                continue

            for event in await self._map_notification(method, params):
                yield event
                if isinstance(event, ev.TurnCompleted):
                    return

    # -- notification mapping -------------------------------------------- #

    async def _map_notification(
        self, method: str, params: dict,
    ) -> list[ev.AgentEvent]:
        out: list[ev.AgentEvent] = []

        if method == "item/agentMessage/delta":
            delta = params.get("delta") or params.get("text") or ""
            if delta:
                out.append(ev.TextDelta(text=str(delta)))

        elif method in (
            "item/reasoning/textDelta",
            "item/reasoning/summaryTextDelta",
        ):
            delta = params.get("delta") or params.get("text") or ""
            if delta:
                out.append(ev.ThinkingDelta(text=str(delta)))

        elif method == "item/started":
            out.extend(await self._map_item_started(params.get("item") or {}))

        elif method == "item/completed":
            out.extend(await self._map_item_completed(params.get("item") or {}))

        elif method == "thread/tokenUsage/updated":
            usage = params.get("tokenUsage") or {}
            if isinstance(usage, dict):
                self._turn_usage = usage
                window = usage.get("modelContextWindow")
                if isinstance(window, int) and window > 0:
                    self._context_window = window

        elif method == "model/rerouted":
            new_model = (
                params.get("model") or params.get("toModel")
                or params.get("to")
            )
            if new_model:
                self.model = str(new_model)
                out.append(ev.ModelObserved(model=self.model))

        elif method == "item/plan/delta":
            out.append(ev.SystemEvent(subtype="codex_plan", data=params))

        elif method == "turn/completed":
            out.append(self._map_turn_completed(params))

        elif method == "error":
            message = self._error_message(params)
            if params.get("willRetry"):
                logger.info("codex retryable error: %s", message)
                out.append(ev.SystemEvent(
                    subtype="codex_error",
                    data={"message": message, "will_retry": True},
                ))
            else:
                # Non-retryable errors are followed by turn/completed
                # (status=failed) — remember the message for it, surface
                # a system event meanwhile.
                logger.warning("codex error: %s", message)
                self._last_error = message
                out.append(ev.SystemEvent(
                    subtype="codex_error",
                    data={"message": message, "will_retry": False},
                ))

        elif method in (
            "turn/started",
            "thread/started",
            "item/commandExecution/outputDelta",   # final output arrives on item/completed
            "item/fileChange/outputDelta",
            "item/fileChange/patchUpdated",
            "item/mcpToolCall/progress",
            "account/rateLimits/updated",
            "thread/compacted",
            "serverRequest/resolved",
        ):
            pass  # consumed elsewhere / intentionally ignored

        else:
            logger.debug("codex: unhandled notification %s", method)

        return out

    _last_error: str | None = None

    @staticmethod
    def _error_message(params: dict) -> str:
        err = params.get("error")
        if isinstance(err, dict):
            return str(err.get("message") or err)
        return str(err or params.get("message") or "unknown codex error")

    async def _map_item_started(self, item: dict) -> list[ev.AgentEvent]:
        item_id = str(item.get("id") or "")
        item_type = str(item.get("type") or "")
        if item_id:
            self._items[item_id] = item
        out: list[ev.AgentEvent] = []

        if item_type == "commandExecution":
            out.append(ev.ToolUse(
                tool_use_id=item_id or None,
                name="Bash",
                input={
                    "command": self._command_str(item),
                    "cwd": item.get("cwd"),
                },
            ))
        elif item_type == "fileChange":
            for n, change in enumerate(self._changes(item)):
                path = str(change.get("path") or "")
                # Pre-image snapshot for the diff panel. The live probe
                # showed item/started fires POST-apply on the real
                # app-server, so the disk already holds the new content —
                # reconstruct the before-text by reverse-applying the
                # change's unified diff (verification-first: a pre-apply
                # timing simply fails the reverse and the disk content IS
                # the pre-image). No diff on this event yet → defer to
                # item/completed, which carries the full changes[].
                if change.get("diff"):
                    await self._snapshot_pre_image(path, change)
                out.append(ev.ToolUse(
                    tool_use_id=self._change_id(item_id, n),
                    name="Edit",
                    input={
                        "file_path": path,
                        "kind": change.get("kind"),
                    },
                ))
        elif item_type == "mcpToolCall":
            out.append(ev.ToolUse(
                tool_use_id=item_id or None,
                name=self._mcp_tool_name(item),
                input=self._mcp_tool_input(item),
            ))
        elif item_type == "webSearch":
            out.append(ev.ToolUse(
                tool_use_id=item_id or None,
                name="WebSearch",
                input={"query": item.get("query", "")},
            ))
        elif item_type in ("plan", "planUpdate", "todoList"):
            out.append(ev.SystemEvent(subtype="codex_plan", data=item))
        # agentMessage/reasoning items stream via their delta
        # notifications; nothing to emit at start.
        return out

    async def _map_item_completed(self, item: dict) -> list[ev.AgentEvent]:
        item_id = str(item.get("id") or "")
        item_type = str(item.get("type") or "")
        started = self._items.pop(item_id, None)
        out: list[ev.AgentEvent] = []

        if item_type == "commandExecution":
            exit_code = item.get("exitCode")
            is_error = isinstance(exit_code, int) and exit_code != 0
            output = str(item.get("aggregatedOutput") or "")
            if isinstance(exit_code, int):
                output = output or ""
                output += ("" if not output or output.endswith("\n") else "\n")
                output += f"(exit code {exit_code})" if is_error else ""
            out.append(ev.ToolResult(
                tool_use_id=item_id or None,
                content=output.rstrip("\n") or "(no output)",
                is_error=is_error,
            ))
        elif item_type == "fileChange":
            failed = str(item.get("status") or "").lower() == "failed"
            changes = self._changes(item) or self._changes(started or {})
            for n, change in enumerate(changes):
                # Deferred snapshot: item/started carried no diff for this
                # path (mark_snapshotted dedupes when it did).
                await self._snapshot_pre_image(
                    str(change.get("path") or ""), change,
                )
                diff = str(change.get("diff") or "")
                out.append(ev.ToolResult(
                    tool_use_id=self._change_id(item_id, n),
                    content=diff or f"({change.get('kind') or 'change'} applied)",
                    is_error=failed,
                ))
        elif item_type == "mcpToolCall":
            status = str(item.get("status") or "").lower()
            result = item.get("result")
            if result is None:
                result = item.get("output") or item.get("error") or ""
            out.append(ev.ToolResult(
                tool_use_id=item_id or None,
                content=result if isinstance(result, str) else json.dumps(
                    result, default=str,
                ),
                is_error=status == "failed",
            ))
        elif item_type == "webSearch":
            out.append(ev.ToolResult(
                tool_use_id=item_id or None,
                content=json.dumps(
                    item.get("results", item.get("result", "")), default=str,
                )[:4000],
                is_error=False,
            ))
        # agentMessage completion: text was already streamed via deltas.
        return out

    async def _snapshot_pre_image(self, path: str, change: dict) -> None:
        """Capture the BEFORE-content of a changed file (once per path).

        Timing-agnostic (live-verified 2026-07-10, plan §17): the real
        app-server applies patches before ``item/started``, so the
        pre-image is reconstructed by reverse-applying the change's
        unified diff to the disk content; when the event beats the write
        (approval flows), the reverse fails verification and the disk
        content — which IS the pre-image — is used directly.
        """
        if not path or self._spec.snapshot is None:
            return
        hub = self._spec.interactive
        if hub is not None and not hub.mark_snapshotted(path):
            return  # already captured this session
        from nerve.agent.interactive import _read_file_safe

        kind = str(change.get("kind") or "").lower()
        content: str | None
        if kind == "add":
            content = None  # new file — no pre-image
        else:
            disk = _read_file_safe(path)
            content = disk
            diff = str(change.get("diff") or "")
            if disk is not None and diff:
                reconstructed = reverse_apply_unified_diff(diff, disk)
                if reconstructed is not None:
                    content = reconstructed
        try:
            await self._spec.snapshot(self._spec.session_id, path, content)
        except Exception as e:
            logger.warning("Snapshot failed for %s: %s", path, e)

    def _map_turn_completed(self, params: dict) -> ev.TurnCompleted:
        turn = params.get("turn") or {}
        status = str(turn.get("status") or "completed").lower()
        if status not in ("completed", "interrupted", "failed"):
            logger.warning("codex: unexpected turn status %r", status)
            status = "completed"

        error = None
        if status == "failed":
            terr = turn.get("error")
            if isinstance(terr, dict):
                error = str(terr.get("message") or terr)
            else:
                error = str(terr) if terr else (self._last_error or "turn failed")

        usage = self._normalize_usage(self._turn_usage)
        cost = compute_cost(self.model, usage, self._backend.codex.pricing)
        return ev.TurnCompleted(
            native_session_id=self._thread_id,
            model=self.model,
            usage=usage,
            total_cost_usd=cost,           # per-turn (cost_is_cumulative=False)
            duration_ms=turn.get("durationMs"),
            duration_api_ms=None,
            num_turns=1,
            context_window=self._context_window,
            status=status,  # type: ignore[arg-type]
            error=error,
        )

    @staticmethod
    def _normalize_usage(usage: dict | None) -> ev.NormalizedUsage | None:
        """``thread/tokenUsage/updated`` → NormalizedUsage.

        OpenAI's ``inputTokens`` INCLUDES the cached subset; nerve's
        Anthropic-style accounting keeps them disjoint (input = full
        price only), so the cached count is subtracted out here — the
        pricing module and the usage-dict contract both rely on it.
        """
        if not usage:
            return None
        last = usage.get("last") or {}
        if not isinstance(last, dict):
            return None
        input_tokens = int(last.get("inputTokens") or 0)
        cached = int(last.get("cachedInputTokens") or 0)
        return ev.NormalizedUsage(
            input_tokens=max(0, input_tokens - cached),
            output_tokens=int(last.get("outputTokens") or 0),
            cache_read_tokens=cached,
            cache_creation_tokens=0,
            raw={"last": last, "total": usage.get("total")},
        )

    # -- item helpers ----------------------------------------------------- #

    @staticmethod
    def _command_str(item: dict) -> str:
        command = item.get("command")
        if isinstance(command, list):
            return " ".join(str(part) for part in command)
        return str(command or "")

    @staticmethod
    def _changes(item: dict) -> list[dict]:
        changes = item.get("changes")
        if isinstance(changes, list):
            return [c for c in changes if isinstance(c, dict)]
        return []

    @staticmethod
    def _change_id(item_id: str, n: int) -> str:
        return f"{item_id}:{n}" if item_id else f"change:{n}"

    @staticmethod
    def _mcp_tool_name(item: dict) -> str:
        server = str(item.get("server") or "mcp")
        tool = str(item.get("tool") or item.get("toolName") or "call")
        # codex reports plain server/tool; render the canonical MCP id so
        # existing stats/UI paths (mcp__server__tool parsing) work.
        if tool.startswith("mcp__"):
            return tool
        return f"mcp__{server}__{tool}"

    @staticmethod
    def _mcp_tool_input(item: dict) -> dict:
        args = item.get("arguments") or item.get("input") or {}
        if isinstance(args, str):
            try:
                parsed = json.loads(args)
                return parsed if isinstance(parsed, dict) else {"arguments": args}
            except ValueError:
                return {"arguments": args}
        return args if isinstance(args, dict) else {}

    # -- approvals (server-initiated requests) ---------------------------- #

    async def _handle_server_request(self, method: str, params: dict) -> dict:
        if method in (
            "item/commandExecution/requestApproval",
            "execCommandApproval",
        ):
            return await self._approval(
                "command_approval", self._approval_payload(params),
            )
        if method in ("item/fileChange/requestApproval", "applyPatchApproval"):
            return await self._approval(
                "file_approval", self._approval_payload(params),
            )
        if method == "item/permissions/requestApproval":
            return await self._approval(
                "permission_approval", self._approval_payload(params),
            )
        if method == "item/tool/requestUserInput":
            # v1: nerve's ask_user covers interactive questions; decline
            # tool-level input requests explicitly (docs plan §7).
            logger.warning(
                "codex requested tool user input — declining (unsupported): %s",
                str(params)[:200],
            )
            return {}
        if method == "mcpServer/elicitation/request":
            logger.warning("codex MCP elicitation declined (unsupported)")
            return {}

        if method.endswith("requestApproval") or method.endswith("Approval"):
            logger.warning(
                "codex: unknown approval request %s — declining", method,
            )
            return {"decision": "decline"}
        logger.warning("codex: unknown server request %s — empty reply", method)
        return {}

    def _approval_payload(self, params: dict) -> dict:
        """Attach the originating item's context (the raw request carries
        only ids/reason — the UI wants the command / changed files)."""
        payload = dict(params)
        item_id = str(params.get("itemId") or "")
        item = self._items.get(item_id)
        if item:
            payload["item"] = item
        return payload

    async def _approval(self, kind: str, payload: dict) -> dict:
        hub = self._spec.interactive
        if hub is None:
            return {"decision": "accept"}
        outcome = await hub.request_approval(kind, payload)
        return {"decision": "accept" if outcome.approved else "decline"}

    # -- lifecycle -------------------------------------------------------- #

    async def interrupt(self) -> None:
        if self._thread_id and self._turn_id:
            try:
                await self._transport.request("turn/interrupt", {
                    "threadId": self._thread_id,
                    "turnId": self._turn_id,
                }, timeout=10.0)
            except (CodexRpcError, TransportDiedError) as e:
                logger.debug("codex interrupt failed: %s", e)

    async def disconnect(self) -> None:
        await self._transport.close()

    def is_alive(self) -> bool:
        return self._transport.is_alive()

    # -- idle stream: not supported (no autonomous turns) ------------------ #

    def try_receive_idle_events(self) -> list[ev.AgentEvent] | None:
        return None

    async def receive_idle_events(
        self, timeout: float | None,
    ) -> list[ev.AgentEvent] | None:
        return None

    def buffer_used(self) -> int:
        return 0
