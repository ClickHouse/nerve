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
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time
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
from nerve.agent.backends.codex.mcp_stdio_wrapper import EXTERNAL_MCP_ENV_PREFIX
from nerve.agent.backends.codex.pricing import compute_cost
from nerve.agent.backends.codex.ultracode import (
    ensure_installed as ensure_ultracode_installed,
    installation_status as ultracode_installation_status,
    materialize_worker_wrapper,
    read_verified_run_journal,
)
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
- Native structured questions and plan updates are bridged into Nerve's UI.
  The persistent/asynchronous question tool is `mcp__nerve__ask_user`.
</backend-notes>
"""


class CodexTurnError(BackendError):
    """A codex turn failed with a non-retryable error."""


_MISSING_THREAD_RE = re.compile(
    r"(?:no rollout found for thread(?: id)?|rollout (?:was )?not found|"
    r"thread(?: id)? .{0,80}(?:not found|does not exist|expired)|unknown thread)",
    re.IGNORECASE,
)

# Ultracode workers are non-interactive and therefore cannot safely inherit
# every mutating Nerve tool under an unconditional MCP approval. Keep the
# child surface useful for context/research while making future tools fail
# closed until explicitly reviewed.
_ULTRACODE_CHILD_NERVE_TOOLS = (
    "session_context",
    "memory_recall",
    "memory_expand_category",
    "conversation_history",
    "memory_records_by_date",
    "task_search",
    "task_list",
    "task_read",
    "task_status_list",
    "plan_list",
    "plan_read",
    "skill_list",
    "skill_get",
    "skill_read_reference",
    "sync_status",
    "list_sources",
    "read_source",
)


def _is_missing_thread_error(error: CodexRpcError) -> bool:
    """Return True only for a positively identified missing thread.

    Authentication, validation, permission, transport, and protocol errors
    must remain visible.  Treating every JSON-RPC error as a cache miss loses
    conversation context silently.
    """
    detail = f"{error.message}\n{json.dumps(error.data, default=str)}"
    return bool(_MISSING_THREAD_RE.search(detail))


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
        interactive_builtins=True,
        reports_context_window=True,
    )

    def __init__(self, deps: Any):
        self._deps = deps
        self.config = deps.config
        self.codex = deps.config.codex
        Path(self._home_dir()).mkdir(parents=True, exist_ok=True)
        self._preflight_cache: tuple[float, dict[str, Any]] | None = None
        self._preflight_lock = asyncio.Lock()
        self._live_models: set[str] = set()
        self._rate_limits: dict[str, Any] | None = None

    # -- policy ---------------------------------------------------------- #

    def default_model(self, source: str) -> str:
        if source in ("cron", "hook") and self.codex.cron_model:
            return self.codex.cron_model
        return self.codex.model

    def excluded_tools(self) -> set[str]:
        return set()

    async def validate_model(self, model: str) -> None:
        """Reject obvious cross-backend model leakage before spawning Codex.

        The live app-server model list is checked again after authentication;
        this fast guard catches the frontend's historical Anthropic/Ollama
        carry-over without starting a subprocess for a doomed turn.
        """
        allowed = {
            self.codex.model,
            self.codex.cron_model or self.codex.model,
            *(self.codex.pricing or {}).keys(),
            *self._live_models,
        }
        if model in allowed:
            return
        preflight = await self.preflight()
        live = set(preflight.get("models") or []) if preflight.get("available") else set()
        if model in live:
            return
        detail = preflight.get("reason") if not preflight.get("available") else None
        suffix = f"; preflight failed: {detail}" if detail else ""
        raise BackendError(
            f"Model {model!r} is not available for the Codex backend "
            f"(available: {sorted(allowed | live)}){suffix}"
        )

    def validate_resume_target(self, native_id: str, cwd: str) -> bool:
        # No cheap filesystem check for codex threads; create_client
        # recovers from a stale id via ResumeDroppedError instead.
        return True

    def _home_dir(self) -> str:
        return os.path.expanduser(self.codex.home_dir)

    # -- client construction --------------------------------------------- #

    async def create_client(self, spec: SessionSpec) -> "CodexClient":
        await self._check_cli_version()
        if self.codex.ultracode.enabled:
            await ensure_ultracode_installed(self.config)
        client = CodexClient(self, spec)
        try:
            await client.connect()
            return client
        except BaseException:
            await client.disconnect()
            raise

    @staticmethod
    def _version_tuple(value: str) -> tuple[int, ...]:
        match = re.search(r"(\d+(?:\.\d+){1,3})", value)
        if not match:
            raise BackendError(f"Could not parse Codex CLI version from {value!r}")
        return tuple(int(part) for part in match.group(1).split("."))

    async def _check_cli_version(self) -> str:
        def _read() -> str:
            try:
                completed = subprocess.run(
                    [self.codex.bin_path, "--version"],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
            except (OSError, subprocess.SubprocessError) as e:
                raise BackendError(
                    f"Codex CLI is unavailable at {self.codex.bin_path!r}: {e}"
                ) from e
            return (completed.stdout or completed.stderr).strip()

        output = await asyncio.to_thread(_read)
        current = self._version_tuple(output)
        minimum = self._version_tuple(self.codex.min_version)
        maximum = self._version_tuple(self.codex.max_version)
        if current < minimum or current >= maximum:
            raise BackendError(
                f"Unsupported {output}; Nerve tested Codex versions "
                f">={self.codex.min_version}, <{self.codex.max_version}"
            )
        return output

    async def preflight(self, *, force: bool = False) -> dict[str, Any]:
        """Check binary/version, auth, protocol, models, MCP, and plugin state."""
        now = time.monotonic()
        if (
            not force and self._preflight_cache is not None
            and now - self._preflight_cache[0] < 60
        ):
            return dict(self._preflight_cache[1])
        async with self._preflight_lock:
            try:
                version = await self._check_cli_version()
                async def _decline(method: str, params: dict) -> dict:
                    if method.endswith("requestApproval"):
                        return {"decision": "decline"}
                    if method == "mcpServer/elicitation/request":
                        return {"action": "decline"}
                    return {}

                transport = CodexAppServerClient(
                    bin_path=self.codex.bin_path,
                    cwd=str(self.config.workspace),
                    env={**os.environ, "CODEX_HOME": self._home_dir()},
                    server_request_handler=_decline,
                    request_timeout=10,
                )
                try:
                    await transport.start()
                    account = await transport.request("account/read", {}, timeout=10)
                    if not account.get("account"):
                        raise BackendError(
                            "Codex is not authenticated. To fix: " + self.auth_hint()
                        )
                    model_result = await transport.request("model/list", {}, timeout=10)
                    try:
                        rate_result = await transport.request(
                            "account/rateLimits/read", {}, timeout=10,
                        )
                    except CodexRpcError:
                        rate_result = {}
                finally:
                    await transport.close()
                models = model_result.get("data") or model_result.get("models") or []
                model_ids = sorted({
                    str(item.get("id") or item.get("model") or item.get("slug"))
                    for item in models if isinstance(item, dict)
                    and (item.get("id") or item.get("model") or item.get("slug"))
                })
                if model_ids and self.codex.model not in model_ids:
                    raise BackendError(
                        f"Configured Codex model {self.codex.model!r} is unavailable"
                    )
                self._live_models = set(model_ids)
                rate_limits = rate_result.get("rateLimits")
                if isinstance(rate_limits, dict):
                    self._rate_limits = rate_limits
                account_obj = account.get("account") or {}
                account_type = (
                    account_obj.get("type") if isinstance(account_obj, dict) else None
                )
                effective_auth = self._normalize_auth_mode(account_type)
                plugin = None
                if self.codex.ultracode.enabled:
                    # Diagnostics must be observational. Installation/repair
                    # occurs only when a real Codex client is created.
                    plugin = ultracode_installation_status(self.config)
                result = {
                    "available": True,
                    "version": version,
                    "auth": effective_auth,
                    "configured_auth": self.codex.auth,
                    "auth_mismatch": (
                        effective_auth not in ("unknown", self.codex.auth)
                    ),
                    "account_type": account_type,
                    "models": model_ids or [self.codex.model],
                    "default_model": self.codex.model,
                    "rate_limits": self._rate_limits,
                    "ultracode": plugin,
                }
            except Exception as e:
                result = {"available": False, "reason": str(e)}
            self._preflight_cache = (time.monotonic(), result)
            return dict(result)

    @staticmethod
    def _normalize_auth_mode(value: Any) -> str:
        normalized = str(value or "").replace("_", "").replace("-", "").lower()
        if normalized in {"apikey", "api"}:
            return "api_key"
        if normalized in {"chatgpt", "chatgptauth", "oauth"}:
            return "chatgpt"
        return "unknown"

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
        # Keep external MCP credentials out of process argv. Stdio values are
        # inherited by name through ``env_vars``; HTTP headers use synthetic
        # environment names referenced by ``env_http_headers``.
        for srv in self._deps.external_mcp_servers():
            if not srv.enabled or srv.name == "nerve":
                continue
            if getattr(srv, "command", None):
                for key, value in (getattr(srv, "env", None) or {}).items():
                    if not self._TOML_KEY_RE.fullmatch(str(key)):
                        continue
                    secret_name = self._mcp_secret_env_name(
                        str(srv.name), str(key), "STDIO",
                    )
                    env[secret_name] = str(value)
            elif getattr(srv, "url", None):
                for header, value in (getattr(srv, "headers", None) or {}).items():
                    secret_name = self._mcp_secret_env_name(
                        str(srv.name), str(header), "HTTP",
                    )
                    env[secret_name] = str(value)
        if self.codex.ultracode.enabled:
            wrapper = materialize_worker_wrapper(self._home_dir())
            env["CODEX_CLI_PATH"] = str(wrapper)
            env["NERVE_CODEX_REAL_BIN"] = self.codex.bin_path
            env["ULTRACODE_NO_AUTO_UPDATE"] = "1"
            env["ULTRACODE_UI"] = "1" if self.codex.ultracode.ui else "0"
            env["ULTRACODE_TRANSPORT"] = self.codex.ultracode.default_transport
            env["ULTRACODE_MAX_CONCURRENCY"] = str(
                self.codex.ultracode.max_concurrency,
            )
            env["ULTRACODE_DEFAULT_TOKEN_BUDGET"] = str(
                self.codex.ultracode.default_token_budget,
            )
            env["ULTRACODE_MAX_AGENTS"] = str(
                self.codex.ultracode.max_agents,
            )
            env["NERVE_MCP_PARENT_SESSION_ID"] = spec.session_id
        return env

    def build_config_overrides(self, spec: SessionSpec) -> list[str]:
        """``-c key=value`` process-level config overrides.

        Process == session here, so spawn-level overrides ARE per-session
        config. This is the same mechanism the official SDKs use and is
        honored for every thread the process hosts.
        """
        overrides: list[str] = []
        # At the Nerve workspace root, AGENTS.md is the same identity bundle
        # already rendered into developerInstructions. For an explicit nested
        # project cwd, retain normal discovery so repository-local AGENTS.md
        # instructions are not lost.
        try:
            if Path(spec.cwd).resolve() == Path(self.config.workspace).resolve():
                overrides.append("project_doc_max_bytes=0")
        except OSError:
            pass

        # Nerve tools over the gateway's Streamable HTTP MCP endpoint.
        port = self._deps.gateway_port()
        if port and self._deps.mint_session_token is not None:
            base = "mcp_servers.nerve"
            # Requests to the mounted app's root must retain the trailing
            # slash.  Respect a customized endpoint path instead of silently
            # hard-coding the default /mcp/v1 mount.
            endpoint = "/" + self.config.mcp_endpoint.path.strip("/") + "/"
            url = f"http://127.0.0.1:{port}{endpoint}"
            overrides += [
                f"{base}.url={_toml_str(url)}",
                f"{base}.bearer_token_env_var={_toml_str('NERVE_MCP_TOKEN')}",
                f"{base}.required=true",
                f"{base}.startup_timeout_sec=30",
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

    _TOML_KEY_RE = re.compile(r"^[A-Za-z0-9_-]+$")

    @staticmethod
    def _mcp_secret_env_name(server: str, key: str, kind: str) -> str:
        digest = hashlib.sha256(
            f"{kind}\0{server}\0{key}".encode("utf-8"),
        ).hexdigest()[:20].upper()
        return f"{EXTERNAL_MCP_ENV_PREFIX}{kind}_{digest}"

    @classmethod
    def _translate_mcp_server(cls, srv: Any) -> list[str]:
        """Translate one nerve ``McpServerConfig`` into codex overrides."""
        if not cls._TOML_KEY_RE.match(str(srv.name)):
            raise ValueError(
                f"server name {srv.name!r} is not a bare TOML key segment"
            )
        base = f"mcp_servers.{srv.name}"
        out: list[str] = []
        command = getattr(srv, "command", None)
        url = getattr(srv, "url", None)
        if command:
            args = getattr(srv, "args", None) or []
            env = getattr(srv, "env", None) or {}
            wrapper_args: list[str] = [
                "-m", "nerve.agent.backends.codex.mcp_stdio_wrapper",
            ]
            env_names: list[str] = []
            for k in env:
                if not cls._TOML_KEY_RE.match(str(k)):
                    raise ValueError(f"env key {k!r} is not a bare TOML key")
                secret_name = cls._mcp_secret_env_name(
                    str(srv.name), str(k), "STDIO",
                )
                env_names.append(secret_name)
                wrapper_args.extend(["--env", str(k), secret_name])
            if env_names:
                wrapper_args.extend(["--", str(command), *(str(a) for a in args)])
                out.append(f"{base}.command={_toml_str(sys.executable)}")
                arr = ", ".join(_toml_str(value) for value in wrapper_args)
                out.append(f"{base}.args=[{arr}]")
            else:
                out.append(f"{base}.command={_toml_str(command)}")
                if args:
                    arr = ", ".join(_toml_str(str(a)) for a in args)
                    out.append(f"{base}.args=[{arr}]")
            if env_names:
                out.append(
                    f"{base}.env_vars="
                    + json.dumps(env_names, ensure_ascii=False)
                )
        elif url:
            out.append(f"{base}.url={_toml_str(url)}")
            headers = getattr(srv, "headers", None) or {}
            for k in headers:
                env_name = cls._mcp_secret_env_name(
                    str(srv.name), str(k), "HTTP",
                )
                out.append(
                    f'{base}.env_http_headers.{_toml_str(str(k))}='
                    f'{_toml_str(env_name)}'
                )
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
        config_overrides = backend.build_config_overrides(spec)
        env = backend.build_env(spec)
        if backend.codex.ultracode.enabled:
            # Child workers need Nerve's MCP bridge, but not the parent's
            # external MCP servers.  Besides reducing startup overhead, this
            # prevents third-party credentials from being copied into
            # NERVE_CODEX_CHILD_CONFIG and then exposed in worker argv.
            child_overrides = [
                value for value in config_overrides
                if value.startswith("mcp_servers.nerve.")
            ]
            # Ultracode is non-interactive (approval_policy=never). Explicitly
            # approve the trusted, session-scoped Nerve server or Codex marks
            # every child MCP call as "user cancelled".
            child_overrides.append(
                'mcp_servers.nerve.default_tools_approval_mode="approve"'
            )
            child_overrides.append(
                "mcp_servers.nerve.enabled_tools="
                + json.dumps(_ULTRACODE_CHILD_NERVE_TOOLS)
            )
            env["NERVE_CODEX_CHILD_CONFIG"] = json.dumps(child_overrides)
            port = backend._deps.gateway_port()
            if port:
                env["NERVE_MCP_WORKER_TOKEN_URL"] = (
                    f"http://127.0.0.1:{port}/api/codex/worker-token"
                )
        self._transport = CodexAppServerClient(
            bin_path=backend.codex.bin_path,
            cwd=spec.cwd,
            env=env,
            server_request_handler=self._handle_server_request,
            config_overrides=config_overrides,
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
        self._ultracode_usage: dict[str, int] | None = None
        self._ultracode_estimated_cost: float = 0.0
        self._ultracode_worker_count = 0
        self._ultracode_priced_worker_count = 0
        self._ultracode_runs: set[str] = set()
        self._effective_auth = "unknown"

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
        await self._validate_live_model()

        spec = self._spec
        backend = self._backend
        params = backend.thread_params(spec)

        response: dict | None = None
        if spec.resume_native_id:
            try:
                await self._transport.request(
                    "thread/read",
                    {"threadId": spec.resume_native_id, "includeTurns": False},
                )
            except CodexRpcError as e:
                if not _is_missing_thread_error(e):
                    raise
                if spec.fork:
                    raise BackendError(
                        f"Cannot fork missing Codex thread {spec.resume_native_id!r}"
                    ) from e
                logger.warning(
                    "Codex resume target %s is confirmed missing for session %s; "
                    "starting a fresh thread",
                    spec.resume_native_id[:12], spec.session_id,
                )
                self._resume_dropped = True
                response = await self._transport.request("thread/start", params)

        try:
            if response is not None:
                pass
            elif spec.resume_native_id and spec.fork:
                fork_params = {**params, "threadId": spec.resume_native_id}
                if spec.fork_last_turn_id:
                    fork_params["lastTurnId"] = spec.fork_last_turn_id
                response = await self._transport.request(
                    "thread/fork",
                    fork_params,
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
            if not spec.resume_native_id or spec.fork or not _is_missing_thread_error(e):
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

    async def _validate_live_model(self) -> None:
        """Confirm the selected model against the authenticated app-server."""
        try:
            response = await self._transport.request("model/list", {})
        except CodexRpcError as e:
            raise BackendError(
                f"Codex model discovery failed; the app-server protocol is "
                f"not compatible: {e}"
            ) from e
        models = response.get("data") or response.get("models") or []
        ids = {
            str(item.get("id") or item.get("model") or item.get("slug"))
            for item in models if isinstance(item, dict)
        }
        ids.discard("")
        if ids and self.model not in ids:
            raise BackendError(
                f"Codex model {self.model!r} is unavailable for the current "
                f"account (available: {sorted(ids)})"
            )

    async def _ensure_auth(self) -> None:
        """Best-effort auth check with a clear operator hint.

        An api-key config logs in automatically (persisted in
        CODEX_HOME/auth.json); ChatGPT auth must be done once manually.
        """
        backend = self._backend
        try:
            account = await self._transport.request("account/read", {})
        except CodexRpcError as e:
            raise BackendError(f"Codex authentication check failed: {e}") from e
        if isinstance(account, dict) and account.get("account"):
            account_obj = account.get("account") or {}
            account_type = (
                account_obj.get("type") if isinstance(account_obj, dict) else None
            )
            self._effective_auth = backend._normalize_auth_mode(account_type)
            if self._effective_auth not in ("unknown", backend.codex.auth):
                logger.warning(
                    "Codex configured auth=%s but isolated home is logged in "
                    "with %s; billing follows the effective account",
                    backend.codex.auth, self._effective_auth,
                )
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
                    self._effective_auth = "api_key"
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
        self._last_error = None
        self._items.clear()
        self._ultracode_usage = None
        self._ultracode_estimated_cost = 0.0
        self._ultracode_worker_count = 0
        self._ultracode_priced_worker_count = 0
        self._ultracode_runs.clear()
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
                local_path = att.get("path")
                if local_path:
                    notes.append(
                        "[PDF attachment preserved locally at "
                        f"{local_path}. Read or convert that file with local "
                        "tools; the app-server has no native PDF input type.]"
                    )
                else:
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
        idle_timeout = (
            float(self._backend.codex.turn_idle_timeout_seconds)
            or self._spec.idle_timeout
        )
        # The thread model is serving this turn — surface it for
        # serving-model tracking (parity with AssistantMessage.model).
        yield ev.ModelObserved(model=self.model)

        while True:
            if self._transport.notifications.empty() and not self._transport.is_alive():
                raise TransportDiedError("codex app-server died mid-turn")
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
            new_model = params.get("toModel") or params.get("model")
            if new_model:
                self.model = str(new_model)
                out.append(ev.ModelObserved(model=self.model))

        elif method == "item/plan/delta":
            out.append(ev.SystemEvent(subtype="codex_plan", data=params))

        elif method == "item/commandExecution/outputDelta":
            delta = params.get("delta") or params.get("text") or ""
            if delta:
                out.append(ev.ToolOutputDelta(
                    tool_use_id=str(params.get("itemId") or "") or None,
                    content=str(delta),
                ))

        elif method == "account/rateLimits/updated":
            if isinstance(params.get("rateLimits"), dict):
                self._backend._rate_limits = params["rateLimits"]
            out.append(ev.SystemEvent(subtype="codex_rate_limits", data=params))

        elif method in ("thread/compacted", "mcpServer/startupProgress"):
            out.append(ev.SystemEvent(subtype="codex_status", data={
                "method": method, **params,
            }))

        elif method == "turn/completed":
            turn_id = ((params.get("turn") or {}).get("id"))
            if turn_id and self._turn_id and str(turn_id) != self._turn_id:
                logger.debug(
                    "codex: dropping stale turn/completed for %s", turn_id,
                )
            else:
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
            "item/fileChange/outputDelta",
            "item/fileChange/patchUpdated",
            "item/mcpToolCall/progress",
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
                        "kind": self._change_kind(change) or None,
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
        elif item_type == "collabAgentToolCall":
            out.append(ev.SubagentStarted(
                tool_use_id=item_id or "codex-subagent",
                subagent_type=str(item.get("tool") or "Agent"),
                description=str(item.get("prompt") or "Codex subagent"),
                model=str(item.get("model")) if item.get("model") else None,
            ))
        elif item_type == "dynamicToolCall":
            out.append(ev.ToolUse(
                tool_use_id=item_id or None,
                name=str(item.get("tool") or "DynamicTool"),
                input=item.get("arguments") if isinstance(item.get("arguments"), dict) else {},
            ))
        elif "plan" in item_type.lower() or "todo" in item_type.lower():
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
            self._capture_ultracode_accounting(output)
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
                    content=diff or f"({self._change_kind(change) or 'change'} applied)",
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
        elif item_type == "collabAgentToolCall":
            status = str(item.get("status") or "").lower()
            out.append(ev.ToolResult(
                tool_use_id=item_id or None,
                content=json.dumps({
                    "receivers": item.get("receiverThreadIds") or [],
                    "agents": item.get("agentsStates") or {},
                }, default=str),
                is_error=status in {"failed", "cancelled"},
            ))
        elif item_type == "dynamicToolCall":
            status = str(item.get("status") or "").lower()
            output = self._dynamic_tool_output(item)
            if (
                str(item.get("tool") or "") == "exec"
                # Codex code-mode dynamic tools are intentionally
                # un-namespaced.  A namespaced lookalike must not be able to
                # import usage into the parent turn.
                and item.get("namespace") in (None, "")
            ):
                self._capture_ultracode_accounting(output)
            out.append(ev.ToolResult(
                tool_use_id=item_id or None,
                content=output or "(no output)",
                is_error=status == "failed" or item.get("success") is False,
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

        kind = self._change_kind(change).lower()
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
        if usage is None and self._ultracode_usage:
            # A turn can complete before app-server emits a parent tokenUsage
            # notification. Child usage is still authoritative and must not
            # disappear merely because the parent count is absent.
            usage = ev.NormalizedUsage()
        if usage is not None and self._ultracode_usage:
            child = self._ultracode_usage
            usage.input_tokens += max(
                0,
                child.get("input_tokens", 0)
                - child.get("cached_input_tokens", 0),
            )
            usage.cache_read_tokens += child.get("cached_input_tokens", 0)
            # Codex output_tokens already includes the reasoning subset (the
            # separate field is diagnostic detail, not additional billing).
            usage.output_tokens += child.get("output_tokens", 0)
            usage.raw["ultracode"] = dict(child)
        estimate = compute_cost(self.model, usage, self._backend.codex.pricing)
        if (
            estimate is not None
            and self._ultracode_worker_count > 0
            and self._ultracode_priced_worker_count == self._ultracode_worker_count
        ):
            # compute_cost above priced every child token at the parent model;
            # per-worker journal pricing is more precise, so replace that
            # approximate child component when worker model data was present.
            parent_usage = self._normalize_usage(self._turn_usage)
            parent_cost = compute_cost(
                self.model, parent_usage, self._backend.codex.pricing,
            )
            if parent_cost is not None:
                estimate = parent_cost + self._ultracode_estimated_cost
        if self._effective_auth == "api_key":
            cost = estimate
            cost_basis = "api_billed" if estimate is not None else "unknown"
            estimated_cost = None
        elif self._effective_auth == "chatgpt":
            # ChatGPT authentication consumes subscription credits/rate limits;
            # it is not an API USD charge.  Keep the optional API-equivalent
            # estimate separate so dashboards cannot present it as a bill.
            cost = None
            cost_basis = (
                "api_equivalent_estimate" if estimate is not None
                else "chatgpt_credit"
            )
            estimated_cost = estimate
        else:
            cost = None
            cost_basis = "unknown"
            estimated_cost = estimate
        return ev.TurnCompleted(
            native_session_id=self._thread_id,
            native_turn_id=self._turn_id,
            model=self.model,
            usage=usage,
            total_cost_usd=cost,           # per-turn (cost_is_cumulative=False)
            cost_basis=cost_basis,
            estimated_cost_usd=estimated_cost,
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

    @staticmethod
    def _dynamic_tool_output(item: dict[str, Any]) -> str:
        """Flatten app-server ``DynamicToolCall.contentItems`` text blocks."""
        parts: list[str] = []
        content_items = item.get("contentItems")
        if isinstance(content_items, list):
            for content in content_items:
                if not isinstance(content, dict):
                    continue
                if content.get("type") == "inputText" and isinstance(content.get("text"), str):
                    parts.append(content["text"])
                elif content.get("type") == "inputImage" and content.get("imageUrl"):
                    parts.append("[image output]")
        if parts:
            return "\n".join(parts)
        fallback = item.get("content") or item.get("result") or ""
        return fallback if isinstance(fallback, str) else json.dumps(fallback, default=str)

    def _capture_ultracode_accounting(self, output: str) -> None:
        """Import verified child usage from an Ultracode journal once per run.

        Dynamic tool outputs can wrap or truncate the CLI's final JSON.  Treat
        the output only as a run-id hint, then load the authoritative journal
        from Nerve's isolated Codex home.  This avoids trusting a worker-emitted
        lookalike aggregate and keeps malformed counters out of turn handling.
        """
        run_ids = re.findall(r'ultra-[A-Za-z0-9][A-Za-z0-9_-]{0,191}', output)
        if not run_ids:
            return
        for run_id in reversed(list(dict.fromkeys(run_ids))):
            if run_id in self._ultracode_runs:
                continue
            record = read_verified_run_journal(self._backend.config, run_id)
            if record is None:
                continue
            if str(record.get("status") or "").lower() not in {
                "completed", "failed", "cancelled", "partial", "refuted",
            }:
                continue
            aggregate = record.get("aggregate_usage")
            if not isinstance(aggregate, dict):
                continue

            keys = (
                "input_tokens", "cached_input_tokens",
                "output_tokens", "reasoning_output_tokens",
            )
            counts: dict[str, int] = {}
            valid = True
            for key in keys:
                value = aggregate.get(key, 0)
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    valid = False
                    break
                counts[key] = value
            if not valid:
                logger.warning("Ignoring malformed Ultracode usage in %s", run_id)
                continue

            self._ultracode_runs.add(run_id)
            if self._ultracode_usage is None:
                self._ultracode_usage = {key: 0 for key in keys}
            for key, value in counts.items():
                self._ultracode_usage[key] += value

            # Journals carry per-worker model + usage. Price those separately
            # when possible; ChatGPT auth still stores this only as an estimate.
            for worker in record.get("workers") or []:
                if not isinstance(worker, dict) or not isinstance(worker.get("usage"), dict):
                    continue
                raw = worker["usage"]
                worker_counts: dict[str, int] = {}
                for key in ("input_tokens", "cached_input_tokens", "output_tokens"):
                    value = raw.get(key, 0)
                    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                        worker_counts = {}
                        break
                    worker_counts[key] = value
                if not worker_counts:
                    continue
                self._ultracode_worker_count += 1
                normalized = ev.NormalizedUsage(
                    input_tokens=max(
                        0,
                        worker_counts["input_tokens"]
                        - worker_counts["cached_input_tokens"],
                    ),
                    output_tokens=worker_counts["output_tokens"],
                    cache_read_tokens=worker_counts["cached_input_tokens"],
                )
                worker_cost = compute_cost(
                    str(worker.get("model") or self.model),
                    normalized,
                    self._backend.codex.pricing,
                )
                if worker_cost is not None:
                    self._ultracode_estimated_cost += worker_cost
                    self._ultracode_priced_worker_count += 1

    # -- item helpers ----------------------------------------------------- #

    @staticmethod
    def _command_str(item: dict) -> str:
        command = item.get("command")
        if isinstance(command, list):
            return " ".join(str(part) for part in command)
        return str(command or "")

    @staticmethod
    def _change_kind(change: dict) -> str:
        """``FileUpdateChange.kind`` is a tagged object ``{"type": "add" |
        "delete" | "update"}`` in the v2 schema — normalize to the string
        (tolerating legacy/plain-string shapes)."""
        kind = change.get("kind")
        if isinstance(kind, dict):
            kind = kind.get("type")
        return str(kind or "")

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
        if method == "item/commandExecution/requestApproval":
            return await self._approval(
                "command_approval", self._approval_payload(params),
            )
        if method == "item/fileChange/requestApproval":
            return await self._approval(
                "file_approval", self._approval_payload(params),
            )
        if method == "item/permissions/requestApproval":
            # The response type requires a constructed GrantedPermissionProfile
            # — there is no decline variant to express. Unsupported in v1
            # (docs plan §14): raising here makes the transport answer with
            # a JSON-RPC error, which codex treats as not-granted and
            # continues sandboxed. Surfaced to the log so the operator
            # knows a grant was requested.
            logger.warning(
                "codex requested a permission grant — unsupported (v1), "
                "denied via JSON-RPC error: %s", str(params)[:200],
            )
            raise BackendError("permission grants are not supported by nerve v1")
        if method == "item/tool/requestUserInput":
            return await self._request_user_input(params)
        if method == "mcpServer/elicitation/request":
            return await self._request_mcp_elicitation(params)

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

    async def _request_user_input(self, params: dict) -> dict:
        hub = self._spec.interactive
        if hub is None:
            return {"answers": {}}
        raw_questions = [
            q for q in (params.get("questions") or []) if isinstance(q, dict)
        ]
        questions = []
        for question in raw_questions:
            options = question.get("options") or []
            questions.append({
                "id": str(question.get("id") or question.get("question") or ""),
                "question": str(question.get("question") or ""),
                "header": str(question.get("header") or "Question")[:12],
                "options": [
                    {
                        "label": str(option.get("label") or ""),
                        "description": str(option.get("description") or ""),
                    }
                    for option in options if isinstance(option, dict)
                ],
                "multiSelect": False,
                "freeText": not options or bool(question.get("isOther")),
                "allowOther": bool(question.get("isOther")),
                "isSecret": bool(question.get("isSecret")),
                "required": True,
            })
        if not questions:
            return {"answers": {}}
        auto_ms = params.get("autoResolutionMs")
        timeout = float(auto_ms) / 1000.0 if isinstance(auto_ms, int) and auto_ms > 0 else None
        outcome = await hub.request_interaction(
            "AskUserQuestion", {
                "questions": questions,
                "outOfBand": True,
                "message": "Codex needs your input",
            }, timeout=timeout,
        )
        if not outcome.approved or not outcome.result:
            return {"answers": {}}
        answers: dict[str, dict[str, list[str]]] = {}
        for raw, rendered in zip(raw_questions, questions, strict=False):
            value = (
                outcome.result.get(rendered["question"])
                or outcome.result.get(str(raw.get("id") or ""))
            )
            if value is None:
                continue
            labels = [part.strip() for part in str(value).split(",") if part.strip()]
            answers[str(raw.get("id") or rendered["question"])] = {"answers": labels}
        return {"answers": answers}

    async def _request_mcp_elicitation(self, params: dict) -> dict:
        """Bridge typed MCP form and URL elicitation into Nerve's UI."""
        hub = self._spec.interactive
        if hub is None:
            return {"action": "decline"}
        mode = params.get("mode")
        if mode == "url":
            url = str(params.get("url") or "")
            if not url.startswith(("https://", "http://")):
                return {"action": "decline"}
            outcome = await hub.request_interaction(
                "AskUserQuestion",
                {
                    "outOfBand": True,
                    "message": str(params.get("message") or "Complete this request"),
                    "url": url,
                    "questions": [{
                        "id": "completed",
                        "question": "Open the link, complete the flow, then continue.",
                        "header": "External",
                        "options": [{"label": "Completed", "value": "true"}],
                        "required": True,
                    }],
                },
            )
            return {"action": "accept"} if outcome.approved else {"action": "decline"}
        if mode not in ("form", "openai/form"):
            return {"action": "decline"}
        schema = params.get("requestedSchema") or {}
        properties = schema.get("properties") if isinstance(schema, dict) else None
        if not isinstance(properties, dict) or not properties:
            return {"action": "decline"}
        required = set(schema.get("required") or [])
        questions: list[dict[str, Any]] = []
        property_types: dict[str, str] = {}
        for key, prop in properties.items():
            if not isinstance(prop, dict):
                return {"action": "decline"}
            prop_type = str(prop.get("type") or "string")
            enum = prop.get("enum")
            one_of = prop.get("oneOf")
            multi = prop_type == "array"
            enum_source = prop.get("items") if multi and isinstance(prop.get("items"), dict) else prop
            if multi:
                enum = enum_source.get("enum")
                one_of = enum_source.get("oneOf")
            options: list[dict[str, str]] = []
            if isinstance(one_of, list) and one_of:
                options = [
                    {
                        "label": str(option.get("title") or option.get("const")),
                        "value": str(option.get("const")),
                    }
                    for option in one_of
                    if isinstance(option, dict) and option.get("const") is not None
                ]
            elif isinstance(enum, list) and enum:
                names = enum_source.get("enumNames") or []
                options = [
                    {
                        "label": str(names[index]) if index < len(names) else str(value),
                        "value": str(value),
                    }
                    for index, value in enumerate(enum)
                ]
            elif prop_type == "boolean":
                options = [
                    {"label": "Yes", "value": "true"},
                    {"label": "No", "value": "false"},
                ]
            elif prop_type not in ("string", "number", "integer"):
                return {"action": "decline"}
            property_types[str(key)] = prop_type
            questions.append({
                "id": str(key),
                "question": str(prop.get("description") or prop.get("title") or key),
                "header": str(prop.get("title") or key)[:12],
                "options": options,
                "multiSelect": multi,
                "freeText": not options,
                "isSecret": bool(prop.get("writeOnly")) or prop.get("format") == "password",
                "required": str(key) in required,
            })
        outcome = await hub.request_interaction(
            "AskUserQuestion", {
                "questions": questions,
                "outOfBand": True,
                "message": str(params.get("message") or "An MCP server needs input"),
            },
        )
        if not outcome.approved or not outcome.result:
            return {"action": "decline"}
        content: dict[str, Any] = {}
        for key, prop_type in property_types.items():
            value = outcome.result.get(key)
            if value is None:
                continue
            if prop_type == "boolean":
                content[key] = str(value).lower() == "true"
            elif prop_type == "integer":
                try:
                    content[key] = int(str(value))
                except ValueError:
                    return {"action": "decline"}
            elif prop_type == "number":
                try:
                    content[key] = float(str(value))
                except ValueError:
                    return {"action": "decline"}
            elif prop_type == "array":
                content[key] = [part.strip() for part in str(value).split(",") if part.strip()]
            else:
                content[key] = value
        return {"action": "accept", "content": content}

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
