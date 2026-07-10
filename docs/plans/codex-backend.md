# Codex Backend — Multi-Backend Agent Engine

**Status:** implementation plan v2 (2026-07-10; v1 revised after adversarial review — all
blockers/majors incorporated, see §16)
**Branch:** `pufit/codex-backend`
**Goal:** run Nerve sessions on OpenAI Codex (GPT‑5.6 family) with **full parity** to the
Claude path — interactive web sessions, Telegram, cron, wakeups, forks, resume — behind a
clean backend abstraction. Claude remains the default; the backend is selected by config
and **sticky per session**.

---

## 0. Ground truth (verified 2026-07-10 against codex-cli 0.144.1 exported schema)

### Nerve today

- `AgentEngine` (nerve/agent/engine.py, ~3900 lines) is hardwired to
  `claude_agent_sdk.ClaudeSDKClient`. One persistent client (= one CLI subprocess) per
  session; `client.query()` per turn; `client.receive_response()` streamed and translated
  in `_process_sdk_message()` into broadcaster events + `_TurnState` accumulation.
- Session persistence: `sessions.sdk_session_id` + SDK `resume` / `fork_session`;
  claude resume targets validated by `_sdk_resume_file_exists` (engine.py:1539) — a
  Claude-CLI-specific jsonl check.
- Interactive pausing: `can_use_tool` callback → `InteractiveToolHandler`
  (AskUserQuestion / EnterPlanMode / ExitPlanMode pause mid-turn; everything else
  auto-approved). Non-web sources auto-deny interactive tools. The handler currently
  imports `claude_agent_sdk.types.PermissionResult*`.
- Hooks: PreToolUse (file snapshots for diff view, image validation, background-agent
  permission parity), PostToolUse (ScheduleWakeup capture → nerve's wakeup sweep;
  wakeups fire back into the *same session* via `engine.run(..., source="wakeup")`).
- Tools: runtime-agnostic `ToolRegistry`/`ToolContext`/`ToolResult` with two adapters
  in-tree: in-process SDK MCP (claude_sdk_adapter.py) and the **Streamable HTTP MCP
  endpoint** (nerve/mcp_server/, `/mcp/v1`, JWT via `gateway.auth.decode_token`,
  per-request `ctx_resolver`, `SatelliteSessionResolver` for external clients).
- Cost: SDK reports *cumulative* `total_cost_usd`; `_finalize_turn` persists a
  high-water mark (`meta["_sdk_cumulative_cost"]`) and `compute_turn_cost` diffs it;
  `estimate_turn_cost` falls back to a DEFAULT_PRICING table when the model is unknown.
- Usage dict keys are Anthropic-shaped everywhere downstream: `_finalize_turn` reads
  `input_tokens` / `cache_read_input_tokens` / `cache_creation_input_tokens`, cache-ttl
  split reads `cache_creation.ephemeral_*`, and the web UI reads the same keys off the
  `done` event.
- Autonomous CLI turns: `_idle_stream_watcher` + `_drain_pending_messages` probe the
  SDK's buffered stream non-blockingly, park with timeouts, and frame turns off
  `system/init` messages — Claude-specific capability.

### Codex (codex-cli 0.144.1; `codex app-server`; schema exported via
`codex app-server generate-json-schema`)

- Long-lived subprocess, bidirectional **JSON-RPC 2.0 over stdio** (JSONL).
- Client→server (subset we use): `initialize` (clientInfo + capabilities incl.
  `optOutNotificationMethods`), `thread/start`, `thread/resume`, `thread/fork`,
  `turn/start`, `turn/interrupt` (`{threadId, turnId}`), `model/list`, `account/read`,
  `account/login/start {type: apiKey}`.
- `ThreadStartParams`: `model`, `cwd`, `sandbox` (SandboxMode:
  `read-only|workspace-write|danger-full-access`), `approvalPolicy` (AskForApproval:
  `untrusted|on-request|never` — **no `on-failure` in v2**), `baseInstructions`,
  `developerInstructions` (also available on resume/fork — needed for client rebuilds),
  `config` (free per-thread config-override dict, `additionalProperties: true`;
  `mcp_servers`, `project_doc_max_bytes` are valid keys; overrides are ignored for
  already-running threads), `ephemeral`, `personality`, `modelProvider`.
  `thread/start` response carries `thread.id` immediately.
- `TurnStartParams`: `input: [UserInput]` — text `{type:"text",text}`, images ONLY as
  `{type:"image", url}` (data: URLs ok) or `{type:"localImage", path}`; **no PDF input
  type**. Per-turn overrides: `model`, `effort` (free string, model-dependent), `cwd`,
  `approvalPolicy`, `sandboxPolicy` (SandboxPolicy object — different type from
  thread-level SandboxMode), `summary`, `outputSchema`, `personality`.
- Server→client **requests** (turn pauses until the client responds):
  `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`
  (`{itemId, reason?, grantRoot?}` — no diff payload; correlate via itemId),
  `item/permissions/requestApproval`, `item/tool/requestUserInput`,
  `mcpServer/elicitation/request`. Decisions include `accept|acceptForSession|decline|cancel`.
- Server→client **notifications** (subset): `thread/started`, `turn/started`,
  `item/started`, `item/completed`, `item/agentMessage/delta`,
  `item/reasoning/textDelta`, `item/reasoning/summaryTextDelta`,
  `item/commandExecution/outputDelta`, `item/fileChange/patchUpdated`,
  `item/mcpToolCall/progress`, `item/plan/delta`, **`thread/tokenUsage/updated`**
  (`{threadId, turnId, tokenUsage: {last, total, modelContextWindow}}` — THE usage
  source), `turn/completed` (`Turn = {id, items, status, startedAt, completedAt,
  durationMs, error}` — **no usage field**; `status ∈
  completed|interrupted|failed|inProgress`; `error: TurnError` when failed), generic
  `error {error, willRetry}`, `account/rateLimits/updated`, `thread/compacted`,
  `model/rerouted`. **There is no `turn/failed` method.**
- Item payloads: `CommandExecutionThreadItem = {command, cwd, commandActions,
  aggregatedOutput, exitCode, status...}` (no `description` field);
  `FileChangeThreadItem.changes = [{path, kind, diff}]` — an **array** of per-file
  changes with unified diffs.
- Token usage: `TokenUsageBreakdown` = input, cached_input, output, reasoning_output,
  total. **No cost-in-USD anywhere** → we price it ourselves.
- MCP server config (`RawMcpServerConfig`, confirmed in binary): stdio
  (`command/args/env`) and streamable HTTP (`url`, `http_headers`,
  `bearer_token_env_var`). MCP tool identifiers keep the `mcp__server__tool` naming, so
  existing prompt references to `mcp__nerve__*` hold under codex.
- Official Python SDK (`openai-codex` 0.1.0b2) is a thin wrapper over this protocol,
  but dispatches server-requests **synchronously on the reader thread** (a blocking
  approval handler stalls all routing incl. the `turn/interrupt` response → deadlock)
  and `AsyncCodexClient` accepts no `approval_handler` at all. Interactive pausing is
  impossible on it → **we implement our own asyncio-native app-server client**
  (~400 lines against a schema-exported protocol; zero new runtime deps).

---

## 1. Design principles

1. **One seam, two implementations.** Engine keeps everything backend-agnostic
   (session lifecycle, DB persistence, broadcasting, turn state, memorization, cron,
   locks, idle sweep, turn framing for autonomous drains). Backends own: process/client
   lifecycle, option building, native event → normalized event translation,
   permission/approval wiring, resume-target validation, usage/cost normalization.
2. **Normalize at the boundary.** Engine consumes only nerve-owned event types.
   No `claude_agent_sdk` or codex types cross the seam — including
   `InteractiveToolHandler`, whose claude-typed `can_use_tool` adapter moves into the
   claude backend (§7).
3. **Capabilities, not isinstance.** Backend differences the engine must act on
   (cumulative vs per-turn cost, idle-stream draining, cache-ttl policy, resume
   validation, context-window reporting) are declared as flags/methods on the backend.
4. **Sticky backends.** A session's backend is chosen once (at first client build),
   persisted in `sessions.backend`, and **always wins over config** afterwards.
   Wakeup/internal turns on an existing session can never cross backends.
5. **Zero behavior change for Claude.** Default config keeps `backend: claude`; the
   refactor is a pure extraction. The existing suite stays green (files that patch
   moved helpers are updated mechanically — enumerated in §12).
6. **Defensive protocol handling.** Unknown notifications → debug log; unknown
   server-requests → safe decline/empty response; missing fields never crash a turn.

---

## 2. Package layout

```
nerve/agent/backends/
    __init__.py         # re-exports + get_backend() registry
    base.py             # AgentBackend/AgentClient protocols, SessionSpec/TurnInput/BackendCapabilities
    events.py           # normalized AgentEvent union + NormalizedUsage
    claude.py           # ClaudeBackend/ClaudeClient — extraction of today's code
    codex/
        __init__.py
        appserver.py    # CodexAppServerClient: asyncio JSON-RPC stdio transport
        backend.py      # CodexBackend/CodexClient: threads, turns, event mapping
        protocol.py     # method names, param builders, notification→event mapping helpers
        pricing.py      # usage → USD from config-driven price table
```

## 3. The seam (base.py)

```python
@dataclass
class SessionSpec:
    session_id: str
    source: str                      # web | telegram | cron | wakeup | hook
    model: str | None                # explicit override; None → backend default for source
    effort: str                      # nerve vocabulary (backend maps via effort_map)
    system_prompt: str               # fully rendered nerve system prompt
    cwd: str
    resume_native_id: str | None     # backend-native session/thread id
    fork: bool                       # resume_native_id is a parent to fork from
    interactive: InteractionHub      # backend-neutral pause/approve machinery (§7)
    snapshot: SnapshotFn             # async (session_id, path, content|None) -> None
    record_wakeup: WakeupFn          # async (session_id, tool_input) -> None
    cache_ttl: str                   # claude-only; codex ignores
    max_turns: int
    idle_timeout: float              # per-message hang detection (engine-resolved, §10)
    extra: dict                      # escape hatch (betas, thinking, plugins...)

@dataclass
class TurnInput:
    text: str
    images: list[dict] | None        # engine-normalized: {media_type, data(b64)} | {path}
                                     # claude: native blocks; codex: b64→data: URL /
                                     # path→localImage. PDFs: claude native; codex —
                                     # backend returns UnsupportedInput → engine appends
                                     # a clear inline note instead (no silent drop).

class AgentClient(Protocol):
    @property
    def native_session_id(self) -> str | None: ...
        # codex: set at thread/start (immediately); claude: first stream message.
        # Engine's /stop-mid-turn persistence path reads THIS (replaces the old
        # early-capture from raw SDK messages).
    async def connect(self) -> None: ...
    async def start_turn(self, turn: TurnInput) -> None: ...
    def receive_turn(self) -> AsyncIterator[AgentEvent]: ...
        # yields until TurnCompleted; MUST also terminate on interrupted turns
    async def interrupt(self) -> None: ...
    async def disconnect(self) -> None: ...     # owns ALL process-teardown internals
                                                # (claude: today's _safe_disconnect body)
    def is_alive(self) -> bool: ...
    # -- autonomous/idle stream (capability-gated; codex: try→None, receive→raises) --
    def try_receive_idle_event(self) -> AgentEvent | None: ...     # never parks
    async def receive_idle_event(self, timeout: float) -> AgentEvent | None: ...
    def buffer_used(self) -> int: ...           # 0 when N/A

@dataclass(frozen=True)
class BackendCapabilities:
    cost_is_cumulative: bool         # claude True (engine diffs); codex False (per-turn, precomputed)
    supports_idle_stream: bool       # claude True; codex False
    supports_cache_ttl: bool         # claude True; codex False
    interactive_builtins: bool       # claude True (AskUserQuestion/plan mode)
    reports_context_window: bool     # codex True (thread/tokenUsage/updated)

class AgentBackend(Protocol):
    name: str                        # "claude" | "codex"
    capabilities: BackendCapabilities
    def default_model(self, source: str) -> str: ...
    async def create_client(self, spec: SessionSpec) -> AgentClient: ...
    def validate_resume_target(self, native_id: str, cwd: str) -> bool: ...
        # claude: today's _sdk_resume_file_exists jsonl check.
        # codex: returns True (cheap check impossible); stale ids are handled by
        # create_client falling back: thread/resume error → clear id → thread/start
        # fresh (and the engine is told the id was dropped so it clears the DB column).
    def excluded_tools(self) -> set[str]: ...   # nerve-registry tools NOT to expose
```

**Backend resolution (engine):**
1. `sessions.backend` column set → that backend, always (wakeup/internal/cron-fired
   turns on existing sessions inherit it; a missing backend implementation at runtime
   is a hard error telling the operator to restore config).
2. Unset (new session) → `agent.cron_backend` for sources `cron|hook`, else
   `agent.backend`; stamped into `sessions.backend` at first client build. `wakeup` is
   NOT a cron source (matches today's `_CRAN_EFFORT_SOURCES` treatment — wakeups always
   land on existing sessions anyway).
3. Session-metadata `backend_override` (set at creation time by API/UI later) wins over
   config for new sessions — the A/B hook.
4. Ollama guard: `ollama.enabled` + non-claude model string routes through the claude
   CLI proxy **today**; that combination therefore forces the claude backend. A codex
   backend with an ollama model is a config-validation error.

**Model resolution:** explicit `model=` argument wins; else `backend.default_model
(source)` (claude: `agent.model`/`agent.cron_model`; codex: `codex.model`/
`codex.cron_model`). `run_cron`/`run_persistent_cron`/`run_hook` **stop pre-resolving**
`self.config.agent.cron_model` at their call sites (engine.py:3657/3695/3724) and pass
the job's explicit model or None; same fix in `_get_or_create_client`'s
`requested_model` and the langfuse tag default (engine.py:2973).

## 4. Normalized events (events.py)

```python
@dataclass
class NormalizedUsage:
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    cache_creation_tokens: int       # codex: 0
    raw: dict                        # backend-native payload

    def to_anthropic_shape(self) -> dict:
        # THE usage-dict contract: engine persists/broadcasts THIS shape, so
        # _finalize_turn, extract_cache_ttl_split (absent keys → zeros), and the
        # web UI's done-event reader keep working for both backends:
        # {input_tokens, output_tokens, cache_read_input_tokens,
        #  cache_creation_input_tokens, ...raw passthrough for claude}
        # For claude the raw dict IS already this shape and is passed through
        # untouched (preserving cache_creation.ephemeral_* for the TTL split).

AgentEvent = (
    TextDelta(text, parent_tool_use_id=None)
  | ThinkingDelta(text, parent_tool_use_id=None)
  | ToolUse(tool_use_id, name, input, parent_tool_use_id=None)
  | ToolResult(tool_use_id, content, is_error, parent_tool_use_id=None)
  | SubagentStarted(tool_use_id, subagent_type, description, model)   # claude-only
  | ModelObserved(model)             # feeds _track_serving_model / st.last_model;
                                     # claude: AssistantMessage.model when
                                     # parent_tool_use_id is None; codex: resolved
                                     # thread model at turn start + model/rerouted
  | SystemEvent(subtype, data)       # claude system messages (init/task chips/workflow),
                                     # codex plan deltas, rate-limit updates
  | TurnCompleted(
        native_session_id, model,
        usage: NormalizedUsage | None,
        total_cost_usd: float | None,      # claude: cumulative; codex: THIS turn, precomputed
        duration_ms, duration_api_ms, num_turns,
        context_window: int | None,        # codex: modelContextWindow; claude: None
        status: "completed"|"interrupted"|"failed",
        error: str | None)
)
```

Engine's `_process_sdk_message` becomes `_process_agent_event(session_id, event, st)`:
same accumulation/broadcast body re-keyed on event types. One SDK message may translate
to N events (multi-block AssistantMessage). Claude translation lives in
`claude.py::translate_message()` and is unit-tested for parity (§12).

## 5. ClaudeBackend (claude.py) — pure extraction, complete inventory

Moves from engine.py / interactive.py with behavior unchanged:

- `_build_options` (system-prompt file spill, thinking/effort/betas/extra_args/
  disallowed_tools/env/plugins/mcp_servers), `_parse_thinking_config`,
  `_effective_effort`, `_model_family`, `_model_supports_legacy_enabled_thinking`,
  `_build_env` (Anthropic/Bedrock/proxy + cache-ttl env), `_build_hooks` (snapshot,
  image validation incl. `_validate_image_file`/`_validate_image_data`, wakeup capture
  → `spec.record_wakeup`, background-permission hook), the CLI stderr filter,
  **`_sdk_resume_file_exists`** (→ `validate_resume_target`), **`_safe_disconnect`**
  (→ `ClaudeClient.disconnect()`, keeping the transport/process/task-group teardown
  internals), `_is_client_dead`, `_sdk_buffer_used`, `_sdk_message_stream` (→ the
  idle-event methods; parsing via the SDK message parser stays inside the backend),
  and the `can_use_tool` adapter + `PermissionResult*` mapping from interactive.py (§7).
- `ClaudeClient.start_turn` keeps the image/document async-generator query path
  (engine.py:2993) — documents (PDFs) remain claude-native.
- In-process MCP server (claude_sdk_adapter) unchanged, now built with the backend's
  exclusion set applied.

Engine keeps: locks/semaphore, `_TurnState`, broadcasting, DB writes, title generation
(Anthropic direct client — see §10 note), memorization, cron/wakeup services, the
idle-client sweep (calls `client.disconnect()`), `_drain_pending_messages`' **turn
framing** (re-keyed on `SystemEvent("init")` / `TurnCompleted` events; the
probe/park loop uses `try_receive_idle_event` / `receive_idle_event(timeout)`), the
generic per-message timeout wrapper, and cost bookkeeping (§9).

## 6. CodexAppServerClient (codex/appserver.py)

Asyncio-native JSON-RPC client; the only transport-aware code:

- Spawn `codex app-server` via `asyncio.create_subprocess_exec` (stdin/stdout PIPE,
  stderr → severity-filtered logger). Env: isolated `CODEX_HOME` (§10), auth env,
  `NERVE_MCP_TOKEN` (§8).
- `initialize` with `clientInfo={name:"nerve",...}`; `optOutNotificationMethods` for
  surfaces we never consume (realtime/*, fuzzyFileSearch/*).
- Single reader task; dispatch: responses → pending `Future`s; notifications →
  per-turn queue + global queue; **server requests** → `asyncio.create_task(handler)`,
  reply `{id, result}` on resolution — approvals can await user input indefinitely
  without stalling the stream (this is the property the official SDK lacks).
  Unknown server-request methods → safe default (decline-shaped for `*Approval`,
  `{}` otherwise) + warning.
- `request(method, params, timeout)` / write lock on stdin; EOF → fail all pending
  with `CodexTransportError`; `is_alive()` = process poll + reader liveness. Engine's
  existing hung-client retry path is reused because `receive_turn` surfaces the same
  timeout/error shapes.
- Dicts in, dicts out; no pydantic/codegen dependency. Method names + param builders
  centralized in `protocol.py`. A schema version marker
  (`tests/fixtures/codex_schema_meta.json`) records what we verified against.

## 7. CodexBackend / CodexClient (codex/backend.py) + interaction seam

### Interaction seam (applies to both backends)

`InteractiveToolHandler` splits:
- **`InteractionHub`** (interactive.py, backend-neutral): pending/resolve/deny/cancel
  machinery, WebSocket `interaction` broadcast, `session_awaiting_input`, timeout —
  today's code minus claude types, plus `request_approval(kind, payload) ->
  ApprovalDecision` reusing the same machinery with new interaction types
  `command_approval` / `file_approval`.
- **Claude adapter** (backends/claude.py): `can_use_tool` callback translating hub
  decisions into `PermissionResultAllow/Deny` — the only place claude permission types
  live. `tests/test_interactive.py`'s type imports update accordingly.
- **Frontend** (in scope — full parity includes tightened-sandbox operation):
  extend the interaction type union (`web/src/stores/chatStore.ts`), add a generic
  `ApprovalBlock` card (command / file-change variants; file approvals render the
  correlated `item/started` payload the backend attaches — the raw request carries no
  diff), route answers through the existing `answer_interaction` WS message. Rebuild
  the frontend (`npm run build`) as the nerve-dev skill prescribes.

### Session → thread lifecycle

- `create_client(spec)`: spawn app-server (one process per nerve session — mirrors the
  claude process model so idle sweep / kill / rebuild semantics carry over; RSS cost
  measured in the smoke test and recorded here before any wide flip), `initialize`,
  then `thread/start` | `thread/resume` | `thread/fork`.
  **Resume-miss recovery:** `thread/resume`/`thread/fork` JSON-RPC error → log, report
  `resume_dropped` to the engine (clears `sessions.sdk_session_id`), `thread/start`
  fresh. Never brick a session on a wiped `~/.nerve/codex/sessions`.
- Thread params: `cwd`, `model` (resolved per §3), `developerInstructions =
  spec.system_prompt + backend notes` (see §9-prompt), `sandbox` + `approvalPolicy`
  from config (defaults `danger-full-access` + `never` = parity with claude
  auto-approve), `config` override dict: `mcp_servers` (nerve + translated external
  servers, §8), `project_doc_max_bytes: 0`, web-search toggle.
- `native_session_id` = thread id (known at `thread/start` — available to the engine's
  cancel-persistence path immediately).

### Turns

- `start_turn`: `turn/start {threadId, input:[text + images (data:-URL / localImage)],
  effort: effort_map[spec.effort], model when overridden}`; capture `turn.id`
  (interrupt needs it), subscribe the turn queue.
- `receive_turn` yields until `turn/completed` **whatever its status**:

| codex notification | AgentEvent |
|---|---|
| `item/agentMessage/delta` | `TextDelta` |
| `item/reasoning/textDelta` / `summaryTextDelta` | `ThinkingDelta` |
| `item/started {commandExecution}` | `ToolUse(id, "Bash", {command, cwd})` |
| `item/commandExecution/outputDelta` | buffered → flushed on `item/completed` as `ToolResult(aggregatedOutput, is_error = exitCode != 0)` |
| `item/started {fileChange}` | per `changes[]` entry: best-effort pre-apply `spec.snapshot(path)` then `ToolUse(f"{itemId}:{n}", "Edit", {file_path: path})` |
| `item/fileChange/patchUpdated` / `item/completed {fileChange}` | per entry: `ToolResult(f"{itemId}:{n}", diff)` — multi-file changes produce N pairs, so the diff panel gets every file |
| `item/started {mcpToolCall}` | `ToolUse(id, "mcp__<server>__<tool>", input)` |
| `item/mcpToolCall/progress` / `item/completed {mcpToolCall}` | `ToolResult` |
| `item/started|completed {webSearch}` | `ToolUse`/`ToolResult("WebSearch")` |
| `item/plan/delta` + plan items | `SystemEvent("codex_plan", ...)` (logged v1; UI chip later) |
| `thread/tokenUsage/updated` (matching turnId) | retained: `last` breakdown + `modelContextWindow` |
| `model/rerouted` | `ModelObserved(new_model)` |
| `turn/completed` | `TurnCompleted(thread_id, model, usage=retained tokenUsage → NormalizedUsage (cachedInputTokens→cache_read_tokens), total_cost_usd=pricing.compute(model, usage) — None when the model has no table entry, duration_ms=turn.durationMs, context_window, status=turn.status, error=turn.error)` — `interrupted` terminates the iterator cleanly so /stop's graceful-wait works |
| generic `error {willRetry}` | `willRetry=true` → `SystemEvent`; else raise `CodexTurnError` into the engine's existing error path |
| unknown | debug log |

Tool names reuse the Claude vocabulary ("Bash"/"Edit"/"WebSearch"/`mcp__*`) so the
existing UI (tool chips, `_maybe_broadcast_file_changed` diff panel keyed on
`input.file_path`, snapshot-vs-current diffing) works unchanged. Inline
`EditToolBlock` old/new rendering shows the panel path only — acceptable, documented.

### Approvals / interactive / wakeups

- Default config: `approval_policy: never` + full-access sandbox → no approval
  requests, parity with today. Tightened configs route
  `item/commandExecution/requestApproval` / `item/fileChange/requestApproval` /
  `item/permissions/requestApproval` → `InteractionHub.request_approval(...)`
  (auto-decline on non-interactive sources, same rule as today) → decision
  `{decision: accept|decline}`. `item/tool/requestUserInput` and
  `mcpServer/elicitation/request`: v1 auto-decline + log.
- `interrupt()` → `turn/interrupt {threadId, turnId}`.
- ScheduleWakeup: new nerve-registry tool `schedule_wakeup` (same clamping/semantics,
  handler calls `spec.record_wakeup` → identical wakeup rows; the sweep fires the
  session with its sticky backend, so no cross-backend hazard per §3). Exposure rules:
  ClaudeBackend excludes it (CLI built-in + hook already cover it); the shared HTTP MCP
  endpoint **rejects it for satellite (`source="external"`) sessions in the handler**
  (engine-run wakeups on never-engine-run sessions make no sense); prompt tool list
  (`prompts._format_tool_list`) receives the backend's exclusion set so claude prompts
  don't advertise it.

## 8. Nerve tools for codex sessions — session-bound MCP

Reuse the production Streamable HTTP endpoint:

- Per-thread `config.mcp_servers.nerve = {url: "http://127.0.0.1:<gateway_port>/mcp/v1",
  bearer_token_env_var: "NERVE_MCP_TOKEN", tool_timeout_sec: 3600}`.
  `bearer_token_env_var` confirmed supported in 0.144.1 (the existing external-agents
  writer uses `http_headers = {Authorization = "Bearer …"}` — also fine as fallback;
  final choice at implementation, env-var preferred to keep the token out of any TOML).
  If per-thread `config.mcp_servers` proves inert in practice (schema allows it), the
  fallback is writing `~/.nerve/codex/config.toml` at spawn — same content, still
  per-session because the process is per-session (verified by the fake-server test
  asserting the chosen mechanism + the real smoke test).
- **External MCP servers parity:** enabled `McpServerConfig` entries are translated
  into the same override dict (stdio `command/args/env`; http `url` +
  `http_headers`/`bearer_token_env_var`). Untranslatable entries (claude-plugin MCPs —
  those ride `options.plugins` on claude) are skipped with a warning. Claude Code
  plugins are claude-only by nature; documented in §14.
- **Session binding:** mint a per-session JWT with claims `{nerve_session_id,
  aud: "nerve-mcp", exp: none}` (revocation = gateway secret rotation, exactly the
  trust model of the existing external tokens; no 24h expiry — a busy session never
  gets swept, so a 24h token would 401 mid-conversation). Auth changes:
  `gateway.auth.decode_token` gains an `audience=` passthrough and — because PyJWT
  rejects any token carrying `aud` when the caller doesn't request one —
  `authenticate_mcp` tries aud-less first, then `aud="nerve-mcp"`; the decoded payload
  is **returned to the mount** and stashed for the request so the `ctx_resolver` can
  read `nerve_session_id` (today http.py:190 discards it). Claim present → ToolContext
  binds the real session id (notify/ask_user/memorize/task_* attribute correctly, the
  audit writer keeps working); absent → `SatelliteSessionResolver`, unchanged.
- `ask_user(wait=true)` blocks inside the tool call → works over HTTP MCP with the 1h
  `tool_timeout_sec`.
- Codex's `DynamicToolSpec`/`item/tool/call` (client-registered tools, would remove the
  HTTP hop) is not referenced from v2 thread/turn params in 0.144.1 → future work.

## 9. System prompt & prompt-cache strategy

- Nerve's rendered system prompt → **`developerInstructions`** at thread
  start/resume/fork (supported on all three — client rebuilds keep the prompt).
  `baseInstructions` (codex harness behavior) untouched.
- Backend appends a `<backend-notes>` block (not in prompts.py — it stays neutral):
  nerve tools live on the `nerve` MCP server; use `schedule_wakeup` (not
  ScheduleWakeup); AskUserQuestion/plan-mode don't exist — use `ask_user`.
- Workspace AGENTS.md would duplicate the identity bundle → thread config
  `project_doc_max_bytes: 0`.
- Prompt caching: OpenAI caches automatically (no TTL knob) → `supports_cache_ttl=False`
  skips cache_policy for codex sessions; `cachedInputTokens` maps to
  `cache_read_input_tokens` in the usage contract so diagnostics stay meaningful.

## 10. Config, model, auth, cost

```yaml
agent:
  backend: claude          # claude | codex — new interactive sessions
  cron_backend: null       # null → backend (new cron/hook sessions only; wakeups inherit)
codex:
  bin_path: codex                    # PATH-resolved; min version check (>= 0.144)
  home_dir: ~/.nerve/codex           # isolated CODEX_HOME (auth + config + sessions)
  model: gpt-5.6-sol            # verified live via model/list (see §17)
  cron_model: null                   # null → codex.model
  auth: chatgpt                      # chatgpt | api_key
  api_key: null                      # or api_key_env: OPENAI_API_KEY
  sandbox: danger-full-access        # read-only | workspace-write | danger-full-access
  approval_policy: never             # never | on-request | untrusted   (v2 protocol set)
  effort_map: {max: xhigh, xhigh: xhigh, high: high, medium: medium, low: low}
  web_search: true
  tool_timeout_sec: 3600
  turn_idle_timeout_seconds: null    # null → agent.cli_idle_timeout_seconds; engine
                                     # resolves into SessionSpec.idle_timeout
  pricing:                           # $/1M tokens; cached input billed at cached rate
    gpt-5.6-sol:    {input: 5.0,  cached_input: 0.5,  output: 30.0}   # default — MUST have an entry
    gpt-5.6-terra:  {input: 2.5,  cached_input: 0.25, output: 15.0}
    gpt-5.6-luna:   {input: 1.0,  cached_input: 0.1,  output: 6.0}
```

- **Auth:** isolated `CODEX_HOME` keeps nerve's auth/config/session state away from
  Artem's `~/.codex` (which external-agents manages and points back at nerve).
  `api_key` → backend runs `account/login/start {type: apiKey}` once per home when
  `account/read` says logged-out. `chatgpt` → one-time manual
  `CODEX_HOME=~/.nerve/codex codex login`; unauthenticated state surfaces that exact
  command in the error. Note: session **title generation** stays on the Anthropic
  direct client — in an (unlikely) Anthropic-credential-less deployment titles stay
  placeholders; documented, out of scope.
- **DB:** migration `v038_session_backend.py` — `ALTER TABLE sessions ADD COLUMN
  backend TEXT` (read default `claude`). Stamped at first client build; resume guard
  per §3.
- **Cost accounting:**
  - claude (cost_is_cumulative=True): unchanged — `_sdk_cumulative_cost` high-water
    mark + `compute_turn_cost` diff + estimate backstop.
  - codex: `TurnCompleted.total_cost_usd` is already per-turn (backend-priced;
    **None when the model isn't in the pricing table — never estimated**). Engine
    passes it directly to `record_turn_usage` and **skips** `compute_turn_cost`,
    the `_sdk_cumulative_cost` write, and `_reset_cost_baseline` (all gated on the
    capability flag).
  - Usage dict: `NormalizedUsage.to_anthropic_shape()` is what lands in
    `st.last_usage`, the DB usage row, and the `done` broadcast (§4) — web UI and
    cache-ttl split keep working; `raw` retained inside it for diagnostics.
  - Context bar: `TurnCompleted.context_window` (codex) overrides the engine's
    Anthropic-hardcoded `max_context` (engine.py:2535); claude path unchanged.
- **Observability:** Langfuse instrumentation is claude-SDK-specific
  (`configure_claude_agent_sdk`); codex turns produce usage rows + audit events but no
  LF traces in v1 — **known gap**, listed in §14.

## 11. Engine refactor (diff shape)

1. `nerve/agent/backends/` per §2 (events, base, claude extraction, codex).
2. engine.py:
   - `__init__`: build backend registry from config (claude always; codex when
     configured); `_session_backends: dict[str, str]` runtime map (SessionManager
     keeps storing the bare `AgentClient` — `stop_session`/`shutdown` call
     `interrupt()`/`disconnect()` on the protocol object, no tuples).
   - `_resolve_backend(session_row, source)` per §3 (sticky-first).
   - `_get_or_create_client`: same orchestration (recall freeze, cache_ttl now gated,
     interaction hub registration, DB stamps incl. `backend`) ending in
     `backend.create_client(spec)`; resume validation via
     `backend.validate_resume_target`; `resume_dropped` from codex clears the column.
   - `_run_inner`: `client.start_turn(TurnInput(...))` + `async for event in
     client.receive_turn()` → `_process_agent_event`; timeout wrapper unchanged
     (reads `SessionSpec.idle_timeout`); cancel path persists
     `client.native_session_id`.
   - `_process_sdk_message` → `_process_agent_event` (same body, event-keyed;
     `ModelObserved` feeds `_track_serving_model`).
   - `_drain_pending_messages`/`_idle_stream_watcher`: gated on
     `supports_idle_stream`; framing logic stays, consuming
     `try_receive_idle_event`/`receive_idle_event`.
   - Claude-specific helpers removed (moved), thin deprecation re-exports only where
     tests need a transition (§12 updates them properly instead where possible).
   - Cron/hook call sites stop pre-resolving models (§3).
3. `nerve/mcp_server/auth.py` + `http.py`: audience-aware decode, payload surfaced to
   ctx_resolver, session-claim binding (§8).
4. `nerve/agent/tools/`: `schedule_wakeup` handler + registry entry; adapters accept an
   exclusion set; HTTP endpoint rejects `schedule_wakeup` for satellite sessions;
   `prompts._format_tool_list` takes exclusions.
5. config.py: `AgentConfig.backend/cron_backend` + `CodexConfig` dataclass +
   `NerveConfig.codex` + validation (unknown backend → hard error; codex+ollama-model
   → error; approval_policy restricted to the v2 set; pricing table must cover
   `codex.model`).
6. DB migration `v038_session_backend.py`.
7. Frontend: interaction-type union + `ApprovalBlock` (command/file variants) +
   answer routing; `npm run build`.
8. docs: this plan; `docs/architecture.md` engine section; `docs/config.md` codex
   section; `docs/sdk-sessions.md` (backend column, sticky resolution, cross-backend
   guard).

## 12. Testing

New (offline; no real codex binary in CI):

- `tests/test_backend_events.py` — claude translate_message parity: text/thinking/
  tool-use/tool-result blocks, parent_tool_use_id, ModelObserved gating on sub-agent
  messages, ResultMessage → TurnCompleted (usage passthrough, cumulative cost),
  ordered_blocks/broadcast outcomes equal to the pre-refactor behavior (reusing
  existing engine-test fixtures).
- `tests/test_codex_protocol.py` — notification→event mapping per §7's table (fixtures
  shaped from the exported schema): multi-file fileChange fan-out, outputDelta
  buffering + exitCode→is_error, tokenUsage retention → NormalizedUsage mapping
  (cachedInputTokens→cache_read), turn status completed/interrupted/failed, generic
  error willRetry split, unknown-notification tolerance; pricing math incl. cached
  tokens and the unknown-model→None rule; usage `to_anthropic_shape` contract.
- `tests/test_codex_appserver.py` — **fake app-server** subprocess
  (`tests/fixtures/fake_codex_appserver.py`, scripted JSONL JSON-RPC): handshake,
  turn streaming, **async approval round-trip with interleaved deltas** (proves the
  reader never blocks — the exact deadlock the official SDK has), interrupt (incl.
  response arriving while an approval is pending), `interrupted` turn terminating
  receive_turn, resume + fork param shapes, **resume-miss → fresh-start fallback +
  resume_dropped signal**, process death mid-turn → transport error → engine retry
  path, unknown server-request safe default, mcp_servers/config-override payload
  assertion.
- `tests/test_engine_backend_selection.py` — sticky resolution (stored backend beats
  config; wakeup on claude session under `cron_backend: codex` stays claude),
  new-session config routing + `backend_override`, cross-backend guard clears native
  id, `sessions.backend` stamping, cron call sites passing model=None through,
  ollama+codex validation error, excluded-tools filtering (claude excludes
  schedule_wakeup; prompt tool list respects it).
- `tests/test_mcp_session_binding.py` — aud-less token → satellite path unchanged;
  `aud="nerve-mcp"` + session claim → real-session ToolContext; wrong aud → 401;
  schedule_wakeup rejected for satellites; audit rows carry the real session id.
- Migration test upsert into the existing migration-suite pattern (v038).
- **Updated (enumerated — mechanical, assertions preserved):** `tests/test_engine.py`
  (`_effective_effort`/`_process_sdk_message`/`_track_serving_model`/
  `_sdk_resume_file_exists` move → import from backends.claude / re-keyed on events),
  `tests/test_autonomous_turns.py` (drain fixtures move to event-shaped fakes),
  `tests/test_cache_policy.py` (`_build_env` import), `tests/test_interactive.py`
  (hub split; PermissionResult assertions move to the claude-adapter test).
- Everything else must pass untouched. Full suite green before review.

Manual/integration (not CI): `scripts/codex_smoke.py` — real app-server: auth check,
one thread, one trivial turn, assert text + usage + thread id + **RSS of the
app-server process** (recorded in this doc §17 before any wide flip), fileChange
`item/started` pre-apply ordering probe (validates the snapshot assumption; if it
fires post-apply, switch snapshots to reverse-applying the received diff — noted in
code where the fallback goes).

## 13. Risks & mitigations

| Risk | Mitigation |
|---|---|
| App-server API drift (experimental surface) | min-version check; defensive parsing; schema snapshot marker; smoke script |
| fileChange `item/started` fires post-apply → snapshot captures new content | smoke probe; fallback: reconstruct pre-image by reverse-applying the received unified diff |
| Per-thread `config.mcp_servers` override ignored | fake-server asserts what we send; smoke verifies effect; fallback: config.toml in isolated CODEX_HOME (still per-session) |
| Beta Python SDK temptation | rejected with verified reason (reader-thread approval dispatch); documented |
| Cross-backend resume corruption | sticky backend column + guard; wakeups inherit |
| Per-session JWT | aud-scoped, session-claimed, env-only, no exp (revocation = secret rotation — same trust model as existing external tokens) |
| Cost table drift | config-driven; unknown model → cost None, never estimated (codex path bypasses the estimate backstop by design) |
| App-server RSS per session | measured in smoke before flip; idle sweep already bounds live client count |
| `turn/steer` unused → messages queue during turns | same as today (per-session serialization); steering out of scope v1 |
| Plan mode / AskUserQuestion absent on codex | ask_user covers; approvals UI ships for tightened sandboxes |
| Live instance safety | worktree only; default config unchanged; no restart without Artem |

## 14. Out of scope (v1) — explicit non-parity list

- Langfuse tracing for codex turns (usage rows + audit only).
- **Permission grants** (`item/permissions/requestApproval`): the response
  type requires a constructed `GrantedPermissionProfile` with no decline
  variant — nerve answers with a JSON-RPC error, which codex treats as
  not-granted and continues sandboxed (logged). Command and file-change
  approvals ARE supported.
- Concurrent approval cards: the web UI holds one pending interaction at
  a time (pre-existing store design); a second simultaneous approval
  (possible under `untrusted`) waits server-side until the first resolves
  or times out.
- Claude Code plugin MCPs on codex (plugin lifecycle is claude-CLI-owned).
- PDF/document inputs on codex (inline note to the model instead of silent drop).
- Dynamic client-registered tools over app-server; `turn/steer`; `thread/rollback`;
  codex hooks system; realtime/voice.
- Mixed-backend forking (guard refuses).
- Codex-side thread naming/archive sync.
- `model_provider` overrides (incl. Ollama-through-codex — validation error).

## 15. Rollout

1. Merge with `backend: claude` default → zero behavior change; suite green.
2. Artem: `CODEX_HOME=~/.nerve/codex codex login` (or api_key config); run
   `scripts/codex_smoke.py`; record RSS + snapshot-ordering results in §17.
3. A/B: `backend_override` on a fresh session, or `agent.cron_backend: codex` (safe
   now — sticky resolution keeps existing sessions and their wakeups on claude).
4. Watch: usage rows (`raw`), cost attribution, audit trail, approval UX.
5. Full flip (`agent.backend: codex`) = config edit + `nerve restart` — Artem's call.

## 15b. New-chat backend selector (UI, added on request)

The composer shows a segmented **Claude / Codex** control on new (virtual)
chats: Claude in the brand-orange tint, Codex in teal, tooltips carrying
each backend's default model. The choice binds at server-side session
creation (`POST /api/sessions {backend}` → metadata `backend_override`,
validated against the engine's backend registry) and the control
disappears once the conversation starts — the header's existing model
badge then shows what the session runs on. Codex-selected chats hide the
Ollama model picker (its entries can't be served by codex). `GET
/api/models` now advertises `backends: {default, options:[{id,label,
model}]}` for the selector. Visually verified against a scratch gateway
(screenshots: codex-selector-3/4.png in the workspace).

## 16. Review log

- v1 → v2 (2026-07-10): adversarial subagent review (agent a4093b396028e149b) found 4
  blockers / 12 majors / 14 minors — all incorporated: JWT audience handling (§8),
  `_sdk_resume_file_exists`/`_safe_disconnect` into the seam (§5), sticky backend
  resolution (§3), turn-completion contract rewritten around
  `thread/tokenUsage/updated` + `turn.status` (§0/§7), ModelObserved event (§4),
  usage-shape contract (§4/§10), capability-gated cost bookkeeping + pricing entry for
  the default model (§10), SessionManager stores protocol clients (§11), external MCP
  translation (§8), cron model call sites (§3), approval frontend in scope (§7),
  InteractionHub split (§7), idle-drain contract (§3/§5), no-exp session JWT (§8),
  multi-file fileChange fan-out (§7), codex resume-miss recovery (§7), plus all minors
  (image conversion, `on-failure` removed, effort-as-string, schedule_wakeup scoping,
  timeout plumbing, langfuse/title-gen/PDF notes, context bar, `interrupted`
  termination, migration v038, test-file enumeration).

## 17. Post-implementation verification notes

**Implemented 2026-07-10** (branch `pufit/codex-backend`). Deviations and
findings vs the plan:

- **MCP config mechanism:** spawn-level `-c key=value` overrides only (the
  per-thread `config` dict path was dropped — process==session makes spawn
  scope per-session anyway, and `-c` is the mechanism the official SDKs use).
  Asserted by `test_config_overrides_carry_mcp_bridge` against the fake
  app-server's argv mirror.
- **Resume-miss recovery** implemented as a `client.resume_dropped` flag
  (not the `ResumeDroppedError` carrier exception) — the backend recovers
  internally and the engine clears the DB column on the flag.
- **`build_backends` constructs BOTH backends always** (construction is
  side-effect-free except the codex-home mkdir): a stored `backend=codex`
  session must stay resumable after config flips back to claude.
- **got_content semantics** (drain + retry gating) now key on *content
  events* rather than mere AssistantMessage arrival — an empty assistant
  message no longer opens/persists an empty autonomous turn (behavior
  delta, deliberate; documented in test updates).
- **Failed codex turns** complete the turn with an inline
  `⚠️ Turn failed: …` note + `status=failed` in result meta (engine's
  transport-death path stays reserved for actual runtime death).
- Offline verification: fake app-server suite (11 tests) proves the
  approval round-trip streams deltas while pending (the beta-SDK deadlock
  case), interrupt→`turn/completed(interrupted)` terminates `receive_turn`,
  transport death raises into the engine retry path, resume-miss falls back
  fresh, multi-file fileChange fan-out + pre-apply snapshots, usage
  normalization (cached ⊆ input split) and pricing math.
- Full pytest suite green (see PR); frontend `tsc --noEmit` + `npm run
  build` clean with the new ApprovalCard.

**Live verification (2026-07-10, `scripts/codex_smoke.py --auth api_key`,
real app-server + OpenAI API key):**

- ✅ API-key auth: `account/login/start {type: apiKey}` → threads + turns
  work end-to-end. Auth state persisted in `~/.nerve/codex/auth.json`.
- ✅ Real turn on **gpt-5.6-sol**: `SMOKE-OK`, status=completed,
  usage in=11013 / out=8, reported context window **353,400 tokens**,
  computed cost $0.055 (pricing table math verified against live usage).
- ⚠️→fixed **`gpt-5.6-codex` does not exist.** Live `model/list`:
  `gpt-5.6-sol, gpt-5.6-terra, gpt-5.6-luna, gpt-5.5, gpt-5.4,
  gpt-5.4-mini, gpt-5.2`. Default `codex.model` corrected to
  `gpt-5.6-sol`; the failed-turn path incidentally got a live test and
  behaved as designed (status=failed surfaced, transport healthy).
- ✅ **RSS: ~48 MB per app-server process** (connect and after turns) —
  cheaper than a Claude CLI subprocess; the idle sweep bounds count as
  before. No per-session memory concern.
- ⚠️→fixed **fileChange `item/started` fires POST-apply** on the real
  binary (the §13 risk, confirmed live). Implemented the reverse-diff
  fallback: `backends/codex/diffs.py::reverse_apply_unified_diff`
  reconstructs the pre-image from the change's unified diff
  (verification-first — a pre-apply timing fails the reverse and the
  disk content is used, so both timings are correct; snapshots defer to
  `item/completed` when `item/started` carries no diff yet). Re-probe:
  `final='CHANGED' snapshot-time='ORIGINAL'` ✅ — the diff panel gets a
  true before/after pair.
- Not exercised live: `mcp_servers.nerve` override effectiveness (smoke
  runs without a gateway; asserted against the fake's argv mirror — the
  first real nerve-session turn will confirm tool calls land).

**Adversarial review round 2 (2026-07-10, post-implementation subagent
review of the full branch — 3 MAJOR / 9 MINOR / 4 NIT, all addressed or
descoped explicitly):**

- MAJOR: the Claude generic-error path lost the early-captured resume id
  (crashed turns restarted conversations). Fixed: the exception handler
  now pulls `client.native_session_id` (like the cancel path) — EXCEPT
  for poisoned contexts, which must start fresh (the pre-refactor code
  re-persisted the poisoned id there; deliberately not preserved).
- MAJOR: `FileUpdateChange.kind` is a tagged object (`{"type": "add"}`)
  in the v2 schema, not a string. Fixed via `_change_kind` normalization;
  the fake app-server + tests now emit the schema shape; ApprovalCard
  renders both shapes.
- MAJOR: `item/permissions/requestApproval` reply shape was invalid —
  descoped to a JSON-RPC-error denial (see §14).
- MINOR fixes: `resume_dropped` no longer un-done by `mark_active`
  (stale local id dropped; engine-level regression test added);
  elicitation → `{"action": "decline"}` and requestUserInput →
  `{"answers": {}}` (schema-correct); legacy execCommandApproval/
  applyPatchApproval aliases removed (nerve never opts into the legacy
  API); late `turn/completed` now scoped by turn id; CRLF files
  round-trip through the reverse-diff; malformed INACTIVE codex config
  can no longer brick startup (lenient coercion + warnings);
  `codex.turn_idle_timeout_seconds` actually wired; `model/rerouted`
  reads `toModel` (the real field); realtime opt-out method names
  corrected; `_last_error` reset per turn; MCP server names validated as
  TOML key segments.
- Plan corrections (was overclaiming): ollama+codex is a load-time
  WARNING, not an error (the hazard is per-model, guarded by the claude
  path's ollama routing); `codex/protocol.py` was folded into
  `backend.py`; `tests/test_backend_events.py` coverage lives in
  test_engine.py/test_autonomous_turns.py; v038 has no dedicated
  migration test (covered by the schema-version suite).
