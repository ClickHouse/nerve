import {
  ApiError, csrfHeaders, handleHostedError, isHosted, setTokenReader,
  type AuthMode,
} from './hosted';

const API_BASE = '/api';

/**
 * What the login form has to collect. `setup` is an install whose setup is not
 * complete: login is refused and only the setup-token claim is permitted.
 * `none` is a passwordless install by choice, which is only ever one account;
 * `password` is one account with a credential, which needs no username;
 * `username_password` is two or more.
 */
export type LoginKind = 'setup' | 'none' | 'password' | 'username_password';

/**
 * `GET /api/auth/status` — unauthenticated, so it carries no username and no
 * account count. The browser derives first-run routing from `login` itself.
 */
export interface AuthStatus {
  /** Kept for older clients; `login !== 'none'`. */
  auth_required: boolean;
  login: LoginKind;
  /** `external` is hosted Nerve behind the gateway. Absent from an older
   *  backend, which is local. */
  mode?: AuthMode;
}

/** What `POST /api/setup/claim` hands back: a session for the account it just
 *  set up, so the browser never has to re-type the password it set. */
export interface SetupClaim {
  token: string;
}

// ---------------------------------------------------------------------------
// Setup wizard (`GET /api/setup` and friends). The server derives every status
// from configuration, workspace files and the database; the browser never
// computes one. Secrets travel only as `{configured, last4}`.
// ---------------------------------------------------------------------------

/**
 * Where a configured value lives. `file` is `config.local.yaml`, written by
 * this wizard or `nerve init`; the other three were provided outside the
 * wizard — an environment variable (directly or as a `${VAR}` reference),
 * the tracked `settings.yaml` of a configuration repository, or the shared
 * channel gateway — and the page shows them as managed, with nothing to
 * replace or remove. Absent from an older backend, which means `file`.
 */
export type CredentialSource = 'file' | 'environment' | 'config-repo' | 'gateway';

/** A secret as the server reports it: whether it is set, its last four, and where it lives. */
export interface SecretOut {
  configured: boolean;
  last4: string | null;
  source?: CredentialSource | null;
}

export type SetupAuthMode = 'local' | 'external';
export type SetupMode = 'personal' | 'worker';
export type SetupStepStatus = 'done' | 'pending' | 'skipped' | 'blocked';
/**
 * Why a step is `blocked`: it waits for the Mode step (`sequencing`), the
 * instance is in lockdown, or the instance keeps no credentials on disk and
 * the step's only purpose is to store one (`policy`).
 */
export type SetupBlockedKind = 'sequencing' | 'lockdown' | 'policy';
export type SetupStepId =
  | 'claim' | 'mode' | 'provider' | 'profile' | 'memory'
  | 'channels' | 'sources' | 'automation' | 'agents';
export type SetupRestartMode = 'restart' | 'reload' | 'unsupported';

export interface SetupClaimDetail {
  username: string | null;
  passwordless: boolean;
}

/**
 * A worker's onboarding session. `ended` is a session that stopped without
 * writing a plan into TASK.md (a placeholder key, a crash): the page links
 * to it and offers to try again instead of waiting for it.
 */
export type SetupOnboarding = 'not_applicable' | 'pending' | 'running' | 'ended' | 'done';

export interface SetupModeDetail {
  mode: SetupMode | null;
  templates_present: string[];
  task_present: boolean;
  onboarding: SetupOnboarding;
  onboarding_session_id: string | null;
}

export type SetupProviderType = 'anthropic' | 'oauth' | 'host_login' | 'bedrock';
export type SetupCredential =
  | 'bedrock' | 'proxy' | 'api_key' | 'oauth_token' | 'environment' | 'none';

export interface SetupProviderDetail {
  type: SetupProviderType | string;
  credential: SetupCredential;
  /**
   * Where the effective credential came from, when the server states it
   * for the provider as a whole; otherwise it is read off the saved
   * secret's own `source` (and `credential: 'environment'`).
   */
  credential_source?: CredentialSource | null;
  anthropic_api_key: SecretOut;
  claude_oauth_token: SecretOut;
  proxy_enabled: boolean;
  aws_region: string;
  aws_profile: string;
  model: string;
  effort: string;
  cron_model: string;
  /** A short label the frontend turns into copy; `oauth_background_jobs` today. */
  warning: string | null;
  last_test: { ok: boolean; checked_at: string } | null;
}

export interface SetupProfileDetail {
  /** What the instance holds (USER.md once filled; before that the person's own name). */
  name: string | null;
  name_source: 'account' | 'actor_header' | 'user_md' | string;
  /**
   * The requesting person's own name: the gateway header in external mode,
   * the account's display name locally. Always theirs, never the shared
   * USER.md value.
   */
  actor_name?: string | null;
  display_name_editable: boolean;
  /** `["name"]` under lockdown in local mode; `null` otherwise. */
  writable_fields: string[] | null;
  timezone: string;
  timezone_is_default: boolean;
  /** `HH:MM`, or `''` for both while quiet hours are off. */
  quiet_start: string;
  quiet_end: string;
  /**
   * Whether quiet hours are on (both times set). Off by default for a
   * worker. Absent from an older backend, which always has both times.
   */
  quiet_enabled?: boolean;
  /** The quiet hours are still the mode's default: personal 02:00–08:00, worker off. */
  quiet_is_default: boolean;
  language: string | null;
  agent_name: string | null;
  agent_emoji: string | null;
  about_set: boolean;
  user_md_pristine: Record<string, boolean>;
  identity_md_pristine: Record<string, boolean>;
}

export interface SetupMemoryDetail {
  openai_api_key: SecretOut;
  embeddings_chosen: boolean;
  embed_model: string;
}

export interface SetupTelegramDetail {
  enabled: boolean;
  bot_token: SecretOut;
  bot_username: string | null;
  live: boolean;
  start_error: string | null;
  dm_policy: string;
  allowed_users_count: number;
  pairing_available: boolean;
  pairing_unavailable_reason: string | null;
}

/** The shared channel gateway's view of this instance's Slack streams (`slack.mode: hosted`). */
export interface SlackGatewayStatus {
  streams: number;
  connected: boolean;
  last_event_at: string | null;
}

export interface SetupSlackDetail {
  enabled: boolean;
  mode: 'socket' | 'hosted' | string;
  bot_token: SecretOut;
  app_token: SecretOut;
  allow_users: string[];
  allow_direct_messages: boolean;
  allow_channels: string[];
  /** Present when the channel gateway holds the connection; `null` or absent otherwise. */
  gateway?: SlackGatewayStatus | null;
}

export interface SetupChannelsDetail {
  telegram: SetupTelegramDetail;
  slack: SetupSlackDetail;
}

/** The Telegram account a message-sync session is signed in as. */
export interface TelegramSyncUser {
  id: number;
  username: string | null;
  first_name: string;
}

/**
 * What the checklist knows about the sync's user session file: whether it
 * exists, whether it is authorised (`null` when it could not be checked —
 * the sync was using it), and whose it is.
 */
export interface TelegramSyncSessionSummary {
  present: boolean;
  authorized: boolean | null;
  user: TelegramSyncUser | null;
}

export interface SetupSourcesDetail {
  github: { enabled: boolean; source?: CredentialSource | null };
  gmail: { enabled: boolean; accounts: string[]; source?: CredentialSource | null };
  telegram: {
    enabled: boolean;
    api_id: SecretOut;
    api_hash: SecretOut;
    /** Absent from an older backend, which knows nothing about the session. */
    session?: TelegramSyncSessionSummary | null;
  };
}

export interface SetupCronJob {
  id: string;
  name: string;
  description: string;
  schedule: string;
  schedule_text: string;
  requires?: string | null;
  requirement_met?: boolean;
  enabled?: boolean;
  /** Same value as `default_enabled`; the older name. */
  default?: boolean;
  /**
   * Whether the mode's first-run default turns this optional job on. A
   * worker's `task-planner` is off until its onboarding session decides.
   * Absent from an older backend.
   */
  default_enabled?: boolean;
}

export interface SetupAutomationDetail {
  file_present: boolean;
  timezone: string;
  core: SetupCronJob[];
  optional: SetupCronJob[];
}

export interface SetupAgentsDetail {
  enabled: string[];
  conflict_policy: 'backup' | 'skip' | 'merge' | string;
  available: string[];
}

export type SetupStepDetail =
  | SetupClaimDetail | SetupModeDetail | SetupProviderDetail | SetupProfileDetail
  | SetupMemoryDetail | SetupChannelsDetail | SetupSourcesDetail
  | SetupAutomationDetail | SetupAgentsDetail;

export interface SetupStep {
  id: SetupStepId;
  title: string;
  status: SetupStepStatus;
  required: boolean;
  can_skip: boolean;
  blocked_reason: string | null;
  /** Set while `status` is `blocked`; absent from an older backend. */
  blocked_kind?: SetupBlockedKind | null;
  read_only: boolean;
  read_only_reason: string | null;
  /** Hash of the persisted part of `detail`; forms remount when it changes. */
  etag: string;
  summary: string;
  files: string[];
  detail: SetupStepDetail;
}

export interface SetupState {
  generation: string;
  auth_mode: SetupAuthMode;
  mode: SetupMode | null;
  lockdown: boolean;
  /**
   * `false` on an instance that keeps no credentials on disk
   * (`setup.local_secrets`): the steps that would store one are blocked
   * with `policy`. Absent from an older backend, which means `true`.
   */
  local_secrets?: boolean;
  writable: boolean;
  read_only_reason: string | null;
  restart_pending: boolean;
  restart_reasons: string[];
  restart_mode: SetupRestartMode;
  restart_message: string;
  /**
   * The operator's command when `restart_mode` is `unsupported` (what
   * `POST /api/system/restart` would answer as `command`). Optional: a
   * backend that does not send it yet makes the Review step fall back to
   * reading the command out of `restart_message`.
   */
  restart_command?: string | null;
  finished: boolean;
  steps_left: number;
  warning: string | null;
  steps: SetupStep[];
}

export interface RestartResponse {
  mode: SetupRestartMode;
  generation: string;
  message: string;
  command: string | null;
  timeout_hint_seconds: number;
}

export interface SetupFinishResponse {
  state: SetupState;
  action: 'reloaded' | 'restart_required';
  onboarding: { status: SetupOnboarding; session_id: string | null };
}

export interface ProviderTestResult {
  ok: boolean;
  checked: 'api' | 'skipped' | string;
  code: 'ok' | 'invalid_key' | 'network' | 'model_access' | 'timeout' | 'no_cli' | 'unknown' | string;
  detail: string;
  hint: string | null;
  latency_ms: number | null;
}

export interface HostLogin {
  available: boolean;
  kind: 'oauth' | 'api_key' | string | null;
  source: string | null;
  /**
   * How the detection ended: `found`, `none` (every source was readable and
   * held no login) or `unreadable` (a source — the Keychain — could not be
   * consulted from the service). Absent from an older backend.
   */
  lookup?: 'found' | 'none' | 'unreadable' | string;
  /** Why nothing was found, in words; may carry the terminal fallback. */
  reason?: string | null;
}

export interface SetupModel {
  id: string;
  label: string;
  family: 'opus' | 'fable' | 'sonnet' | 'haiku' | 'other' | string;
  /** The instance's configured model (Nerve's default on a fresh install). */
  recommended: boolean;
}

export interface SetupModelsResponse {
  /** The configured model first, then the catalogue as the API orders it (newest first). */
  models: SetupModel[];
  current: { model: string; effort: string };
  efforts: string[];
  discovered: boolean;
  /**
   * Nerve's own default, so the picker can say so when the instance was
   * changed to something else. Absent from an older backend.
   */
  default_model?: string;
}

export interface SimpleTestResult {
  ok: boolean;
  detail: string | null;
  hint?: string | null;
}

export interface TelegramTestResult extends SimpleTestResult {
  bot_username?: string | null;
}

export interface SlackTestResult extends SimpleTestResult {
  team?: string | null;
  bot_user?: string | null;
}

export interface PairingStatus {
  available: boolean;
  reason: string | null;
  live: boolean;
  bot_username: string | null;
  link: string | null;
  allowed_users_count: number;
  code_active: boolean;
  expires_at: string | null;
}

export interface PairingCode {
  code: string;
  expires_at: string;
  bot_username: string | null;
  link: string | null;
  allowed_users_count: number;
}

export interface SourcesPreflight {
  github: { cli: boolean; authenticated: boolean; login: string | null; hint: string | null };
  gmail: { cli: boolean; hint: string | null };
  telegram: { credentials_configured: boolean };
  checked_at: string;
}

/**
 * The Telegram message sync's user session, as `GET
 * /api/setup/sources/telegram/session` reports it — and as every login
 * step except `start` answers. `configured` is the API id and hash being
 * saved; `authorized: null` means the session could not be checked —
 * `reason: "busy"` (the running sync holds the file) or `"unreachable"`
 * (Telegram did not answer the probe); other reasons: `not_configured`,
 * `no_session`, `telethon_missing`. `pending` names the login step the
 * server is waiting on: `code`, or `password` when the account has two-step
 * verification. The phone comes back masked only; the code, the password
 * and the hash never come back. The login POSTs answer `400 {detail,
 * field}` for a refused value, `429 {detail, retry_after}` for a flood
 * wait, `409 {detail}` when nothing is pending or the code expired, and
 * `502 {detail}` when Telegram could not be reached.
 */
export interface TelegramSyncSession {
  configured: boolean;
  session_present: boolean;
  authorized: boolean | null;
  user: TelegramSyncUser | null;
  pending: 'code' | 'password' | null;
  phone_masked: string | null;
  reason: 'busy' | 'unreachable' | 'not_configured' | 'no_session' | 'telethon_missing' | string | null;
}

/** `POST …/session/start`: Telegram sent a login code. */
export interface TelegramSyncLoginStarted {
  pending: 'code';
  phone_masked: string;
  expires_in: number;
}

export interface DetectedAgent {
  id: string;
  label: string;
  installed: boolean;
  version: string | null;
  config_paths: string[];
  config_exists: boolean;
  enabled: boolean;
}

export interface AgentsDetect {
  agents: DetectedAgent[];
}

export interface EnvironmentCheck {
  id: string;
  label: string;
  status: 'ok' | 'warn' | 'missing';
  detail: string | null;
  hint: string | null;
}

export interface SetupEnvironment {
  checks: EnvironmentCheck[];
  served: {
    scheme: string;
    host_is_loopback: boolean;
    client_is_loopback: boolean;
    https_hint: boolean;
  };
  bind_host: string;
  port: number;
  in_container: boolean;
  supervision: 'none' | 'systemd' | 'docker' | string;
  passwordless: boolean | null;
  restart_mode: SetupRestartMode;
  restart_message: string;
  checked_at: string;
}

export interface HealthResponse {
  status: string;
  version?: string;
  /** Boot generation; absent from an older backend. */
  generation?: string;
}

/** One local account, as `/api/accounts` returns it. Never carries a credential. */
export interface Account {
  id: string;
  /**
   * The permanent identity behind the login, and what attribution is written
   * against — a different column from `id`, and the one that outlives every
   * rename. This is how a view that has a message's author id finds a person.
   */
  actor_id: string;
  username: string | null;
  display_name: string | null;
  enabled: boolean;
  has_password: boolean;
  created_at: string;
}

/** One actor, as `/api/actors` returns it: an identity plus the names to show
 *  for it *right now*. Sessions and messages store the id, never the name, so
 *  a rename changes every label without touching a stored row. `username` is
 *  the login name of the account behind a human actor — a label falls back to
 *  it when no display name is set — and `null` for an actor without one (the
 *  agent's system principal has no account). Nothing else about the account
 *  travels with it. */
export interface ActorRef {
  id: string;
  kind: 'human' | 'system';
  display_name: string | null;
  username: string | null;
}

/** `GET /api/auth/me`: the actor this session acts as, and its local account.
 *  `account` is `null` for a credential without an account. */
export interface Viewer {
  actor: ActorRef;
  account: Account | null;
}

/** One page of a lazily-loaded sidebar group (Archived / System). */
export interface Page {
  sessions: any[];
  /** More rows exist past next_offset — the sidebar renders its '...' row. */
  has_more: boolean;
  next_offset: number;
}

export interface TaskStatusDef {
  name: string;
  label: string;
  color: string;
  description: string;
  is_system: number;
  sort_order: number;
  created_at?: string;
}

export interface TaskEvent {
  id: number;
  task_id: string;
  /** null on the row recording the task's creation. */
  from_status: string | null;
  to_status: string;
  /** Origin of the change: `web`, a session id, `system` or `backfill`. */
  actor: string;
  /** Who made the change. `null` on rows written before it was recorded. */
  actor_id: string | null;
  /** On whose behalf the change was made. `null` when unknown. */
  caused_by_actor_id: string | null;
  created_at: string;
}

export interface Task {
  id: string;
  title: string;
  status: string;
  deadline: string | null;
  source: string;
  source_url: string | null;
  tags: string;
  /** Board rank within its lane, ascending. Server-assigned. */
  position: number;
  created_at: string;
  updated_at: string;
  content?: string;
}

export interface UltracodeUsage {
  input_tokens?: number;
  cached_input_tokens?: number;
  output_tokens?: number;
  reasoning_output_tokens?: number;
  total_tokens?: number;
}

export interface UltracodeRunStep {
  id?: string;
  step_id?: string;
  index?: number;
  kind?: string;
  title?: string;
  label?: string;
  phase?: string | null;
  status?: string;
  model?: string;
  reasoning_effort?: string;
  depends_on?: string[];
  duration_ms?: number;
  usage?: UltracodeUsage;
  result?: unknown;
  value?: unknown;
  error?: string | null;
  spec?: Record<string, unknown>;
}

export interface UltracodeRunEvent {
  at?: string;
  type?: string;
  label?: string;
  phase?: string | null;
  status?: string;
  message?: string;
  worker_id?: string;
  worker_index?: number;
  schema_valid?: boolean;
  data?: Record<string, unknown>;
}

export interface UltracodeRun {
  id: string;
  name?: string | null;
  display_name?: string | null;
  slug?: string | null;
  kind?: string | null;
  status?: string;
  task?: string | null;
  cwd?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  updated_at?: string | null;
  duration_ms?: number | null;
  workers?: UltracodeRunStep[] | number;
  steps?: UltracodeRunStep[];
  events?: UltracodeRunEvent[];
  aggregate_usage?: UltracodeUsage;
  options?: Record<string, unknown>;
  result?: unknown;
  error?: string | null;
  running?: number;
  pending?: number;
  completed?: number;
  failed?: number;
  cancelled?: number;
}

// Workflow runs — budgeted autonomous agent runs.
// Mirrors public_run() in nerve/workflows/service.py (snake_case wire shape).
export interface WorkflowRunSpec {
  prompt: string;
  model?: string;
  effort?: string;
  cwd?: string;
}

export type WorkflowRunStatus =
  | 'pending'
  | 'running'
  | 'done'
  | 'failed'
  | 'killed'
  | 'budget_exhausted';

export interface WorkflowRun {
  id: string;
  engine: 'claude-workflow' | 'codex-ultracode';
  title: string;
  spec: WorkflowRunSpec;
  status: WorkflowRunStatus;
  budget_usd: number | null;
  spent_usd: number;
  warned_at: string | null;
  session_id: string | null;
  journal_dir: string | null;
  /** Origin text: `api`, `session:<id>`, `review-loop:<id>`, `cron:<job>`. */
  created_by: string;
  /** Who created the run, and on whose behalf; null on older runs. */
  created_by_actor_id?: string | null;
  caused_by_actor_id?: string | null;
  /** Who killed the run, and on whose behalf; null when not known. */
  killed_by_actor_id?: string | null;
  killed_caused_by_actor_id?: string | null;
  error: string | null;
  result: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  updated_at: string;
}

export interface WorkflowRunJournalEvent {
  ts: string;
  run_id: string;
  event: string;
  [key: string]: unknown;
}

// ── Review loops (implement→verify cycles over workflow runs) ──

export type ReviewLoopStatus =
  | 'pending'
  | 'implementing'
  | 'verifying'
  | 'awaiting_user'
  | 'passed'
  | 'failed'
  | 'killed';

export interface ReviewLoopCriterion {
  id: string;
  statement: string;
  source: 'user' | 'verifier';
  added_iteration: number;
  last_status: 'pending' | 'met' | 'unmet' | 'unverifiable';
}

export interface ReviewLoop {
  id: string;
  title: string;
  session_id: string | null;
  status: ReviewLoopStatus;
  failure_reason: string | null;
  goal_prompt: string;
  verifier_prompt: string;
  criteria_adoption: 'no' | 'ask' | 'auto';
  criteria: ReviewLoopCriterion[];
  implementer: { engine: string; model?: string; effort?: string };
  verifier: { engine: string; model?: string; effort?: string };
  cwd: string | null;
  max_iterations: number;
  budget_usd: number;
  spent_usd: number;
  iteration: number;
  current_run_id: string | null;
  created_by_actor_id?: string | null;
  caused_by_actor_id?: string | null;
  killed_by_actor_id?: string | null;
  killed_caused_by_actor_id?: string | null;
  created_at: string;
  updated_at: string;
  ended_at: string | null;
}

/**
 * Result of `POST /api/review-loops/{id}/decision`. `applied` is true when
 * the decision changed the loop; `ok` is false when the leg that it resumes
 * did not start.
 */
export interface ReviewLoopDecisionResult {
  ok: boolean;
  applied: boolean;
  message: string;
}

/** One decision on a parked loop, oldest first in the loop detail. */
export interface ReviewLoopDecision {
  id: number;
  loop_id: string;
  decision: string;
  applied: boolean;
  message: string;
  /** Origin text: `api` or `approval-card`. */
  decided_by: string;
  actor_id: string | null;
  caused_by_actor_id: string | null;
  created_at: string;
}

export interface ReviewLoopAttempt {
  id: number;
  loop_id: string;
  iteration: number;
  role: 'implementer' | 'verifier';
  attempt_no: number;
  run_id: string;
  status: string;
  spend_usd: number;
  verdict: {
    verdict?: string;
    summary?: string;
    criteria?: { id: string; status: string; evidence?: string; fix_hint?: string }[];
  } | null;
  detail: Record<string, unknown> | null;
  created_at: string;
  settled_at: string | null;
}

export interface ReviewLoopCreatePayload {
  goal: string;
  verifier: string;
  budget_usd?: number;
  max_iterations?: number;
  criteria_adoption?: 'no' | 'ask' | 'auto';
  implementer?: { engine?: string; model?: string; effort?: string };
  verifier_leg?: { engine?: string; model?: string; effort?: string };
}

export interface WorkflowRunJournal {
  run_json: Record<string, unknown> | null;
  events: WorkflowRunJournalEvent[];
  has_result: boolean;
  result: string;
}

let authToken: string | null = localStorage.getItem('nerve_token');
let tokenRevision = 0;

/** Install a token and return the revision that owns it. */
export function setToken(token: string): number {
  authToken = token;
  tokenRevision += 1;
  localStorage.setItem('nerve_token', token);
  return tokenRevision;
}

/** Clear the current token, optionally only if a caller still owns it. */
export function clearToken(expectedRevision?: number): boolean {
  if (expectedRevision !== undefined && expectedRevision !== tokenRevision) {
    return false;
  }
  authToken = null;
  tokenRevision += 1;
  localStorage.removeItem('nerve_token');
  return true;
}

export function getToken(): string | null {
  return authToken;
}

setTokenReader(getToken);

/** Response header carrying a server-refreshed session token. */
const SESSION_TOKEN_HEADER = 'X-Nerve-Token';

/**
 * Adopt a slid session token. The gateway re-mints the token once it is past
 * half its lifetime and returns it on the response, so a tab that keeps
 * talking to the server never expires — no daily re-login, no logout
 * mid-sentence.
 */
function absorbRefreshedToken(res: Response, requestRevision: number): void {
  const fresh = res.headers.get(SESSION_TOKEN_HEADER);
  if (requestRevision === tokenRevision && fresh && fresh !== authToken) {
    setToken(fresh);
  }
}

/**
 * What to do when the server says 401 in local mode. Registered by the auth
 * store so this module doesn't have to import it (that would be a cycle).
 *
 * The default is deliberately NOT `window.location.reload()`: a reload is how
 * an expired token used to destroy whatever you were typing. Any background
 * poll could trip it, the page would blow away mid-keystroke, and an unsent
 * draft in a *new* chat — whose session id lives only in memory — was
 * orphaned and then swept by pruneDrafts. The app now stays mounted and asks
 * for the password over the top of your work.
 */
let onUnauthorized: (() => void) | null = null;

export function setUnauthorizedHandler(handler: () => void): void {
  onUnauthorized = handler;
}

function handleUnauthorized(requestRevision: number): Error {
  // A response belongs to the token revision that sent it. A logout, login,
  // or accepted slide makes older 401s informational rather than authority to
  // clear the new session or put its UI into the expiry state.
  if (clearToken(requestRevision)) onUnauthorized?.();
  return new Error('Unauthorized');
}

/**
 * Send one API request with the credentials of the current mode. Every API
 * `fetch` goes through here.
 *
 * Local mode sends the session token as a bearer and adopts a slid token.
 * Hosted mode sends no bearer, because the gateway treats every bearer as an
 * API token, and the cookie of the gateway session goes with the request. An
 * unsafe method gets the CSRF header that the gateway requires. A failed
 * response goes to `handleHostedError`, which also sets hosted mode when the
 * body is a gateway error.
 */
async function send(
  path: string, init: RequestInit, headers: Record<string, string>, authenticate = true,
): Promise<Response> {
  const hosted = isHosted();
  const requestToken = hosted || !authenticate ? null : authToken;
  const requestRevision = tokenRevision;
  if (requestToken) {
    headers['Authorization'] = `Bearer ${requestToken}`;
  }
  if (hosted) Object.assign(headers, csrfHeaders(init.method));

  const res = await fetch(`${API_BASE}${path}`, { ...init, headers });

  if (res.ok) {
    if (!hosted && authenticate) absorbRefreshedToken(res, requestRevision);
    return res;
  }

  const error = new ApiError(res.status, await res.text());
  if (handleHostedError(error)) throw error;
  if (res.status === 401 && authenticate) throw handleUnauthorized(requestRevision);
  if (authenticate) absorbRefreshedToken(res, requestRevision);
  throw error;
}

async function request<T>(path: string, options: RequestInit = {}, authenticate = true): Promise<T> {
  const res = await send(path, options, {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string> || {}),
  }, authenticate);
  return res.json();
}

/**
 * `GET /health` — not under `/api`, unauthenticated, and deliberately a raw
 * `fetch`: during a restart it answers with connection errors and 5xx, and
 * none of those may reach the 401 handler or the hosted re-entry path.
 */
async function health(): Promise<HealthResponse> {
  const res = await fetch('/health', { cache: 'no-store' });
  if (!res.ok) throw new ApiError(res.status, await res.text());
  return res.json() as Promise<HealthResponse>;
}

/** Consecutive `/health` answers that carried no generation (not 5xx). */
let healthMisses = 0;
const HEALTH_MISSES_BEFORE_FALLBACK = 2;

/**
 * The boot generation of whatever process is answering right now, or `null`
 * while nothing usable answers (connection refused, 502/503/504, a gateway
 * page). A front gateway that does not forward `/health` answers it with a
 * 404, a 401/403 from its own auth wall, a 405, or an HTML page; after two
 * such answers in a row this reads the generation from a raw `GET /api/setup`
 * instead, with the gateway cookie and CSRF header — still outside
 * `request()`, so a 401 mid-restart is just "not yet", not a sign-out.
 *
 * Only a network error or a 5xx means "still restarting". Every other answer
 * without a JSON `generation` is a miss: Nerve's own `/health` is
 * unauthenticated and always JSON, so anything else is something in front of
 * it, and the fallback is the only path that reaches the new process there.
 */
/**
 * A per-request deadline for the probe. During a restart a half-open
 * connection would otherwise hang one poll for the browser's default (minutes),
 * and the store's time-box could not fire. A timed-out request rejects and
 * counts as "still restarting". Older runtimes without `AbortSignal.timeout`
 * get an unbounded request, as before.
 */
const HEALTH_PROBE_TIMEOUT_MS = 5000;
function probeSignal(): AbortSignal | undefined {
  return typeof AbortSignal !== 'undefined' && typeof AbortSignal.timeout === 'function'
    ? AbortSignal.timeout(HEALTH_PROBE_TIMEOUT_MS)
    : undefined;
}

async function healthGeneration(): Promise<string | null> {
  if (healthMisses < HEALTH_MISSES_BEFORE_FALLBACK) {
    let res: Response;
    try {
      res = await fetch('/health', { cache: 'no-store', signal: probeSignal() });
    } catch {
      return null;  // network or the deadline: still restarting
    }
    if (res.status >= 500) {
      return null;  // 5xx: still restarting
    } else if (!res.ok) {
      healthMisses += 1;  // 401/403/404/405…: a gateway answering for Nerve
    } else {
      try {
        const body = await res.json() as HealthResponse;
        healthMisses = 0;
        return typeof body.generation === 'string' ? body.generation : null;
      } catch {
        healthMisses += 1;  // a 200 that is not JSON: a gateway page
      }
    }
    if (healthMisses < HEALTH_MISSES_BEFORE_FALLBACK) return null;
  }
  try {
    const headers: Record<string, string> = { Accept: 'application/json' };
    if (isHosted()) Object.assign(headers, csrfHeaders('GET'));
    else if (authToken) headers['Authorization'] = `Bearer ${authToken}`;
    const res = await fetch(`${API_BASE}/setup`, {
      credentials: 'same-origin', headers, cache: 'no-store', signal: probeSignal(),
    });
    if (!res.ok) return null;
    const body = await res.json() as { generation?: unknown };
    return typeof body.generation === 'string' ? body.generation : null;
  } catch {
    return null;
  }
}

export const api = {
  // Auth
  /**
   * `username` is optional while exactly one account exists and required once
   * there are two or more: the account an upgrade created has none, so the
   * server accepts a password on its own while there is exactly one. A username
   * that does resolve is accepted at any time. Which shape to collect is what
   * `authStatus().login` says.
   */
  login: (password: string, username?: string) =>
    request<{ token: string }>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ password, username: username?.trim() || null }),
    }),

  checkAuth: () => request<{ authenticated: boolean }>('/auth/check'),

  // Mode discovery must work even when a local JWT is left in storage on a
  // now-hosted origin. A bearer would override the valid gateway cookie.
  authStatus: () => request<AuthStatus>('/auth/status', {}, false),

  /**
   * Who this session acts as. Doubles as the authentication check at startup:
   * it needs a valid session *and* answers which actor and account the session
   * belongs to. Attribution compares authors with the actor; re-authentication
   * is bound to the account.
   */
  getViewer: () => request<Viewer>('/auth/me'),

  // The one unauthenticated write. The mandatory setup token is sent only
  // in this JSON body and is never retained by the API client. Send either
  // `password` or `passwordless: true`.
  setupClaim: (body: {
    username?: string; password?: string; passwordless?: boolean;
    setup_token: string; display_name?: string;
  }) => request<SetupClaim>('/setup/claim', {
    method: 'POST', body: JSON.stringify(body),
  }),

  // Setup wizard. Every call goes through `request()` so it carries the
  // bearer locally and the gateway cookie plus CSRF header when hosted; none
  // of these ever builds a `?token=` URL.
  setupState: () => request<SetupState>('/setup'),

  /** `PUT /api/setup/{step}` — patch semantics; answers the whole state. */
  setupStep: (step: SetupStepId, body: Record<string, unknown>) =>
    request<SetupState>(`/setup/${encodeURIComponent(step)}`, {
      method: 'PUT', body: JSON.stringify(body),
    }),

  setupSkip: (step: SetupStepId, skipped: boolean) =>
    request<SetupState>(
      `/setup/steps/${encodeURIComponent(step)}/${skipped ? 'skip' : 'unskip'}`,
      { method: 'POST', body: '{}' },
    ),

  setupFinish: () =>
    request<SetupFinishResponse>('/setup/finish', { method: 'POST', body: '{}' }),

  setupProviderTest: (body: Record<string, unknown>) =>
    request<ProviderTestResult>('/setup/provider/test', {
      method: 'POST', body: JSON.stringify(body),
    }),

  setupHostLogin: (refresh = false) =>
    request<HostLogin>(`/setup/provider/host-login${refresh ? '?refresh=1' : ''}`),

  setupModels: () => request<SetupModelsResponse>('/setup/models'),

  setupMemoryTest: (body: { openai_api_key: string }) =>
    request<SimpleTestResult>('/setup/memory/test', {
      method: 'POST', body: JSON.stringify(body),
    }),

  setupTelegramTest: (body: { bot_token: string }) =>
    request<TelegramTestResult>('/setup/channels/telegram/test', {
      method: 'POST', body: JSON.stringify(body),
    }),

  setupSlackTest: (body: { bot_token: string; app_token: string }) =>
    request<SlackTestResult>('/setup/channels/slack/test', {
      method: 'POST', body: JSON.stringify(body),
    }),

  /** Pure read: never carries the code. */
  setupPairing: () => request<PairingStatus>('/setup/channels/telegram/pairing'),

  /** Issues a code; the only response that carries it. */
  setupPairingStart: () =>
    request<PairingCode>('/setup/channels/telegram/pairing', {
      method: 'POST', body: '{}',
    }),

  setupSourcesPreflight: () => request<SourcesPreflight>('/setup/sources/preflight'),

  // Telegram message sync: the one-time sign-in of the person's own account,
  // done on the server (the session file never leaves it). The phone, the
  // code and the password travel in the JSON body only.
  setupTelegramSession: () => request<TelegramSyncSession>('/setup/sources/telegram/session'),
  setupTelegramSessionStart: (body: { phone: string }) =>
    request<TelegramSyncLoginStarted>('/setup/sources/telegram/session/start', {
      method: 'POST', body: JSON.stringify(body),
    }),
  setupTelegramSessionCode: (body: { code: string }) =>
    request<TelegramSyncSession>('/setup/sources/telegram/session/code', {
      method: 'POST', body: JSON.stringify(body),
    }),
  setupTelegramSessionPassword: (body: { password: string }) =>
    request<TelegramSyncSession>('/setup/sources/telegram/session/password', {
      method: 'POST', body: JSON.stringify(body),
    }),
  setupTelegramSessionCancel: () =>
    request<TelegramSyncSession>('/setup/sources/telegram/session/cancel', { method: 'POST', body: '{}' }),
  setupTelegramSessionLogout: () =>
    request<TelegramSyncSession>('/setup/sources/telegram/session/logout', { method: 'POST', body: '{}' }),

  setupAgentsDetect: () => request<AgentsDetect>('/setup/agents/detect'),

  setupEnvironment: () => request<SetupEnvironment>('/setup/environment'),

  restartSystem: () =>
    request<RestartResponse>('/system/restart', { method: 'POST', body: '{}' }),

  health,
  healthGeneration,

  // Accounts
  listAccounts: () => request<{ accounts: Account[] }>('/accounts'),

  createAccount: (body: { username: string; password: string; display_name?: string }) =>
    request<Account>('/accounts', { method: 'POST', body: JSON.stringify(body) }),

  updateAccount: (id: string, body: { username?: string; display_name?: string }) =>
    request<Account>(`/accounts/${encodeURIComponent(id)}`, {
      method: 'PATCH',
      body: JSON.stringify(body),
    }),

  setAccountEnabled: (id: string, enabled: boolean) =>
    request<Account>(
      `/accounts/${encodeURIComponent(id)}/${enabled ? 'enable' : 'disable'}`,
      { method: 'POST' },
    ),

  changeOwnPassword: (body: { current_password?: string; new_password: string }) =>
    request<Account>('/accounts/me/password', {
      method: 'PUT',
      body: JSON.stringify(body),
    }),

  // Actors — resolve the ids stored on sessions and messages to names.
  // One row per person plus the system principal, so a single fetch labels a
  // whole list; re-fetch after a rename rather than caching a name anywhere.
  listActors: () => request<{ actors: ActorRef[] }>('/actors'),

  // Models — chat models offered to the composer's picker, per backend
  // (the configured Claude list, Codex app-server models, and any
  // locally-installed Ollama models, auto-discovered server-side).
  getModels: () =>
    request<{
      default: string;
      defaults?: Record<string, string>;
      backends?: {
        default: string;
        options: { id: string; label: string; model: string; models?: string[]; available?: boolean; reason?: string }[];
        diagnostics?: Record<string, { available: boolean; reason?: string }>;
      };
      models: { id: string; provider: string; backend: string }[];
      ollama: { enabled: boolean; routable: boolean; available: boolean };
    }>('/models'),

  // Sessions
  listSessions: (offset = 0) =>
    request<{ sessions: any[]; archived_count: number; system_count: number; has_more: boolean; next_offset: number }>(
      `/sessions?offset=${offset}`),
  listArchivedSessions: (offset = 0) =>
    request<Page>(`/sessions/archived?offset=${offset}`),
  listSystemSessions: (offset = 0) =>
    request<Page>(`/sessions/system?offset=${offset}`),
  searchSessions: (q: string) =>
    request<{ sessions: any[] }>(`/sessions/search?q=${encodeURIComponent(q)}`),
  getSession: (id: string) => request<any>(`/sessions/${id}`),
  createSession: (title?: string, backend?: string | null, cwd?: string | null, reviewLoop?: ReviewLoopCreatePayload | null, model?: string | null) =>
    request<any>('/sessions', {
      method: 'POST',
      body: JSON.stringify({
        title,
        ...(backend ? { backend } : {}),
        // Composer's model pick — persisted on the session row at creation
        // so the header badge is right from the first render (omitted →
        // the backend's default model).
        ...(model ? { model } : {}),
        ...(cwd ? { cwd } : {}),
        ...(reviewLoop ? { review_loop: reviewLoop } : {}),
      }),
    }),
  // Run later — defer a composed prompt into a (freshly created) session
  // without spending tokens now. Persists the prompt as the pending user
  // message + a synthetic ack; timed delays schedule a one-shot wakeup.
  runLater: (
    sessionId: string,
    message: string,
    delay: string,
    fileIds?: string[],
    imageBlocks?: Array<{ url: string; filename: string; media_type: string }>,
  ) =>
    request<{ session_id: string; scheduled: boolean; ack: string; fire_at: string | null }>(
      '/sessions/run-later',
      {
        method: 'POST',
        body: JSON.stringify({
          session_id: sessionId,
          message,
          delay,
          ...(fileIds && fileIds.length ? { file_ids: fileIds } : {}),
          ...(imageBlocks && imageBlocks.length ? { image_blocks: imageBlocks } : {}),
        }),
      },
    ),
  listReviewLoops: (status?: string) =>
    request<{ loops: ReviewLoop[] }>(`/review-loops${status ? `?status=${status}` : ''}`),
  getReviewLoop: (loopId: string) =>
    request<{ loop: ReviewLoop; attempts: ReviewLoopAttempt[]; decisions: ReviewLoopDecision[] }>(
      `/review-loops/${loopId}`,
    ),
  killReviewLoop: (loopId: string, reason = '') =>
    request<ReviewLoop>(`/review-loops/${loopId}/kill`, {
      method: 'POST', body: JSON.stringify({ reason }),
    }),
  decideReviewLoop: (loopId: string, decision: string) =>
    request<ReviewLoopDecisionResult>(`/review-loops/${loopId}/decision`, {
      method: 'POST', body: JSON.stringify({ decision }),
    }),
  getReviewLoopState: (loopId: string) =>
    request<{ exists: boolean; truncated: boolean; content: string; path: string }>(
      `/review-loops/${loopId}/state`,
    ),
  deleteSession: (id: string) =>
    request<any>(`/sessions/${id}`, { method: 'DELETE' }),
  updateSession: (id: string, data: { title?: string; starred?: boolean; model?: string; parent_session_id?: string | null }) =>
    request<any>(`/sessions/${id}`, { method: 'PATCH', body: JSON.stringify(data) }),
  getMessages: (sessionId: string, limit = 100) =>
    request<{ messages: any[]; last_usage?: { input_tokens: number; output_tokens: number; cache_creation_input_tokens: number; cache_read_input_tokens: number; cache_creation?: { ephemeral_5m_input_tokens?: number; ephemeral_1h_input_tokens?: number }; max_context_tokens: number; num_turns?: number } }>(`/sessions/${sessionId}/messages?limit=${limit}`),
  forkSession: (sourceSessionId: string, atMessageId?: string, title?: string) =>
    request<any>('/sessions/fork', {
      method: 'POST',
      body: JSON.stringify({ source_session_id: sourceSessionId, at_message_id: atMessageId, title }),
    }),
  resumeSession: (id: string) =>
    request<any>(`/sessions/${id}/resume`, { method: 'POST' }),
  archiveSession: (id: string) =>
    request<any>(`/sessions/${id}/archive`, { method: 'POST' }),
  unarchiveSession: (id: string) =>
    request<any>(`/sessions/${id}/unarchive`, { method: 'POST' }),
  getSessionStatus: (id: string) =>
    request<any>(`/sessions/${id}/status`),
  getSessionEvents: (id: string, limit = 50) =>
    request<{ events: any[] }>(`/sessions/${id}/events?limit=${limit}`),

  // Chat (non-streaming)
  chat: (message: string, sessionId?: string) =>
    request<{ response: string; session_id: string }>('/chat', {
      method: 'POST',
      body: JSON.stringify({ message, ...(sessionId && { session_id: sessionId }) }),
    }),

  // Tasks
  listTasks: (params?: { status?: string; tag?: string; sort?: string; limit?: number; offset?: number }) => {
    const qs = new URLSearchParams();
    if (params?.status) qs.set('status', params.status);
    if (params?.tag) qs.set('tag', params.tag);
    if (params?.sort) qs.set('sort', params.sort);
    if (params?.limit !== undefined) qs.set('limit', String(params.limit));
    if (params?.offset !== undefined) qs.set('offset', String(params.offset));
    const q = qs.toString();
    return request<{ tasks: any[]; total: number; limit: number; offset: number }>(
      `/tasks${q ? '?' + q : ''}`,
    );
  },
  searchTasks: (query: string, status?: string) => {
    const qs = new URLSearchParams({ q: query });
    if (status) qs.set('status', status);
    return request<{ tasks: any[]; total: number; limit: number; offset: number }>(
      `/tasks/search?${qs}`,
    );
  },
  /** Every board lane in one round trip. */
  getTaskBoard: (params?: { tag?: string; limit?: number; q?: string }) => {
    const qs = new URLSearchParams();
    if (params?.tag) qs.set('tag', params.tag);
    if (params?.q) qs.set('q', params.q);
    if (params?.limit !== undefined) qs.set('limit', String(params.limit));
    const q = qs.toString();
    return request<{
      statuses: TaskStatusDef[];
      lanes: { status: string; total: number; tasks: Task[] }[];
      /** task_id → ISO time it entered its current status. Absent for
          tasks whose last transition predates the history table. */
      status_since: Record<string, string>;
    }>(`/tasks/board${q ? '?' + q : ''}`);
  },
  listTaskTags: (includeDone = false) =>
    request<{ tags: { name: string; count: number }[] }>(
      `/tasks/tags${includeDone ? '?include_done=true' : ''}`,
    ),
  /**
   * Move a task within or between lanes. Sends the neighbours it should
   * land between rather than a rank — the server computes the ordering,
   * so a board a few seconds stale can't write a conflicting position.
   */
  moveTask: (id: string, data: { status?: string; before_id?: string | null; after_id?: string | null }) =>
    request<{ task: Task }>(`/tasks/${id}/move`, {
      method: 'POST',
      body: JSON.stringify(data),
    }),
  listTaskEvents: (id: string) =>
    request<{ events: TaskEvent[] }>(`/tasks/${id}/events`),
  getTask: (id: string) => request<any>(`/tasks/${id}`),
  createTask: (data: {
    title: string; content?: string; deadline?: string;
    tags?: string; status?: string; confirm_duplicate?: boolean;
  }) => request<{ task: Task; message: string }>('/tasks', {
    method: 'POST', body: JSON.stringify(data),
  }),
  /**
   * Partial update. Server reads `deadline` and `tags` by *presence*: omit
   * the key to leave the field alone, pass '' to clear it.
   */
  updateTask: (id: string, data: {
    status?: string; note?: string; content?: string; title?: string;
    deadline?: string; tags?: string;
  }) => request<{ task: Task; task_id: string; updated: boolean }>(`/tasks/${id}`, {
    method: 'PATCH', body: JSON.stringify(data),
  }),

  // Task statuses (configurable)
  listTaskStatuses: () =>
    request<{ statuses: TaskStatusDef[] }>('/task-statuses'),
  createTaskStatus: (data: { name: string; label?: string; color?: string; description?: string }) =>
    request<TaskStatusDef>('/task-statuses', { method: 'POST', body: JSON.stringify(data) }),
  updateTaskStatus: (name: string, data: { label?: string; color?: string; description?: string; sort_order?: number }) =>
    request<TaskStatusDef>(`/task-statuses/${encodeURIComponent(name)}`, { method: 'PATCH', body: JSON.stringify(data) }),
  reorderTaskStatuses: (names: string[]) =>
    request<{ statuses: TaskStatusDef[] }>('/task-statuses/reorder', {
      method: 'POST', body: JSON.stringify({ names }),
    }),
  deleteTaskStatus: (name: string) =>
    request<{ name: string; deleted: boolean }>(`/task-statuses/${encodeURIComponent(name)}`, { method: 'DELETE' }),

  // Memory
  listMemoryFiles: () => request<{ files: any[] }>('/memory/files'),
  readMemoryFile: (path: string) =>
    request<{ path: string; content: string; read_only?: boolean }>(`/memory/file/${path}`),
  writeMemoryFile: (path: string, content: string) =>
    request<any>(`/memory/file/${path}`, {
      method: 'PUT',
      body: JSON.stringify({ content }),
    }),

  // memU
  getMemuData: () => request<any>('/memory/memu'),
  createMemuCategory: (name: string, description: string) =>
    request<any>('/memory/memu/categories', {
      method: 'POST',
      body: JSON.stringify({ name, description }),
    }),
  updateMemuItem: (id: string, data: { content?: string; memory_type?: string; categories?: string[] }) =>
    request<{ id: string; updated: boolean }>(`/memory/memu/items/${id}`, {
      method: 'PATCH',
      body: JSON.stringify(data),
    }),
  deleteMemuItem: (id: string) =>
    request<{ id: string; deleted: boolean }>(`/memory/memu/items/${id}`, {
      method: 'DELETE',
    }),

  updateMemuCategory: (id: string, data: { summary?: string; description?: string }) =>
    request<{ id: string; updated: boolean }>(`/memory/memu/categories/${id}`, {
      method: 'PATCH',
      body: JSON.stringify(data),
    }),
  getMemuAuditLog: (params?: { action?: string; target_type?: string; limit?: number; offset?: number }) => {
    const qs = new URLSearchParams();
    if (params?.action) qs.set('action', params.action);
    if (params?.target_type) qs.set('target_type', params.target_type);
    if (params?.limit) qs.set('limit', String(params.limit));
    if (params?.offset) qs.set('offset', String(params.offset));
    const q = qs.toString();
    return request<{ logs: any[]; offset: number; limit: number }>(
      `/memory/memu/audit${q ? '?' + q : ''}`
    );
  },

  // memU health
  getMemuHealth: () => request<any>('/memory/memu/health'),

  // Memorization
  triggerMemorizationSweep: () =>
    request<any>('/memorization/sweep', { method: 'POST' }),

  // Sources
  triggerSourceSync: (sourceName: string) =>
    request<any>(`/sources/${encodeURIComponent(sourceName)}/sync`, { method: 'POST' }),
  triggerAllSourcesSync: () =>
    request<any>('/sources/sync-all', { method: 'POST' }),

  // Sources inbox
  getSourceMessages: (params?: { source?: string; limit?: number; before?: string; session?: string }) => {
    const qs = new URLSearchParams();
    if (params?.source) qs.set('source', params.source);
    if (params?.limit) qs.set('limit', String(params.limit));
    if (params?.before) qs.set('before', params.before);
    if (params?.session) qs.set('session', params.session);
    const q = qs.toString();
    return request<{ messages: any[]; has_more: boolean }>(`/sources/messages${q ? '?' + q : ''}`);
  },
  getSourceMessage: (source: string, id: string) =>
    request<any>(`/sources/messages/${encodeURIComponent(source)}/${encodeURIComponent(id)}`),
  deleteSourceMessages: (source?: string) => {
    const qs = source ? `?source=${encodeURIComponent(source)}` : '';
    return request<{ deleted: number }>(`/sources/messages${qs}`, { method: 'DELETE' });
  },
  getSourceOverview: () => request<any>('/sources/overview'),
  getSourceRuns: (params?: { source?: string; limit?: number }) => {
    const qs = new URLSearchParams();
    if (params?.source) qs.set('source', params.source);
    if (params?.limit) qs.set('limit', String(params.limit));
    const q = qs.toString();
    return request<{ runs: any[] }>(`/sources/runs${q ? '?' + q : ''}`);
  },
  getSourceStats: (hours?: number) =>
    request<{ stats: any; hours: number }>(`/sources/stats${hours ? '?hours=' + hours : ''}`),
  getConsumerCursors: (consumer?: string) => {
    const qs = consumer ? `?consumer=${encodeURIComponent(consumer)}` : '';
    return request<{ consumers: any[] }>(`/sources/consumers${qs}`);
  },
  getSourceHealth: () =>
    request<{ health: Record<string, {
      state: 'healthy' | 'degraded' | 'open';
      consecutive_failures: number;
      last_error: string | null;
      last_error_at: string | null;
      last_success_at: string | null;
      backoff_until: string | null;
    }> }>('/sources/health'),

  // Modified files
  getModifiedFiles: (sessionId: string) =>
    request<{ files: any[]; summary: { total_files: number; total_additions: number; total_deletions: number } }>(
      `/sessions/${sessionId}/modified-files`
    ),
  getFileDiff: (sessionId: string, path: string, context = 4) =>
    request<any>(`/sessions/${sessionId}/file-diff?path=${encodeURIComponent(path)}&context=${context}`),

  // Diagnostics
  getDiagnostics: () => request<any>('/diagnostics'),
  getCronLogs: (jobId?: string, limit = 50, offset = 0) =>
    request<{ logs: any[]; total: number; limit: number; offset: number }>(
      `/cron/logs?job_id=${jobId || ''}&limit=${limit}&offset=${offset}`),

  // Observability — lightweight status for UI deep-links
  getObservabilityStatus: () =>
    request<{
      langfuse: {
        enabled: boolean;
        host: string | null;
        project_id: string | null;
        auth_ok: boolean;
        last_flush_at: string | null;
        usage_rewriter: boolean;
      };
    }>('/observability/status'),

  // Prompt rewrite — refine the first prompt of a new chat
  getPromptRewriteStatus: () =>
    request<{ enabled: boolean; model: string }>('/prompt-rewrite/status'),
  rewritePrompt: (prompt: string, signal?: AbortSignal) =>
    request<{ rewritten: string; changed: boolean; model: string }>('/prompt-rewrite', {
      method: 'POST',
      body: JSON.stringify({ prompt }),
      signal,
    }),

  // Cron jobs
  listCronJobs: () => request<{ jobs: any[] }>('/cron/jobs'),
  triggerCronJob: (jobId: string) =>
    request<any>(`/cron/jobs/${encodeURIComponent(jobId)}/trigger`, { method: 'POST' }),
  rotateCronJob: (jobId: string) =>
    request<any>(`/cron/jobs/${encodeURIComponent(jobId)}/rotate`, { method: 'POST' }),

  // Skills
  listSkills: () => request<{ skills: any[] }>('/skills'),
  getSkill: (id: string) => request<any>(`/skills/${encodeURIComponent(id)}`),
  createSkill: (data: { name: string; description: string; content?: string; version?: string }) =>
    request<any>('/skills', { method: 'POST', body: JSON.stringify(data) }),
  updateSkill: (id: string, content: string) =>
    request<any>(`/skills/${encodeURIComponent(id)}`, { method: 'PUT', body: JSON.stringify({ content }) }),
  deleteSkill: (id: string) =>
    request<any>(`/skills/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  toggleSkill: (id: string, enabled: boolean) =>
    request<any>(`/skills/${encodeURIComponent(id)}/toggle`, { method: 'PATCH', body: JSON.stringify({ enabled }) }),
  getSkillUsage: (id: string, limit = 50) =>
    request<any>(`/skills/${encodeURIComponent(id)}/usage?limit=${limit}`),
  getSkillsStats: () => request<any>('/skills/stats'),
  syncSkills: () => request<any>('/skills/sync', { method: 'POST' }),

  // MCP Servers
  // `managed_by` is `organization` in external mode, where the MCP gateway's
  // catalog decides the servers.
  listMcpServers: () =>
    request<{ servers: any[]; managed_by?: string | null }>('/mcp-servers'),
  getMcpServer: (name: string) =>
    request<any>(`/mcp-servers/${encodeURIComponent(name)}`),
  getMcpServerUsage: (name: string, limit = 50) =>
    request<any>(`/mcp-servers/${encodeURIComponent(name)}/usage?limit=${limit}`),
  reloadMcpServers: () =>
    request<any>('/mcp-servers/reload', { method: 'POST' }),

  // Plans
  listPlans: (status?: string, taskId?: string) => {
    const qs = new URLSearchParams();
    if (status) qs.set('status', status);
    if (taskId) qs.set('task_id', taskId);
    const q = qs.toString();
    return request<{ plans: any[] }>(`/plans${q ? '?' + q : ''}`);
  },
  getPlan: (id: string) => request<any>(`/plans/${id}`),
  updatePlan: (id: string, data: { status?: string; feedback?: string }) =>
    request<any>(`/plans/${id}`, { method: 'PATCH', body: JSON.stringify(data) }),
  approvePlan: (id: string) =>
    request<{ plan_id: string; impl_session_id: string }>(`/plans/${id}/approve`, {
      method: 'POST',
    }),
  revisePlan: (id: string, feedback: string) =>
    request<any>(`/plans/${id}/revise`, { method: 'POST', body: JSON.stringify({ feedback }) }),
  getTaskPlans: (taskId: string) =>
    request<{ plans: any[] }>(`/tasks/${taskId}/plans`),

  // Notifications
  listNotifications: (status?: string, type?: string, sessionId?: string) => {
    const qs = new URLSearchParams();
    if (status) qs.set('status', status);
    if (type) qs.set('type', type);
    if (sessionId) qs.set('session_id', sessionId);
    const q = qs.toString();
    return request<{ notifications: any[]; pending_count: number }>(
      `/notifications${q ? '?' + q : ''}`
    );
  },
  getNotification: (id: string) => request<any>(`/notifications/${id}`),
  answerNotification: (id: string, answer: string) =>
    request<any>(`/notifications/${id}/answer`, {
      method: 'POST',
      body: JSON.stringify({ answer }),
    }),
  dismissNotification: (id: string) =>
    request<any>(`/notifications/${id}/dismiss`, { method: 'POST' }),
  dismissAllNotifications: () =>
    request<{ dismissed: number }>('/notifications/dismiss-all', { method: 'POST' }),

  // Notification silences (deterministic suppression rules)
  listSilences: () =>
    request<{ silences: any[] }>('/notifications/silences'),
  createSilence: (pattern: string, reason: string, ttl_hours: number) =>
    request<any>('/notifications/silences', {
      method: 'POST',
      body: JSON.stringify({ pattern, reason, ttl_hours }),
    }),
  deleteSilence: (id: string) =>
    request<any>(`/notifications/silences/${id}`, { method: 'DELETE' }),

  // Ultracode read-only dashboard
  getUltracodeDashboardStatus: () =>
    request<{ enabled: boolean }>('/codex/ultracode/dashboard'),
  listUltracodeRuns: (limit = 40) =>
    request<{ runs: UltracodeRun[] }>(`/codex/ultracode/runs?limit=${encodeURIComponent(String(limit))}`),
  getUltracodeRun: (id: string) =>
    request<{ run: UltracodeRun }>(`/codex/ultracode/runs/${encodeURIComponent(id)}`),

  // Workflow runs — budgeted autonomous agent runs
  listWorkflowRuns: (status?: string, limit = 50) => {
    const qs = new URLSearchParams();
    if (status) qs.set('status', status);
    qs.set('limit', String(limit));
    return request<{ runs: WorkflowRun[]; total: number }>(`/workflow-runs?${qs}`);
  },
  getWorkflowRun: (id: string) =>
    request<WorkflowRun>(`/workflow-runs/${encodeURIComponent(id)}`),
  killWorkflowRun: (id: string, reason = '') =>
    request<WorkflowRun>(`/workflow-runs/${encodeURIComponent(id)}/kill`, {
      method: 'POST',
      body: JSON.stringify({ reason }),
    }),
  getWorkflowRunJournal: (id: string) =>
    request<WorkflowRunJournal>(`/workflow-runs/${encodeURIComponent(id)}/journal`),

  // Files
  uploadFiles: async (files: File[], sessionId: string): Promise<{ files: Array<{ id: string; filename: string; media_type: string; file_type: string; size: number }> }> => {
    const formData = new FormData();
    formData.append('session_id', sessionId);
    files.forEach(f => formData.append('files', f));

    // No Content-Type: the browser sets the multipart boundary.
    const res = await send('/files/upload', { method: 'POST', body: formData }, {});
    return res.json();
  },

  // External agents (Codex, Claude Code) ----------------------------
  listExternalAgents: () =>
    request<{
      enabled: boolean;
      sync_interval_minutes: number;
      conflict_policy: string;
      available: Array<{
        name: string;
        display_name: string;
        cli_command: string | null;
        cli_installed: boolean;
        cli_version: string | null;
        config_paths: string[];
      }>;
      configured: Array<{
        name: string;
        enabled: boolean;
        display_name?: string;
        cli_installed?: boolean;
        cli_version?: string | null;
        last_run_at?: string | null;
        last_error?: string | null;
        files?: Array<{
          path: string;
          hash: string;
          written_at: string | null;
          skipped: boolean;
          error: string | null;
        }>;
      }>;
    }>('/external-agents'),

  triggerExternalAgentsSync: () =>
    request<{ status: string; agents: Record<string, unknown> }>(
      '/external-agents/sync', { method: 'POST' }
    ),

  toggleExternalAgent: (name: string, enabled: boolean) =>
    request<{ name: string; enabled: boolean }>(
      `/external-agents/${encodeURIComponent(name)}/${enabled ? 'enable' : 'disable'}`,
      { method: 'POST' }
    ),

  removeExternalAgent: (name: string) =>
    request<{ status: string; name: string }>(
      `/external-agents/${encodeURIComponent(name)}`,
      { method: 'DELETE' }
    ),

};
