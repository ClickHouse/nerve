const API_BASE = '/api';

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
  actor: string;
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
  created_by: string;
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
  created_at: string;
  updated_at: string;
  ended_at: string | null;
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

export function setToken(token: string) {
  authToken = token;
  localStorage.setItem('nerve_token', token);
}

export function clearToken() {
  authToken = null;
  localStorage.removeItem('nerve_token');
}

export function getToken(): string | null {
  return authToken;
}

/** Response header carrying a server-refreshed session token. */
const SESSION_TOKEN_HEADER = 'X-Nerve-Token';

/**
 * Adopt a slid session token. The gateway re-mints the token once it is past
 * half its lifetime and returns it on the response, so a tab that keeps
 * talking to the server never expires — no daily re-login, no logout
 * mid-sentence.
 */
function absorbRefreshedToken(res: Response): void {
  const fresh = res.headers.get(SESSION_TOKEN_HEADER);
  if (fresh && fresh !== authToken) setToken(fresh);
}

/**
 * What to do when the server says 401. Registered by the auth store so this
 * module doesn't have to import it (that would be a cycle).
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

function handleUnauthorized(): Error {
  clearToken();
  onUnauthorized?.();
  return new Error('Unauthorized');
}

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string> || {}),
  };
  if (authToken) {
    headers['Authorization'] = `Bearer ${authToken}`;
  }

  const res = await fetch(`${API_BASE}${path}`, { ...options, headers });

  absorbRefreshedToken(res);

  if (res.status === 401) {
    throw handleUnauthorized();
  }

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status}: ${body}`);
  }

  return res.json();
}

export const api = {
  // Auth
  login: (password: string) =>
    request<{ token: string }>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ password }),
    }),

  checkAuth: () => request<{ authenticated: boolean }>('/auth/check'),

  authStatus: () => request<{ auth_required: boolean }>('/auth/status'),

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
  listReviewLoops: (status?: string) =>
    request<{ loops: ReviewLoop[] }>(`/review-loops${status ? `?status=${status}` : ''}`),
  getReviewLoop: (loopId: string) =>
    request<{ loop: ReviewLoop; attempts: ReviewLoopAttempt[] }>(`/review-loops/${loopId}`),
  killReviewLoop: (loopId: string, reason = '') =>
    request<ReviewLoop>(`/review-loops/${loopId}/kill`, {
      method: 'POST', body: JSON.stringify({ reason }),
    }),
  decideReviewLoop: (loopId: string, decision: string) =>
    request<{ ok: boolean; message: string }>(`/review-loops/${loopId}/decision`, {
      method: 'POST', body: JSON.stringify({ decision }),
    }),
  getReviewLoopState: (loopId: string) =>
    request<{ exists: boolean; truncated: boolean; content: string; path: string }>(
      `/review-loops/${loopId}/state`,
    ),
  deleteSession: (id: string) =>
    request<any>(`/sessions/${id}`, { method: 'DELETE' }),
  updateSession: (id: string, data: { title?: string; starred?: boolean; model?: string }) =>
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
        auth_ok: boolean;
        last_flush_at: string | null;
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
  listMcpServers: () => request<{ servers: any[] }>('/mcp-servers'),
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

    const headers: Record<string, string> = {};
    if (authToken) headers['Authorization'] = `Bearer ${authToken}`;

    const res = await fetch(`${API_BASE}/files/upload`, {
      method: 'POST',
      headers,
      body: formData,
    });

    absorbRefreshedToken(res);

    if (res.status === 401) {
      throw handleUnauthorized();
    }
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`${res.status}: ${body}`);
    }
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
