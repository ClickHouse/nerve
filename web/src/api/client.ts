const API_BASE = '/api';

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

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string> || {}),
  };
  if (authToken) {
    headers['Authorization'] = `Bearer ${authToken}`;
  }

  const res = await fetch(`${API_BASE}${path}`, { ...options, headers });

  if (res.status === 401) {
    clearToken();
    window.location.reload();
    throw new Error('Unauthorized');
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

  // Sessions
  listSessions: () => request<{ sessions: any[] }>('/sessions'),
  searchSessions: (q: string) =>
    request<{ sessions: any[] }>(`/sessions/search?q=${encodeURIComponent(q)}`),
  getSession: (id: string) => request<any>(`/sessions/${id}`),
  createSession: (title?: string) =>
    request<any>('/sessions', {
      method: 'POST',
      body: JSON.stringify({ title }),
    }),
  deleteSession: (id: string) =>
    request<any>(`/sessions/${id}`, { method: 'DELETE' }),
  updateSession: (id: string, data: { title?: string; starred?: boolean }) =>
    request<any>(`/sessions/${id}`, { method: 'PATCH', body: JSON.stringify(data) }),
  getMessages: (sessionId: string, limit = 100) =>
    request<{ messages: any[]; last_usage?: { input_tokens: number; output_tokens: number; cache_creation_input_tokens: number; cache_read_input_tokens: number; max_context_tokens: number } }>(`/sessions/${sessionId}/messages?limit=${limit}`),
  forkSession: (sourceSessionId: string, atMessageId?: string, title?: string) =>
    request<any>('/sessions/fork', {
      method: 'POST',
      body: JSON.stringify({ source_session_id: sourceSessionId, at_message_id: atMessageId, title }),
    }),
  resumeSession: (id: string) =>
    request<any>(`/sessions/${id}/resume`, { method: 'POST' }),
  archiveSession: (id: string) =>
    request<any>(`/sessions/${id}/archive`, { method: 'POST' }),
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
  listTasks: (status?: string) =>
    request<{ tasks: any[] }>(`/tasks${status ? `?status=${status}` : ''}`),
  searchTasks: (query: string, status?: string) => {
    const qs = new URLSearchParams({ q: query });
    if (status) qs.set('status', status);
    return request<{ tasks: any[] }>(`/tasks/search?${qs}`);
  },
  getTask: (id: string) => request<any>(`/tasks/${id}`),
  createTask: (data: { title: string; content?: string; deadline?: string }) =>
    request<any>('/tasks', { method: 'POST', body: JSON.stringify(data) }),
  updateTask: (id: string, data: { status?: string; note?: string; content?: string }) =>
    request<any>(`/tasks/${id}`, { method: 'PATCH', body: JSON.stringify(data) }),

  // Memory
  listMemoryFiles: () => request<{ files: any[] }>('/memory/files'),
  readMemoryFile: (path: string) =>
    request<{ path: string; content: string }>(`/memory/file/${path}`),
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
  getCronLogs: (jobId?: string, limit = 50) =>
    request<{ logs: any[] }>(`/cron/logs?job_id=${jobId || ''}&limit=${limit}`),

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
    request<{ plan_id: string; impl_session_id: string }>(`/plans/${id}/approve`, { method: 'POST' }),
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

};
