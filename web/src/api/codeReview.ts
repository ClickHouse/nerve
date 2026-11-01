import { request } from './client';

export interface ReviewPR {
  id: string; task_id: string; profile: string; repository: string; number: number;
  title: string; url: string; state: string; draft: number; head: string; base: string;
  current_version: string | null; reviewed_version: string | null; review_state: string;
  severity: string; ci: string; content_updated_at: string; last_polled_at: string; error: string;
}
export interface ReviewVersion {
  id: string; head: string; base: string; status: string; created_at: string;
  updated_at: string; error: string; spent_usd: number;
}
export interface ReviewStatus {
  board_name: string; running: boolean; worker_available: boolean; worker_error?: string; last_tick_at?: string;
  profiles: { id: string; enabled: boolean; paused: number; schedule: string;
    timezone: string; registered: boolean; overdue: boolean; next_run_at: string | null;
    last_success_at: string | null; last_polled_at: string | null; error: string;
    version_budget_usd: number; daily_budget_usd: number; day_committed_usd: number; day: string;
  }[];
}
export interface ReviewDetail { pr: ReviewPR; versions: ReviewVersion[]; total: number }
export interface VersionReport {
  version: ReviewVersion & { snapshot: Record<string, unknown> };
  attempts: { run_id: string; spent: number; reserved: number; uncertain: number; settled: number }[];
  reports: { version: string; run: string; admission: string; error: string; spent_usd: number; uncertain_usd?: number; output: Record<string, string> }[];
}
export const codeReview = {
  status: () => request<ReviewStatus>('/code-review/status'),
  pause: (id: string) => request<ReviewStatus>(`/code-review/profiles/${encodeURIComponent(id)}/pause`, { method: 'POST' }),
  resume: (id: string) => request<ReviewStatus>(`/code-review/profiles/${encodeURIComponent(id)}/resume`, { method: 'POST' }),
  board: (column: string, offset = 0) => request<{ items: ReviewPR[]; total: number }>(`/code-review/board?column=${column}&offset=${offset}`),
  detail: (id: string, offset = 0) => request<ReviewDetail>(`/code-review/prs/${encodeURIComponent(id)}?offset=${offset}`),
  version: (id: string, version: string) => request<VersionReport>(`/code-review/prs/${encodeURIComponent(id)}/versions/${encodeURIComponent(version)}`),
};
