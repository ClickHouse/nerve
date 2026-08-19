import { create } from 'zustand';
import { api } from '../api/client';

export const LOGS_PAGE_SIZE = 50;

export interface CronJob {
  id: string;
  type: 'cron' | 'source';
  schedule: string;
  description: string;
  /** Prompt file path when the job's prompt is file-based. */
  prompt_file?: string;
  enabled: boolean;
  session_mode?: string;
  /** Human-readable run-gate conditions; job runs only if all are satisfied. */
  gates?: string[];
  next_run: string | null;
  /** Most recently active chat session for this job (cron:{id}[:{run}]). */
  last_session_id?: string | null;
  /**
   * Why this job's enabled flag cannot be toggled here, or null when it can.
   * Set for source runners (they have no flag in the cron files) and under
   * lockdown (the cron file is reviewed config, so the change belongs in a PR).
   */
  toggle_refusal?: string | null;
}

export interface CronLog {
  id: number;
  job_id: string;
  started_at: string;
  finished_at: string | null;
  status: string | null;
  output: string | null;
  error: string | null;
  /** Session the run executed in — deep-links to the chat page. */
  session_id?: string | null;
}

interface CronState {
  jobs: CronJob[];
  logs: CronLog[];
  logsTotal: number;
  logsOffset: number;
  selectedJobId: string | null;
  loading: boolean;
  triggering: string | null;
  rotating: string | null;
  toggling: string | null;
  /**
   * Why the last toggle failed, keyed by job id. A rejected toggle has to say
   * so: the switch springs back to where it was, which on its own is
   * indistinguishable from a click that never registered — and a 403 under
   * lockdown is an expected answer, not a bug.
   */
  toggleErrors: Record<string, string>;

  loadJobs: () => Promise<void>;
  loadLogs: (offset?: number) => Promise<void>;
  setLogsPage: (offset: number) => void;
  selectJob: (jobId: string | null) => void;
  triggerJob: (jobId: string) => Promise<void>;
  rotateSession: (jobId: string) => Promise<void>;
  setJobEnabled: (jobId: string, enabled: boolean) => Promise<void>;
  refresh: () => Promise<void>;
}

/**
 * The human-readable half of a failed `request()`, which throws
 * `Error("<status>: <raw body>")` where the body is FastAPI's
 * `{"detail": "..."}`. Falls back to the whole message when it is not that
 * shape, so an unexpected failure is still shown rather than swallowed.
 */
function errorDetail(e: unknown): string {
  const raw = e instanceof Error ? e.message : String(e);
  const match = raw.match(/^\d+:\s*([\s\S]*)$/);
  if (!match) return raw;
  try {
    const parsed = JSON.parse(match[1]);
    if (parsed && typeof parsed.detail === 'string') return parsed.detail;
  } catch {
    // Not JSON — fall through to the raw body.
  }
  return match[1] || raw;
}

export const useCronStore = create<CronState>((set, get) => ({
  jobs: [],
  logs: [],
  logsTotal: 0,
  logsOffset: 0,
  selectedJobId: null,
  loading: false,
  triggering: null,
  rotating: null,
  toggling: null,
  toggleErrors: {},

  loadJobs: async () => {
    try {
      const { jobs } = await api.listCronJobs();
      set({ jobs });
    } catch (e) {
      console.error('Failed to load cron jobs:', e);
    }
  },

  loadLogs: async (offset?: number) => {
    const { selectedJobId, logsOffset } = get();
    const effectiveOffset = offset ?? logsOffset;
    set({ loading: true });
    try {
      const { logs, total } = await api.getCronLogs(
        selectedJobId || undefined, LOGS_PAGE_SIZE, effectiveOffset,
      );
      set({ logs, logsTotal: total, logsOffset: effectiveOffset, loading: false });
    } catch (e) {
      console.error('Failed to load cron logs:', e);
      set({ loading: false });
    }
  },

  setLogsPage: (offset: number) => {
    get().loadLogs(Math.max(0, offset));
  },

  selectJob: (jobId: string | null) => {
    set({ selectedJobId: jobId, logsOffset: 0 });
    get().loadLogs(0);
  },

  triggerJob: async (jobId: string) => {
    set({ triggering: jobId });
    try {
      await api.triggerCronJob(jobId);
      // Short delay to let the job start and log
      await new Promise(r => setTimeout(r, 500));
      await get().refresh();
    } catch (e) {
      console.error('Failed to trigger job:', e);
    } finally {
      set({ triggering: null });
    }
  },

  rotateSession: async (jobId: string) => {
    set({ rotating: jobId });
    try {
      await api.rotateCronJob(jobId);
      await get().refresh();
    } catch (e) {
      console.error('Failed to rotate session:', e);
    } finally {
      set({ rotating: null });
    }
  },

  setJobEnabled: async (jobId: string, enabled: boolean) => {
    // Clear this job's previous complaint up front, so a retry that succeeds
    // doesn't leave the old message sitting under a switch that now works.
    set(s => {
      const remaining = { ...s.toggleErrors };
      delete remaining[jobId];
      return { toggling: jobId, toggleErrors: remaining };
    });
    try {
      await api.setCronJobEnabled(jobId, enabled);
      // Reload the list rather than patching the flag locally: the server also
      // recomputed next_run, and a disabled job has none.
      await get().loadJobs();
    } catch (e) {
      console.error('Failed to toggle cron job:', e);
      set(s => ({ toggleErrors: { ...s.toggleErrors, [jobId]: errorDetail(e) } }));
    } finally {
      set({ toggling: null });
    }
  },

  refresh: async () => {
    await Promise.all([get().loadJobs(), get().loadLogs()]);
  },
}));
