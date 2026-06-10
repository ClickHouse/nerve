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

  loadJobs: () => Promise<void>;
  loadLogs: (offset?: number) => Promise<void>;
  setLogsPage: (offset: number) => void;
  selectJob: (jobId: string | null) => void;
  triggerJob: (jobId: string) => Promise<void>;
  rotateSession: (jobId: string) => Promise<void>;
  refresh: () => Promise<void>;
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

  refresh: async () => {
    await Promise.all([get().loadJobs(), get().loadLogs()]);
  },
}));
