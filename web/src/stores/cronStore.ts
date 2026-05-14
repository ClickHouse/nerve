import { create } from 'zustand';
import { api } from '../api/client';

export interface CronJob {
  id: string;
  type: 'cron' | 'source';
  schedule: string;
  description: string;
  enabled: boolean;
  session_mode?: string;
  next_run: string | null;
}

export interface CronLog {
  id: number;
  job_id: string;
  started_at: string;
  finished_at: string | null;
  status: string | null;
  output: string | null;
  error: string | null;
}

interface CronState {
  jobs: CronJob[];
  logs: CronLog[];
  selectedJobId: string | null;
  loading: boolean;
  triggering: string | null;
  rotating: string | null;

  loadJobs: () => Promise<void>;
  loadLogs: () => Promise<void>;
  selectJob: (jobId: string | null) => void;
  triggerJob: (jobId: string) => Promise<void>;
  rotateSession: (jobId: string) => Promise<void>;
  refresh: () => Promise<void>;
}

export const useCronStore = create<CronState>((set, get) => ({
  jobs: [],
  logs: [],
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

  loadLogs: async () => {
    const { selectedJobId } = get();
    set({ loading: true });
    try {
      // Cap at 30 — the UI only renders a list, and unfiltered queries
      // sort the entire cron_logs table. Fewer rows = faster page load.
      const { logs } = await api.getCronLogs(selectedJobId || undefined, 30);
      set({ logs, loading: false });
    } catch (e) {
      console.error('Failed to load cron logs:', e);
      set({ loading: false });
    }
  },

  selectJob: (jobId: string | null) => {
    set({ selectedJobId: jobId });
    get().loadLogs();
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
