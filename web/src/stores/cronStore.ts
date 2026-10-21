import { create } from 'zustand';
import { api, type CronSchedule } from '../api/client';

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
  /** What the gates say about the next run, judged now; null when unknown. */
  forecast?: 'run' | 'skip' | null;
  /** Why the next run is forecast to skip. */
  forecast_reason?: string | null;
}

/** Each job with the forecast the schedule reports for it. */
function withForecasts(jobs: CronJob[], schedule: CronSchedule | null): CronJob[] {
  const byId = new Map((schedule?.jobs ?? []).map(f => [f.id, f]));
  return jobs.map(job => {
    const f = byId.get(job.id);
    return { ...job, forecast: f?.forecast ?? null, forecast_reason: f?.reason ?? null };
  });
}

/** Each job with the forecast it had in `previous`, until fresh ones arrive. */
function keepForecasts(jobs: CronJob[], previous: CronJob[]): CronJob[] {
  const byId = new Map(previous.map(job => [job.id, job]));
  return jobs.map(job => {
    const before = byId.get(job.id);
    return {
      ...job,
      forecast: before?.forecast ?? null,
      forecast_reason: before?.forecast_reason ?? null,
    };
  });
}

/**
 * How long the Cron page waits for the forecasts. Every job's gates are asked
 * for them on the server, so a slow gate makes them slow; the job list never
 * waits for them, and after this long the page goes on without them.
 */
export const FORECAST_TIMEOUT_MS = 10_000;

/** The schedule, or null when it fails or takes longer than FORECAST_TIMEOUT_MS. */
async function loadSchedule(): Promise<CronSchedule | null> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timedOut = new Promise<null>(resolve => {
    timer = setTimeout(() => {
      controller.abort();
      console.error(`The cron forecasts took longer than ${FORECAST_TIMEOUT_MS / 1000}s; showing the jobs without them`);
      resolve(null);
    }, FORECAST_TIMEOUT_MS);
  });
  try {
    return await Promise.race([api.getCronSchedule(controller.signal), timedOut]);
  } catch (e) {
    console.error('Failed to load the cron schedule:', e);
    return null;
  } finally {
    clearTimeout(timer);
  }
}

/** Counts loadJobs calls, so that answers to an older one never replace a newer one's. */
let jobsLoads = 0;

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
    const load = ++jobsLoads;
    // Asked for alongside the list, but the list never waits for them: the
    // jobs show as soon as they arrive (with the forecasts they had), and the
    // fresh forecasts are filled in when they come.
    const schedule = loadSchedule();
    try {
      const { jobs } = await api.listCronJobs();
      if (load !== jobsLoads) return;
      set({ jobs: keepForecasts(jobs, get().jobs) });
    } catch (e) {
      console.error('Failed to load cron jobs:', e);
      return;
    }
    void schedule.then(forecasts => {
      if (load === jobsLoads) set({ jobs: withForecasts(get().jobs, forecasts) });
    });
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
