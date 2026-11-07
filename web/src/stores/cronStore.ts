import { create } from 'zustand';
import { api, type CronSchedule } from '../api/client';

export const LOGS_PAGE_SIZE = 50;

/** A run's result: null while the run goes on. */
export type CronRunStatus = 'success' | 'error' | 'missed' | null;

/** The latest run of a job (`GET /api/cron/jobs`). */
export interface CronLastRun {
  status: CronRunStatus;
  started_at: string;
  finished_at: string | null;
}

/** The runs that started in the last 7 days, by result. */
export interface CronJobStats {
  runs: number;
  success: number;
  error: number;
  missed: number;
}

export interface CronJob {
  id: string;
  type: 'cron' | 'source';
  /** What a run does: an agent turn, a workflow run, or a source poll. */
  kind: 'agent' | 'workflow' | 'source';
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
  last_run: CronLastRun | null;
  stats_7d: CronJobStats;
  /** Why the Active switch cannot change this job; null when it can. */
  toggle_blocked: string | null;
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

/** The `detail` text of a refused request (`409: {"detail": "..."}`), or `fallback`. */
function refusal(e: unknown, fallback: string): string {
  const raw = e instanceof Error ? e.message : '';
  try {
    const detail: unknown = JSON.parse(raw.slice(raw.indexOf(':') + 1)).detail;
    if (typeof detail === 'string') return detail;
  } catch {
    // Not a JSON body.
  }
  return fallback;
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
  /** New records of a source run; null for other runs. */
  records?: number | null;
}

interface CronState {
  jobs: CronJob[];
  logs: CronLog[];
  logsTotal: number;
  logsOffset: number;
  /** Source runs that found nothing new, for the selected job or all jobs. */
  logsEmpty: number;
  /** Whether the history shows the runs that found nothing new. */
  showEmpty: boolean;
  /** The selected job; null for the runs of all jobs. */
  selectedJobId: string | null;
  loading: boolean;
  triggering: string | null;
  rotating: string | null;
  /** The job whose Active switch is being saved. */
  toggling: string | null;
  /** Why the last switch of a job failed. */
  toggleError: { jobId: string; message: string } | null;

  /** Load the page: the jobs, then the first job's sheet if none is selected. */
  open: () => Promise<void>;
  loadJobs: () => Promise<void>;
  loadLogs: (offset?: number) => Promise<void>;
  setLogsPage: (offset: number) => void;
  setShowEmpty: (show: boolean) => void;
  selectJob: (jobId: string | null) => void;
  triggerJob: (jobId: string) => Promise<void>;
  rotateSession: (jobId: string) => Promise<void>;
  setJobEnabled: (jobId: string, enabled: boolean) => Promise<void>;
  refresh: () => Promise<void>;
}

export const useCronStore = create<CronState>((set, get) => ({
  jobs: [],
  logs: [],
  logsTotal: 0,
  logsOffset: 0,
  logsEmpty: 0,
  showEmpty: false,
  selectedJobId: null,
  loading: false,
  triggering: null,
  rotating: null,
  toggling: null,
  toggleError: null,

  open: async () => {
    await get().loadJobs();
    const { selectedJobId, jobs } = get();
    if (selectedJobId === null && jobs.length > 0) get().selectJob(jobs[0].id);
    else await get().loadLogs();
  },

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
    const { selectedJobId, logsOffset, showEmpty } = get();
    const effectiveOffset = offset ?? logsOffset;
    set({ loading: true });
    try {
      const { logs, total, empty } = await api.getCronLogs(
        selectedJobId || undefined, LOGS_PAGE_SIZE, effectiveOffset, !showEmpty,
      );
      set({
        logs, logsTotal: total, logsEmpty: empty, logsOffset: effectiveOffset, loading: false,
      });
    } catch (e) {
      console.error('Failed to load cron logs:', e);
      set({ loading: false });
    }
  },

  setLogsPage: (offset: number) => {
    get().loadLogs(Math.max(0, offset));
  },

  setShowEmpty: (show: boolean) => {
    set({ showEmpty: show });
    get().loadLogs(0);
  },

  selectJob: (jobId: string | null) => {
    set({ selectedJobId: jobId, logsOffset: 0, toggleError: null });
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
    set({ toggling: jobId, toggleError: null });
    try {
      await api.setCronJobEnabled(jobId, enabled);
      await get().loadJobs();
    } catch (e) {
      set({ toggleError: { jobId, message: refusal(e, 'The job could not be switched.') } });
    } finally {
      set({ toggling: null });
    }
  },

  refresh: async () => {
    await Promise.all([get().loadJobs(), get().loadLogs()]);
  },
}));
