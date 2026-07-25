import { create } from 'zustand';
import { api, type WorkflowRun } from '../api/client';

/** Statuses that mean the run is still consuming budget. */
export const ACTIVE_RUN_STATUSES = new Set<WorkflowRun['status']>(['pending', 'running']);

export function isActiveRun(run: WorkflowRun): boolean {
  return ACTIVE_RUN_STATUSES.has(run.status);
}

interface WorkflowRunState {
  runs: WorkflowRun[];
  total: number;
  /** True until the first list fetch settles — drives the initial spinner. */
  loading: boolean;
  /** In-flight guard so the 15s poll can't stack requests. */
  fetching: boolean;
  error: string | null;

  loadRuns: () => Promise<void>;
  killRun: (id: string, reason?: string) => Promise<void>;
  /** Upsert from the `workflow_run_update` WS event (global channel). */
  handleRunUpdate: (run: WorkflowRun) => void;
}

export const useWorkflowRunStore = create<WorkflowRunState>((set, get) => ({
  runs: [],
  total: 0,
  loading: true,
  fetching: false,
  error: null,

  loadRuns: async () => {
    if (get().fetching) return;
    set({ fetching: true });
    try {
      const { runs, total } = await api.listWorkflowRuns(undefined, 100);
      set({ runs: runs || [], total: total ?? (runs?.length || 0), error: null });
    } catch (e) {
      console.error('Failed to load workflow runs:', e);
      set({ error: e instanceof Error ? e.message : String(e) });
    } finally {
      set({ fetching: false, loading: false });
    }
  },

  killRun: async (id: string, reason = '') => {
    try {
      const run = await api.killWorkflowRun(id, reason);
      get().handleRunUpdate(run);
      set({ error: null });
    } catch (e) {
      console.error('Failed to kill workflow run:', e);
      set({ error: e instanceof Error ? e.message : String(e) });
    }
  },

  handleRunUpdate: (run: WorkflowRun) => {
    set(s => {
      const idx = s.runs.findIndex(r => r.id === run.id);
      if (idx === -1) {
        // New run — the list endpoint returns newest first, so prepend.
        return { runs: [run, ...s.runs], total: s.total + 1 };
      }
      const runs = s.runs.slice();
      runs[idx] = run;
      return { runs };
    });
  },
}));
