import { create } from 'zustand';
import { api } from '../api/client';

/** The implementation session of an approved plan: GET /api/plans/{id} only. */
export interface ImplSession {
  id: string;
  title: string | null;
  updated_at: string | null;
  is_running: boolean;
  awaiting_input: boolean;
  /** No turn runs, but a wake-up is scheduled or background work runs. */
  parked: boolean;
}

export interface Plan {
  id: string;
  task_id: string;
  task_title: string | null;
  session_id: string | null;
  impl_session_id: string | null;
  status: string;
  content: string;
  /** Up to 3 short lines from the agent that wrote the plan. Null when it gave none. */
  summary?: string | null;
  feedback: string | null;
  version: number;
  parent_plan_id: string | null;
  model: string | null;
  plan_type: string;
  created_at: string;
  reviewed_at: string | null;
  // Who made the last review decision (approve, decline, revision request)
  // and on whose behalf. Null when nobody reviewed it or on older rows.
  reviewed_by_actor_id?: string | null;
  reviewed_caused_by_actor_id?: string | null;
  // Who wrote this version and on whose behalf. Null on older rows.
  created_by_actor_id?: string | null;
  caused_by_actor_id?: string | null;
  /** Null when the plan has no implementation session or its row is gone. */
  impl_session?: ImplSession | null;
}

interface PlanState {
  plans: Plan[];
  selectedPlan: Plan | null;
  filter: string;
  loading: boolean;
  detailLoading: boolean;
  actionLoading: boolean;
  actionError: string | null;

  loadPlans: () => Promise<void>;
  setFilter: (f: string) => void;
  loadPlan: (id: string) => Promise<void>;
  /** Load the open plan again, without the loading state. */
  refreshPlan: (id: string) => Promise<void>;
  updatePlan: (id: string, status: string, feedback?: string) => Promise<void>;
  approvePlan: (id: string) => Promise<{ impl_session_id: string } | null>;
  revisePlan: (id: string, feedback: string) => Promise<boolean>;
  clearSelectedPlan: () => void;
}

// Extract a user-friendly message out of an Error thrown by api/client.
// The client formats non-2xx responses as `${status}: ${body}` where the
// body is FastAPI's JSON error object. Strip that wrapping so we can show
// the `detail` string instead of raw JSON.
function extractErrorMessage(err: unknown, fallback: string): string {
  const raw = err instanceof Error ? err.message : String(err);
  const match = raw.match(/^\d+:\s*(.*)$/s);
  const body = match ? match[1] : raw;
  try {
    const parsed = JSON.parse(body);
    if (parsed && typeof parsed === 'object' && typeof parsed.detail === 'string') {
      return parsed.detail;
    }
  } catch {
    // Not JSON — fall through and use the body as-is.
  }
  return body || fallback;
}

/** Plan statuses after implementing. */
const TERMINAL = new Set(['done', 'failed']);

export const usePlanStore = create<PlanState>((set, get) => {
  // After a decision, install the plan as the server stored it: the
  // reviewer columns and a status that another decision set come from there.
  async function reloadSelected(id: string): Promise<void> {
    try {
      const plan = await api.getPlan(id);
      const open = get().selectedPlan;
      // Loads can overlap. A plan never goes back to implementing from done
      // or failed, so such an answer is older than the plan on screen.
      const stale = plan.status === 'implementing' && TERMINAL.has(open?.status ?? '');
      if (open?.id === id && !stale) set({ selectedPlan: plan });
    } catch (e) {
      console.error('Failed to reload plan:', e);
    }
  }

  return {
    plans: [],
    selectedPlan: null,
    filter: 'pending',
    loading: true,
    detailLoading: false,
    actionLoading: false,
    actionError: null,

    loadPlans: async () => {
      try {
        const { filter } = get();
        const { plans } = await api.listPlans(filter || undefined);
        set({ plans, loading: false });
      } catch (e) {
        console.error('Failed to load plans:', e);
        set({ loading: false });
      }
    },

    setFilter: (f: string) => {
      set({ filter: f });
      get().loadPlans();
    },

    loadPlan: async (id: string) => {
      set({ detailLoading: true, selectedPlan: null });
      try {
        const plan = await api.getPlan(id);
        set({ selectedPlan: plan, detailLoading: false });
      } catch (e) {
        console.error('Failed to load plan:', e);
        set({ detailLoading: false });
      }
    },

    refreshPlan: reloadSelected,

    updatePlan: async (id: string, status: string, feedback?: string) => {
      set({ actionLoading: true, actionError: null });
      try {
        await api.updatePlan(id, { status, feedback });
        const sel = get().selectedPlan;
        if (sel && sel.id === id) {
          set({ selectedPlan: { ...sel, status, ...(feedback ? { feedback } : {}) } });
        }
        await reloadSelected(id);
        get().loadPlans();
      } catch (e) {
        console.error('Failed to update plan:', e);
        set({ actionError: extractErrorMessage(e, 'Failed to update the plan') });
      } finally {
        set({ actionLoading: false });
      }
    },

    approvePlan: async (id: string) => {
      set({ actionLoading: true, actionError: null });
      try {
        const result = await api.approvePlan(id);
        const sel = get().selectedPlan;
        if (sel && sel.id === id) {
          set({ selectedPlan: { ...sel, status: 'implementing', impl_session_id: result.impl_session_id } });
        }
        await reloadSelected(id);
        get().loadPlans();
        return result;
      } catch (e) {
        console.error('Failed to approve plan:', e);
        set({ actionError: extractErrorMessage(e, 'Failed to approve the plan') });
        return null;
      } finally {
        set({ actionLoading: false });
      }
    },

    revisePlan: async (id: string, feedback: string) => {
      set({ actionLoading: true, actionError: null });
      try {
        await api.revisePlan(id, feedback);
        const sel = get().selectedPlan;
        if (sel && sel.id === id) {
          set({ selectedPlan: { ...sel, feedback } });
        }
        await reloadSelected(id);
        return true;
      } catch (e) {
        console.error('Failed to request revision:', e);
        set({ actionError: extractErrorMessage(e, 'Failed to request revision') });
        return false;
      } finally {
        set({ actionLoading: false });
      }
    },

    clearSelectedPlan: () => set({ selectedPlan: null, actionError: null }),
  };
});
