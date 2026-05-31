import { create } from 'zustand';
import { api } from '../api/client';

export interface Plan {
  id: string;
  task_id: string;
  task_title: string | null;
  session_id: string | null;
  impl_session_id: string | null;
  status: string;
  content: string;
  feedback: string | null;
  version: number;
  parent_plan_id: string | null;
  model: string | null;
  plan_type: string;
  created_at: string;
  reviewed_at: string | null;
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
  updatePlan: (id: string, status: string, feedback?: string) => Promise<void>;
  approvePlan: (id: string, options?: { runtime?: string; hoa_mode?: string; hoa_agents?: string[]; hoa_pipeline_id?: string }) => Promise<{ impl_session_id: string } | null>;
  revisePlan: (id: string, feedback: string) => Promise<boolean>;
  clearActionError: () => void;
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

export const usePlanStore = create<PlanState>((set, get) => ({
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

  updatePlan: async (id: string, status: string, feedback?: string) => {
    set({ actionLoading: true });
    try {
      await api.updatePlan(id, { status, feedback });
      // Refresh
      const sel = get().selectedPlan;
      if (sel && sel.id === id) {
        set({ selectedPlan: { ...sel, status, ...(feedback ? { feedback } : {}) } });
      }
      get().loadPlans();
    } catch (e) {
      console.error('Failed to update plan:', e);
    } finally {
      set({ actionLoading: false });
    }
  },

  approvePlan: async (id: string, options?: { runtime?: string; hoa_mode?: string; hoa_agents?: string[]; hoa_pipeline_id?: string }) => {
    set({ actionLoading: true });
    try {
      const result = await api.approvePlan(id, options);
      // Refresh
      const sel = get().selectedPlan;
      if (sel && sel.id === id) {
        set({ selectedPlan: { ...sel, status: 'implementing', impl_session_id: result.impl_session_id } });
      }
      get().loadPlans();
      return result;
    } catch (e) {
      console.error('Failed to approve plan:', e);
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
      return true;
    } catch (e) {
      console.error('Failed to request revision:', e);
      set({ actionError: extractErrorMessage(e, 'Failed to request revision') });
      return false;
    } finally {
      set({ actionLoading: false });
    }
  },

  clearActionError: () => set({ actionError: null }),
  clearSelectedPlan: () => set({ selectedPlan: null, actionError: null }),
}));
