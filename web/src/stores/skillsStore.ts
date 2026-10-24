import { create } from 'zustand';
import { api } from '../api/client';

/**
 * Where a skill comes from. `platform`: the operator or the hosting platform
 * wrote it in the config directory, and it is read only here.
 */
export type SkillSource = 'workspace' | 'platform';

export interface Skill {
  id: string;
  name: string;
  description: string;
  version: string;
  enabled: boolean;
  source: SkillSource;
  user_invocable: boolean;
  model_invocable: boolean;
  allowed_tools: string[] | null;
  total_invocations: number;
  success_count: number;
  avg_duration_ms: number | null;
  last_used: string | null;
  created_at: string;
  updated_at: string;
}

export interface SkillDetail extends Skill {
  content: string;
  raw: string;
  has_references: boolean;
  has_scripts: boolean;
  has_assets: boolean;
  references: string[];
  stats: {
    total_invocations: number;
    success_count: number;
    avg_duration_ms: number | null;
    last_used: string | null;
  };
  recent_usage: Array<{
    id: number;
    skill_id: string;
    session_id: string | null;
    invoked_by: string;
    duration_ms: number | null;
    success: boolean;
    error: string | null;
    created_at: string;
  }>;
}

interface SkillsState {
  skills: Skill[];
  selectedSkill: SkillDetail | null;
  loading: boolean;
  detailLoading: boolean;
  actionLoading: boolean;
  showCreateDialog: boolean;

  loadSkills: () => Promise<void>;
  loadSkill: (id: string) => Promise<void>;
  createSkill: (data: { name: string; description: string; content?: string }) => Promise<string | null>;
  updateSkill: (id: string, content: string) => Promise<void>;
  deleteSkill: (id: string) => Promise<void>;
  toggleSkill: (id: string, enabled: boolean) => Promise<void>;
  syncSkills: () => Promise<void>;
  setShowCreateDialog: (show: boolean) => void;
  clearSelectedSkill: () => void;
}

export const useSkillsStore = create<SkillsState>((set, get) => ({
  skills: [],
  selectedSkill: null,
  loading: true,
  detailLoading: false,
  actionLoading: false,
  showCreateDialog: false,

  loadSkills: async () => {
    try {
      const { skills } = await api.listSkills();
      set({ skills, loading: false });
    } catch (e) {
      console.error('Failed to load skills:', e);
      set({ loading: false });
    }
  },

  loadSkill: async (id: string) => {
    set({ detailLoading: true, selectedSkill: null });
    try {
      const skill = await api.getSkill(id);
      set({ selectedSkill: skill, detailLoading: false });
    } catch (e) {
      console.error('Failed to load skill:', e);
      set({ detailLoading: false });
    }
  },

  createSkill: async (data) => {
    set({ actionLoading: true });
    try {
      const result = await api.createSkill(data);
      await get().loadSkills();
      set({ actionLoading: false, showCreateDialog: false });
      return result.id;
    } catch (e) {
      console.error('Failed to create skill:', e);
      set({ actionLoading: false });
      return null;
    }
  },

  updateSkill: async (id: string, content: string) => {
    set({ actionLoading: true });
    try {
      await api.updateSkill(id, content);
      // Refresh detail
      await get().loadSkill(id);
      await get().loadSkills();
    } catch (e) {
      console.error('Failed to update skill:', e);
    } finally {
      set({ actionLoading: false });
    }
  },

  deleteSkill: async (id: string) => {
    set({ actionLoading: true });
    try {
      await api.deleteSkill(id);
      set({ selectedSkill: null });
      await get().loadSkills();
    } catch (e) {
      console.error('Failed to delete skill:', e);
    } finally {
      set({ actionLoading: false });
    }
  },

  toggleSkill: async (id: string, enabled: boolean) => {
    try {
      await api.toggleSkill(id, enabled);
      // Update local state immediately
      set(state => ({
        skills: state.skills.map(s => s.id === id ? { ...s, enabled } : s),
        selectedSkill: state.selectedSkill?.id === id
          ? { ...state.selectedSkill, enabled }
          : state.selectedSkill,
      }));
    } catch (e) {
      console.error('Failed to toggle skill:', e);
    }
  },

  syncSkills: async () => {
    set({ actionLoading: true });
    try {
      await api.syncSkills();
      await get().loadSkills();
    } catch (e) {
      console.error('Failed to sync skills:', e);
    } finally {
      set({ actionLoading: false });
    }
  },

  setShowCreateDialog: (show: boolean) => set({ showCreateDialog: show }),
  clearSelectedSkill: () => set({ selectedSkill: null }),
}));
