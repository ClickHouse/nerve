import type { CSSProperties } from 'react';
import { create } from 'zustand';
import { api, type TaskStatusDef } from '../api/client';

export type { TaskStatusDef };

interface TaskStatusState {
  statuses: TaskStatusDef[];
  loaded: boolean;
  loading: boolean;

  load: (force?: boolean) => Promise<void>;
  create: (data: { name: string; label?: string; color?: string; description?: string }) => Promise<void>;
  update: (name: string, data: { label?: string; color?: string; description?: string; sort_order?: number }) => Promise<void>;
  remove: (name: string) => Promise<void>;
  byName: (name: string) => TaskStatusDef | undefined;
}

export const useTaskStatusStore = create<TaskStatusState>((set, get) => ({
  statuses: [],
  loaded: false,
  loading: false,

  load: async (force = false) => {
    if (get().loading) return;
    if (get().loaded && !force) return;
    set({ loading: true });
    try {
      const { statuses } = await api.listTaskStatuses();
      set({ statuses, loaded: true, loading: false });
    } catch (e) {
      console.error('Failed to load task statuses:', e);
      set({ loading: false });
    }
  },

  create: async (data) => {
    await api.createTaskStatus(data);
    await get().load(true);
  },

  update: async (name, data) => {
    await api.updateTaskStatus(name, data);
    await get().load(true);
  },

  // Throws on failure (e.g. 409 in-use, 400 protected) so callers can surface
  // the server's message.
  remove: async (name) => {
    await api.deleteTaskStatus(name);
    await get().load(true);
  },

  byName: (name) => get().statuses.find((s) => s.name === name),
}));

const FALLBACK_COLOR = '#6b7280';

/**
 * Inline style for a status badge from an arbitrary hex color. Alpha
 * suffixes keep user-chosen colors legible on both light and dark themes.
 */
export function statusBadgeStyle(color: string | undefined): CSSProperties {
  const c = color || FALLBACK_COLOR;
  return {
    color: c,
    backgroundColor: `${c}1a`, // ~10% opacity
    borderColor: `${c}33`,     // ~20% opacity
  };
}
