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
  reorder: (names: string[]) => Promise<void>;
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

  /**
   * Set column order. Applied locally first so the dragged column doesn't
   * snap back while the request is in flight; the server's echo is
   * authoritative, and a failure reloads rather than guessing.
   */
  reorder: async (names) => {
    const snapshot = get().statuses;
    const byName = new Map(snapshot.map((s) => [s.name, s]));
    const optimistic = names
      .map((n) => byName.get(n))
      .filter((s): s is TaskStatusDef => Boolean(s));
    if (optimistic.length === snapshot.length) set({ statuses: optimistic });
    try {
      const { statuses } = await api.reorderTaskStatuses(names);
      set({ statuses });
    } catch (e) {
      console.error('Failed to reorder statuses:', e);
      set({ statuses: snapshot });
      await get().load(true);
    }
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

/**
 * The colours of a status for a form control, as CSS variables:
 * `--status-tint-bg` and `--status-tint-border`. Set them inline and apply
 * them with classes (see `STATUS_TINT_CLASSES`). An inline `borderColor`
 * would override the control's `focus:` border, which is its only focus
 * indicator. A class leaves the `focus:` variant in effect.
 *
 * No text colour: a user-chosen colour on its own tint can be below the
 * 4.5:1 contrast that small text needs, so the text keeps the colour of the
 * theme.
 */
export function statusTintStyle(color: string | undefined): CSSProperties {
  const c = color || FALLBACK_COLOR;
  return {
    '--status-tint-bg': `${c}1a`,     // ~10% opacity
    '--status-tint-border': `${c}99`, // ~60% opacity
  } as CSSProperties;
}

/** The classes that apply the variables of `statusTintStyle`. */
export const STATUS_TINT_CLASSES =
  'bg-[color:var(--status-tint-bg)] border-[color:var(--status-tint-border)]';
