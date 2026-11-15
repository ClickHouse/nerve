import { create } from 'zustand';

/**
 * Models the user starred in the composer's model menu. The menu shows them
 * first, above the families. Kept in this browser's localStorage.
 *
 * A star names a backend and a model id, because the same id could exist on
 * two backends.
 */

const STORAGE_KEY = 'nerve_starred_models';

export const starKey = (backend: string, id: string) => `${backend}/${id}`;

function load(): string[] {
  try {
    const value: unknown = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? '[]');
    return Array.isArray(value) ? value.filter((v): v is string => typeof v === 'string') : [];
  } catch {
    return [];
  }
}

interface StarredModelsState {
  /** Star keys, in the order the user starred them. */
  starred: string[];
  toggle: (key: string) => void;
}

export const useStarredModels = create<StarredModelsState>((set, get) => ({
  starred: load(),
  toggle: (key) => {
    const current = get().starred;
    const next = current.includes(key) ? current.filter(k => k !== key) : [...current, key];
    localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    set({ starred: next });
  },
}));
