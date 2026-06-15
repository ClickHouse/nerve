import { create } from 'zustand';

/**
 * Misc UI state that doesn't belong to a single feature store.
 * Right now: just the keyboard-shortcuts modal flag.
 */
interface UIState {
  shortcutsModalOpen: boolean;
  openShortcutsModal: () => void;
  closeShortcutsModal: () => void;
  toggleShortcutsModal: () => void;
}

export const useUIStore = create<UIState>((set, get) => ({
  shortcutsModalOpen: false,
  openShortcutsModal: () => set({ shortcutsModalOpen: true }),
  closeShortcutsModal: () => set({ shortcutsModalOpen: false }),
  toggleShortcutsModal: () => set({ shortcutsModalOpen: !get().shortcutsModalOpen }),
}));
