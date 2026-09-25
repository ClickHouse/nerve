import { create } from 'zustand';

type ThemePreference = 'system' | 'light' | 'dark';

/** Click UI has no 'system' — every preference must resolve to a concrete theme. */
export type ResolvedTheme = 'light' | 'dark';

interface ThemeState {
  preference: ThemePreference;
  /** `preference` with 'system' collapsed to what the OS actually asks for. */
  resolved: ResolvedTheme;
  setTheme: (pref: ThemePreference) => void;
  cycleTheme: () => void;
}

const STORAGE_KEY = 'nerve-theme';
const CYCLE_ORDER: ThemePreference[] = ['dark', 'light', 'system'];

function systemTheme(): ResolvedTheme {
  return window.matchMedia?.('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
}

function resolve(pref: ThemePreference): ResolvedTheme {
  return pref === 'system' ? systemTheme() : pref;
}

function applyTheme(pref: ThemePreference) {
  const el = document.documentElement;
  if (pref === 'system') {
    el.removeAttribute('data-theme');
  } else {
    el.setAttribute('data-theme', pref);
  }
  // Click UI keys its tokens off `data-cui-theme` and has no 'system' value, so
  // it always gets the resolved theme. Nerve's own `data-theme` stays the
  // source of truth for Tailwind (absent = follow the OS via a media query).
  el.setAttribute('data-cui-theme', resolve(pref));
}

function getInitialPreference(): ThemePreference {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored === 'light' || stored === 'dark' || stored === 'system') return stored;
  return 'dark';
}

export const useThemeStore = create<ThemeState>((set, get) => {
  // Apply initial theme
  const initial = getInitialPreference();
  applyTheme(initial);

  // Keep 'system' honest: if the OS flips while the app is open, re-resolve.
  window.matchMedia?.('(prefers-color-scheme: light)').addEventListener('change', () => {
    if (get().preference !== 'system') return;
    applyTheme('system');
    set({ resolved: systemTheme() });
  });

  return {
    preference: initial,
    resolved: resolve(initial),

    setTheme: (pref) => {
      localStorage.setItem(STORAGE_KEY, pref);
      applyTheme(pref);
      set({ preference: pref, resolved: resolve(pref) });
    },

    cycleTheme: () => {
      const current = get().preference;
      const idx = CYCLE_ORDER.indexOf(current);
      const next = CYCLE_ORDER[(idx + 1) % CYCLE_ORDER.length];
      get().setTheme(next);
    },
  };
});
