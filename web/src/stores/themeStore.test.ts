import { beforeEach, describe, expect, it, vi } from 'vitest';
import indexHtml from '../../index.html?raw';

// The store reads localStorage when its module is first imported, so every
// test imports a fresh copy.
async function loadStore() {
  vi.resetModules();
  return (await import('./themeStore')).useThemeStore;
}

function stamp() {
  const el = document.documentElement;
  return { theme: el.getAttribute('data-theme'), cui: el.getAttribute('data-cui-theme') };
}

function clearStamp() {
  document.documentElement.removeAttribute('data-theme');
  document.documentElement.removeAttribute('data-cui-theme');
}

beforeEach(() => {
  localStorage.clear();
  clearStamp();
});

describe('the first theme', () => {
  it('follows the system when nothing is stored', async () => {
    const store = await loadStore();
    expect(store.getState().preference).toBe('system');
    // jsdom's matchMedia (test/setup.ts) reports no light preference.
    expect(store.getState().resolved).toBe('dark');
    expect(stamp()).toEqual({ theme: null, cui: 'dark' });
    expect(localStorage.getItem('nerve-theme')).toBeNull();
  });

  it('follows the system when the stored value is not a preference', async () => {
    localStorage.setItem('nerve-theme', 'purple');
    const store = await loadStore();
    expect(store.getState().preference).toBe('system');
  });

  it.each(['light', 'dark', 'system'] as const)('keeps a stored %s', async (pref) => {
    localStorage.setItem('nerve-theme', pref);
    const store = await loadStore();
    expect(store.getState().preference).toBe(pref);
  });
});

describe('the pre-paint stamp in index.html', () => {
  const script = /<script>([\s\S]*?)<\/script>/.exec(indexHtml)?.[1] ?? '';

  it('is found', () => {
    expect(script).toContain('nerve-theme');
  });

  it.each([null, 'light', 'dark', 'system', 'purple'])(
    'sets what the store sets (stored %s)',
    async (stored) => {
      if (stored) localStorage.setItem('nerve-theme', stored);
      new Function(script)();
      const prePaint = stamp();
      clearStamp();
      await loadStore();
      expect(stamp()).toEqual(prePaint);
    },
  );
});
