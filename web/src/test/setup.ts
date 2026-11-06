import '@testing-library/jest-dom/vitest';
import { cleanup } from '@testing-library/react';
import { afterEach, expect, vi } from 'vitest';

// An in-memory `localStorage` for every test file. Node 25 and later define an
// inert `localStorage` global that shadows jsdom's Storage, and the stores and
// the API client read localStorage when they are first imported, so without
// this a test file crashes at import on those Node versions. It is installed
// on every Node version (also replacing jsdom's Storage), so every run sees
// the same storage. This file runs before each test file's imports. A test
// that needs its own storage can still define one over it.
function installMemoryStorage(): void {
  const data = new Map<string, string>();
  const storage: Storage = {
    getItem: (key: string) => (data.has(key) ? data.get(key)! : null),
    setItem: (key: string, value: string) => { data.set(key, String(value)); },
    removeItem: (key: string) => { data.delete(key); },
    clear: () => data.clear(),
    key: (index: number) => [...data.keys()][index] ?? null,
    get length() { return data.size; },
  };
  for (const target of [globalThis, globalThis.window]) {
    if (target) Object.defineProperty(target, 'localStorage', { value: storage, configurable: true, writable: true });
  }
}
installMemoryStorage();

// Unmount between tests. Components here portal into document.body and
// register document-level listeners, so a leaked mount is not just a
// memory concern — a stale Modal would keep claiming Escape and quietly
// change the next test's outcome.
afterEach(() => {
  cleanup();
});

// jsdom implements neither of these, and both are load-bearing in the
// components under test rather than incidental.
if (!window.matchMedia) {
  window.matchMedia = ((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  })) as typeof window.matchMedia;
}

if (!window.requestAnimationFrame) {
  window.requestAnimationFrame = ((cb: FrameRequestCallback) =>
    setTimeout(() => cb(performance.now()), 0) as unknown as number);
  window.cancelAnimationFrame = ((id: number) => clearTimeout(id)) as typeof window.cancelAnimationFrame;
}

// jsdom has no layout engine and ships no `scrollIntoView` at all — the method
// is absent rather than a no-op. Components that keep a live region pinned to
// the bottom call it from a mount effect, so without this they throw during
// render for reasons unrelated to whatever is under test.
if (!Element.prototype.scrollIntoView) {
  Element.prototype.scrollIntoView = () => {};
}

// jsdom reports every element as having no layout, so `offsetParent` is
// null for everything. The Modal's focus trap filters candidates by
// visibility using exactly that, which would leave it with nothing to
// focus and make every trap test vacuous. Report elements as laid out
// unless a test explicitly hides them.
Object.defineProperty(HTMLElement.prototype, 'offsetParent', {
  configurable: true,
  get(this: HTMLElement) {
    if (this.hidden || this.style.display === 'none') return null;
    return this.parentElement ?? document.body;
  },
});

expect.extend({});

// Surface unhandled console.error output (React key warnings, act()
// warnings) instead of letting it scroll past in CI.
const originalError = console.error;
vi.spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
  originalError(...(args as Parameters<typeof console.error>));
});
