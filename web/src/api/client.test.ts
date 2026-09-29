// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// Node can expose a non-jsdom localStorage. Install the storage the client
// reads at module initialization before importing it.
const data = new Map<string, string>();
const storage = {
  getItem: (key: string) => data.get(key) ?? null,
  setItem: (key: string, value: string) => void data.set(key, String(value)),
  removeItem: (key: string) => void data.delete(key),
  clear: () => data.clear(),
  key: (index: number) => [...data.keys()][index] ?? null,
  get length() { return data.size; },
};
for (const target of [globalThis, globalThis.window]) {
  Object.defineProperty(target, 'localStorage', {
    value: storage, configurable: true, writable: true,
  });
}

const { api, clearToken, getToken, setToken, setUnauthorizedHandler } =
  await import('./client');

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  return {
    promise: new Promise<T>((done) => { resolve = done; }),
    resolve: (value) => resolve(value),
  };
}

function response(status: number, refreshedToken?: string): Response {
  const headers = new Headers({ 'Content-Type': 'application/json' });
  if (refreshedToken) headers.set('X-Nerve-Token', refreshedToken);
  return new Response(JSON.stringify({ authenticated: status === 200 }), {
    status,
    headers,
  });
}

let fetchMock: ReturnType<typeof vi.fn>;
let unauthorized: ReturnType<typeof vi.fn>;

beforeEach(() => {
  data.clear();
  clearToken();
  unauthorized = vi.fn();
  setUnauthorizedHandler(unauthorized);
  fetchMock = vi.fn();
  vi.stubGlobal('fetch', fetchMock);
});

afterEach(() => vi.unstubAllGlobals());

describe('token response ownership', () => {
  it('absorbs a refreshed token for the revision that sent the request', async () => {
    setToken('session-one');
    fetchMock.mockResolvedValue(response(200, 'session-one-slid'));

    await api.checkAuth();

    expect(getToken()).toBe('session-one-slid');
  });

  it('ignores a late refresh after logout and another login', async () => {
    setToken('session-one');
    const pending = deferred<Response>();
    fetchMock.mockReturnValue(pending.promise);
    const request = api.checkAuth();

    clearToken();
    setToken('session-two');
    pending.resolve(response(200, 'session-one-slid'));
    await request;

    expect(getToken()).toBe('session-two');
  });

  it('clears and reports a 401 from the current revision', async () => {
    setToken('session-one');
    fetchMock.mockResolvedValue(response(401));

    await expect(api.checkAuth()).rejects.toThrow('Unauthorized');

    expect(getToken()).toBeNull();
    expect(unauthorized).toHaveBeenCalledOnce();
  });

  it('does not let a stale 401 clear or expire a newer login', async () => {
    setToken('session-one');
    const pending = deferred<Response>();
    fetchMock.mockReturnValue(pending.promise);
    const request = api.checkAuth();

    clearToken();
    setToken('session-two');
    pending.resolve(response(401));
    await expect(request).rejects.toThrow('Unauthorized');

    expect(getToken()).toBe('session-two');
    expect(unauthorized).not.toHaveBeenCalled();
  });
});
