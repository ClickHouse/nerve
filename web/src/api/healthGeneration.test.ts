import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * `api.healthGeneration()` is the restart wait's probe. It never goes
 * through `request()`, so a failing answer cannot sign the tab out, and it
 * falls back to a raw authenticated `GET /api/setup` once `/health` has
 * answered twice in a row with anything that is not Nerve's JSON — a 404,
 * an auth wall's 401/403, a 405, an HTML page — which is what a front
 * gateway that does not forward `/health` looks like. Only 5xx and network
 * errors mean "still restarting".
 *
 * The miss counter is module state, so every case starts from a fresh
 * module.
 */

type ClientModule = typeof import('./client');
type HostedModule = typeof import('./hosted');
type FetchMock = ReturnType<typeof vi.fn>;

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
}

let api: ClientModule['api'];
let setToken: ClientModule['setToken'];
let setAuthMode: HostedModule['setAuthMode'];
let fetchMock: FetchMock;

beforeEach(async () => {
  vi.resetModules();
  fetchMock = vi.fn();
  vi.stubGlobal('fetch', fetchMock);
  ({ api, setToken } = await import('./client'));
  ({ setAuthMode } = await import('./hosted'));
});

afterEach(() => {
  vi.unstubAllGlobals();
});

const setupCall = () => fetchMock.mock.calls.find(([url]) => url === '/api/setup') as [string, RequestInit] | undefined;

describe('healthGeneration', () => {
  it('reads the generation from /health with no credentials', async () => {
    fetchMock.mockResolvedValue(jsonResponse(200, { status: 'ok', generation: 'G7' }));
    expect(await api.healthGeneration()).toBe('G7');
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe('/health');
    expect(init.headers).toBeUndefined();
  });

  it('treats a 503 and a network error as "still restarting", without ever falling back', async () => {
    fetchMock.mockResolvedValueOnce(new Response('bad gateway', { status: 503 }));
    expect(await api.healthGeneration()).toBeNull();
    fetchMock.mockRejectedValueOnce(new TypeError('connection refused'));
    expect(await api.healthGeneration()).toBeNull();
    fetchMock.mockResolvedValueOnce(new Response('bad gateway', { status: 502 }));
    expect(await api.healthGeneration()).toBeNull();
    expect(fetchMock.mock.calls.every(([url]) => url === '/health')).toBe(true);
  });

  it('falls back to a raw /api/setup with the gateway cookie after two 404s in hosted mode', async () => {
    setAuthMode('external');
    fetchMock.mockImplementation(async (url: string) => (
      url === '/health'
        ? new Response('not found', { status: 404 })
        : jsonResponse(200, { generation: 'G9', steps: [] })
    ));
    expect(await api.healthGeneration()).toBeNull();   // first miss
    expect(await api.healthGeneration()).toBe('G9');   // second miss → fallback
    const call = setupCall();
    expect(call).toBeDefined();
    expect(call![1].credentials).toBe('same-origin');
    expect((call![1].headers as Record<string, string>).Authorization).toBeUndefined();
    expect(JSON.stringify(call![0])).not.toContain('token=');
  });

  it('counts an auth wall answering /health with 401 twice as misses and falls back', async () => {
    setAuthMode('external');
    fetchMock.mockImplementation(async (url: string) => (
      url === '/health'
        ? new Response('{"reason":"login_required"}', { status: 401 })
        : jsonResponse(200, { generation: 'G4' })
    ));
    expect(await api.healthGeneration()).toBeNull();   // first miss, no fallback yet
    expect(setupCall()).toBeUndefined();
    expect(await api.healthGeneration()).toBe('G4');   // second miss → fallback engaged
    expect(setupCall()).toBeDefined();
    // From here on the probe goes straight to the fallback.
    expect(await api.healthGeneration()).toBe('G4');
    expect(fetchMock.mock.calls.filter(([url]) => url === '/health')).toHaveLength(2);
  });

  it('treats 403 and 405 the same way, and a 401 from the fallback as "not yet"', async () => {
    setAuthMode('external');
    let setupStatus = 401;
    fetchMock.mockImplementation(async (url: string) => {
      if (url === '/health') return new Response('forbidden', { status: fetchMock.mock.calls.length === 1 ? 403 : 405 });
      return setupStatus === 200 ? jsonResponse(200, { generation: 'G5' }) : new Response('', { status: setupStatus });
    });
    expect(await api.healthGeneration()).toBeNull();
    expect(await api.healthGeneration()).toBeNull();   // fallback reached, answered 401: still restarting
    expect(setupCall()).toBeDefined();
    setupStatus = 200;
    expect(await api.healthGeneration()).toBe('G5');
  });

  it('sends the bearer, not a ?token=, when falling back in local mode after two gateway pages', async () => {
    setToken('local-session');
    fetchMock.mockImplementation(async (url: string) => (
      url === '/health'
        ? new Response('<html>gateway</html>', { status: 200 })
        : jsonResponse(200, { generation: 'G3' })
    ));
    expect(await api.healthGeneration()).toBeNull();   // a 200 that is not JSON: a gateway page
    expect(await api.healthGeneration()).toBe('G3');
    const call = setupCall()!;
    expect((call[1].headers as Record<string, string>).Authorization).toBe('Bearer local-session');
    expect(call[0]).not.toContain('token=');
  });
});
