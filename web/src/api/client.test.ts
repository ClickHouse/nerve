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
const { ApiError, setAuthMode, setBeforeReenter, useHostedStore } = await import('./hosted');

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

/** The headers of one recorded `fetch` call. */
function sentHeaders(call = 0): Record<string, string> {
  return fetchMock.mock.calls[call][1].headers as Record<string, string>;
}

function gatewayError(status: number, reason: string): Response {
  return new Response(JSON.stringify({ reason, requestId: 'r-1' }), {
    status, headers: { 'Content-Type': 'application/json' },
  });
}

describe('local requests', () => {
  it('send the bearer and no CSRF header', async () => {
    setToken('session-one');
    fetchMock.mockResolvedValue(response(200));

    await api.deleteSession('s1');

    expect(sentHeaders()['Authorization']).toBe('Bearer session-one');
    expect(sentHeaders()).not.toHaveProperty('X-Nerve-CSRF');
  });

  it('fail with a typed error that keeps the "status: body" message', async () => {
    fetchMock.mockResolvedValue(new Response('{"detail":"gone"}', { status: 404 }));

    const error = await api.getSession('s1').catch((e: unknown) => e);

    expect(error).toBeInstanceOf(ApiError);
    expect((error as InstanceType<typeof ApiError>).message).toBe('404: {"detail":"gone"}');
    expect((error as InstanceType<typeof ApiError>).status).toBe(404);
  });

  it('keep the local handling for an error without a gateway body', async () => {
    fetchMock.mockResolvedValue(
      new Response('{"detail":{"reason":"refused"}}', { status: 409 }),
    );

    await expect(api.getSession('s1')).rejects.toThrow('409: ');

    expect(useHostedStore.getState().mode).toBe('local');
    expect(useHostedStore.getState().problem).toBeNull();
  });
});

describe('a gateway error body in local mode', () => {
  let assign: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    useHostedStore.setState({ mode: 'local', problem: null, reentering: false });
    setBeforeReenter(null);
    assign = vi.fn();
    vi.stubGlobal('location', {
      ...window.location, pathname: '/', search: '', assign,
    });
  });

  afterEach(() => {
    useHostedStore.setState({ mode: 'local', problem: null, reentering: false });
  });

  it('sets hosted mode and shows the screen when the status call fails', async () => {
    fetchMock.mockResolvedValue(gatewayError(503, 'agent_starting'));

    await expect(api.authStatus()).rejects.toThrow('503: ');

    expect(useHostedStore.getState().mode).toBe('external');
    expect(useHostedStore.getState().problem).toBe('unavailable');
  });

  it('signs in through the gateway on a gateway 401, not through the local login', async () => {
    setToken('session-one');
    fetchMock.mockResolvedValue(gatewayError(401, 'login_required'));

    await expect(api.authStatus()).rejects.toThrow('401: ');

    expect(useHostedStore.getState().mode).toBe('external');
    expect(assign).toHaveBeenCalledWith('/_nerve/login?return_to=%2F');
    expect(unauthorized).not.toHaveBeenCalled();
    expect(getToken()).toBe('session-one');
  });
});

describe('hosted requests', () => {
  let assign: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    useHostedStore.setState({ mode: 'local', problem: null, reentering: false });
    setBeforeReenter(null);
    setAuthMode('external');
    assign = vi.fn();
    vi.stubGlobal('location', {
      ...window.location, pathname: '/chat/s1', search: '', assign,
    });
  });

  afterEach(() => {
    useHostedStore.setState({ mode: 'local', problem: null, reentering: false });
  });

  it('send no bearer, also when a token is stored', async () => {
    setToken('stale');
    fetchMock.mockResolvedValue(response(200));

    await api.checkAuth();

    expect(sentHeaders()).not.toHaveProperty('Authorization');
    expect(sentHeaders()).not.toHaveProperty('X-Nerve-CSRF');
  });

  it('ignore a refreshed token', async () => {
    fetchMock.mockResolvedValue(response(200, 'minted'));

    await api.checkAuth();

    expect(getToken()).toBeNull();
    expect(data.has('nerve_token')).toBe(false);
  });

  it('send the CSRF header on a method that can change state', async () => {
    fetchMock.mockImplementation(async () => response(200));

    await api.deleteSession('s1');
    await api.createSession('a title');

    expect(sentHeaders(0)['X-Nerve-CSRF']).toBe('1');
    expect(sentHeaders(1)['X-Nerve-CSRF']).toBe('1');
    expect(sentHeaders(1)['Content-Type']).toBe('application/json');
  });

  it('send the CSRF header and no bearer with an upload', async () => {
    setToken('stale');
    fetchMock.mockResolvedValue(new Response(JSON.stringify({ files: [] }), { status: 200 }));

    await api.uploadFiles([new File(['x'], 'a.txt')], 's1');

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe('/api/files/upload');
    expect(init.method).toBe('POST');
    expect(init.body).toBeInstanceOf(FormData);
    expect(init.headers).toEqual({ 'X-Nerve-CSRF': '1' });
  });

  it('do not sign in again on a 401 from Nerve while the gateway session is valid', async () => {
    // Nerve refuses the request, for example for an actor header that it
    // cannot read. A login would come back to the same 401.
    fetchMock.mockImplementation(async (url: string) => (
      url === '/_nerve/session'
        ? new Response('{"authenticated":true}', { status: 200 })
        : new Response('{"detail":"Invalid actor context"}', { status: 401 })
    ));

    await expect(api.listSessions()).rejects.toThrow('401: ');
    await vi.waitFor(() => expect(useHostedStore.getState().problem).toBe('unavailable'));

    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual(['/api/sessions?offset=0', '/_nerve/session']);
    expect(assign).not.toHaveBeenCalled();
    expect(useHostedStore.getState().reentering).toBe(false);
  });

  it('apply the same rule to a 401 from Nerve on an upload', async () => {
    fetchMock.mockImplementation(async (url: string) => (
      url === '/_nerve/session'
        ? new Response('{"authenticated":true}', { status: 200 })
        : new Response('{"detail":"Invalid actor context"}', { status: 401 })
    ));

    await expect(api.uploadFiles([new File(['x'], 'a.txt')], 's1')).rejects.toThrow('401: ');
    await vi.waitFor(() => expect(useHostedStore.getState().problem).toBe('unavailable'));

    expect(assign).not.toHaveBeenCalled();
  });

  it('sign in again after a 401 from Nerve when the gateway session is gone', async () => {
    fetchMock.mockImplementation(async (url: string) => (
      url === '/_nerve/session'
        ? gatewayError(401, 'login_required')
        : new Response('{"detail":"Not authenticated"}', { status: 401 })
    ));

    await expect(api.listSessions()).rejects.toThrow('401: ');

    await vi.waitFor(() => expect(assign).toHaveBeenCalledWith('/_nerve/login?return_to=%2Fchat%2Fs1'));
  });

  it('keep the drafts and sign in again through the gateway on 401', async () => {
    const keep = vi.fn();
    setBeforeReenter(keep);
    setToken('stale');
    fetchMock.mockResolvedValue(gatewayError(401, 'login_required'));

    const error = await api.listSessions().catch((e: unknown) => e);

    expect(error).toBeInstanceOf(ApiError);
    expect((error as InstanceType<typeof ApiError>).status).toBe(401);
    expect(keep).toHaveBeenCalledOnce();
    expect(assign).toHaveBeenCalledWith('/_nerve/login?return_to=%2Fchat%2Fs1');
    // The local expiry path does not run.
    expect(unauthorized).not.toHaveBeenCalled();
    expect(getToken()).toBe('stale');
  });

  it('sign in again at once on a gateway 401 to an upload', async () => {
    fetchMock.mockResolvedValue(gatewayError(401, 'login_required'));

    await expect(api.uploadFiles([new File(['x'], 'a.txt')], 's1')).rejects.toThrow('401: ');

    expect(assign).toHaveBeenCalledOnce();
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it('show the no-access screen on 403 access_denied, without a login', async () => {
    fetchMock.mockResolvedValue(gatewayError(403, 'access_denied'));

    const error = await api.listSessions().catch((e: unknown) => e);

    expect((error as InstanceType<typeof ApiError>).reason).toBe('access_denied');
    expect(useHostedStore.getState().problem).toBe('access_denied');
    expect(assign).not.toHaveBeenCalled();
  });

  it('keep the no-access screen when later requests get 401', async () => {
    fetchMock.mockResolvedValueOnce(gatewayError(403, 'access_denied'));
    await api.listSessions().catch(() => {});

    fetchMock.mockResolvedValueOnce(gatewayError(401, 'login_required'));
    await api.listSessions().catch(() => {});

    expect(useHostedStore.getState().problem).toBe('access_denied');
    expect(assign).not.toHaveBeenCalled();
  });

  it('treat another 403 as an ordinary error', async () => {
    fetchMock.mockResolvedValue(gatewayError(403, 'invalid_origin'));

    await expect(api.deleteSession('s1')).rejects.toThrow('403: ');

    expect(useHostedStore.getState().problem).toBeNull();
  });

  it('show the archived screen on 410 agent_archived', async () => {
    fetchMock.mockResolvedValue(gatewayError(410, 'agent_archived'));

    await expect(api.listSessions()).rejects.toThrow('410: ');

    expect(useHostedStore.getState().problem).toBe('agent_archived');
  });

  it.each([
    [503, 'unavailable'], [503, 'backend_unavailable'], [503, 'agent_starting'],
    [503, 'agent_paused'], [503, 'agent_unavailable'], [504, 'backend_timeout'],
  ])('show the unavailable screen on %i %s', async (status, reason) => {
    fetchMock.mockResolvedValue(gatewayError(status, reason));

    await expect(api.listSessions()).rejects.toThrow(`${status}: `);

    expect(useHostedStore.getState().problem).toBe('unavailable');
  });
});
