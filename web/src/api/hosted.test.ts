import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ApiError, authUrl, csrfHeaders, hostedLogout, isHosted, loginUrl, probeSession,
  problemOf, reenter, reportError, setAuthMode, setBeforeReenter, setTokenReader,
  showProblem, useHostedStore,
} from './hosted';

let assign: ReturnType<typeof vi.fn>;
let fetchMock: ReturnType<typeof vi.fn>;

/** Put the tab at a path, with a `location.assign` that records navigations. */
function at(pathname: string, search = ''): void {
  vi.stubGlobal('location', { ...window.location, pathname, search, assign });
}

function reply(status: number, body: unknown = {}): Response {
  return new Response(JSON.stringify(body), {
    status, headers: { 'Content-Type': 'application/json' },
  });
}

beforeEach(() => {
  useHostedStore.setState({ mode: 'local', problem: null, reentering: false });
  setBeforeReenter(null);
  setTokenReader(() => null);
  assign = vi.fn();
  fetchMock = vi.fn();
  vi.stubGlobal('fetch', fetchMock);
  at('/chat/s1', '?q=1');
});

afterEach(() => vi.unstubAllGlobals());

describe('mode', () => {
  it('is local until the status names external', () => {
    expect(isHosted()).toBe(false);

    setAuthMode('external');

    expect(isHosted()).toBe(true);
  });

  it('treats a missing or unknown mode as local', () => {
    for (const mode of [undefined, null, 'local', 'hosted', 'EXTERNAL', 1]) {
      setAuthMode('external');
      setAuthMode(mode);
      expect(isHosted()).toBe(false);
    }
  });
});

describe('authUrl', () => {
  it('adds the local token, with the correct separator', () => {
    setTokenReader(() => 'tok');

    expect(authUrl('/api/files/a')).toBe('/api/files/a?token=tok');
    expect(authUrl('/api/files/a?download=1')).toBe('/api/files/a?download=1&token=tok');
  });

  it('adds nothing in local mode when the tab has no token', () => {
    expect(authUrl('/api/files/a')).toBe('/api/files/a');
    expect(authUrl('/api/files/a')).not.toContain('null');
  });

  it('adds nothing in hosted mode, also when a token is stored', () => {
    setTokenReader(() => 'stale');
    setAuthMode('external');

    expect(authUrl('/api/files/a')).toBe('/api/files/a');
    expect(authUrl('ws://host/ws')).toBe('ws://host/ws');
  });
});

describe('csrfHeaders', () => {
  it('marks every method that can change state', () => {
    for (const method of ['POST', 'put', 'PATCH', 'DELETE']) {
      expect(csrfHeaders(method)).toEqual({ 'X-Nerve-CSRF': '1' });
    }
  });

  it('leaves safe methods alone', () => {
    for (const method of [undefined, 'GET', 'head', 'OPTIONS']) {
      expect(csrfHeaders(method)).toEqual({});
    }
  });
});

describe('ApiError', () => {
  it('keeps the "status: body" message and reads the gateway reason', () => {
    const body = '{"reason":"access_denied","requestId":"r-1"}';
    const error = new ApiError(403, body);

    expect(error).toBeInstanceOf(Error);
    expect(error.message).toBe(`403: ${body}`);
    expect(error.status).toBe(403);
    expect(error.reason).toBe('access_denied');
  });

  it('has no reason for a body that is not a gateway error', () => {
    expect(new ApiError(500, 'Internal Server Error').reason).toBeNull();
    expect(new ApiError(404, '{"detail":"Not Found"}').reason).toBeNull();
    expect(new ApiError(400, '{"reason":7}').reason).toBeNull();
  });
});

describe('problemOf', () => {
  it('maps the gateway answers that stop or pause the app', () => {
    expect(problemOf(403, 'access_denied')).toBe('access_denied');
    expect(problemOf(410, 'agent_archived')).toBe('agent_archived');
    expect(problemOf(503, 'agent_archived')).toBe('agent_archived');
    expect(problemOf(503, 'unavailable')).toBe('unavailable');
    expect(problemOf(503, 'backend_unavailable')).toBe('unavailable');
    expect(problemOf(502, 'backend_unavailable')).toBe('unavailable');
  });

  it('ignores every other answer', () => {
    expect(problemOf(403, 'invalid_origin')).toBeNull();
    expect(problemOf(403, null)).toBeNull();
    expect(problemOf(404, 'unknown_agent')).toBeNull();
    expect(problemOf(500, null)).toBeNull();
    expect(problemOf(503, null)).toBeNull();
  });
});

describe('showProblem', () => {
  it('does not let a temporary problem replace one that stops the app', () => {
    reportError(new ApiError(403, '{"reason":"access_denied"}'));
    showProblem('unavailable');

    expect(useHostedStore.getState().problem).toBe('access_denied');
  });
});

describe('reenter', () => {
  it('keeps the unsent text, then goes to the gateway login for this page', () => {
    const order: string[] = [];
    setBeforeReenter(() => order.push('keep'));
    assign.mockImplementation(() => order.push('assign'));

    reenter();

    expect(order).toEqual(['keep', 'assign']);
    expect(assign).toHaveBeenCalledWith('/_nerve/login?return_to=%2Fchat%2Fs1%3Fq%3D1');
    expect(useHostedStore.getState().reentering).toBe(true);
  });

  it('acts only once', () => {
    const keep = vi.fn();
    setBeforeReenter(keep);

    reenter();
    reenter();

    expect(keep).toHaveBeenCalledOnce();
    expect(assign).toHaveBeenCalledOnce();
  });

  it('still goes to the login when keeping the text fails', () => {
    vi.spyOn(console, 'error').mockImplementationOnce(() => {});
    setBeforeReenter(() => { throw new Error('quota'); });

    reenter();

    expect(assign).toHaveBeenCalledOnce();
  });

  it('does nothing while a problem stops the app', () => {
    showProblem('access_denied');

    reenter();

    expect(assign).not.toHaveBeenCalled();
    expect(useHostedStore.getState().problem).toBe('access_denied');
  });

  it('shows the unavailable screen and starts no login loop without the gateway', () => {
    at('/_nerve/login', '?return_to=%2F');

    reenter();

    expect(assign).not.toHaveBeenCalled();
    expect(useHostedStore.getState().problem).toBe('unavailable');
  });

  it('returns to the root when the gateway would refuse the path', () => {
    at('//other.example/x');
    expect(loginUrl()).toBe('/_nerve/login?return_to=%2F');

    at('/chat', `?q=${'a'.repeat(2048)}`);
    expect(loginUrl()).toBe('/_nerve/login?return_to=%2F');
  });
});

describe('probeSession', () => {
  it('asks the gateway and allows a retry when the session is valid', async () => {
    fetchMock.mockResolvedValue(reply(200, { authenticated: true }));

    await expect(probeSession()).resolves.toBe(true);

    expect(fetchMock.mock.calls[0][0]).toBe('/_nerve/session');
    expect(assign).not.toHaveBeenCalled();
    expect(useHostedStore.getState().problem).toBeNull();
  });

  it('signs in again on 401', async () => {
    fetchMock.mockResolvedValue(reply(401, { reason: 'login_required' }));

    await expect(probeSession()).resolves.toBe(false);

    expect(assign).toHaveBeenCalledWith('/_nerve/login?return_to=%2Fchat%2Fs1%3Fq%3D1');
  });

  it('shows the no-access screen on 403', async () => {
    fetchMock.mockResolvedValue(reply(403, { reason: 'access_denied' }));

    await expect(probeSession()).resolves.toBe(false);

    expect(useHostedStore.getState().problem).toBe('access_denied');
    expect(assign).not.toHaveBeenCalled();
  });

  it('shows the archived screen on 410', async () => {
    fetchMock.mockResolvedValue(reply(410, { reason: 'agent_archived' }));

    await expect(probeSession()).resolves.toBe(false);

    expect(useHostedStore.getState().problem).toBe('agent_archived');
  });

  it('allows a retry when the gateway gives no decision', async () => {
    fetchMock.mockResolvedValueOnce(reply(503, { reason: 'unavailable' }));
    await expect(probeSession()).resolves.toBe(true);

    fetchMock.mockRejectedValueOnce(new TypeError('network'));
    await expect(probeSession()).resolves.toBe(true);

    expect(assign).not.toHaveBeenCalled();
  });
});

describe('hostedLogout', () => {
  it('ends the gateway session with the CSRF header, then goes to the root', async () => {
    fetchMock.mockResolvedValue(new Response(null, { status: 204 }));

    await hostedLogout();

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe('/_nerve/logout');
    expect(init.method).toBe('POST');
    expect(init.headers).toEqual({ 'X-Nerve-CSRF': '1' });
    expect(assign).toHaveBeenCalledWith('/');
  });

  it('goes to the root also when the request fails', async () => {
    fetchMock.mockRejectedValue(new TypeError('network'));

    await hostedLogout();

    expect(assign).toHaveBeenCalledWith('/');
  });
});
