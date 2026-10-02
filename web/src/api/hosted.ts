import { create } from 'zustand';

/**
 * The browser side of hosted Nerve.
 *
 * Hosted Nerve runs in external authentication mode behind the gateway. The
 * gateway signs the browser in with its own session cookie and tells Nerve
 * who the actor is. The browser holds no Nerve token: it sends no
 * `Authorization` header and adds no `?token=` to a URL. The gateway owns
 * login (`/_nerve/login`), the session check (`/_nerve/session`) and logout
 * (`/_nerve/logout`).
 *
 * `GET /api/auth/status` reports the mode. The mode is local until the status
 * answers, and also when the status has no `mode` or an unknown one, so the UI
 * works with an older backend.
 */

/** The authentication mode that `GET /api/auth/status` reports. */
export type AuthMode = 'local' | 'external';

/**
 * A gateway answer that the app shows on its own screen. `access_denied` and
 * `agent_archived` stop the app until the page loads again. `unavailable` is
 * temporary.
 */
export type HostedProblem = 'access_denied' | 'agent_archived' | 'unavailable';

interface HostedState {
  mode: AuthMode;
  problem: HostedProblem | null;
  /** The page is on its way to the gateway login. */
  reentering: boolean;
}

export const useHostedStore = create<HostedState>(() => ({
  mode: 'local',
  problem: null,
  reentering: false,
}));

/** Set the mode from the `mode` field of `GET /api/auth/status`. */
export function setAuthMode(mode: unknown): void {
  useHostedStore.setState({ mode: mode === 'external' ? 'external' : 'local' });
}

/** Whether this Nerve runs in external mode behind the gateway. */
export function isHosted(): boolean {
  return useHostedStore.getState().mode === 'external';
}

/** The path prefix that the gateway keeps for its own routes. */
const GATEWAY_PREFIX = '/_nerve/';
const CSRF_HEADER = 'X-Nerve-CSRF';
const SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);

/** The header that the gateway requires on a cookie request with an unsafe method. */
export function csrfHeaders(method: string | undefined): Record<string, string> {
  return SAFE_METHODS.has((method ?? 'GET').toUpperCase()) ? {} : { [CSRF_HEADER]: '1' };
}

let readToken: () => string | null = () => null;

/**
 * Install the function that reads the local session token. `api/client`
 * calls this when it loads, so this module does not import the client.
 */
export function setTokenReader(reader: () => string | null): void {
  readToken = reader;
}

/**
 * A URL that the browser loads without request headers (an image, a download
 * link, the WebSocket), with the local session token in `?token=`. In hosted
 * mode, and when this tab has no token, the URL does not change.
 */
export function authUrl(url: string): string {
  if (isHosted()) return url;
  const token = readToken();
  if (!token) return url;
  return `${url}${url.includes('?') ? '&' : '?'}token=${encodeURIComponent(token)}`;
}

/**
 * A response that is not OK. The message is `"<status>: <body>"`, which
 * `errorDetail` parses. `reason` is the reason code of a gateway error body
 * (`{"reason": "..."}`), and `null` for every other body.
 */
export class ApiError extends Error {
  readonly status: number;
  readonly reason: string | null;

  constructor(status: number, body: string) {
    super(`${status}: ${body}`);
    this.status = status;
    this.reason = reasonOf(body);
  }
}

function reasonOf(body: string): string | null {
  try {
    const parsed: unknown = JSON.parse(body);
    if (parsed && typeof parsed === 'object' && 'reason' in parsed
      && typeof parsed.reason === 'string') {
      return parsed.reason;
    }
  } catch {
    // Not JSON, so not a gateway error.
  }
  return null;
}

/** The problem that a gateway error shows, or `null` for an ordinary error. */
export function problemOf(status: number, reason: string | null): HostedProblem | null {
  if (status === 403 && reason === 'access_denied') return 'access_denied';
  // The gateway answers 410 for an archived agent host, and 503 when the
  // machine plane cannot start an archived agent.
  if ((status === 410 || status === 503) && reason === 'agent_archived') return 'agent_archived';
  if (status === 503 && reason === 'unavailable') return 'unavailable';
  // 503 before the relay confirmed the agent VM, 502 when the connection to
  // the VM broke before a response.
  if ((status === 502 || status === 503) && reason === 'backend_unavailable') return 'unavailable';
  return null;
}

/** Whether a problem stops the app until the page loads again. */
export function stopsApp(problem: HostedProblem | null): problem is 'access_denied' | 'agent_archived' {
  return problem === 'access_denied' || problem === 'agent_archived';
}

/**
 * Show the screen of a problem. `unavailable` does not replace a problem that
 * stops the app.
 */
export function showProblem(problem: HostedProblem): void {
  useHostedStore.setState((state) => (
    problem === 'unavailable' && state.problem !== null && state.problem !== 'unavailable'
      ? state
      : { problem }
  ));
}

/** Show the screen of a failed API request, if the gateway sent it. */
export function reportError(error: ApiError): void {
  const problem = problemOf(error.status, error.reason);
  if (problem) showProblem(problem);
}

/** Remove the `unavailable` screen, so that the app can try again. */
export function clearUnavailable(): void {
  useHostedStore.setState((state) => (state.problem === 'unavailable' ? { problem: null } : state));
}

let beforeReenter: (() => void) | null = null;

/**
 * Install the work that keeps unsent text before the page leaves for the
 * gateway login. The chat store calls this when it loads.
 */
export function setBeforeReenter(hook: (() => void) | null): void {
  beforeReenter = hook;
}

/** The longest `return_to` that the gateway accepts. */
const MAX_RETURN_TO = 2048;

/** The gateway login URL that comes back to the current page. */
export function loginUrl(): string {
  const target = window.location.pathname + window.location.search;
  // The gateway accepts only a relative path. Use the root for anything else.
  const returnTo = target.startsWith('/') && !target.startsWith('//')
    && target.length <= MAX_RETURN_TO ? target : '/';
  return `${GATEWAY_PREFIX}login?return_to=${encodeURIComponent(returnTo)}`;
}

/**
 * Sign in again through the gateway. Keep the unsent text in the drafts, then
 * go to the gateway login, which comes back to this page. Only the first call
 * has an effect.
 *
 * Re-entry does nothing while a problem stops the app: the gateway removes
 * its session cookie when it refuses access, so later requests get 401, and
 * the screen must stay. A page at a path below `/_nerve/` runs without the
 * gateway, because the gateway does not forward those paths. A login from
 * there loads this page again, so re-entry shows the unavailable screen and
 * does not navigate.
 */
export function reenter(): void {
  const { reentering, problem } = useHostedStore.getState();
  if (reentering || stopsApp(problem)) return;
  if (window.location.pathname.startsWith(GATEWAY_PREFIX)) {
    showProblem('unavailable');
    return;
  }
  useHostedStore.setState({ reentering: true });
  try {
    beforeReenter?.();
  } catch (e) {
    console.error('Could not keep the unsent text:', e);
  }
  window.location.assign(loginUrl());
}

/**
 * Ask the gateway if the session of this tab is still valid. The WebSocket
 * calls this before it opens a new socket, because the browser does not show
 * why an upgrade failed.
 *
 * A 401 starts re-entry. A 403 or a 410 shows its screen. Returns `true` when
 * the caller can retry: the session is valid (so the backend is down), or the
 * gateway gave no decision.
 */
export async function probeSession(): Promise<boolean> {
  let res: Response;
  try {
    res = await fetch(`${GATEWAY_PREFIX}session`, {
      headers: { Accept: 'application/json' },
      credentials: 'same-origin',
    });
  } catch {
    return true;
  }
  switch (res.status) {
    case 401:
      reenter();
      return false;
    case 403:
      showProblem('access_denied');
      return false;
    case 410:
      showProblem('agent_archived');
      return false;
    default:
      return true;
  }
}

/**
 * End the gateway session, then go to the root page. The gateway sends a
 * navigation without a session to its login.
 */
export async function hostedLogout(): Promise<void> {
  try {
    await fetch(`${GATEWAY_PREFIX}logout`, {
      method: 'POST',
      headers: { [CSRF_HEADER]: '1' },
      credentials: 'same-origin',
    });
  } catch {
    // Go to the root page all the same. A session that is still valid opens the app again.
  }
  window.location.assign('/');
}
