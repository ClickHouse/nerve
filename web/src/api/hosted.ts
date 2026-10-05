import { create } from 'zustand';

/**
 * The browser side of hosted Nerve.
 *
 * Hosted Nerve runs in external authentication mode behind the gateway. The
 * gateway signs the browser in with its own session cookie and tells Nerve
 * who the actor is. The browser holds no Nerve token: it sends no
 * `Authorization` header and adds no `?token=` to a URL. The gateway owns
 * login (`/_nerve/login`) and the session check (`/_nerve/session`). Hosted
 * account controls belong to the control plane; the agent UI has no logout.
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
  /** Fixed for this page after /api/auth/me verifies the viewer. */
  principalId: string | null;
  problem: HostedProblem | null;
  /** The page is on its way to the gateway login. */
  reentering: boolean;
}

export const useHostedStore = create<HostedState>(() => ({
  mode: 'local',
  principalId: null,
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

/** A different person must enter a fresh page, with their own saved state. */
export function bindHostedPrincipal(principalId: string): boolean {
  const current = useHostedStore.getState().principalId;
  if (current !== null && current !== principalId) {
    reenter();
    return false;
  }
  useHostedStore.setState({ principalId });
  return true;
}

export function hostedSessionActive(): boolean {
  const { principalId, reentering, problem } = useHostedStore.getState();
  return principalId !== null && !reentering && !stopsApp(problem);
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
 * `errorDetail` parses. `reason` is the reason code of a gateway error body,
 * and `null` for every other body. Only the gateway writes a body with both
 * a string `reason` and a string `requestId`. Nerve puts its own error
 * details below `detail`.
 */
export class ApiError extends Error {
  readonly status: number;
  readonly reason: string | null;

  constructor(status: number, body: string) {
    super(`${status}: ${body}`);
    this.status = status;
    this.reason = gatewayReasonOf(body);
  }
}

function gatewayReasonOf(body: string): string | null {
  try {
    const parsed: unknown = JSON.parse(body);
    if (parsed && typeof parsed === 'object' && 'reason' in parsed && 'requestId' in parsed
      && typeof parsed.reason === 'string' && typeof parsed.requestId === 'string') {
      return parsed.reason;
    }
  } catch {
    // Not JSON, so not a gateway error.
  }
  return null;
}

/** Gateway reasons that mean the agent cannot answer at this time. */
const TEMPORARY_REASONS: Record<number, readonly string[]> = {
  // 502: the connection to the agent VM broke before a response.
  502: ['backend_unavailable'],
  // 503: the gateway has no access data yet, the agent VM is not ready or
  // cannot be reached, or the machine plane cannot start it.
  503: ['unavailable', 'backend_unavailable', 'agent_starting', 'agent_paused', 'agent_unavailable'],
  // 504: the agent VM took the request and sent no response in time.
  504: ['backend_timeout'],
};

/** The problem that a gateway error shows, or `null` for an ordinary error. */
export function problemOf(status: number, reason: string | null): HostedProblem | null {
  if (status === 403 && reason === 'access_denied') return 'access_denied';
  // The gateway answers 410 for an archived agent host, and 503 when the
  // machine plane cannot start an archived agent.
  if ((status === 410 || status === 503) && reason === 'agent_archived') return 'agent_archived';
  if (reason !== null && TEMPORARY_REASONS[status]?.includes(reason)) return 'unavailable';
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

/**
 * Act on a failed API request in hosted mode. A 401 signs in again or checks
 * the session (see `reportUnauthorized`). A gateway error with a known
 * reason shows its screen. The promise settles when any session check has
 * answered.
 */
export function reportError(error: ApiError): Promise<void> {
  if (error.status === 401) return reportUnauthorized(error);
  const problem = problemOf(error.status, error.reason);
  if (problem) showProblem(problem);
  return Promise.resolve();
}

/**
 * Act on a failed API request that the gateway or hosted mode can explain.
 * A gateway error body proves that the page runs behind the gateway, so it
 * sets the mode to external, also when the status has not answered. Returns
 * `false` in local mode, and the caller handles the error.
 */
export function handleHostedError(error: ApiError): boolean {
  if (error.reason !== null) setAuthMode('external');
  if (!isHosted()) return false;
  void reportError(error);
  return true;
}

/** Remove the `unavailable` screen, so that the app can try again. */
export function clearUnavailable(): void {
  useHostedStore.setState((state) => (state.problem === 'unavailable' ? { problem: null } : state));
}

let beforeReenter: (() => void) | null = null;
let preparingReentry = false;

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
  if (preparingReentry || reentering || stopsApp(problem)) return;
  if (window.location.pathname.startsWith(GATEWAY_PREFIX)) {
    showProblem('unavailable');
    return;
  }
  // Save before subscribers unmount the composer or disconnect its socket.
  preparingReentry = true;
  try {
    beforeReenter?.();
  } catch (e) {
    console.error('Could not keep the unsent text:', e);
  } finally {
    useHostedStore.setState({ reentering: true });
    preparingReentry = false;
  }
  window.location.assign(loginUrl());
}

/**
 * Ask the gateway if the session of this tab is still valid. The WebSocket
 * calls this before it opens a new socket, because the browser does not show
 * why an upgrade failed. An API request calls it after a 401 that the gateway
 * did not send.
 *
 * A 401 starts re-entry. A 403 or a 410 shows its screen. A changed principal
 * starts re-entry too, saving this person's queued text before leaving.
 * Only a valid session for the same person permits opening another socket.
 * An unavailable check can be retried, but cannot authorize a new connection.
 */
export async function probeSession(stillCurrent: () => boolean = () => true): Promise<boolean> {
  let res: Response;
  try {
    res = await fetch(`${GATEWAY_PREFIX}session`, {
      headers: { Accept: 'application/json' },
      credentials: 'same-origin',
    });
  } catch {
    return false;
  }
  if (!stillCurrent()) return false;
  switch (res.status) {
    case 200: {
      try {
        const body: unknown = await res.json();
        if (!stillCurrent()) return false;
        if (!body || typeof body !== 'object' || !('authenticated' in body)
          || body.authenticated !== true || !('principalId' in body)
          || typeof body.principalId !== 'string' || !body.principalId) return false;
        const { principalId, reentering, problem } = useHostedStore.getState();
        if (reentering || stopsApp(problem)) return false;
        if (principalId !== null && body.principalId !== principalId) {
          reenter();
          return false;
        }
        return true;
      } catch {
        return false;
      }
    }
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
      return false;
  }
}

/** The reason of a gateway 401: the request has no valid gateway session. */
const LOGIN_REQUIRED = 'login_required';

/** The session check after a 401 that the gateway did not send. */
let refusalCheck: Promise<void> | null = null;

/**
 * Act on a 401 in hosted mode. The gateway answers `login_required` when the
 * request has no valid session, so that 401 signs in again at once.
 *
 * Any other 401 comes from Nerve, for example for an actor header that it
 * cannot read. A login does not change that, and an immediate re-entry would
 * repeat the login forever. Ask the gateway about the session first: a 401
 * there signs in again, and a 403 or a 410 shows its screen. A valid session,
 * or no decision, shows the unavailable screen. Requests that fail at the
 * same time share one check.
 */
function reportUnauthorized(error: ApiError): Promise<void> {
  if (error.reason === LOGIN_REQUIRED) {
    reenter();
    return Promise.resolve();
  }
  if (!refusalCheck) {
    refusalCheck = probeSession().catch(() => false).then(() => {
      refusalCheck = null;
      if (!useHostedStore.getState().reentering) showProblem('unavailable');
    });
  }
  return refusalCheck;
}
