import { create } from 'zustand';
import {
  api, setToken, clearToken, getToken, setUnauthorizedHandler,
  type Account, type LoginKind,
} from '../api/client';
import { useActorStore } from './actorStore';
import { clearAllDrafts } from './helpers/draftStorage';
import { clearAllReads } from './helpers/readStorage';

/** Who a session belongs to. Enough to tell one person's session from another's. */
export interface SignedInAccount {
  id: string;
  username: string | null;
  /** Stable author identity; account id and display names may change. */
  actor_id: string;
}

/**
 * Everything held on this browser that belongs to *a person* rather than to the
 * app. Purged on a deliberate sign-out, and on the one other occasion a
 * different person could end up in front of a mounted app (see `login`).
 */
function purgeAccountScopedState(): void {
  clearAllDrafts();
  clearAllReads();
  // Do not let a name snapshot outlive the session that read it.
  useActorStore.getState().reset();
}

/**
 * What the login form collects when nobody has told us yet.
 *
 * Both fields. A username the server does not need is accepted and ignored
 * while one account exists — and a blank one is accepted too — so asking for it
 * costs a field, never a login. Guessing the other way (a password-only form on
 * an instance that now has two accounts) costs every submission.
 */
const FAIL_CLOSED_LOGIN: LoginKind = 'username_password';

interface AuthState {
  authenticated: boolean;
  loading: boolean;
  /**
   * Startup has finished deciding where this tab belongs.
   *
   * Nothing renders before it, including for a tab that arrives holding a
   * token: a token says the tab may come in, not where the root route should
   * land. That comes from the status descriptor.
   */
  ready: boolean;
  error: string | null;
  /**
   * The session died under a mounted app (token expired, or the gateway
   * restarted with a new secret) rather than the app starting logged out.
   *
   * The distinction matters: a cold start shows the full-page login, but an
   * expiry keeps the app — and everything you had typed — mounted and asks
   * for the password in an overlay. Re-authenticating drops you back exactly
   * where you were.
   */
  sessionExpired: boolean;
  /**
   * What the login form must collect, from `/api/auth/status`.
   *
   * `null` means it has never been answered, and the form shows its loading
   * state rather than guessing. A *failed* read falls back to
   * {@link FAIL_CLOSED_LOGIN} rather than staying null — so the form is never
   * stranded — and never to `'none'`, which is the one value that makes the app
   * log itself in without asking.
   */
  loginMode: LoginKind | null;
  /**
   * The account this session belongs to, once it is known.
   *
   * Read at startup and after every sign-in, from `/api/accounts/me`. It is
   * what binds re-authentication to the person whose app is on screen: the
   * session-expired overlay sits on top of a *mounted* application holding that
   * person's drafts and loaded state, so it may only be unlocked by them.
   * `null` means unknown — the overlay then offers nothing but a sign-out.
   */
  account: SignedInAccount | null;
  login: (password: string, username?: string) => Promise<void>;
  logout: () => void;
  checkAuth: () => Promise<void>;
  /**
   * Re-read the descriptor. Called on entry to every login surface and after
   * anything that can change it, because it goes stale without this tab doing
   * anything at all: another tab — or a colleague — creating the second account
   * is what turns a password-only form into one that can never succeed again.
   */
  refreshStatus: () => Promise<void>;
}

/**
 * Whether this tab has ever held a working session.
 *
 * Distinguishes "expired while you were using it" (keep the app mounted, ask
 * in an overlay) from "opened with a dead token in storage" (nothing to
 * preserve — show the normal login page). Module-level rather than store
 * state because it's a fact about the page load, not rendered UI.
 */
let sessionEstablished = false;

/**
 * Which status request is the current one.
 *
 * Refreshes are fired from several places at once — a 401, a refused sign-in, a
 * login surface mounting — and the responses can come back in any order. The
 * last *response* used to win, which could restore `password` after a newer
 * answer had said `username_password` and leave the form unable to succeed.
 * Only the latest *request* may apply its result now.
 */
let statusGeneration = 0;

/** Drops login/startup decisions superseded by logout or another auth session. */
let authGeneration = 0;

function beginAuthSession(): number {
  return ++authGeneration;
}

function isCurrentAuthSession(generation: number): boolean {
  return generation === authGeneration;
}

/** Bind delayed work to the login/logout generation that started it. */
export function bindAuthSession(): { stillCurrent: () => boolean } {
  const generation = authGeneration;
  return { stillCurrent: () => isCurrentAuthSession(generation) };
}

/** Bind delayed optimistic work to the actor and auth session that requested it. */
export function bindSender(): { actorId: string | null; stillCurrent: () => boolean } {
  const authSession = bindAuthSession();
  const actorId = useAuthStore.getState().account?.actor_id ?? null;
  return { actorId, stillCurrent: authSession.stillCurrent };
}

function identityOf(account: Account): SignedInAccount {
  return { id: account.id, username: account.username, actor_id: account.actor_id };
}

/** The signed-in account, or `null` if it cannot be read right now. */
async function loadIdentity(): Promise<SignedInAccount | null> {
  try {
    return identityOf(await api.getOwnAccount());
  } catch {
    return null;
  }
}

export const useAuthStore = create<AuthState>((set, get) => ({
  authenticated: !!getToken(),
  loading: false,
  ready: false,
  error: null,
  sessionExpired: false,
  loginMode: null,
  account: null,

  login: async (password: string, username?: string) => {
    const generation = beginAuthSession();
    const previous = get().account;
    const wasExpired = get().sessionExpired;
    set({ loading: true, error: null });
    let token: string;
    try {
      ({ token } = await api.login(password, username));
    } catch (e: any) {
      if (!isCurrentAuthSession(generation)) return;
      set({ error: e.message || 'Login failed', loading: false });
      // A refused sign-in is a good moment to find out the form was asking for
      // the wrong thing, which is what a stale descriptor looks like from here.
      void get().refreshStatus();
      return;
    }
    // Never install a credential for a session logout already ended.
    if (!isCurrentAuthSession(generation)) return;

    const tokenRevision = setToken(token);
    const identity = await loadIdentity();
    // Take back only this attempt's token revision. JWTs minted for the same
    // account in one second can be byte-identical, so string equality cannot
    // distinguish this stale attempt from a newer login.
    if (!isCurrentAuthSession(generation)) {
      clearToken(tokenRevision);
      return;
    }

    if (wasExpired) {
      // Unlocking a *mounted* application — one still holding the previous
      // person's drafts, read state and loaded sessions. That needs a
      // positively confirmed match, not the absence of a mismatch: a username
      // is mutable and reusable, so the one this overlay submitted can have
      // come to name a different account since it was read.
      if (!identity) {
        // Cannot confirm. Keep the door shut rather than open it on a maybe —
        // the token is discarded, so nothing was gained by the attempt.
        clearToken();
        set({
          loading: false,
          error: 'Could not confirm this account just now. Try again, or log out.',
        });
        return;
      }
      if (!previous || identity.id !== previous.id) {
        // A different person, in front of somebody else's mounted application.
        // Nothing of the previous account may survive, so this is a sign-out
        // rather than a sign-in.
        clearToken();
        purgeAccountScopedState();
        sessionEstablished = false;
        set({
          authenticated: false, loading: false, sessionExpired: false,
          account: null,
          error: 'That is a different account. Sign in again to use it.',
        });
        return;
      }
    }

    sessionEstablished = true;
    set({
      authenticated: true, loading: false, sessionExpired: false,
      account: identity,
    });
  },

  logout: () => {
    beginAuthSession();
    clearToken();
    // Purge unsent drafts so nothing leaks to the next user on a shared
    // browser. Only on a *deliberate* logout — an expired session must never
    // take your unsent work with it.
    purgeAccountScopedState();
    sessionEstablished = false;  // back to a cold start: next 401 is not an "expiry"
    set({
      authenticated: false, loading: false, sessionExpired: false,
      error: null, account: null,
    });
    void get().refreshStatus();
  },

  refreshStatus: async () => {
    const generation = ++statusGeneration;
    try {
      applyStatus(generation, await api.authStatus());
    } catch {
      applyStatus(generation, null);
    }
  },

  checkAuth: async () => {
    const authSession = beginAuthSession();
    const token = getToken();
    const generation = ++statusGeneration;
    // Both at once. Nothing renders until both have answered — that is what
    // `ready` means — so asking in sequence would double the blank screen.
    // `/api/accounts/me` *is* the session check: it needs a valid token and it
    // says which account the token belongs to, which is one request rather than
    // two for strictly more than `/api/auth/check` answered.
    const [statusOutcome, identityOutcome] = await Promise.allSettled([
      api.authStatus(),
      token ? api.getOwnAccount() : Promise.resolve(null),
    ]);

    const status = statusOutcome.status === 'fulfilled' ? statusOutcome.value : null;
    applyStatus(generation, status);

    if (!isCurrentAuthSession(authSession)) return;

    if (token && identityOutcome.status === 'fulfilled' && identityOutcome.value) {
      sessionEstablished = true;
      set({
        authenticated: true, ready: true,
        account: identityOf(identityOutcome.value),
      });
      return;
    }

    // No token, or one the server would not take. Both mean this tab holds
    // nothing usable, so both take the same path from here — which is the
    // point: a passwordless install with an expired or rotated token belongs
    // exactly where one with no token at all belongs, and used to land on a
    // login form it did not need and could not use.
    const hadToken = !!token;
    if (hadToken) clearToken();
    set({ account: null });

    // Auto-login only when the server reports a passwordless install. That
    // state requires exactly one account; a multi-account install requires an
    // empty password names nobody and the server refuses it.
    if (status?.login === 'none') {
      try {
        const { token: fresh } = await api.login('');
        if (!isCurrentAuthSession(authSession)) return;
        const tokenRevision = setToken(fresh);
        const identity = await loadIdentity();
        if (!isCurrentAuthSession(authSession)) {
          clearToken(tokenRevision);
          return;
        }
        sessionEstablished = true;
        set({
          authenticated: true, ready: true, sessionExpired: false,
          account: identity,
        });
        return;
      } catch {
        // Fall through to the login page.
      }
    }
    // A stored token that was already dead on arrival is a cold start, not an
    // expiry under a live app, so this falls through to the plain login page.
    // Keyed off sessionEstablished rather than hardcoded so a later re-check of
    // a session that *was* working still gets the overlay instead of silently
    // discarding the screen.
    set({
      authenticated: false, ready: true,
      sessionExpired: hadToken && sessionEstablished,
    });
  },
}));

/**
 * Apply one status response, if it is still the newest one asked for.
 *
 * `null` means the read failed: keep the last known answer over a transient
 * failure, and fail closed only when there has never been one.
 */
function applyStatus(generation: number, status: Awaited<ReturnType<typeof api.authStatus>> | null): void {
  if (generation !== statusGeneration) return;   // a newer request is in flight
  if (status) {
    useAuthStore.setState({
      loginMode: status.login,
    });
    return;
  }
  useAuthStore.setState((state) => ({
    loginMode: state.loginMode ?? FAIL_CLOSED_LOGIN,
  }));
}

// Any 401 from the API layer lands here. Flag the session as expired instead
// of reloading the page: the app stays mounted, unsent drafts stay in the
// composer, and SessionExpiredOverlay collects the password over the top.
// `ready: true` covers a 401 arriving during the initial checkAuth(): nothing
// renders until startup has decided, and a 401 has decided.
setUnauthorizedHandler(() => {
  useAuthStore.setState({
    authenticated: false,
    ready: true,
    // Only a session that was actually working gets the overlay treatment. A
    // 401 on a tab that never authenticated is just "logged out" — the normal
    // login page, not an overlay over an empty app.
    sessionExpired: sessionEstablished,
  });
  // Whatever ended the session may also have changed what signing back in
  // takes — a second account, most of all. Ask before drawing the form.
  void useAuthStore.getState().refreshStatus();
});

/** Stable actor for optimistic rows; null for callers without an account row. */
export function selfActorId(): string | null {
  return useAuthStore.getState().account?.actor_id ?? null;
}
