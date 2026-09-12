import { create } from 'zustand';
import {
  api, setToken, clearToken, getToken, setUnauthorizedHandler,
  type LoginKind,
} from '../api/client';
import { clearAllDrafts } from './helpers/draftStorage';
import { clearAllReads } from './helpers/readStorage';

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
   * token: a token says the tab may come in, not *where* it should land. The
   * instance may still be unset-up, and that is decided from the status
   * descriptor, which arrives a moment after a token can be read out of
   * storage. The app used to render on the token alone and navigate to /chat
   * before the answer came back — by which time the component that would have
   * redirected was already unmounted.
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
  /** A descriptor read is in flight. */
  statusLoading: boolean;
  /**
   * The sole account has no password, so everyone who can reach this instance
   * is signed in as it. Routed to `/setup`. Giving the account a username does
   * not change it; only a password does.
   */
  setupPending: boolean;
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

export const useAuthStore = create<AuthState>((set, get) => ({
  authenticated: !!getToken(),
  loading: false,
  ready: false,
  error: null,
  sessionExpired: false,
  loginMode: null,
  statusLoading: false,
  setupPending: false,

  login: async (password: string, username?: string) => {
    set({ loading: true, error: null });
    try {
      const { token } = await api.login(password, username);
      setToken(token);
      sessionEstablished = true;
      set({ authenticated: true, loading: false, sessionExpired: false });
    } catch (e: any) {
      set({ error: e.message || 'Login failed', loading: false });
      // A refused sign-in is a good moment to find out the form was asking for
      // the wrong thing, which is what a stale descriptor looks like from here.
      void get().refreshStatus();
    }
  },

  logout: () => {
    clearToken();
    // Purge unsent drafts so nothing leaks to the next user on a shared
    // browser. Only on a *deliberate* logout — an expired session must never
    // take your unsent work with it.
    clearAllDrafts();
    clearAllReads();
    sessionEstablished = false;  // back to a cold start: next 401 is not an "expiry"
    set({ authenticated: false, sessionExpired: false, error: null });
    void get().refreshStatus();
  },

  refreshStatus: async () => {
    set({ statusLoading: true });
    try {
      const status = await api.authStatus();
      set({
        loginMode: status.login,
        setupPending: status.setup_pending,
        statusLoading: false,
      });
    } catch {
      // Keep the last known answer over a transient failure, and fail closed
      // only when there has never been one.
      set((state) => ({
        loginMode: state.loginMode ?? FAIL_CLOSED_LOGIN,
        statusLoading: false,
      }));
    }
  },

  checkAuth: async () => {
    const token = getToken();
    set({ statusLoading: true });
    // Both at once. Nothing renders until both have answered — that is what
    // `ready` means — so asking in sequence would double the blank screen.
    const [statusOutcome, sessionOutcome] = await Promise.allSettled([
      api.authStatus(),
      token ? api.checkAuth() : Promise.resolve(null),
    ]);

    const status = statusOutcome.status === 'fulfilled' ? statusOutcome.value : null;
    if (status) {
      set({
        loginMode: status.login,
        setupPending: status.setup_pending,
        statusLoading: false,
      });
    } else {
      set((state) => ({
        loginMode: state.loginMode ?? FAIL_CLOSED_LOGIN,
        statusLoading: false,
      }));
    }

    if (!token) {
      // Auto-login only for the one state where there is genuinely nothing to
      // ask for: a passwordless install, which by construction has exactly one
      // account. It must not survive into a multi-account install, where an
      // empty password names nobody and the server refuses it.
      if (status?.login === 'none') {
        try {
          const { token: fresh } = await api.login('');
          setToken(fresh);
          sessionEstablished = true;
          set({ authenticated: true, ready: true });
          return;
        } catch {
          // Fall through to the login page.
        }
      }
      set({ authenticated: false, ready: true });
      return;
    }

    if (sessionOutcome.status === 'fulfilled') {
      sessionEstablished = true;
      set({ authenticated: true, ready: true });
      return;
    }
    // On the startup path the stored token was already dead on arrival — a
    // cold start, not an expiry under a live app, so fall through to the
    // plain login page. Keyed off sessionEstablished rather than hardcoded
    // so a later re-check of a session that *was* working still gets the
    // overlay instead of silently discarding the screen.
    clearToken();
    set({ authenticated: false, ready: true, sessionExpired: sessionEstablished });
  },
}));

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
