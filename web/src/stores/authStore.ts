import { create } from 'zustand';
import {
  api, setToken, clearToken, getToken, setUnauthorizedHandler,
  type LoginKind,
} from '../api/client';
import { useActorStore } from './actorStore';
import { clearAllDrafts } from './helpers/draftStorage';
import { clearAllReads } from './helpers/readStorage';

interface AuthState {
  authenticated: boolean;
  loading: boolean;
  checking: boolean;
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
   * Defaults to `'password'` — never `'none'`. A status call that fails must
   * not leave the app believing the instance is passwordless, because that is
   * the one value that makes it log itself in without asking.
   */
  loginMode: LoginKind;
  /** The instance has never been set up: no password and no username on its
   *  one account. Routed to `/setup`. */
  setupPending: boolean;
  /**
   * The signed-in account's *actor* id — the id its sessions and messages are
   * stored under.
   *
   * Attribution needs it for one thing the server cannot help with: a message
   * you have just sent exists in your own transcript before it exists anywhere
   * else. The gateway excludes the sender from the `user_message` echo (there
   * is nothing to echo back to the tab that sent it), so without this your own
   * bubble carries no sender until the page is reloaded — and in a fresh
   * two-tab exchange that leaves each tab holding one attributed message and
   * one unattributed one, which reads as a single person and suppresses every
   * label on both sides.
   *
   * Null until it is read, and null forever on a caller with no account row —
   * the agent's own principal, an MCP token — for which `/api/accounts` is
   * refused. That is the ordinary unattributed path and renders as it always
   * has, so nothing depends on this being present.
   */
  selfActorId: string | null;
  login: (password: string, username?: string) => Promise<void>;
  logout: () => void;
  checkAuth: () => Promise<void>;
  /** Re-read the descriptor after something that can change it (adding the
   *  second account, setting the first password). */
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
 * Read who we are signed in as, once per session.
 *
 * `/api/accounts` is the only endpoint that publishes it today: it is keyed by
 * account, carries `is_self`, and PR 3 added `actor_id` to the row. Key on
 * `actor_id` and never on `id` — the account id is the login, the actor id is
 * the person, and only the actor id is what a message was stored under.
 *
 * Swallows everything. A 403 is the expected answer for a caller with no
 * account row, a 401 is already handled globally, and neither is a reason to
 * fail a login: the only thing lost is the label on your own unsaved bubble.
 *
 * When a `GET /api/auth/me` arrives (PR 6), this function is the only thing
 * that changes.
 */
async function readSelfActorId(set: (partial: { selfActorId: string | null }) => void): Promise<void> {
  try {
    const { accounts } = await api.listAccounts();
    set({ selfActorId: accounts.find((a) => a.is_self)?.actor_id ?? null });
  } catch {
    set({ selfActorId: null });
  }
}

export const useAuthStore = create<AuthState>((set) => ({
  authenticated: !!getToken(),
  loading: false,
  checking: !getToken(),
  error: null,
  sessionExpired: false,
  loginMode: 'password',
  setupPending: false,
  selfActorId: null,

  login: async (password: string, username?: string) => {
    set({ loading: true, error: null });
    try {
      const { token } = await api.login(password, username);
      setToken(token);
      sessionEstablished = true;
      set({ authenticated: true, loading: false, sessionExpired: false });
      // Not awaited: nothing on screen waits for it, and a slow or refused
      // account list must not hold up the app coming back after a re-login.
      void readSelfActorId(set);
    } catch (e: any) {
      set({ error: e.message || 'Login failed', loading: false });
    }
  },

  logout: () => {
    clearToken();
    // Purge unsent drafts so nothing leaks to the next user on a shared
    // browser. Only on a *deliberate* logout — an expired session must never
    // take your unsent work with it.
    clearAllDrafts();
    clearAllReads();
    // Display names are read fresh per app session, and a deliberate logout
    // ends one. Not a secrecy measure — the next person to sign in can read
    // /api/actors too — but a map that outlives the session that fetched it is
    // a cache, and the whole point of this one is that it is not.
    useActorStore.getState().reset();
    sessionEstablished = false;  // back to a cold start: next 401 is not an "expiry"
    // Who we are is per session too, and the next person to sign in here is
    // not this one — an optimistic bubble stamped with the previous account's
    // actor would be a false attribution, which is worse than none.
    set({ authenticated: false, sessionExpired: false, selfActorId: null });
  },

  refreshStatus: async () => {
    try {
      const status = await api.authStatus();
      set({ loginMode: status.login, setupPending: status.setup_pending });
    } catch {
      // Leave the last known shape in place rather than guessing.
    }
  },

  checkAuth: async () => {
    // Asked on *both* branches, unlike before. A tab that starts with a valid
    // token still needs to know what the login form should collect, because
    // its session can expire later and the overlay has to ask for the right
    // thing — and because a passwordless install that has never been set up
    // belongs on /setup however it arrived.
    let status: Awaited<ReturnType<typeof api.authStatus>> | null = null;
    try {
      status = await api.authStatus();
      set({ loginMode: status.login, setupPending: status.setup_pending });
    } catch {
      // Status unreadable — fall through with the safe default (a password is
      // required, setup is not pending), which asks rather than assumes.
    }

    if (!getToken()) {
      // Auto-login only for the one state where there is genuinely nothing to
      // ask for: a passwordless install, which by construction has exactly one
      // account. It must not survive into a multi-account install, where an
      // empty password names nobody and the server refuses it.
      if (status?.login === 'none') {
        try {
          const { token } = await api.login('');
          setToken(token);
          sessionEstablished = true;
          // checking must be cleared here too — App renders null while it
          // is true, so leaving it set blanks the app after auto-login.
          set({ authenticated: true, checking: false });
          void readSelfActorId(set);
          return;
        } catch {
          // Fall through to the login page.
        }
      }
      set({ authenticated: false, checking: false });
      return;
    }
    try {
      await api.checkAuth();
      sessionEstablished = true;
      set({ authenticated: true, checking: false });
      // A reload arrives here rather than through `login`, and it is the more
      // common way a tab reaches an authenticated app.
      void readSelfActorId(set);
    } catch {
      // On the startup path the stored token was already dead on arrival — a
      // cold start, not an expiry under a live app, so fall through to the
      // plain login page. Keyed off sessionEstablished rather than hardcoded
      // so a later re-check of a session that *was* working still gets the
      // overlay instead of silently discarding the screen.
      clearToken();
      set({ authenticated: false, checking: false, sessionExpired: sessionEstablished });
    }
  },
}));

// Any 401 from the API layer lands here. Flag the session as expired instead
// of reloading the page: the app stays mounted, unsent drafts stay in the
// composer, and SessionExpiredOverlay collects the password over the top.
// `checking: false` guards the case where a 401 arrives during the initial
// checkAuth() — App renders nothing while `checking` is true.
setUnauthorizedHandler(() => {
  useAuthStore.setState({
    authenticated: false,
    checking: false,
    // Only a session that was actually working gets the overlay treatment. A
    // 401 on a tab that never authenticated is just "logged out" — the normal
    // login page, not an overlay over an empty app.
    sessionExpired: sessionEstablished,
  });
});
