import { create } from 'zustand';
import { api, setToken, clearToken, getToken, setUnauthorizedHandler } from '../api/client';
import { clearAllDrafts } from './helpers/draftStorage';

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
  login: (password: string) => Promise<void>;
  logout: () => void;
  checkAuth: () => Promise<void>;
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

export const useAuthStore = create<AuthState>((set) => ({
  authenticated: !!getToken(),
  loading: false,
  checking: !getToken(),
  error: null,
  sessionExpired: false,

  login: async (password: string) => {
    set({ loading: true, error: null });
    try {
      const { token } = await api.login(password);
      setToken(token);
      sessionEstablished = true;
      set({ authenticated: true, loading: false, sessionExpired: false });
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
    sessionEstablished = false;  // back to a cold start: next 401 is not an "expiry"
    set({ authenticated: false, sessionExpired: false });
  },

  checkAuth: async () => {
    if (!getToken()) {
      // No token — check if auth is even required
      try {
        const { auth_required } = await api.authStatus();
        if (!auth_required) {
          // No password configured — auto-login
          const { token } = await api.login('');
          setToken(token);
          // checking must be cleared here too — App renders null while it
          // is true, so leaving it set blanks the app after auto-login.
          set({ authenticated: true, checking: false });
          return;
        }
      } catch {
        // Status check failed — fall through to login page
      }
      set({ authenticated: false, checking: false });
      return;
    }
    try {
      await api.checkAuth();
      sessionEstablished = true;
      set({ authenticated: true, checking: false });
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
