import { create } from 'zustand';
import { api, setToken, type SetupState } from '../api/client';
import { errorDetail } from './accountStore';
import { useActorStore } from './actorStore';
import { useAuthStore } from './authStore';

/**
 * The first-run checklist.
 *
 * Two things make this store different from the others:
 *
 * **The claim signs you in.** `POST /api/setup/claim` hands back a session for
 * the account it just secured, and it is stored exactly as a login would store
 * it — so the tab that set the password is the tab that is signed in with it,
 * with no second round trip and nothing to re-type. The auth descriptor and
 * the actor map are re-read afterwards, because setting the first password
 * changes what the login form must collect and giving a display name changes
 * every label in the app.
 *
 * **The state is the server's.** Every mutation returns the whole checklist,
 * so there is one source of truth for "what is done" and no client-side guess
 * to go stale — which matters here because most steps are *derived* from
 * configuration rather than remembered, and because a step can be finished
 * from another tab or from the command line.
 */
export interface SetupStoreState {
  state: SetupState | null;
  loading: boolean;
  /** The step id a write is in flight for, so only that card is busy. */
  busy: string | null;
  error: string | null;
  /** The restart has been asked for and we are waiting for the new process. */
  reconnecting: boolean;
  load: () => Promise<void>;
  claim: (body: {
    username: string; password: string;
    display_name?: string; setup_token?: string;
  }) => Promise<boolean>;
  save: (step: string, run: () => Promise<SetupState>) => Promise<boolean>;
  skip: (step: string, skipped: boolean) => Promise<boolean>;
  restart: () => Promise<boolean>;
  clearError: () => void;
  reset: () => void;
}

/**
 * How long to keep asking whether the new process is up, and how often.
 *
 * A restart is a stop, a wait for the old process to go, and a start: a few
 * seconds on a laptop, longer on a small box that has to re-open the database
 * and re-initialise the agent. Giving up after two minutes turns a slow
 * restart into a wrong error message, so the poll is patient and says what it
 * is doing.
 */
export const RECONNECT_INTERVAL_MS = 1000;
export const RECONNECT_TIMEOUT_MS = 120_000;

export const useSetupStore = create<SetupStoreState>((set, get) => ({
  state: null,
  loading: true,
  busy: null,
  error: null,
  reconnecting: false,

  clearError: () => set({ error: null }),

  reset: () => set({
    state: null, loading: true, busy: null, error: null, reconnecting: false,
  }),

  load: async () => {
    try {
      set({ state: await api.setupState(), loading: false, error: null });
    } catch (e) {
      set({ loading: false, error: errorDetail(e, 'Could not read the setup state') });
    }
  },

  claim: async (body) => {
    set({ busy: 'account', error: null });
    try {
      const claimed = await api.setupClaim(body);
      // Exactly what a login does with the token it is given.
      setToken(claimed.token);
      // Then the store's own startup path, rather than a hand-written "you are
      // signed in now": it validates the token, re-reads the descriptor and
      // sets the session state that decides whether this tab sees the app or
      // a login form. The tab that claimed the instance with no session — a
      // dead token in storage — has to end up *inside*, not back at a prompt
      // for the password it just set.
      await useAuthStore.getState().checkAuth();
      // Names are read per app session and a display name may have just been
      // set; nothing stores the name it changed.
      await useActorStore.getState().refresh();
      await get().load();
      return true;
    } catch (e) {
      set({ error: errorDetail(e, 'Could not claim this instance') });
      return false;
    } finally {
      set({ busy: null });
    }
  },

  save: async (step, run) => {
    set({ busy: step, error: null });
    try {
      set({ state: await run() });
      return true;
    } catch (e) {
      set({ error: errorDetail(e, 'Could not save that') });
      return false;
    } finally {
      set({ busy: null });
    }
  },

  skip: async (step, skipped) => {
    set({ busy: step, error: null });
    try {
      set({ state: await api.setupSkip(step, skipped) });
      return true;
    } catch (e) {
      set({ error: errorDetail(e, 'Could not update the checklist') });
      return false;
    } finally {
      set({ busy: null });
    }
  },

  restart: async () => {
    set({ busy: 'restart', error: null });
    try {
      await api.restartSystem();
    } catch (e) {
      set({ busy: null, error: errorDetail(e, 'Could not restart the instance') });
      return false;
    }
    set({ busy: null, reconnecting: true });

    // Wait for the new process rather than for a timer: the daemon we were
    // talking to is going away, so every request until the new one is
    // listening fails, and only a successful answer means it is back. The
    // session token is untouched throughout — the signing secret is pinned
    // and persisted, and nothing in the wizard rotates it, so the tab comes
    // back already signed in.
    const deadline = Date.now() + RECONNECT_TIMEOUT_MS;
    while (Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, RECONNECT_INTERVAL_MS));
      try {
        await api.authStatus();
        await useAuthStore.getState().refreshStatus();
        await get().load();
        set({ reconnecting: false });
        return true;
      } catch {
        // Still down — or half up. Ask again.
      }
    }
    set({
      reconnecting: false,
      error: 'The instance did not answer after the restart. Check the server '
        + 'log (`nerve logs`), then reload this page.',
    });
    return false;
  },
}));
