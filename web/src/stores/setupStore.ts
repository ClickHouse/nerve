import { create } from 'zustand';
import { api, setToken, type SetupState } from '../api/client';
import { errorDetail } from './accountStore';
import { useActorStore } from './actorStore';
import { bindSender, useAuthStore } from './authStore';

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

/**
 * Whether the checklist still has something to answer.
 *
 * `finished` is the server's judgement: the required step done, every other
 * one answered one way or the other, and nothing still waiting on a restart.
 * Unknown (nothing loaded, or the read failed) is *not* unfinished — the app
 * must not grow a permanent nag because one request failed once.
 */
export function setupIsUnfinished(state: SetupStoreState): boolean {
  return state.state !== null && !state.state.finished;
}

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
    // Stamped with the session that asked. A read started before a sign-out
    // and answered after it describes somebody else's instance state — and
    // would repopulate a store that was deliberately emptied.
    const asked = bindSender().stillCurrent;
    try {
      const state = await api.setupState();
      if (!asked()) return;
      set({ state, loading: false, error: null });
    } catch (e) {
      if (!asked()) return;
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
      if (step === 'profile') {
        // A display name is a label everywhere else in the app and is stored
        // nowhere else — nothing carries the name it changed, so without this
        // every message and session keeps the old one until a reload. It
        // never rejects, and a failed re-read must not turn a rename that
        // committed into an error.
        await useActorStore.getState().refresh();
      }
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
    let before: string;
    try {
      // Not a background task on the server: whether a restart was *begun* is
      // knowable there, so a failure to start one arrives here as an error
      // rather than as a page waiting for a process that is never coming.
      before = (await api.restartSystem()).boot;
    } catch (e) {
      set({ busy: null, error: errorDetail(e, 'Could not restart the instance') });
      return false;
    }
    set({ busy: null, reconnecting: true });

    // Wait for a *different* process, not for any answer at all. The daemon
    // that accepted the request keeps serving while it shuts down, so "is
    // anybody there" is satisfied by the very process being replaced — and
    // the restart-only settings the wizard just wrote would still not be in
    // force. `boot` is a fresh random value on every start, so a changed one
    // is proof and nothing else is.
    //
    // The session token is untouched throughout — the signing secret is
    // pinned and persisted, the session epoch lives on the account rather
    // than in the process, and nothing in the wizard rotates either — so the
    // tab comes back already signed in.
    const deadline = Date.now() + RECONNECT_TIMEOUT_MS;
    while (Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, RECONNECT_INTERVAL_MS));
      let boot: string | undefined;
      try {
        boot = (await api.health()).boot;
      } catch {
        continue;  // still down, or half up
      }
      // An instance that publishes no generation at all is one this client is
      // newer than; falling back to "it answered" is the old behaviour, and
      // better than waiting forever for a field that will never arrive.
      if (boot === before) continue;
      try {
        await useAuthStore.getState().refreshStatus();
        await get().load();
      } catch {
        // The new process is up; a failed first read is not a failed restart.
      }
      set({ reconnecting: false });
      return true;
    }
    set({
      reconnecting: false,
      error: 'The instance did not come back after the restart. Check the '
        + 'server log (`nerve logs`), then reload this page.',
    });
    return false;
  },
}));
