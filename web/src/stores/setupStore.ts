import { create } from 'zustand';
import { api, type SetupState } from '../api/client';
import { errorDetail } from './accountStore';
import { useActorStore } from './actorStore';
import { bindSender } from './authStore';

/**
 * The post-claim setup checklist.
 *
 * **The state is the server's.** Every mutation returns the whole checklist,
 * so there is one source of truth for "what is done" and no client-side guess
 * to go stale — which matters here because most steps are *derived* from
 * configuration rather than remembered, and because a step can be finished
 * from another tab or from the command line.
 *
 * **Nothing here restarts the instance.** The settings that are read at
 * startup are reported as pending along with the command that applies them;
 * running it is an operator action on the server, and the page picks the
 * result up on its next read.
 *
 * The claim itself is deliberately not in this store. It is the one
 * unauthenticated write in the product, it is guarded by the setup token
 * rather than by a session, and it belongs with the form that collects it.
 */
export interface SetupStoreState {
  state: SetupState | null;
  loading: boolean;
  /** The step id a write is in flight for, so only that card is busy. */
  busy: string | null;
  error: string | null;
  load: () => Promise<void>;
  save: (step: string, run: () => Promise<SetupState>) => Promise<boolean>;
  skip: (step: string, skipped: boolean) => Promise<boolean>;
  clearError: () => void;
  reset: () => void;
}

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

export const useSetupStore = create<SetupStoreState>((set) => ({
  state: null,
  loading: true,
  busy: null,
  error: null,

  clearError: () => set({ error: null }),

  reset: () => set({ state: null, loading: true, busy: null, error: null }),

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

  save: async (step, run) => {
    // Stamped like `load()`. Alice starts a save, signs out, Bob signs in —
    // and her answer, arriving afterwards, would install her instance state
    // over his and clear the busy and error state of whatever *he* is doing.
    // Every path after the await asks whether it is still hers to commit.
    const asked = bindSender().stillCurrent;
    set({ busy: step, error: null });
    try {
      const state = await run();
      if (!asked()) return false;
      set({ state });
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
      if (!asked()) return false;
      set({ error: errorDetail(e, 'Could not save that') });
      return false;
    } finally {
      if (asked()) set({ busy: null });
    }
  },

  skip: async (step, skipped) => {
    const asked = bindSender().stillCurrent;
    set({ busy: step, error: null });
    try {
      const state = await api.setupSkip(step, skipped);
      if (!asked()) return false;
      set({ state });
      return true;
    } catch (e) {
      if (!asked()) return false;
      set({ error: errorDetail(e, 'Could not update the checklist') });
      return false;
    } finally {
      if (asked()) set({ busy: null });
    }
  },
}));
