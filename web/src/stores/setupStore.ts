import { create } from 'zustand';
import {
  api,
  type AgentsDetect, type HostLogin, type RestartResponse, type SetupEnvironment,
  type SetupFinishResponse, type SetupModeDetail, type SetupModelsResponse, type SetupOnboarding,
  type SetupOnboardingAwaiting, type SetupOnboardingKind, type SetupState, type SetupStep, type SetupStepId,
  type SourcesPreflight,
} from '../api/client';
import { ApiError, clearUnavailable } from '../api/hosted';
import { errorDetail } from './accountStore';
import { useActorStore } from './actorStore';
import { bindSender, useAuthStore } from './authStore';
import { viewerStorageKey } from './helpers/viewerStorage';

/**
 * The setup wizard's state.
 *
 * **The state is the server's.** Every mutation returns the whole checklist,
 * so there is one source of truth for "what is done" and nothing client-side
 * to go stale — a step can be finished from another tab, by another person
 * on an instance behind a front gateway (external auth mode), or from the
 * command line, and the next read shows it.
 *
 * **A read never overwrites a newer write.** A `load()` started before a
 * mutation (a `visibilitychange`, a step change) is served first under the
 * server's state lock but can resolve in the browser *after* the mutation's
 * answer; its pre-save state is dropped (`mutationSeq`), so the rail and
 * badges never flip back to a status the server has already left.
 *
 * **The restart wait is owned here, not by the overlay.** Dismissing the
 * overlay hides it; the poll keeps running to its time-box, and when the new
 * process answers the store reloads itself so the banner and the Review step
 * update on their own.
 */

/** A `400 {detail, field}` refusal, attached to the field it names. */
export interface SetupFieldError {
  field: string;
  detail: string;
}

/** What a form held when a 409 refused its save, and the etag of the step it was opened on. */
export interface UnsavedForm {
  step: SetupStepId;
  etag: string | null;
  values: unknown;
}

export interface SetupStoreState {
  state: SetupState | null;
  loading: boolean;
  /** The step id a write is in flight for. One write at a time. */
  busy: string | null;
  error: string | null;
  fieldError: SetupFieldError | null;
  /** A restart has been asked for and we are waiting for the new process. */
  reconnecting: boolean;
  reconnectElapsedMs: number;
  reconnectFailed: boolean;
  restartOutcome: RestartResponse | null;
  bannerDismissed: boolean;
  /** "Not now"/Close pressed on the overlay; the wait itself keeps running. */
  overlayDismissed: boolean;
  /** A step just saved that wants to say so before the person moves on. */
  savedNotice: SetupStepId | null;
  /**
   * What a form held when a 409 refused its save. The store re-reads the
   * state then, and a read that finds the step changed remounts the form,
   * which would start again from the server's values; the form mounted in
   * its place starts from these instead (`unsavedFor`), so the refusal does
   * not also discard the person's changes. Dropped once taken, by a read that
   * finds the step unchanged (nothing is remounted), by the next write and by
   * `reset()`.
   */
  unsaved: UnsavedForm | null;
  preflight: SourcesPreflight | null;
  agentsDetected: AgentsDetect | null;
  environment: SetupEnvironment | null;
  models: SetupModelsResponse | null;
  hostLogin: HostLogin | null;
  load: () => Promise<void>;
  /** One write. `unsaved` is what the form holds, kept for it if a 409 refuses the write. */
  save: (step: SetupStepId, run: () => Promise<SetupState>, unsaved?: unknown) => Promise<boolean>;
  skip: (step: SetupStepId, skipped: boolean) => Promise<boolean>;
  finish: () => Promise<SetupFinishResponse | null>;
  /** Open the onboarding conversation (Finish, which is idempotent). Its session id, or null. */
  startOnboarding: () => Promise<string | null>;
  /** End the onboarding conversation without its setup. */
  skipOnboarding: () => Promise<boolean>;
  restart: () => Promise<boolean>;
  waitForGeneration: (before: string, timeoutMs: number) => Promise<boolean>;
  /** Another bounded wait on the generation the last restart reported. */
  retryWait: () => Promise<boolean>;
  loadPreflight: () => Promise<void>;
  loadAgents: () => Promise<void>;
  loadEnvironment: () => Promise<void>;
  loadModels: () => Promise<void>;
  loadHostLogin: (refresh?: boolean) => Promise<void>;
  dismissBanner: () => void;
  dismissRestartOverlay: () => void;
  noteSaved: (step: SetupStepId) => void;
  clearSavedNotice: () => void;
  dropUnsaved: () => void;
  clearError: () => void;
  reset: () => void;
}

/**
 * How often to ask whether the new process is up, and for how long.
 *
 * A restart is a stop, a wait for the old process to go, and a start: a few
 * seconds on a laptop, longer on a small box that re-opens the database and
 * re-initialises the agent. The server's `timeout_hint_seconds` lengthens
 * this for a container (180 s) or an operator-driven restart (300 s).
 */
export const RECONNECT_INTERVAL_MS = 1000;
export const RECONNECT_TIMEOUT_MS = 120_000;
const UNSUPPORTED_TIMEOUT_MS = 300_000;

const BANNER_KEY = 'nerve_setup_banner_dismissed';

function readBannerDismissed(): boolean {
  try {
    return sessionStorage.getItem(viewerStorageKey(BANNER_KEY)) === '1';
  } catch {
    return false;
  }
}

function writeBannerDismissed(): void {
  try {
    sessionStorage.setItem(viewerStorageKey(BANNER_KEY), '1');
  } catch {
    // Storage unavailable: the dismissal lasts for this page only.
  }
}

/** The `{detail, field}` of a 400 refusal, or `null` for any other error. */
export function fieldErrorOf(e: unknown): SetupFieldError | null {
  const raw = e instanceof Error ? e.message : String(e ?? '');
  const body = raw.slice(raw.indexOf(':') + 1).trim();
  try {
    const parsed: unknown = JSON.parse(body);
    if (parsed && typeof parsed === 'object' && 'field' in parsed && 'detail' in parsed
      && typeof parsed.field === 'string' && typeof parsed.detail === 'string') {
      return { field: parsed.field, detail: parsed.detail };
    }
  } catch {
    // Not JSON.
  }
  return null;
}

function statusOf(e: unknown): number | null {
  return e instanceof ApiError ? e.status : null;
}

/** Whether the checklist still has something to answer. Unknown is not unfinished. */
export function setupIsUnfinished(s: SetupStoreState): boolean {
  return s.state !== null && !s.state.finished;
}

/**
 * Whether the instance has never been set up: the checklist is unfinished
 * and the Mode step, where the wizard starts, is still pending. Unknown is
 * not "never set up", nor is a Mode step a lockdown has blocked — nothing on
 * the page can answer that one. A partly set-up instance (Mode chosen,
 * steps left) is the banner's case, not this one.
 */
export function setupNotStarted(s: SetupStoreState): boolean {
  return setupIsUnfinished(s) && stepById(s, 'mode')?.status === 'pending';
}

/**
 * The onboarding, from the Mode step's detail, and what its agent waits for
 * (`awaiting`, from the state's `onboarding` block; `null` while it is not
 * running, and from an older backend).
 */
export function onboardingOf(state: SetupState | null): {
  status: SetupOnboarding; sessionId: string | null; kind: SetupOnboardingKind;
  awaiting: SetupOnboardingAwaiting | null;
} {
  const detail = state?.steps.find((x) => x.id === 'mode')?.detail as SetupModeDetail | undefined;
  const status = detail?.onboarding ?? 'not_applicable';
  return {
    status,
    sessionId: detail?.onboarding_session_id ?? null,
    kind: detail?.onboarding_kind ?? 'conversation',
    awaiting: status === 'running' ? state?.onboarding?.awaiting ?? null : null,
  };
}

/** Setup is complete and the onboarding conversation can start now. */
export function onboardingCanStart(state: SetupState | null): boolean {
  const { status, kind } = onboardingOf(state);
  return !!state?.finished && status === 'pending' && kind === 'conversation';
}

export function stepsLeft(s: SetupStoreState): number {
  return s.state?.steps_left ?? 0;
}

/** Whether a model provider is configured; `null` while unknown. */
export function providerConfigured(s: SetupStoreState): boolean | null {
  const step = s.state?.steps.find((x) => x.id === 'provider');
  if (!step) return null;
  return step.status === 'done';
}

export function stepById(s: SetupStoreState, id: string): SetupStep | undefined {
  return s.state?.steps.find((x) => x.id === id);
}

/**
 * What a refused save left for the form now mounted for `step` (`unsaved`),
 * or `null`. Only a form the read after the refusal remounted takes it: its
 * step's etag is not the one the refused form was opened on.
 */
export function unsavedFor(step: SetupStep): unknown {
  const kept = useSetupStore.getState().unsaved;
  return kept && kept.step === step.id && kept.etag !== step.etag ? kept.values : null;
}

/** Which wait is the current one; an older wait that wakes up stops. */
let waitToken = 0;

/**
 * Bumped when a mutation starts. `load()` snapshots it and drops its answer
 * if a save, skip, finish or restart began while the read was in flight.
 */
let mutationSeq = 0;

/**
 * Bumped by `reset()`. The answer to a request that started before a reset
 * belongs to the state that `reset()` cleared, so it is dropped.
 */
let resetCount = 0;

/**
 * Binds a request that starts now: its answer may change the store only if
 * the same session is signed in and the store was not reset meanwhile.
 */
function bindRequest(): () => boolean {
  const sameSession = bindSender().stillCurrent;
  const started = resetCount;
  return () => sameSession() && started === resetCount;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export const useSetupStore = create<SetupStoreState>((set, get) => ({
  state: null,
  loading: true,
  busy: null,
  error: null,
  fieldError: null,
  reconnecting: false,
  reconnectElapsedMs: 0,
  reconnectFailed: false,
  restartOutcome: null,
  bannerDismissed: false,
  overlayDismissed: false,
  savedNotice: null,
  unsaved: null,
  preflight: null,
  agentsDetected: null,
  environment: null,
  models: null,
  hostLogin: null,

  clearError: () => set({ error: null, fieldError: null }),
  noteSaved: (step) => set({ savedNotice: step }),
  clearSavedNotice: () => set((s) => (s.savedNotice === null ? s : { savedNotice: null })),
  dropUnsaved: () => set((s) => (s.unsaved === null ? s : { unsaved: null })),

  reset: () => {
    waitToken += 1;
    resetCount += 1;
    set({
      state: null, loading: true, busy: null, error: null, fieldError: null,
      reconnecting: false, reconnectElapsedMs: 0, reconnectFailed: false,
      restartOutcome: null, bannerDismissed: false, overlayDismissed: false,
      savedNotice: null, unsaved: null,
      preflight: null, agentsDetected: null, environment: null, models: null,
      hostLogin: null,
    });
  },

  load: async () => {
    // Stamped with the session that asked. A read started before a sign-out
    // and answered after it describes somebody else's view of the instance.
    const asked = bindRequest();
    const seq = mutationSeq;
    try {
      const state = await api.setupState();
      if (!asked()) return;
      // A mutation started meanwhile: its answer is newer than this read,
      // whichever arrives last. The next load() (step change) re-reads.
      if (seq !== mutationSeq) return;
      // What a refused save kept waits for the form this read remounts; a
      // step found unchanged remounts nothing, so nothing would take it.
      const kept = get().unsaved;
      const remounts = kept !== null && state.steps.some((s) => s.id === kept.step && s.etag !== kept.etag);
      set({ state, loading: false, bannerDismissed: readBannerDismissed(), unsaved: remounts ? kept : null });
    } catch (e) {
      if (!asked()) return;
      if (seq !== mutationSeq) return;
      // An older backend has no checklist: stay unknown, so nothing nags.
      if (statusOf(e) === 404) {
        set({ loading: false });
        return;
      }
      set({ loading: false, error: errorDetail(e, 'Could not read the setup state') });
    }
  },

  save: async (step, run, unsaved) => {
    const asked = bindRequest();
    mutationSeq += 1;
    set({ busy: step, error: null, fieldError: null, unsaved: null });
    try {
      const state = await run();
      if (!asked()) return false;
      set({ state });
      if (step === 'profile') {
        // A display name is a label everywhere else in the app; nothing
        // carries the name it changed. Never rejects.
        await useActorStore.getState().refresh();
      }
      return true;
    } catch (e) {
      if (!asked()) return false;
      const fieldError = fieldErrorOf(e);
      const refused = statusOf(e) === 409;
      set({
        fieldError,
        error: fieldError ? null : errorDetail(e, 'Could not save that'),
        // Kept before the re-read below, which may remount the form.
        unsaved: refused && unsaved !== undefined
          ? { step, etag: stepById(get(), step)?.etag ?? null, values: unsaved } : null,
      });
      // Somebody else got there first, or the step is read-only now: show
      // the server's sentence verbatim and re-read, rather than trust the view
      // this form was opened on.
      if (refused) void get().load();
      return false;
    } finally {
      if (asked()) set({ busy: null });
    }
  },

  skip: async (step, skipped) => {
    const asked = bindRequest();
    mutationSeq += 1;
    set({ busy: step, error: null, fieldError: null, unsaved: null });
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

  finish: async () => {
    const asked = bindRequest();
    mutationSeq += 1;
    set({ busy: 'finish', error: null, fieldError: null, unsaved: null });
    try {
      const response = await api.setupFinish();
      if (!asked()) return null;
      set({ state: response.state });
      return response;
    } catch (e) {
      if (!asked()) return null;
      set({ error: errorDetail(e, 'Could not finish setup') });
      return null;
    } finally {
      if (asked()) set({ busy: null });
    }
  },

  startOnboarding: async () => {
    const response = await get().finish();
    const { status, session_id: sessionId } = response?.onboarding ?? {};
    return status === 'running' ? sessionId ?? null : null;
  },

  skipOnboarding: async () => {
    const asked = bindRequest();
    mutationSeq += 1;
    set({ busy: 'onboarding', error: null, fieldError: null });
    try {
      const state = await api.setupSkipOnboarding();
      if (!asked()) return false;
      set({ state });
      return true;
    } catch (e) {
      if (!asked()) return false;
      set({ error: errorDetail(e, 'Could not skip onboarding') });
      return false;
    } finally {
      if (asked()) set({ busy: null });
    }
  },

  restart: async () => {
    const asked = bindRequest();
    mutationSeq += 1;
    set({ busy: 'restart', error: null, fieldError: null, unsaved: null, overlayDismissed: false });
    let outcome: RestartResponse;
    try {
      outcome = await api.restartSystem();
    } catch (e) {
      if (!asked()) return false;
      if (statusOf(e) === 409 && get().state) {
        // Another tab asked first. The process is going away either way, so
        // wait on the generation this tab knows about.
        const before = get().state!.generation;
        set({ busy: null, restartOutcome: {
          mode: 'restart', generation: before, message: errorDetail(e, 'A restart is already in progress.'),
          command: null, timeout_hint_seconds: 0,
        } });
        return get().waitForGeneration(before, RECONNECT_TIMEOUT_MS);
      }
      set({ busy: null, error: errorDetail(e, 'Could not restart Nerve') });
      return false;
    }
    if (!asked()) return false;
    set({ busy: null, restartOutcome: outcome });

    if (outcome.mode === 'reload') {
      // Nothing needed a restart; the server reloaded. Just re-read.
      await get().load();
      return true;
    }
    // `unsupported` means somebody with access to the server must restart
    // it; the wait still runs so a restart done there is noticed.
    const timeout = outcome.timeout_hint_seconds > 0
      ? outcome.timeout_hint_seconds * 1000
      : outcome.mode === 'unsupported' ? UNSUPPORTED_TIMEOUT_MS : RECONNECT_TIMEOUT_MS;
    return get().waitForGeneration(outcome.generation, timeout);
  },

  waitForGeneration: async (before, timeoutMs) => {
    // Wait for a *different* process, not for any answer at all. The daemon
    // that accepted the request keeps serving while it shuts down, so "is
    // anybody there" is satisfied by the very process being replaced.
    const token = ++waitToken;
    let asked = bindSender().stillCurrent;
    set({ reconnecting: true, reconnectFailed: false, reconnectElapsedMs: 0 });
    // Stop without a verdict. A newer wait owns `reconnecting` (leave it);
    // a sign-out does not, so the overlay must not outlive the person who
    // started it (`reset()` clears it too, belt and braces).
    const abandon = () => {
      if (token === waitToken) set({ reconnecting: false });
      return false;
    };
    // The deadline is wall-clock, not a count of sleeps: a probe that waits
    // on a half-open connection (bounded to a few seconds in the client, but
    // not zero) must count against the time-box too.
    const started = Date.now();
    let elapsed = 0;
    while (elapsed < timeoutMs) {
      await sleep(RECONNECT_INTERVAL_MS);
      elapsed = Date.now() - started;
      if (!asked() || token !== waitToken) return abandon();
      set({ reconnectElapsedMs: elapsed });
      const generation = await api.healthGeneration();
      if (!asked() || token !== waitToken) return abandon();
      if (generation === null || generation === before) continue;

      // The new process is up. Reconnect through the auth path of whichever
      // mode this is — `checkAuth()`: behind a front gateway it goes through
      // `enterHosted` (the gateway cookie, the principal re-bound and its
      // storage re-selected; a dead session re-enters through the gateway's
      // own sign-in, never a login form); locally it re-validates the stored
      // bearer (the signing secret is pinned, so it survives) and, on a
      // passwordless install, signs in again by itself. A failed check is
      // not a failed restart: the auth store has already shown what it
      // shows for that.
      clearUnavailable();
      try {
        await useAuthStore.getState().checkAuth();
      } catch {
        // checkAuth() reports through the auth store, never by throwing.
      }
      // `checkAuth()` begins a new auth session, so the guard bound at the
      // start of this wait is stale by design: re-bind to the session it
      // established (a sign-out mid-wait is respected from here on too).
      asked = bindSender().stillCurrent;
      if (!asked() || token !== waitToken) return abandon();
      await get().load();
      if (token !== waitToken) return false;
      set({ reconnecting: false, reconnectElapsedMs: elapsed, restartOutcome: null });
      return true;
    }
    if (token !== waitToken) return false;
    set({ reconnecting: false, reconnectFailed: true });
    return false;
  },

  retryWait: async () => {
    const outcome = get().restartOutcome;
    const before = outcome?.generation ?? get().state?.generation;
    if (!before) return false;
    const timeout = outcome && outcome.timeout_hint_seconds > 0
      ? outcome.timeout_hint_seconds * 1000
      : RECONNECT_TIMEOUT_MS;
    set({ overlayDismissed: false });
    return get().waitForGeneration(before, timeout);
  },

  loadPreflight: async () => {
    const asked = bindRequest();
    try {
      const preflight = await api.setupSourcesPreflight();
      if (asked()) set({ preflight });
    } catch {
      // The cards render without preflight results; "Re-check" tries again.
    }
  },

  loadAgents: async () => {
    const asked = bindRequest();
    try {
      const agentsDetected = await api.setupAgentsDetect();
      if (asked()) set({ agentsDetected });
    } catch {
      // Detection is a convenience; the form still works from `detail`.
    }
  },

  loadEnvironment: async () => {
    const asked = bindRequest();
    try {
      const environment = await api.setupEnvironment();
      if (asked()) set({ environment });
    } catch {
      // The Review step says the check-up could not run.
    }
  },

  loadModels: async () => {
    const asked = bindRequest();
    try {
      const models = await api.setupModels();
      if (asked()) set({ models });
    } catch {
      // The picker falls back to a free-text model id.
    }
  },

  loadHostLogin: async (refresh = false) => {
    const asked = bindRequest();
    try {
      const hostLogin = await api.setupHostLogin(refresh);
      if (asked()) set({ hostLogin });
    } catch {
      if (asked()) set({ hostLogin: { available: false, kind: null, source: null } });
    }
  },

  dismissBanner: () => {
    writeBannerDismissed();
    set({ bannerDismissed: true });
  },

  dismissRestartOverlay: () => set({ overlayDismissed: true }),
}));
