import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../api/client', async () => {
  const actual = await vi.importActual<typeof import('../api/client')>('../api/client');
  return {
    ...actual,
    api: {
      authStatus: vi.fn(async () => ({ auth_required: true, login: 'password' })),
      getViewer: vi.fn(),
      login: vi.fn(),
      listActors: vi.fn(async () => ({ actors: [] })),
      setupState: vi.fn(),
      setupStep: vi.fn(),
      setupSkip: vi.fn(),
      setupFinish: vi.fn(),
      restartSystem: vi.fn(),
      healthGeneration: vi.fn(),
    },
    setUnauthorizedHandler: vi.fn(),
  };
});
vi.mock('./helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('./helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const { api, clearToken, setToken } = await import('../api/client');
const { ApiError, setAuthMode, useHostedStore } = await import('../api/hosted');
const { useAuthStore } = await import('./authStore');
const { selectViewerStorage } = await import('./helpers/viewerStorage');
const {
  RECONNECT_INTERVAL_MS, RECONNECT_TIMEOUT_MS, fieldErrorOf, providerConfigured,
  setupIsUnfinished, setupNotStarted, stepById, stepsLeft, unsavedFor, useSetupStore,
} = await import('./setupStore');
const { makeFreshState, makeState, step, withStep } = await import('../test/setupFixtures');

type Mock = ReturnType<typeof vi.fn>;
const setupState = api.setupState as unknown as Mock;
const setupStep = api.setupStep as unknown as Mock;
const setupSkip = api.setupSkip as unknown as Mock;
const setupFinish = api.setupFinish as unknown as Mock;
const restartSystem = api.restartSystem as unknown as Mock;
const healthGeneration = api.healthGeneration as unknown as Mock;
const listActors = api.listActors as unknown as Mock;
const authStatus = api.authStatus as unknown as Mock;
const getViewer = api.getViewer as unknown as Mock;

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let release!: (value: T) => void;
  const promise = new Promise<T>((resolve) => { release = resolve; });
  return { promise, resolve: release };
}

const restartOutcome = (mode: 'restart' | 'reload' | 'unsupported', extra = {}) => ({
  mode, generation: 'G0', message: 'msg', command: null, timeout_hint_seconds: 0, ...extra,
});

beforeEach(() => {
  vi.clearAllMocks();
  sessionStorage.clear();
  selectViewerStorage(null);
  useHostedStore.setState({ mode: 'local', problem: null, reentering: false, principalId: null });
  useSetupStore.getState().reset();
  useAuthStore.setState({
    authenticated: true, ready: true, loading: false, error: null, sessionExpired: false,
    loginMode: 'password', viewer: { id: 'actor-1', kind: 'human', display_name: 'Alice', username: 'alice' },
    account: { id: 'acc-1', username: 'alice' },
  });
  setupState.mockResolvedValue(makeState());
  getViewer.mockResolvedValue({ actor: { id: 'actor-1', kind: 'human', display_name: 'Alice', username: 'alice' }, account: null });
});

afterEach(() => {
  vi.useRealTimers();
  clearToken();
});

describe('load', () => {
  it('replaces the state with the server answer', async () => {
    await useSetupStore.getState().load();
    expect(useSetupStore.getState().state?.steps_left).toBe(7);
    expect(useSetupStore.getState().loading).toBe(false);
  });

  it('works the same in hosted mode', async () => {
    setAuthMode('external');
    await useSetupStore.getState().load();
    expect(setupState).toHaveBeenCalledTimes(1);
    expect(useSetupStore.getState().state).not.toBeNull();
  });

  it('stays unknown, without an error, when an older backend answers 404', async () => {
    setupState.mockRejectedValue(new ApiError(404, 'Not Found'));
    await useSetupStore.getState().load();
    expect(useSetupStore.getState().state).toBeNull();
    expect(useSetupStore.getState().error).toBeNull();
    expect(useSetupStore.getState().loading).toBe(false);
  });

  it('reports any other failure', async () => {
    setupState.mockRejectedValue(new Error('500: {"detail": "boom"}'));
    await useSetupStore.getState().load();
    expect(useSetupStore.getState().error).toBe('boom');
  });

  it('drops an answer that arrives after a sign-out', async () => {
    const pending = deferred<ReturnType<typeof makeState>>();
    setupState.mockReturnValue(pending.promise);
    const loading = useSetupStore.getState().load();
    useAuthStore.getState().logout();
    pending.resolve(makeState());
    await loading;
    expect(useSetupStore.getState().state).toBeNull();
  });

  it('drops an answer that arrives after a reset, so a later view never opens on the state it cleared', async () => {
    const pending = deferred<ReturnType<typeof makeState>>();
    setupState.mockReturnValueOnce(pending.promise);
    const loading = useSetupStore.getState().load();
    useSetupStore.getState().reset();
    pending.resolve(makeState());
    await loading;
    expect(useSetupStore.getState().state).toBeNull();
  });

  it('drops a read that was in flight when a mutation started, so a stale state never overwrites a save', async () => {
    // The server serves the GET first (pre-save state) under its lock; the
    // browser receives it after the save's answer.
    const stale = deferred<ReturnType<typeof makeState>>();
    setupState.mockReturnValueOnce(stale.promise);
    const reading = useSetupStore.getState().load();
    const saved = withStep(makeState(), step('provider', { status: 'done' }));
    setupStep.mockResolvedValue(saved);
    await useSetupStore.getState().save('provider', () => api.setupStep('provider', {}));
    expect(useSetupStore.getState().state?.steps_left).toBe(6);
    stale.resolve(makeState());        // 7 pending: the world before the save
    await reading;
    expect(useSetupStore.getState().state?.steps_left).toBe(6);
    expect(stepById(useSetupStore.getState(), 'provider')?.status).toBe('done');
  });
});

describe('save, skip, finish', () => {
  it('replaces the state and refreshes actor names after a profile save', async () => {
    const next = withStep(makeState(), step('profile', { status: 'done' }));
    setupStep.mockResolvedValue(next);
    const ok = await useSetupStore.getState().save('profile', () => api.setupStep('profile', { name: 'Alice' }));
    expect(ok).toBe(true);
    expect(stepById(useSetupStore.getState(), 'profile')?.status).toBe('done');
    expect(listActors).toHaveBeenCalled();
    expect(useSetupStore.getState().busy).toBeNull();
  });

  it('drops a save answered after a reset', async () => {
    const pending = deferred<ReturnType<typeof makeState>>();
    setupStep.mockReturnValueOnce(pending.promise);
    const saving = useSetupStore.getState().save('profile', () => api.setupStep('profile', {}));
    useSetupStore.getState().reset();
    pending.resolve(withStep(makeState(), step('profile', { status: 'done' })));
    expect(await saving).toBe(false);
    expect(useSetupStore.getState().state).toBeNull();
  });

  it('attaches a 400 with a field to that field, not to the page', async () => {
    setupStep.mockRejectedValue(new ApiError(400, '{"detail": "Not a time zone name.", "field": "timezone"}'));
    const ok = await useSetupStore.getState().save('profile', () => api.setupStep('profile', { timezone: 'x' }));
    expect(ok).toBe(false);
    expect(useSetupStore.getState().fieldError).toEqual({ field: 'timezone', detail: 'Not a time zone name.' });
    expect(useSetupStore.getState().error).toBeNull();
  });

  it('shows a 409 verbatim and re-reads the state', async () => {
    setupStep.mockRejectedValue(new ApiError(409, '{"detail": "Mode was chosen already."}'));
    await useSetupStore.getState().save('mode', () => api.setupStep('mode', { mode: 'worker' }));
    expect(useSetupStore.getState().error).toBe('Mode was chosen already.');
    expect(setupState).toHaveBeenCalledTimes(1);
  });

  it('keeps what a refused form held for the form that a changed step remounts, and for nothing else', async () => {
    await useSetupStore.getState().load();
    const opened = stepById(useSetupStore.getState(), 'sources')!;
    const values = { gmail: false };
    const refuse = (status: number) => setupStep.mockRejectedValueOnce(
      new ApiError(status, '{"detail": "Refused.", "field": "github"}'),
    );
    const saveSources = () => useSetupStore.getState().save('sources', () => api.setupStep('sources', {}), values);

    // The read after a 409 finds the step changed: the form it remounts takes them.
    const changed = withStep(makeState(), step('sources', { etag: 'sources-etag-2' }));
    setupState.mockResolvedValueOnce(changed);
    refuse(409);
    await saveSources();
    await vi.waitFor(() => expect(useSetupStore.getState().state).toBe(changed));
    expect(unsavedFor(stepById(useSetupStore.getState(), 'sources')!)).toEqual(values);
    expect(unsavedFor(opened)).toBeNull();
    // The next write drops them.
    setupSkip.mockResolvedValueOnce(makeState());
    await useSetupStore.getState().skip('memory', true);
    expect(useSetupStore.getState().unsaved).toBeNull();

    // A step the read finds unchanged remounts nothing, so they are not kept.
    setupState.mockResolvedValueOnce(makeState());
    refuse(409);
    await saveSources();
    await vi.waitFor(() => expect(setupState).toHaveBeenCalledTimes(3));
    await vi.waitFor(() => expect(useSetupStore.getState().unsaved).toBeNull());

    // A 400 re-reads nothing and keeps nothing.
    refuse(400);
    await saveSources();
    expect(useSetupStore.getState().unsaved).toBeNull();
  });

  it('skips and puts back through the server', async () => {
    const skipped = withStep(makeState(), step('memory', { status: 'skipped' }));
    setupSkip.mockResolvedValueOnce(skipped).mockResolvedValueOnce(makeState());
    await useSetupStore.getState().skip('memory', true);
    expect(setupSkip).toHaveBeenCalledWith('memory', true);
    expect(stepById(useSetupStore.getState(), 'memory')?.status).toBe('skipped');
    await useSetupStore.getState().skip('memory', false);
    expect(setupSkip).toHaveBeenCalledWith('memory', false);
    expect(stepById(useSetupStore.getState(), 'memory')?.status).toBe('pending');
  });

  it('finish stores the returned state and hands back the action', async () => {
    const finished = makeState({ finished: true, steps_left: 0 });
    setupFinish.mockResolvedValue({ state: finished, action: 'reloaded', onboarding: { status: 'not_applicable', session_id: null } });
    const result = await useSetupStore.getState().finish();
    expect(result?.action).toBe('reloaded');
    expect(useSetupStore.getState().state?.finished).toBe(true);
  });
});

describe('restart', () => {
  it('reload: nothing to wait for, just re-reads', async () => {
    restartSystem.mockResolvedValue(restartOutcome('reload'));
    const ok = await useSetupStore.getState().restart();
    expect(ok).toBe(true);
    expect(setupState).toHaveBeenCalledTimes(1);
    expect(useSetupStore.getState().reconnecting).toBe(false);
    expect(healthGeneration).not.toHaveBeenCalled();
  });

  it('unsupported: keeps the outcome and still polls for a new generation', async () => {
    vi.useFakeTimers();
    restartSystem.mockResolvedValue(restartOutcome('unsupported', { command: 'systemctl restart nerve.service', timeout_hint_seconds: 300 }));
    healthGeneration.mockResolvedValue(null);
    const run = useSetupStore.getState().restart();
    await vi.advanceTimersByTimeAsync(RECONNECT_INTERVAL_MS * 3);
    expect(useSetupStore.getState().restartOutcome?.mode).toBe('unsupported');
    expect(useSetupStore.getState().restartOutcome?.command).toBe('systemctl restart nerve.service');
    expect(useSetupStore.getState().reconnecting).toBe(true);
    expect(healthGeneration).toHaveBeenCalledTimes(3);
    healthGeneration.mockResolvedValue('G1');
    await vi.advanceTimersByTimeAsync(RECONNECT_INTERVAL_MS);
    await expect(run).resolves.toBe(true);
  });

  it('restart: polls /health until the generation changes, then reconnects through checkAuth and reloads', async () => {
    vi.useFakeTimers();
    setToken('session-token');   // the stored bearer survives the restart (the signing secret is pinned)
    restartSystem.mockResolvedValue(restartOutcome('restart', { timeout_hint_seconds: 120 }));
    healthGeneration
      .mockResolvedValueOnce(null)       // down
      .mockResolvedValueOnce('G0')       // the old process, still answering
      .mockResolvedValueOnce('G1');      // the new one
    const run = useSetupStore.getState().restart();
    await vi.advanceTimersByTimeAsync(RECONNECT_INTERVAL_MS);
    expect(useSetupStore.getState().reconnecting).toBe(true);
    expect(useSetupStore.getState().reconnectElapsedMs).toBe(RECONNECT_INTERVAL_MS);
    await vi.advanceTimersByTimeAsync(RECONNECT_INTERVAL_MS * 2);
    await expect(run).resolves.toBe(true);
    expect(authStatus).toHaveBeenCalled();           // checkAuth: the descriptor …
    expect(getViewer).toHaveBeenCalled();            // … and the session survived
    expect(api.login).not.toHaveBeenCalled();
    expect(useAuthStore.getState().authenticated).toBe(true);
    expect(setupState).toHaveBeenCalledTimes(1);     // load()
    expect(useSetupStore.getState().reconnecting).toBe(false);
    expect(useSetupStore.getState().reconnectFailed).toBe(false);
  });

  it('behind a front gateway the reconnect never reaches for a login form, even when the viewer read is refused', async () => {
    vi.useFakeTimers();
    setAuthMode('external');
    authStatus.mockResolvedValue({ mode: 'external', auth_required: true, login: 'username_password' });
    getViewer.mockRejectedValue(new ApiError(401, 'Unauthorized'));
    restartSystem.mockResolvedValue(restartOutcome('restart', { timeout_hint_seconds: 120 }));
    healthGeneration.mockResolvedValue('G1');
    const run = useSetupStore.getState().restart();
    await vi.advanceTimersByTimeAsync(RECONNECT_INTERVAL_MS);
    await expect(run).resolves.toBe(true);
    expect(authStatus).toHaveBeenCalled();           // checkAuth → enterHosted
    expect(getViewer).toHaveBeenCalled();
    expect(api.login).not.toHaveBeenCalled();        // no password login exists in this mode
    expect(useHostedStore.getState().mode).toBe('external');
    expect(useSetupStore.getState().reconnecting).toBe(false);
    expect(useSetupStore.getState().reconnectFailed).toBe(false);
  });

  it('a second tab gets 409 and still waits on the generation it knows', async () => {
    vi.useFakeTimers();
    await useSetupStore.getState().load();
    restartSystem.mockRejectedValue(new ApiError(409, '{"detail": "A restart is already in progress."}'));
    healthGeneration.mockResolvedValue('G1');
    const run = useSetupStore.getState().restart();
    await vi.advanceTimersByTimeAsync(RECONNECT_INTERVAL_MS);
    await expect(run).resolves.toBe(true);
    expect(useSetupStore.getState().error).toBeNull();
  });

  it('times out, stops polling, and Retry runs another bounded wait', async () => {
    vi.useFakeTimers();
    restartSystem.mockResolvedValue(restartOutcome('restart'));
    healthGeneration.mockResolvedValue(null);
    const run = useSetupStore.getState().restart();
    await vi.advanceTimersByTimeAsync(RECONNECT_TIMEOUT_MS);
    await expect(run).resolves.toBe(false);
    expect(useSetupStore.getState().reconnectFailed).toBe(true);
    expect(useSetupStore.getState().reconnecting).toBe(false);
    const calls = healthGeneration.mock.calls.length;
    await vi.advanceTimersByTimeAsync(RECONNECT_INTERVAL_MS * 5);
    expect(healthGeneration).toHaveBeenCalledTimes(calls);  // no more polling

    healthGeneration.mockResolvedValue('G1');
    const retry = useSetupStore.getState().retryWait();
    expect(useSetupStore.getState().reconnecting).toBe(true);
    await vi.advanceTimersByTimeAsync(RECONNECT_INTERVAL_MS);
    await expect(retry).resolves.toBe(true);
    expect(useSetupStore.getState().reconnectFailed).toBe(false);
  });

  it('dismissing the overlay hides it without stopping the poll', async () => {
    vi.useFakeTimers();
    restartSystem.mockResolvedValue(restartOutcome('restart'));
    healthGeneration.mockResolvedValue(null);
    const run = useSetupStore.getState().restart();
    await vi.advanceTimersByTimeAsync(RECONNECT_INTERVAL_MS);
    useSetupStore.getState().dismissRestartOverlay();
    expect(useSetupStore.getState().overlayDismissed).toBe(true);
    expect(useSetupStore.getState().reconnecting).toBe(true);
    const after = makeState({ finished: true, steps_left: 0, generation: 'G1' });
    setupState.mockResolvedValue(after);
    healthGeneration.mockResolvedValue('G1');
    await vi.advanceTimersByTimeAsync(RECONNECT_INTERVAL_MS);
    await expect(run).resolves.toBe(true);
    expect(useSetupStore.getState().state?.generation).toBe('G1');  // load() ran after dismissal
  });

  it('a sign-out during the wait ends it: no overlay and no state for the next person', async () => {
    vi.useFakeTimers();
    await useSetupStore.getState().load();
    restartSystem.mockResolvedValue(restartOutcome('restart'));
    healthGeneration.mockResolvedValue(null);
    const run = useSetupStore.getState().restart();
    await vi.advanceTimersByTimeAsync(RECONNECT_INTERVAL_MS);
    expect(useSetupStore.getState().reconnecting).toBe(true);
    expect(useSetupStore.getState().state).not.toBeNull();

    useAuthStore.getState().logout();   // purgeAccountScopedState → setupStore.reset()
    expect(useSetupStore.getState().reconnecting).toBe(false);
    expect(useSetupStore.getState().state).toBeNull();
    expect(useSetupStore.getState().restartOutcome).toBeNull();
    await vi.advanceTimersByTimeAsync(RECONNECT_INTERVAL_MS * 2);
    await expect(run).resolves.toBe(false);
    expect(useSetupStore.getState().reconnecting).toBe(false);
    expect(useSetupStore.getState().reconnectFailed).toBe(false);
    const calls = healthGeneration.mock.calls.length;
    await vi.advanceTimersByTimeAsync(RECONNECT_INTERVAL_MS * 3);
    expect(healthGeneration).toHaveBeenCalledTimes(calls);   // the abandoned wait polls no more
  });

  it('a restart that cannot even be asked for is an error, not a wait', async () => {
    restartSystem.mockRejectedValue(new Error('500: {"detail": "The restart could not be started"}'));
    const ok = await useSetupStore.getState().restart();
    expect(ok).toBe(false);
    expect(useSetupStore.getState().error).toMatch(/could not be started/);
    expect(useSetupStore.getState().reconnecting).toBe(false);
  });
});

describe('banner dismissal', () => {
  it('is remembered for the tab session, per viewer', async () => {
    await useSetupStore.getState().load();
    expect(useSetupStore.getState().bannerDismissed).toBe(false);
    useSetupStore.getState().dismissBanner();
    expect(sessionStorage.getItem('nerve_setup_banner_dismissed')).toBe('1');
    useSetupStore.getState().reset();
    await useSetupStore.getState().load();
    expect(useSetupStore.getState().bannerDismissed).toBe(true);

    selectViewerStorage('principal-2');
    useSetupStore.getState().reset();
    await useSetupStore.getState().load();
    expect(useSetupStore.getState().bannerDismissed).toBe(false);
    useSetupStore.getState().dismissBanner();
    expect(sessionStorage.getItem('nerve_principal_principal-2:nerve_setup_banner_dismissed')).toBe('1');
  });
});

describe('selectors and helpers', () => {
  it('reads unknown as finished and counts only pending steps', async () => {
    expect(setupIsUnfinished(useSetupStore.getState())).toBe(false);
    expect(stepsLeft(useSetupStore.getState())).toBe(0);
    expect(providerConfigured(useSetupStore.getState())).toBeNull();
    await useSetupStore.getState().load();
    expect(setupIsUnfinished(useSetupStore.getState())).toBe(true);
    expect(stepsLeft(useSetupStore.getState())).toBe(7);
    expect(providerConfigured(useSetupStore.getState())).toBe(false);
    useSetupStore.setState({ state: withStep(makeState(), step('provider', { status: 'done' })) });
    expect(providerConfigured(useSetupStore.getState())).toBe(true);
  });

  it('reads "not started" only while Mode is pending on an unfinished checklist', () => {
    expect(setupNotStarted(useSetupStore.getState())).toBe(false);   // unknown
    useSetupStore.setState({ state: makeFreshState() });
    expect(setupNotStarted(useSetupStore.getState())).toBe(true);
    // Mode chosen with steps left is the banner's case; finished is nobody's.
    useSetupStore.setState({ state: makeState() });
    expect(setupNotStarted(useSetupStore.getState())).toBe(false);
    useSetupStore.setState({ state: makeState({ finished: true, steps_left: 0 }) });
    expect(setupNotStarted(useSetupStore.getState())).toBe(false);
    // Under lockdown nothing on the page can answer Mode, so it is not "never set up".
    useSetupStore.setState({
      state: withStep(makeFreshState({ lockdown: true }), step('mode', {
        status: 'blocked', blocked_kind: 'lockdown',
        blocked_reason: 'Configuration is managed from the config repository on this instance.',
      })),
    });
    expect(setupNotStarted(useSetupStore.getState())).toBe(false);
  });

  it('parses a field error and nothing else', () => {
    expect(fieldErrorOf(new Error('400: {"detail": "No.", "field": "bot_token"}'))).toEqual({ field: 'bot_token', detail: 'No.' });
    expect(fieldErrorOf(new Error('409: {"detail": "No."}'))).toBeNull();
    expect(fieldErrorOf(new Error('nope'))).toBeNull();
  });
});
