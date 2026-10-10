import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes, useLocation, useParams } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { SetupAutomationDetail, SetupState } from '../../api/client';

vi.mock('../../api/client', async () => {
  const actual = await vi.importActual<typeof import('../../api/client')>('../../api/client');
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
      setupProviderTest: vi.fn(),
      setupHostLogin: vi.fn(),
      setupModels: vi.fn(),
      setupMemoryTest: vi.fn(),
      setupTelegramTest: vi.fn(),
      setupSlackTest: vi.fn(),
      setupPairing: vi.fn(),
      setupPairingStart: vi.fn(),
      setupSourcesPreflight: vi.fn(),
      setupAgentsDetect: vi.fn(),
      setupEnvironment: vi.fn(),
      restartSystem: vi.fn(),
      healthGeneration: vi.fn(),
    },
    setUnauthorizedHandler: vi.fn(),
  };
});
vi.mock('../../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const { api } = await import('../../api/client');
const { ApiError, useHostedStore } = await import('../../api/hosted');
const { useAuthStore } = await import('../../stores/authStore');
const { useSetupStore } = await import('../../stores/setupStore');
const { browserTimeZone } = await import('../../utils/timeZones');
const {
  CHANNELS, MODE_LOCK_REASON, makeExternalState, makeState, on, step, withStep,
} = await import('../../test/setupFixtures');
const { SetupPage } = await import('../../pages/SetupPage');
const { ReadyScreen } = await import('./ReadyScreen');

type Mock = ReturnType<typeof vi.fn>;
const m = (fn: unknown) => fn as Mock;

let current: SetupState;

function ChatProbe() {
  const location = useLocation();
  const { sessionId } = useParams();
  const seed = (location.state as { suggestedMessage?: string } | null)?.suggestedMessage;
  return <div>chat page {sessionId ?? ''} {seed ?? ''}</div>;
}

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/setup" element={<SetupPage />} />
        <Route path="/setup/:stepId" element={<SetupPage />} />
        <Route path="/chat" element={<ChatProbe />} />
        <Route path="/chat/:sessionId" element={<ChatProbe />} />
      </Routes>
    </MemoryRouter>,
  );
}

const heading = (name: string | RegExp) => screen.findByRole('heading', { level: 1, name });
const continueButton = () => screen.getByRole('button', { name: /^(Continue|Next|Save anyway)/ });
/** Every step decided, as the Review sees it at the end of the forward flow. */
const decided = (overrides: Partial<SetupState> = {}) => makeState({
  steps: makeState().steps.map((s) => (s.status === 'pending' ? step(s.id, { status: 'done' }) : s)),
  ...overrides,
});

const ENVIRONMENT = {
  checks: [
    { id: 'claude_cli', label: 'Claude Code CLI', status: 'ok', detail: '2.1.0', hint: null },
    { id: 'gh', label: 'GitHub CLI', status: 'missing', detail: null, hint: 'Install gh and run gh auth login.' },
  ],
  served: { scheme: 'http', host_is_loopback: true, client_is_loopback: true, https_hint: false },
  bind_host: '127.0.0.1', port: 8977, in_container: false, supervision: 'none', passwordless: false,
  restart_mode: 'restart', restart_message: 'Restarting stops this process and starts a new one.',
  checked_at: '2026-10-10T09:12:03Z',
};

const realMatchMedia = window.matchMedia;

beforeEach(() => {
  vi.clearAllMocks();
  sessionStorage.clear();
  current = makeState();
  useHostedStore.setState({ mode: 'local', problem: null, reentering: false, principalId: null });
  useSetupStore.getState().reset();
  useAuthStore.setState({
    authenticated: true, ready: true, loading: false, error: null, sessionExpired: false,
    loginMode: 'password',
    viewer: { id: 'actor-1', kind: 'human', display_name: 'Alice', username: 'alice' },
    account: { id: 'acc-1', username: 'alice' },
  });
  m(api.setupState).mockImplementation(async () => current);
  m(api.setupStep).mockImplementation(async () => current);
  m(api.setupSkip).mockImplementation(async () => current);
  m(api.setupHostLogin).mockResolvedValue({ available: false, kind: null, source: null });
  m(api.setupModels).mockResolvedValue({
    models: [
      { id: 'claude-opus-5', label: 'Claude Opus 5', family: 'opus', recommended: true },
      { id: 'claude-sonnet-4-6', label: 'Claude Sonnet 4.6', family: 'sonnet', recommended: false },
    ],
    current: { model: 'claude-sonnet-4-6', effort: 'high' },
    efforts: ['max', 'xhigh', 'high', 'medium', 'low'],
    discovered: true,
  });
  m(api.setupProviderTest).mockResolvedValue({ ok: true, checked: 'api', code: 'ok', detail: 'claude-haiku answered', hint: null, latency_ms: 800 });
  m(api.setupSourcesPreflight).mockResolvedValue({
    github: { cli: true, authenticated: false, login: null, hint: 'Run gh auth login on the server.' },
    gmail: { cli: false, hint: 'Install gog.' },
    telegram: { credentials_configured: false },
    checked_at: '2026-10-10T09:12:03Z',
  });
  m(api.setupAgentsDetect).mockResolvedValue({ agents: [
    { id: 'codex', label: 'Codex', installed: true, version: 'codex-cli 0.40.0', config_paths: ['~/.codex/config.toml'], config_exists: false, enabled: false },
    { id: 'claude-code', label: 'Claude Code', installed: false, version: null, config_paths: ['~/.claude/settings.json'], config_exists: false, enabled: false },
  ] });
  m(api.setupEnvironment).mockResolvedValue(ENVIRONMENT);
  m(api.setupPairing).mockResolvedValue({
    available: true, reason: null, live: true, bot_username: 'example_assistant_bot',
    link: 'https://t.me/example_assistant_bot', allowed_users_count: 0, code_active: false, expires_at: null,
  });
  m(api.setupPairingStart).mockResolvedValue({
    code: '483920', expires_at: new Date(Date.now() + 3_600_000).toISOString(),
    bot_username: 'example_assistant_bot', link: 'https://t.me/example_assistant_bot', allowed_users_count: 0,
  });
  m(api.getViewer).mockResolvedValue({ actor: { id: 'actor-1', kind: 'human', display_name: 'Alice', username: 'alice' }, account: null });
});

afterEach(() => {
  window.matchMedia = realMatchMedia;
  vi.useRealTimers();
});

describe('rail and overview', () => {
  it('marks the current step and shows a status per step', async () => {
    renderAt('/setup/provider');
    await heading('Connect a model');
    const nav = screen.getByRole('navigation', { name: 'Setup steps' });
    const model = within(nav).getByRole('link', { name: /^Model/ });
    expect(model).toHaveAttribute('aria-current', 'step');
    expect(model).toHaveTextContent('To do');
    expect(within(nav).getByRole('link', { name: /^Mode(?!l)/ })).toHaveTextContent('Done');
    expect(within(nav).getByRole('link', { name: /^Mode(?!l)/ })).not.toHaveAttribute('aria-current');
    expect(within(nav).getByRole('link', { name: /Review/ })).toBeInTheDocument();
    // One <h1> per page: the step heading. The header title is plain text.
    expect(screen.getAllByRole('heading', { level: 1 })).toHaveLength(1);
    expect(screen.getByText('Set up Nerve').tagName).not.toBe('H1');
  });

  it('steps that wait for Mode read "After Mode", not "Blocked" with a lock', async () => {
    const waiting = (id: Parameters<typeof step>[0]) => step(id, {
      status: 'blocked', read_only: true,
      blocked_reason: 'Choose a mode first — the files this step fills in depend on it',
    });
    current = makeState({
      mode: null,
      steps: [
        step('claim', { status: 'done' }), step('mode', {}, { mode: null, templates_present: [] }), step('provider'),
        waiting('profile'), step('memory'), step('channels'), waiting('sources'), waiting('automation'), waiting('agents'),
      ],
    });
    renderAt('/setup');
    await heading(/^Welcome, /);
    const nav = screen.getByRole('navigation', { name: 'Setup steps' });
    expect(within(nav).getAllByText('After Mode')).toHaveLength(4);
    expect(within(nav).queryByText('Blocked')).not.toBeInTheDocument();
    expect(within(nav).queryByText(/Choose a mode first/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Choose a mode first/)).not.toBeInTheDocument();   // nor on the overview list
  });

  it('Overview orients the person and Continue goes to the first pending step', async () => {
    renderAt('/setup');
    await heading('Welcome, Alice');
    // Locally the heading has said who; the line under it moves on instead
    // of repeating the name.
    expect(screen.queryByText(/Signed in as/)).not.toBeInTheDocument();
    expect(screen.getByText('Three decisions and Nerve is ready: a mode, a model, a few facts about you.')).toBeInTheDocument();
    expect(screen.getByText(/2 of 9/)).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(await heading('Connect a model')).toBeInTheDocument();
  });

  it('external mode: no claim step, the actor name, no account controls', async () => {
    useHostedStore.setState({ mode: 'external' });
    useAuthStore.setState({
      viewer: { id: '11111111-2222-4333-8444-555555555555', kind: 'human', display_name: 'Alice Example', username: null },
      account: null,
    });
    current = makeExternalState();
    renderAt('/setup');
    await heading(/^Welcome, /);
    expect(screen.getByText(/Signed in as/)).toHaveTextContent("Alice Example through your organisation's sign-in");
    expect(screen.getByText(/configures the instance for everyone/)).toBeInTheDocument();
    const nav = screen.getByRole('navigation', { name: 'Setup steps' });
    expect(within(nav).queryByRole('link', { name: /Claim/ })).not.toBeInTheDocument();
    expect(within(nav).getByRole('link', { name: /^Mode(?!l)/ })).toBeInTheDocument();

    await userEvent.click(within(nav).getByRole('link', { name: /About you/ }));
    await heading('About you');
    expect(screen.getByLabelText('Your name')).toHaveValue('Alice Example');
    expect(screen.getByLabelText('Your name')).toHaveAccessibleDescription(/Prefilled from your organisation's sign-in/);
    expect(screen.getByLabelText('Your name')).toHaveAccessibleDescription(/signed in as Alice Example/);
    expect(screen.queryByLabelText(/password/i)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/setup token/i)).not.toBeInTheDocument();
  });

  it('external mode: a saved USER.md name is called shared, and the help still says who is signed in', async () => {
    useHostedStore.setState({ mode: 'external' });
    useAuthStore.setState({
      viewer: { id: '11111111-2222-4333-8444-555555555555', kind: 'human', display_name: 'Alice Example', username: null },
      account: null,
    });
    // Somebody else saved "Bob" into USER.md; Alice opens the step.
    current = withStep(makeExternalState(), step('profile', { status: 'done' }, {
      name: 'Bob', name_source: 'user_md', actor_name: 'Alice Example', display_name_editable: false,
    }));
    renderAt('/setup/profile');
    await heading('About you');
    const field = screen.getByLabelText('Your name');
    expect(field).toHaveValue('Bob');
    expect(field).toHaveAccessibleDescription(/shared by everyone on this instance/);
    expect(field).toHaveAccessibleDescription(/You are signed in as Alice Example/);
    expect(field).not.toHaveAccessibleDescription(/Prefilled from/);
  });

  it('the model picker names the server default instead of "keep the current model"', async () => {
    current = withStep(makeState(), step('provider', { status: 'done' }, {
      credential: 'api_key', anthropic_api_key: on('ab12'), model: 'claude-sonnet-4-6', effort: 'high',
    }));
    renderAt('/setup/provider');
    await heading('Connect a model');
    expect(await screen.findByRole('option', { name: 'Server default (claude-sonnet-4-6)' })).toBeInTheDocument();
    expect(screen.queryByRole('option', { name: /Keep the current model/ })).not.toBeInTheDocument();
    // The guidance says which effort the server currently has, instead of
    // calling one of them "recommended" while another is the default.
    expect(screen.getByText(/This is the server's current default/)).toBeInTheDocument();
    expect(screen.queryByText(/recommended setting/)).not.toBeInTheDocument();
  });

  it('Sources points back at Automation when a source is on but the inbox job is off', async () => {
    const automation = makeState().steps.find((s) => s.id === 'automation')!.detail as SetupAutomationDetail;
    current = withStep(makeState(), step('automation', { status: 'done' }, {
      optional: automation.optional.map((job) => (job.id === 'inbox-processor' ? { ...job, enabled: false } : job)),
    }));
    renderAt('/setup/sources');
    await heading('What should Nerve read?');
    expect(screen.queryByText(/Inbox processor job is off/)).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('checkbox', { name: /GitHub/ }));
    const notice = screen.getByText(/Inbox processor job is off/).closest('[role="status"]') as HTMLElement;
    expect(within(notice).getByRole('link', { name: 'Automation' })).toHaveAttribute('href', '/setup/automation');
    await userEvent.click(screen.getByRole('checkbox', { name: /GitHub/ }));
    expect(screen.queryByText(/Inbox processor job is off/)).not.toBeInTheDocument();
  });

  it('a finished instance opens the Overview with Change links and counts the skips', async () => {
    current = makeState({
      finished: true, steps_left: 0,
      steps: makeState().steps.map((s) => (
        s.id === 'mode' || s.id === 'claim' ? s
          : s.id === 'memory' || s.id === 'channels' ? step(s.id, { status: 'skipped' })
            : step(s.id, { status: 'done' })
      )),
    });
    renderAt('/setup');
    await heading('Everything is set up');
    expect(screen.getAllByRole('link', { name: 'Change' }).length).toBeGreaterThan(0);
    expect(screen.getByText('7 done · 2 skipped')).toBeInTheDocument();
    expect(screen.queryByText(/Already configured/)).not.toBeInTheDocument();
    // Done means chat: the primary action goes there; the review is a link.
    expect(screen.getByRole('link', { name: 'Open review' })).toHaveAttribute('href', '/setup/review');
    await userEvent.click(screen.getByRole('button', { name: /Back to chat/ }));
    expect(await screen.findByText(/chat page/)).toBeInTheDocument();
  });

  it('greets the person by name and leaves "Set up Nerve" to the page header alone', async () => {
    renderAt('/setup');
    const h1 = await heading('Welcome, Alice');
    expect(screen.getAllByRole('heading', { level: 1 })).toEqual([h1]);
    expect(screen.getByText('Set up Nerve').tagName).not.toBe('H1');
    // The claim step is the person's account once it is done.
    const nav = screen.getByRole('navigation', { name: 'Setup steps' });
    expect(within(nav).getByRole('link', { name: /^Account/ })).toBeInTheDocument();
    expect(within(nav).queryByRole('link', { name: /^Claim/ })).not.toBeInTheDocument();
  });

  it('"Already configured" names only steps whose configuration provably came from outside the wizard', async () => {
    // A credential from the server's environment is such a step.
    current = withStep(makeState(), step('provider', { status: 'done' }, { credential: 'environment' }));
    const view = renderAt('/setup');
    await heading(/^Welcome, /);
    expect(screen.getByText(/Already configured/)).toHaveTextContent('Already configured: Model.');
    view.unmount();

    // A key pasted into this wizard — a minute ago, after a reload, or by
    // somebody else — is not, whatever this tab happened to read first.
    useSetupStore.getState().reset();
    current = withStep(makeState(), step('provider', { status: 'done' }, { credential: 'api_key', anthropic_api_key: on('ab12') }));
    renderAt('/setup');
    await heading(/^Welcome, /);
    expect(screen.getByText(/3 of 9 steps done/)).toBeInTheDocument();
    expect(screen.queryByText(/Already configured/)).not.toBeInTheDocument();
  });

  it('the Account row points at Accounts instead of a Change that changes nothing', async () => {
    const view = renderAt('/setup');
    await heading(/^Welcome, /);
    // The claim is the only done, writable step here, and it gets no Change.
    expect(screen.queryByRole('link', { name: 'Change' })).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Accounts' })).toHaveAttribute('href', '/accounts');
    view.unmount();

    renderAt('/setup/review');
    await heading('Review and finish');
    const checklist = screen.getByRole('region', { name: 'Checklist' });
    expect(within(checklist).queryByRole('link', { name: 'Change' })).not.toBeInTheDocument();
    expect(within(checklist).getByRole('link', { name: 'Accounts' })).toHaveAttribute('href', '/accounts');
  });
});

describe('editing a step from the Overview or the Review', () => {
  const profileDone = () => withStep(makeState(), step('profile', { status: 'done' }, {
    timezone: 'Europe/Berlin', timezone_is_default: false,
    quiet_start: '23:00', quiet_end: '06:30', quiet_is_default: false, language: 'English',
  }));

  it('"Change" on the Overview opens the step to edit it: the primary reads Save and returns to the Overview', async () => {
    current = profileDone();
    renderAt('/setup');
    await heading(/^Welcome, /);
    await userEvent.click(screen.getByRole('link', { name: 'Change' }));
    await heading('About you');
    expect(screen.queryByRole('button', { name: /^Continue/ })).not.toBeInTheDocument();
    const language = screen.getByLabelText('Response language');
    await userEvent.clear(language);
    await userEvent.type(language, 'Deutsch');
    await userEvent.click(screen.getByRole('button', { name: /^Save/ }));
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('profile', { language: 'Deutsch' }));
    // Back where it was opened from — not on /setup/memory.
    expect(await heading(/^Welcome, /)).toBeInTheDocument();
    expect(screen.queryByRole('heading', { level: 1, name: 'Memory' })).not.toBeInTheDocument();
  });

  it('Back from a step opened with "Change" also returns to where it was opened from', async () => {
    current = profileDone();
    renderAt('/setup');
    await heading(/^Welcome, /);
    await userEvent.click(screen.getByRole('link', { name: 'Change' }));
    await heading('About you');
    await userEvent.click(screen.getByRole('button', { name: 'Back' }));
    expect(await heading(/^Welcome, /)).toBeInTheDocument();
  });

  it('"Change" on the Review returns to the Review', async () => {
    current = profileDone();
    renderAt('/setup/review');
    await heading('Review and finish');
    const checklist = screen.getByRole('region', { name: 'Checklist' });
    await userEvent.click(within(checklist).getByRole('link', { name: 'Change' }));
    await heading('About you');
    await userEvent.click(screen.getByRole('button', { name: /^Save/ }));   // nothing changed: no request
    expect(await heading('Review and finish')).toBeInTheDocument();
    expect(api.setupStep).not.toHaveBeenCalled();
  });

  it('the forward flow is unchanged: a done step reached directly still continues to the next one', async () => {
    current = profileDone();
    renderAt('/setup/profile');
    await heading('About you');
    expect(screen.getByRole('button', { name: /^Continue/ })).toBeInTheDocument();
    await userEvent.click(continueButton());
    expect(await heading('Memory')).toBeInTheDocument();
  });

  it('Back from Mode lands on the Overview, not on the read-only Account card', async () => {
    renderAt('/setup/mode');
    await heading('What should Nerve be for you?');
    await userEvent.click(screen.getByRole('button', { name: 'Back' }));
    expect(await heading(/^Welcome, /)).toBeInTheDocument();
    expect(screen.queryByRole('heading', { level: 1, name: 'Account' })).not.toBeInTheDocument();
  });
});

describe('navigation semantics', () => {
  it('a blocked step shows its reason, reads Next and writes nothing', async () => {
    current = withStep(makeState(), step('sources', {
      status: 'blocked', blocked_reason: 'Choose a mode first — the files this step fills in depend on it', read_only: true,
    }));
    renderAt('/setup/sources');
    await heading('What should Nerve read?');
    expect(screen.getAllByRole('status').some((el) => /Choose a mode first/.test(el.textContent ?? ''))).toBe(true);
    const next = screen.getByRole('button', { name: /Next/ });
    await userEvent.click(next);
    expect(api.setupStep).not.toHaveBeenCalled();
    expect(await heading('Scheduled work')).toBeInTheDocument();
  });

  it('Skip for now records the skip and moves on; Put back undoes it', async () => {
    const skipped = withStep(makeState(), step('memory', { status: 'skipped' }));
    m(api.setupSkip).mockImplementation(async (_id: string, on: boolean) => (on ? skipped : makeState()));
    renderAt('/setup/memory');
    await heading('Memory');
    await userEvent.click(screen.getByRole('button', { name: 'Skip for now' }));
    expect(api.setupSkip).toHaveBeenCalledWith('memory', true);
    expect(await heading('Talk to Nerve where you already are')).toBeInTheDocument();

    current = skipped;
    const nav = screen.getByRole('navigation', { name: 'Setup steps' });
    await userEvent.click(within(nav).getByRole('link', { name: /Memory/ }));
    await heading('Memory');
    await userEvent.click(await screen.findByRole('button', { name: 'Put back' }));
    expect(api.setupSkip).toHaveBeenCalledWith('memory', false);
  });

  it('a required step cannot be skipped', async () => {
    renderAt('/setup/provider');
    await heading('Connect a model');
    expect(screen.queryByRole('button', { name: 'Skip for now' })).not.toBeInTheDocument();
  });

  it('Back on a dirty form asks before leaving', async () => {
    renderAt('/setup/profile');
    await heading('About you');
    await userEvent.type(screen.getByLabelText('Your name'), ' B');
    await userEvent.click(screen.getByRole('button', { name: 'Back' }));
    const dialog = screen.getByRole('alertdialog', { name: 'Leave without saving?' });
    // Focus moves into the dialog, onto the safe answer.
    const keep = within(dialog).getByRole('button', { name: 'Keep editing' });
    await waitFor(() => expect(keep).toHaveFocus());
    await userEvent.click(keep);
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 1, name: 'About you' })).toBeInTheDocument();
    // ... and back to the Back button that opened it.
    expect(screen.getByRole('button', { name: 'Back' })).toHaveFocus();
  });

  it('Escape in the leave dialog means Keep editing', async () => {
    renderAt('/setup/profile');
    await heading('About you');
    await userEvent.type(screen.getByLabelText('Your name'), ' B');
    await userEvent.click(screen.getByRole('button', { name: 'Back' }));
    const dialog = screen.getByRole('alertdialog', { name: 'Leave without saving?' });
    await waitFor(() => expect(within(dialog).getByRole('button', { name: 'Keep editing' })).toHaveFocus());
    await userEvent.keyboard('{Escape}');
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument();
    expect(screen.getByLabelText('Your name')).toHaveValue('Alice B');   // the edit is kept
    expect(screen.getByRole('button', { name: 'Back' })).toHaveFocus();
  });

  it('moves focus to the heading when the step changes', async () => {
    renderAt('/setup/memory');
    await heading('Memory');
    await userEvent.click(screen.getByRole('button', { name: 'Skip for now' }));
    const next = await heading('Talk to Nerve where you already are');
    await waitFor(() => expect(next).toHaveFocus());
  });

  it('focuses the heading on the first mount too, once the state has loaded', async () => {
    // The claim lands here: the heading only exists after load() answers.
    renderAt('/setup/mode');
    const h1 = await heading('What should Nerve be for you?');
    await waitFor(() => expect(h1).toHaveFocus());
  });

  it('Enter in a single-line field submits the step', async () => {
    renderAt('/setup/profile');
    await heading('About you');
    await userEvent.type(screen.getByLabelText('Your name'), '{Enter}');
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('profile', expect.any(Object)));
  });
});

describe('forms open on server values and send the right payload', () => {
  it('accepting the prefill still saves it: a pending profile sends the browser zone and 22:00–07:00', async () => {
    renderAt('/setup/profile');
    await heading('About you');
    const zone = browserTimeZone() || 'America/New_York';
    expect(screen.getByLabelText('Time zone')).toHaveValue(zone);
    expect(screen.getByLabelText('Quiet hours start')).toHaveValue('22:00');
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledTimes(1));
    expect(api.setupStep).toHaveBeenCalledWith('profile', expect.objectContaining({
      timezone: zone, quiet_start: '22:00', quiet_end: '07:00',
    }));
  });

  it('a pending profile without a server name starts from the signed-in person\'s name; theme pills are sentence case', async () => {
    current = withStep(makeState(), step('profile', {}, { name: null }));
    renderAt('/setup/profile');
    await heading('About you');
    expect(screen.getByLabelText('Your name')).toHaveValue('Alice');
    expect(screen.getByRole('radio', { name: 'System' })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'Light' })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'Dark' })).toBeInTheDocument();
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('profile', expect.objectContaining({ name: 'Alice' })));
  });

  it('external mode never prefills a short-id label as the name', async () => {
    useHostedStore.setState({ mode: 'external' });
    useAuthStore.setState({
      viewer: { id: '11111111-2222-4333-8444-555555555555', kind: 'human', display_name: null, username: null },
      account: null,
    });
    current = withStep(makeExternalState(), step('profile', {}, { name: null, name_source: 'actor_header', display_name_editable: false }));
    renderAt('/setup/profile');
    await heading('About you');
    expect(screen.getByLabelText('Your name')).toHaveValue('');
  });

  it('a done profile sends only what changed', async () => {
    current = withStep(makeState(), step('profile', { status: 'done' }, {
      timezone: 'Europe/Berlin', timezone_is_default: false,
      quiet_start: '23:00', quiet_end: '06:30', quiet_is_default: false, language: 'English',
    }));
    renderAt('/setup/profile');
    await heading('About you');
    expect(screen.getByLabelText('Time zone')).toHaveValue('Europe/Berlin');
    const language = screen.getByLabelText('Response language');
    await userEvent.clear(language);
    await userEvent.type(language, 'Deutsch');
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('profile', { language: 'Deutsch' }));
  });

  it('a done profile with nothing changed sends no request and just advances', async () => {
    current = withStep(makeState(), step('profile', { status: 'done' }, {
      timezone: 'Europe/Berlin', timezone_is_default: false, quiet_is_default: false,
    }));
    renderAt('/setup/profile');
    await heading('About you');
    await userEvent.click(continueButton());
    expect(await heading('Memory')).toBeInTheDocument();
    expect(api.setupStep).not.toHaveBeenCalled();
  });

  it('automation: a pending step opens on what the file enables, flags an idle job, and always sends the full set', async () => {
    renderAt('/setup/automation');
    await heading('Scheduled work');
    // The file enables the inbox processor; no source is connected yet, so it
    // stays on (Continue must not silently switch it off) and wears "idle".
    expect(screen.getByRole('checkbox', { name: /Inbox processor/ })).toBeChecked();
    expect(screen.getByText('idle')).toBeInTheDocument();
    expect(screen.getByText(/stays idle until a source is connected \(Sources step\)/)).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: /Task planner/ })).not.toBeChecked();
    expect(screen.getByText(/every 30 minutes \(America\/New_York\)/)).toBeInTheDocument();
    await userEvent.click(screen.getByRole('checkbox', { name: /Task planner/ }));
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('automation', { crons: ['inbox-processor', 'task-planner'] }));
  });

  it('a save refused with a field error moves focus to that field and announces the text', async () => {
    m(api.setupStep).mockRejectedValue(new ApiError(400, '{"detail": "Not a time zone name. Use an IANA name such as Europe/Berlin.", "field": "timezone"}'));
    renderAt('/setup/profile');
    await heading('About you');
    const zone = screen.getByLabelText('Time zone');
    await userEvent.clear(zone);
    await userEvent.type(zone, 'Mars/Olympus');
    await userEvent.click(continueButton());
    await waitFor(() => expect(zone).toHaveAttribute('aria-invalid', 'true'));
    await waitFor(() => expect(zone).toHaveFocus());
    expect(zone).toHaveAccessibleDescription(/Not a time zone name/);
    expect(screen.getByRole('alert')).toHaveTextContent('Not a time zone name');
    // Nothing was `disabled` while saving, so focus was never dropped on <body>.
    expect(continueButton()).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Back' })).toBeEnabled();
  });

  it('choice cards are one tab stop, and the arrow keys move the choice and the focus', async () => {
    current = withStep(makeState({ mode: null }), step('mode', {}, { mode: null, templates_present: [] }));
    renderAt('/setup/mode');
    await heading('What should Nerve be for you?');
    const personal = screen.getByRole('radio', { name: /Personal assistant/ });
    const worker = screen.getByRole('radio', { name: /Worker/ });
    expect(personal).toHaveAttribute('tabindex', '0');
    expect(worker).toHaveAttribute('tabindex', '-1');
    personal.focus();
    await userEvent.keyboard('{ArrowDown}');
    expect(worker).toBeChecked();
    expect(worker).toHaveFocus();
    expect(worker).toHaveAttribute('tabindex', '0');
    expect(personal).toHaveAttribute('tabindex', '-1');
    await userEvent.keyboard('{ArrowUp}');
    expect(personal).toBeChecked();
    expect(personal).toHaveFocus();
    await userEvent.keyboard('{End}');
    expect(worker).toBeChecked();
  });

  it('a worker\'s Mode step says what the onboarding is doing in words, never the session id', async () => {
    const worker = (onboarding: 'pending' | 'ended', id: string | null) => makeState({
      mode: 'worker',
      steps: [
        step('claim', { status: 'done' }),
        step('mode', { status: 'done', read_only: true, read_only_reason: MODE_LOCK_REASON }, {
          mode: 'worker', task_present: true, onboarding, onboarding_session_id: id,
        }),
        step('provider', { status: 'done' }), step('profile', { title: 'Operator' }),
      ],
    });
    current = worker('ended', 'sess-1');
    const view = renderAt('/setup/mode');
    await heading('What should Nerve be for you?');
    const line = screen.getByText(/ended without writing a plan/);
    expect(line).not.toHaveTextContent('sess-1');
    expect(line).not.toHaveTextContent(/Onboarding:/);
    expect(within(line).getByRole('link', { name: 'open it' })).toHaveAttribute('href', '/chat/sess-1');
    view.unmount();

    useSetupStore.getState().reset();
    current = worker('pending', null);
    renderAt('/setup/mode');
    await heading('What should Nerve be for you?');
    expect(await screen.findByText('The onboarding session starts after setup finishes.')).toBeInTheDocument();
  });

  it('automation: a done step opens on the file exactly', async () => {
    current = withStep(makeState(), step('automation', { status: 'done' }));
    renderAt('/setup/automation');
    await heading('Scheduled work');
    expect(screen.getByRole('checkbox', { name: /Inbox processor/ })).toBeChecked();
  });

  it('sources: "None of these" is a decision with all three booleans', async () => {
    renderAt('/setup/sources');
    await heading('What should Nerve read?');
    expect(await screen.findByText(/Run gh auth login/)).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'None of these' }));
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('sources', { github: false, gmail: false, telegram: false }));
  });

  it('sources: enabling a card whose tool failed preflight warns that it stays idle', async () => {
    renderAt('/setup/sources');
    await heading('What should Nerve read?');
    await screen.findByText(/Run gh auth login/);
    await userEvent.click(screen.getByRole('checkbox', { name: /GitHub/ }));
    expect(screen.getByText(/stays idle until the tool is set up/)).toBeInTheDocument();
  });

  it('agents: nothing is pre-checked, the footer line says what Continue writes, and [] is a decision', async () => {
    renderAt('/setup/agents');
    await heading('Other coding agents');
    expect(await screen.findByText(/codex-cli 0\.40\.0/)).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: /Codex/ })).not.toBeChecked();
    expect(screen.getByRole('checkbox', { name: /Claude Code/ })).not.toBeChecked();
    expect(screen.getByText(/not on PATH/)).toBeInTheDocument();
    expect(screen.getByText(/Nothing is written on this server/)).toBeInTheDocument();
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('agents', { agents: [], conflict_policy: 'backup' }));
  });

  it('agents: an existing config file gets the policy under its own row and the write line names it', async () => {
    m(api.setupAgentsDetect).mockResolvedValue({ agents: [
      { id: 'codex', label: 'Codex', installed: true, version: 'codex-cli 0.40.0', config_paths: ['~/.codex/config.toml'], config_exists: true, enabled: false },
      { id: 'claude-code', label: 'Claude Code', installed: true, version: '2.1.0', config_paths: ['~/.claude/settings.json'], config_exists: false, enabled: false },
    ] });
    renderAt('/setup/agents');
    await heading('Other coding agents');
    await screen.findByText(/codex-cli 0\.40\.0/);
    expect(screen.queryByRole('radiogroup', { name: 'Conflict policy' })).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole('checkbox', { name: /Claude Code/ }));
    expect(screen.getByText(/Will write ~\/\.claude\/settings\.json \(new file\)\./)).toBeInTheDocument();
    expect(screen.queryByRole('radiogroup', { name: 'Conflict policy' })).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole('checkbox', { name: /Codex/ }));
    const row = screen.getByRole('checkbox', { name: /Codex/ }).closest('li')!;
    expect(within(row).getByRole('radiogroup', { name: 'Conflict policy' })).toBeInTheDocument();
    expect(screen.getByText(/Will write ~\/\.codex\/config\.toml; a backup of the existing file is kept\./)).toBeInTheDocument();
    await userEvent.click(within(row).getByRole('radio', { name: /Merge/ }));
    expect(screen.getByText(/the Nerve entry is added and the rest is left alone/)).toBeInTheDocument();
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('agents', { agents: ['claude-code', 'codex'], conflict_policy: 'merge' }));
  });

  it('memory: continuing without embeddings is a decision; a key is sent when chosen', async () => {
    renderAt('/setup/memory');
    await heading('Memory');
    expect(screen.getByRole('radio', { name: /Continue without/ })).toBeChecked();
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('memory', { openai_api_key: null, embeddings: false }));

    m(api.setupStep).mockClear();
    const nav = screen.getByRole('navigation', { name: 'Setup steps' });
    await userEvent.click(within(nav).getByRole('link', { name: /Memory/ }));
    await heading('Memory');
    await userEvent.click(screen.getByRole('radio', { name: /Use embeddings/ }));
    await userEvent.type(screen.getByLabelText('OpenAI API key'), 'sk-REPLACE');
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('memory', { openai_api_key: 'sk-REPLACE' }));
  });

  it('mode is read-only once chosen, with the reason', async () => {
    renderAt('/setup/mode');
    await heading('What should Nerve be for you?');
    expect(screen.getByRole('radio', { name: /Personal assistant/ })).toBeDisabled();
    expect(screen.getByRole('radio', { name: /Personal assistant/ })).toBeChecked();
    expect(screen.queryByText('Recommended')).not.toBeInTheDocument();
    expect(screen.getByText(MODE_LOCK_REASON)).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: /Next/ }));
    expect(api.setupStep).not.toHaveBeenCalled();
  });

  it('mode: Personal assistant is preselected and tagged, so Continue is live on arrival', async () => {
    current = withStep(makeState({ mode: null }), step('mode', {}, { mode: null, templates_present: [] }));
    renderAt('/setup/mode');
    await heading('What should Nerve be for you?');
    expect(screen.getByText(/Chosen once\./)).toBeInTheDocument();
    const personal = screen.getByRole('radio', { name: /Personal assistant/ });
    expect(personal).toBeChecked();
    expect(within(personal).getByText('Recommended')).toBeInTheDocument();
    expect(continueButton()).toBeEnabled();
    // The preselection is the opening value, not an edit: Back does not ask.
    await userEvent.click(screen.getByRole('button', { name: 'Back' }));
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument();
    await waitFor(() => expect(screen.queryByRole('heading', { level: 1, name: 'What should Nerve be for you?' })).not.toBeInTheDocument());
  });

  it('a worker needs a task description; a starter template fills one in', async () => {
    current = withStep(makeState({ mode: null }), step('mode', {}, { mode: null, templates_present: [] }));
    renderAt('/setup/mode');
    await heading('What should Nerve be for you?');
    await userEvent.click(screen.getByRole('radio', { name: /Worker/ }));
    await userEvent.type(screen.getByLabelText(/What should this worker do/), 'x');
    await userEvent.click(continueButton());
    expect(screen.getByLabelText(/What should this worker do/)).toHaveAccessibleDescription(/at least one full sentence/);
    expect(api.setupStep).not.toHaveBeenCalled();

    await userEvent.click(screen.getByRole('button', { name: 'Monitor a build' }));
    expect(screen.getByText(/On first boot the worker reads this description/)).toBeInTheDocument();
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('mode', {
      mode: 'worker', task_description: expect.stringContaining('nightly build'),
    }));
  });
});

describe('provider', () => {
  it('offers the host login card only when the server found one', async () => {
    renderAt('/setup/provider');
    await heading('Connect a model');
    await waitFor(() => expect(api.setupHostLogin).toHaveBeenCalled());
    expect(screen.queryByRole('radio', { name: /Claude Code login/ })).not.toBeInTheDocument();
    expect(screen.getByText('No Claude Code login was found on this server.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Look again' })).toBeInTheDocument();

    m(api.setupHostLogin).mockResolvedValue({ available: true, kind: 'oauth', source: 'macOS Keychain (Claude Code)' });
    await userEvent.click(screen.getByRole('button', { name: 'Look again' }));
    expect(api.setupHostLogin).toHaveBeenLastCalledWith(true);
    expect(await screen.findByRole('radio', { name: /Claude Code login/ })).toHaveTextContent('macOS Keychain');
    expect(screen.getAllByRole('status').some((el) => /A Claude Code login was found \(macOS Keychain/.test(el.textContent ?? ''))).toBe(true);
    expect(screen.queryByText('No Claude Code login was found on this server.')).not.toBeInTheDocument();
  });

  it('a pending step stays after the first save and shows the picker with what the server applied', async () => {
    const saved = withStep(makeState(), step('provider', { status: 'done', etag: 'provider-etag-2', summary: 'Anthropic API key (…ab12) · claude-opus-5 · effort max' }, {
      credential: 'api_key', anthropic_api_key: on('ab12'), model: 'claude-opus-5', effort: 'max',
    }));
    m(api.setupStep).mockImplementation(async () => { current = saved; return saved; });
    renderAt('/setup/provider');
    await heading('Connect a model');
    expect(screen.queryByLabelText('Model')).not.toBeInTheDocument();   // nothing to pick before a credential
    await userEvent.type(screen.getByLabelText('Anthropic API key'), 'sk-ant-REPLACE');
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('provider', { type: 'anthropic', anthropic_api_key: 'sk-ant-REPLACE' }));

    // Still here, saved, and the defaults are on screen rather than silent.
    expect(screen.getByRole('heading', { level: 1, name: 'Connect a model' })).toBeInTheDocument();
    expect(await screen.findByText(/Saved\. Pick the default model and effort/)).toBeInTheDocument();
    expect(await screen.findByLabelText('Model')).toHaveValue('claude-opus-5');
    expect(screen.getByRole('radio', { name: 'max' })).toBeChecked();
    expect(screen.getByText(/leave blank to keep/)).toHaveTextContent('…ab12');

    // Change the effort: the next Continue sends just that, then advances.
    await userEvent.click(screen.getByRole('radio', { name: 'high' }));
    m(api.setupStep).mockClear();
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('provider', { effort: 'high' }));
    expect(await heading('About you')).toBeInTheDocument();
    expect(screen.queryByText(/Saved\. Pick the default model/)).not.toBeInTheDocument();
  });

  it('tests live, shows the model picker after success and saves type, key and model', async () => {
    renderAt('/setup/provider');
    await heading('Connect a model');
    await userEvent.type(screen.getByLabelText('Anthropic API key'), 'sk-ant-REPLACE');
    await userEvent.click(screen.getByRole('button', { name: 'Test connection' }));
    expect(api.setupProviderTest).toHaveBeenCalledWith({ type: 'anthropic', anthropic_api_key: 'sk-ant-REPLACE' });
    expect(await screen.findByText('claude-haiku answered')).toBeInTheDocument();
    await waitFor(() => expect(api.setupModels).toHaveBeenCalled());
    const model = await screen.findByLabelText('Model');
    await userEvent.selectOptions(model, 'claude-opus-5');
    expect(screen.getByText(/Most capable/)).toBeInTheDocument();
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('provider', {
      type: 'anthropic', anthropic_api_key: 'sk-ant-REPLACE', model: 'claude-opus-5',
    }));
  });

  it('reports a precise failure from the server-side test', async () => {
    m(api.setupProviderTest).mockResolvedValue({
      ok: false, checked: 'api', code: 'invalid_key',
      detail: 'Anthropic rejected this key (401 authentication_error).',
      hint: 'Keys start with sk-ant-. Check for a missing last character or a revoked key.', latency_ms: 640,
    });
    renderAt('/setup/provider');
    await heading('Connect a model');
    await userEvent.type(screen.getByLabelText('Anthropic API key'), 'sk-ant-REPLACE');
    await userEvent.click(screen.getByRole('button', { name: 'Test connection' }));
    expect(await screen.findByText(/Anthropic rejected this key/)).toBeInTheDocument();
    expect(screen.getByText(/missing last character/)).toBeInTheDocument();
    expect(screen.queryByLabelText('Model')).not.toBeInTheDocument();
  });

  it('"Save anyway" after a key that does not look right', async () => {
    renderAt('/setup/provider');
    await heading('Connect a model');
    await userEvent.type(screen.getByLabelText('Anthropic API key'), 'not-a-key');
    await userEvent.click(continueButton());
    expect(screen.getByText(/does not look like an Anthropic API key/)).toBeInTheDocument();
    expect(api.setupStep).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole('button', { name: /Save anyway/ }));
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('provider', { type: 'anthropic', anthropic_api_key: 'not-a-key' }));
  });

  it('renders a configured secret as its last four, never a value, and Replace opens an empty field', async () => {
    current = withStep(makeState(), step('provider', { status: 'done', summary: 'Anthropic API key (…ab12)' }, {
      credential: 'api_key', anthropic_api_key: on('ab12'),
    }));
    renderAt('/setup/provider');
    await heading('Connect a model');
    expect(screen.getByText(/leave blank to keep/)).toHaveTextContent('…ab12');
    expect(screen.queryByLabelText('Anthropic API key', { selector: 'input' })).not.toBeInTheDocument();
    expect(document.body.textContent).not.toContain('sk-ant');
    expect(screen.getByRole('link', { name: 'Finish now' })).toHaveAttribute('href', '/setup/review');
    await userEvent.click(screen.getByRole('button', { name: 'Replace' }));
    const input = screen.getByLabelText('Anthropic API key', { selector: 'input' });
    expect(input).toHaveValue('');
    expect(input).toHaveAttribute('type', 'password');
  });

  it('a credential from the environment is read-only; the model picker is not', async () => {
    current = withStep(makeState(), step('provider', { status: 'done' }, { credential: 'environment' }));
    renderAt('/setup/provider');
    await heading('Connect a model');
    expect(screen.getByText(/Configured from the environment/)).toBeInTheDocument();
    // No way to type or replace a key: the cards, the key field, Replace,
    // Remove and Test are all absent, so a PUT can never carry a credential.
    expect(screen.queryByRole('radiogroup', { name: 'Model provider' })).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/API key|subscription token/)).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /^(Replace|Remove|Test connection)$/ })).not.toBeInTheDocument();
    // The model and effort are still this instance's to choose.
    expect(await screen.findByLabelText('Model')).toBeEnabled();
    await userEvent.click(screen.getByRole('radio', { name: 'medium' }));
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('provider', { effort: 'medium' }));
  });

  it('after a failed live test, Continue warns and relabels before it saves the key unverified', async () => {
    m(api.setupProviderTest).mockResolvedValue({
      ok: false, checked: 'api', code: 'invalid_key',
      detail: 'Anthropic rejected this key (401 authentication_error).', hint: null, latency_ms: 640,
    });
    renderAt('/setup/provider');
    await heading('Connect a model');
    await userEvent.type(screen.getByLabelText('Anthropic API key'), 'sk-ant-REPLACE');
    await userEvent.click(screen.getByRole('button', { name: 'Test connection' }));
    await screen.findByText(/Anthropic rejected this key/);

    await userEvent.click(continueButton());
    expect(api.setupStep).not.toHaveBeenCalled();
    expect(screen.getByText(/would be saved unverified/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Save anyway/ })).toBeInTheDocument();

    // Editing the key withdraws the warning (it was about the old value) …
    await userEvent.type(screen.getByLabelText('Anthropic API key'), 'X');
    expect(screen.queryByText(/would be saved unverified/)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^Continue/ })).toBeInTheDocument();

    // … and a key that failed again needs the second press to be saved.
    await userEvent.click(screen.getByRole('button', { name: 'Test connection' }));
    await waitFor(() => expect(api.setupProviderTest).toHaveBeenCalledTimes(2));
    await userEvent.click(continueButton());
    expect(api.setupStep).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole('button', { name: /Save anyway/ }));
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('provider', { type: 'anthropic', anthropic_api_key: 'sk-ant-REPLACEX' }));
  });

  it('says the login could not be read — not that there is none — when the server could not consult the Keychain', async () => {
    m(api.setupHostLogin).mockResolvedValue({
      available: false, kind: null, source: null, lookup: 'unreadable',
      reason: 'The Keychain is not readable from a background process. On the server, nerve init --terminal runs the same detection interactively; or paste an API key here.',
    });
    renderAt('/setup/provider');
    await heading('Connect a model');
    expect(await screen.findByText('No Claude Code login could be read on this server.')).toBeInTheDocument();
    expect(screen.getByText(/nerve init --terminal runs the same detection/)).toBeInTheDocument();
    expect(screen.queryByText(/was found on this server/)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Look again' })).toBeInTheDocument();
  });

  it('a done step opens compact: the cards wait behind Replace, Remove follows Test, effort is one tab stop', async () => {
    current = withStep(makeState(), step('provider', { status: 'done' }, {
      credential: 'api_key', anthropic_api_key: on('ab12'), model: 'claude-sonnet-4-6', effort: 'high',
    }));
    renderAt('/setup/provider');
    await heading('Connect a model');
    expect(screen.queryByRole('radiogroup', { name: 'Model provider' })).not.toBeInTheDocument();
    expect(screen.queryByText(/No Claude Code login/)).not.toBeInTheDocument();
    expect(screen.getByText(/leave blank to keep/)).toHaveTextContent('…ab12');
    const test = screen.getByRole('button', { name: 'Test connection' });
    const remove = screen.getByRole('button', { name: 'Remove' });
    expect(test.compareDocumentPosition(remove) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    await screen.findByLabelText('Model');
    const pills = screen.getAllByRole('radio', { name: /^(max|xhigh|high|medium|low)$/ });
    expect(pills.filter((p) => p.getAttribute('tabindex') === '0')).toHaveLength(1);
    const high = screen.getByRole('radio', { name: 'high' });
    expect(high).toHaveAttribute('tabindex', '0');
    high.focus();
    await userEvent.keyboard('{ArrowRight}');
    expect(screen.getByRole('radio', { name: 'medium' })).toBeChecked();
    expect(screen.getByRole('radio', { name: 'medium' })).toHaveFocus();

    // Replace reveals the chooser, with the key field open and empty.
    await userEvent.click(screen.getByRole('button', { name: 'Replace' }));
    expect(screen.getByRole('radiogroup', { name: 'Model provider' })).toBeInTheDocument();
    expect(screen.getByLabelText('Anthropic API key', { selector: 'input' })).toHaveValue('');
  });

  it('Remove on a done step is confirmed by the note and sent as null on Continue', async () => {
    current = withStep(makeState(), step('provider', { status: 'done' }, {
      credential: 'api_key', anthropic_api_key: on('ab12'),
    }));
    renderAt('/setup/provider');
    await heading('Connect a model');
    await userEvent.click(screen.getByRole('button', { name: 'Remove' }));
    expect(screen.getByText('Will be removed when you continue.')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Remove' })).not.toBeInTheDocument();
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('provider', { type: 'anthropic', anthropic_api_key: null }));
  });

  it('warns when the only credential is a subscription token', async () => {
    current = withStep(makeState(), step('provider', { status: 'done' }, {
      type: 'oauth', credential: 'oauth_token', claude_oauth_token: on('9z8y'), warning: 'oauth_background_jobs',
    }));
    renderAt('/setup/provider');
    await heading('Connect a model');
    expect(screen.getByText(/Chat works; memory, titles and scheduled jobs need an API key/)).toBeInTheDocument();
  });
});

describe('channels', () => {
  const configured = (extra: Record<string, unknown> = {}) => withStep(makeState(), step('channels', { status: 'done' }, {
    telegram: {
      enabled: true, bot_token: on('Xw9z'), bot_username: 'example_assistant_bot', live: false, start_error: null,
      dm_policy: 'pairing', allowed_users_count: 0, pairing_available: false,
      pairing_unavailable_reason: 'The bot starts with this token after the restart at the end of setup.',
      ...extra,
    },
    slack: CHANNELS.slack,
  }));
  /** The Telegram card; both cards have a "Bot token" field. */
  const telegramCard = () => within(screen.getByRole('region', { name: 'Telegram' }));

  it('shows the unavailable notice and takes a manual user id', async () => {
    current = configured();
    renderAt('/setup/channels');
    await heading('Talk to Nerve where you already are');
    expect(screen.getByText(/after the restart at the end of setup/)).toBeInTheDocument();
    expect(screen.getByText(/@example_assistant_bot/)).toBeInTheDocument();
    await userEvent.type(screen.getByLabelText(/numeric Telegram user id/), '123456789');
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('channels', { telegram: { allowed_users_add: [123456789] } }));
  });

  it('offers in-wizard pairing when the bot is live', async () => {
    current = configured({ live: true, pairing_available: true, pairing_unavailable_reason: null });
    renderAt('/setup/channels');
    await heading('Talk to Nerve where you already are');
    await userEvent.click(await screen.findByRole('button', { name: 'Get pairing code' }));
    expect(await screen.findByLabelText('Pairing code')).toHaveTextContent('483920');
    expect(screen.getByRole('link', { name: /Open @example_assistant_bot in Telegram/ }))
      .toHaveAttribute('href', 'https://t.me/example_assistant_bot');
    expect(screen.getByText(/Waiting for your message/)).toBeInTheDocument();
  });

  it('shows a start error from the last boot above the token', async () => {
    current = configured({ start_error: 'InvalidToken: The token <token> was rejected by the server.' });
    renderAt('/setup/channels');
    await heading('Talk to Nerve where you already are');
    expect(screen.getByText(/could not start with the saved token/)).toHaveTextContent('Replace the token and restart.');
  });

  it('lands a 400 with field bot_token under the token field', async () => {
    m(api.setupStep).mockRejectedValue(new ApiError(400, '{"detail": "Telegram rejected this token: unauthorized", "field": "bot_token"}'));
    renderAt('/setup/channels');
    await heading('Talk to Nerve where you already are');
    const token = telegramCard().getByLabelText('Bot token');
    await userEvent.type(token, `123456:${'A'.repeat(35)}`);
    await userEvent.click(continueButton());
    await waitFor(() => expect(token).toHaveAttribute('aria-invalid', 'true'));
    expect(token).toHaveAccessibleDescription(/Telegram rejected this token/);
    expect(screen.getByRole('heading', { level: 1, name: 'Talk to Nerve where you already are' })).toBeInTheDocument();
  });

  it('refuses a malformed token before asking the server', async () => {
    renderAt('/setup/channels');
    await heading('Talk to Nerve where you already are');
    await userEvent.type(telegramCard().getByLabelText('Bot token'), 'nope');
    await userEvent.click(continueButton());
    expect(telegramCard().getByLabelText('Bot token')).toHaveAccessibleDescription(/does not look like a bot token/);
    expect(api.setupStep).not.toHaveBeenCalled();
  });

  it('tests a token and names the bot', async () => {
    m(api.setupTelegramTest).mockResolvedValue({ ok: true, bot_username: 'example_assistant_bot', detail: null });
    renderAt('/setup/channels');
    await heading('Talk to Nerve where you already are');
    await userEvent.type(telegramCard().getByLabelText('Bot token'), `123456:${'A'.repeat(35)}`);
    await userEvent.click(screen.getByRole('button', { name: 'Test token' }));
    expect(await screen.findByText(/Token works: @example_assistant_bot/)).toBeInTheDocument();
  });

  it('Continue with nothing entered is the "no channels" decision: it skips and moves on', async () => {
    const skipped = withStep(makeState(), step('channels', { status: 'skipped' }));
    m(api.setupSkip).mockImplementation(async () => { current = skipped; return skipped; });
    renderAt('/setup/channels');
    await heading('Talk to Nerve where you already are');
    expect(screen.queryByRole('button', { name: 'Skip for now' })).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: /No channels for now/ }));
    expect(api.setupSkip).toHaveBeenCalledWith('channels', true);
    expect(api.setupStep).not.toHaveBeenCalled();
    expect(await heading('What should Nerve read?')).toBeInTheDocument();
  });

  it('typing a token turns the primary back into Continue and brings Skip back', async () => {
    renderAt('/setup/channels');
    await heading('Talk to Nerve where you already are');
    await userEvent.type(telegramCard().getByLabelText('Bot token'), '123');
    expect(screen.getByRole('button', { name: /^Continue/ })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Skip for now' })).toBeInTheDocument();
  });

  it('keeps Slack behind a disclosure and lands a Slack rejection under the Slack token, not Telegram', async () => {
    renderAt('/setup/channels');
    await heading('Talk to Nerve where you already are');
    expect(screen.queryByRole('region', { name: 'Slack' })).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: /Also connect Slack \(advanced\)/ }));
    const slackCard = within(screen.getByRole('region', { name: 'Slack' }));
    await userEvent.type(slackCard.getByLabelText('Bot token'), 'xoxb-REPLACE');
    await userEvent.type(slackCard.getByLabelText('App-level token'), 'xapp-REPLACE');
    // The server names Slack's field `bot_token` too.
    m(api.setupStep).mockRejectedValue(new ApiError(400, '{"detail": "Slack rejected the bot token (invalid_auth).", "field": "bot_token"}'));
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('channels', {
      slack: { bot_token: 'xoxb-REPLACE', app_token: 'xapp-REPLACE', enabled: true },
    }));
    const slackToken = slackCard.getByLabelText('Bot token');
    await waitFor(() => expect(slackToken).toHaveAttribute('aria-invalid', 'true'));
    expect(slackToken).toHaveAccessibleDescription(/Slack rejected the bot token/);
    expect(telegramCard().getByLabelText('Bot token')).not.toHaveAttribute('aria-invalid');
  });

  it('opens Slack by itself when a Slack token is already configured', async () => {
    current = withStep(makeState(), step('channels', { status: 'done' }, {
      telegram: CHANNELS.telegram,
      slack: { ...CHANNELS.slack, enabled: true, bot_token: on('abcd'), app_token: on('efgh') },
    }));
    renderAt('/setup/channels');
    await heading('Talk to Nerve where you already are');
    expect(screen.getByRole('region', { name: 'Slack' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Also connect Slack/ })).not.toBeInTheDocument();
  });

  it('shows Slack as managed by the gateway in hosted channel mode', async () => {
    current = withStep(makeState(), step('channels', {}, {
      telegram: CHANNELS.telegram,
      slack: { enabled: true, mode: 'hosted', bot_token: on('abcd'), app_token: on('efgh'), allow_users: [], allow_direct_messages: true, allow_channels: [] },
    }));
    renderAt('/setup/channels');
    await heading('Talk to Nerve where you already are');
    expect(screen.getByText(/managed by the channel gateway/)).toBeInTheDocument();
    expect(within(screen.getByRole('region', { name: 'Slack' })).queryByLabelText('Bot token')).not.toBeInTheDocument();
  });
});

describe('remount only when persisted values change', () => {
  it('a reload on visibilitychange after a Test keeps typed input', async () => {
    renderAt('/setup/provider');
    await heading('Connect a model');
    const input = screen.getByLabelText('Anthropic API key');
    await userEvent.type(input, 'sk-ant-REPLACE-TYPED');
    const calls = m(api.setupState).mock.calls.length;

    // Same etag, new volatile detail: the form stays mounted.
    current = withStep(makeState(), step('provider', {}, { last_test: { ok: true, checked_at: '2026-10-10T09:12:03Z' } }));
    act(() => { document.dispatchEvent(new Event('visibilitychange')); });
    await waitFor(() => expect(m(api.setupState).mock.calls.length).toBeGreaterThan(calls));
    await waitFor(() => expect(useSetupStore.getState().state?.steps.find((s) => s.id === 'provider')?.detail).toMatchObject({ last_test: { ok: true } }));
    expect(screen.getByLabelText('Anthropic API key')).toHaveValue('sk-ant-REPLACE-TYPED');

    // A changed etag (somebody else saved) remounts with fresh hydration.
    current = withStep(makeState(), step('provider', { etag: 'provider-etag-2' }, { model: 'claude-opus-5' }));
    act(() => { document.dispatchEvent(new Event('visibilitychange')); });
    await waitFor(() => expect(screen.getByLabelText('Anthropic API key')).toHaveValue(''));
  });
});

describe('lockdown', () => {
  const LOCK = 'Configuration is managed from the config repository on this instance; changes arrive by PR and sync.';

  function lockedState() {
    const blocked = (id: Parameters<typeof step>[0]) => step(id, {
      status: 'blocked', read_only: true, blocked_reason: `${LOCK} Configure this step from the config repository.`,
    });
    return makeState({
      lockdown: true, writable: false, read_only_reason: LOCK, steps_left: 1,
      steps: [
        step('claim', { status: 'done' }),
        step('mode', { status: 'done', read_only: true, read_only_reason: MODE_LOCK_REASON }),
        blocked('provider'),
        step('profile', {}, { writable_fields: ['name'] }),
        blocked('memory'), blocked('channels'), blocked('sources'), blocked('automation'), blocked('agents'),
      ],
    });
  }

  it('shows Lock badges, Next on blocked steps, and a notice on the overview', async () => {
    current = lockedState();
    renderAt('/setup');
    await heading(/^Welcome, /);
    expect(screen.getAllByRole('status').some((el) => el.textContent?.includes(LOCK))).toBe(true);
    const nav = screen.getByRole('navigation', { name: 'Setup steps' });
    expect(within(nav).getAllByText('Blocked')).toHaveLength(6);

    await userEvent.click(within(nav).getByRole('link', { name: /Model/ }));
    await heading('Connect a model');
    expect(screen.getByRole('button', { name: /Next/ })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Continue/ })).not.toBeInTheDocument();
  });

  it('local lockdown: only the name is editable on the profile and Continue sends just that', async () => {
    current = lockedState();
    renderAt('/setup/profile');
    await heading('About you');
    expect(screen.getByLabelText('Your name')).toBeEnabled();
    expect(screen.getByLabelText('Time zone')).toBeDisabled();
    await userEvent.clear(screen.getByLabelText('Your name'));
    await userEvent.type(screen.getByLabelText('Your name'), 'Alice B');
    await userEvent.click(continueButton());
    await waitFor(() => expect(api.setupStep).toHaveBeenCalledWith('profile', { name: 'Alice B' }));
  });
});

describe('review and the ending', () => {
  it('lists what waits on a restart and what the server check-up found', async () => {
    current = makeState({
      restart_pending: true,
      restart_reasons: ["timezone: 'America/New_York' → 'Europe/Berlin'", 'telegram.bot_token: changed'],
    });
    m(api.setupEnvironment).mockResolvedValue({ ...ENVIRONMENT, served: { ...ENVIRONMENT.served, https_hint: true } });
    renderAt('/setup/review');
    await heading('Review and finish');
    const reasons = screen.getByRole('list', { name: 'Restart reasons' });
    // In words first, the raw line as secondary text.
    expect(reasons).toHaveTextContent('Time zone: America/New_York → Europe/Berlin');
    expect(reasons).toHaveTextContent('Telegram bot settings');
    expect(reasons).toHaveTextContent('telegram.bot_token: changed');
    expect(screen.getByText('Some of what you changed is read at startup.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Restart Nerve' })).toBeInTheDocument();
    expect(await screen.findByText(/Install gh and run gh auth login/)).toBeInTheDocument();
    expect(screen.getByText(/served over plain HTTP/)).toBeInTheDocument();
    // The apply card is the first section: the primary action is above the fold.
    const apply = screen.getByRole('region', { name: 'Apply' });
    const checklist = screen.getByRole('region', { name: 'Checklist' });
    expect(apply.compareDocumentPosition(checklist) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it('a fresh install explains its one restart and merges the mode\'s memory line into a sentence', async () => {
    // Only the Mode write produces memory debt, so a reason list of nothing
    // else is a first run — derived from the reasons, so it holds after a
    // page reload between Mode and Review (this tab never saw Mode pending).
    current = makeState({ restart_pending: true, restart_reasons: ['memory: changed'] });
    renderAt('/setup/review');
    await heading('Review and finish');
    expect(screen.getByText('One restart finishes a fresh setup; this page reconnects on its own.')).toBeInTheDocument();
    const reasons = screen.getByRole('list', { name: 'Restart reasons' });
    expect(reasons).toHaveTextContent('The mode you chose (its memory layout is read at startup)');
    expect(reasons).not.toHaveTextContent(/Memory settings/);
    expect(reasons).toHaveTextContent('memory: changed');
    // Before the click the card speaks in the future tense; "Restarting." is
    // the overlay's sentence.
    expect(screen.getByText('Restarting stops this process and starts a new one; this page reconnects on its own.')).toBeInTheDocument();
    expect(screen.queryByText(/^Restarting\./)).not.toBeInTheDocument();
  });

  it('a page reload mid-way still shows the generic headline when more than the mode changed', async () => {
    current = makeState({ restart_pending: true, restart_reasons: ['memory: changed', 'timezone: changed'] });
    renderAt('/setup/review');
    await heading('Review and finish');
    expect(screen.getByText('Some of what you changed is read at startup.')).toBeInTheDocument();
  });

  it('lists what is still to do above the Finish button, with a one-click Skip', async () => {
    current = makeState({
      steps: makeState().steps.map((s) => (
        s.id === 'memory' || s.id === 'channels' ? s : s.status === 'pending' ? step(s.id, { status: 'done' }) : s
      )),
    });
    const afterSkip = withStep(current, step('channels', { status: 'skipped' }));
    m(api.setupSkip).mockImplementation(async () => { current = afterSkip; return afterSkip; });
    renderAt('/setup/review');
    await heading('Review and finish');
    const todo = screen.getByRole('list', { name: 'Still to do' });
    expect(todo).toHaveTextContent('Memory — still to do');
    expect(todo).toHaveTextContent('Channels — still to do');
    const channels = within(todo).getByText('Channels — still to do').closest('li')!;
    expect(within(channels).getByRole('link', { name: 'Open' })).toHaveAttribute('href', '/setup/channels');
    await userEvent.click(within(channels).getByRole('button', { name: 'Skip' }));
    expect(api.setupSkip).toHaveBeenCalledWith('channels', true);
    await waitFor(() => expect(screen.getByRole('list', { name: 'Still to do' })).not.toHaveTextContent('Channels'));
    // The Finish button follows the list in the same card.
    const apply = screen.getByRole('region', { name: 'Apply' });
    expect(within(apply).getByRole('button', { name: 'Finish' })).toBeInTheDocument();
    expect(todo.compareDocumentPosition(within(apply).getByRole('button', { name: 'Finish' })) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it('under a supervisor it shows the exact command before anyone clicks', async () => {
    current = makeState({
      restart_pending: true, restart_reasons: ['timezone: changed'], restart_mode: 'unsupported',
      restart_message: 'This instance is managed by systemd. On the server run: systemctl restart nerve.service — this page reconnects by itself.',
    });
    m(api.setupEnvironment).mockResolvedValue({
      ...ENVIRONMENT, supervision: 'systemd', restart_mode: 'unsupported',
      restart_message: 'This instance is managed by systemd. On the server run: systemctl restart nerve.service — this page reconnects by itself.',
    });
    renderAt('/setup/review');
    await heading('Review and finish');
    expect(await screen.findByText('systemctl restart nerve.service')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Copy' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Finish and wait for the restart' })).toBeInTheDocument();
  });

  it('Say hello introduces the viewer, never the shared USER.md name or a bare username', async () => {
    const readyAt = (state: SetupState) => render(
      <MemoryRouter initialEntries={['/setup/review']}>
        <Routes>
          <Route path="/setup/review" element={<ReadyScreen state={state} />} />
          <Route path="/chat" element={<ChatProbe />} />
        </Routes>
      </MemoryRouter>,
    );

    // External: Bob was saved into USER.md by somebody else; Alice is signed in.
    useHostedStore.setState({ mode: 'external' });
    useAuthStore.setState({
      viewer: { id: '11111111-2222-4333-8444-555555555555', kind: 'human', display_name: 'Alice Example', username: null },
      account: null,
    });
    let view = readyAt(withStep(makeExternalState({ finished: true, steps_left: 0 }), step('profile', { status: 'done' }, {
      name: 'Bob', name_source: 'user_md', actor_name: 'Alice Example',
    })));
    expect(await heading('Nerve is ready')).toHaveFocus();
    await userEvent.click(screen.getByRole('button', { name: /Say hello to Nerve/ }));
    expect(await screen.findByText(/chat page/)).toHaveTextContent("Hi! I'm Alice Example. Tell me");
    view.unmount();

    // Local, no display name anywhere: the clause is dropped rather than "I'm alice".
    useHostedStore.setState({ mode: 'local' });
    useAuthStore.setState({
      viewer: { id: 'actor-1', kind: 'human', display_name: null, username: 'alice' },
      account: { id: 'acc-1', username: 'alice' },
    });
    view = readyAt(withStep(makeState({ finished: true, steps_left: 0 }), step('profile', { status: 'done' }, { name: null, actor_name: null })));
    await userEvent.click(screen.getByRole('button', { name: /Say hello to Nerve/ }));
    const probe = await screen.findByText(/chat page/);
    expect(probe).toHaveTextContent('Hi! Tell me what you can do');
    expect(probe).not.toHaveTextContent(/I'm/);
    view.unmount();

    // Local with a USER.md name but no account display name: the USER.md name is the person's own.
    readyAt(withStep(makeState({ finished: true, steps_left: 0 }), step('profile', { status: 'done' }, { name: 'Alice', actor_name: null })));
    await userEvent.click(screen.getByRole('button', { name: /Say hello to Nerve/ }));
    expect(await screen.findByText(/chat page/)).toHaveTextContent("Hi! I'm Alice. Tell me");
  });

  it('Finish waits until every step is decided, and says so', async () => {
    renderAt('/setup/review');   // seven steps still pending
    await heading('Review and finish');
    const finish = screen.getByRole('button', { name: 'Finish' });
    expect(finish).toBeDisabled();
    expect(finish).toHaveAccessibleDescription('Open or skip the steps above to finish.');
    expect(screen.getByRole('list', { name: 'Still to do' })).toBeInTheDocument();
  });

  it('the Review names a provider whose last live test failed', async () => {
    current = withStep(decided(), step('provider', { status: 'done', summary: 'Anthropic API key (…ab12)' }, {
      credential: 'api_key', anthropic_api_key: on('ab12'), last_test: { ok: false, checked_at: '2026-10-10T09:12:03Z' },
    }));
    renderAt('/setup/review');
    await heading('Review and finish');
    const checklist = screen.getByRole('region', { name: 'Checklist' });
    expect(within(checklist).getByText(/last connection test failed/)).toBeInTheDocument();
  });

  it('the ready screen says when optional steps are still open instead of contradicting the banner', () => {
    render(<MemoryRouter><ReadyScreen state={makeState({ finished: false, steps_left: 2 })} /></MemoryRouter>);
    expect(screen.getByText(/2 optional steps are still open/)).toBeInTheDocument();
  });

  it('Finish without a pending restart reloads and ends on "Nerve is ready"; Say hello seeds the chat', async () => {
    current = decided();
    const finished = makeState({ finished: true, steps_left: 0 });
    m(api.setupFinish).mockResolvedValue({ state: finished, action: 'reloaded', onboarding: { status: 'not_applicable', session_id: null } });
    renderAt('/setup/review');
    await heading('Review and finish');
    expect(screen.getByText(/applies without a restart/)).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Finish' }));
    expect(await heading('Nerve is ready')).toBeInTheDocument();
    expect(api.restartSystem).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole('button', { name: /Say hello to Nerve/ }));
    expect(await screen.findByText(/chat page/)).toHaveTextContent("Hi! I'm Alice. Tell me what you can do");
  });

  it('Finish with a pending restart restarts, waits for the new generation, and ends ready', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    current = decided({ restart_pending: true, restart_reasons: ['timezone: changed'] });
    m(api.setupFinish).mockResolvedValue({ state: current, action: 'restart_required', onboarding: { status: 'not_applicable', session_id: null } });
    m(api.restartSystem).mockResolvedValue({ mode: 'restart', generation: 'G0', message: 'Restarting.', command: null, timeout_hint_seconds: 120 });
    m(api.healthGeneration).mockResolvedValueOnce('G0').mockResolvedValue('G1');
    renderAt('/setup/review');
    await heading('Review and finish');
    fireEvent.click(screen.getByRole('button', { name: 'Restart Nerve' }));
    expect(await screen.findByRole('dialog', { name: /Restarting Nerve/ })).toBeInTheDocument();
    current = makeState({ finished: true, steps_left: 0, generation: 'G1' });
    await act(async () => { await vi.advanceTimersByTimeAsync(2100); });
    expect(await heading('Nerve is ready')).toBeInTheDocument();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(api.healthGeneration).toHaveBeenCalled();
  });

  it('says so when Nerve does not come back in time', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    current = decided({ restart_pending: true, restart_reasons: ['timezone: changed'] });
    m(api.setupFinish).mockResolvedValue({ state: current, action: 'restart_required', onboarding: { status: 'not_applicable', session_id: null } });
    m(api.restartSystem).mockResolvedValue({ mode: 'restart', generation: 'G0', message: 'Restarting.', command: null, timeout_hint_seconds: 120 });
    m(api.healthGeneration).mockResolvedValue(null);
    renderAt('/setup/review');
    await heading('Review and finish');
    fireEvent.click(screen.getByRole('button', { name: 'Restart Nerve' }));
    await screen.findByRole('dialog', { name: /Restarting Nerve/ });
    await act(async () => { await vi.advanceTimersByTimeAsync(121_000); });
    expect(await screen.findByRole('dialog', { name: 'Nerve did not come back' })).toHaveAccessibleDescription(/nerve logs/);
    expect(screen.getByRole('button', { name: 'Retry waiting' })).toBeInTheDocument();
  });

  it('a worker ends with the onboarding session', async () => {
    const worker = makeState({
      mode: 'worker',
      steps: [step('claim', { status: 'done' }), step('mode', { status: 'done', read_only: true }, {
        mode: 'worker', task_present: true, onboarding: 'running', onboarding_session_id: 'sess-1',
      }), step('provider', { status: 'done' }), step('profile', { title: 'Operator' })],
    });
    render(
      <MemoryRouter initialEntries={['/setup/review']}>
        <Routes>
          <Route path="/setup/review" element={<ReadyScreen state={worker} onboarding={{ status: 'running', session_id: 'sess-1' }} />} />
          <Route path="/chat/:sessionId" element={<ChatProbe />} />
        </Routes>
      </MemoryRouter>,
    );
    expect(screen.getByRole('heading', { level: 1, name: /Your worker is ready/ })).toBeInTheDocument();
    expect(screen.getByRole('status')).toHaveTextContent('Onboarding session running');
    await userEvent.click(screen.getByRole('button', { name: 'Open the onboarding session' }));
    expect(await screen.findByText(/chat page sess-1/)).toBeInTheDocument();
  });

  it('the worker ending follows the live onboarding state over the stale finish() answer', async () => {
    const running = makeState({
      mode: 'worker',
      steps: [step('claim', { status: 'done' }), step('mode', { status: 'done', read_only: true }, {
        mode: 'worker', task_present: true, onboarding: 'running', onboarding_session_id: 'sess-2',
      }), step('provider', { status: 'done' }), step('profile', { title: 'Operator' })],
    });
    // finish() answered before the session existed; a later load() says it is running.
    render(
      <MemoryRouter initialEntries={['/setup/review']}>
        <Routes>
          <Route path="/setup/review" element={<ReadyScreen state={running} onboarding={{ status: 'pending', session_id: null }} />} />
          <Route path="/chat/:sessionId" element={<ChatProbe />} />
        </Routes>
      </MemoryRouter>,
    );
    expect(screen.getByRole('status')).toHaveTextContent('Onboarding session running');
    expect(screen.queryByText(/Onboarding session starting/)).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Open the onboarding session' }));
    expect(await screen.findByText(/chat page sess-2/)).toBeInTheDocument();
  });

  it('a worker whose onboarding ended without a plan gets the link and a Try again, not a spinner', async () => {
    const ended = makeState({
      mode: 'worker', finished: true, steps_left: 0,
      steps: [step('claim', { status: 'done' }), step('mode', { status: 'done', read_only: true }, {
        mode: 'worker', task_present: true, onboarding: 'ended', onboarding_session_id: 'sess-9',
      }), step('provider', { status: 'done' }), step('profile', { title: 'Operator', status: 'done' })],
    });
    // Try again: the server schedules the onboarding once more and names the new session.
    m(api.setupFinish).mockResolvedValue({ state: ended, action: 'reloaded', onboarding: { status: 'running', session_id: 'sess-10' } });
    render(
      <MemoryRouter initialEntries={['/setup/review']}>
        <Routes>
          <Route path="/setup/review" element={<ReadyScreen state={ended} onboarding={{ status: 'pending', session_id: null }} />} />
          <Route path="/chat/:sessionId" element={<ChatProbe />} />
        </Routes>
      </MemoryRouter>,
    );
    expect(screen.getByRole('status')).toHaveTextContent('The onboarding session ended without writing a plan — open it to see why.');
    expect(screen.queryByText(/Onboarding session starting/)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Open the onboarding session' })).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Try again' }));
    await waitFor(() => expect(api.setupFinish).toHaveBeenCalledTimes(1));
    // The checklist still describes the session that ended; the answer names
    // the one just scheduled, and the page follows that one.
    expect(await screen.findByText('Onboarding session running')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Open the onboarding session' }));
    expect(await screen.findByText(/chat page sess-10/)).toBeInTheDocument();
  });

  it('the worker ending reaches "running" through the 5 s poll after finish() answered pending', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const pending = makeState({
      mode: 'worker', finished: true, steps_left: 0,
      steps: [step('claim', { status: 'done' }), step('mode', { status: 'done', read_only: true }, {
        mode: 'worker', task_present: true, onboarding: 'pending', onboarding_session_id: null,
      }), step('provider', { status: 'done' }), step('profile', { title: 'Operator', status: 'done' })],
    });
    current = pending;
    m(api.setupFinish).mockResolvedValue({ state: pending, action: 'reloaded', onboarding: { status: 'pending', session_id: null } });
    renderAt('/setup/review');
    await heading('Review and finish');
    fireEvent.click(screen.getByRole('button', { name: 'Finish' }));
    expect(await heading('Your worker is ready')).toBeInTheDocument();
    expect(screen.getByRole('status')).toHaveTextContent('Onboarding session starting');

    current = withStep(pending, step('mode', { status: 'done', read_only: true }, {
      mode: 'worker', task_present: true, onboarding: 'running', onboarding_session_id: 'sess-3',
    }));
    await act(async () => { await vi.advanceTimersByTimeAsync(5100); });
    expect(await screen.findByRole('button', { name: 'Open the onboarding session' })).toBeInTheDocument();
    expect(screen.getByRole('status')).toHaveTextContent('Onboarding session running');
  });

  it('the ready screen pairing widget degrades to the /pair instruction when the bot has no link', async () => {
    const telegram = withStep(makeState({ finished: true, steps_left: 0 }), step('channels', { status: 'done' }, {
      telegram: {
        enabled: true, bot_token: on('Xw9z'), bot_username: null, live: true, start_error: null,
        dm_policy: 'pairing', allowed_users_count: 0, pairing_available: true, pairing_unavailable_reason: null,
      },
      slack: CHANNELS.slack,
    }));
    m(api.setupPairing).mockResolvedValue({
      available: true, reason: null, live: true, bot_username: null, link: null,
      allowed_users_count: 0, code_active: false, expires_at: null,
    });
    m(api.setupPairingStart).mockResolvedValue({
      code: '483920', expires_at: new Date(Date.now() + 3_600_000).toISOString(),
      bot_username: null, link: null, allowed_users_count: 0,
    });
    render(<MemoryRouter><ReadyScreen state={telegram} /></MemoryRouter>);
    expect(screen.getByRole('list', { name: 'Summary' })).toHaveTextContent('Telegram bot ready to pair');
    await userEvent.click(await screen.findByRole('button', { name: 'Get pairing code' }));
    expect(await screen.findByText(/Open your bot in Telegram and send/)).toHaveTextContent('/pair 483920');
    expect(screen.queryByRole('link', { name: /in Telegram/ })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Copy' })).toBeInTheDocument();
  });
});

describe('phone layout', () => {
  it('puts the rail in a drawer behind a toggle and shows the step position', async () => {
    window.matchMedia = ((query: string) => ({
      matches: query === '(max-width: 767px)',
      media: query, onchange: null,
      addListener: () => {}, removeListener: () => {},
      addEventListener: () => {}, removeEventListener: () => {}, dispatchEvent: () => false,
    })) as typeof window.matchMedia;
    renderAt('/setup/provider');
    await heading('Connect a model');
    // Nine steps, the same nine the Overview counts; Review is named, not numbered.
    expect(screen.getByText('Step 3 of 9 · Model')).toBeInTheDocument();
    const toggle = screen.getByRole('button', { name: 'Show setup steps' });
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    await userEvent.click(toggle);
    const drawer = screen.getByRole('dialog', { name: 'Setup steps' });
    expect(within(drawer).getByRole('navigation', { name: 'Setup steps' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Continue/ })).toBeInTheDocument();
  });
});
