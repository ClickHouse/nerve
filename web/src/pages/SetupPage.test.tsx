import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { SetupState } from '../api/client';

// The real token helpers, so "what ends up in storage" is a fact rather than a
// mock assertion: the setup token is a bearer credential and the strongest
// thing this suite can say about it is that it never reaches localStorage.
vi.mock('../api/client', async () => {
  const actual = await vi.importActual<typeof import('../api/client')>('../api/client');
  return {
    ...actual,
    api: {
      setupState: vi.fn(),
      setupClaim: vi.fn(),
      setupProvider: vi.fn(),
      setupProfile: vi.fn(),
      setupChannels: vi.fn(),
      setupAutomation: vi.fn(),
      setupSkip: vi.fn(),
      listActors: vi.fn(),
      authStatus: vi.fn(),
      login: vi.fn(),
      getOwnAccount: vi.fn(),
      listAccounts: vi.fn().mockResolvedValue({ accounts: [] }),
    },
    setUnauthorizedHandler: vi.fn(),
  };
});

vi.mock('../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const client = await import('../api/client');
const { SetupPage, SetupReminder } = await import('./SetupPage');
const { useSetupStore } = await import('../stores/setupStore');
const { useAuthStore } = await import('../stores/authStore');
const { useActorStore } = await import('../stores/actorStore');

const api = client.api as unknown as Record<string, ReturnType<typeof vi.fn>>;

function state(overrides: Partial<SetupState> = {}): SetupState {
  return {
    setup_pending: false,
    lockdown: false,
    writable: true,
    read_only_reason: null,
    restart_pending: false,
    restart_pending_paths: [],
    restart_pending_reasons: [],
    restart_command: 'nerve restart',
    warning: null,
    finished: false,
    steps: [
      {
        id: 'account', title: 'Claim this instance', status: 'done',
        required: true, can_skip: false, detail: 'The account has a password.',
      },
      {
        id: 'provider', title: 'Provider credential', status: 'pending',
        required: false, can_skip: true, detail: '',
      },
      {
        id: 'profile', title: 'Timezone and name', status: 'pending',
        required: false, can_skip: true, detail: 'UTC',
      },
      {
        id: 'channels', title: 'Telegram', status: 'skipped',
        required: false, can_skip: true, detail: '',
      },
      {
        id: 'automation', title: 'Automation', status: 'pending',
        required: false, can_skip: true, detail: '',
      },
    ],
    crons: [
      {
        id: 'inbox-processor', name: 'Inbox Processor',
        description: 'Polls your sources.', enabled: false,
      },
    ],
    values: {
      timezone: 'UTC',
      display_name: 'Alice Example',
      has_anthropic_key: false,
      has_openai_key: false,
      has_telegram_token: false,
      sync_github: true,
      sync_gmail: false,
      sync_telegram: false,
    },
    ...overrides,
  };
}

function renderPage() {
  return render(<MemoryRouter initialEntries={['/setup']}><SetupPage /></MemoryRouter>);
}

beforeEach(() => {
  vi.clearAllMocks();
  client.clearToken();
  localStorage.clear();
  useSetupStore.setState({ state: null, loading: true, busy: null, error: null });
  useAuthStore.setState({
    authenticated: true,
    loading: false,
    ready: true,
    error: null,
    sessionExpired: false,
    loginMode: 'password',
    account: { id: 'acc-1', username: 'alice', actor_id: 'actor-1' },
  });
  api.setupState.mockResolvedValue(state());
  api.setupClaim.mockResolvedValue({ token: 'client-session-token' });
  api.authStatus.mockResolvedValue({ auth_required: true, login: 'password' });
  // The name on the page comes from the actor map, like every other name in
  // the app — the account row carries the login, the actor carries the name.
  useActorStore.getState().reset();
  api.listActors.mockResolvedValue({
    actors: [{ id: 'actor-1', kind: 'human', display_name: 'Alice Example' }],
  });
  api.getOwnAccount.mockResolvedValue({
    id: 'acc-1', actor_id: 'actor-1', username: 'alice',
    display_name: 'Alice Example', enabled: true, has_password: true,
    created_at: 't',
  });
});

describe('claiming an unclaimed instance', () => {
  beforeEach(() => {
    useAuthStore.setState({ loginMode: 'none' });
    api.setupState.mockResolvedValue(state({ setup_pending: true }));
  });

  it('is what the page shows first, and says why it matters', async () => {
    renderPage();
    expect(await screen.findByRole('form', { name: 'Claim this instance' })).toBeTruthy();
    expect(screen.getByText(/signed in as the owner/i)).toBeTruthy();
    // The checklist is a post-claim surface; none of it is offered yet.
    expect(screen.queryByRole('region', { name: 'Provider credential' })).toBeNull();
  });

  it('requires the setup token from every caller, and says where to read it',
    async () => {
      renderPage();
      await screen.findByRole('form', { name: 'Claim this instance' });
      expect(screen.getByLabelText('Username')).toBeRequired();
      expect(screen.getByLabelText('Password')).toBeRequired();
      // No loopback exemption: the field is mandatory however the page was
      // reached, and the submit stays dead until it is filled in.
      expect(screen.getByLabelText('Setup token')).toBeRequired();
      expect(screen.getByRole('button', { name: 'Claim and sign in' })).toBeDisabled();
      expect(screen.getByText(/nerve status/)).toBeTruthy();
      expect(screen.getByText(/HTTPS or a protected tunnel/)).toBeTruthy();
    });

  it('reads the checklist only once there is somebody to read it for', async () => {
    useAuthStore.setState({ authenticated: false, loginMode: 'none' });
    renderPage();
    expect(await screen.findByRole('form', { name: 'Claim this instance' })).toBeTruthy();
    expect(api.setupState).not.toHaveBeenCalled();
  });

  it('keeps only the client session, then lands on the checklist', async () => {
    // The page opens unclaimed (the store is seeded that way); the descriptor
    // the claim re-reads afterwards is the claimed one, which is what moves
    // this tab from the form to the checklist.
    api.setupState.mockResolvedValue(state());

    renderPage();
    await screen.findByRole('form', { name: 'Claim this instance' });
    await userEvent.type(screen.getByLabelText('Username'), 'alice');
    await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
    await userEvent.type(screen.getByLabelText('Display name'), 'Alice');
    await userEvent.type(screen.getByLabelText('Setup token'), 'host-only-token');
    await userEvent.click(screen.getByRole('button', { name: 'Claim and sign in' }));

    await waitFor(() => expect(api.setupClaim).toHaveBeenCalledWith({
      username: 'alice',
      password: 'a-real-password',
      setup_token: 'host-only-token',
      display_name: 'Alice',
    }));
    // Canonical auth and the actor map are re-read: the first password changes
    // what the login form must collect, and a display name changes every label.
    await waitFor(() => expect(api.getOwnAccount).toHaveBeenCalled());
    expect(api.listActors).toHaveBeenCalled();
    expect(localStorage.getItem('nerve_token')).toBe('client-session-token');
    expect(JSON.stringify(localStorage)).not.toContain('host-only-token');

    // The same route is the checklist once the instance is claimed.
    expect(
      await screen.findByRole('region', { name: 'Provider credential' }),
    ).toBeTruthy();
  });

  it('omits a blank optional display name', async () => {
    renderPage();
    await screen.findByRole('form', { name: 'Claim this instance' });
    await userEvent.type(screen.getByLabelText('Username'), 'alice');
    await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
    await userEvent.type(screen.getByLabelText('Setup token'), 'host-only-token');
    await userEvent.click(screen.getByRole('button', { name: 'Claim and sign in' }));

    await waitFor(() => expect(api.setupClaim).toHaveBeenCalled());
    expect(api.setupClaim.mock.calls[0][0]).toEqual({
      username: 'alice',
      password: 'a-real-password',
      setup_token: 'host-only-token',
      display_name: undefined,
    });
  });

  it('shows the server’s refusal without storing either credential', async () => {
    api.setupClaim.mockRejectedValue(new Error('403: invalid setup token'));
    renderPage();
    await screen.findByRole('form', { name: 'Claim this instance' });
    await userEvent.type(screen.getByLabelText('Username'), 'alice');
    await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
    await userEvent.type(screen.getByLabelText('Setup token'), 'wrong-token');
    await userEvent.click(screen.getByRole('button', { name: 'Claim and sign in' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('invalid setup token');
    expect(localStorage.getItem('nerve_token')).toBeNull();
    expect(JSON.stringify(localStorage)).not.toContain('wrong-token');
  });
});

describe('the checklist', () => {
  it('lists every step with its status', async () => {
    renderPage();
    await screen.findByRole('region', { name: 'Provider credential' });
    for (const title of [
      'Claim this instance', 'Provider credential', 'Timezone and name',
      'Telegram', 'Automation',
    ]) {
      expect(screen.getByRole('region', { name: title })).toBeTruthy();
    }
    const claimed = screen.getByRole('region', { name: 'Claim this instance' });
    expect(within(claimed).getByText('done')).toBeTruthy();
    const telegram = screen.getByRole('region', { name: 'Telegram' });
    expect(within(telegram).getByText('skipped')).toBeTruthy();
  });

  it('says who you are signed in as', async () => {
    renderPage();
    expect(await screen.findByText(/Signed in as Alice Example/)).toBeTruthy();
  });

  it('skips a step and puts it back', async () => {
    api.setupSkip.mockResolvedValue(state({
      steps: state().steps.map((s) => (
        s.id === 'provider' ? { ...s, status: 'skipped' as const } : s
      )),
    }));
    renderPage();
    const provider = await screen.findByRole('region', { name: 'Provider credential' });
    await userEvent.click(within(provider).getByRole('button', { name: 'Skip' }));
    await waitFor(() => expect(api.setupSkip).toHaveBeenCalledWith('provider', true));

    await within(screen.getByRole('region', { name: 'Provider credential' }))
      .findByText('skipped');
    await userEvent.click(
      within(screen.getByRole('region', { name: 'Provider credential' }))
        .getByRole('button', { name: 'Put back' }),
    );
    await waitFor(() => expect(api.setupSkip).toHaveBeenLastCalledWith('provider', false));
  });

  it('never offers to skip the one required step', async () => {
    renderPage();
    const account = await screen.findByRole('region', { name: 'Claim this instance' });
    expect(within(account).queryByRole('button', { name: 'Skip' })).toBeNull();
  });

  it('saves a provider key and takes the server’s new state', async () => {
    api.setupProvider.mockResolvedValue(state({
      restart_pending: true, restart_pending_paths: ['anthropic_api_key'],
      steps: state().steps.map((s) => (
        s.id === 'provider'
          ? { ...s, status: 'done' as const, detail: 'Saved.' }
          : s
      )),
    }));
    renderPage();
    const provider = await screen.findByRole('region', { name: 'Provider credential' });
    await userEvent.type(
      within(provider).getByLabelText('Anthropic API key'), 'a-key',
    );
    await userEvent.click(within(provider).getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(api.setupProvider).toHaveBeenCalledWith({
      anthropic_api_key: 'a-key', openai_api_key: undefined,
    }));
    expect(await screen.findByText(/Waiting on a restart: anthropic_api_key/)).toBeTruthy();
  });

  it('says a browser cannot reach the keychain', async () => {
    renderPage();
    const provider = await screen.findByRole('region', { name: 'Provider credential' });
    expect(within(provider).getByText(/cannot reach your laptop's keychain/i)).toBeTruthy();
  });

  it('offers the crons the install actually has', async () => {
    renderPage();
    const automation = await screen.findByRole('region', { name: 'Automation' });
    expect(within(automation).getByLabelText('Inbox Processor')).toBeTruthy();
  });
});

describe('applying what a step wrote', () => {
  it('names the command an operator runs, and offers no button of its own',
    async () => {
      api.setupState.mockResolvedValue(state({
        restart_pending: true,
        restart_pending_paths: ['timezone'],
        restart_pending_reasons: ['the scheduler is still running the old crons'],
      }));
      renderPage();
      const notice = await screen.findByRole('region', { name: 'Restart to apply' });
      expect(within(notice).getByText(/Waiting on a restart: timezone/)).toBeTruthy();
      expect(within(notice).getByText(/still running the old crons/)).toBeTruthy();
      expect(within(notice).getByText(/nerve restart/)).toBeTruthy();
      expect(within(notice).getByText(/reload this page/i)).toBeTruthy();
      // The browser holds no process control over the box it is talking to.
      expect(within(notice).queryByRole('button')).toBeNull();
    });

  it('says nothing is waiting when nothing is', async () => {
    renderPage();
    const notice = await screen.findByRole('region', { name: 'Restart to apply' });
    expect(within(notice).getByText(/Nothing is waiting on a restart/)).toBeTruthy();
    expect(within(notice).queryByRole('button')).toBeNull();
  });
});

describe('a step that saved its configuration but not its note', () => {
  it('says so rather than reporting a plain success', async () => {
    api.setupProvider.mockResolvedValue(state({
      warning: 'The configuration was written, but the checklist could not '
        + 'record the provider step.',
    }));
    renderPage();
    const provider = await screen.findByRole('region', { name: 'Provider credential' });
    await userEvent.type(
      within(provider).getByLabelText('Anthropic API key'), 'a-key',
    );
    await userEvent.click(within(provider).getByRole('button', { name: 'Save' }));

    expect(
      await screen.findByText(/could not record the provider step/),
    ).toBeTruthy();
  });
});

describe('lockdown', () => {
  beforeEach(() => {
    api.setupState.mockResolvedValue(state({
      lockdown: true, writable: false,
      read_only_reason: 'This instance is in lockdown: its configuration is fleet-managed.',
    }));
  });

  it('explains why nothing can be saved', async () => {
    renderPage();
    expect(await screen.findByText(/fleet-managed/)).toBeTruthy();
  });

  it('does not render a form to fill in', async () => {
    renderPage();
    const provider = await screen.findByRole('region', { name: 'Provider credential' });
    expect(within(provider).queryByLabelText('Anthropic API key')).toBeNull();
    expect(within(provider).getByText(/read-only/i)).toBeTruthy();
  });
});

describe('the finish-setup affordance', () => {
  function renderReminder(path = '/chat') {
    return render(
      <MemoryRouter initialEntries={[path]}><SetupReminder /></MemoryRouter>,
    );
  }

  it('is visible while the instance is unclaimed', () => {
    useAuthStore.setState({ loginMode: 'none' });
    renderReminder();
    expect(screen.getByRole('link', { name: /finish setup/i })).toBeTruthy();
  });

  it('stays after the claim while the checklist is unfinished', () => {
    // The state the affordance is actually for: an unclaimed instance already
    // routes here by itself, and an abandoned *post-claim* checklist would
    // otherwise have nothing pointing at it anywhere in the app.
    useAuthStore.setState({ loginMode: 'password' });
    useSetupStore.setState({ state: state({ finished: false }) });
    renderReminder();
    expect(screen.getByRole('link', { name: /finish setup/i })).toBeTruthy();
    expect(screen.getByText(/Setup is not finished/)).toBeTruthy();
  });

  it('goes away once the server says the list is finished', () => {
    useAuthStore.setState({ loginMode: 'password' });
    useSetupStore.setState({ state: state({ finished: true }) });
    renderReminder();
    expect(screen.queryByRole('link', { name: /finish setup/i })).toBeNull();
  });

  it('says nothing while the checklist has not been read', () => {
    // A failed or pending read must not grow a permanent nag.
    useAuthStore.setState({ loginMode: 'password' });
    useSetupStore.setState({ state: null });
    renderReminder();
    expect(screen.queryByRole('link', { name: /finish setup/i })).toBeNull();
  });

  it('does not nag on the setup page itself', () => {
    useAuthStore.setState({ loginMode: 'none' });
    renderReminder('/setup');
    expect(screen.queryByRole('link', { name: /finish setup/i })).toBeNull();
  });
});

describe('hydration', () => {
  it('opens the profile form on the instance\'s timezone, not the browser\'s', async () => {
    api.setupState.mockResolvedValue(state({
      values: { ...state().values, timezone: 'Pacific/Auckland' },
    }));
    renderPage();
    const profile = await screen.findByRole('region', { name: 'Timezone and name' });
    expect(
      (within(profile).getByLabelText('Time zone') as HTMLInputElement).value,
    ).toBe('Pacific/Auckland');
  });

  it('sends only the field that changed', async () => {
    api.setupProfile.mockResolvedValue(state());
    renderPage();
    const profile = await screen.findByRole('region', { name: 'Timezone and name' });
    const name = within(profile).getByLabelText('Your display name');
    await userEvent.clear(name);
    await userEvent.type(name, 'Alice Second');
    await userEvent.click(within(profile).getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(api.setupProfile).toHaveBeenCalled());
    // The timezone is shared configuration; editing a name must not move it.
    expect(api.setupProfile.mock.calls[0][0]).toEqual({
      display_name: 'Alice Second',
    });
  });

  it('hydrates the sync toggles and submits only what moved', async () => {
    api.setupAutomation.mockResolvedValue(state());
    renderPage();
    const automation = await screen.findByRole('region', { name: 'Automation' });
    expect(
      (within(automation).getByLabelText('GitHub') as HTMLInputElement).checked,
    ).toBe(true);

    await userEvent.click(within(automation).getByLabelText('Inbox Processor'));
    await userEvent.click(within(automation).getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(api.setupAutomation).toHaveBeenCalled());
    expect(api.setupAutomation.mock.calls[0][0]).toEqual({
      crons: ['inbox-processor'],
    });
  });

  it('refreshes the names in the app after a rename', async () => {
    api.setupProfile.mockResolvedValue(state());
    renderPage();
    const profile = await screen.findByRole('region', { name: 'Timezone and name' });
    const name = within(profile).getByLabelText('Your display name');
    await userEvent.clear(name);
    await userEvent.type(name, 'Alice Renamed');
    await userEvent.click(within(profile).getByRole('button', { name: 'Save' }));
    // Nothing stores the name it changed, so every label reads it again.
    await waitFor(() => expect(api.listActors).toHaveBeenCalled());
  });
});

describe('Telegram sync', () => {
  it('is offered, hydrated, and sends only what moved', async () => {
    api.setupState.mockResolvedValue(state({
      values: { ...state().values, sync_telegram: false },
    }));
    api.setupAutomation.mockResolvedValue(state());
    renderPage();
    const automation = await screen.findByRole('region', { name: 'Automation' });

    const toggle = within(automation).getByLabelText('Telegram') as HTMLInputElement;
    expect(toggle.checked).toBe(false);
    await userEvent.click(toggle);

    // Its credentials are not the bot token, and they only appear once the
    // source is on.
    await userEvent.type(
      within(automation).getByLabelText('Telegram API id'), '1234567',
    );
    await userEvent.type(
      within(automation).getByLabelText('Telegram API hash'), 'a-hash-placeholder',
    );
    await userEvent.click(within(automation).getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(api.setupAutomation).toHaveBeenCalled());
    expect(api.setupAutomation.mock.calls[0][0]).toEqual({
      telegram: true,
      telegram_api_id: 1234567,
      telegram_api_hash: 'a-hash-placeholder',
    });
  });

  it('hides the credential fields while the source is off', async () => {
    renderPage();
    const automation = await screen.findByRole('region', { name: 'Automation' });
    expect(within(automation).queryByLabelText('Telegram API id')).toBeNull();
  });
});

describe('signing out', () => {
  it('takes the checklist state with it', async () => {
    api.setupState.mockResolvedValue(state({
      values: { ...state().values, display_name: 'Alice Example' },
    }));
    useAuthStore.setState({
      account: { id: 'acc-alice', username: 'alice', actor_id: 'actor-alice' },
    });
    renderPage();
    const profile = await screen.findByRole('region', { name: 'Timezone and name' });
    expect(
      (within(profile).getByLabelText('Your display name') as HTMLInputElement).value,
    ).toBe('Alice Example');

    act(() => { useAuthStore.getState().logout(); });
    // Nothing of hers survives for whoever signs in next.
    expect(useSetupStore.getState().state).toBeNull();
  });

  it('does not let a save started before it overwrite the next person', async () => {
    let answer: (v: unknown) => void = () => {};
    api.setupProvider.mockReturnValue(new Promise((r) => { answer = r; }));
    const saving = useSetupStore.getState().save(
      'provider', () => api.setupProvider({ anthropic_api_key: 'alice-key' }),
    );

    act(() => { useAuthStore.getState().logout(); });
    // Bob signs in and his own checklist lands.
    const bobs = state({ finished: true });
    act(() => { useSetupStore.setState({ state: bobs, busy: 'channels' }); });

    await act(async () => { answer(state({ finished: false })); await saving; });

    expect(useSetupStore.getState().state).toBe(bobs);
    expect(useSetupStore.getState().busy).toBe('channels');
  });

  it('does not let a skip started before it overwrite the next person', async () => {
    let answer: (v: unknown) => void = () => {};
    api.setupSkip.mockReturnValue(new Promise((r) => { answer = r; }));
    const skipping = useSetupStore.getState().skip('channels', true);

    act(() => { useAuthStore.getState().logout(); });
    const bobs = state({ finished: true });
    act(() => { useSetupStore.setState({ state: bobs, busy: null }); });

    await act(async () => { answer(state({ finished: false })); await skipping; });

    expect(useSetupStore.getState().state).toBe(bobs);
  });

  it('does not let a read started before it repopulate the store', async () => {
    let answer: (v: unknown) => void = () => {};
    api.setupState.mockReturnValue(new Promise((r) => { answer = r; }));
    const load = useSetupStore.getState().load();

    act(() => { useAuthStore.getState().logout(); });
    await act(async () => { answer(state()); await load; });

    expect(useSetupStore.getState().state).toBeNull();
  });
});

describe('a second person on the same browser', () => {
  it('does not offer them the last person\'s name as their own', async () => {
    // Alice fills in her name and signs out; Bob signs in and the checklist
    // re-renders for him. The form keeps `useState`, which new props do not
    // reset — so without remounting, Bob's form holds Alice's name and the
    // next save submits it as *his* profile.
    api.setupState.mockResolvedValue(state({
      values: { ...state().values, display_name: 'Alice Example' },
    }));
    useAuthStore.setState({
      account: { id: 'acc-alice', username: 'alice', actor_id: 'actor-alice' },
    });
    renderPage();
    const profile = await screen.findByRole('region', { name: 'Timezone and name' });
    expect(
      (within(profile).getByLabelText('Your display name') as HTMLInputElement).value,
    ).toBe('Alice Example');

    act(() => {
      useAuthStore.setState({
        account: { id: 'acc-bob', username: 'bob', actor_id: 'actor-bob' },
      });
      useSetupStore.setState({
        state: state({ values: { ...state().values, display_name: null } }),
      });
    });

    await waitFor(() => expect(
      (screen.getByLabelText('Your display name') as HTMLInputElement).value,
    ).toBe(''));
  });
});

describe('one mutation at a time', () => {
  it('disables every other control while a save is in flight', async () => {
    let release: (v: unknown) => void = () => {};
    api.setupProvider.mockReturnValue(new Promise((r) => { release = r; }));
    renderPage();
    const provider = await screen.findByRole('region', { name: 'Provider credential' });
    await userEvent.type(
      within(provider).getByLabelText('Anthropic API key'), 'a-key',
    );
    await userEvent.click(within(provider).getByRole('button', { name: 'Save' }));

    const automation = screen.getByRole('region', { name: 'Automation' });
    await waitFor(() => expect(
      (within(automation).getByRole('button', { name: 'Skip' }) as HTMLButtonElement)
        .disabled,
    ).toBe(true));

    // Let the save finish *inside* act and wait for the render it causes.
    // Resolving it on the way out of the test leaves React applying state to
    // a component nobody is watching any more.
    await act(async () => { release(state()); });
    await waitFor(() => expect(
      (within(screen.getByRole('region', { name: 'Automation' }))
        .getByRole('button', { name: 'Skip' }) as HTMLButtonElement).disabled,
    ).toBe(false));
    expect(useSetupStore.getState().busy).toBeNull();
  });
});
