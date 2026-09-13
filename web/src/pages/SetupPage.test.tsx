import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { SetupState } from '../api/client';

vi.mock('../api/client', () => ({
  api: {
    setupState: vi.fn(),
    setupClaim: vi.fn(),
    setupProvider: vi.fn(),
    setupProfile: vi.fn(),
    setupChannels: vi.fn(),
    setupAutomation: vi.fn(),
    setupSkip: vi.fn(),
    restartSystem: vi.fn(),
    health: vi.fn(),
    me: vi.fn(),
    listActors: vi.fn(),
    authStatus: vi.fn(),
    login: vi.fn(),
    getOwnAccount: vi.fn(),
    listAccounts: vi.fn().mockResolvedValue({ accounts: [] }),
  },
  setToken: vi.fn(),
  clearToken: vi.fn(),
  getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

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
  return render(<MemoryRouter><SetupPage /></MemoryRouter>);
}

beforeEach(() => {
  vi.clearAllMocks();
  useSetupStore.setState({
    state: null, loading: true, busy: null, error: null, reconnecting: false,
  });
  useAuthStore.setState({
    authenticated: true, setupPending: false,
    account: { id: 'acc-1', username: 'alice', actor_id: 'actor-1' },
  });
  api.setupState.mockResolvedValue(state());
  api.me.mockResolvedValue({
    actor_id: 'actor-1', account_id: 'acc-1', username: 'alice',
    display_name: 'Alice Example', kind: 'human',
  });
  api.authStatus.mockResolvedValue({
    auth_required: true, mode: 'local', login: 'password',
    setup_pending: false, multiple_accounts: false,
  });
  // The name on the page comes from the actor map, like every other name in
  // the app — the account row carries the login, the actor carries the name.
  useActorStore.getState().reset();
  api.listActors.mockResolvedValue({
    actors: [{
      id: 'actor-1', kind: 'human', display_name: 'Alice Example',
      profile_version: 1,
    }],
  });
  api.getOwnAccount.mockResolvedValue({
    id: 'acc-1', actor_id: 'actor-1', username: 'alice',
    display_name: 'Alice Example', enabled: true, has_password: true,
    created_at: '', updated_at: '', disabled_at: null, is_self: true,
  });
  (client.getToken as unknown as ReturnType<typeof vi.fn>)
    .mockReturnValue('a-synthetic-session-token');
});

describe('claiming an unclaimed instance', () => {
  beforeEach(() => {
    useAuthStore.setState({ setupPending: true });
    api.setupState.mockResolvedValue(state({ setup_pending: true }));
  });

  it('is what the page shows first, and says why it matters', async () => {
    renderPage();
    const form = await screen.findByRole('form', { name: 'Claim this instance' });
    expect(form).toBeTruthy();
    expect(screen.getByText(/signed in as the owner/i)).toBeTruthy();
  });

  it('offers the token as optional, and says where to find it', async () => {
    renderPage();
    const token = await screen.findByLabelText('Setup token');
    expect(token.hasAttribute('required')).toBe(false);
    expect(screen.getByText(/needs no token/i)).toBeTruthy();
    expect(screen.getByText(/docker logs/i)).toBeTruthy();
  });

  it('signs the tab in with the session the claim returns', async () => {
    api.setupClaim.mockResolvedValue({
      token: 'a-synthetic-session-token', account_id: 'acc-1',
      actor_id: 'actor-1', username: 'alice', display_name: null,
    });
    // Unclaimed on the way in, claimed on the read that follows the claim.
    api.setupState
      .mockResolvedValueOnce(state({ setup_pending: true }))
      .mockResolvedValue(state());

    renderPage();
    await screen.findByRole('form', { name: 'Claim this instance' });
    await userEvent.type(screen.getByLabelText('Username'), 'alice');
    await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
    await userEvent.click(screen.getByRole('button', { name: /claim and sign in/i }));

    await waitFor(() => expect(api.setupClaim).toHaveBeenCalled());
    expect(api.setupClaim.mock.calls[0][0]).toMatchObject({
      username: 'alice', password: 'a-real-password',
    });
    // Exactly what a login does: store the token, then run the auth store's
    // own startup path so this tab is signed in rather than sent back to a
    // form for the password it just set.
    expect(client.setToken).toHaveBeenCalledWith('a-synthetic-session-token');
    await waitFor(() => expect(api.getOwnAccount).toHaveBeenCalled());
    expect(api.authStatus).toHaveBeenCalled();
    expect(api.listActors).toHaveBeenCalled();
    expect(useAuthStore.getState().authenticated).toBe(true);
  });

  it('sends no token field when none was typed', async () => {
    api.setupClaim.mockResolvedValue({
      token: 't', account_id: 'a', actor_id: 'b', username: 'alice',
      display_name: null,
    });
    renderPage();
    await screen.findByRole('form', { name: 'Claim this instance' });
    await userEvent.type(screen.getByLabelText('Username'), 'alice');
    await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
    await userEvent.click(screen.getByRole('button', { name: /claim and sign in/i }));
    await waitFor(() => expect(api.setupClaim).toHaveBeenCalled());
    expect(api.setupClaim.mock.calls[0][0].setup_token).toBeUndefined();
  });

  it('shows the server’s refusal rather than a guess', async () => {
    api.setupClaim.mockRejectedValue(
      new Error('403: {"detail": "A setup token is required from this address."}'),
    );
    renderPage();
    await screen.findByRole('form', { name: 'Claim this instance' });
    await userEvent.type(screen.getByLabelText('Username'), 'alice');
    await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
    await userEvent.click(screen.getByRole('button', { name: /claim and sign in/i }));

    const alert = await screen.findByRole('alert');
    expect(alert.textContent).toContain('setup token is required');
  });

  it('is reachable with no session at all', async () => {
    useAuthStore.setState({ authenticated: false, setupPending: true });
    renderPage();
    expect(await screen.findByRole('form', { name: 'Claim this instance' })).toBeTruthy();
    // Nothing authenticated is fetched while there is nobody to fetch it for.
    expect(api.setupState).not.toHaveBeenCalled();
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

describe('the restart', () => {
  it('waits for a different process, not for any answer at all', async () => {
    api.restartSystem.mockResolvedValue({
      restarting: true, method: 'helper', message: 'Restarting.',
      boot: 'the-old-process',
    });
    // Down once; then the *old* process answering as it shuts down; then the
    // new one. Only the last of those is a restart.
    api.health
      .mockRejectedValueOnce(new Error('Failed to fetch'))
      .mockResolvedValueOnce({ status: 'ok', boot: 'the-old-process' })
      .mockResolvedValue({ status: 'ok', boot: 'the-new-process' });

    renderPage();
    const restart = await screen.findByRole('region', { name: 'Restart' });
    await userEvent.click(within(restart).getByRole('button', { name: /restart now/i }));

    expect(await screen.findByText(/Restarting this instance/)).toBeTruthy();
    expect(screen.getByText(/you stay signed in/i)).toBeTruthy();

    await waitFor(
      () => expect(screen.queryByText(/Restarting this instance/)).toBeNull(),
      { timeout: 8000 },
    );
    expect(api.health.mock.calls.length).toBeGreaterThanOrEqual(3);
    // The token was never cleared: the reconnect lands signed in.
    expect(client.clearToken).not.toHaveBeenCalled();
  }, 15000);

  it('reports a restart that could not be started', async () => {
    api.restartSystem.mockRejectedValue(
      new Error('500: {"detail": "no"}'),
    );
    renderPage();
    const restart = await screen.findByRole('region', { name: 'Restart' });
    await userEvent.click(within(restart).getByRole('button', { name: /restart now/i }));
    expect((await screen.findByRole('alert')).textContent).toContain('no');
    expect(screen.queryByText(/Restarting this instance/)).toBeNull();
  });
});

describe('the finish-setup affordance', () => {
  function renderReminder(path = '/chat') {
    return render(
      <MemoryRouter initialEntries={[path]}><SetupReminder /></MemoryRouter>,
    );
  }

  it('is visible while the instance is unclaimed', () => {
    useAuthStore.setState({ setupPending: true });
    renderReminder();
    expect(screen.getByRole('link', { name: /finish setup/i })).toBeTruthy();
  });

  it('stays after the claim while the checklist is unfinished', () => {
    // The state the affordance is actually for: an unclaimed instance already
    // redirects here by itself, and an abandoned *post-claim* checklist used
    // to have nothing pointing at it anywhere in the app.
    useAuthStore.setState({ setupPending: false });
    useSetupStore.setState({ state: state({ finished: false }) });
    renderReminder();
    expect(screen.getByRole('link', { name: /finish setup/i })).toBeTruthy();
    expect(screen.getByText(/Setup is not finished/)).toBeTruthy();
  });

  it('goes away once the server says the list is finished', () => {
    useAuthStore.setState({ setupPending: false });
    useSetupStore.setState({ state: state({ finished: true }) });
    renderReminder();
    expect(screen.queryByRole('link', { name: /finish setup/i })).toBeNull();
  });

  it('says nothing while the checklist has not been read', () => {
    // A failed or pending read must not grow a permanent nag.
    useAuthStore.setState({ setupPending: false });
    useSetupStore.setState({ state: null });
    renderReminder();
    expect(screen.queryByRole('link', { name: /finish setup/i })).toBeNull();
  });

  it('does not nag on the setup page itself', () => {
    useAuthStore.setState({ setupPending: true });
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
    // a component nobody is watching any more — which is what the "not
    // wrapped in act" warnings were, and why the assertions after it would
    // have run before the behaviour they describe.
    await act(async () => { release(state()); });
    await waitFor(() => expect(
      (within(screen.getByRole('region', { name: 'Automation' }))
        .getByRole('button', { name: 'Skip' }) as HTMLButtonElement).disabled,
    ).toBe(false));
    expect(useSetupStore.getState().busy).toBeNull();
  });
});
