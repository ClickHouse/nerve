import { render, screen, waitFor, within } from '@testing-library/react';
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
    me: vi.fn(),
    listActors: vi.fn().mockResolvedValue({ actors: [] }),
    authStatus: vi.fn(),
    login: vi.fn(),
    checkAuth: vi.fn(),
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

const api = client.api as unknown as Record<string, ReturnType<typeof vi.fn>>;

function state(overrides: Partial<SetupState> = {}): SetupState {
  return {
    setup_pending: false,
    lockdown: false,
    writable: true,
    read_only_reason: null,
    restart_pending: false,
    restart_pending_paths: [],
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
  useAuthStore.setState({ authenticated: true, setupPending: false });
  api.setupState.mockResolvedValue(state());
  api.me.mockResolvedValue({
    actor_id: 'actor-1', account_id: 'acc-1', username: 'alice',
    display_name: 'Alice Example', kind: 'human',
  });
  api.authStatus.mockResolvedValue({
    auth_required: true, mode: 'local', login: 'password',
    setup_pending: false, multiple_accounts: false,
  });
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
    // Exactly what a login does: store the token, then re-read the descriptor
    // and the actor map, because both just changed.
    expect(client.setToken).toHaveBeenCalledWith('a-synthetic-session-token');
    await waitFor(() => expect(api.authStatus).toHaveBeenCalled());
    expect(api.listActors).toHaveBeenCalled();
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
  it('shows a reconnecting state and comes back on its own', async () => {
    api.restartSystem.mockResolvedValue({
      restarting: true, method: 'helper', message: 'Restarting.',
    });
    // Down once, then up.
    api.authStatus
      .mockRejectedValueOnce(new Error('Failed to fetch'))
      .mockResolvedValue({
        auth_required: true, mode: 'local', login: 'password',
        setup_pending: false, multiple_accounts: false,
      });

    renderPage();
    const restart = await screen.findByRole('region', { name: 'Restart' });
    await userEvent.click(within(restart).getByRole('button', { name: /restart now/i }));

    expect(await screen.findByText(/Restarting this instance/)).toBeTruthy();
    expect(screen.getByText(/you stay signed in/i)).toBeTruthy();

    await waitFor(
      () => expect(screen.queryByText(/Restarting this instance/)).toBeNull(),
      { timeout: 5000 },
    );
    // The token was never cleared: the reconnect lands signed in.
    expect(client.clearToken).not.toHaveBeenCalled();
  }, 10000);

  it('reports a restart that could not be started', async () => {
    api.restartSystem.mockRejectedValue(
      new Error('409: {"detail": "no"}'),
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

  it('is not shown once it has been claimed', () => {
    useAuthStore.setState({ setupPending: false });
    renderReminder();
    expect(screen.queryByRole('link', { name: /finish setup/i })).toBeNull();
  });

  it('does not nag on the setup page itself', () => {
    useAuthStore.setState({ setupPending: true });
    renderReminder('/setup');
    expect(screen.queryByRole('link', { name: /finish setup/i })).toBeNull();
  });
});
