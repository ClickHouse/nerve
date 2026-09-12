import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Account } from '../api/client';

vi.mock('../api/client', () => ({
  api: {
    listAccounts: vi.fn(),
    createAccount: vi.fn(),
    updateAccount: vi.fn(),
    setAccountEnabled: vi.fn(),
    changeOwnPassword: vi.fn(),
    authStatus: vi.fn().mockResolvedValue({
      auth_required: true, mode: 'local', login: 'password',
      setup_pending: false, multiple_accounts: false,
    }),
    login: vi.fn(),
    checkAuth: vi.fn(),
  },
  setToken: vi.fn(),
  clearToken: vi.fn(),
  getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

vi.mock('../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const client = await import('../api/client');
const { AccountsPage } = await import('./AccountsPage');
const { useAccountStore, errorDetail, blockedReason } = await import('../stores/accountStore');

const api = client.api as unknown as Record<string, ReturnType<typeof vi.fn>>;

function account(overrides: Partial<Account> = {}): Account {
  return {
    id: 'acc-1',
    actor_id: 'actor-1',
    username: 'alice',
    display_name: 'Alice',
    enabled: true,
    has_password: true,
    created_at: '2026-08-05T00:00:00Z',
    updated_at: '2026-08-05T00:00:00Z',
    disabled_at: null,
    is_self: true,
    ...overrides,
  };
}

function renderPage() {
  return render(<MemoryRouter><AccountsPage /></MemoryRouter>);
}

beforeEach(() => {
  vi.clearAllMocks();
  api.listAccounts.mockReset();
  api.createAccount.mockReset();
  api.setAccountEnabled.mockReset();
  api.changeOwnPassword.mockReset();
  api.updateAccount.mockReset();
  api.authStatus.mockResolvedValue({
    auth_required: true, mode: 'local', login: 'password',
    setup_pending: false, multiple_accounts: false,
  });
  useAccountStore.setState({ accounts: [], loading: true, busyId: null, error: null });
});

describe('the list', () => {
  it('shows username, display name and state for every account', async () => {
    api.listAccounts.mockResolvedValue({
      accounts: [
        account(),
        account({
          id: 'acc-2', username: 'bob', display_name: 'Bob',
          enabled: false, disabled_at: '2026-08-06T00:00:00Z', is_self: false,
        }),
      ],
    });
    renderPage();

    expect(await screen.findByText('alice')).toBeInTheDocument();
    expect(screen.getByText('bob')).toBeInTheDocument();
    expect(screen.getByText('you')).toBeInTheDocument();
    expect(screen.getByText('disabled')).toBeInTheDocument();
  });

  it('names an account with no username rather than showing a blank row', async () => {
    api.listAccounts.mockResolvedValue({
      accounts: [account({ username: null, display_name: null, has_password: false })],
    });
    renderPage();
    expect(await screen.findByText('no username')).toBeInTheDocument();
    expect(screen.getByText('no password')).toBeInTheDocument();
  });

  it('reports a failure to load instead of rendering an empty list', async () => {
    api.listAccounts.mockRejectedValue(new Error('500: {"detail": "nope"}'));
    renderPage();
    expect(await screen.findByRole('alert')).toHaveTextContent('nope');
  });
});

describe('adding a person', () => {
  it('posts the form and re-reads both the list and the login descriptor', async () => {
    api.listAccounts.mockResolvedValue({ accounts: [account()] });
    api.createAccount.mockResolvedValue(account({ id: 'acc-2', username: 'bob' }));
    renderPage();
    await screen.findByText('alice');

    // PageHeader renders its actions twice (a mobile copy and a desktop one);
    // jsdom applies no media queries, so both are in the DOM.
    await userEvent.click(screen.getAllByRole('button', { name: /Add account/ })[0]);
    const form = screen.getByRole('form', { name: 'Add account' });
    await userEvent.type(within(form).getByLabelText('New account username'), 'bob');
    await userEvent.type(within(form).getByLabelText('New account password'), 'a-passphrase');
    await userEvent.click(within(form).getByRole('button', { name: 'Create' }));

    await waitFor(() => expect(api.createAccount).toHaveBeenCalledWith({
      username: 'bob', password: 'a-passphrase', display_name: undefined,
    }));
    // The second account changes what the login form must collect, so the
    // descriptor is re-read rather than left stale.
    await waitFor(() => expect(api.authStatus).toHaveBeenCalled());
    expect(api.listAccounts).toHaveBeenCalledTimes(2);
  });

  it('surfaces the server’s reason and keeps the form open', async () => {
    api.listAccounts.mockResolvedValue({ accounts: [account()] });
    api.createAccount.mockRejectedValue(
      new Error('409: {"detail": "This instance is passwordless, so a second account '
        + 'could not be told apart from the first."}'),
    );
    renderPage();
    await screen.findByText('alice');

    // PageHeader renders its actions twice (a mobile copy and a desktop one);
    // jsdom applies no media queries, so both are in the DOM.
    await userEvent.click(screen.getAllByRole('button', { name: /Add account/ })[0]);
    const form = screen.getByRole('form', { name: 'Add account' });
    await userEvent.type(within(form).getByLabelText('New account username'), 'bob');
    await userEvent.type(within(form).getByLabelText('New account password'), 'x');
    await userEvent.click(within(form).getByRole('button', { name: 'Create' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('passwordless');
    expect(screen.getByRole('form', { name: 'Add account' })).toBeInTheDocument();
  });
});

describe('disabling and enabling', () => {
  it('asks before disabling, and only then calls the endpoint', async () => {
    api.listAccounts.mockResolvedValue({
      accounts: [
        account(),
        account({ id: 'acc-2', username: 'bob', is_self: false }),
      ],
    });
    api.setAccountEnabled.mockResolvedValue(account());
    renderPage();
    await screen.findByText('bob');

    await userEvent.click(screen.getAllByRole('button', { name: 'Disable' })[1]);
    // Nothing has happened yet — the click opened a question.
    expect(api.setAccountEnabled).not.toHaveBeenCalled();
    expect(await screen.findByRole('alertdialog')).toHaveTextContent(/Disable bob\?/);

    await userEvent.click(screen.getByRole('button', { name: 'Disable account' }));
    expect(api.setAccountEnabled).toHaveBeenCalledWith('acc-2', false);
  });

  it('lets the question be answered no', async () => {
    api.listAccounts.mockResolvedValue({
      accounts: [account(), account({ id: 'acc-2', username: 'bob', is_self: false })],
    });
    renderPage();
    await screen.findByText('bob');

    await userEvent.click(screen.getAllByRole('button', { name: 'Disable' })[1]);
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));

    expect(api.setAccountEnabled).not.toHaveBeenCalled();
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument();
  });

  it('warns harder about disabling yourself, and says what it costs', async () => {
    api.listAccounts.mockResolvedValue({
      accounts: [account(), account({ id: 'acc-2', username: 'bob', is_self: false })],
    });
    api.setAccountEnabled.mockResolvedValue(account());
    renderPage();
    await screen.findByText('alice');

    await userEvent.click(screen.getAllByRole('button', { name: 'Disable' })[0]);
    const dialog = await screen.findByRole('alertdialog');
    expect(dialog).toHaveTextContent(/signed out/);
    expect(dialog).toHaveTextContent(/somebody else/);
    // A distinct label, so "confirm" is not the same word as "start".
    await userEvent.click(
      within(dialog).getByRole('button', { name: 'Disable my account' }),
    );
    expect(api.setAccountEnabled).toHaveBeenCalledWith('acc-1', false);
  });

  it('enables without asking — it takes nothing away', async () => {
    api.listAccounts.mockResolvedValue({
      accounts: [
        account(),
        account({ id: 'acc-2', username: 'bob', enabled: false, is_self: false,
                  disabled_at: '2026-08-06T00:00:00Z' }),
      ],
    });
    api.setAccountEnabled.mockResolvedValue(account());
    renderPage();
    await screen.findByText('alice');

    await userEvent.click(screen.getByRole('button', { name: 'Enable' }));
    expect(api.setAccountEnabled).toHaveBeenCalledWith('acc-2', true);
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument();
  });

  it('shows the last-account refusal as the server explains it', async () => {
    api.listAccounts.mockResolvedValue({ accounts: [account()] });
    api.setAccountEnabled.mockRejectedValue(
      new Error('409: {"detail": "This is the last enabled account."}'),
    );
    renderPage();
    await screen.findByText('alice');

    await userEvent.click(screen.getByRole('button', { name: 'Disable' }));
    await userEvent.click(screen.getByRole('button', { name: 'Disable my account' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('last enabled account');
  });
});

describe('the first account’s username and password', () => {
  it('offers "Set username" on an account that has none', async () => {
    api.listAccounts.mockResolvedValue({
      accounts: [account({ username: null, has_password: false })],
    });
    api.updateAccount.mockResolvedValue(account());
    renderPage();
    await screen.findByText('no username');

    await userEvent.click(screen.getByRole('button', { name: 'Set username' }));
    await userEvent.type(screen.getByLabelText(/^Username for/), 'alice');
    await userEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(api.updateAccount).toHaveBeenCalledWith(
      'acc-1', { username: 'alice' },
    ));
  });

  it('asks for no current password when the account has none yet', async () => {
    api.listAccounts.mockResolvedValue({
      accounts: [account({ has_password: false })],
    });
    api.changeOwnPassword.mockResolvedValue(account());
    renderPage();
    await screen.findByText('alice');

    const form = screen.getByRole('form', { name: 'Your password' });
    expect(within(form).queryByLabelText('Current password')).not.toBeInTheDocument();
    await userEvent.type(within(form).getByLabelText('New password'), 'a-passphrase');
    await userEvent.click(within(form).getByRole('button', { name: 'Save password' }));

    await waitFor(() => expect(api.changeOwnPassword).toHaveBeenCalledWith({
      current_password: undefined, new_password: 'a-passphrase',
    }));
  });

  it('requires the current password once there is one', async () => {
    api.listAccounts.mockResolvedValue({ accounts: [account()] });
    api.changeOwnPassword.mockResolvedValue(account());
    renderPage();
    await screen.findByText('alice');

    const form = screen.getByRole('form', { name: 'Your password' });
    await userEvent.type(within(form).getByLabelText('Current password'), 'the-old-one');
    await userEvent.type(within(form).getByLabelText('New password'), 'the-new-one');
    await userEvent.click(within(form).getByRole('button', { name: 'Save password' }));

    await waitFor(() => expect(api.changeOwnPassword).toHaveBeenCalledWith({
      current_password: 'the-old-one', new_password: 'the-new-one',
    }));
  });
});

describe('what blocks adding a person', () => {
  it('explains a passwordless sole account', () => {
    expect(blockedReason([account({ has_password: false })])).toMatch(/password/);
  });

  it('explains an account with no username', () => {
    expect(blockedReason([account({ username: null })])).toMatch(/username/);
  });

  it('says nothing once both are set', () => {
    expect(blockedReason([account()])).toBeNull();
  });

  it('is shown on the page', async () => {
    api.listAccounts.mockResolvedValue({
      accounts: [account({ has_password: false })],
    });
    renderPage();
    expect(await screen.findByText(/Set a password on this account/)).toBeInTheDocument();
  });
});

describe('errorDetail', () => {
  it('unwraps the server’s message', () => {
    expect(errorDetail(new Error('409: {"detail": "no"}'))).toBe('no');
  });

  it('unwraps a validation error', () => {
    expect(errorDetail(
      new Error('422: {"detail": [{"msg": "String should have at least 1 character"}]}'),
    )).toMatch(/at least 1 character/);
  });

  it('falls back to the raw text when it is not JSON', () => {
    expect(errorDetail(new Error('Failed to fetch'))).toBe('Failed to fetch');
  });
});

describe('a committed write is not a failed one', () => {
  it('reports success when only the follow-up list refresh fails', async () => {
    // Retrying a create that already happened is a username conflict; retrying
    // a password change uses a current password that is no longer current. So
    // "did the write land" and "could the page redraw" have to be different
    // answers.
    api.listAccounts.mockResolvedValueOnce({ accounts: [account()] });
    await useAccountStore.getState().load();

    const created = account({ id: 'acc-2', username: 'bob', is_self: false });
    api.createAccount.mockResolvedValue(created);
    api.listAccounts.mockRejectedValue(new Error('500: {"detail": "gone"}'));

    const ok = await useAccountStore.getState().create({
      username: 'bob', password: 'a-passphrase',
    });

    expect(ok).toBe(true);
    // The row the server returned is on screen even though the list is stale...
    expect(useAccountStore.getState().accounts.map((a) => a.id))
      .toEqual(['acc-1', 'acc-2']);
    // ...and the message says what actually happened.
    expect(useAccountStore.getState().error).toMatch(/Saved/);
    expect(useAccountStore.getState().error).not.toMatch(/Could not create/);
  });

  it('reports failure when the write itself fails', async () => {
    api.createAccount.mockRejectedValue(
      new Error('409: {"detail": "The username is already taken"}'),
    );
    const ok = await useAccountStore.getState().create({
      username: 'bob', password: 'a-passphrase',
    });
    expect(ok).toBe(false);
    expect(useAccountStore.getState().error).toMatch(/already taken/);
  });

  it('applies an updated row from the response, not only from the refresh', async () => {
    api.listAccounts.mockResolvedValueOnce({ accounts: [account()] });
    await useAccountStore.getState().load();

    api.setAccountEnabled.mockResolvedValue(
      account({ enabled: false, disabled_at: '2026-08-06T00:00:00Z' }),
    );
    api.listAccounts.mockRejectedValue(new Error('network'));

    expect(await useAccountStore.getState().setEnabled('acc-1', false)).toBe(true);
    expect(useAccountStore.getState().accounts[0].enabled).toBe(false);
    expect(useAccountStore.getState().busyId).toBeNull();
  });

  it('lets the refresh be retried on its own', async () => {
    api.changeOwnPassword.mockResolvedValue(account());
    api.listAccounts.mockRejectedValueOnce(new Error('network'));
    expect(await useAccountStore.getState().changeOwnPassword({
      new_password: 'a-passphrase',
    })).toBe(true);
    expect(useAccountStore.getState().error).toMatch(/Saved/);

    api.listAccounts.mockResolvedValue({ accounts: [account()] });
    await useAccountStore.getState().load();
    expect(useAccountStore.getState().error).toBeNull();
  });

  it('keeps the add form open only when the write failed', async () => {
    api.listAccounts.mockResolvedValue({ accounts: [account()] });
    api.createAccount.mockResolvedValue(
      account({ id: 'acc-2', username: 'bob', is_self: false }),
    );
    renderPage();
    await screen.findByText('alice');

    await userEvent.click(screen.getAllByRole('button', { name: /Add account/ })[0]);
    const form = screen.getByRole('form', { name: 'Add account' });
    await userEvent.type(within(form).getByLabelText('New account username'), 'bob');
    await userEvent.type(within(form).getByLabelText('New account password'), 'a-passphrase');
    // The list refresh fails *after* the create commits.
    api.listAccounts.mockRejectedValue(new Error('network'));
    await userEvent.click(within(form).getByRole('button', { name: 'Create' }));

    await waitFor(() =>
      expect(screen.queryByRole('form', { name: 'Add account' })).not.toBeInTheDocument());
  });
});
