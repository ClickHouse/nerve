import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../api/client', async () => {
  const actual = await vi.importActual<typeof import('../../api/client')>(
    '../../api/client',
  );
  return {
    ...actual,
    api: { authStatus: vi.fn(), login: vi.fn(), checkAuth: vi.fn() },
    setToken: vi.fn(),
    clearToken: vi.fn(),
    getToken: vi.fn(() => null),
    setUnauthorizedHandler: vi.fn(),
  };
});
vi.mock('../../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

import { api } from '../../api/client';
import { LoginPage } from './LoginPage';
import { SessionExpiredOverlay } from './SessionExpiredOverlay';
import { useAuthStore } from '../../stores/authStore';

const authStatus = api.authStatus as unknown as ReturnType<typeof vi.fn>;
const apiLogin = api.login as unknown as ReturnType<typeof vi.fn>;

// The store's own implementations, captured before any test replaces them.
const realRefreshStatus = useAuthStore.getState().refreshStatus;
const realLogin = useAuthStore.getState().login;
const realLogout = useAuthStore.getState().logout;

/**
 * The login form has two shapes, and which one it takes is the server's
 * answer, not a guess. The one that matters most is the *unchanged* one: an
 * install that upgrades keeps typing a password into the same box and is never
 * shown a field for a username it does not have.
 */

const login = vi.fn();
const refreshStatus = vi.fn().mockResolvedValue(undefined);

beforeEach(() => {
  vi.clearAllMocks();
  useAuthStore.setState({
    authenticated: false,
    loading: false,
    ready: true,
    error: null,
    sessionExpired: false,
    loginMode: 'password',
    statusLoading: false,
    setupPending: false,
    login,
    refreshStatus,
  });
});

describe('LoginPage', () => {
  it('shows only a password field while there is one account', async () => {
    render(<LoginPage />);

    expect(screen.queryByLabelText('Username')).not.toBeInTheDocument();
    await userEvent.type(screen.getByLabelText('Password'), 'a-passphrase');
    await userEvent.click(screen.getByRole('button', { name: 'Login' }));

    expect(login).toHaveBeenCalledWith('a-passphrase', '');
  });

  it('shows both fields once a username is required', async () => {
    useAuthStore.setState({ loginMode: 'username_password' });
    render(<LoginPage />);

    await userEvent.type(screen.getByLabelText('Username'), 'alice');
    await userEvent.type(screen.getByLabelText('Password'), 'a-passphrase');
    await userEvent.click(screen.getByRole('button', { name: 'Login' }));

    expect(login).toHaveBeenCalledWith('a-passphrase', 'alice');
  });

  it('asks for a password even on a passwordless install', () => {
    // `login: 'none'` normally never reaches the form — the store logs itself
    // in — but if that failed, the page must still be usable rather than blank.
    useAuthStore.setState({ loginMode: 'none' });
    render(<LoginPage />);
    expect(screen.getByLabelText('Password')).toBeInTheDocument();
    expect(screen.queryByLabelText('Username')).not.toBeInTheDocument();
  });

  it('renders the error the store is holding', () => {
    useAuthStore.setState({ error: 'Invalid username or password' });
    render(<LoginPage />);
    expect(screen.getByText('Invalid username or password')).toBeInTheDocument();
  });
});

describe('SessionExpiredOverlay', () => {
  it('asks for the same fields the login page would', async () => {
    useAuthStore.setState({ loginMode: 'username_password' });
    render(<SessionExpiredOverlay />);

    await userEvent.type(screen.getByLabelText('Username'), 'bob');
    await userEvent.type(screen.getByLabelText('Password'), 'another-passphrase');
    await userEvent.click(screen.getByRole('button', { name: 'Unlock' }));

    expect(login).toHaveBeenCalledWith('another-passphrase', 'bob');
  });

  it('keeps the single-account shape unchanged', () => {
    render(<SessionExpiredOverlay />);
    expect(screen.queryByLabelText('Username')).not.toBeInTheDocument();
    expect(screen.getByLabelText('Password')).toBeInTheDocument();
  });
});

describe('the form does not go stale', () => {
  beforeEach(() => {
    // The real refresh, over a mocked client, for the staleness tests only.
    useAuthStore.setState({ refreshStatus: realRefreshStatus });
  });

  it('re-reads the descriptor on mount and picks up the second account', async () => {
    // The tab was left on a password-only form; meanwhile somebody else added
    // an account, and the server now requires a username.
    useAuthStore.setState({ loginMode: 'password' });
    authStatus.mockResolvedValue({
      auth_required: true, mode: 'local', login: 'username_password',
      setup_pending: false, multiple_accounts: true,
    });

    render(<LoginPage />);

    expect(await screen.findByLabelText('Username')).toBeInTheDocument();
    expect(authStatus).toHaveBeenCalled();
  });

  it('shows a loading line rather than guessing before the first answer', async () => {
    useAuthStore.setState({ loginMode: null });
    let resolve: (value: unknown) => void = () => {};
    authStatus.mockReturnValue(new Promise((r) => { resolve = r; }));

    render(<LoginPage />);

    expect(screen.getByRole('status')).toHaveTextContent(/Checking how to sign in/);
    expect(screen.queryByLabelText('Password')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Login' })).toBeDisabled();

    resolve({
      auth_required: true, mode: 'local', login: 'password',
      setup_pending: false, multiple_accounts: false,
    });
    expect(await screen.findByLabelText('Password')).toBeInTheDocument();
    expect(screen.queryByLabelText('Username')).not.toBeInTheDocument();
  });

  it('asks for both when it has never had an answer', async () => {
    useAuthStore.setState({ loginMode: null });
    authStatus.mockRejectedValue(new Error('network'));

    render(<LoginPage />);

    // Fail closed. A single-account install still accepts a blank username, so
    // the cost of being wrong this way is a field, not a login.
    expect(await screen.findByLabelText('Username')).toBeInTheDocument();
    expect(screen.getByLabelText('Password')).toBeInTheDocument();
  });

  it('re-reads it when a sign-in is refused', async () => {
    useAuthStore.setState({ loginMode: 'password', login: realLogin });
    apiLogin.mockRejectedValue(new Error('401: Unauthorized'));
    authStatus.mockResolvedValue({
      auth_required: true, mode: 'local', login: 'username_password',
      setup_pending: false, multiple_accounts: true,
    });

    render(<LoginPage />);
    await userEvent.type(await screen.findByLabelText('Password'), 'a-passphrase');
    await userEvent.click(screen.getByRole('button', { name: 'Login' }));

    // The refusal is the moment to discover the form was asking for the wrong
    // thing, rather than letting somebody retype it forever.
    expect(await screen.findByLabelText('Username')).toBeInTheDocument();
  });

  it('re-reads it when the overlay appears', async () => {
    useAuthStore.setState({ loginMode: 'password' });
    authStatus.mockResolvedValue({
      auth_required: true, mode: 'local', login: 'username_password',
      setup_pending: false, multiple_accounts: true,
    });

    render(<SessionExpiredOverlay />);

    expect(await screen.findByLabelText('Username')).toBeInTheDocument();
  });

  it('re-reads it on logout', async () => {
    useAuthStore.setState({ loginMode: 'password', logout: realLogout });
    authStatus.mockResolvedValue({
      auth_required: true, mode: 'local', login: 'username_password',
      setup_pending: false, multiple_accounts: true,
    });

    render(<SessionExpiredOverlay />);
    await userEvent.click(
      await screen.findByRole('button', { name: /Log out/ }),
    );

    await waitFor(() =>
      expect(useAuthStore.getState().loginMode).toBe('username_password'));
  });
});
