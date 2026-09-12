import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { LoginPage } from './LoginPage';
import { SessionExpiredOverlay } from './SessionExpiredOverlay';
import { useAuthStore } from '../../stores/authStore';

/**
 * The login form has two shapes, and which one it takes is the server's
 * answer, not a guess. The one that matters most is the *unchanged* one: an
 * install that upgrades keeps typing a password into the same box and is never
 * shown a field for a username it does not have.
 */

const login = vi.fn();

beforeEach(() => {
  vi.clearAllMocks();
  useAuthStore.setState({
    authenticated: false,
    loading: false,
    checking: false,
    error: null,
    sessionExpired: false,
    loginMode: 'password',
    setupPending: false,
    login,
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
