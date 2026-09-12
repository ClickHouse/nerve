import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { AuthStatus } from '../api/client';

vi.mock('../api/client', () => ({
  api: {
    login: vi.fn(),
    checkAuth: vi.fn(),
    authStatus: vi.fn(),
  },
  setToken: vi.fn(),
  clearToken: vi.fn(),
  getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

vi.mock('./helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('./helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const client = await import('../api/client');
const { useAuthStore } = await import('./authStore');

const api = client.api as unknown as {
  login: ReturnType<typeof vi.fn>;
  checkAuth: ReturnType<typeof vi.fn>;
  authStatus: ReturnType<typeof vi.fn>;
};
const getToken = client.getToken as unknown as ReturnType<typeof vi.fn>;
const setToken = client.setToken as unknown as ReturnType<typeof vi.fn>;

function status(overrides: Partial<AuthStatus> = {}): AuthStatus {
  return {
    auth_required: true,
    mode: 'local',
    login: 'password',
    setup_pending: false,
    multiple_accounts: false,
    ...overrides,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  useAuthStore.setState({
    authenticated: false,
    loading: false,
    ready: false,
    error: null,
    sessionExpired: false,
    loginMode: null,
    statusLoading: false,
    setupPending: false,
  });
});

describe('checkAuth: where a cold start lands', () => {
  it('auto-logs-in only when the instance is passwordless', async () => {
    getToken.mockReturnValue(null);
    api.authStatus.mockResolvedValue(status({ login: 'none', auth_required: false }));
    api.login.mockResolvedValue({ token: 'a-token' });

    await useAuthStore.getState().checkAuth();

    expect(api.login).toHaveBeenCalledWith('');
    expect(setToken).toHaveBeenCalledWith('a-token');
    expect(useAuthStore.getState().authenticated).toBe(true);
    expect(useAuthStore.getState().ready).toBe(true);
  });

  it('never auto-logs-in on a single account that has a password', async () => {
    getToken.mockReturnValue(null);
    api.authStatus.mockResolvedValue(status({ login: 'password' }));

    await useAuthStore.getState().checkAuth();

    expect(api.login).not.toHaveBeenCalled();
    expect(useAuthStore.getState().authenticated).toBe(false);
    expect(useAuthStore.getState().loginMode).toBe('password');
  });

  it('never auto-logs-in once a second account exists', async () => {
    getToken.mockReturnValue(null);
    api.authStatus.mockResolvedValue(
      status({ login: 'username_password', multiple_accounts: true }),
    );

    await useAuthStore.getState().checkAuth();

    expect(api.login).not.toHaveBeenCalled();
    expect(useAuthStore.getState().loginMode).toBe('username_password');
  });

  it('asks rather than assumes when the status call fails', async () => {
    getToken.mockReturnValue(null);
    api.authStatus.mockRejectedValue(new Error('network'));

    await useAuthStore.getState().checkAuth();

    // The dangerous default would be 'none', which logs itself in. Failing
    // closed means asking for both fields, which a single-account install
    // still accepts with the username left blank.
    expect(api.login).not.toHaveBeenCalled();
    expect(useAuthStore.getState().loginMode).toBe('username_password');
    expect(useAuthStore.getState().authenticated).toBe(false);
    expect(useAuthStore.getState().ready).toBe(true);
  });

  it('falls through to the login page when the auto-login is refused', async () => {
    getToken.mockReturnValue(null);
    api.authStatus.mockResolvedValue(status({ login: 'none' }));
    api.login.mockRejectedValue(new Error('401'));

    await useAuthStore.getState().checkAuth();

    expect(useAuthStore.getState().authenticated).toBe(false);
    expect(useAuthStore.getState().ready).toBe(true);
  });

  it('routes an unset-up instance to /setup', async () => {
    getToken.mockReturnValue(null);
    api.authStatus.mockResolvedValue(
      status({ login: 'none', auth_required: false, setup_pending: true }),
    );
    api.login.mockResolvedValue({ token: 'a-token' });

    await useAuthStore.getState().checkAuth();

    // Signed in — an abandoned setup still leaves a working instance — and
    // flagged, which is what moves the root redirect.
    expect(useAuthStore.getState().authenticated).toBe(true);
    expect(useAuthStore.getState().setupPending).toBe(true);
  });
});

describe('checkAuth: a tab that already holds a token', () => {
  it('still reads the descriptor, so the expiry overlay asks for the right fields',
    async () => {
      getToken.mockReturnValue('an-existing-token');
      api.checkAuth.mockResolvedValue({ authenticated: true });
      api.authStatus.mockResolvedValue(
        status({ login: 'username_password', multiple_accounts: true }),
      );

      await useAuthStore.getState().checkAuth();

      expect(api.authStatus).toHaveBeenCalled();
      expect(useAuthStore.getState().loginMode).toBe('username_password');
      expect(useAuthStore.getState().authenticated).toBe(true);
    });

  it('does not auto-login when its token turns out to be dead', async () => {
    getToken.mockReturnValue('a-dead-token');
    api.checkAuth.mockRejectedValue(new Error('401'));
    api.authStatus.mockResolvedValue(status({ login: 'none' }));

    await useAuthStore.getState().checkAuth();

    expect(api.login).not.toHaveBeenCalled();
    expect(useAuthStore.getState().authenticated).toBe(false);
  });
});

describe('login', () => {
  it('sends the username alongside the password', async () => {
    api.login.mockResolvedValue({ token: 't' });
    await useAuthStore.getState().login('a-passphrase', 'alice');
    expect(api.login).toHaveBeenCalledWith('a-passphrase', 'alice');
    expect(useAuthStore.getState().authenticated).toBe(true);
  });

  it('sends no username when the form did not collect one', async () => {
    api.login.mockResolvedValue({ token: 't' });
    await useAuthStore.getState().login('a-passphrase');
    expect(api.login).toHaveBeenCalledWith('a-passphrase', undefined);
  });

  it('reports a failure without authenticating', async () => {
    api.login.mockRejectedValue(new Error('Unauthorized'));
    await useAuthStore.getState().login('wrong', 'alice');
    expect(useAuthStore.getState().authenticated).toBe(false);
    expect(useAuthStore.getState().error).toBe('Unauthorized');
    expect(useAuthStore.getState().loading).toBe(false);
  });
});

describe('refreshStatus', () => {
  it('picks up the change when a second account is added', async () => {
    api.authStatus.mockResolvedValue(
      status({ login: 'username_password', multiple_accounts: true }),
    );
    await useAuthStore.getState().refreshStatus();
    expect(useAuthStore.getState().loginMode).toBe('username_password');
  });

  it('keeps the last known shape when the call fails', async () => {
    useAuthStore.setState({ loginMode: 'password' });
    api.authStatus.mockRejectedValue(new Error('network'));
    await useAuthStore.getState().refreshStatus();
    expect(useAuthStore.getState().loginMode).toBe('password');
  });

  it('fails closed when it has never had an answer', async () => {
    api.authStatus.mockRejectedValue(new Error('network'));
    await useAuthStore.getState().refreshStatus();
    // Never null — the form would be stranded on its loading state — and never
    // 'none', which is the value that logs the app in without asking.
    expect(useAuthStore.getState().loginMode).toBe('username_password');
    expect(useAuthStore.getState().statusLoading).toBe(false);
  });
});
