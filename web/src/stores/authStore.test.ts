import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { AuthStatus } from '../api/client';

vi.mock('../api/client', () => ({
  api: {
    login: vi.fn(),
    authStatus: vi.fn(),
    getOwnAccount: vi.fn(),
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
  authStatus: ReturnType<typeof vi.fn>;
  getOwnAccount: ReturnType<typeof vi.fn>;
};

/** The signed-in account, as `/api/accounts/me` returns it. */
function me(overrides: Record<string, unknown> = {}) {
  return {
    id: 'acc-1', actor_id: 'actor-1', username: 'alice', display_name: 'Alice',
    enabled: true, has_password: true, created_at: 't', updated_at: 't',
    disabled_at: null, is_self: true, ...overrides,
  };
}
const getToken = client.getToken as unknown as ReturnType<typeof vi.fn>;
const setToken = client.setToken as unknown as ReturnType<typeof vi.fn>;
const clearToken = client.clearToken as unknown as ReturnType<typeof vi.fn>;
const { clearAllDrafts } = await import('./helpers/draftStorage');
const { clearAllReads } = await import('./helpers/readStorage');

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
    account: null,
  });
  api.getOwnAccount.mockResolvedValue(me());
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
      api.getOwnAccount.mockResolvedValue(me());
      api.authStatus.mockResolvedValue(
        status({ login: 'username_password', multiple_accounts: true }),
      );

      await useAuthStore.getState().checkAuth();

      expect(api.authStatus).toHaveBeenCalled();
      expect(useAuthStore.getState().loginMode).toBe('username_password');
      expect(useAuthStore.getState().authenticated).toBe(true);
    });

  it('falls through to the no-token path when its token is dead', async () => {
    // A dead token means this tab holds nothing usable, which is the same
    // position as holding no token at all — so it belongs on the same path. It
    // used to stop at "clear the token", landing a passwordless install on a
    // login form it does not need and cannot use.
    getToken.mockReturnValue('a-dead-token');
    api.getOwnAccount.mockRejectedValue(new Error('401'));
    api.authStatus.mockResolvedValue(
      status({ login: 'none', auth_required: false, setup_pending: true }),
    );
    api.login.mockResolvedValue({ token: 'a-fresh-token' });

    await useAuthStore.getState().checkAuth();

    expect(api.login).toHaveBeenCalledWith('');
    expect(useAuthStore.getState().authenticated).toBe(true);
    expect(useAuthStore.getState().setupPending).toBe(true);
    expect(useAuthStore.getState().sessionExpired).toBe(false);
  });

  it('still shows the login page when a dead token meets a password', async () => {
    getToken.mockReturnValue('a-dead-token');
    api.getOwnAccount.mockRejectedValue(new Error('401'));
    api.authStatus.mockResolvedValue(status({ login: 'password' }));

    await useAuthStore.getState().checkAuth();

    expect(api.login).not.toHaveBeenCalled();
    expect(useAuthStore.getState().authenticated).toBe(false);
    expect(useAuthStore.getState().ready).toBe(true);
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


describe('who the session belongs to', () => {
  it('is read at startup and after signing in', async () => {
    getToken.mockReturnValue('a-token');
    api.authStatus.mockResolvedValue(status());
    api.getOwnAccount.mockResolvedValue(me({ id: 'acc-7', username: 'bob' }));

    await useAuthStore.getState().checkAuth();

    expect(useAuthStore.getState().account).toEqual({ id: 'acc-7', username: 'bob' });
  });

  it('is cleared on logout, along with everything account-scoped', async () => {
    useAuthStore.setState({ account: { id: 'acc-1', username: 'alice' } });
    api.authStatus.mockResolvedValue(status());

    useAuthStore.getState().logout();

    expect(useAuthStore.getState().account).toBeNull();
    expect(clearAllDrafts).toHaveBeenCalled();
    expect(clearAllReads).toHaveBeenCalled();
  });

  it('signs out rather than in when a different account unlocks a mounted app',
    async () => {
      // Unreachable through the overlay, which binds to one account — this is
      // the belt. If it ever happens, nothing of the previous account may
      // survive, so it is a sign-out and not a sign-in.
      useAuthStore.setState({
        account: { id: 'acc-1', username: 'alice' },
        sessionExpired: true,
        authenticated: false,
      });
      api.login.mockResolvedValue({ token: 'bobs-token' });
      api.getOwnAccount.mockResolvedValue(me({ id: 'acc-2', username: 'bob' }));

      await useAuthStore.getState().login('a-passphrase', 'bob');

      expect(useAuthStore.getState().authenticated).toBe(false);
      expect(useAuthStore.getState().account).toBeNull();
      expect(useAuthStore.getState().error).toMatch(/different account/);
      expect(clearAllDrafts).toHaveBeenCalled();
      expect(clearToken).toHaveBeenCalled();
    });

  it('keeps the app when the same account unlocks it', async () => {
    useAuthStore.setState({
      account: { id: 'acc-1', username: 'alice' },
      sessionExpired: true,
      authenticated: false,
    });
    api.login.mockResolvedValue({ token: 'a-fresh-token' });
    api.getOwnAccount.mockResolvedValue(me({ id: 'acc-1', username: 'alice' }));

    await useAuthStore.getState().login('a-passphrase', 'alice');

    expect(useAuthStore.getState().authenticated).toBe(true);
    expect(useAuthStore.getState().sessionExpired).toBe(false);
    expect(clearAllDrafts).not.toHaveBeenCalled();
  });

  it('does not purge on an ordinary cold sign-in', async () => {
    api.login.mockResolvedValue({ token: 'a-fresh-token' });
    api.getOwnAccount.mockResolvedValue(me({ id: 'acc-2', username: 'bob' }));

    await useAuthStore.getState().login('a-passphrase', 'bob');

    expect(useAuthStore.getState().authenticated).toBe(true);
    expect(clearAllDrafts).not.toHaveBeenCalled();
  });
});

describe('overlapping status refreshes', () => {
  it('applies the newest request, not the last response', async () => {
    // A 401, a refused sign-in and an overlay mounting can all fire one of
    // these. An older answer arriving last used to restore `password` after a
    // newer one had said `username_password`, leaving a form that can only
    // fail.
    let resolveFirst: (v: unknown) => void = () => {};
    api.authStatus.mockReturnValueOnce(new Promise((r) => { resolveFirst = r; }));
    const first = useAuthStore.getState().refreshStatus();

    api.authStatus.mockResolvedValueOnce(
      status({ login: 'username_password', multiple_accounts: true }),
    );
    await useAuthStore.getState().refreshStatus();
    expect(useAuthStore.getState().loginMode).toBe('username_password');

    // The stale one lands afterwards and must change nothing.
    resolveFirst(status({ login: 'password' }));
    await first;
    expect(useAuthStore.getState().loginMode).toBe('username_password');
    expect(useAuthStore.getState().statusLoading).toBe(false);
  });

  it('a stale failure does not clear a newer answer either', async () => {
    let rejectFirst: (e: unknown) => void = () => {};
    api.authStatus.mockReturnValueOnce(new Promise((_r, reject) => { rejectFirst = reject; }));
    const first = useAuthStore.getState().refreshStatus();

    api.authStatus.mockResolvedValueOnce(status({ login: 'none' }));
    await useAuthStore.getState().refreshStatus();

    rejectFirst(new Error('network'));
    await first;
    expect(useAuthStore.getState().loginMode).toBe('none');
  });
});
