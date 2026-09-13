import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Outlet } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * Where a tab lands when the instance has never been claimed.
 *
 * Two arrivals, one destination. A tab that is signed in — which on a
 * passwordless install is every tab, because the app logs itself in with an
 * empty password — is redirected from the root to `/setup`. A tab that is
 * *not* signed in gets the setup page anyway rather than a login form: the
 * claim endpoint is unauthenticated (the state it ends admits everybody), and
 * asking for a password that does not exist yet is a dead end. The case that
 * produces it is the tab that arrives holding a token the server no longer
 * accepts.
 */

vi.mock('../api/client', async () => {
  const actual = await vi.importActual<typeof import('../api/client')>('../api/client');
  return {
    ...actual,
    api: {
      authStatus: vi.fn(),
      login: vi.fn(),
      getOwnAccount: vi.fn(),
      setupState: vi.fn(),
      setupClaim: vi.fn(),
      me: vi.fn(),
      listActors: vi.fn(),
    },
    setToken: vi.fn(),
    clearToken: vi.fn(),
    getToken: vi.fn(() => null),
    setUnauthorizedHandler: vi.fn(),
  };
});
vi.mock('../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));
vi.mock('../api/websocket', () => ({
  ws: { connect: vi.fn(), disconnect: vi.fn(), onMessage: vi.fn(() => () => {}) },
}));
vi.mock('../stores/chatStore', () => {
  const state = {
    handleWSMessage: vi.fn(),
    loadSessions: vi.fn(),
    revealSessionList: vi.fn(),
    requestSearchFocus: vi.fn(),
    createSession: vi.fn(),
    stopSession: vi.fn(),
    isStreaming: false,
  };
  const hook = (() => state) as unknown as { (): typeof state; getState: () => typeof state };
  hook.getState = () => state;
  return { useChatStore: hook };
});
// The shell and the chat page are heavy and are not what this is about; the
// shell is replaced by its outlet so routing still resolves through it.
vi.mock('../components/Layout/AppShell', () => ({ AppShell: () => <Outlet /> }));
vi.mock('../pages/ChatPage', () => ({ ChatPage: () => <div>the chat page</div> }));
vi.mock('../components/Notifications/NotificationToast', () => ({
  NotificationToast: () => null,
}));

const { api, getToken } = await import('../api/client');
const App = (await import('../App')).default;
const { useAuthStore } = await import('../stores/authStore');

const authStatus = api.authStatus as unknown as ReturnType<typeof vi.fn>;
const apiLogin = api.login as unknown as ReturnType<typeof vi.fn>;
const getOwnAccount = api.getOwnAccount as unknown as ReturnType<typeof vi.fn>;
const setupState = api.setupState as unknown as ReturnType<typeof vi.fn>;
const setupClaim = api.setupClaim as unknown as ReturnType<typeof vi.fn>;
const me = api.me as unknown as ReturnType<typeof vi.fn>;
const listActors = api.listActors as unknown as ReturnType<typeof vi.fn>;

const UNCLAIMED = {
  auth_required: false, mode: 'local', login: 'none',
  setup_pending: true, multiple_accounts: false,
};
const CLAIMED = {
  auth_required: true, mode: 'local', login: 'password',
  setup_pending: false, multiple_accounts: false,
};

function renderApp(path = '/') {
  return render(<MemoryRouter initialEntries={[path]}><App /></MemoryRouter>);
}

beforeEach(() => {
  vi.clearAllMocks();
  (getToken as unknown as ReturnType<typeof vi.fn>).mockReturnValue(null);
  useAuthStore.setState({
    authenticated: false, ready: false, sessionExpired: false,
    loginMode: 'password', setupPending: false, error: null, loading: false,
    account: null,
  });
  getOwnAccount.mockRejectedValue(new Error('401: Unauthorized'));
  setupState.mockResolvedValue({
    setup_pending: true, lockdown: false, writable: true,
    read_only_reason: null, restart_pending: false, restart_pending_paths: [],
    finished: false, steps: [], crons: [],
  });
});

describe('an unclaimed instance', () => {
  it('sends a signed-in tab to the setup page', async () => {
    authStatus.mockResolvedValue(UNCLAIMED);
    apiLogin.mockResolvedValue({ token: 'a-passwordless-session' });

    renderApp('/');

    expect(
      await screen.findByRole('form', { name: 'Claim this instance' }),
    ).toBeTruthy();
    expect(screen.queryByText('the chat page')).toBeNull();
  });

  it('shows the claim form instead of a login form with no session', async () => {
    authStatus.mockResolvedValue(UNCLAIMED);
    // The auto-login a passwordless install would do, refused: what a tab
    // holding a token the server no longer accepts ends up doing.
    apiLogin.mockRejectedValue(new Error('401: Unauthorized'));

    renderApp('/');

    expect(
      await screen.findByRole('form', { name: 'Claim this instance' }),
    ).toBeTruthy();
    // The claim form, not the login form: there is no password to type yet,
    // so what it asks for is the one to *set*, beside a username and the
    // optional setup token.
    expect(screen.getByLabelText('Setup token')).toBeTruthy();
    expect(screen.getByRole('button', { name: /claim and sign in/i })).toBeTruthy();
  });

  it('claiming from that tab leaves it signed in, not back at a form', async () => {
    const userEvent = (await import('@testing-library/user-event')).default;
    authStatus.mockResolvedValueOnce(UNCLAIMED).mockResolvedValue(CLAIMED);
    apiLogin.mockRejectedValue(new Error('401: Unauthorized'));
    getOwnAccount.mockResolvedValue({
      id: 'acc-1', actor_id: 'actor-1', username: 'alice', display_name: null,
      enabled: true, has_password: true, created_at: '', updated_at: '',
      disabled_at: null, is_self: true,
    });
    listActors.mockResolvedValue({ actors: [] });
    me.mockResolvedValue({
      actor_id: 'actor-1', account_id: 'acc-1', username: 'alice',
      display_name: null, kind: 'human',
    });
    setupClaim.mockResolvedValue({
      token: 'a-fresh-session', account_id: 'acc-1', actor_id: 'actor-1',
      username: 'alice', display_name: null,
    });
    setupState.mockResolvedValue({
      setup_pending: false, lockdown: false, writable: true,
      read_only_reason: null, restart_pending: false, restart_pending_paths: [],
      restart_pending_reasons: [], warning: null, finished: true,
      steps: [], crons: [],
      values: {
        timezone: 'UTC', display_name: null, has_anthropic_key: false,
        has_openai_key: false, has_telegram_token: false, sync_github: false,
        sync_gmail: false, sync_telegram: false,
      },
    });
    (getToken as unknown as ReturnType<typeof vi.fn>).mockReturnValue(null);

    renderApp('/');
    await screen.findByRole('form', { name: 'Claim this instance' });
    await userEvent.type(screen.getByLabelText('Username'), 'alice');
    await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
    // From here the stored token is what a signed-in tab would have.
    (getToken as unknown as ReturnType<typeof vi.fn>).mockReturnValue('a-fresh-session');
    await userEvent.click(screen.getByRole('button', { name: /claim and sign in/i }));

    await waitFor(() => expect(useAuthStore.getState().authenticated).toBe(true));
    // Not the login page: the password it just set is not something to retype.
    expect(screen.queryByRole('form', { name: 'Claim this instance' })).toBeNull();
  });
});

describe('a claimed instance', () => {
  it('opens the app, not the wizard', async () => {
    authStatus.mockResolvedValue(CLAIMED);
    (getToken as unknown as ReturnType<typeof vi.fn>).mockReturnValue('a-session');
    getOwnAccount.mockResolvedValue({
      id: 'acc-1', actor_id: 'actor-1', username: 'alice', display_name: null,
      enabled: true, has_password: true, created_at: '', updated_at: '',
      disabled_at: null, is_self: true,
    });

    renderApp('/');

    expect(await screen.findByText('the chat page')).toBeTruthy();
    expect(screen.queryByRole('form', { name: 'Claim this instance' })).toBeNull();
  });

  it('shows the login page when there is no session', async () => {
    authStatus.mockResolvedValue(CLAIMED);

    renderApp('/');

    await waitFor(() => expect(useAuthStore.getState().ready).toBe(true));
    expect(screen.queryByRole('form', { name: 'Claim this instance' })).toBeNull();
    expect(screen.queryByText('the chat page')).toBeNull();
  });
});
