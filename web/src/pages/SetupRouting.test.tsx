import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Outlet } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * Claiming from the tab that could not sign in, end to end through the app.
 *
 * Where each kind of tab *lands* is `App.test.tsx`; this is the one journey
 * that only the whole app can show. A tab holding a token the server no longer
 * accepts gets the claim form rather than a login form — there is no password
 * to type yet — and once it claims it has to end up **inside**, on the
 * checklist, rather than back at a form asking for the password it just set.
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
const { useSetupStore } = await import('../stores/setupStore');

const authStatus = api.authStatus as unknown as ReturnType<typeof vi.fn>;
const apiLogin = api.login as unknown as ReturnType<typeof vi.fn>;
const getOwnAccount = api.getOwnAccount as unknown as ReturnType<typeof vi.fn>;
const setupState = api.setupState as unknown as ReturnType<typeof vi.fn>;
const setupClaim = api.setupClaim as unknown as ReturnType<typeof vi.fn>;
const listActors = api.listActors as unknown as ReturnType<typeof vi.fn>;
const storedToken = getToken as unknown as ReturnType<typeof vi.fn>;

const UNCLAIMED = { auth_required: false, login: 'none' };
const CLAIMED = { auth_required: true, login: 'password' };

const CHECKLIST = {
  setup_pending: false, lockdown: false, writable: true,
  read_only_reason: null, restart_pending: false, restart_pending_paths: [],
  restart_pending_reasons: [], restart_command: 'nerve restart',
  warning: null, finished: false,
  steps: [{
    id: 'provider', title: 'Provider credential', status: 'pending',
    required: false, can_skip: true, detail: '',
  }],
  crons: [],
  values: {
    timezone: 'UTC', display_name: null, has_anthropic_key: false,
    has_openai_key: false, has_telegram_token: false, sync_github: false,
    sync_gmail: false, sync_telegram: false,
  },
};

beforeEach(() => {
  vi.clearAllMocks();
  storedToken.mockReturnValue(null);
  useSetupStore.setState({ state: null, loading: true, busy: null, error: null });
  useAuthStore.setState({
    authenticated: false, ready: false, sessionExpired: false,
    loginMode: 'password', error: null, loading: false, account: null,
  });
  getOwnAccount.mockRejectedValue(new Error('401: Unauthorized'));
  listActors.mockResolvedValue({ actors: [] });
  setupState.mockResolvedValue(CHECKLIST);
});

describe('claiming from a tab that has no usable session', () => {
  it('lands on the checklist, signed in, rather than back at a form', async () => {
    const userEvent = (await import('@testing-library/user-event')).default;
    authStatus.mockResolvedValueOnce(UNCLAIMED).mockResolvedValue(CLAIMED);
    // The auto-login a passwordless install would do, refused: what a tab
    // holding a token the server no longer accepts ends up doing.
    apiLogin.mockRejectedValue(new Error('401: Unauthorized'));
    setupClaim.mockResolvedValue({ token: 'a-fresh-session' });

    render(<MemoryRouter initialEntries={['/']}><App /></MemoryRouter>);

    // The claim form, not the login form: there is no password to type yet,
    // so what it asks for is the one to *set* — beside the mandatory token.
    await screen.findByRole('form', { name: 'Claim this instance' });
    expect(screen.getByLabelText('Setup token')).toBeRequired();

    await userEvent.type(screen.getByLabelText('Username'), 'alice');
    await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
    await userEvent.type(screen.getByLabelText('Setup token'), 'the-setup-token');
    // From here the stored token is what a signed-in tab would have.
    getOwnAccount.mockResolvedValue({
      id: 'acc-1', actor_id: 'actor-1', username: 'alice', display_name: null,
      enabled: true, has_password: true, created_at: 't',
    });
    storedToken.mockReturnValue('a-fresh-session');
    await userEvent.click(screen.getByRole('button', { name: /claim and sign in/i }));

    await waitFor(() => expect(useAuthStore.getState().authenticated).toBe(true));
    expect(
      await screen.findByRole('region', { name: 'Provider credential' }),
    ).toBeTruthy();
    expect(screen.queryByRole('form', { name: 'Claim this instance' })).toBeNull();
  });
});
