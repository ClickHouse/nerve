import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Outlet } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { AuthStatus } from './api/client';

/**
 * Where a tab lands on startup.
 *
 * The one that matters is the tab that arrives *holding a token*. A token says
 * it may come in; it does not say where. An instance whose sole account has no
 * password belongs on the setup page however the tab arrived, and the app used
 * to render on the token alone — reaching `/chat` before the status descriptor
 * answered, by which time the component that would have redirected was gone.
 */

vi.mock('./api/client', async () => {
  const actual = await vi.importActual<typeof import('./api/client')>('./api/client');
  return {
    ...actual,
    api: { authStatus: vi.fn(), checkAuth: vi.fn(), login: vi.fn() },
    setToken: vi.fn(),
    clearToken: vi.fn(),
    getToken: vi.fn(() => null),
    setUnauthorizedHandler: vi.fn(),
  };
});
vi.mock('./stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('./stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));
vi.mock('./api/websocket', () => ({
  ws: { connect: vi.fn(), disconnect: vi.fn(), onMessage: vi.fn(() => () => {}) },
}));
vi.mock('./stores/chatStore', () => {
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
vi.mock('./components/Layout/AppShell', () => ({ AppShell: () => <Outlet /> }));
vi.mock('./pages/ChatPage', () => ({ ChatPage: () => <div>the chat page</div> }));
vi.mock('./components/Notifications/NotificationToast', () => ({
  NotificationToast: () => null,
}));

import { api, getToken } from './api/client';
import App from './App';
import { useAuthStore } from './stores/authStore';

const authStatus = api.authStatus as unknown as ReturnType<typeof vi.fn>;
const checkAuth = api.checkAuth as unknown as ReturnType<typeof vi.fn>;
const apiLogin = api.login as unknown as ReturnType<typeof vi.fn>;
const tokenInStorage = getToken as unknown as ReturnType<typeof vi.fn>;

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

function renderApp() {
  return render(<MemoryRouter initialEntries={['/']}><App /></MemoryRouter>);
}

beforeEach(() => {
  vi.clearAllMocks();
  tokenInStorage.mockReturnValue(null);
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

describe('startup with a token already in storage', () => {
  beforeEach(() => {
    tokenInStorage.mockReturnValue('a-stored-token');
    checkAuth.mockResolvedValue({ authenticated: true });
  });

  it('lands on the setup page when the instance has no password', async () => {
    authStatus.mockResolvedValue(
      status({ login: 'none', auth_required: false, setup_pending: true }),
    );

    renderApp();

    expect(await screen.findByText(/Setup is not finished/)).toBeInTheDocument();
    expect(screen.queryByText('the chat page')).not.toBeInTheDocument();
  });

  it('lands on chat when it does', async () => {
    authStatus.mockResolvedValue(status());

    renderApp();

    expect(await screen.findByText('the chat page')).toBeInTheDocument();
  });

  it('renders nothing at all until both answers are in', async () => {
    // Loaded fresh, with the token already in storage, so the store computes
    // its *initial* state the way a real reload does. Resetting the store in a
    // fixture would paper over exactly the bug this is about: the app deciding
    // it was ready because a token existed.
    vi.resetModules();
    let answer: (value: AuthStatus) => void = () => {};
    authStatus.mockReturnValue(new Promise<AuthStatus>((r) => { answer = r; }));
    const { default: FreshApp } = await import('./App');

    const { container } = render(
      <MemoryRouter initialEntries={['/']}><FreshApp /></MemoryRouter>,
    );

    // Not the app, not the login page — the decision has not been made.
    expect(container).toBeEmptyDOMElement();
    expect(screen.queryByText('the chat page')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Password')).not.toBeInTheDocument();

    answer(status({ login: 'none', auth_required: false, setup_pending: true }));
    expect(await screen.findByText(/Setup is not finished/)).toBeInTheDocument();
  });

  it('reaches setup on a real reload, not just with a reset store', async () => {
    // The same fresh-module path, followed all the way through: a grandfathered
    // token on a passwordless install is precisely the upgrading install PR 3
    // has to route to setup, and it is the one shape a reset store hides.
    vi.resetModules();
    authStatus.mockResolvedValue(
      status({ login: 'none', auth_required: false, setup_pending: true }),
    );
    const { default: FreshApp } = await import('./App');

    render(<MemoryRouter initialEntries={['/']}><FreshApp /></MemoryRouter>);

    expect(await screen.findByText(/Setup is not finished/)).toBeInTheDocument();
    expect(screen.queryByText('the chat page')).not.toBeInTheDocument();
  });

  it('shows the login page when the stored token turns out to be dead', async () => {
    authStatus.mockResolvedValue(status());
    checkAuth.mockRejectedValue(new Error('401'));

    renderApp();

    expect(await screen.findByLabelText('Password')).toBeInTheDocument();
  });
});

describe('startup with no token', () => {
  it('auto-logs-in a passwordless install and still lands on setup', async () => {
    authStatus.mockResolvedValue(
      status({ login: 'none', auth_required: false, setup_pending: true }),
    );
    apiLogin.mockResolvedValue({ token: 'a-fresh-token' });

    renderApp();

    expect(await screen.findByText(/Setup is not finished/)).toBeInTheDocument();
    expect(apiLogin).toHaveBeenCalledWith('');
  });

  it('shows the login page when a password is required', async () => {
    authStatus.mockResolvedValue(status());

    renderApp();

    expect(await screen.findByLabelText('Password')).toBeInTheDocument();
    expect(apiLogin).not.toHaveBeenCalled();
  });

  it('shows the login page when the status call fails', async () => {
    authStatus.mockRejectedValue(new Error('network'));

    renderApp();

    // Fail closed: ask, and ask for both, rather than assume a passwordless
    // instance and log in with nothing.
    expect(await screen.findByLabelText('Password')).toBeInTheDocument();
    await waitFor(() =>
      expect(useAuthStore.getState().loginMode).toBe('username_password'));
    expect(apiLogin).not.toHaveBeenCalled();
  });
});
