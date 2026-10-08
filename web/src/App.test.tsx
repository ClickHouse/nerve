import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Outlet } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { AuthStatus } from './api/client';

/** Startup routing waits for auth status even when a stored token exists. */

vi.mock('./api/client', async () => {
  const actual = await vi.importActual<typeof import('./api/client')>('./api/client');
  return {
    ...actual,
    api: {
      authStatus: vi.fn(), getViewer: vi.fn(), login: vi.fn(),
      listActors: vi.fn().mockResolvedValue({ actors: [] }),
    },
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
vi.mock('./pages/AccountsPage', () => ({
  AccountsPage: () => <div>the accounts page</div>,
}));
vi.mock('./components/Notifications/NotificationToast', () => ({
  NotificationToast: () => null,
}));

import { api, getToken } from './api/client';
import {
  ApiError, handleHostedError, reportError, showProblem, useHostedStore,
} from './api/hosted';
import { ws } from './api/websocket';
import App from './App';
import { useAuthStore } from './stores/authStore';
import { useChatStore } from './stores/chatStore';

const authStatus = api.authStatus as unknown as ReturnType<typeof vi.fn>;
const getViewer = api.getViewer as unknown as ReturnType<typeof vi.fn>;
const apiLogin = api.login as unknown as ReturnType<typeof vi.fn>;
const tokenInStorage = getToken as unknown as ReturnType<typeof vi.fn>;

function me() {
  return {
    actor: { id: 'actor-1', kind: 'human', display_name: 'Alice' },
    account: {
      id: 'acc-1', actor_id: 'actor-1', username: 'alice', display_name: 'Alice',
      enabled: true, has_password: true, created_at: 't',
    },
  };
}

function status(overrides: Partial<AuthStatus> = {}): AuthStatus {
  return {
    auth_required: true,
    login: 'password',
    ...overrides,
  };
}

function renderApp() {
  return render(<MemoryRouter initialEntries={['/']}><App /></MemoryRouter>);
}

beforeEach(() => {
  vi.clearAllMocks();
  tokenInStorage.mockReturnValue(null);
  getViewer.mockResolvedValue(me());
  useAuthStore.setState({
    authenticated: false,
    loading: false,
    ready: false,
    error: null,
    sessionExpired: false,
    loginMode: null,
    viewer: null,
    account: null,
  });
});

describe('startup with a token already in storage', () => {
  beforeEach(() => {
    tokenInStorage.mockReturnValue('a-stored-token');
    getViewer.mockResolvedValue(me());
  });

  it('lands on the setup page while setup is required', async () => {
    authStatus.mockResolvedValue(status({ login: 'setup' }));

    renderApp();

    expect(await screen.findByRole('form', { name: 'Claim this instance' })).toBeInTheDocument();
    expect(screen.queryByText('the chat page')).not.toBeInTheDocument();
  });

  it('lands on chat when the instance is passwordless by choice', async () => {
    authStatus.mockResolvedValue(
      status({ login: 'none', auth_required: false }),
    );

    renderApp();

    expect(await screen.findByText('the chat page')).toBeInTheDocument();
  });

  it('lands on chat when it has a password', async () => {
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
    expect(ws.connect).not.toHaveBeenCalled();

    answer(status({ login: 'setup' }));
    expect(await screen.findByRole('form', { name: 'Claim this instance' })).toBeInTheDocument();
  });

  it('reaches setup on a real reload, not just with a reset store', async () => {
    // Exercise the fresh-module path with a token on an instance that
    // requires setup. A reset store hides this state.
    vi.resetModules();
    authStatus.mockResolvedValue(status({ login: 'setup' }));
    const { default: FreshApp } = await import('./App');

    render(<MemoryRouter initialEntries={['/']}><FreshApp /></MemoryRouter>);

    expect(await screen.findByRole('form', { name: 'Claim this instance' })).toBeInTheDocument();
    expect(screen.queryByText('the chat page')).not.toBeInTheDocument();
  });

  it('shows the login page when the stored token turns out to be dead', async () => {
    // Fresh module: whether a tab has ever *held* a working session is a fact
    // about the page load, and it is what decides between the login page and
    // the expiry overlay. A reused module carries the previous test's answer.
    vi.resetModules();
    authStatus.mockResolvedValue(status());
    getViewer.mockRejectedValue(new Error('401'));
    const { default: FreshApp } = await import('./App');

    render(<MemoryRouter initialEntries={['/']}><FreshApp /></MemoryRouter>);

    expect(await screen.findByLabelText('Password')).toBeInTheDocument();
    expect(screen.queryByText('the chat page')).not.toBeInTheDocument();
  });

  it('a dead token on a passwordless install still reaches the app', async () => {
    // The token is useless, which puts this tab exactly where a tab with no
    // token at all stands — so it takes the same path, rather than stopping at
    // a login form a passwordless install has no answer for.
    authStatus.mockResolvedValue(
      status({ login: 'none', auth_required: false }),
    );
    getViewer.mockRejectedValueOnce(new Error('401'));
    apiLogin.mockResolvedValue({ token: 'a-fresh-token' });

    renderApp();

    expect(await screen.findByText('the chat page')).toBeInTheDocument();
    expect(apiLogin).toHaveBeenCalledWith('');
  });

  it('opens the app when the descriptor cannot be read but the token is good',
    async () => {
      // The token answers "may this tab come in"; only the descriptor answers
      // "where". With no answer, the app is the safe place to be; routing every
      // working install to Accounts would pretend they were passwordless.
      authStatus.mockRejectedValue(new Error('network'));

      renderApp();

      expect(await screen.findByText('the chat page')).toBeInTheDocument();
      await waitFor(() =>
        expect(useAuthStore.getState().loginMode).toBe('username_password'));
    });
});

describe('startup with no token', () => {
  it('auto-logs-in a passwordless install and lands on chat', async () => {
    authStatus.mockResolvedValue(
      status({ login: 'none', auth_required: false }),
    );
    apiLogin.mockResolvedValue({ token: 'a-fresh-token' });

    renderApp();

    expect(await screen.findByText('the chat page')).toBeInTheDocument();
    expect(apiLogin).toHaveBeenCalledWith('');
  });

  it('shows setup and does not log in while setup is required', async () => {
    authStatus.mockResolvedValue(status({ login: 'setup' }));

    renderApp();

    expect(await screen.findByRole('form', { name: 'Claim this instance' })).toBeInTheDocument();
    expect(apiLogin).not.toHaveBeenCalled();
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

describe('hosted startup', () => {
  /** The actor of a gateway principal: no account, so no login name. */
  function hostedMe() {
    return {
      actor: { id: 'principal-1', kind: 'human', display_name: 'Carol', username: null },
      account: null,
    };
  }

  function renderAt(path: string) {
    return render(<MemoryRouter initialEntries={[path]}><App /></MemoryRouter>);
  }

  beforeEach(() => {
    useHostedStore.setState({ mode: 'local', problem: null, reentering: false });
    authStatus.mockResolvedValue(status({ mode: 'external', login: 'password' }));
    getViewer.mockResolvedValue(hostedMe());
  });

  it('opens the app without a token or a login page', async () => {
    renderApp();

    expect(await screen.findByText('the chat page')).toBeInTheDocument();
    expect(screen.queryByLabelText('Password')).not.toBeInTheDocument();
    expect(apiLogin).not.toHaveBeenCalled();
    expect(ws.connect).toHaveBeenCalled();
  });

  it('never shows the setup page', async () => {
    authStatus.mockResolvedValue(status({ mode: 'external', login: 'setup' }));

    renderApp();

    expect(await screen.findByText('the chat page')).toBeInTheDocument();
    expect(screen.queryByRole('form', { name: 'Claim this instance' })).not.toBeInTheDocument();
  });

  it('has no accounts page', async () => {
    // The router warns that no route matches, which is the point.
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

    renderAt('/accounts');

    await waitFor(() => expect(useAuthStore.getState().authenticated).toBe(true));
    expect(screen.queryByText('the accounts page')).not.toBeInTheDocument();
    warn.mockRestore();
  });

  it('keeps the accounts page in local mode', async () => {
    authStatus.mockResolvedValue(status());
    tokenInStorage.mockReturnValue('a-stored-token');
    getViewer.mockResolvedValue(me());

    renderAt('/accounts');

    expect(await screen.findByText('the accounts page')).toBeInTheDocument();
  });

  it('shows the no-access screen, and not the app or a login', async () => {
    getViewer.mockImplementation(async () => {
      const error = new ApiError(403, '{"reason":"access_denied","requestId":"r-1"}');
      reportError(error);
      throw error;
    });

    renderApp();

    const screenMain = await screen.findByRole('main', { name: 'No access' });
    expect(screenMain).toHaveAccessibleDescription(/do not have access/);
    expect(screen.queryByRole('button', { name: 'Try again' })).not.toBeInTheDocument();
    expect(screen.queryByText('the chat page')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Password')).not.toBeInTheDocument();
    expect(ws.connect).not.toHaveBeenCalled();
  });

  it('replaces a live app with the no-access screen and closes the socket', async () => {
    renderApp();
    expect(await screen.findByText('the chat page')).toBeInTheDocument();

    act(() => showProblem('access_denied'));

    expect(screen.getByRole('main', { name: 'No access' })).toBeInTheDocument();
    expect(screen.queryByText('the chat page')).not.toBeInTheDocument();
    expect(ws.disconnect).toHaveBeenCalled();
  });

  it('shows the archived screen', async () => {
    renderApp();
    expect(await screen.findByText('the chat page')).toBeInTheDocument();

    act(() => showProblem('agent_archived'));

    expect(screen.getByRole('main', { name: 'Agent archived' })).toBeInTheDocument();
  });

  it('offers a retry when the agent is unavailable at startup', async () => {
    getViewer.mockRejectedValueOnce(new TypeError('network'));

    renderApp();
    fireEvent.click(await screen.findByRole('button', { name: 'Try again' }));

    expect(await screen.findByText('the chat page')).toBeInTheDocument();
    expect(authStatus).toHaveBeenCalledTimes(2);
    expect(useHostedStore.getState().problem).toBeNull();
  });

  it('shows the unavailable dialog over a live app and keeps the app', async () => {
    renderApp();
    expect(await screen.findByText('the chat page')).toBeInTheDocument();
    const loadSessions = useChatStore.getState().loadSessions as ReturnType<typeof vi.fn>;
    loadSessions.mockClear();

    act(() => showProblem('unavailable'));

    const dialog = screen.getByRole('dialog', { name: 'Agent unavailable' });
    expect(dialog).toHaveAttribute('aria-modal', 'true');
    expect(screen.getByRole('button', { name: 'Try again' })).toHaveFocus();
    expect(screen.getByText('the chat page')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Try again' }));

    expect(screen.queryByRole('dialog', { name: 'Agent unavailable' })).not.toBeInTheDocument();
    expect(loadSessions).toHaveBeenCalledOnce();
  });

  it('shows no session-expired overlay', async () => {
    renderApp();
    expect(await screen.findByText('the chat page')).toBeInTheDocument();

    act(() => useAuthStore.setState({ sessionExpired: true }));

    expect(screen.queryByRole('dialog', { name: 'Session expired' })).not.toBeInTheDocument();
  });
});

describe('startup when the status call fails behind the gateway', () => {
  /** A status call that fails the way `api/client` fails it for this body. */
  function statusFailsOnce(status: number, body: string) {
    authStatus.mockImplementationOnce(async () => {
      const error = new ApiError(status, body);
      handleHostedError(error);
      throw error;
    });
  }

  beforeEach(() => {
    useHostedStore.setState({ mode: 'local', problem: null, reentering: false });
    authStatus.mockResolvedValue(status({ mode: 'external' }));
    getViewer.mockResolvedValue({
      actor: { id: 'principal-1', kind: 'human', display_name: 'Carol', username: null },
      account: null,
    });
  });

  it('shows the unavailable screen with a retry, and no login page', async () => {
    statusFailsOnce(503, '{"reason":"agent_starting","requestId":"r-1"}');

    renderApp();

    expect(await screen.findByRole('main', { name: 'Agent unavailable' })).toBeInTheDocument();
    expect(screen.queryByLabelText('Password')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Try again' }));

    expect(await screen.findByText('the chat page')).toBeInTheDocument();
  });

  it('shows the no-access screen for access_denied', async () => {
    statusFailsOnce(403, '{"reason":"access_denied","requestId":"r-1"}');

    renderApp();

    expect(await screen.findByRole('main', { name: 'No access' })).toBeInTheDocument();
    expect(screen.queryByLabelText('Password')).not.toBeInTheDocument();
  });

  it('shows the login page for a failure without a gateway body', async () => {
    // The login page reads the status again, so every read fails.
    authStatus.mockRejectedValue(new ApiError(502, '<html>Bad Gateway</html>'));

    renderApp();

    expect(await screen.findByLabelText('Password')).toBeInTheDocument();
    expect(useHostedStore.getState().mode).toBe('local');
  });
});
