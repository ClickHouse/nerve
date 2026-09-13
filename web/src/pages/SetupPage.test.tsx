import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../api/client', async () => {
  const actual = await vi.importActual<typeof import('../api/client')>('../api/client');
  return {
    ...actual,
    api: {
      authStatus: vi.fn(),
      getOwnAccount: vi.fn(),
      login: vi.fn(),
      setupClaim: vi.fn(),
      listActors: vi.fn(),
    },
    setUnauthorizedHandler: vi.fn(),
  };
});
vi.mock('../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const { api, clearToken } = await import('../api/client');
const { useAuthStore } = await import('../stores/authStore');
const { useActorStore } = await import('../stores/actorStore');
const { SetupPage } = await import('./SetupPage');

const setupClaim = api.setupClaim as unknown as ReturnType<typeof vi.fn>;
const authStatus = api.authStatus as unknown as ReturnType<typeof vi.fn>;
const getOwnAccount = api.getOwnAccount as unknown as ReturnType<typeof vi.fn>;
const listActors = api.listActors as unknown as ReturnType<typeof vi.fn>;

const CLAIMED = {
  auth_required: true,
  mode: 'local',
  login: 'password',
  setup_pending: false,
  multiple_accounts: false,
};
const ACCOUNT = {
  id: 'account-1',
  actor_id: 'actor-1',
  username: 'alice',
  display_name: 'Alice',
  enabled: true,
  has_password: true,
  created_at: 't',
  updated_at: 't',
  disabled_at: null,
  is_self: true,
};

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/setup']}>
      <Routes>
        <Route path="/setup" element={<SetupPage />} />
        <Route path="/chat" element={<div>chat destination</div>} />
        <Route path="/accounts" element={<div>accounts destination</div>} />
      </Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  clearToken();
  useActorStore.getState().reset();
  useAuthStore.setState({
    authenticated: true,
    loading: false,
    ready: true,
    error: null,
    sessionExpired: false,
    loginMode: 'none',
    statusLoading: false,
    setupPending: true,
    account: null,
  });
  setupClaim.mockResolvedValue({ token: 'client-session-token' });
  authStatus.mockResolvedValue(CLAIMED);
  getOwnAccount.mockResolvedValue(ACCOUNT);
  listActors.mockResolvedValue({ actors: [{
    id: 'actor-1',
    kind: 'human',
    display_name: 'Alice',
    profile_version: 1,
  }] });
});

describe('first-account claim', () => {
  it('requires account credentials and the setup token', async () => {
    renderPage();

    expect(screen.getByLabelText('Username')).toBeRequired();
    expect(screen.getByLabelText('Password')).toBeRequired();
    expect(screen.getByLabelText('Setup token')).toBeRequired();
    expect(screen.getByRole('button', { name: 'Claim and sign in' })).toBeDisabled();
    expect(screen.getByText(/HTTPS or a protected tunnel/)).toBeInTheDocument();
    expect(screen.queryByText(/provider credential/i)).not.toBeInTheDocument();
  });

  it('stores only the client session, refreshes canonical auth, and routes chat',
    async () => {
      renderPage();
      await userEvent.type(screen.getByLabelText('Username'), 'alice');
      await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
      await userEvent.type(screen.getByLabelText('Display name'), 'Alice');
      await userEvent.type(screen.getByLabelText('Setup token'), 'host-only-token');
      await userEvent.click(screen.getByRole('button', { name: 'Claim and sign in' }));

      expect(await screen.findByText('chat destination')).toBeInTheDocument();
      expect(setupClaim).toHaveBeenCalledWith({
        username: 'alice',
        password: 'a-real-password',
        setup_token: 'host-only-token',
        display_name: 'Alice',
      });
      expect(authStatus).toHaveBeenCalled();
      expect(getOwnAccount).toHaveBeenCalled();
      expect(listActors).toHaveBeenCalled();
      expect(localStorage.getItem('nerve_token')).toBe('client-session-token');
      expect(localStorage.getItem('setup_token')).toBeNull();
      expect(JSON.stringify(localStorage)).not.toContain('host-only-token');
      expect(useAuthStore.getState().account?.id).toBe('account-1');
    });

  it('omits a blank optional display name', async () => {
    renderPage();
    await userEvent.type(screen.getByLabelText('Username'), 'alice');
    await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
    await userEvent.type(screen.getByLabelText('Setup token'), 'host-only-token');
    await userEvent.click(screen.getByRole('button', { name: 'Claim and sign in' }));

    await waitFor(() => expect(setupClaim).toHaveBeenCalled());
    expect(setupClaim.mock.calls[0][0]).toEqual({
      username: 'alice',
      password: 'a-real-password',
      setup_token: 'host-only-token',
      display_name: undefined,
    });
  });

  it('shows a refusal without storing either credential', async () => {
    setupClaim.mockRejectedValue(new Error('403: invalid setup token'));
    renderPage();
    await userEvent.type(screen.getByLabelText('Username'), 'alice');
    await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
    await userEvent.type(screen.getByLabelText('Setup token'), 'wrong-token');
    await userEvent.click(screen.getByRole('button', { name: 'Claim and sign in' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('invalid setup token');
    expect(localStorage.getItem('nerve_token')).toBeNull();
    expect(JSON.stringify(localStorage)).not.toContain('wrong-token');
  });

  it('routes a claimed instance to accounts', async () => {
    useAuthStore.setState({ setupPending: false });
    renderPage();
    expect(await screen.findByText('accounts destination')).toBeInTheDocument();
  });
});
