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
      getViewer: vi.fn(),
      login: vi.fn(),
      setupClaim: vi.fn(),
      listActors: vi.fn(),
      setupState: vi.fn(),
    },
    setUnauthorizedHandler: vi.fn(),
  };
});
vi.mock('../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const { api, clearToken } = await import('../api/client');
const { useHostedStore } = await import('../api/hosted');
const { useAuthStore } = await import('../stores/authStore');
const { useActorStore } = await import('../stores/actorStore');
const { useSetupStore } = await import('../stores/setupStore');
const { makeState } = await import('../test/setupFixtures');
const { SetupPage } = await import('./SetupPage');

const setupClaim = api.setupClaim as unknown as ReturnType<typeof vi.fn>;
const authStatus = api.authStatus as unknown as ReturnType<typeof vi.fn>;
const getViewer = api.getViewer as unknown as ReturnType<typeof vi.fn>;
const listActors = api.listActors as unknown as ReturnType<typeof vi.fn>;
const setupState = api.setupState as unknown as ReturnType<typeof vi.fn>;

const CLAIMED = {
  auth_required: true,
  login: 'password',
};
const VIEWER = {
  actor: { id: 'actor-1', kind: 'human', display_name: 'Alice' },
  account: {
    id: 'account-1',
    actor_id: 'actor-1',
    username: 'alice',
    display_name: 'Alice',
    enabled: true,
    has_password: true,
    created_at: 't',
  },
};

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/setup']}>
      <Routes>
        <Route path="/setup" element={<SetupPage />} />
        <Route path="/setup/:stepId" element={<div>step destination</div>} />
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
  useSetupStore.getState().reset();
  useHostedStore.setState({ mode: 'local', problem: null, reentering: false, principalId: null });
  useAuthStore.setState({
    authenticated: true,
    loading: false,
    ready: true,
    error: null,
    sessionExpired: false,
    loginMode: 'setup',
    viewer: null,
    account: null,
  });
  setupClaim.mockResolvedValue({ token: 'client-session-token' });
  authStatus.mockResolvedValue(CLAIMED);
  getViewer.mockResolvedValue(VIEWER);
  listActors.mockResolvedValue({ actors: [{
    id: 'actor-1',
    kind: 'human',
    display_name: 'Alice',
  }] });
  setupState.mockResolvedValue(makeState());
});

describe('first-account claim', () => {
  it('requires account credentials and the setup token', async () => {
    renderPage();

    expect(screen.getByLabelText('Username')).toBeRequired();
    expect(screen.getByLabelText('Password')).toBeRequired();
    expect(screen.getByLabelText('Setup token')).toBeRequired();
    // Never disabled: a keyboard user must be able to reach it and be told why.
    expect(screen.getByRole('button', { name: 'Create and continue' })).toBeEnabled();
    expect(screen.getByText(/HTTPS or a protected tunnel/)).toBeInTheDocument();
    expect(screen.queryByText(/provider credential/i)).not.toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 1, name: 'Create your sign-in' })).toBeInTheDocument();
    expect(screen.queryByText(/^Claim/)).not.toBeInTheDocument();
  });

  it('names the first missing field, focused, instead of disabling the button', async () => {
    renderPage();
    const button = screen.getByRole('button', { name: 'Create and continue' });
    await userEvent.click(button);
    expect(setupClaim).not.toHaveBeenCalled();
    const username = screen.getByLabelText('Username');
    expect(username).toHaveFocus();
    expect(username).toHaveAttribute('aria-invalid', 'true');
    expect(username).toHaveAccessibleDescription(/Enter a username/);
    expect(screen.getByText('Fill in the username, password and setup token to continue.')).toBeInTheDocument();

    await userEvent.type(username, 'alice');
    expect(username).not.toHaveAttribute('aria-invalid');
    await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
    await userEvent.click(button);
    expect(setupClaim).not.toHaveBeenCalled();
    expect(screen.getByLabelText('Setup token')).toHaveFocus();
    expect(screen.getByLabelText('Setup token')).toHaveAccessibleDescription(/Enter the setup token/);

    await userEvent.type(screen.getByLabelText('Setup token'), 'host-only-token');
    expect(screen.queryByText(/Fill in the username/)).not.toBeInTheDocument();
    await userEvent.click(button);
    await waitFor(() => expect(setupClaim).toHaveBeenCalled());
  });

  it('stores only the client session, refreshes canonical auth, and continues to the Mode step',
    async () => {
      renderPage();
      await userEvent.type(screen.getByLabelText('Username'), 'alice');
      await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
      await userEvent.type(screen.getByLabelText('Display name'), 'Alice');
      await userEvent.type(screen.getByLabelText('Setup token'), 'host-only-token');
      await userEvent.click(screen.getByRole('button', { name: 'Create and continue' }));

      expect(await screen.findByText('step destination')).toBeInTheDocument();
      expect(screen.queryByText('chat destination')).not.toBeInTheDocument();
      expect(setupClaim).toHaveBeenCalledWith({
        username: 'alice',
        password: 'a-real-password',
        setup_token: 'host-only-token',
        display_name: 'Alice',
      });
      expect(authStatus).toHaveBeenCalled();
      expect(getViewer).toHaveBeenCalled();
      expect(listActors).toHaveBeenCalled();
      expect(localStorage.getItem('nerve_token')).toBe('client-session-token');
      expect(localStorage.getItem('setup_token')).toBeNull();
      expect(JSON.stringify(localStorage)).not.toContain('host-only-token');
      expect(JSON.stringify(sessionStorage)).not.toContain('host-only-token');
      expect(useAuthStore.getState().account?.id).toBe('account-1');
      expect(useAuthStore.getState().viewer?.id).toBe('actor-1');
      // Still signed in: the claim does not bounce to a login form.
      expect(useAuthStore.getState().authenticated).toBe(true);
    });

  it('omits a blank optional display name', async () => {
    renderPage();
    await userEvent.type(screen.getByLabelText('Username'), 'alice');
    await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
    await userEvent.type(screen.getByLabelText('Setup token'), 'host-only-token');
    await userEvent.click(screen.getByRole('button', { name: 'Create and continue' }));

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
    await userEvent.click(screen.getByRole('button', { name: 'Create and continue' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('invalid setup token');
    expect(localStorage.getItem('nerve_token')).toBeNull();
    expect(JSON.stringify(localStorage)).not.toContain('wrong-token');
  });

  it('clears the token and puts the cursor back there when the server rejects it', async () => {
    setupClaim.mockRejectedValue(new Error('403: A valid setup token is required.'));
    renderPage();
    await userEvent.type(screen.getByLabelText('Username'), 'alice');
    await userEvent.type(screen.getByLabelText('Password'), 'a-real-password');
    await userEvent.type(screen.getByLabelText('Setup token'), 'wrong-token');
    await userEvent.click(screen.getByRole('button', { name: 'Create and continue' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('valid setup token');
    expect(screen.getByLabelText('Setup token')).toHaveValue('');
    expect(screen.getByLabelText('Setup token')).toHaveFocus();
    expect(screen.getByLabelText('Username')).toHaveValue('alice');   // only the token is cleared
    expect(screen.getByRole('button', { name: 'Create and continue' })).toBeEnabled();
  });

  it('labels every field visibly, with the hints under them', () => {
    renderPage();
    for (const name of ['Username', 'Password', 'Display name', 'Setup token']) {
      const input = screen.getByLabelText(name);
      expect(input.tagName).toBe('INPUT');
      // A real <label htmlFor>, visible — not a placeholder or an aria-label.
      const label = screen.getByText(name, { selector: 'label' });
      expect(label).toHaveAttribute('for', input.id);
      expect(input).not.toHaveAttribute('placeholder');
    }
    expect(screen.getByLabelText('Setup token')).toHaveAccessibleDescription(/nerve status/);
    expect(screen.getByLabelText('Display name')).toHaveAccessibleDescription(/Optional/);
  });

  it('keeps the installation passwordless only by explicit choice', async () => {
    renderPage();
    expect(screen.queryByRole('note')).not.toBeInTheDocument();

    await userEvent.click(screen.getByLabelText('Keep this installation passwordless'));
    expect(screen.queryByLabelText('Password')).not.toBeInTheDocument();
    expect(screen.getByLabelText('Username')).not.toBeRequired();
    expect(screen.getByRole('note')).toHaveTextContent(/anyone who can reach/);

    await userEvent.type(screen.getByLabelText('Setup token'), 'host-only-token');
    await userEvent.click(screen.getByRole('button', { name: 'Create and continue' }));

    expect(await screen.findByText('step destination')).toBeInTheDocument();
    expect(setupClaim.mock.calls[0][0]).toEqual({
      username: undefined,
      passwordless: true,
      setup_token: 'host-only-token',
      display_name: undefined,
    });
    expect(JSON.stringify(localStorage)).not.toContain('host-only-token');
  });

  it('rates the password as it is typed, in words', async () => {
    renderPage();
    const meter = () => screen.getByRole('meter', { name: 'Password strength' });
    await userEvent.type(screen.getByLabelText('Password'), 'abc');
    expect(meter()).toHaveAttribute('aria-valuetext', 'Too short');
    await userEvent.clear(screen.getByLabelText('Password'));
    await userEvent.type(screen.getByLabelText('Password'), 'correct horse battery staple');
    expect(meter()).toHaveAttribute('aria-valuetext', expect.stringMatching(/Good|Strong/));
  });

  it('can reveal the setup token while typing it, without storing it', async () => {
    renderPage();
    expect(screen.getByLabelText('Setup token')).toHaveAttribute('type', 'password');
    await userEvent.click(screen.getByRole('button', { name: 'Show setup token' }));
    expect(screen.getByLabelText('Setup token')).toHaveAttribute('type', 'text');
    expect(JSON.stringify(localStorage)).not.toContain('token');
  });

  it.each(['password', 'none'] as const)(
    'renders the setup overview, not the claim, once the instance is claimed (%s)', async (loginMode) => {
      useAuthStore.setState({ loginMode });
      renderPage();
      expect(await screen.findByRole('navigation', { name: 'Setup steps' })).toBeInTheDocument();
      expect(screen.queryByRole('form', { name: 'Create your sign-in' })).not.toBeInTheDocument();
      expect(screen.queryByText('chat destination')).not.toBeInTheDocument();
    });

  it('never shows the claim in external mode, even while login says setup', async () => {
    useHostedStore.setState({ mode: 'external' });
    renderPage();
    expect(await screen.findByRole('navigation', { name: 'Setup steps' })).toBeInTheDocument();
    expect(screen.queryByRole('form', { name: 'Create your sign-in' })).not.toBeInTheDocument();
  });
});
