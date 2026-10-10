import { act, fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../api/client', async () => {
  const actual = await vi.importActual<typeof import('../../api/client')>('../../api/client');
  return {
    ...actual,
    api: { authStatus: vi.fn(), getViewer: vi.fn(), login: vi.fn(), listActors: vi.fn() },
    setUnauthorizedHandler: vi.fn(),
  };
});
vi.mock('../../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const { useSetupStore } = await import('../../stores/setupStore');
const { RestartOverlay, DISMISSIBLE_AFTER_MS } = await import('./RestartOverlay');

const outcome = (mode: 'restart' | 'unsupported', extra = {}) => ({
  mode, generation: 'G0', message: '', command: null, timeout_hint_seconds: 0, ...extra,
});

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/setup/review" element={<><RestartOverlay /><div>review page</div></>} />
        <Route path="/chat" element={<><RestartOverlay /><div>chat page</div></>} />
      </Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  useSetupStore.getState().reset();
});

describe('unsupported (a supervisor owns the process)', () => {
  beforeEach(() => {
    useSetupStore.setState({
      reconnecting: true,
      restartOutcome: outcome('unsupported', {
        command: 'systemctl restart nerve.service', timeout_hint_seconds: 300,
        message: 'This instance is managed by systemd.',
      }),
    });
  });

  it('names who must act, shows the command with Copy, and Not now closes it', async () => {
    renderAt('/chat');
    const dialog = screen.getByRole('dialog', { name: /must be done on the server/ });
    expect(dialog).toHaveAccessibleDescription(/Someone with access to the server must run/);
    expect(screen.getByText('systemctl restart nerve.service')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Copy' })).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Not now' }));

    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(useSetupStore.getState().overlayDismissed).toBe(true);
    // The wait keeps running in the store; closing only returns to the review.
    expect(useSetupStore.getState().reconnecting).toBe(true);
    expect(await screen.findByText('review page')).toBeInTheDocument();
  });

  it('Escape does the same', async () => {
    renderAt('/setup/review');
    await userEvent.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(useSetupStore.getState().overlayDismissed).toBe(true);
  });

  it('without a command it names the supervisor\'s operator and promises no command', () => {
    useSetupStore.setState({
      restartOutcome: outcome('unsupported', {
        command: null, timeout_hint_seconds: 300,
        message: 'This instance is managed by a supervisor this build cannot name.',
      }),
    });
    renderAt('/setup/review');
    const dialog = screen.getByRole('dialog', { name: /must be done on the server/ });
    expect(dialog).toHaveAccessibleDescription(/Someone who runs this instance's supervisor must restart Nerve/);
    expect(dialog).not.toHaveAccessibleDescription(/command below/);
    expect(dialog).toHaveAccessibleDescription(/cannot name/);   // the server's own sentence still shows
    expect(screen.queryByRole('button', { name: 'Copy' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Not now' })).toBeInTheDocument();
  });
});

describe('timed out', () => {
  beforeEach(() => {
    useSetupStore.setState({ reconnectFailed: true, restartOutcome: outcome('restart', { timeout_hint_seconds: 120 }) });
  });

  it('offers Retry waiting and Close', async () => {
    const retryWait = vi.fn(async () => true);
    useSetupStore.setState({ retryWait });
    renderAt('/setup/review');
    expect(screen.getByRole('dialog', { name: 'Nerve did not come back' })).toHaveAccessibleDescription(/2 minutes/);

    await userEvent.click(screen.getByRole('button', { name: 'Retry waiting' }));
    expect(retryWait).toHaveBeenCalled();

    await userEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });
});

describe('restarting', () => {
  beforeEach(() => {
    useSetupStore.setState({ reconnecting: true, reconnectElapsedMs: 0, restartOutcome: outcome('restart') });
  });

  it('has no secondary action before ten seconds, and Escape does nothing then', async () => {
    renderAt('/setup/review');
    const dialog = screen.getByRole('dialog', { name: /Restarting Nerve/ });
    expect(dialog).toHaveAccessibleDescription(/can be put away after ten/);
    expect(screen.queryByRole('button', { name: 'Not now' })).not.toBeInTheDocument();

    fireEvent.keyDown(dialog, { key: 'Escape' });
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    expect(useSetupStore.getState().overlayDismissed).toBe(false);
  });

  it('offers Not now after ten seconds of waiting', async () => {
    renderAt('/setup/review');
    act(() => useSetupStore.setState({ reconnectElapsedMs: DISMISSIBLE_AFTER_MS }));
    expect(screen.getByRole('dialog', { name: /Restarting Nerve… \(10 s\)/ })).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Not now' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(useSetupStore.getState().reconnecting).toBe(true);
  });

  it('is gone once the new process answered', () => {
    renderAt('/setup/review');
    act(() => useSetupStore.setState({ reconnecting: false, restartOutcome: null }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });
});
