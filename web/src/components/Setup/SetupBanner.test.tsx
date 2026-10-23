import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Link, MemoryRouter, Route, Routes, useParams } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../api/client', async () => {
  const actual = await vi.importActual<typeof import('../../api/client')>('../../api/client');
  return {
    ...actual,
    api: {
      authStatus: vi.fn(), getViewer: vi.fn(), login: vi.fn(), listActors: vi.fn(), setupState: vi.fn(),
      setupFinish: vi.fn(),
    },
    setUnauthorizedHandler: vi.fn(),
  };
});
vi.mock('../../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const { api } = await import('../../api/client');
const { useSetupStore } = await import('../../stores/setupStore');
const { makeState, step } = await import('../../test/setupFixtures');
const { SetupBanner } = await import('./SetupBanner');

function renderAt(path = '/chat') {
  return render(<MemoryRouter initialEntries={[path]}><SetupBanner /></MemoryRouter>);
}

const realMatchMedia = window.matchMedia;

beforeEach(() => {
  sessionStorage.clear();
  useSetupStore.getState().reset();
  useSetupStore.setState({ state: makeState({ steps_left: 3 }) });
});

afterEach(() => {
  window.matchMedia = realMatchMedia;
});

describe('SetupBanner', () => {
  it('counts the steps left and links to the wizard', () => {
    renderAt();
    const link = screen.getByRole('status');
    expect(link).toHaveTextContent('Finish setting up Nerve — 3 steps left · Continue');
    expect(link).toHaveAttribute('href', '/setup');
  });

  it('mentions a pending restart beside the count', () => {
    useSetupStore.setState({ state: makeState({ steps_left: 1, restart_pending: true }) });
    renderAt();
    expect(screen.getByRole('status')).toHaveTextContent('1 step left · A restart is pending');
  });

  it('asks for the restart, never "0 steps left", when only the restart remains', () => {
    useSetupStore.setState({ state: makeState({ steps_left: 0, restart_pending: true }) });
    renderAt();
    const link = screen.getByRole('status');
    expect(link).toHaveTextContent('Restart Nerve to apply your setup · Restart');
    expect(link).not.toHaveTextContent('0 steps');
    expect(link).toHaveAttribute('href', '/setup/review');
  });

  it('is hidden on the setup pages, when finished, while unknown, and while reconnecting', () => {
    // Each case unmounts before the store changes, so no mounted banner is
    // updated outside act().
    let view = renderAt('/setup/provider');
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    view.unmount();

    useSetupStore.setState({ state: makeState({ finished: true, steps_left: 0 }) });
    view = renderAt();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    view.unmount();

    useSetupStore.setState({ state: null });
    view = renderAt();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    view.unmount();

    useSetupStore.setState({ state: makeState({ steps_left: 2 }), reconnecting: true });
    renderAt();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
  });

  it('is hidden with nothing to do and no restart pending', () => {
    useSetupStore.setState({ state: makeState({ steps_left: 0, restart_pending: false }) });
    renderAt();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
  });

  it('can be dismissed for this tab session', async () => {
    renderAt();
    await userEvent.click(screen.getByRole('button', { name: 'Dismiss' }));
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    expect(sessionStorage.getItem('nerve_setup_banner_dismissed')).toBe('1');
  });

  it('sits clear of the nav rail on a desktop and under the header on a phone', () => {
    // Position is a layout fact that only the class can express; jsdom drops
    // the `env()` safe-area inline styles, so the anchor class is the signal.
    // Desktop: bottom-left, but offset past the 56 px rail whose bottom holds
    // the theme toggle and Logout — never `left-4`, which lies on top of them.
    const { unmount } = renderAt();
    const desktop = screen.getByRole('status').parentElement!;
    expect(desktop.className).toMatch(/\bbottom-4\b/);
    expect(desktop.className).toMatch(/\bleft-\[4\.5rem\]/);
    expect(desktop.className).not.toMatch(/\bleft-4\b/);
    unmount();

    window.matchMedia = ((query: string) => ({
      matches: query === '(max-width: 767px)',
      media: query, onchange: null,
      addListener: () => {}, removeListener: () => {},
      addEventListener: () => {}, removeEventListener: () => {}, dispatchEvent: () => false,
    })) as typeof window.matchMedia;
    renderAt();
    // Phone: below the page header (safe-area inset + the header's height),
    // not over it; the bottom belongs to the nav bar and the composer.
    const box = screen.getByRole('status').parentElement!;
    expect(box.className).toMatch(/\btop-\[calc\(env\(safe-area-inset-top\)\+3\.5rem\)\]/);
    expect(box.className).not.toMatch(/\btop-2\b/);
    expect(box.className).not.toMatch(/\bbottom-4\b/);
  });
});

describe('SetupBanner while the onboarding conversation waits', () => {
  const waiting = (overrides = {}) => makeState({
    finished: true, steps_left: 0,
    steps: [step('mode', { status: 'done' }, {
      mode: 'personal', onboarding: 'running', onboarding_session_id: 'onboarding-1',
    })],
    ...overrides,
  });

  beforeEach(() => {
    useSetupStore.setState({ state: waiting() });
    vi.mocked(api.setupState).mockImplementation(async () => useSetupStore.getState().state!);
  });

  function renderWithLink(path: string) {
    return render(
      <MemoryRouter initialEntries={[path]}>
        <Link to="/tasks">Tasks</Link>
        <SetupBanner />
      </MemoryRouter>,
    );
  }

  it('sends the person back to the conversation from every page, also after setup', async () => {
    renderWithLink('/setup');
    const link = await screen.findByRole('status');
    expect(link).toHaveTextContent('Your agent waits for you in its onboarding conversation · Continue');
    expect(link).toHaveAttribute('href', '/chat/onboarding-1');
    expect(screen.queryByRole('button', { name: 'Dismiss' })).not.toBeInTheDocument();
  });

  it('is hidden on the conversation itself', () => {
    renderWithLink('/chat/onboarding-1');
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
  });

  it('hides only until the next page, and reads the state again on each page', async () => {
    renderWithLink('/chat');
    await userEvent.click(screen.getByRole('button', { name: 'Hide until the next page' }));
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    const reads = vi.mocked(api.setupState).mock.calls.length;
    await userEvent.click(screen.getByRole('link', { name: 'Tasks' }));
    expect(await screen.findByRole('status')).toHaveTextContent('onboarding conversation');
    expect(vi.mocked(api.setupState).mock.calls.length).toBeGreaterThan(reads);
  });

  it('is gone once onboarding is done or skipped', () => {
    for (const onboarding of ['done', 'skipped'] as const) {
      useSetupStore.setState({ state: waiting({
        steps: [step('mode', { status: 'done' }, { mode: 'personal', onboarding, onboarding_session_id: 'onboarding-1' })],
      }) });
      const view = renderWithLink('/chat');
      expect(screen.queryByRole('status')).not.toBeInTheDocument();
      view.unmount();
    }
  });
});

describe('SetupBanner when setup is complete and the conversation has not started', () => {
  const withMode = (detail: Record<string, unknown>, overrides = {}) => makeState({
    finished: true, steps_left: 0,
    steps: [step('mode', { status: 'done' }, {
      mode: 'personal', onboarding: 'pending', onboarding_session_id: null, ...detail,
    })],
    ...overrides,
  });

  function ChatProbe() {
    return <p>chat page {useParams().sessionId}</p>;
  }

  function renderRoutes(path = '/tasks') {
    return render(
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route path="/chat/:sessionId" element={<ChatProbe />} />
          <Route path="*" element={<p>other page</p>} />
        </Routes>
        <SetupBanner />
      </MemoryRouter>,
    );
  }

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('starts the conversation once, without Finish, and opens it', async () => {
    const running = withMode({ onboarding: 'running', onboarding_session_id: 'onboarding-9' });
    vi.mocked(api.setupFinish).mockResolvedValue({
      state: running, action: 'reloaded', onboarding: { status: 'running', session_id: 'onboarding-9' },
    });
    vi.mocked(api.setupState).mockResolvedValue(running);
    useSetupStore.setState({ state: withMode({}) });
    renderRoutes();
    await userEvent.click(screen.getByRole('button', { name: /ready for its onboarding conversation · Start/ }));
    expect(await screen.findByText('chat page onboarding-9')).toBeInTheDocument();
    expect(api.setupFinish).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole('button', { name: /Start/ })).not.toBeInTheDocument();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
  });

  it('waits for the steps first, and is not offered to a worker that sets itself up', () => {
    useSetupStore.setState({ state: withMode({}, { finished: false, steps_left: 2 }) });
    let view = renderRoutes();
    expect(screen.queryByRole('button', { name: /Start/ })).not.toBeInTheDocument();
    expect(screen.getByRole('status')).toHaveTextContent('2 steps left');
    view.unmount();

    useSetupStore.setState({ state: withMode({ onboarding_kind: 'autonomous' }) });
    view = renderRoutes();
    expect(screen.queryByRole('button', { name: /Start/ })).not.toBeInTheDocument();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
  });

  it('only links to the session while a worker sets itself up', () => {
    useSetupStore.setState({ state: withMode({
      mode: 'worker', onboarding: 'running', onboarding_session_id: 'onboarding-2', onboarding_kind: 'autonomous',
    }) });
    vi.mocked(api.setupState).mockImplementation(async () => useSetupStore.getState().state!);
    renderRoutes();
    const link = screen.getByRole('status');
    expect(link).toHaveTextContent('Your worker is setting itself up from its task description · Open');
    expect(link).toHaveAttribute('href', '/chat/onboarding-2');
    expect(screen.queryByRole('button', { name: /Start/ })).not.toBeInTheDocument();
  });
});
