import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

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
const { makeState } = await import('../../test/setupFixtures');
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
