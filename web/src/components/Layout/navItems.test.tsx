import { act, fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../api/client', () => ({
  api: {
    getUltracodeDashboardStatus: vi.fn(async () => ({ enabled: false })),
    listNotifications: vi.fn(async () => ({ notifications: [], pending_count: 0 })),
  },
  getToken: vi.fn(() => null),
  setToken: vi.fn(),
  clearToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));
vi.mock('../../api/websocket', () => ({ ws: { connected: false } }));

const { NavRail } = await import('./NavRail');
const { BottomNav } = await import('./BottomNav');
const { setAuthMode, useHostedStore } = await import('../../api/hosted');

/** Render a nav and let its feature flag and notification reads answer. */
async function renderNav(nav: 'rail' | 'bottom') {
  render(
    <MemoryRouter initialEntries={['/chat']}>
      {nav === 'rail' ? <NavRail /> : <BottomNav />}
    </MemoryRouter>,
  );
  await act(async () => {});
}

/** Open the "More" drawer of the bottom bar, where Accounts lives. */
function openMore() {
  fireEvent.click(screen.getByRole('button', { name: 'More' }));
}

beforeEach(() => {
  useHostedStore.setState({ mode: 'local', problem: null, reentering: false });
});

describe('local account controls', () => {
  it('shows in the nav rail in local mode', async () => {
    await renderNav('rail');

    expect(screen.getByRole('button', { name: 'Accounts' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Logout' })).toBeInTheDocument();
  });

  it('is absent from the nav rail in hosted mode', async () => {
    setAuthMode('external');

    await renderNav('rail');

    expect(screen.queryByRole('button', { name: 'Accounts' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Logout' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Memory' })).toBeInTheDocument();
  });

  it('shows in the bottom bar menu in local mode', async () => {
    await renderNav('bottom');
    openMore();

    expect(screen.getByRole('button', { name: 'Accounts' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Log out' })).toBeInTheDocument();
  });

  it('is absent from the bottom bar menu in hosted mode', async () => {
    setAuthMode('external');

    await renderNav('bottom');
    openMore();

    expect(screen.queryByRole('button', { name: 'Accounts' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Log out' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Memory' })).toBeInTheDocument();
  });
});
