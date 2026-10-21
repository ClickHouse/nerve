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
const { useSetupStore } = await import('../../stores/setupStore');
const { makeState } = await import('../../test/setupFixtures');
const { NAV_ITEMS, visibleNavItems } = await import('./navItems');

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
  useSetupStore.getState().reset();
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

describe('the Setup entry', () => {
  it('is hidden while the checklist is unknown and appears the moment it is read, with no other re-render', async () => {
    await renderNav('rail');
    expect(screen.queryByRole('button', { name: 'Setup' })).not.toBeInTheDocument();

    // Only the setup store changes; nothing else about the rail does. If the
    // filter read the store with getState() this would stay hidden.
    act(() => { useSetupStore.setState({ state: makeState() }); });

    expect(screen.getByRole('button', { name: 'Setup' })).toBeInTheDocument();
  });

  it('stays visible once setup is finished (re-enterable)', async () => {
    useSetupStore.setState({ state: makeState({ finished: true, steps_left: 0 }) });
    await renderNav('rail');
    expect(screen.getByRole('button', { name: 'Setup' })).toBeInTheDocument();
  });

  it('shows in hosted mode too', async () => {
    setAuthMode('external');
    useSetupStore.setState({ state: makeState({ auth_mode: 'external' }) });
    await renderNav('rail');
    expect(screen.getByRole('button', { name: 'Setup' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Accounts' })).not.toBeInTheDocument();
  });

  it('lives behind More on the phone', async () => {
    useSetupStore.setState({ state: makeState() });
    await renderNav('bottom');
    // The drawer stays mounted while closed, parked and inert.
    expect(screen.getByRole('button', { name: 'Setup' }).closest('[inert]')).not.toBeNull();
    openMore();
    expect(screen.getByRole('button', { name: 'Setup' }).closest('[inert]')).toBeNull();
  });

  it('is decided by a pure filter', () => {
    const paths = (items: { path: string }[]) => items.map((i) => i.path);
    expect(paths(visibleNavItems(NAV_ITEMS, { hosted: false, ultracodeEnabled: false, setupKnown: false })))
      .not.toContain('/setup');
    expect(paths(visibleNavItems(NAV_ITEMS, { hosted: true, ultracodeEnabled: false, setupKnown: true })))
      .toEqual(expect.arrayContaining(['/setup']));
    expect(paths(visibleNavItems(NAV_ITEMS, { hosted: true, ultracodeEnabled: false, setupKnown: true })))
      .not.toContain('/accounts');
  });
});
