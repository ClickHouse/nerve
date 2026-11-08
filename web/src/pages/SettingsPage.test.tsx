import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from 'vitest';
import { MOBILE_QUERY } from '../hooks/useMediaQuery';

vi.mock('../api/client', () => ({
  api: { settingsNotifications: vi.fn(() => new Promise(() => {})) },
  getToken: vi.fn(() => 'session-token'),
  setToken: vi.fn(),
  clearToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

const { SettingsPage } = await import('./SettingsPage');

function LocationProbe() {
  return <output aria-label="Location">{useLocation().pathname}</output>;
}

function renderAt(path: string) {
  render(
    <MemoryRouter initialEntries={[path]}>
      <Routes><Route path="/settings/:section?" element={<SettingsPage />} /></Routes>
      <LocationProbe />
    </MemoryRouter>,
  );
}

const location = () => screen.getByRole('status', { name: 'Location' });

describe('SettingsPage on a desktop', () => {
  it('opens the first section, beside the section list', () => {
    renderAt('/settings');
    expect(location()).toHaveTextContent('/settings/notifications');
    const nav = screen.getByRole('navigation', { name: 'Settings sections' });
    expect(screen.getByRole('link', { name: /Notifications/ })).toHaveAttribute('aria-current', 'page');
    expect(nav).toContainElement(screen.getByRole('link', { name: 'Run setup again' }));
    expect(screen.getByRole('heading', { name: 'Notifications' })).toBeInTheDocument();
  });

  it('goes back to the list for a section that does not exist', () => {
    renderAt('/settings/nothing');
    expect(location()).toHaveTextContent('/settings/notifications');
  });
});

describe('SettingsPage on a phone', () => {
  let matchMedia: MockInstance<typeof window.matchMedia>;
  beforeEach(() => {
    const stub = window.matchMedia;
    matchMedia = vi.spyOn(window, 'matchMedia').mockImplementation(
      (query: string) => ({ ...stub(query), matches: query === MOBILE_QUERY }),
    );
  });
  afterEach(() => matchMedia.mockRestore());

  it('shows the section list first, with the way back to setup', () => {
    renderAt('/settings');
    expect(location()).toHaveTextContent('/settings');
    expect(screen.getByRole('link', { name: /Notifications/ })).toHaveAttribute('href', '/settings/notifications');
    expect(screen.getByRole('link', { name: 'Run setup again' })).toHaveAttribute('href', '/setup');
    expect(screen.queryByRole('heading', { name: 'Notifications' })).not.toBeInTheDocument();
  });

  it('shows one section with a link back to the list', () => {
    renderAt('/settings/notifications');
    expect(screen.getByRole('heading', { name: 'Notifications' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'All settings' })).toHaveAttribute('href', '/settings');
    expect(screen.queryByRole('navigation', { name: 'Settings sections' })).not.toBeInTheDocument();
  });
});
