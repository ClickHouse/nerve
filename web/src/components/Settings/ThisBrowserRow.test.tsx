import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { NotificationSettings } from '../../api/settings';
import { useBrowserAlertStore } from '../../stores/browserAlertStore';
import { useSettingsStore } from '../../stores/settingsStore';
import { stubBrowser } from '../../test/browserAlertFixtures';
import { ThisBrowserRow } from './ThisBrowserRow';

const renderRow = () => render(<ul><ThisBrowserRow /></ul>);

function quietHours(start: string, end: string) {
  const field = (value: string) => ({ value, state: 'editable' as const, file: 'settings.yaml', reason: null });
  useSettingsStore.setState({
    notifications: { fields: { quiet_start: field(start), quiet_end: field(end) } } as unknown as NotificationSettings,
  });
}

beforeEach(() => {
  localStorage.clear();
  useBrowserAlertStore.setState({ enabled: false, level: 'important', offerDone: false });
  useSettingsStore.setState(useSettingsStore.getInitialState(), true);
});

afterEach(() => vi.unstubAllGlobals());

describe('ThisBrowserRow', () => {
  it('turns alerts on after the browser allows them, then offers the level', async () => {
    const request = stubBrowser();
    renderRow();
    const toggle = screen.getByRole('switch', { name: 'Alerts while a tab is open' });
    expect(toggle).toHaveAttribute('aria-checked', 'false');
    expect(screen.queryByRole('radiogroup')).not.toBeInTheDocument();

    await userEvent.click(toggle);

    expect(request).toHaveBeenCalled();
    await waitFor(() => expect(toggle).toHaveAttribute('aria-checked', 'true'));
    expect(screen.getByRole('radio', { name: 'Questions and important' })).toHaveAttribute('aria-checked', 'true');
    await userEvent.click(screen.getByRole('radio', { name: 'Everything' }));
    expect(useBrowserAlertStore.getState().level).toBe('all');
    expect(screen.getByText('Applies to this browser only')).toBeInTheDocument();
  });

  it('turns alerts off', async () => {
    stubBrowser({ permission: 'granted' });
    useBrowserAlertStore.setState({ enabled: true });
    renderRow();

    await userEvent.click(screen.getByRole('switch', { name: 'Alerts while a tab is open' }));

    expect(useBrowserAlertStore.getState().enabled).toBe(false);
  });

  it('says that alerts are silent in quiet hours', () => {
    stubBrowser({ permission: 'granted' });
    useBrowserAlertStore.setState({ enabled: true });
    quietHours('22:00', '07:00');
    renderRow();
    expect(screen.getByText('During quiet hours, alerts make no sound unless urgent.')).toBeInTheDocument();
  });

  it.each([
    [{ secure: false }, 'Needs HTTPS', 'Open the panel over HTTPS'],
    [{ permission: 'denied' as const }, 'Blocked', 'Allow notifications in the site settings'],
    [{ permission: null }, 'Not supported', 'cannot show alerts'],
  ])('shows why alerts are not available (%o)', (browser, badge, help) => {
    stubBrowser(browser);
    renderRow();
    expect(screen.queryByRole('switch')).not.toBeInTheDocument();
    expect(screen.getByText(badge)).toBeInTheDocument();
    expect(screen.getByText(help, { exact: false })).toBeInTheDocument();
  });
});
