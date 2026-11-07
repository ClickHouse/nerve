import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/hosted';
import { useBrowserAlertStore } from '../../stores/browserAlertStore';
import { stubBrowser } from '../../test/browserAlertFixtures';
import {
  PUBLIC_KEY, device, fakeSubscription, stubPushBrowser, unstubPushBrowser,
} from '../../test/pushFixtures';

vi.mock('../../api/client', () => ({
  api: { pushDevices: vi.fn(), savePushDevice: vi.fn(), deletePushDevice: vi.fn() },
  getToken: vi.fn(() => 'session-token'),
  setToken: vi.fn(),
  clearToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

const client = await import('../../api/client');
const { usePushStore } = await import('../../stores/pushStore');
const { ThisBrowserRow } = await import('./ThisBrowserRow');
const api = client.api as unknown as Record<string, ReturnType<typeof vi.fn>>;

const PHONE = 'Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Mobile Safari/537.36';
const renderRow = () => render(<ul><ThisBrowserRow /></ul>);
const pushSwitch = () => screen.getByRole('switch', { name: 'Get notifications when no tab is open' });

function serverHas(...devices: ReturnType<typeof device>[]) {
  api.pushDevices.mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: devices });
}

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  useBrowserAlertStore.setState({ enabled: false, level: 'important', offerDone: false });
  usePushStore.setState(usePushStore.getInitialState(), true);
  serverHas();
  api.deletePushDevice.mockResolvedValue({ deleted: true });
});

afterEach(() => {
  unstubPushBrowser();
  vi.unstubAllGlobals();
});

describe('Push in the This browser row', () => {
  it('turns push on with the level of the row, which then shows', async () => {
    stubBrowser();
    stubPushBrowser();
    renderRow();
    expect(pushSwitch()).toHaveAttribute('aria-checked', 'false');
    expect(screen.queryByRole('radiogroup')).not.toBeInTheDocument();
    api.savePushDevice.mockImplementation(async () => {
      serverHas(device());
      return device();
    });

    await userEvent.click(pushSwitch());

    await waitFor(() => expect(pushSwitch()).toHaveAttribute('aria-checked', 'true'));
    expect(api.savePushDevice).toHaveBeenCalledWith(expect.objectContaining({ level: 'important' }));
    expect(screen.getByRole('switch', { name: 'Alerts while a tab is open' })).toHaveAttribute('aria-checked', 'false');
    await userEvent.click(screen.getByRole('radio', { name: 'Everything' }));
    await waitFor(() => expect(api.savePushDevice).toHaveBeenLastCalledWith(expect.objectContaining({ level: 'all' })));
    expect(useBrowserAlertStore.getState().level).toBe('all');
  });

  it('keeps the level when push does not take the new one', async () => {
    stubBrowser({ permission: 'granted' });
    stubPushBrowser({ subscription: fakeSubscription() });
    serverHas(device());
    api.savePushDevice.mockRejectedValue(new ApiError(500, '{"detail": "Not saved."}'));
    renderRow();
    await waitFor(() => expect(pushSwitch()).toHaveAttribute('aria-checked', 'true'));

    await userEvent.click(screen.getByRole('radio', { name: 'Everything' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('Not saved.');
    expect(useBrowserAlertStore.getState().level).toBe('important');
    expect(screen.getByRole('radio', { name: 'Questions and important' })).toHaveAttribute('aria-checked', 'true');
  });

  it('lists the other devices of the person, and removes one', async () => {
    stubBrowser({ permission: 'granted' });
    stubPushBrowser({ subscription: fakeSubscription() });
    serverHas(
      device({ id: 1 }),
      device({ id: 2, endpoint: 'https://fcm.googleapis.com/fcm/send/phone', user_agent: PHONE, last_success_at: new Date().toISOString() }),
    );
    renderRow();

    const list = await screen.findByRole('list', { name: 'Your other devices' });
    const rows = within(list).getAllByRole('listitem');
    expect(rows).toHaveLength(1);
    expect(rows[0]).toHaveTextContent('Chrome on Android');
    expect(rows[0]).toHaveTextContent('Last notification just now');
    expect(pushSwitch()).toHaveAttribute('aria-checked', 'true');

    serverHas(device({ id: 1 }));
    await userEvent.click(within(list).getByRole('button', { name: 'Remove Chrome on Android' }));

    expect(api.deletePushDevice).toHaveBeenCalledWith(2);
    await waitFor(() => expect(screen.queryByRole('list', { name: 'Your other devices' })).not.toBeInTheDocument());
  });

  it('shows no push switch when the browser blocks the site', async () => {
    stubBrowser({ permission: 'denied' });
    stubPushBrowser();
    serverHas(device({ id: 4, endpoint: 'https://fcm.googleapis.com/fcm/send/phone', user_agent: PHONE }));
    renderRow();
    expect(await screen.findByText('Chrome on Android')).toBeInTheDocument();
    expect(screen.queryByRole('switch', { name: 'Get notifications when no tab is open' })).not.toBeInTheDocument();
  });

  it('tells an iPhone tab to add the panel to the Home Screen', () => {
    stubBrowser({ permission: null });
    vi.spyOn(navigator, 'userAgent', 'get').mockReturnValue(
      'Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1',
    );
    renderRow();
    expect(screen.getByText('On iPhone and iPad, add the panel to the Home Screen to get notifications.')).toBeInTheDocument();
    expect(screen.queryByRole('switch')).not.toBeInTheDocument();
  });
});
