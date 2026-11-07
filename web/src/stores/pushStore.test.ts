import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../api/hosted';
import {
  ENDPOINT, OTHER_KEY, PUBLIC_KEY, device, fakeSubscription, stubPushBrowser, unstubPushBrowser,
} from '../test/pushFixtures';
import { stubBrowser } from '../test/browserAlertFixtures';

vi.mock('../api/client', () => ({
  api: { pushDevices: vi.fn(), savePushDevice: vi.fn(), deletePushDevice: vi.fn() },
  getToken: vi.fn(() => 'session-token'),
  setToken: vi.fn(),
  clearToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

const client = await import('../api/client');
const { thisDevice, usePushStore } = await import('./pushStore');
const api = client.api as unknown as Record<string, ReturnType<typeof vi.fn>>;
const store = () => usePushStore.getState();

beforeEach(() => {
  vi.clearAllMocks();
  usePushStore.setState(usePushStore.getInitialState(), true);
  api.pushDevices.mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: [] });
  api.savePushDevice.mockResolvedValue(device());
  api.deletePushDevice.mockResolvedValue({ deleted: true });
});

afterEach(() => {
  unstubPushBrowser();
  vi.unstubAllGlobals();
});

describe('pushStore', () => {
  it('asks for permission, subscribes with the instance key and saves the level', async () => {
    const request = stubBrowser();
    const browser = stubPushBrowser();
    api.pushDevices
      .mockResolvedValueOnce({ public_key: PUBLIC_KEY, subscriptions: [] })
      .mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: [device()] });

    await store().turnOn('all');

    expect(request).toHaveBeenCalled();
    expect(browser.container.register).toHaveBeenCalledWith('/sw.js');
    expect(browser.pushManager.subscribe).toHaveBeenCalledWith(expect.objectContaining({ userVisibleOnly: true }));
    expect(api.savePushDevice).toHaveBeenCalledWith({
      endpoint: ENDPOINT, keys: { p256dh: 'p256dh-key', auth: 'auth-key' }, level: 'all',
    });
    expect(thisDevice(store())?.id).toBe(1);
    expect(store().busy).toBe(false);
  });

  it('does not subscribe when the browser refuses the permission', async () => {
    stubBrowser({ answer: 'denied' });
    const browser = stubPushBrowser();
    await store().turnOn('important');
    expect(browser.pushManager.subscribe).not.toHaveBeenCalled();
    expect(api.savePushDevice).not.toHaveBeenCalled();
  });

  it('subscribes again when the subscription has an old key', async () => {
    stubBrowser({ permission: 'granted' });
    const old = fakeSubscription(ENDPOINT, OTHER_KEY);
    const browser = stubPushBrowser({ subscription: old });
    await store().load();
    expect(store().endpoint).toBeNull();

    await store().turnOn('important');

    expect(old.unsubscribe).toHaveBeenCalled();
    expect(browser.pushManager.subscribe).toHaveBeenCalled();
  });

  it('turns push off on the server and in the browser', async () => {
    stubBrowser({ permission: 'granted' });
    const subscription = fakeSubscription();
    stubPushBrowser({ subscription });
    api.pushDevices.mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: [device({ id: 7 })] });
    await store().load();

    await store().turnOff();

    expect(api.deletePushDevice).toHaveBeenCalledWith(7);
    expect(subscription.unsubscribe).toHaveBeenCalled();
  });

  it('saves a new level only while push is on here', async () => {
    stubBrowser({ permission: 'granted' });
    stubPushBrowser({ subscription: fakeSubscription() });
    await store().setLevel('all');
    expect(api.savePushDevice).not.toHaveBeenCalled();

    api.pushDevices.mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: [device()] });
    await store().load();
    await store().setLevel('all');
    expect(api.savePushDevice).toHaveBeenCalledWith(expect.objectContaining({ level: 'all' }));
  });

  it('removes another browser and shows what the server refuses', async () => {
    api.pushDevices.mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: [] });
    await store().remove(3);
    expect(api.deletePushDevice).toHaveBeenCalledWith(3);

    api.deletePushDevice.mockRejectedValueOnce(new ApiError(404, '{"detail": "This browser does not get push notifications."}'));
    await store().remove(3);
    expect(store().error).toBe('This browser does not get push notifications.');
  });
});
