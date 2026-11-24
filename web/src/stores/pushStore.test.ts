import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError, useHostedStore } from '../api/hosted';
import { endpointDigest } from '../utils/webPush';
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
  useHostedStore.setState({ mode: 'local' });
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

  it('runs both steps of turning off and reads the state again when one fails', async () => {
    stubBrowser({ permission: 'granted' });
    const subscription = fakeSubscription();
    subscription.unsubscribe.mockRejectedValueOnce(new Error('unsubscribe failed'));
    stubPushBrowser({ subscription });
    api.pushDevices.mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: [device({ id: 7 })] });
    await store().load();
    api.pushDevices.mockClear();
    api.pushDevices.mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: [] });

    expect(await store().turnOff()).toBe(false);

    expect(api.deletePushDevice).toHaveBeenCalledWith(7);
    expect(api.pushDevices).toHaveBeenCalled();
    expect(thisDevice(store())).toBeUndefined();
    expect(store().busy).toBe(false);
    expect(store().error).toBe('Could not change push notifications for this browser.');

    api.deletePushDevice.mockRejectedValueOnce(new ApiError(500, '{"detail": "Database is busy."}'));
    api.pushDevices.mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: [device({ id: 7 })] });
    await store().load();
    expect(await store().turnOff()).toBe(false);
    expect(subscription.unsubscribe).toHaveBeenCalledTimes(2);
    expect(store().error).toBe('Database is busy.');
  });

  it('saves a new level only while push is on here', async () => {
    stubBrowser({ permission: 'granted' });
    stubPushBrowser({ subscription: fakeSubscription() });
    await store().setLevel('all');
    expect(api.savePushDevice).not.toHaveBeenCalled();

    api.pushDevices.mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: [device()] });
    await store().load();
    expect(await store().setLevel('all')).toBe(true);
    expect(api.savePushDevice).toHaveBeenCalledWith(expect.objectContaining({ level: 'all' }));

    api.savePushDevice.mockRejectedValueOnce(new ApiError(500, '{"detail": "Not saved."}'));
    expect(await store().setLevel('important')).toBe(false);
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

describe('pushStore in hosted mode', () => {
  const hosted = (digest: string, id = 'a1b2') => device({ id, endpoint: undefined, endpoint_digest: digest, last_success_at: undefined });

  beforeEach(() => useHostedStore.setState({ mode: 'external' }));

  it('finds this browser by the digest of its endpoint', async () => {
    stubBrowser({ permission: 'granted' });
    stubPushBrowser({ subscription: fakeSubscription() });
    const mine = hosted(await endpointDigest(ENDPOINT), 'mine');
    api.pushDevices.mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: [hosted('other-digest', 'other'), mine] });

    await store().load();

    expect(thisDevice(store())).toBe(mine);
    await store().turnOff();
    expect(api.deletePushDevice).toHaveBeenCalledWith('mine');
  });

  it('finds no browser when no digest matches', async () => {
    stubBrowser({ permission: 'granted' });
    stubPushBrowser({ subscription: fakeSubscription() });
    api.pushDevices.mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: [hosted('other-digest')] });

    await store().load();

    expect(store().endpoint).toBe(ENDPOINT);
    expect(thisDevice(store())).toBeUndefined();
  });

  it('says that push is not available, with no error, when the gateway has no push routes', async () => {
    stubPushBrowser();
    api.pushDevices.mockRejectedValue(new ApiError(404, '{"reason": "not_found", "requestId": "r-1"}'));

    await store().load();

    expect(store().available).toBe(false);
    expect(store().error).toBeNull();
  });

  it('names the limit of browsers, and no gateway body in another error', async () => {
    stubBrowser({ permission: 'granted' });
    stubPushBrowser();
    api.pushDevices.mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: [] });
    api.savePushDevice.mockRejectedValueOnce(new ApiError(409, '{"reason": "subscription_limit", "requestId": "r-1"}'));

    expect(await store().turnOn('important')).toBe(false);
    expect(store().error).toBe('Push is on in the maximum number of browsers already. Remove one first.');

    api.savePushDevice.mockRejectedValueOnce(new ApiError(400, '{"reason": "invalid_request", "requestId": "r-2"}'));
    expect(await store().turnOn('important')).toBe(false);
    expect(store().error).toBe('Could not change push notifications for this browser.');
  });

  it('sends only the endpoint, the keys and the level', async () => {
    stubBrowser({ permission: 'granted' });
    stubPushBrowser();
    api.pushDevices.mockResolvedValue({ public_key: PUBLIC_KEY, subscriptions: [] });

    await store().turnOn('all');

    expect(api.savePushDevice).toHaveBeenCalledWith({
      endpoint: ENDPOINT, keys: { p256dh: 'p256dh-key', auth: 'auth-key' }, level: 'all',
    });
  });

  it('shows an error for another failure', async () => {
    stubPushBrowser();
    api.pushDevices.mockRejectedValue(new ApiError(503, '{"reason": "unavailable", "requestId": "r-1"}'));

    await store().load();

    expect(store().available).toBe(true);
    expect(store().error).toBe('Could not load your devices.');
  });
});

describe('endpointDigest', () => {
  it('is SHA-256 of the endpoint in base64url without padding', async () => {
    // SHA-256 of "abc" (FIPS 180-2 test vector).
    expect(await endpointDigest('abc')).toBe('ungWv48Bz-pBQUDeXa4iI7ADYaOWF3qctBD_YfIAFa0');
  });
});
