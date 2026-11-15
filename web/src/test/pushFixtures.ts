import { vi } from 'vitest';
import type { PushDevice } from '../api/push';

/** A VAPID public key: an uncompressed P-256 point of 65 bytes, base64url. */
export const PUBLIC_KEY = 'B' + 'A'.repeat(86);
export const OTHER_KEY = 'B' + 'Q'.repeat(86);
export const ENDPOINT = 'https://fcm.googleapis.com/fcm/send/this-browser';

function keyBytes(key: string): ArrayBuffer {
  const binary = atob(key.replace(/-/g, '+').replace(/_/g, '/') + '=');
  return Uint8Array.from(binary, (c) => c.charCodeAt(0)).buffer;
}

export function fakeSubscription(endpoint = ENDPOINT, key = PUBLIC_KEY) {
  return {
    endpoint,
    options: { applicationServerKey: keyBytes(key) },
    toJSON: () => ({ endpoint, keys: { p256dh: 'p256dh-key', auth: 'auth-key' } }),
    unsubscribe: vi.fn(async () => true),
  };
}

export type FakeSubscription = ReturnType<typeof fakeSubscription>;

/**
 * A browser with service workers and the Push API. `subscription` is the
 * browser's push subscription; `subscribe()` makes a new one with the key
 * it gets. The registration records the notifications that it shows.
 */
export function stubPushBrowser({ subscription = null as FakeSubscription | null, active = true } = {}) {
  const state = { subscription };
  const pushManager = {
    getSubscription: vi.fn(async () => state.subscription),
    subscribe: vi.fn(async ({ applicationServerKey }: { applicationServerKey: Uint8Array }) => {
      const key = btoa(String.fromCharCode(...applicationServerKey)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
      state.subscription = fakeSubscription(ENDPOINT, key);
      return state.subscription;
    }),
  };
  const registration = {
    active: active ? {} : null,
    pushManager,
    showNotification: vi.fn(async () => undefined),
    getNotifications: vi.fn(async () => [] as { close: () => void }[]),
  };
  const container = {
    register: vi.fn(async () => registration),
    ready: Promise.resolve(registration),
    getRegistration: vi.fn(async () => registration),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
  };
  vi.stubGlobal('isSecureContext', true);
  vi.stubGlobal('PushManager', function PushManager() {});
  Object.defineProperty(navigator, 'serviceWorker', { value: container, configurable: true });
  return { state, pushManager, registration, container };
}

/** Removes what `stubPushBrowser` added to `navigator`; `vi.unstubAllGlobals()` does the rest. */
export function unstubPushBrowser() {
  Reflect.deleteProperty(navigator, 'serviceWorker');
}

export function device(overrides: Partial<PushDevice> = {}): PushDevice {
  return {
    id: 1,
    endpoint: ENDPOINT,
    level: 'important',
    user_agent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36',
    created_at: '2026-11-05T09:00:00+00:00',
    last_success_at: null,
    ...overrides,
  };
}
