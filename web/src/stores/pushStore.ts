import { create } from 'zustand';
import { api } from '../api/client';
import { ApiError } from '../api/hosted';
import type { PushDevice } from '../api/push';
import type { NotificationLevel } from '../api/settings';
import {
  base64UrlBytes, browserSubscription, madeWithKey, pushSupported, registerWorker,
} from '../utils/webPush';
import { errorDetail } from './accountStore';

const LOAD_FAILED = 'Could not load your devices.';
const CHANGE_FAILED = 'Could not change push notifications for this browser.';

export interface PushState {
  /** The browsers of this person that get push notifications. */
  devices: PushDevice[];
  /** The endpoint of this browser's subscription, when it uses the instance's current key. */
  endpoint: string | null;
  publicKey: string | null;
  busy: boolean;
  error: string | null;
  load: () => Promise<void>;
  /** Asks for permission when the browser did not ask yet, subscribes and saves the subscription. */
  turnOn: (level: NotificationLevel) => Promise<void>;
  turnOff: () => Promise<void>;
  /** Saves the level of this browser, when push is on. */
  setLevel: (level: NotificationLevel) => Promise<void>;
  /** Turns push off for another browser of this person. */
  remove: (id: number) => Promise<void>;
}

/** The server's row of this browser, when push is on here. */
export function thisDevice(state: Pick<PushState, 'devices' | 'endpoint'>): PushDevice | undefined {
  return state.endpoint ? state.devices.find((d) => d.endpoint === state.endpoint) : undefined;
}

function failure(e: unknown): string {
  return e instanceof ApiError ? errorDetail(e, CHANGE_FAILED) : CHANGE_FAILED;
}

async function save(subscription: PushSubscription, level: NotificationLevel): Promise<void> {
  const { endpoint, keys } = subscription.toJSON();
  if (!endpoint || !keys?.p256dh || !keys.auth) throw new Error('incomplete subscription');
  await api.savePushDevice({ endpoint, keys: { p256dh: keys.p256dh, auth: keys.auth }, level });
}

/**
 * Web Push for this browser and the list of this person's browsers. The
 * browser keeps its subscription; the server keeps one row per browser.
 * Push is on here when both have it.
 */
export const usePushStore = create<PushState>((set, get) => {
  /** Runs one change, then reads the devices again. */
  const change = async (work: () => Promise<void>) => {
    set({ busy: true, error: null });
    try {
      await work();
      await get().load();
    } catch (e) {
      set({ error: failure(e) });
    } finally {
      set({ busy: false });
    }
  };

  return {
    devices: [],
    endpoint: null,
    publicKey: null,
    busy: false,
    error: null,
    load: async () => {
      try {
        const [{ public_key, subscriptions }, subscription] = await Promise.all([
          api.pushDevices(), browserSubscription(),
        ]);
        const current = subscription && madeWithKey(subscription, public_key) ? subscription.endpoint : null;
        set({ devices: subscriptions, publicKey: public_key, endpoint: current });
      } catch {
        set({ error: LOAD_FAILED });
      }
    },
    turnOn: (level) => change(async () => {
      if (!pushSupported()) return;
      const permission = Notification.permission === 'default'
        ? await Notification.requestPermission()
        : Notification.permission;
      if (permission !== 'granted') return;
      const registration = await registerWorker();
      if (!registration) throw new Error('no service worker');
      const publicKey = get().publicKey ?? (await api.pushDevices()).public_key;
      let subscription = await registration.pushManager.getSubscription();
      // The instance has a new key (for example after a restore): subscribe again.
      if (subscription && !madeWithKey(subscription, publicKey)) {
        await subscription.unsubscribe();
        subscription = null;
      }
      subscription ??= await registration.pushManager.subscribe({
        userVisibleOnly: true, applicationServerKey: base64UrlBytes(publicKey),
      });
      await save(subscription, level);
    }),
    turnOff: () => change(async () => {
      const device = thisDevice(get());
      if (device) await api.deletePushDevice(device.id);
      await (await browserSubscription())?.unsubscribe();
    }),
    setLevel: async (level) => {
      const subscription = thisDevice(get()) ? await browserSubscription() : null;
      if (subscription) await change(() => save(subscription, level));
    },
    remove: (id) => change(async () => {
      await api.deletePushDevice(id);
    }),
  };
});
