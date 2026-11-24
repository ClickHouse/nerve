import { create } from 'zustand';
import { api } from '../api/client';
import { ApiError, isHosted } from '../api/hosted';
import type { PushDevice } from '../api/push';
import type { NotificationLevel } from '../api/settings';
import {
  base64UrlBytes, browserSubscription, endpointDigest, madeWithKey, pushSupported, registerWorker,
} from '../utils/webPush';
import { errorDetail } from './accountStore';

const LOAD_FAILED = 'Could not load your devices.';
const CHANGE_FAILED = 'Could not change push notifications for this browser.';
/** The reason of the gateway when the person has push on in its maximum of browsers. */
const SUBSCRIPTION_LIMIT = 'subscription_limit';
const TOO_MANY = 'Push is on in the maximum number of browsers already. Remove one first.';
/** The reason of the gateway when it has no push routes for this agent. */
const NOT_FOUND = 'not_found';

export interface PushState {
  /** The browsers of this person that get push notifications. */
  devices: PushDevice[];
  /** The endpoint of this browser's subscription, when it uses the instance's current key. */
  endpoint: string | null;
  /** SHA-256 of `endpoint` (base64url), in hosted mode only: the gateway lists browsers by it. */
  endpointDigest: string | null;
  /** False when the gateway offers no push for this agent (hosted mode). */
  available: boolean;
  publicKey: string | null;
  busy: boolean;
  error: string | null;
  load: () => Promise<void>;
  /** Asks for permission when the browser did not ask yet, subscribes and saves the subscription. */
  turnOn: (level: NotificationLevel) => Promise<boolean>;
  turnOff: () => Promise<boolean>;
  /** Saves the level of this browser when push is on here. False when the save failed. */
  setLevel: (level: NotificationLevel) => Promise<boolean>;
  /** Turns push off for another browser of this person. */
  remove: (id: number | string) => Promise<boolean>;
}

/**
 * The server's row of this browser, when push is on here. A local row names
 * its endpoint; a row of the gateway names only the digest of its endpoint.
 */
export function thisDevice(
  state: Pick<PushState, 'devices' | 'endpoint' | 'endpointDigest'>,
): PushDevice | undefined {
  if (!state.endpoint) return undefined;
  return state.devices.find((d) => (d.endpoint !== undefined
    ? d.endpoint === state.endpoint
    : d.endpoint_digest !== undefined && d.endpoint_digest === state.endpointDigest));
}

/** The gateway has no push routes for this agent, for example with push switched off. */
function notOffered(e: unknown): boolean {
  return isHosted() && e instanceof ApiError && e.status === 404 && e.reason === NOT_FOUND;
}

function failure(e: unknown): string {
  if (!(e instanceof ApiError)) return CHANGE_FAILED;
  // A gateway error body has a reason code and no text for people.
  if (e.reason === SUBSCRIPTION_LIMIT) return TOO_MANY;
  return e.reason === null ? errorDetail(e, CHANGE_FAILED) : CHANGE_FAILED;
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
  /** Runs one change and reads the devices again, also after a failure. True when the change worked. */
  const change = async (work: () => Promise<void>): Promise<boolean> => {
    set({ busy: true, error: null });
    let ok = true;
    try {
      await work();
    } catch (e) {
      ok = false;
      set({ error: failure(e) });
    } finally {
      await get().load();
      set({ busy: false });
    }
    return ok;
  };

  return {
    devices: [],
    endpoint: null,
    endpointDigest: null,
    available: true,
    publicKey: null,
    busy: false,
    error: null,
    load: async () => {
      try {
        const [{ public_key, subscriptions }, subscription] = await Promise.all([
          api.pushDevices(), browserSubscription(),
        ]);
        const current = subscription && madeWithKey(subscription, public_key) ? subscription.endpoint : null;
        const digest = current && isHosted() ? await endpointDigest(current) : null;
        set({
          devices: subscriptions, publicKey: public_key, endpoint: current, endpointDigest: digest, available: true,
        });
      } catch (e) {
        if (notOffered(e)) {
          set({ devices: [], endpoint: null, endpointDigest: null, available: false, error: null });
          return;
        }
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
    // Both steps run, also when the other one fails.
    turnOff: () => change(async () => {
      const device = thisDevice(get());
      const [row, browser] = await Promise.allSettled([
        device ? api.deletePushDevice(device.id) : null,
        browserSubscription().then((subscription) => subscription?.unsubscribe()),
      ]);
      if (row.status === 'rejected') throw row.reason;
      if (browser.status === 'rejected') throw browser.reason;
    }),
    setLevel: async (level) => {
      const subscription = thisDevice(get()) ? await browserSubscription() : null;
      return subscription ? change(() => save(subscription, level)) : true;
    },
    remove: (id) => change(async () => {
      await api.deletePushDevice(id);
    }),
  };
});
