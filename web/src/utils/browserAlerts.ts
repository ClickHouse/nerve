import type { NotificationLevel } from '../api/settings';
import type { WSMessage } from '../api/websocket';
import { activeWorker, workerSupported } from './webPush';

export type NotificationEvent = Extract<WSMessage, { type: 'notification' }>;

/** The channel that the panel tabs of one browser share. */
export const ALERT_CHANNEL = 'nerve-browser-alerts';
/**
 * How long a tab in the background waits before it shows an alert. In this
 * time a tab in front, or a tab that showed the alert, tells the other tabs.
 */
export const ALERT_DELAY_MS = 300;
/** The number of body characters that an alert shows. */
export const ALERT_BODY_LIMIT = 180;
/** Nerve's app icon, and the white N that Android shows in the status bar. The same files as in `public/sw.js`. */
export const ALERT_ICON = '/nerve-icon-192.png';
export const ALERT_BADGE = '/nerve-badge.png';

const IMPORTANT_TYPES = new Set(['question', 'approval']);
const IMPORTANT_PRIORITIES = new Set(['high', 'urgent']);

/** The level rule of the server: `important` is questions, approvals, high and urgent. */
export function levelWants(level: NotificationLevel, type: string, priority: string): boolean {
  return level === 'all' || IMPORTANT_TYPES.has(type) || IMPORTANT_PRIORITIES.has(priority);
}

/** The person looks at this tab: it is visible and its window has focus. */
export function tabInFront(): boolean {
  return document.visibilityState === 'visible' && document.hasFocus();
}

/** `seen`: a tab in front shows the item. `shown`: a tab showed the alert. */
interface TabMessage {
  kind: 'seen' | 'shown';
  key: string;
}

export interface Alerter {
  /** A new item. `wanted`: the choices of this browser want an alert for it. */
  receive: (event: NotificationEvent, wanted: boolean) => void;
  /** The item was answered or expired: its alert goes away. */
  close: (id: string) => void;
  dispose: () => void;
}

/**
 * System alerts for the panel tabs of one browser, one alert for each item.
 *
 * Every tab gets each item through its WebSocket. A tab in front tells the
 * others that the person sees the item. A tab in the background waits
 * `ALERT_DELAY_MS`, then shows the alert unless another tab told it about
 * the item, and tells the others that it showed it. Two tabs can wait the
 * same time, and the channel delivers later, so a tab first takes the
 * item's Web Lock: only the tab that gets it shows the alert, and it keeps
 * the lock while the alert is open. Without the Web Locks API, the alert's
 * `tag` (the item id) makes the browser keep one alert per item.
 *
 * When the panel's service worker runs, the worker shows the alert, so an
 * alert and a push notification for the same item share the tag and the
 * browser keeps one of them. The worker handles the click (see `sw.js`).
 */
export function createAlerter({ inFront = tabInFront, onOpen }: {
  inFront?: () => boolean;
  onOpen: () => void;
}): Alerter {
  const channel = typeof BroadcastChannel === 'function' ? new BroadcastChannel(ALERT_CHANNEL) : null;
  // Keys of the items that another tab took care of. A re-delivered item has
  // a new key, so it can alert again.
  const handled = new Set<string>();
  const timers = new Map<string, ReturnType<typeof setTimeout>>();
  const open = new Map<string, Notification>();

  const cancel = (key: string) => {
    clearTimeout(timers.get(key));
    timers.delete(key);
  };
  const tell = (kind: TabMessage['kind'], key: string) => channel?.postMessage({ kind, key } satisfies TabMessage);
  const onMessage = (e: MessageEvent<TabMessage>) => {
    handled.add(e.data.key);
    cancel(e.data.key);
  };
  channel?.addEventListener('message', onMessage);

  /** Shows the alert. Resolves when the alert closes, or at once for an alert of the service worker. */
  const show = async (event: NotificationEvent, key: string) => {
    const worker = workerSupported() ? await activeWorker() : null;
    if (handled.has(key) || inFront()) return;
    const options = {
      body: event.body.slice(0, ALERT_BODY_LIMIT),
      icon: ALERT_ICON,
      badge: ALERT_BADGE,
      tag: event.notification_id,
      silent: event.silent === true,
    };
    if (!worker) return showInPage(event, key, options);
    try {
      await worker.showNotification(event.title, options);
    } catch {
      return; // the permission went away
    }
    tell('shown', key);
  };

  const showInPage = (event: NotificationEvent, key: string, options: NotificationOptions) => new Promise<void>((closed) => {
    const id = event.notification_id;
    let alert: Notification;
    try {
      alert = new Notification(event.title, options);
    } catch {
      // Some phone browsers show alerts only through a service worker.
      return closed();
    }
    alert.onclick = () => {
      window.focus();
      onOpen();
      alert.close();
    };
    alert.onclose = () => {
      if (open.get(id) === alert) open.delete(id);
      closed();
    };
    open.set(id, alert);
    tell('shown', key);
  });

  const claim = (event: NotificationEvent, key: string) => {
    if (!navigator.locks) return void show(event, key);
    void navigator.locks.request(`${ALERT_CHANNEL}:${key}`, { ifAvailable: true }, (lock) => (
      lock ? show(event, key) : undefined
    ));
  };

  return {
    receive: (event, wanted) => {
      const key = `${event.notification_id}:${event.redelivery_count ?? 0}`;
      if (inFront()) {
        tell('seen', key);
        return;
      }
      if (!wanted || handled.has(key) || timers.has(key)) return;
      timers.set(key, setTimeout(() => {
        timers.delete(key);
        claim(event, key);
      }, ALERT_DELAY_MS));
    },
    close: (id) => {
      for (const key of [...timers.keys()]) if (key.startsWith(`${id}:`)) cancel(key);
      open.get(id)?.close();
      open.delete(id);
      void activeWorker().then((worker) => worker?.getNotifications({ tag: id }))
        .then((shown) => shown?.forEach((alert) => alert.close()));
    },
    dispose: () => {
      for (const key of [...timers.keys()]) cancel(key);
      channel?.removeEventListener('message', onMessage);
      channel?.close();
    },
  };
}
