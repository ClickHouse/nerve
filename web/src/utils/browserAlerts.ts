import type { NotificationLevel } from '../api/settings';
import type { WSMessage } from '../api/websocket';

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
 * the item, and tells the others that it showed it. The alert's `tag` is
 * the item id, so the browser keeps one alert per item if two tabs show it
 * at the same time.
 */
export function createAlerter({ inFront = tabInFront, onOpen }: {
  inFront?: () => boolean;
  onOpen: (id: string) => void;
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

  const show = (event: NotificationEvent, key: string) => {
    const id = event.notification_id;
    let alert: Notification;
    try {
      alert = new Notification(event.title, {
        body: event.body.slice(0, ALERT_BODY_LIMIT),
        tag: id,
        silent: event.silent === true,
      });
    } catch {
      // Some phone browsers show alerts only through a service worker.
      return;
    }
    alert.onclick = () => {
      window.focus();
      onOpen(id);
      alert.close();
    };
    alert.onclose = () => { if (open.get(id) === alert) open.delete(id); };
    open.set(id, alert);
    tell('shown', key);
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
        if (!handled.has(key) && !inFront()) show(event, key);
      }, ALERT_DELAY_MS));
    },
    close: (id) => {
      for (const key of [...timers.keys()]) if (key.startsWith(`${id}:`)) cancel(key);
      open.get(id)?.close();
      open.delete(id);
    },
    dispose: () => {
      for (const key of [...timers.keys()]) cancel(key);
      channel?.removeEventListener('message', onMessage);
      channel?.close();
    },
  };
}
