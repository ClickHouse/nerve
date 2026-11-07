import { vi } from 'vitest';
import { useBrowserAlertStore } from '../stores/browserAlertStore';

/** A browser with the Notification API at `permission`; `answer` is what the permission prompt gives. */
export function stubBrowser({ secure = true, permission = 'default', answer = 'granted' }: {
  secure?: boolean;
  permission?: NotificationPermission | null;
  answer?: NotificationPermission;
} = {}) {
  vi.stubGlobal('isSecureContext', secure);
  const requestPermission = vi.fn(async () => answer);
  vi.stubGlobal('Notification', permission === null ? undefined : Object.assign(function Notification() {}, {
    permission, requestPermission,
  }));
  useBrowserAlertStore.getState().refresh();
  return requestPermission;
}

/**
 * BroadcastChannel between the "tabs" of one test: a message reaches every
 * other channel of the name, at once or (`later`) in a microtask, as in a browser.
 */
export class FakeChannel {
  static all: FakeChannel[] = [];
  static later = false;
  listeners: ((e: MessageEvent) => void)[] = [];
  closed = false;
  name: string;
  constructor(name: string) {
    this.name = name;
    FakeChannel.all.push(this);
  }
  postMessage(data: unknown) {
    const deliver = () => {
      for (const other of FakeChannel.all) {
        if (other !== this && other.name === this.name && !other.closed) {
          other.listeners.forEach((l) => l({ data } as MessageEvent));
        }
      }
    };
    if (FakeChannel.later) queueMicrotask(deliver);
    else deliver();
  }
  addEventListener(_type: string, listener: (e: MessageEvent) => void) { this.listeners.push(listener); }
  removeEventListener(_type: string, listener: (e: MessageEvent) => void) {
    this.listeners = this.listeners.filter((l) => l !== listener);
  }
  close() { this.closed = true; }
}
