import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ALERT_BADGE, ALERT_BODY_LIMIT, ALERT_CHANNEL, ALERT_DELAY_MS, ALERT_ICON, createAlerter, levelWants,
  type NotificationEvent,
} from './browserAlerts';
import { stubPushBrowser, unstubPushBrowser } from '../test/pushFixtures';

/**
 * BroadcastChannel between the "tabs" of one test: a message reaches every
 * other channel of the name, at once or (`later`) in a microtask, as in a browser.
 */
class FakeChannel {
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

class FakeNotification {
  static shown: FakeNotification[] = [];
  onclick: (() => void) | null = null;
  onclose: (() => void) | null = null;
  closed = false;
  title: string;
  options: NotificationOptions;
  constructor(title: string, options: NotificationOptions) {
    this.title = title;
    this.options = options;
    FakeNotification.shown.push(this);
  }
  close() { this.closed = true; this.onclose?.(); }
}

/** The Web Locks API of one browser, for `ifAvailable` requests. Returns the names that are held. */
function stubLocks(): Set<string> {
  const held = new Set<string>();
  const request = async (name: string, _options: LockOptions, callback: (lock: Lock | null) => unknown) => {
    if (held.has(name)) return callback(null);
    held.add(name);
    try {
      return await callback({ name, mode: 'exclusive' });
    } finally {
      held.delete(name);
    }
  };
  Object.defineProperty(navigator, 'locks', { configurable: true, value: { request } });
  return held;
}

function event(over: Partial<NotificationEvent> = {}): NotificationEvent {
  return {
    type: 'notification', notification_id: 'ask-1', notification_type: 'question', session_id: 's1',
    title: 'Deploy now?', body: 'The build passed.', priority: 'normal', options: ['Yes', 'No'], ...over,
  };
}

beforeEach(() => {
  vi.useFakeTimers();
  FakeChannel.all = [];
  FakeChannel.later = false;
  FakeNotification.shown = [];
  vi.stubGlobal('BroadcastChannel', FakeChannel);
  vi.stubGlobal('Notification', FakeNotification);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  unstubPushBrowser();
  Reflect.deleteProperty(navigator, 'locks');
});

describe('levelWants', () => {
  it('lets questions, approvals, high and urgent through at the important level', () => {
    expect(levelWants('important', 'question', 'normal')).toBe(true);
    expect(levelWants('important', 'approval', 'low')).toBe(true);
    expect(levelWants('important', 'notify', 'high')).toBe(true);
    expect(levelWants('important', 'notify', 'urgent')).toBe(true);
    expect(levelWants('important', 'notify', 'normal')).toBe(false);
    expect(levelWants('all', 'notify', 'low')).toBe(true);
  });
});

describe('createAlerter', () => {
  it('shows one alert after the delay in a tab in the background', () => {
    const alerter = createAlerter({ inFront: () => false, onOpen: vi.fn() });
    alerter.receive(event({ body: 'x'.repeat(500), silent: true }), true);
    expect(FakeNotification.shown).toHaveLength(0);

    vi.advanceTimersByTime(ALERT_DELAY_MS);

    expect(FakeNotification.shown).toHaveLength(1);
    const [alert] = FakeNotification.shown;
    expect(alert.title).toBe('Deploy now?');
    expect(alert.options).toEqual({
      body: 'x'.repeat(ALERT_BODY_LIMIT), icon: ALERT_ICON, badge: ALERT_BADGE, tag: 'ask-1', silent: true,
    });
    expect(FakeChannel.all[0].name).toBe(ALERT_CHANNEL);
  });

  it('makes a sound when the event is not silent', () => {
    createAlerter({ inFront: () => false, onOpen: vi.fn() }).receive(event(), true);
    vi.advanceTimersByTime(ALERT_DELAY_MS);
    expect(FakeNotification.shown[0].options.silent).toBe(false);
  });

  it('shows nothing for an item the choices do not want', () => {
    createAlerter({ inFront: () => false, onOpen: vi.fn() }).receive(event(), false);
    vi.advanceTimersByTime(ALERT_DELAY_MS);
    expect(FakeNotification.shown).toHaveLength(0);
  });

  it('shows nothing in any tab when a tab in front shows the item', () => {
    const background = createAlerter({ inFront: () => false, onOpen: vi.fn() });
    const front = createAlerter({ inFront: () => true, onOpen: vi.fn() });
    background.receive(event(), true);
    front.receive(event(), true);

    vi.advanceTimersByTime(ALERT_DELAY_MS);

    expect(FakeNotification.shown).toHaveLength(0);
  });

  it('shows one alert for two tabs in the background', () => {
    const first = createAlerter({ inFront: () => false, onOpen: vi.fn() });
    const second = createAlerter({ inFront: () => false, onOpen: vi.fn() });
    first.receive(event(), true);
    vi.advanceTimersByTime(ALERT_DELAY_MS);
    second.receive(event(), true);
    vi.advanceTimersByTime(ALERT_DELAY_MS);

    expect(FakeNotification.shown).toHaveLength(1);
  });

  it('shows one alert when two tabs in the background wait the same time', async () => {
    FakeChannel.later = true;
    const held = stubLocks();
    const first = createAlerter({ inFront: () => false, onOpen: vi.fn() });
    const second = createAlerter({ inFront: () => false, onOpen: vi.fn() });
    first.receive(event(), true);
    second.receive(event(), true);

    vi.advanceTimersByTime(ALERT_DELAY_MS);
    await Promise.resolve();

    expect(FakeNotification.shown).toHaveLength(1);
    // The tab keeps the item's lock while its alert is open.
    expect(held).toEqual(new Set([`${ALERT_CHANNEL}:ask-1:0`]));
    first.close('ask-1');
    second.close('ask-1');
    await vi.waitFor(() => expect(held.size).toBe(0));
  });

  it('alerts again for a re-delivered item', () => {
    const first = createAlerter({ inFront: () => false, onOpen: vi.fn() });
    const second = createAlerter({ inFront: () => false, onOpen: vi.fn() });
    first.receive(event(), true);
    vi.advanceTimersByTime(ALERT_DELAY_MS);
    second.receive(event({ redelivered: true, redelivery_count: 1 }), true);
    vi.advanceTimersByTime(ALERT_DELAY_MS);

    expect(FakeNotification.shown).toHaveLength(2);
  });

  it('closes the alert when the item is answered elsewhere, and cancels one that waits', () => {
    const alerter = createAlerter({ inFront: () => false, onOpen: vi.fn() });
    alerter.receive(event(), true);
    vi.advanceTimersByTime(ALERT_DELAY_MS);
    alerter.receive(event({ notification_id: 'ask-2' }), true);

    alerter.close('ask-1');
    alerter.close('ask-2');
    vi.advanceTimersByTime(ALERT_DELAY_MS);

    expect(FakeNotification.shown).toHaveLength(1);
    expect(FakeNotification.shown[0].closed).toBe(true);
  });

  it('focuses the tab and opens the item on a click', () => {
    const onOpen = vi.fn();
    const focus = vi.spyOn(window, 'focus').mockImplementation(() => {});
    createAlerter({ inFront: () => false, onOpen }).receive(event(), true);
    vi.advanceTimersByTime(ALERT_DELAY_MS);

    FakeNotification.shown[0].onclick?.();

    expect(focus).toHaveBeenCalled();
    expect(onOpen).toHaveBeenCalledTimes(1);
    expect(FakeNotification.shown[0].closed).toBe(true);
  });

  it('shows the alert through the service worker when it runs, and closes it by tag', async () => {
    const worker = stubPushBrowser();
    const alerter = createAlerter({ inFront: () => false, onOpen: vi.fn() });
    alerter.receive(event({ silent: true }), true);

    await vi.advanceTimersByTimeAsync(ALERT_DELAY_MS);

    expect(FakeNotification.shown).toHaveLength(0);
    expect(worker.registration.showNotification).toHaveBeenCalledWith(
      'Deploy now?', { body: 'The build passed.', icon: ALERT_ICON, badge: ALERT_BADGE, tag: 'ask-1', silent: true },
    );
    const shown = { close: vi.fn() };
    worker.registration.getNotifications.mockResolvedValue([shown]);
    alerter.close('ask-1');
    await vi.waitFor(() => expect(shown.close).toHaveBeenCalled());
    expect(worker.registration.getNotifications).toHaveBeenCalledWith({ tag: 'ask-1' });
  });

  it('shows nothing when the browser refuses the alert', () => {
    vi.stubGlobal('Notification', class { constructor() { throw new TypeError('Illegal constructor'); } });
    const alerter = createAlerter({ inFront: () => false, onOpen: vi.fn() });
    alerter.receive(event(), true);
    expect(() => vi.advanceTimersByTime(ALERT_DELAY_MS)).not.toThrow();
  });
});
