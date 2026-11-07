import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ALERT_BODY_LIMIT, ALERT_CHANNEL, ALERT_DELAY_MS, createAlerter, levelWants, type NotificationEvent,
} from './browserAlerts';

/** BroadcastChannel between the "tabs" of one test: a message reaches every other channel of the name. */
class FakeChannel {
  static all: FakeChannel[] = [];
  listeners: ((e: MessageEvent) => void)[] = [];
  closed = false;
  name: string;
  constructor(name: string) {
    this.name = name;
    FakeChannel.all.push(this);
  }
  postMessage(data: unknown) {
    for (const other of FakeChannel.all) {
      if (other !== this && other.name === this.name && !other.closed) {
        other.listeners.forEach((l) => l({ data } as MessageEvent));
      }
    }
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

function event(over: Partial<NotificationEvent> = {}): NotificationEvent {
  return {
    type: 'notification', notification_id: 'ask-1', notification_type: 'question', session_id: 's1',
    title: 'Deploy now?', body: 'The build passed.', priority: 'normal', options: ['Yes', 'No'], ...over,
  };
}

beforeEach(() => {
  vi.useFakeTimers();
  FakeChannel.all = [];
  FakeNotification.shown = [];
  vi.stubGlobal('BroadcastChannel', FakeChannel);
  vi.stubGlobal('Notification', FakeNotification);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
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
    expect(alert.options).toEqual({ body: 'x'.repeat(ALERT_BODY_LIMIT), tag: 'ask-1', silent: true });
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
    expect(onOpen).toHaveBeenCalledWith('ask-1');
    expect(FakeNotification.shown[0].closed).toBe(true);
  });

  it('shows nothing when the browser refuses the alert', () => {
    vi.stubGlobal('Notification', class { constructor() { throw new TypeError('Illegal constructor'); } });
    const alerter = createAlerter({ inFront: () => false, onOpen: vi.fn() });
    alerter.receive(event(), true);
    expect(() => vi.advanceTimersByTime(ALERT_DELAY_MS)).not.toThrow();
  });
});
