import { act, render, screen } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { WSMessage } from '../../api/websocket';
import { useBrowserAlertStore } from '../../stores/browserAlertStore';
import { ALERT_DELAY_MS } from '../../utils/browserAlerts';

const handlers: ((msg: WSMessage) => void)[] = [];
vi.mock('../../api/websocket', () => ({
  ws: { onMessage: vi.fn((h: (msg: WSMessage) => void) => { handlers.push(h); return () => {}; }) },
}));

const { BrowserAlerts } = await import('./BrowserAlerts');

class FakeNotification {
  static shown: FakeNotification[] = [];
  static permission = 'granted';
  onclick: (() => void) | null = null;
  onclose: (() => void) | null = null;
  closed = false;
  title: string;
  constructor(title: string) {
    this.title = title;
    FakeNotification.shown.push(this);
  }
  close() { this.closed = true; }
}

let front = true;
function setFront(next: boolean) {
  front = next;
  act(() => { document.dispatchEvent(new Event('visibilitychange')); });
}

type NotificationEvent = Extract<WSMessage, { type: 'notification' }>;

/** A `notification` event (with `over` applied) to every WebSocket handler. */
function send(over: Partial<NotificationEvent> = {}) {
  dispatch({
    type: 'notification', notification_id: 'ask-1', notification_type: 'question', session_id: 's1',
    title: 'Deploy now?', body: '', priority: 'normal', options: null, ...over,
  });
}

function dispatch(msg: WSMessage) {
  act(() => handlers.forEach((h) => h(msg)));
}

function Where() {
  return <p data-testid="path">{useLocation().pathname}</p>;
}

function renderAlerts() {
  render(<MemoryRouter initialEntries={['/chat']}><BrowserAlerts /><Where /></MemoryRouter>);
}

beforeEach(() => {
  handlers.length = 0;
  FakeNotification.shown = [];
  front = true;
  document.title = 'Nerve';
  vi.spyOn(document, 'hasFocus').mockImplementation(() => front);
  Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => (front ? 'visible' : 'hidden') });
  localStorage.clear();
  useBrowserAlertStore.setState({ enabled: false, level: 'important', offerDone: false });
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('BrowserAlerts', () => {
  it('counts the items that arrive while the tab is not in front', async () => {
    renderAlerts();
    send({});
    expect(document.title).toBe('Nerve');

    setFront(false);
    send({ notification_id: 'ask-2' });
    send({ notification_id: 'n-3', notification_type: 'notify', silenced: true });
    expect(document.title).toBe('(1) Nerve');
    send({ notification_id: 'n-4', notification_type: 'notify' });
    expect(document.title).toBe('(2) Nerve');

    // A page that sets its own title keeps the count.
    act(() => { document.title = 'Fix the login test'; });
    await Promise.resolve();
    expect(document.title).toBe('(2) Fix the login test');

    setFront(true);
    expect(document.title).toBe('Fix the login test');
  });

  it('puts a dot on the favicon while items wait, and takes it away on return', async () => {
    const link = Object.assign(document.createElement('link'), { rel: 'icon', href: '/favicon.ico' });
    document.head.append(link);
    const context = { fillStyle: '', drawImage: vi.fn(), beginPath: vi.fn(), arc: vi.fn(), fill: vi.fn() };
    vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockReturnValue(context as unknown as CanvasRenderingContext2D);
    vi.spyOn(HTMLCanvasElement.prototype, 'toDataURL').mockReturnValue('data:image/png;base64,ZG90');
    vi.stubGlobal('Image', class {
      onload: (() => void) | null = null;
      onerror: (() => void) | null = null;
      set src(_url: string) { queueMicrotask(() => this.onerror?.()); }
    });
    renderAlerts();
    setFront(false);

    send({});
    await act(async () => { await Promise.resolve(); });

    expect(link.getAttribute('href')).toBe('data:image/png;base64,ZG90');
    expect(context.arc).toHaveBeenCalled();
    expect(context.drawImage).not.toHaveBeenCalled();
    setFront(true);
    expect(link.getAttribute('href')).toBe('/favicon.ico');
    link.remove();
  });

  it('shows an alert while the tab is not in front, opens the notifications on a click, closes it on an answer', () => {
    vi.useFakeTimers();
    vi.stubGlobal('isSecureContext', true);
    vi.stubGlobal('Notification', FakeNotification);
    vi.spyOn(window, 'focus').mockImplementation(() => {});
    useBrowserAlertStore.setState({ enabled: true, support: 'granted' });
    renderAlerts();
    setFront(false);

    send({ notification_id: 'n-1', notification_type: 'notify' });
    send({});
    send({ notification_id: 'ask-2', title: 'Merge it?' });
    act(() => { vi.advanceTimersByTime(ALERT_DELAY_MS); });

    // The level "Questions and important" leaves out a plain notify.
    expect(FakeNotification.shown.map((n) => n.title)).toEqual(['Deploy now?', 'Merge it?']);
    act(() => FakeNotification.shown[0].onclick?.());
    expect(screen.getByTestId('path')).toHaveTextContent('/notifications');

    expect(FakeNotification.shown[1].closed).toBe(false);
    dispatch({ type: 'notification_answered', notification_id: 'ask-2', session_id: 's1', answer: 'Yes', answered_by: 'telegram' });
    expect(FakeNotification.shown[1].closed).toBe(true);
  });

  it('shows no alert while alerts are off', () => {
    vi.useFakeTimers();
    vi.stubGlobal('isSecureContext', true);
    vi.stubGlobal('Notification', FakeNotification);
    renderAlerts();
    setFront(false);
    send({});
    act(() => { vi.advanceTimersByTime(ALERT_DELAY_MS); });
    expect(FakeNotification.shown).toHaveLength(0);
  });
});
