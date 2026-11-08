import { act, render, screen } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../../api/client';
import type { WSMessage } from '../../api/websocket';
import { useBrowserAlertStore } from '../../stores/browserAlertStore';
import { useChatStore } from '../../stores/chatStore';
import { useNotificationStore } from '../../stores/notificationStore';
import type { Session, SessionAttention } from '../../types/chat';
import { ALERT_CHANNEL, ALERT_DELAY_MS, tellNotificationsChanged } from '../../utils/browserAlerts';
import { ANSWER_DOT, UNREAD_DOT } from '../../utils/tabAttention';
import { OPEN_MESSAGE } from '../../utils/webPush';
import { FakeChannel } from '../../test/browserAlertFixtures';
import { stubPushBrowser, unstubPushBrowser } from '../../test/pushFixtures';

const handlers: ((msg: WSMessage) => void)[] = [];
vi.mock('../../api/websocket', () => ({
  ws: { onMessage: vi.fn((h: (msg: WSMessage) => void) => { handlers.push(h); return () => {}; }) },
}));
vi.mock('../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../api/client')>()),
  api: {
    listNotifications: vi.fn(async () => ({ notifications: [], pending_count: 0, pending_answer_count: 0 })),
    dismissNotification: vi.fn(async () => ({})),
    dismissAllNotifications: vi.fn(async () => ({})),
  },
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

const NONE: SessionAttention = { questions: 0, approvals: 0, plans: 0, awaiting_input: false, review: false };

/** The session list after a reload, with one session that has `attention`. */
function setSessions(attention?: Partial<SessionAttention>) {
  const sessions: Session[] = attention
    ? [{ id: 's1', title: 'Deploy', source: 'web', updated_at: '2026-11-07T10:00:00Z', attention: { ...NONE, ...attention } }]
    : [];
  act(() => useChatStore.setState({ sessions }));
}

function setUnread(pendingCount: number) {
  act(() => useNotificationStore.setState({ pendingCount }));
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
  useChatStore.setState({ sessions: [] });
  useNotificationStore.setState({ pendingCount: 0, pendingAnswerCount: 0, notifications: [], toastQueue: [], filter: 'pending' });
  FakeChannel.all = [];
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  unstubPushBrowser();
});

describe('BrowserAlerts', () => {
  it('registers the service worker and opens the page that it names after a click', async () => {
    const worker = stubPushBrowser();
    renderAlerts();
    await act(async () => { await Promise.resolve(); });
    expect(worker.container.register).toHaveBeenCalledWith('/sw.js');

    const [, onMessage] = worker.container.addEventListener.mock.calls[0] as [string, (e: MessageEvent) => void];
    act(() => onMessage({ data: { type: OPEN_MESSAGE, url: '/notifications' } } as MessageEvent));
    expect(screen.getByTestId('path')).toHaveTextContent('/notifications');
  });

  it('puts "!" before the title while an answer waits, else the unread count, on every page title', async () => {
    renderAlerts();
    expect(document.title).toBe('Nerve');

    // A question arrives: the count of pending answers gives "!" before the session list reloads.
    act(() => useNotificationStore.getState().handleWSNotification({
      notification_id: 'ask-1', notification_type: 'question', session_id: 's1', title: 'Deploy now?', priority: 'normal',
    }));
    expect(document.title).toBe('! Nerve');
    setSessions({ questions: 1 });
    expect(document.title).toBe('! Nerve');

    // A page that sets its own title gets the prefix again, also on a tab in front.
    act(() => { document.title = 'Fix the login test'; });
    await Promise.resolve();
    expect(document.title).toBe('! Fix the login test');

    // Answered in another tab or channel.
    act(() => useNotificationStore.getState().handleWSNotificationAnswered({
      notification_id: 'ask-1', answer: 'Yes', answered_by: 'telegram',
    }));
    setSessions({});
    expect(document.title).toBe('Fix the login test');

    // A plain notify, then read (the list reload after a dismiss).
    setFront(false);
    act(() => useNotificationStore.getState().handleWSNotification({
      notification_id: 'n-2', notification_type: 'notify', session_id: 's1', title: 'Build done', priority: 'normal',
    }));
    expect(document.title).toBe('(1) Fix the login test');
    setFront(true);
    expect(document.title).toBe('(1) Fix the login test');
    setUnread(0);
    expect(document.title).toBe('Fix the login test');
  });

  it('puts "!" before the title for a pending question that the list leaves out', async () => {
    // A cron session asks, and the Notifications page shows the answered rows only.
    vi.mocked(api.listNotifications).mockResolvedValueOnce({ notifications: [], pending_count: 1, pending_answer_count: 1 });
    renderAlerts();

    act(() => useNotificationStore.getState().setFilter('answered'));
    await act(async () => { await Promise.resolve(); });

    expect(api.listNotifications).toHaveBeenCalledWith('answered', undefined);
    expect(document.title).toBe('! Nerve');
  });

  it('reads the notifications again when another tab of the browser dismissed items', async () => {
    vi.stubGlobal('BroadcastChannel', FakeChannel);
    renderAlerts();
    setUnread(1);
    expect(document.title).toBe('(1) Nerve');
    vi.mocked(api.listNotifications).mockClear();

    // What the store of the other tab sends after a dismissal.
    act(() => tellNotificationsChanged());
    await act(async () => { await Promise.resolve(); });

    expect(api.listNotifications).toHaveBeenCalled();
    expect(document.title).toBe('Nerve');
  });

  it('tells the other tabs after a dismissal, and not after a failed one', async () => {
    vi.stubGlobal('BroadcastChannel', FakeChannel);
    const otherTab: unknown[] = [];
    new FakeChannel(ALERT_CHANNEL).addEventListener('message', (e) => otherTab.push(e.data));
    const { dismissNotification, dismissAll } = useNotificationStore.getState();

    await act(() => dismissNotification('n-1'));
    await act(() => dismissAll());
    expect(otherTab).toEqual([{ kind: 'changed' }, { kind: 'changed' }]);

    const quiet = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(api.dismissNotification).mockRejectedValueOnce(new Error('offline'));
    await act(() => dismissNotification('n-2'));
    expect(otherTab).toHaveLength(2);
    quiet.mockRestore();
  });

  it('keeps a page title that starts with the same characters as the prefix', async () => {
    renderAlerts();
    setSessions({ questions: 1 });
    act(() => { document.title = '! Release'; });
    await Promise.resolve();
    expect(document.title).toBe('! ! Release');
    setSessions();
    expect(document.title).toBe('! Release');
  });

  it('colours the favicon dot by what waits, and takes it away when nothing does', async () => {
    const link = Object.assign(document.createElement('link'), { rel: 'icon', href: '/favicon.ico' });
    document.head.append(link);
    const fills: string[] = [];
    const context = {
      fillStyle: '', globalCompositeOperation: 'source-over', drawImage: vi.fn(), beginPath: vi.fn(), arc: vi.fn(),
      fill: vi.fn(() => fills.push(`${context.globalCompositeOperation} ${context.fillStyle}`)),
    };
    vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockReturnValue(context as unknown as CanvasRenderingContext2D);
    vi.spyOn(HTMLCanvasElement.prototype, 'toDataURL').mockImplementation(() => `data:image/png;dot=${context.fillStyle}`);
    let iconLoads = true;
    vi.stubGlobal('Image', class {
      onload: (() => void) | null = null;
      onerror: (() => void) | null = null;
      set src(_url: string) { queueMicrotask(() => (iconLoads ? this.onload : this.onerror)?.()); }
    });
    const drawn = async () => { await act(async () => { await Promise.resolve(); }); return link.getAttribute('href'); };
    renderAlerts();

    setUnread(2);
    expect(await drawn()).toBe(`data:image/png;dot=${UNREAD_DOT}`);
    // A clear ring around the dot first, so the dot stands apart from the icon. Both at the top right.
    expect(fills).toEqual(['destination-out ', `source-over ${UNREAD_DOT}`]);
    expect(context.arc.mock.calls.map(([x, y, r]) => [x, y, r])).toEqual([[25, 7, 9], [25, 7, 7]]);

    setSessions({ approvals: 1 });
    expect(await drawn()).toBe(`data:image/png;dot=${ANSWER_DOT}`);

    setUnread(0);
    setSessions();
    expect(link.getAttribute('href')).toBe('/favicon.ico');

    // Without an icon (no favicon on the instance), only the dot, in the middle.
    iconLoads = false;
    fills.length = 0;
    context.fillStyle = '';
    context.drawImage.mockClear();
    context.arc.mockClear();
    setUnread(1);
    expect(await drawn()).toBe(`data:image/png;dot=${UNREAD_DOT}`);
    expect(context.drawImage).not.toHaveBeenCalled();
    expect(fills).toEqual([`source-over ${UNREAD_DOT}`]);
    expect(context.arc.mock.calls.map(([x, y, r]) => [x, y, r])).toEqual([[16, 16, 7]]);
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
