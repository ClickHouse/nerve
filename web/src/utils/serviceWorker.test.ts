import { describe, expect, it, vi } from 'vitest';
import source from '../../public/sw.js?raw';
import { OPEN_MESSAGE } from './webPush';

interface FakeWindow {
  focused: boolean;
  focus: ReturnType<typeof vi.fn>;
  postMessage: ReturnType<typeof vi.fn>;
}

type Listener = (event: Record<string, unknown>) => void;

const SAFARI_MAC = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15';
const IPHONE_HOME_SCREEN_APP = 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148';
const CHROME_IOS = 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/130.0.6723.90 Mobile/15E148 Safari/604.1';
const CHROME_DESKTOP = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36';
const CHROME_ANDROID = 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36';

/** Runs `public/sw.js` against a fake worker scope with these panel windows, in a browser with this User-Agent. */
function loadWorker(windows: FakeWindow[] = [], userAgent = CHROME_DESKTOP) {
  const listeners = new Map<string, Listener>();
  const scope = {
    navigator: { userAgent },
    addEventListener: (type: string, listener: Listener) => listeners.set(type, listener),
    skipWaiting: vi.fn(),
    clients: {
      matchAll: vi.fn(async () => windows),
      openWindow: vi.fn(async () => null),
      claim: vi.fn(async () => undefined),
    },
    registration: { showNotification: vi.fn(async () => undefined) },
  };
  new Function('self', source)(scope);
  /** Sends an event and waits for the work that it gives to `waitUntil`. */
  const dispatch = async (type: string, event: Record<string, unknown>) => {
    let work: Promise<unknown> = Promise.resolve();
    listeners.get(type)?.({ ...event, waitUntil: (p: Promise<unknown>) => { work = p; } });
    await work;
  };
  return { scope, dispatch };
}

const panelWindow = (focused: boolean): FakeWindow => ({ focused, focus: vi.fn(async () => undefined), postMessage: vi.fn() });
const pushEvent = (payload: unknown) => ({ data: { json: () => (typeof payload === 'string' ? JSON.parse(payload) : payload) } });
const PAYLOAD = { title: 'Ada: Deploy now?', body: 'The build passed.', tag: 'ask-1234', url: '/notifications', silent: true };

describe('the service worker', () => {
  it('shows a push when no panel window has focus', async () => {
    for (const windows of [[], [panelWindow(false)]]) {
      const { scope, dispatch } = loadWorker(windows);
      await dispatch('push', pushEvent(PAYLOAD));
      expect(scope.registration.showNotification).toHaveBeenCalledWith('Ada: Deploy now?', {
        body: 'The build passed.', icon: '/nerve-icon-192.png', badge: '/nerve-badge.png', tag: 'ask-1234',
        silent: true, data: { url: '/notifications' },
      });
    }
  });

  it('shows nothing while the person looks at the panel', async () => {
    for (const userAgent of [CHROME_DESKTOP, CHROME_ANDROID]) {
      const { scope, dispatch } = loadWorker([panelWindow(false), panelWindow(true)], userAgent);
      await dispatch('push', pushEvent(PAYLOAD));
      expect(scope.registration.showNotification).not.toHaveBeenCalled();
    }
  });

  it('on WebKit shows each push, without sound while the person looks at the panel', async () => {
    for (const userAgent of [SAFARI_MAC, IPHONE_HOME_SCREEN_APP, CHROME_IOS]) {
      for (const focused of [true, false]) {
        const { scope, dispatch } = loadWorker([panelWindow(focused)], userAgent);
        await dispatch('push', pushEvent({ ...PAYLOAD, silent: false }));
        expect(scope.registration.showNotification).toHaveBeenCalledWith(
          'Ada: Deploy now?', expect.objectContaining({ tag: 'ask-1234', silent: focused }),
        );
      }
    }
  });

  it('shows a push that is not JSON without a text', async () => {
    const { scope, dispatch } = loadWorker();
    await dispatch('push', pushEvent('not json'));
    expect(scope.registration.showNotification).toHaveBeenCalledWith('Nerve', expect.objectContaining({ body: '' }));
  });

  it('opens the page of a click in the last panel window', async () => {
    const tab = panelWindow(false);
    const { scope, dispatch } = loadWorker([tab, panelWindow(false)]);
    const notification = { close: vi.fn(), data: { url: '/notifications' } };
    await dispatch('notificationclick', { notification });
    expect(notification.close).toHaveBeenCalled();
    expect(tab.focus).toHaveBeenCalled();
    expect(tab.postMessage).toHaveBeenCalledWith({ type: OPEN_MESSAGE, url: '/notifications' });
    expect(scope.clients.openWindow).not.toHaveBeenCalled();
  });

  it('opens a window when no panel window is open, and only pages of the panel', async () => {
    const { scope, dispatch } = loadWorker();
    await dispatch('notificationclick', { notification: { close: vi.fn(), data: { url: '//example.com/x' } } });
    expect(scope.clients.openWindow).toHaveBeenCalledWith('/notifications');
  });
});
