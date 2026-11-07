import { useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { ws } from '../../api/websocket';
import { alertsOn, useBrowserAlertStore } from '../../stores/browserAlertStore';
import { useChatStore } from '../../stores/chatStore';
import { useNotificationStore } from '../../stores/notificationStore';
import { createAlerter, levelWants } from '../../utils/browserAlerts';
import { tabAttention } from '../../utils/tabAttention';
import { OPEN_MESSAGE, registerWorker, workerSupported } from '../../utils/webPush';

const NOTIFICATIONS_PATH = '/notifications';
const ICON_SIZE = 32;
const DOT_RADIUS = 7;
/** The clear ring between the dot and the icon, so the dot stands apart from any icon. */
const DOT_GAP = 2;

/**
 * The parts of browser alerts that run on every page: system alerts while
 * the tab is not in front, and on every tab "! " or "(N) " in the title and
 * a coloured dot on the favicon (`tabAttention`). It registers the panel's
 * service worker, which shows the alerts and the push notifications, and
 * opens the page that the worker names after a click. Renders nothing.
 */
export function BrowserAlerts() {
  const navigate = useNavigate();
  // The alerter lives as long as the panel; a click on an alert uses the current `navigate`.
  const navigateRef = useRef(navigate);
  useEffect(() => { navigateRef.current = navigate; }, [navigate]);
  const sessions = useChatStore((s) => s.sessions);
  const notifications = useNotificationStore((s) => s.notifications);
  const unread = useNotificationStore((s) => s.pendingCount);
  const { prefix, dot } = tabAttention(sessions, notifications, unread);

  useEffect(() => {
    const alerter = createAlerter({ onOpen: () => navigateRef.current(NOTIFICATIONS_PATH) });
    const offMessage = ws.onMessage((msg) => {
      if (msg.type === 'notification' && !msg.silenced) {
        const alerts = useBrowserAlertStore.getState();
        alerter.receive(msg, alertsOn(alerts) && levelWants(alerts.level, msg.notification_type, msg.priority));
      } else if (msg.type === 'notification_answered' || msg.type === 'notification_expired') {
        alerter.close(msg.notification_id);
      }
    });
    const onReturn = () => useBrowserAlertStore.getState().refresh();
    const worker = workerSupported() ? navigator.serviceWorker : null;
    const onWorkerMessage = (e: MessageEvent<{ type?: string; url?: unknown }>) => {
      if (e.data?.type !== OPEN_MESSAGE) return;
      const url = e.data.url;
      navigateRef.current(typeof url === 'string' && url.startsWith('/') ? url : NOTIFICATIONS_PATH);
    };
    void registerWorker();
    worker?.addEventListener('message', onWorkerMessage);
    window.addEventListener('focus', onReturn);
    document.addEventListener('visibilitychange', onReturn);
    return () => {
      offMessage();
      alerter.dispose();
      worker?.removeEventListener('message', onWorkerMessage);
      window.removeEventListener('focus', onReturn);
      document.removeEventListener('visibilitychange', onReturn);
    };
  }, []);

  useTitlePrefix(prefix);
  useFaviconDot(dot);
  return null;
}

/**
 * `prefix` before the title. Pages set the title too, so a new title gets it
 * again. Only the text that this hook wrote counts as prefixed, so a page
 * title that starts with the same characters keeps them.
 */
function useTitlePrefix(prefix: string) {
  useEffect(() => {
    if (!prefix) return;
    let written = '';
    const apply = () => {
      if (document.title === written) return;
      written = prefix + document.title;
      document.title = written;
    };
    apply();
    const observer = new MutationObserver(apply);
    const title = document.querySelector('title');
    if (title) observer.observe(title, { childList: true, characterData: true, subtree: true });
    return () => {
      observer.disconnect();
      if (document.title === written) document.title = written.slice(prefix.length);
    };
  }, [prefix]);
}

function useFaviconDot(color: string | null) {
  useEffect(() => {
    const link = document.querySelector<HTMLLinkElement>('link[rel="icon"]');
    if (!color || !link) return;
    const href = link.getAttribute('href') ?? '';
    let current = true;
    void iconWithDot(href, color).then((url) => { if (current && url) link.href = url; });
    return () => {
      current = false;
      link.setAttribute('href', href);
    };
  }, [color]);
}

/**
 * The icon at `href` with a dot at its top right, as a data URL. When the
 * icon does not load (the instance has no favicon), only the dot, in the middle.
 */
function iconWithDot(href: string, color: string): Promise<string | null> {
  const canvas = document.createElement('canvas');
  canvas.width = ICON_SIZE;
  canvas.height = ICON_SIZE;
  const context = canvas.getContext('2d');
  if (!context) return Promise.resolve(null);
  return new Promise((resolve) => {
    const image = new Image();
    const draw = (loaded: boolean) => {
      const [x, y] = loaded ? [ICON_SIZE - DOT_RADIUS, DOT_RADIUS] : [ICON_SIZE / 2, ICON_SIZE / 2];
      const circle = (radius: number) => {
        context.beginPath();
        context.arc(x, y, radius, 0, 2 * Math.PI);
        context.fill();
      };
      if (loaded) {
        context.drawImage(image, 0, 0, ICON_SIZE, ICON_SIZE);
        context.globalCompositeOperation = 'destination-out';
        circle(DOT_RADIUS + DOT_GAP);
        context.globalCompositeOperation = 'source-over';
      }
      context.fillStyle = color;
      circle(DOT_RADIUS);
      resolve(canvas.toDataURL('image/png'));
    };
    image.onload = () => draw(true);
    image.onerror = () => draw(false);
    image.src = href;
  });
}
