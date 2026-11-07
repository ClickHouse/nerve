import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ws } from '../../api/websocket';
import { alertsOn, useBrowserAlertStore } from '../../stores/browserAlertStore';
import { createAlerter, levelWants, tabInFront } from '../../utils/browserAlerts';

const NOTIFICATIONS_PATH = '/notifications';
const ICON_SIZE = 32;
const DOT_RADIUS = 7;
/** A fixed red: the tab strip of the browser does not follow the panel theme. */
const DOT_COLOR = '#e5484d';

/**
 * The parts of browser alerts that run on every page: system alerts while
 * the tab is not in front, and for the items that arrive while the person
 * is away, "(N)" in the tab title and a dot on the favicon. Renders nothing.
 */
export function BrowserAlerts() {
  const navigate = useNavigate();
  // The alerter lives as long as the panel; a click on an alert uses the current `navigate`.
  const navigateRef = useRef(navigate);
  useEffect(() => { navigateRef.current = navigate; }, [navigate]);
  const [unseen, setUnseen] = useState(0);

  useEffect(() => {
    const alerter = createAlerter({ onOpen: () => navigateRef.current(NOTIFICATIONS_PATH) });
    const offMessage = ws.onMessage((msg) => {
      if (msg.type === 'notification' && !msg.silenced) {
        const alerts = useBrowserAlertStore.getState();
        alerter.receive(msg, alertsOn(alerts) && levelWants(alerts.level, msg.notification_type, msg.priority));
        if (!tabInFront()) setUnseen((n) => n + 1);
      } else if (msg.type === 'notification_answered' || msg.type === 'notification_expired') {
        alerter.close(msg.notification_id);
      }
    });
    const onReturn = () => {
      useBrowserAlertStore.getState().refresh();
      if (tabInFront()) setUnseen(0);
    };
    window.addEventListener('focus', onReturn);
    document.addEventListener('visibilitychange', onReturn);
    return () => {
      offMessage();
      alerter.dispose();
      window.removeEventListener('focus', onReturn);
      document.removeEventListener('visibilitychange', onReturn);
    };
  }, []);

  useTitleCount(unseen);
  useFaviconDot(unseen > 0);
  return null;
}

/** `(N) ` before the title. Pages set the title too, so a new title gets it again. */
function useTitleCount(count: number) {
  useEffect(() => {
    if (count === 0) return;
    const prefix = `(${count}) `;
    const apply = () => {
      if (!document.title.startsWith(prefix)) document.title = prefix + document.title;
    };
    apply();
    const observer = new MutationObserver(apply);
    const title = document.querySelector('title');
    if (title) observer.observe(title, { childList: true, characterData: true, subtree: true });
    return () => {
      observer.disconnect();
      if (document.title.startsWith(prefix)) document.title = document.title.slice(prefix.length);
    };
  }, [count]);
}

function useFaviconDot(on: boolean) {
  useEffect(() => {
    const link = document.querySelector<HTMLLinkElement>('link[rel="icon"]');
    if (!on || !link) return;
    const href = link.getAttribute('href') ?? '';
    let current = true;
    void iconWithDot(href).then((url) => { if (current && url) link.href = url; });
    return () => {
      current = false;
      link.setAttribute('href', href);
    };
  }, [on]);
}

/** The icon at `href` with a dot at its top right, as a data URL. Only the dot when the icon does not load. */
function iconWithDot(href: string): Promise<string | null> {
  const canvas = document.createElement('canvas');
  canvas.width = ICON_SIZE;
  canvas.height = ICON_SIZE;
  const context = canvas.getContext('2d');
  if (!context) return Promise.resolve(null);
  return new Promise((resolve) => {
    const image = new Image();
    const draw = (loaded: boolean) => {
      if (loaded) context.drawImage(image, 0, 0, ICON_SIZE, ICON_SIZE);
      context.fillStyle = DOT_COLOR;
      context.beginPath();
      context.arc(ICON_SIZE - DOT_RADIUS, DOT_RADIUS, DOT_RADIUS, 0, 2 * Math.PI);
      context.fill();
      resolve(canvas.toDataURL('image/png'));
    };
    image.onload = () => draw(true);
    image.onerror = () => draw(false);
    image.src = href;
  });
}
