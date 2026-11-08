// The service worker of the Nerve panel. It shows the push notifications of
// the instance and opens the panel when the person clicks a notification.
// It has no fetch handler: every page and asset comes from the network, so
// the panel never shows an old build.

const DEFAULT_PATH = '/notifications';
// The message that tells a panel tab to open a page (see web/src/utils/webPush.ts).
const OPEN_MESSAGE = 'nerve-open';
// Nerve's app icon, and the white N that Android shows in the status bar.
// The same files as in web/src/utils/browserAlerts.ts.
const ICON = '/nerve-icon-192.png';
const BADGE = '/nerve-badge.png';

// WebKit (Safari, and a Home Screen app on iPhone and iPad) removes the push
// subscription after three pushes that show no notification, also while a
// panel tab has focus, so there the worker shows each push. Chrome on iOS is
// WebKit too, and a Home Screen app has no "Safari" token.
function showsEveryPush(userAgent) {
  return userAgent.includes('AppleWebKit') && !/Chrome\/|Chromium\/|Android/.test(userAgent);
}
const SHOWS_EVERY_PUSH = showsEveryPush(self.navigator.userAgent);

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));

self.addEventListener('push', (event) => event.waitUntil(showPush(event)));
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  event.waitUntil(openPanel(event.notification.data && event.notification.data.url));
});

function panelWindows() {
  return self.clients.matchAll({ type: 'window', includeUncontrolled: true });
}

async function showPush(event) {
  let data = {};
  try {
    data = event.data ? event.data.json() : {};
  } catch {
    // Not JSON: show the notification without a text.
  }
  // While the person looks at the panel, the panel shows the item: show no
  // notification, or on WebKit one without sound.
  const focused = (await panelWindows()).some((client) => client.focused);
  if (focused && !SHOWS_EVERY_PUSH) return;
  await self.registration.showNotification(data.title || 'Nerve', {
    body: data.body || '',
    icon: ICON,
    badge: BADGE,
    tag: data.tag,
    silent: focused || data.silent === true,
    data: { url: data.url },
  });
}

/** A path of this origin, else the Notifications page. */
function panelPath(url) {
  return typeof url === 'string' && url.startsWith('/') && !url.startsWith('//') ? url : DEFAULT_PATH;
}

async function openPanel(url) {
  const path = panelPath(url);
  const [tab] = await panelWindows();
  if (!tab) {
    await self.clients.openWindow(path);
    return;
  }
  await tab.focus();
  tab.postMessage({ type: OPEN_MESSAGE, url: path });
}
