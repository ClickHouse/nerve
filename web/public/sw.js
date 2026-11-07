// The service worker of the Nerve panel. It shows the push notifications of
// the instance and opens the panel when the person clicks a notification.
// It has no fetch handler: every page and asset comes from the network, so
// the panel never shows an old build.

const DEFAULT_PATH = '/notifications';
// The message that tells a panel tab to open a page (see web/src/utils/webPush.ts).
const OPEN_MESSAGE = 'nerve-open';

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
  // The person looks at the panel, which shows the item itself.
  if ((await panelWindows()).some((client) => client.focused)) return;
  await self.registration.showNotification(data.title || 'Nerve', {
    body: data.body || '',
    tag: data.tag,
    silent: data.silent === true,
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
