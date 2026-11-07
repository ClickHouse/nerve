/**
 * The panel's service worker (`public/sw.js`) and Web Push in this browser.
 * The worker shows push notifications, also when no panel tab is open, and
 * the browser alerts of the tabs, so that one item gives one notification.
 */

export const SERVICE_WORKER_URL = '/sw.js';
/** The message that the worker sends to a panel tab to open a page. */
export const OPEN_MESSAGE = 'nerve-open';

/** Browsers run service workers only for HTTPS addresses (and `localhost`). */
export function workerSupported(): boolean {
  return window.isSecureContext && 'serviceWorker' in navigator;
}

/** This browser can get push notifications from the panel. */
export function pushSupported(): boolean {
  return workerSupported() && 'PushManager' in window && typeof window.Notification === 'function';
}

/**
 * Registers the worker (the browser keeps one registration for the URL) and
 * waits until it runs. Null when the browser has no service workers or refuses it.
 */
export async function registerWorker(): Promise<ServiceWorkerRegistration | null> {
  if (!workerSupported()) return null;
  try {
    await navigator.serviceWorker.register(SERVICE_WORKER_URL);
    return await navigator.serviceWorker.ready;
  } catch {
    return null;
  }
}

/** The worker's registration when its worker runs, else null. */
export async function activeWorker(): Promise<ServiceWorkerRegistration | null> {
  if (!workerSupported()) return null;
  const registration = await navigator.serviceWorker.getRegistration();
  return registration?.active ? registration : null;
}

/** The push subscription of this browser, if it has one. */
export async function browserSubscription(): Promise<PushSubscription | null> {
  if (!pushSupported()) return null;
  const registration = await navigator.serviceWorker.getRegistration();
  return registration ? registration.pushManager.getSubscription() : null;
}

export function base64UrlBytes(text: string): Uint8Array<ArrayBuffer> {
  const base64 = text.replace(/-/g, '+').replace(/_/g, '/');
  const binary = atob(base64 + '='.repeat((4 - (base64.length % 4)) % 4));
  return Uint8Array.from(binary, (c) => c.charCodeAt(0));
}

/** The browser made the subscription with this VAPID public key (base64url). */
export function madeWithKey(subscription: PushSubscription, publicKey: string): boolean {
  const used = subscription.options.applicationServerKey;
  if (!used) return false;
  const expected = base64UrlBytes(publicKey);
  const actual = new Uint8Array(used);
  return actual.length === expected.length && actual.every((byte, i) => byte === expected[i]);
}

const BROWSERS: readonly [string, RegExp][] = [
  ['Edge', /Edg(A|iOS)?\//],
  ['Opera', /OPR\//],
  ['Samsung Internet', /SamsungBrowser\//],
  ['Firefox', /Firefox\/|FxiOS\//],
  ['Chrome', /Chrome\/|CriOS\//],
  ['Safari', /Safari\//],
];
// iPhone and iPad user agents name macOS, and Android and ChromeOS name Linux.
const SYSTEMS: readonly [string, RegExp][] = [
  ['iPhone', /iPhone/],
  ['iPad', /iPad/],
  ['Android', /Android/],
  ['ChromeOS', /CrOS/],
  ['Windows', /Windows/],
  ['macOS', /Macintosh|Mac OS X/],
  ['Linux', /Linux/],
];

/** A short name for a browser, such as "Chrome on Android", from its user agent. */
export function deviceLabel(userAgent: string): string {
  const browser = BROWSERS.find(([, pattern]) => pattern.test(userAgent))?.[0] ?? 'Browser';
  const system = SYSTEMS.find(([, pattern]) => pattern.test(userAgent))?.[0];
  return system ? `${browser} on ${system}` : browser;
}

/** An iPhone or iPad browser tab: push works only after "Add to Home Screen". */
export function needsHomeScreen(): boolean {
  const ios = /iPhone|iPad/.test(navigator.userAgent)
    || (/Macintosh/.test(navigator.userAgent) && navigator.maxTouchPoints > 1);
  return ios && !window.matchMedia?.('(display-mode: standalone)').matches;
}
