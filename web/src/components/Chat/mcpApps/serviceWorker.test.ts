import { describe, expect, it, vi } from 'vitest';
import source from '../../../../public/sw.js?raw';

/**
 * The sandbox page of MCP Apps (`public/mcp-app-sandbox.html`) is served
 * from the chat's own address, so its address is inside the scope of the
 * panel's service worker (`public/sw.js`). What keeps the page apart from
 * the chat is a response header of the gateway: `Content-Security-Policy:
 * sandbox …`, which takes the page's origin away.
 *
 * A worker that answered requests could answer the load of that page: from
 * a cache, or with a response of its own making, and then the headers would
 * be the worker's to get right. The worker answers nothing today (it shows
 * push notifications and opens the panel), and these tests keep it so. If a
 * fetch handler is ever wanted, it must leave the sandbox page's address to
 * the network, and these tests change with it.
 */

/** The events `public/sw.js` listens to, when run against a worker scope. */
function listenedEvents(): string[] {
  const types: string[] = [];
  const scope = {
    navigator: { userAgent: 'Mozilla/5.0 Chrome/130.0.0.0' },
    addEventListener: (type: string) => types.push(type),
    skipWaiting: vi.fn(),
    clients: { matchAll: vi.fn(async () => []), openWindow: vi.fn(async () => null), claim: vi.fn(async () => undefined) },
    registration: { showNotification: vi.fn(async () => undefined) },
  };
  new Function('self', source)(scope);
  expect(scope).not.toHaveProperty('onfetch');
  return types;
}

describe('the service worker and the sandbox page', () => {
  it('answers no request: it has no fetch handler', () => {
    const types = listenedEvents();
    expect(types).not.toContain('fetch');
    expect([...types].sort()).toEqual(['activate', 'install', 'notificationclick', 'push']);
  });

  it('has nothing in its source that could answer one', () => {
    expect(source).not.toMatch(/['"`]fetch['"`]/);
    expect(source).not.toMatch(/\bonfetch\b|\brespondWith\b|\bcaches\b|\bimportScripts\b|\bnavigationPreload\b/);
  });
});
