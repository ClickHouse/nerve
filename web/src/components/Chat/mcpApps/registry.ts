import type { McpAppHostHandle } from './types';

/**
 * A running view lives longer than the block that shows it.
 *
 * The chat draws one tool call more than once: the streamed message gives
 * way to the stored one when the turn ends, and React (in development) mounts
 * every block twice. A view that started again each time would flash and lose
 * what the person did in it. So a block that goes away parks its view here
 * for a moment, and a block for the same call that mounts in that moment
 * takes it over, frame and state. A view that nobody takes is told to end
 * (`ui/resource-teardown`) and removed.
 *
 * Parking needs the browser to move a frame without loading it again
 * (`moveBefore`). Where it cannot, the view ends with its block.
 */

/** How long a parked view waits for a block to take it. */
export const PARK_MS = 250;

interface Parked {
  host: McpAppHostHandle;
  timer: ReturnType<typeof setTimeout>;
}

const parked = new Map<string, Parked>();
let lot: HTMLElement | null = null;

/** The key of a view: one per tool call of a session. */
export function hostKey(sessionId: string, toolUseId: string): string {
  return `${sessionId}:${toolUseId}`;
}

/** Where parked views wait: in the document, laid out, out of sight and out of reach. */
function parkingLot(): HTMLElement {
  if (!lot || !lot.isConnected) {
    lot = document.createElement('div');
    lot.setAttribute('data-mcp-app-parking', '');
    lot.setAttribute('aria-hidden', 'true');
    lot.inert = true;
    lot.style.cssText = 'position:fixed;left:-100000px;top:0;visibility:hidden;pointer-events:none;';
    document.body.appendChild(lot);
  }
  return lot;
}

/** The block that showed `host` is gone: park the view, or end it. */
export function releaseHost(key: string, host: McpAppHostHandle): void {
  if (!host.park(parkingLot())) {
    void host.dispose();
    return;
  }
  const earlier = parked.get(key);
  if (earlier) {
    clearTimeout(earlier.timer);
    void earlier.host.dispose();
  }
  const timer = setTimeout(() => {
    if (parked.get(key)?.host === host) parked.delete(key);
    void host.dispose();
  }, PARK_MS);
  parked.set(key, { host, timer });
}

/** The parked view of `key`, for a block that shows it again; null when there is none. */
export function claimHost(key: string): McpAppHostHandle | null {
  const entry = parked.get(key);
  if (!entry) return null;
  clearTimeout(entry.timer);
  parked.delete(key);
  return entry.host;
}

/** End every parked view now, for tests: one test's view must not reach the next. */
export function endParkedHosts(): void {
  for (const [key, entry] of [...parked]) {
    clearTimeout(entry.timer);
    parked.delete(key);
    void entry.host.dispose();
  }
}
