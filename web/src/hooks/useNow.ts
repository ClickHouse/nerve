import { useCallback, useSyncExternalStore } from 'react';

const MIN_INTERVAL_MS = 250;

/** Enabled callers, each with the interval it asked for. */
const subscribers = new Map<() => void, number>();
let current = Date.now();
let timer: ReturnType<typeof setInterval> | null = null;
let period = 0;

function noop() {}

function tick() {
  current = Date.now();
  for (const notify of [...subscribers.keys()]) notify();
}

/** Run the ticker at the shortest requested interval, or stop it when nobody listens. */
function reschedule() {
  const next = subscribers.size > 0 ? Math.min(...subscribers.values()) : 0;
  if (next === period) return;
  if (timer !== null) clearInterval(timer);
  timer = null;
  period = next;
  if (next > 0) timer = setInterval(tick, next);
}

function snapshot(interval: number): number {
  // With no ticker running nobody has refreshed the clock for a while: catch
  // up, at most once per interval so consecutive reads stay equal.
  if (timer === null) {
    const real = Date.now();
    if (Math.abs(real - current) >= interval) current = real;
  }
  return Math.floor(current / interval) * interval;
}

/**
 * The current time, re-rendering the caller every `intervalMs`.
 *
 * Every caller shares one ticker: a page with fifty live agent rows runs one
 * interval, not fifty. The ticker runs at the shortest interval any enabled
 * caller asked for and only while at least one enabled caller is mounted, so
 * a session with nothing live costs nothing. Each caller sees the time rounded
 * down to its own interval, so a caller asking for 5 s re-renders every 5 s
 * even while another one ticks every second.
 *
 * `enabled = false` returns a time that does not tick (it catches up whenever
 * the caller renders for another reason).
 *
 * `useSyncExternalStore` rather than an effect with state: the first render
 * already has the time, and every caller of one tick reads the same value.
 */
export function useNow(intervalMs = 1000, enabled = true): number {
  const interval = Math.max(MIN_INTERVAL_MS, Math.round(intervalMs) || MIN_INTERVAL_MS);
  const subscribe = useCallback((notify: () => void) => {
    if (!enabled) return noop;
    subscribers.set(notify, interval);
    reschedule();
    return () => {
      subscribers.delete(notify);
      reschedule();
    };
  }, [interval, enabled]);
  const getSnapshot = useCallback(() => snapshot(interval), [interval]);
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}

/** Whether the shared ticker is running (for tests). */
export function nowTickerRunning(): boolean {
  return timer !== null;
}
