/**
 * Debounced reload of the session list. Any store or handler can ask for it
 * without an import of chatStore, which registers the loader at startup.
 * A burst of events (one notification per session, a plan decision) costs
 * one fetch: the last request in the window starts it.
 */
export const SESSIONS_RELOAD_DEBOUNCE_MS = 500;

let loader: (() => void) | null = null;
let timer: ReturnType<typeof setTimeout> | null = null;

export function registerSessionsLoader(load: () => void): void {
  loader = load;
}

export function reloadSessionsSoon(): void {
  if (timer) clearTimeout(timer);
  timer = setTimeout(() => {
    timer = null;
    loader?.();
  }, SESSIONS_RELOAD_DEBOUNCE_MS);
}
