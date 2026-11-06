import { useCallback, useSyncExternalStore } from 'react';

/**
 * Everything below Tailwind's `md` breakpoint (768px) is treated as "mobile":
 * one hand, one column, no room for a persistent sidebar. Kept in sync with
 * the `md:` prefixes used in the markup — change both together.
 */
export const MOBILE_QUERY = '(max-width: 767px)';

/**
 * Subscribe to a CSS media query.
 *
 * `useSyncExternalStore` rather than `useState` + an effect so the first
 * render already has the right answer: an effect-based version paints one
 * frame of the desktop layout before correcting itself, which on the chat
 * page means a visible flash of the squeezed three-column shell.
 */
export function useMediaQuery(query: string): boolean {
  const subscribe = useCallback((onStoreChange: () => void) => {
    const mql = window.matchMedia(query);
    mql.addEventListener('change', onStoreChange);
    return () => mql.removeEventListener('change', onStoreChange);
  }, [query]);

  return useSyncExternalStore(
    subscribe,
    () => window.matchMedia(query).matches,
    // Server snapshot: assume desktop, matching the pre-JS markup.
    () => false,
  );
}

/** True on phone-sized viewports (below Tailwind's `md`). */
export function useIsMobile(): boolean {
  return useMediaQuery(MOBILE_QUERY);
}

/**
 * The primary pointer is a finger: phones and tablets. These devices usually
 * type on an on-screen keyboard, which has no Shift+Enter. Independent of the
 * viewport width: a narrow desktop window still has a hardware keyboard, and a
 * tablet is wider than `md`.
 */
export const TOUCH_QUERY = '(pointer: coarse)';

/** True when the primary pointer is a touch screen. */
export function useIsTouch(): boolean {
  return useMediaQuery(TOUCH_QUERY);
}

/**
 * One-shot check for code that runs outside React — store actions and keyboard
 * shortcut handlers, which need the current layout but cannot call hooks.
 */
export function isMobileViewport(): boolean {
  return window.matchMedia(MOBILE_QUERY).matches;
}
