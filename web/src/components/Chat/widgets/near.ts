import { useEffect, useState, type RefObject } from 'react';

/** How far outside the screen a block counts as near it. */
export const NEAR_MARGIN = '600px 0px';

/**
 * True while the block is on the screen or close to it (NEAR_MARGIN); always,
 * where the browser cannot tell. The frames of the chat (widgets, inline
 * previews) load only near the screen.
 */
export function useNear(block: RefObject<HTMLElement | null>): boolean {
  const [near, setNear] = useState(() => typeof IntersectionObserver === 'undefined');
  useEffect(() => {
    const element = block.current;
    if (!element || typeof IntersectionObserver === 'undefined') return;
    const observer = new IntersectionObserver((entries) => {
      setNear(entries[entries.length - 1].isIntersecting);
    }, {
      rootMargin: NEAR_MARGIN,
      // The chat scrolls in an element of its own: the margin counts there too.
      scrollMargin: NEAR_MARGIN,
    } as IntersectionObserverInit);
    observer.observe(element);
    return () => observer.disconnect();
  }, [block]);
  return near;
}
