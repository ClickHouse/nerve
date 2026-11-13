/**
 * How many widgets run at the same time.
 *
 * Each running widget is two frames and a process of its own in the browser.
 * A long chat can hold dozens of ```widget blocks, so at most
 * MAX_LIVE_WIDGETS of them have their frames; the others hold their place
 * and are loaded when they are wanted.
 *
 * A block asks for a slot when it comes near the screen. When all slots are
 * taken, the widget that is furthest from the reader gives its slot up: one
 * that is not near the screen, and of those the one that left it the longest
 * ago. It goes back to a placeholder of the height it had, and is loaded
 * again from its source when it comes back (what was typed or chosen in it
 * is gone: nothing of a widget is kept anywhere but in its source). When
 * every running widget is near the screen, a new one waits for a click.
 */

export const MAX_LIVE_WIDGETS = 6;

/** A running widget's place among the MAX_LIVE_WIDGETS. */
export interface WidgetSlot {
  /** The block came near the screen, or left it. */
  seen(near: boolean): void;
  /** The widget is gone: the place is free. */
  release(): void;
}

interface Holder {
  near: boolean;
  /** When it was last near the screen, on a clock that only counts. */
  at: number;
  evict: () => void;
}

const holders = new Set<Holder>();
let clock = 0;

/** The holder to take a slot from: the one that left the screen the longest ago. With `force`, any one. */
function victim(force: boolean): Holder | null {
  let found: Holder | null = null;
  for (const holder of holders) {
    if (holder.near && !force) continue;
    // One that is off the screen goes before one that is on it; then the oldest.
    if (!found || (found.near && !holder.near) || (found.near === holder.near && holder.at < found.at)) found = holder;
  }
  return found;
}

/**
 * A slot for a widget that is near the screen, or null when all are taken
 * by widgets that are near it too. `evict` is called when the slot is taken
 * away for another widget; the block then removes its frame. With `force`
 * (the person asked for this widget), a slot is always found.
 */
export function claimWidgetSlot(evict: () => void, force = false): WidgetSlot | null {
  if (holders.size >= MAX_LIVE_WIDGETS) {
    const loser = victim(force);
    if (!loser) return null;
    holders.delete(loser);
    loser.evict();
  }
  const holder: Holder = { near: true, at: ++clock, evict };
  holders.add(holder);
  return {
    seen(near) {
      holder.near = near;
      if (near) holder.at = ++clock;
    },
    release() {
      holders.delete(holder);
    },
  };
}

/** How many widgets run now. */
export function liveWidgets(): number {
  return holders.size;
}

/** Forget every slot, for tests. */
export function resetWidgetSlots(): void {
  holders.clear();
  clock = 0;
}
