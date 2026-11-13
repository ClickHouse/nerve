import { beforeEach, describe, expect, it, vi } from 'vitest';
import { MAX_LIVE_WIDGETS, claimWidgetSlot, liveWidgets, resetWidgetSlots, type WidgetSlot } from './budget';

/** A widget block, as the budget sees it: whether it holds a slot, and how often it was told to give it up. */
function block() {
  const evict = vi.fn();
  const state: { slot: WidgetSlot | null; evict: typeof evict } = { slot: null, evict };
  return state;
}

/** Fill every slot with a block that is near the screen. */
function fill() {
  return Array.from({ length: MAX_LIVE_WIDGETS }, () => {
    const holder = block();
    holder.slot = claimWidgetSlot(holder.evict);
    expect(holder.slot).not.toBeNull();
    return holder;
  });
}

beforeEach(() => {
  resetWidgetSlots();
});

describe('the mount budget', () => {
  it('lets a small number of widgets run at the same time', () => {
    expect(MAX_LIVE_WIDGETS).toBe(6);
    const holders = fill();
    expect(liveWidgets()).toBe(MAX_LIVE_WIDGETS);
    for (const holder of holders) expect(holder.evict).not.toHaveBeenCalled();
  });

  it('gives no slot to one more while every running widget is near the screen', () => {
    const holders = fill();
    const late = block();
    expect(claimWidgetSlot(late.evict)).toBeNull();
    expect(liveWidgets()).toBe(MAX_LIVE_WIDGETS);
    for (const holder of holders) expect(holder.evict).not.toHaveBeenCalled();
  });

  it('takes the slot of a widget that is off the screen', () => {
    const holders = fill();
    holders[2].slot!.seen(false);
    const late = block();
    expect(claimWidgetSlot(late.evict)).not.toBeNull();
    expect(holders[2].evict).toHaveBeenCalledTimes(1);
    for (const [index, holder] of holders.entries()) {
      if (index !== 2) expect(holder.evict).not.toHaveBeenCalled();
    }
    expect(liveWidgets()).toBe(MAX_LIVE_WIDGETS);
  });

  it('of those off the screen, takes the one that was last near it the longest ago', () => {
    const holders = fill();
    // The reader scrolls down: 3 leaves, then 0, then 4; 0 comes back for a moment and leaves again.
    holders[3].slot!.seen(false);
    holders[0].slot!.seen(false);
    holders[4].slot!.seen(false);
    holders[0].slot!.seen(true);
    holders[0].slot!.seen(false);
    const order: number[] = [];
    for (const [index, holder] of holders.entries()) holder.evict.mockImplementation(() => order.push(index));
    for (let i = 0; i < 3; i++) expect(claimWidgetSlot(vi.fn())).not.toBeNull();
    // 3 and 4 were last near the screen when they were opened, 3 before 4; 0 after both.
    expect(order).toEqual([3, 4, 0]);
    // The rest are near the screen: no more slots.
    expect(claimWidgetSlot(vi.fn())).toBeNull();
  });

  it('never takes the slot of a widget that came back near the screen', () => {
    const holders = fill();
    holders[1].slot!.seen(false);
    holders[1].slot!.seen(true);
    expect(claimWidgetSlot(vi.fn())).toBeNull();
    expect(holders[1].evict).not.toHaveBeenCalled();
  });

  it('finds a slot when the person asks, from the widget used longest ago', () => {
    const holders = fill();
    // 2 was looked at again just now.
    holders[2].slot!.seen(true);
    const asked = block();
    expect(claimWidgetSlot(asked.evict, true)).not.toBeNull();
    expect(holders[0].evict).toHaveBeenCalledTimes(1);
    expect(liveWidgets()).toBe(MAX_LIVE_WIDGETS);
  });

  it('when the person asks, still takes one off the screen first', () => {
    const holders = fill();
    holders[5].slot!.seen(false);
    expect(claimWidgetSlot(vi.fn(), true)).not.toBeNull();
    expect(holders[5].evict).toHaveBeenCalledTimes(1);
    expect(holders[0].evict).not.toHaveBeenCalled();
  });

  it('frees a slot when its widget goes', () => {
    const holders = fill();
    holders[0].slot!.release();
    expect(liveWidgets()).toBe(MAX_LIVE_WIDGETS - 1);
    expect(claimWidgetSlot(vi.fn())).not.toBeNull();
    for (const holder of holders) expect(holder.evict).not.toHaveBeenCalled();
    // Released twice, it frees nothing more.
    holders[0].slot!.release();
    expect(liveWidgets()).toBe(MAX_LIVE_WIDGETS);
  });

  it('does not count a slot that was taken away', () => {
    const holders = fill();
    holders[0].slot!.seen(false);
    claimWidgetSlot(vi.fn());
    // The block that lost its slot lets go of it, and hears of nothing more.
    holders[0].slot!.release();
    holders[0].slot!.seen(true);
    expect(liveWidgets()).toBe(MAX_LIVE_WIDGETS);
    expect(claimWidgetSlot(vi.fn())).toBeNull();
  });

  it('keeps a long chat at the limit however many widgets scroll by', () => {
    const blocks = Array.from({ length: 40 }, block);
    let live = 0;
    for (const [index, one] of blocks.entries()) {
      one.evict.mockImplementation(() => { one.slot = null; live -= 1; });
      one.slot = claimWidgetSlot(one.evict);
      expect(one.slot, `block ${index}`).not.toBeNull();
      live += 1;
      // The block two places up has left the screen.
      blocks[index - 2]?.slot?.seen(false);
      expect(liveWidgets()).toBeLessThanOrEqual(MAX_LIVE_WIDGETS);
      expect(live).toBe(liveWidgets());
    }
    expect(liveWidgets()).toBe(MAX_LIVE_WIDGETS);
    // The newest ones run; the oldest gave way.
    expect(blocks.slice(-MAX_LIVE_WIDGETS).every(b => b.slot !== null)).toBe(true);
    expect(blocks.slice(0, 40 - MAX_LIVE_WIDGETS).every(b => b.slot === null)).toBe(true);
  });
});
