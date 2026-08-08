import { describe, expect, it } from 'vitest';
import type { Task } from '../../../api/client';
import type { Lane } from '../../../stores/taskStore';
import { isNoOpMove, resolveDropIntent } from './dropIntent';

/**
 * Drop resolution: "card X landed on target Y" → the neighbour pair the
 * move API expects.
 *
 * This is where drag-and-drop bugs actually live. The classic one is
 * dragging a card *downward* within its own lane: if the moved card isn't
 * excluded from the lane before computing anchors, it anchors against its
 * own current position and lands one slot short of where it was dropped.
 * Every direction is pinned below for that reason.
 */

function task(id: string, status = 'pending', position = 0): Task {
  return {
    id, title: id, status, position,
    deadline: null, source: 'manual', source_url: null, tags: '',
    created_at: '2026-08-05T00:00:00Z', updated_at: '2026-08-05T00:00:00Z',
  };
}

function lanes(): Lane[] {
  return [
    {
      status: 'pending',
      total: 3,
      tasks: [task('a', 'pending', 1024), task('b', 'pending', 2048), task('c', 'pending', 3072)],
    },
    { status: 'in_progress', total: 1, tasks: [task('x', 'in_progress', 1024)] },
    { status: 'done', total: 0, tasks: [] },
  ];
}

describe('resolveDropIntent — within a lane', () => {
  it('dropping on the card above puts the card in that slot', () => {
    // c dropped onto a → c takes a's place, a is pushed down.
    expect(resolveDropIntent(lanes(), 'c', 'a')).toEqual({
      status: 'pending', beforeId: null, afterId: 'a',
    });
  });

  it('dropping on a middle card anchors between its neighbours', () => {
    expect(resolveDropIntent(lanes(), 'c', 'b')).toEqual({
      status: 'pending', beforeId: 'a', afterId: 'b',
    });
  });

  it('dragging downward excludes the moved card from its own anchors', () => {
    // a dropped onto c. With `a` still in the list, index(c) === 2 and the
    // preceding card would be `b` — correct only by accident. Excluding `a`
    // first gives index 1, preceded by `b`, followed by `c`. The bug shows
    // up as the card landing above `c` instead of taking its slot.
    expect(resolveDropIntent(lanes(), 'a', 'c')).toEqual({
      status: 'pending', beforeId: 'b', afterId: 'c',
    });
  });

  it('dragging the top card to the middle', () => {
    expect(resolveDropIntent(lanes(), 'a', 'b')).toEqual({
      status: 'pending', beforeId: null, afterId: 'b',
    });
  });

  it('dropping on the lane background appends to the end', () => {
    expect(resolveDropIntent(lanes(), 'a', 'lane:pending')).toEqual({
      status: 'pending', beforeId: 'c', afterId: null,
    });
  });
});

describe('resolveDropIntent — across lanes', () => {
  it('dropping onto a card in another lane takes its slot', () => {
    expect(resolveDropIntent(lanes(), 'a', 'x')).toEqual({
      status: 'in_progress', beforeId: null, afterId: 'x',
    });
  });

  it('dropping on another lane background appends there', () => {
    expect(resolveDropIntent(lanes(), 'a', 'lane:in_progress')).toEqual({
      status: 'in_progress', beforeId: 'x', afterId: null,
    });
  });

  it('dropping into an empty lane yields no anchors', () => {
    expect(resolveDropIntent(lanes(), 'a', 'lane:done')).toEqual({
      status: 'done', beforeId: null, afterId: null,
    });
  });

  it('returns null for an unrecognised drop target', () => {
    expect(resolveDropIntent(lanes(), 'a', 'lane:nonexistent')).toBeNull();
    expect(resolveDropIntent(lanes(), 'a', 'ghost-task')).toBeNull();
  });
});

describe('isNoOpMove', () => {
  it('detects a drop onto the immediate next card as a no-op', () => {
    // The reachable no-op: `a` dropped onto `b`, which already sits
    // directly below it, resolves to the slot `a` is already in. Without
    // this check every such drag costs a round trip and bumps updated_at
    // for nothing.
    const intent = resolveDropIntent(lanes(), 'a', 'b');
    expect(intent).toEqual({ status: 'pending', beforeId: null, afterId: 'b' });
    expect(isNoOpMove(lanes(), 'a', intent!)).toBe(true);
  });

  it('detects a card dropped on its own origin slot', () => {
    expect(isNoOpMove(lanes(), 'b', { status: 'pending', beforeId: 'a', afterId: 'c' }))
      .toBe(true);
  });

  it('treats a real reorder as a change', () => {
    expect(isNoOpMove(lanes(), 'c', { status: 'pending', beforeId: null, afterId: 'a' }))
      .toBe(false);
  });

  it('treats a lane change as a change even at the same index', () => {
    expect(isNoOpMove(lanes(), 'a', { status: 'in_progress', beforeId: null, afterId: 'x' }))
      .toBe(false);
  });

  it('recognises the tail slot as a no-op for the last card', () => {
    expect(isNoOpMove(lanes(), 'c', { status: 'pending', beforeId: 'b', afterId: null }))
      .toBe(true);
  });
});
