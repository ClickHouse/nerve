import { describe, expect, it } from 'vitest';
import type { Active, Over } from '@dnd-kit/core';
import { boardAnnouncements } from './announcements';

/**
 * These strings are the whole of the board for a screen-reader user, and
 * nothing on screen shows them, so a regression here is silent. The ids
 * involved are slugs and namespaced keys, which is exactly what must never
 * reach the announcement.
 */

const labelFor = (name: string) =>
  ({ pending: 'Pending', in_progress: 'In Progress' })[name] ?? name;

const a = boardAnnouncements(labelFor);

/** dnd-kit hands these over as refs, hence the `current` indirection. */
const card = (id: string, title: string) =>
  ({ id, data: { current: { type: 'task', task: { id, title } } } }) as unknown as Active;

/** A card is both draggable and droppable — the two shapes differ. */
const overCard = (id: string, title: string) =>
  card(id, title) as unknown as Over;

const lane = (status: string) =>
  ({ id: `lane:${status}`, data: { current: { type: 'lane', status } } }) as unknown as Over;

describe('boardAnnouncements', () => {
  const dragged = card('2026-08-05-fix-the-encoder', 'Fix the encoder');

  it('names the task instead of reading out its slug', () => {
    expect(a.onDragStart({ active: dragged })).toBe('Picked up task Fix the encoder.');
  });

  it('uses the configured label for a lane, not the status name', () => {
    expect(a.onDragOver({ active: dragged, over: lane('in_progress') })).toBe(
      'Task Fix the encoder is over the In Progress lane.',
    );
  });

  it('says it is over a task when the target is a card, not a lane', () => {
    // The regression: every target was described as a lane, so hovering a
    // card announced "... is over <slug> lane."
    expect(a.onDragOver({ active: dragged, over: overCard('b', 'Something else') })).toBe(
      'Task Fix the encoder is over task Something else.',
    );
  });

  it('does not leak the lane: prefix on drop', () => {
    expect(a.onDragEnd({ active: dragged, over: lane('pending') })).toBe(
      'Task Fix the encoder dropped on the Pending lane.',
    );
  });

  it('reports a drop outside any target as unchanged', () => {
    expect(a.onDragEnd({ active: dragged, over: null })).toBe(
      'Task Fix the encoder dropped, position unchanged.',
    );
  });

  it('says nothing while over no target', () => {
    expect(a.onDragOver({ active: dragged, over: null })).toBe('');
  });

  it('names what was cancelled', () => {
    expect(a.onDragCancel({ active: dragged, over: null })).toBe(
      'Move of task Fix the encoder cancelled.',
    );
  });

  it('falls back to the id rather than throwing on an untyped payload', () => {
    const bare = { id: 'mystery', data: { current: undefined } } as unknown as Active;
    expect(a.onDragStart({ active: bare })).toBe('Picked up mystery.');
  });

  it('falls back to the status name when it has no configured label', () => {
    expect(a.onDragEnd({ active: dragged, over: lane('triage') })).toBe(
      'Task Fix the encoder dropped on the triage lane.',
    );
  });
});
