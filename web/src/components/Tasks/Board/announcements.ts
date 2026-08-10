/**
 * Screen-reader wording for the board's drag gestures.
 *
 * Kept out of the component and pure for the same reason as `dropIntent`:
 * this is the only account of the drag a screen-reader user gets, and it is
 * invisible to everyone else, so it needs tests rather than a look.
 *
 * The identifiers involved are a task slug ("2026-08-05-fix-the-encoder")
 * and namespaced keys ("lane:pending", "col:pending"), so announcing ids
 * reads out the data model instead of the board. Every draggable and
 * droppable already carries a typed payload; these messages name the thing
 * from that.
 *
 * Cards and columns share one DndContext, so `active` is not always a task
 * and the wording has to come from the payload rather than be assumed.
 */
import type { Announcements, Active, Over } from '@dnd-kit/core';
import type { Task } from '../../../api/client';

/** `labelFor` resolves a status name to its configured display label. */
export function boardAnnouncements(labelFor: (status: string) => string): Announcements {
  const describe = (node: Active | Over | null): string => {
    const data = node?.data.current;
    if (data?.type === 'task') return `task ${(data.task as Task).title}`;
    if (data?.type === 'lane') return `the ${labelFor(String(data.status))} lane`;
    if (data?.type === 'column') return `the ${labelFor(String(data.status))} column`;
    // Every draggable and droppable on the board sets `data`, so this is
    // unreachable — but an announcement is the wrong place to throw, and a
    // bare id still says more than an empty string.
    return node ? String(node.id) : 'nothing';
  };

  const sentence = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);

  return {
    onDragStart: ({ active }) => `Picked up ${describe(active)}.`,
    onDragOver: ({ active, over }) =>
      over ? `${sentence(describe(active))} is over ${describe(over)}.` : '',
    onDragEnd: ({ active, over }) =>
      over
        ? `${sentence(describe(active))} dropped on ${describe(over)}.`
        : `${sentence(describe(active))} dropped, position unchanged.`,
    onDragCancel: ({ active }) => `Move of ${describe(active)} cancelled.`,
  };
}
