import type { Lane, MoveIntent } from '../../../stores/taskStore';

/**
 * Drag-drop geometry, kept out of the component file so it can be tested
 * directly (and so the component module stays exports-components-only for
 * fast refresh).
 */

/**
 * Translate "card X was dropped on target Y" into the neighbour pair the
 * move API expects.
 *
 * The drop target is either another card or a lane's background. For a
 * card, the moved item takes that card's slot and pushes it down — so the
 * dropped-on card becomes `afterId`, and whatever preceded it becomes
 * `beforeId`.
 *
 * The moved card is excluded from the lane before anchors are read. Skip
 * that and a downward drag within one lane anchors against the card's own
 * current position, landing it one slot short of where it was dropped.
 */
export function resolveDropIntent(
  lanes: Lane[],
  activeId: string,
  overId: string,
): MoveIntent | null {
  const laneFromOver = overId.startsWith('lane:')
    ? lanes.find((l) => l.status === overId.slice('lane:'.length))
    : lanes.find((l) => l.tasks.some((t) => t.id === overId));
  if (!laneFromOver) return null;

  const others = laneFromOver.tasks.filter((t) => t.id !== activeId);
  const appendToTail = (): MoveIntent => ({
    status: laneFromOver.status,
    beforeId: others[others.length - 1]?.id ?? null,
    afterId: null,
  });

  // Dropped on empty space in the column.
  if (overId.startsWith('lane:')) return appendToTail();

  const at = others.findIndex((t) => t.id === overId);
  // The anchor card is gone from this lane (it moved while the drag was in
  // flight, or it *is* the dragged card — which the caller short-circuits).
  if (at === -1) return appendToTail();

  return {
    status: laneFromOver.status,
    beforeId: others[at - 1]?.id ?? null,
    afterId: others[at].id,
  };
}

/** True when the intent would leave the board exactly as it is. */
export function isNoOpMove(
  lanes: Lane[],
  activeId: string,
  intent: MoveIntent,
): boolean {
  const lane = lanes.find((l) => l.status === intent.status);
  if (!lane) return false;
  const at = lane.tasks.findIndex((t) => t.id === activeId);
  if (at === -1) return false; // changing lanes is never a no-op
  const currentBefore = lane.tasks[at - 1]?.id ?? null;
  const currentAfter = lane.tasks[at + 1]?.id ?? null;
  return intent.beforeId === currentBefore && intent.afterId === currentAfter;
}
