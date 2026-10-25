import type { SetupStep } from '../../api/client';
import { Badge } from '../ui';
import { Check, CircleDashed, Lock } from '../ui/icons';

/**
 * A step's status, as a chip with a glyph — so no state is colour-only.
 * A read-only done step (the mode, once chosen) carries a lock as well.
 *
 * The page shows a badge only for the person's own steps (`stepPlace`). A
 * `blocked` step among them only waits for Mode to be chosen: that is
 * sequencing, so it reads "After Mode" without a lock.
 */
export function StatusBadge({ step }: { step: Pick<SetupStep, 'status' | 'read_only'> }) {
  if (step.status === 'done') {
    return (
      <Badge tone="success">
        <Check size={11} />Done{step.read_only && <Lock size={10} aria-label="locked" />}
      </Badge>
    );
  }
  if (step.status === 'skipped') return <Badge tone="neutral">Skipped</Badge>;
  if (step.status === 'blocked') return <Badge tone="neutral">After Mode</Badge>;
  return <Badge tone="warning"><CircleDashed size={11} />To do</Badge>;
}
