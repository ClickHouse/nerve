import type { SetupStep } from '../../api/client';
import { Badge } from '../ui';
import { Check, CircleDashed, Lock } from '../ui/icons';
import { closedByPolicy, waitsForMode } from './navigation';

/**
 * A step's status, as a chip with a glyph — so no state is colour-only.
 * A read-only done step (the mode, once chosen) carries a lock as well.
 *
 * Three kinds of `blocked`: a step that merely waits for Mode to be chosen
 * reads "After Mode" without a lock (sequencing); one the instance has
 * closed because it keeps no credentials on disk reads "Not available";
 * lockdown and "no config dir" keep the lock and "Blocked".
 */
export function StatusBadge({ step }: { step: Pick<SetupStep, 'status' | 'read_only' | 'blocked_reason' | 'blocked_kind'> }) {
  if (step.status === 'done') {
    return (
      <Badge tone="success">
        <Check size={11} />Done{step.read_only && <Lock size={10} aria-label="locked" />}
      </Badge>
    );
  }
  if (step.status === 'skipped') return <Badge tone="neutral">Skipped</Badge>;
  if (step.status === 'blocked') {
    if (waitsForMode(step)) return <Badge tone="neutral">After Mode</Badge>;
    if (closedByPolicy(step)) return <Badge tone="neutral"><Lock size={11} />Not available</Badge>;
    return <Badge tone="neutral"><Lock size={11} />Blocked</Badge>;
  }
  return <Badge tone="warning"><CircleDashed size={11} />To do</Badge>;
}
