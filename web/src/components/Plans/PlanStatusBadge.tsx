import { Badge, type BadgeTone } from '../ui';

/** A plan's lifecycle state, as a `Badge` tone. An unknown state is `neutral`. */
const STATUS_TONES: Record<string, BadgeTone> = {
  pending: 'warning',
  approved: 'success',
  implementing: 'info',
  declined: 'danger',
  superseded: 'neutral',
  failed: 'danger',
};

/** The status chip of a plan, the same on the plan list, the plan page and the task page. */
export function PlanStatusBadge({ status }: { status: string }) {
  return (
    <Badge size="sm" pill outline tone={STATUS_TONES[status] ?? 'neutral'}>
      {status}
    </Badge>
  );
}
