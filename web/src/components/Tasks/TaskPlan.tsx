import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../../api/client';
import type { Plan } from '../../stores/planStore';
import { PlanStatusBadge } from '../Plans/PlanStatusBadge';
import { Button } from '../ui';
import { ExternalLink, Lightbulb } from '../ui/icons';

/**
 * The latest plan of a task: its version, its status and a link to it.
 * Nothing shows when the task has no plan.
 *
 * A pending plan with no feedback waits for a decision, so it gets a call
 * to action. A pending plan with feedback has a revision request: the
 * agent writes the next version, and nothing waits for the user.
 *
 * `refreshKey` is the task's `updated_at`. Each plan change (propose,
 * update, revision request, approval, decline) writes a note on the task,
 * and that note changes `updated_at`, so the section shows the change
 * while the task is open, without a reload.
 */
export function TaskPlan({ taskId, refreshKey }: { taskId: string; refreshKey?: string }) {
  const navigate = useNavigate();
  const [plan, setPlan] = useState<Plan | null>(null);

  useEffect(() => {
    let cancelled = false;
    // The server sorts the plans of a task by version, newest first.
    api.getTaskPlans(taskId)
      .then(({ plans }) => { if (!cancelled) setPlan((plans[0] as Plan | undefined) ?? null); })
      .catch(() => { if (!cancelled) setPlan(null); });
    return () => { cancelled = true; };
  }, [taskId, refreshKey]);

  if (!plan) return null;

  const openPlan = () => navigate(`/plans/${plan.id}`);

  if (plan.status === 'pending' && !plan.feedback) {
    return (
      <section
        aria-label="Plan"
        className="flex items-center gap-3 px-5 py-2.5 border-b border-border-subtle bg-surface-raised text-sm shrink-0 flex-wrap"
      >
        <Lightbulb size={14} className="text-hue-amber shrink-0" />
        <span className="text-text">A plan waits for your review</span>
        <span className="text-xs text-text-dim">v{plan.version}</span>
        <Button variant="primary" size="xs" className="ml-auto" onClick={openPlan}>
          Open plan
        </Button>
      </section>
    );
  }

  return (
    <section
      aria-label="Plan"
      className="flex items-center gap-2 px-5 py-2 border-b border-border-subtle text-xs shrink-0 flex-wrap"
    >
      <Lightbulb size={12} className="text-text-dim shrink-0" />
      <span className="text-text-dim">Plan v{plan.version}</span>
      <PlanStatusBadge status={plan.status} />
      {plan.status === 'pending' && (
        <span className="text-text-dim">Revision requested. The agent is updating the plan.</span>
      )}
      <Button variant="link" size="xs" onClick={openPlan}>
        <ExternalLink size={11} /> Open plan
      </Button>
    </section>
  );
}
