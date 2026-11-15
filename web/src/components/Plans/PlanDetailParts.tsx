import { ExternalLink } from '../ui/icons';
import { Badge, Button } from '../ui';
import { PlanStatusBadge } from './PlanStatusBadge';
import { PlanProgressCard } from './PlanProgressCard';
import { usePlanStore, type Plan } from '../../stores/planStore';
import { MarkdownContent } from '../Chat/MarkdownContent';
import { ActorAttribution } from '../Chat/ActorLabel';
import { planSummaryFallback } from '../Chat/planDecision';
import { PlanDecisionControls } from '../Chat/PlanDecisionControls';

/**
 * The parts of one plan's detail view. The plan page and the plan dialog in
 * the chat (`PlanReviewModal`) both build on them, so the two cannot drift.
 */

/** Plan type is identity, not status — hence `purple` rather than a feedback tone. */
const TYPE_LABELS: Record<string, string> = {
  'skill-create': 'Skill',
  'skill-update': 'Skill Update',
};

/**
 * The bar's words. Approving starts the implementation session; a revision
 * goes to the session that proposed the plan.
 */
const DECISION_LABELS = { approve: 'Approve & implement', requestChanges: 'Request revision…' };

/**
 * The text under the title: the summary from the agent that wrote the plan,
 * else the plan's first heading and the two lines after it.
 */
function planSummaryText(plan: Plan): string {
  const own = plan.summary?.trim();
  if (own) return own;
  const { heading, lines } = planSummaryFallback(plan.content);
  return [heading, ...lines].filter(Boolean).join('\n');
}

/** Status, type and version of the plan, as they follow its title. */
export function PlanBadges({ plan }: { plan: Plan }) {
  return (
    <>
      <PlanStatusBadge status={plan.status} />
      {plan.plan_type && plan.plan_type !== 'generic' && TYPE_LABELS[plan.plan_type] && (
        <Badge size="sm" pill outline tone="purple">
          {TYPE_LABELS[plan.plan_type]}
        </Badge>
      )}
      <span className="text-xs text-text-faint">v{plan.version}</span>
    </>
  );
}

/** The summary and the line of facts (dates, people, model, link to the task). */
export function PlanSummaryAndFacts({
  plan,
  onViewTask,
  className = '',
}: {
  plan: Plan;
  onViewTask: () => void;
  className?: string;
}) {
  const summary = planSummaryText(plan);
  return (
    <div className={className}>
      {summary && (
        <p
          data-testid="plan-summary"
          title={summary}
          className="ml-7 mb-2 max-w-3xl text-sm leading-snug text-text-secondary whitespace-pre-line line-clamp-3"
        >
          {summary}
        </p>
      )}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-text-faint ml-7">
        <span className="whitespace-nowrap">{plan.created_at?.slice(0, 16).replace('T', ' ')}</span>
        {plan.created_by_actor_id && (
          <span data-testid="plan-author" className="inline-flex items-center gap-1 min-w-0">
            Proposed by{' '}
            <ActorAttribution
              actorId={plan.created_by_actor_id}
              causeId={plan.caused_by_actor_id}
              verb="Proposed by"
            />
          </span>
        )}
        {plan.model && <span>{plan.model}</span>}
        {plan.reviewed_by_actor_id && (
          <span data-testid="plan-reviewer" className="inline-flex items-center gap-1 min-w-0">
            Reviewed by{' '}
            <ActorAttribution
              actorId={plan.reviewed_by_actor_id}
              causeId={plan.reviewed_caused_by_actor_id}
              verb="Reviewed by"
            />
            {plan.reviewed_at && (
              <span>{plan.reviewed_at.slice(0, 16).replace('T', ' ')}</span>
            )}
          </span>
        )}
        <Button variant="link" size="xs" onClick={onViewTask}>
          <ExternalLink size={11} /> View task
        </Button>
      </div>
    </div>
  );
}

/** The progress card, the plan text and the feedback of the last revision request. */
export function PlanContent({ plan }: { plan: Plan }) {
  return (
    <div className="max-w-3xl mx-auto">
      <PlanProgressCard plan={plan} />
      <div className="bg-surface border border-border-subtle rounded-lg p-4 md:p-6">
        <MarkdownContent content={plan.content} />
      </div>

      {/* Feedback from previous revision — quote style */}
      {plan.feedback && (
        <div className="mt-4 flex gap-0">
          <div className="w-1 bg-accent/40 rounded-full shrink-0" />
          <div className="pl-3 py-2">
            <div className="text-xs text-accent/60 font-medium mb-1">Revision feedback</div>
            <div className="text-sm text-text-muted leading-relaxed whitespace-pre-wrap">{plan.feedback}</div>
          </div>
        </div>
      )}
    </div>
  );
}

/**
 * The decision bar of a pending plan: approve, ask for a revision, or
 * decline. Nothing renders for a plan that is not pending. `onApproved`
 * gets the id of the implementation session that approving started.
 */
export function PlanDecisionBar({
  plan,
  onApproved,
  className = '',
}: {
  plan: Plan;
  onApproved: (implSessionId: string) => void;
  className?: string;
}) {
  const actionLoading = usePlanStore(s => s.actionLoading);
  const actionError = usePlanStore(s => s.actionError);
  const updatePlan = usePlanStore(s => s.updatePlan);
  const approvePlan = usePlanStore(s => s.approvePlan);
  const revisePlan = usePlanStore(s => s.revisePlan);

  if (plan.status !== 'pending') return null;

  const handleApprove = async () => {
    const result = await approvePlan(plan.id);
    if (result?.impl_session_id) onApproved(result.impl_session_id);
  };

  return (
    <section aria-label="Plan decision" className={className}>
      <div className="max-w-3xl mx-auto space-y-2">
        {actionError && <p role="alert" className="text-xs text-hue-red">{actionError}</p>}
        <PlanDecisionControls
          layout="bar"
          labels={DECISION_LABELS}
          disabled={actionLoading}
          onApprove={handleApprove}
          onRequestChanges={(feedback) => revisePlan(plan.id, feedback)}
          onDecline={(reason) => updatePlan(plan.id, 'declined', reason || undefined)}
        />
      </div>
    </section>
  );
}
