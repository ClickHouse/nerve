import { useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, ExternalLink } from '../components/ui/icons';
import { Badge, Button, IconButton } from '../components/ui';
import { PlanStatusBadge } from '../components/Plans/PlanStatusBadge';
import { PlanProgressCard } from '../components/Plans/PlanProgressCard';
import { usePlanStore, type Plan } from '../stores/planStore';
import { MarkdownContent } from '../components/Chat/MarkdownContent';
import { ActorAttribution } from '../components/Chat/ActorLabel';
import { planSummaryFallback } from '../components/Chat/planDecision';
import { PlanDecisionControls } from '../components/Chat/PlanDecisionControls';

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

export function PlanDetailPage() {
  const { planId } = useParams<{ planId: string }>();
  const navigate = useNavigate();
  const {
    selectedPlan: plan,
    detailLoading,
    actionLoading,
    actionError,
    loadPlan,
    updatePlan,
    approvePlan,
    revisePlan,
    clearSelectedPlan,
  } = usePlanStore();

  useEffect(() => {
    if (planId) loadPlan(planId);
    return () => clearSelectedPlan();
  }, [planId]);

  if (detailLoading || !plan) {
    return (
      <div className="h-full flex items-center justify-center text-text-faint">
        {detailLoading ? 'Loading...' : 'Plan not found'}
      </div>
    );
  }

  const isPending = plan.status === 'pending';
  const summary = planSummaryText(plan);

  const handleApprove = async () => {
    const result = await approvePlan(plan.id);
    if (result?.impl_session_id) {
      navigate(`/chat/${result.impl_session_id}`);
    }
  };

  const handleDecline = (reason: string) => updatePlan(plan.id, 'declined', reason || undefined);

  const handleRevise = (feedback: string) => revisePlan(plan.id, feedback);

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="border-b border-border-subtle px-6 py-3 bg-bg shrink-0">
        <div className="flex items-center gap-3 mb-2">
          <IconButton label="Back to plans" size="xs" onClick={() => navigate('/plans')}>
            <ArrowLeft size={16} />
          </IconButton>
          <h1 className="text-lg font-semibold text-text">
            {plan.task_title || plan.task_id}
          </h1>
          <PlanStatusBadge status={plan.status} />
          {plan.plan_type && plan.plan_type !== 'generic' && TYPE_LABELS[plan.plan_type] && (
            <Badge size="sm" pill outline tone="purple">
              {TYPE_LABELS[plan.plan_type]}
            </Badge>
          )}
          <span className="text-xs text-text-faint">v{plan.version}</span>
        </div>
        {summary && (
          <p
            data-testid="plan-summary"
            title={summary}
            className="ml-7 mb-2 max-w-3xl text-sm leading-snug text-text-secondary whitespace-pre-line line-clamp-3"
          >
            {summary}
          </p>
        )}
        <div className="flex items-center gap-4 text-xs text-text-faint ml-7">
          <span>{plan.created_at?.slice(0, 16).replace('T', ' ')}</span>
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
          <Button variant="link" size="xs" onClick={() => navigate(`/tasks/${plan.task_id}`)}>
            <ExternalLink size={11} /> View task
          </Button>
        </div>
      </div>

      {/* Plan content */}
      <div className="flex-1 overflow-y-auto p-6">
        <div className="max-w-3xl mx-auto">
          <PlanProgressCard plan={plan} />
          <div className="bg-surface border border-border-subtle rounded-lg p-6">
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
      </div>

      {/* The decision stays in view at every scroll position: the plan
          scrolls above it, and on a phone the bottom nav sits below it. */}
      {isPending && (
        <section aria-label="Plan decision" className="shrink-0 border-t border-border-subtle bg-bg px-4 md:px-6 py-3">
          <div className="max-w-3xl mx-auto space-y-2">
            {actionError && <p role="alert" className="text-xs text-hue-red">{actionError}</p>}
            <PlanDecisionControls
              layout="bar"
              labels={DECISION_LABELS}
              disabled={actionLoading}
              onApprove={handleApprove}
              onRequestChanges={handleRevise}
              onDecline={handleDecline}
            />
          </div>
        </section>
      )}
    </div>
  );
}
