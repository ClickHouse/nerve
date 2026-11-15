import { useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft } from '../components/ui/icons';
import { IconButton } from '../components/ui';
import { usePlanStore } from '../stores/planStore';
import {
  PlanBadges,
  PlanContent,
  PlanDecisionBar,
  PlanSummaryAndFacts,
} from '../components/Plans/PlanDetailParts';

export function PlanDetailPage() {
  const { planId } = useParams<{ planId: string }>();
  const navigate = useNavigate();
  const plan = usePlanStore(s => s.selectedPlan);
  const detailLoading = usePlanStore(s => s.detailLoading);
  const loadPlan = usePlanStore(s => s.loadPlan);
  const clearSelectedPlan = usePlanStore(s => s.clearSelectedPlan);

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

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="border-b border-border-subtle px-4 md:px-6 py-3 bg-bg shrink-0">
        <div className="flex items-center gap-3 mb-2">
          <IconButton label="Back to plans" size="xs" onClick={() => navigate('/plans')}>
            <ArrowLeft size={16} />
          </IconButton>
          <h1 className="text-lg font-semibold text-text">
            {plan.task_title || plan.task_id}
          </h1>
          <PlanBadges plan={plan} />
        </div>
        <PlanSummaryAndFacts plan={plan} onViewTask={() => navigate(`/tasks/${plan.task_id}`)} />
      </div>

      {/* Plan content */}
      <div className="flex-1 overflow-y-auto p-4 md:p-6">
        <PlanContent plan={plan} />
      </div>

      {/* The decision stays in view at every scroll position: the plan
          scrolls above it, and on a phone the bottom nav sits below it. */}
      <PlanDecisionBar
        plan={plan}
        onApproved={(sessionId) => navigate(`/chat/${sessionId}`)}
        className="shrink-0 border-t border-border-subtle bg-bg px-4 md:px-6 py-3"
      />
    </div>
  );
}
