import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Check, X, MessageSquare, ExternalLink } from '../components/ui/icons';
import { Badge, type BadgeTone, Button, IconButton, TextArea } from '../components/ui';
import { usePlanStore } from '../stores/planStore';
import { MarkdownContent } from '../components/Chat/MarkdownContent';

/** A plan's lifecycle state, as a `Badge` tone. `superseded` is the fallback. */
const STATUS_TONES: Record<string, BadgeTone> = {
  pending: 'warning',
  approved: 'success',
  implementing: 'info',
  declined: 'danger',
  superseded: 'neutral',
  failed: 'danger',
};

/** Plan type is identity, not status — hence `purple` rather than a feedback tone. */
const TYPE_LABELS: Record<string, string> = {
  'skill-create': 'Skill',
  'skill-update': 'Skill Update',
};

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
    clearActionError,
    clearSelectedPlan,
  } = usePlanStore();
  const [feedback, setFeedback] = useState('');
  const [showFeedback, setShowFeedback] = useState(false);
  const [declineFeedback, setDeclineFeedback] = useState('');
  const [showDeclineFeedback, setShowDeclineFeedback] = useState(false);

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
  const isImplementing = plan.status === 'implementing';

  const handleApprove = async () => {
    const result = await approvePlan(plan.id);
    if (result?.impl_session_id) {
      navigate(`/chat/${result.impl_session_id}`);
    }
  };

  const handleDecline = () => {
    updatePlan(plan.id, 'declined', declineFeedback.trim() || undefined);
    setDeclineFeedback('');
    setShowDeclineFeedback(false);
  };

  const handleRevise = async () => {
    if (!feedback.trim()) return;
    const ok = await revisePlan(plan.id, feedback.trim());
    if (ok) {
      setFeedback('');
      setShowFeedback(false);
    }
  };

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
          <Badge size="sm" pill outline tone={STATUS_TONES[plan.status] ?? 'neutral'}>
            {plan.status}
          </Badge>
          {plan.plan_type && plan.plan_type !== 'generic' && TYPE_LABELS[plan.plan_type] && (
            <Badge size="sm" pill outline tone="purple">
              {TYPE_LABELS[plan.plan_type]}
            </Badge>
          )}
          <span className="text-xs text-text-faint">v{plan.version}</span>
        </div>
        <div className="flex items-center gap-4 text-xs text-text-faint ml-7">
          <span>{plan.created_at?.slice(0, 16).replace('T', ' ')}</span>
          {plan.model && <span>{plan.model}</span>}
          <Button variant="link" size="xs" onClick={() => navigate(`/tasks/${plan.task_id}`)}>
            <ExternalLink size={11} /> View task
          </Button>
          {isImplementing && plan.impl_session_id && (
            // House convention for a tinted inline action: the identity hue
            // rides the icon, the label stays neutral.
            <Button variant="ghost" size="xs" onClick={() => navigate(`/chat/${plan.impl_session_id}`)}>
              <MessageSquare size={11} className="text-hue-blue" /> Watch implementation
            </Button>
          )}
        </div>
      </div>

      {/* Plan content */}
      <div className="flex-1 overflow-y-auto p-6">
        <div className="max-w-3xl mx-auto">
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

          {/* Action bar for pending plans */}
          {isPending && (
            <div className="mt-6 space-y-3">
              <div className="flex items-center gap-3">
                {/* Approving is this page's one committing action, so it takes
                    the `primary` treatment — a deliberate green→accent change.
                    It was `bg-emerald-600 text-white`, and the alternatives do
                    not hold: `success` is tinted, which would leave the
                    affirmative action quieter than the `dangerSolid` Decline
                    beside it, and there is no solid-success pair to build one
                    from — `--theme-success` is a feedback *foreground*. */}
                <Button variant="primary" size="md" onClick={handleApprove} disabled={actionLoading}>
                  <Check size={14} />
                  Approve &amp; Implement
                </Button>
                <Button
                  variant="dangerSolid"
                  size="md"
                  onClick={() => setShowDeclineFeedback(!showDeclineFeedback)}
                  disabled={actionLoading}
                >
                  <X size={14} /> Decline
                </Button>
                <Button variant="secondary" size="md" onClick={() => setShowFeedback(!showFeedback)}>
                  <MessageSquare size={14} /> Request Revision
                </Button>
              </div>

              {showDeclineFeedback && (
                <div className="space-y-2">
                  <div className="flex gap-0">
                    <div className="w-1 bg-error/40 rounded-full shrink-0" />
                    <div className="flex-1 pl-3">
                      {/* The red focus border is gone on purpose: FIELD_BASE is
                          the app's one focus treatment and it is accent. */}
                      <TextArea
                        value={declineFeedback}
                        onChange={e => setDeclineFeedback(e.target.value)}
                        placeholder="Optional: why is this plan being declined? (leave empty to close without a reason)"
                        rows={3}
                        autoFocus
                      />
                    </div>
                  </div>
                  <div className="flex justify-end">
                    <Button variant="dangerSolid" size="md" onClick={handleDecline} disabled={actionLoading}>
                      Confirm Decline
                    </Button>
                  </div>
                </div>
              )}

              {showFeedback && (
                <div className="space-y-2">
                  <div className="flex gap-0">
                    <div className="w-1 bg-accent/30 rounded-full shrink-0" />
                    <div className="flex-1 pl-3">
                      <TextArea
                        value={feedback}
                        onChange={e => {
                          setFeedback(e.target.value);
                          if (actionError) clearActionError();
                        }}
                        placeholder="Describe what to change..."
                        rows={3}
                        autoFocus
                      />
                    </div>
                  </div>
                  {actionError && (
                    <div className="flex gap-0">
                      <div className="w-1 bg-error/40 rounded-full shrink-0" />
                      <div className="pl-3 py-1 text-xs text-hue-red">{actionError}</div>
                    </div>
                  )}
                  <div className="flex justify-end">
                    <Button
                      variant="primary"
                      size="md"
                      onClick={handleRevise}
                      disabled={actionLoading || !feedback.trim()}
                    >
                      Send Revision Request
                    </Button>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
