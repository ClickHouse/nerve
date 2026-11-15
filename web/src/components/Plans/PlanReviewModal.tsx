import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { IconButton, Modal } from '../ui';
import { Maximize2 } from '../ui/icons';
import { usePlanStore } from '../../stores/planStore';
import {
  PlanBadges,
  PlanContent,
  PlanDecisionBar,
  PlanSummaryAndFacts,
} from './PlanDetailParts';

/**
 * One plan over the chat, so that the person reviews it without leaving the
 * conversation. It shows what `/plans/:planId` shows, with the same decision
 * bar; the button in the title opens the full page instead.
 *
 * The plan comes from the plan store's selected plan, which the full page
 * uses as well. The two never show together, because the dialog covers the
 * chat and the page is another route.
 */
export function PlanReviewModal({ planId, onClose }: { planId: string; onClose: () => void }) {
  const navigate = useNavigate();
  const plan = usePlanStore(s => s.selectedPlan);
  const detailLoading = usePlanStore(s => s.detailLoading);
  const loadPlan = usePlanStore(s => s.loadPlan);
  const clearSelectedPlan = usePlanStore(s => s.clearSelectedPlan);

  useEffect(() => {
    void loadPlan(planId);
    return () => clearSelectedPlan();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [planId]);

  // The plan that is selected may be another one for a moment, while the
  // store swaps to this id.
  const shown = plan?.id === planId ? plan : null;

  /** Leave for another route: the dialog closes first, so it does not cover it. */
  const leaveTo = (path: string) => {
    onClose();
    navigate(path);
  };

  const title = detailLoading ? 'Loading...' : shown ? (shown.task_title || shown.task_id) : 'Plan not found';

  return (
    <Modal
      open
      onClose={onClose}
      title={
        <span className="flex flex-wrap items-center gap-x-2 gap-y-1 min-w-0">
          <span className="min-w-0 break-words">{title}</span>
          {shown && <PlanBadges plan={shown} />}
          <IconButton label="Open as full page" size="xs" onClick={() => leaveTo(`/plans/${planId}`)}>
            <Maximize2 size={13} />
          </IconButton>
        </span>
      }
      // A plan is a document, not a form: it needs room to read.
      size="wide"
      className="h-[85vh] max-h-[85vh]"
      footer={shown && (
        <PlanDecisionBar
          plan={shown}
          onApproved={(sessionId) => leaveTo(`/chat/${sessionId}`)}
        />
      )}
      footerClassName="px-5 py-3 empty:hidden"
    >
      {!shown && (
        <div className="p-8 text-center text-text-faint text-sm">
          {detailLoading ? 'Loading...' : 'Plan not found.'}
        </div>
      )}
      {shown && (
        <>
          <PlanSummaryAndFacts
            plan={shown}
            onViewTask={() => leaveTo(`/tasks/${shown.task_id}`)}
            className="px-5 pt-3"
          />
          <div className="p-4 md:p-5">
            <PlanContent plan={shown} />
          </div>
        </>
      )}
    </Modal>
  );
}
