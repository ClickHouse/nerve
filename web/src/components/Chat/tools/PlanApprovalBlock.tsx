import { useState, useRef } from 'react';
import { FileCheck, Play, Ban, Check } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';
import { useChatStore } from '../../../stores/chatStore';

export function PlanApprovalBlock({ block }: { block: ToolCallBlockData }) {
  const pendingInteraction = useChatStore(s => s.pendingInteraction);
  const answerInteraction = useChatStore(s => s.answerInteraction);
  const denyInteraction = useChatStore(s => s.denyInteraction);
  const [responded, setResponded] = useState(false);
  const [approved, setApproved] = useState(false);

  const isExitPlan = block.tool === 'ExitPlanMode';
  const isEnterPlan = block.tool === 'EnterPlanMode';
  const isInteractive = pendingInteraction && (
    (isExitPlan && pendingInteraction.interactionType === 'plan_exit') ||
    (isEnterPlan && pendingInteraction.interactionType === 'plan_enter')
  );
  // Latch that the prompt was once live. A later disappearance then means it was
  // resolved (here or by a parallel client) — distinguishes the post-resolution
  // settling window from the pre-prompt one so we don't show a stale "waiting".
  const seenInteractive = useRef(false);
  if (isInteractive) seenInteractive.current = true;

  // Already responded or tool completed
  if (responded || block.status === 'complete') {
    const wasApproved = approved || (block.result && !block.isError);
    return (
      <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
        <div className="px-3 py-2.5 flex items-center gap-2">
          {wasApproved
            ? <Check size={14} className="text-hue-green" />
            : <Ban size={14} className="text-hue-red" />
          }
          <span className="text-[13px] font-medium text-text-secondary">
            {isExitPlan ? 'Plan' : 'Plan mode'} {wasApproved ? 'approved' : 'declined'}
          </span>
        </div>
      </div>
    );
  }

  // Waiting for user input (pre-prompt), or settling after another client
  // resolved it while this client's tool_result hasn't landed yet.
  if (!isInteractive) {
    const settling = seenInteractive.current;
    return (
      <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
        <div className="px-3 py-2.5 flex items-center gap-2">
          <FileCheck size={14} className="text-text-muted animate-pulse" />
          <span className="text-[13px] text-text-muted">
            {settling
              ? 'Resolving…'
              : (isExitPlan ? 'Waiting to approve plan...' : 'Waiting to enter plan mode...')}
          </span>
        </div>
      </div>
    );
  }

  return (
    <div className="my-2">
      <div className="border border-accent/20 rounded-lg bg-bg-sunken overflow-hidden">
        <div className="px-4 py-3">
          <div className="flex items-center gap-2 mb-2">
            {isExitPlan
              ? <FileCheck size={15} className="text-accent" />
              : <Play size={15} className="text-accent" />
            }
            <span className="text-[13px] font-medium text-text">
              {isExitPlan
                ? 'Plan ready for approval'
                : 'Claude wants to enter plan mode'
              }
            </span>
          </div>
          {isExitPlan && (
            <p className="text-[12px] text-text-muted mb-3">
              Review the plan in the side panel, then approve or decline.
            </p>
          )}
          {isEnterPlan && (
            <p className="text-[12px] text-text-muted mb-3">
              The agent will explore the codebase and design an implementation approach for your approval.
            </p>
          )}
          <div className="flex gap-2">
            <button
              onClick={() => { setResponded(true); setApproved(true); answerInteraction(null); }}
              className="flex-1 py-2 rounded-md text-[13px] font-medium bg-accent hover:bg-accent-hover text-white cursor-pointer transition-colors flex items-center justify-center gap-2"
            >
              <Check size={13} />
              {isExitPlan ? 'Approve' : 'Allow'}
            </button>
            <button
              onClick={() => { setResponded(true); setApproved(false); denyInteraction('User declined.'); }}
              className="flex-1 py-2 rounded-md text-[13px] font-medium bg-surface-raised hover:bg-surface-hover text-text-muted cursor-pointer transition-colors flex items-center justify-center gap-2"
            >
              <Ban size={13} />
              Decline
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
