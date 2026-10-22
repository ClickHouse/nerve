import { useState } from 'react';
import { Ban, Check, FileCheck, Loader2, MessageSquare, XCircle } from '../../ui/icons';
import type { PanelTab, ToolCallBlockData } from '../../../types/chat';
import { useChatStore } from '../../../stores/chatStore';
import { Button } from '../../ui';
import { PlanDecisionControls } from '../PlanDecisionControls';
import {
  approveEditedPlan, approvePlan, firstLine, PLAN_APPROVED, PLAN_APPROVED_EDITED, planOutcomeOf, planText,
  requestPlanChanges, usePlanDecisionCopy, usePlanDecisionPending, type PlanOutcome,
} from '../planDecision';

/**
 * Plan mode in the chat.
 *
 * `EnterPlanMode` is a status line: entering plan mode only restricts the
 * agent, so the server lets it through without asking (web sessions).
 * `ExitPlanMode` is the decision: the agent waits until the person approves
 * the plan (as it is, or edited) or says what should change. The plan itself
 * shows in the card, so nobody has to find the side panel to read it; the
 * panel stays the long view. A revised plan is a new `ExitPlanMode` call,
 * so it gets a card of its own and the earlier one keeps its outcome.
 */
export function PlanApprovalBlock({ block }: { block: ToolCallBlockData }) {
  return block.tool === 'EnterPlanMode'
    ? <PlanModeLine block={block} />
    : <PlanDecisionBlock block={block} />;
}

// ------------------------------------------------------------------ //
//  EnterPlanMode — a passive status line                               //
// ------------------------------------------------------------------ //

function PlanModeLine({ block }: { block: ToolCallBlockData }) {
  // A server from before plan mode stopped asking may still pause here.
  // Entering plan mode is no decision, so one click lets the agent go on
  // rather than leaving the turn stuck until the pause times out.
  const asks = useChatStore(s => s.pendingInteraction?.interactionType === 'plan_enter');
  const running = block.status === 'running';
  // The finished line names the event, not the plan: it stays true in
  // history, whatever came of the plan afterwards.
  const label = running
    ? (asks ? 'Waiting to start planning' : 'Entering plan mode…')
    : (block.isError ? 'Plan mode declined' : 'Entered plan mode');
  return (
    <div className="my-1.5 flex items-center gap-2 px-1 text-sm text-text-muted">
      {running
        ? <Loader2 size={14} className="shrink-0 motion-safe:animate-spin" aria-hidden="true" />
        : <FileCheck size={14} className="shrink-0" aria-hidden="true" />}
      <span>{label}</span>
      {running && asks && (
        <Button variant="link" size="xs" onClick={() => useChatStore.getState().answerInteraction(null)}>
          Continue
        </Button>
      )}
    </div>
  );
}

// ------------------------------------------------------------------ //
//  ExitPlanMode — the decision                                         //
// ------------------------------------------------------------------ //

function PlanDecisionBlock({ block }: { block: ToolCallBlockData }) {
  const live = usePlanDecisionPending();
  // Approval starts the work in the same turn: while that turn (the one
  // this call belongs to) still streams, an approval reads "applying".
  const inRunningTurn = useChatStore(s => s.isStreaming && s.streamingBlocks.some(
    b => b.type === 'tool_call' && b.toolUseId === block.toolUseId,
  ));
  // What this client decided. Shown at once, before the tool result lands.
  const [decided, setDecided] = useState<PlanOutcome | null>(null);
  // The decision was once offered here. When it goes away without a result
  // yet, someone decided (here or in another client): "Resolving…", not a
  // stale "waiting". Set during render, React's pattern for keeping
  // something from an earlier render.
  const [offered, setOffered] = useState(false);
  if (live && !offered) setOffered(true);

  if (decided) return <OutcomeLine outcome={decided} applying={inRunningTurn} />;
  if (block.status === 'complete') {
    return <OutcomeLine outcome={planOutcomeOf(block.result, block.isError)} applying={inRunningTurn} />;
  }
  if (!live) {
    return (
      <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
        <div className="px-3 py-2.5 flex items-center gap-2">
          <FileCheck size={14} className="text-text-muted motion-safe:animate-pulse" aria-hidden="true" />
          <span className="text-sm leading-tight text-text-muted">
            {offered ? 'Resolving…' : 'Waiting for the plan…'}
          </span>
        </div>
      </div>
    );
  }
  return (
    <DecisionCard
      block={block}
      onApprove={() => { if (approvePlan()) setDecided({ kind: 'approved' }); }}
      onApproveEdited={text => { if (approveEditedPlan(text)) setDecided({ kind: 'approved', edited: true }); }}
      onRequestChanges={feedback => {
        if (requestPlanChanges(feedback)) setDecided({ kind: 'changes', feedback: firstLine(feedback) });
      }}
    />
  );
}

function DecisionCard({ block, onApprove, onApproveEdited, onRequestChanges }: {
  block: ToolCallBlockData;
  onApprove: () => void;
  onApproveEdited: (plan: string) => void;
  onRequestChanges: (feedback: string) => void;
}) {
  const copy = usePlanDecisionCopy();
  // The plan the agent waits on, as the CLI read it from the plan file.
  const fromPause = useChatStore(s => planText(s.pendingInteraction?.toolInput?.plan));
  const fromPanel = useChatStore(s => planText(latestPlanTab(s.panels)?.content));
  const planTabId = useChatStore(s => latestPlanTab(s.panels)?.id ?? null);
  const focusPanelTab = useChatStore(s => s.focusPanelTab);
  const plan = fromPause || planText(block.input.plan) || fromPanel;

  return (
    <div className="my-2">
      <div className="border border-accent/20 rounded-lg bg-bg-sunken overflow-hidden">
        <div className="px-4 py-3">
          <div className="flex items-center gap-2 mb-1">
            <FileCheck size={15} className="text-accent shrink-0" aria-hidden="true" />
            <span className="text-sm leading-tight font-medium text-text">{copy.title}</span>
          </div>
          <p className="text-xs text-text-muted mb-3">{copy.body}</p>
          {!plan && (
            <p className="text-xs text-text-muted mb-3">
              {planTabId ? (
                <Button variant="link" size="xs" onClick={() => focusPanelTab(planTabId)}>
                  Open the side panel to read the plan
                </Button>
              ) : 'Open the side panel to read the plan.'}
            </p>
          )}
          <PlanDecisionControls
            layout="card"
            plan={plan}
            onApprove={onApprove}
            onRequestChanges={onRequestChanges}
            // Edits go back as the plan file's new text: only for the text
            // the CLI sent with this pause.
            onApproveEdited={fromPause ? onApproveEdited : undefined}
          />
        </div>
      </div>
    </div>
  );
}

/** What was decided, in one line; an approval whose turn still runs reads "applying". */
function OutcomeLine({ outcome, applying }: { outcome: PlanOutcome; applying: boolean }) {
  const copy = usePlanDecisionCopy();
  let icon = <Check size={14} className="text-success shrink-0" aria-hidden="true" />;
  let text = outcome.kind === 'approved' && outcome.edited ? PLAN_APPROVED_EDITED : PLAN_APPROVED;
  if (outcome.kind === 'approved' && applying) {
    icon = <Loader2 size={14} className="text-accent shrink-0 motion-safe:animate-spin" aria-hidden="true" />;
    text = outcome.edited ? copy.applyingEdited : copy.applying;
  } else if (outcome.kind === 'changes') {
    icon = <MessageSquare size={14} className="text-accent shrink-0" aria-hidden="true" />;
    text = outcome.feedback ? `Changes requested — ${outcome.feedback}` : 'Changes requested';
  } else if (outcome.kind === 'declined') {
    icon = <Ban size={14} className="text-text-muted shrink-0" aria-hidden="true" />;
    text = 'Declined';
  } else if (outcome.kind === 'closed') {
    icon = <XCircle size={14} className="text-text-muted shrink-0" aria-hidden="true" />;
    text = outcome.reason ? `Closed — ${outcome.reason}` : 'Closed';
  }
  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <div className="px-3 py-2.5 flex items-center gap-2 min-w-0">
        {icon}
        <span className="text-sm leading-tight font-medium text-text-secondary truncate" title={text}>
          {text}
        </span>
      </div>
    </div>
  );
}

/** The plan tab opened last (the one the plan updates go to). */
function latestPlanTab(panels: PanelTab[]): PanelTab | undefined {
  for (let i = panels.length - 1; i >= 0; i--) {
    if (panels[i].type === 'plan') return panels[i];
  }
  return undefined;
}
