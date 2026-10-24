import { useEffect, useId, useState } from 'react';
import { Ban, BookOpen, Check, FileCheck, Loader2, MessageSquare, XCircle } from '../../ui/icons';
import type { ToolCallBlockData } from '../../../types/chat';
import { useChatStore } from '../../../stores/chatStore';
import { Button } from '../../ui';
import { InlineMarkdown } from '../MarkdownContent';
import { PlanDecisionControls } from '../PlanDecisionControls';
import {
  approvedPlanText, approvePlan, editLivePlan, firstLine, latestPlanTab, openDecidedPlan, openLivePlan,
  PLAN_APPROVED, PLAN_APPROVED_EDITED, PLAN_READ, PLAN_SUMMARIZING, PLAN_SUMMARY_LABEL, planOutcomeOf,
  planSummaryFallback, planText, requestPlanChanges, SUMMARY_WAIT_MS, usePlanDecisionCopy, usePlanDecisionPending,
  type PlanOutcome,
} from '../planDecision';

/**
 * Plan mode in the chat.
 *
 * `EnterPlanMode` is a status line: entering plan mode only restricts the
 * agent, so the server lets it through without asking (web sessions).
 * `ExitPlanMode` is the decision: the agent waits until the person approves
 * the plan (as it is, or edited in the panel) or says what should change.
 * The card is compact: a short summary the server writes, and the buttons;
 * the full plan opens in the side panel on request (Read the plan, Edit).
 * A revised plan is a new `ExitPlanMode` call, so it gets a card of its own
 * and the earlier one keeps its outcome.
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
  // Seen running here: a pause for it would have been offered here too
  // (the pause always comes before the result). Unknown for history.
  const [seenRunning] = useState(block.status === 'running');

  if (decided || block.status === 'complete') {
    const outcome = decided ?? planOutcomeOf(block.result, block.isError, seenRunning ? offered : undefined);
    return <DecidedPlan block={block} outcome={outcome} applying={inRunningTurn} />;
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
      // Approve closes the plan tab too (in `approvePlan`), as the footer does.
      onApprove={() => { if (approvePlan()) setDecided({ kind: 'approved' }); }}
      onRequestChanges={feedback => {
        if (requestPlanChanges(feedback)) setDecided({ kind: 'changes', feedback: firstLine(feedback) });
      }}
    />
  );
}

function DecisionCard({ block, onApprove, onRequestChanges }: {
  block: ToolCallBlockData;
  onApprove: () => void;
  onRequestChanges: (feedback: string) => void;
}) {
  const copy = usePlanDecisionCopy();
  const interactionId = useChatStore(s => s.pendingInteraction?.interactionId ?? null);
  // The plan the agent waits on, as the CLI read it from the plan file.
  const fromPause = useChatStore(s => planText(s.pendingInteraction?.toolInput?.plan));
  const fromPanel = useChatStore(s => planText(latestPlanTab(s.panels)?.content));
  const plan = fromPause || planText(block.input.plan) || fromPanel;

  return (
    <div className="my-2">
      <div className="border border-accent/20 rounded-lg bg-bg-sunken overflow-hidden">
        <div className="px-4 py-3 space-y-3">
          <div className="flex items-center gap-2">
            <FileCheck size={15} className="text-accent shrink-0" aria-hidden="true" />
            <span className="text-sm leading-tight font-medium text-text">{copy.title}</span>
          </div>
          {/* Keyed by the pause: a new pause starts its own wait for a summary. */}
          <PlanSummaryBlock key={interactionId ?? 'none'} interactionId={interactionId} plan={plan} />
          <PlanDecisionControls
            layout="card"
            onApprove={onApprove}
            onRequestChanges={onRequestChanges}
            onReadPlan={plan ? () => openLivePlan(plan, block.toolUseId) : undefined}
            // Edits go back as the plan file's new text: only for the text
            // the CLI sent with this pause.
            onEdit={fromPause ? () => editLivePlan(fromPause, block.toolUseId) : undefined}
          />
        </div>
      </div>
    </div>
  );
}

/**
 * The plan in a few lines: the server's summary (`plan_summary`); "Summarizing…"
 * while it is being written, for at most `SUMMARY_WAIT_MS`; then, or when the
 * server has none, the plan's first heading and two lines.
 */
function PlanSummaryBlock({ interactionId, plan }: { interactionId: string | null; plan: string | null }) {
  const summary = useChatStore(s => (interactionId ? s.planSummaries[interactionId] : undefined));
  const [waitedOut, setWaitedOut] = useState(false);
  const labelId = useId();
  useEffect(() => {
    if (summary) return;
    const timer = setTimeout(() => setWaitedOut(true), SUMMARY_WAIT_MS);
    return () => clearTimeout(timer);
  }, [summary]);

  const bullets = summary?.status === 'ok' ? summary.bullets.filter(b => b.trim()) : [];
  const pending = !summary && !waitedOut;
  const fallback = !pending && bullets.length === 0 ? planSummaryFallback(plan) : null;

  return (
    <div role="group" aria-labelledby={labelId} className="rounded-md border border-border-subtle bg-bg px-3 py-2">
      <div id={labelId} className="text-2xs font-semibold uppercase tracking-wider text-text-faint mb-1">
        {PLAN_SUMMARY_LABEL}
      </div>
      {pending ? (
        <div role="status" className="space-y-1.5 py-0.5">
          <div className="h-2.5 w-11/12 rounded bg-surface-raised motion-safe:animate-pulse" aria-hidden="true" />
          <div className="h-2.5 w-3/4 rounded bg-surface-raised motion-safe:animate-pulse" aria-hidden="true" />
          <div className="text-xs text-text-muted">{PLAN_SUMMARIZING}</div>
        </div>
      ) : bullets.length > 0 ? (
        <ul className="list-disc pl-4 space-y-1 text-sm text-text-secondary leading-snug">
          {bullets.map((bullet, i) => <li key={i}><InlineMarkdown content={bullet} /></li>)}
        </ul>
      ) : fallback && fallback.heading ? (
        <div className="text-sm text-text-secondary leading-snug space-y-0.5">
          <p className="font-medium text-text">{fallback.heading}</p>
          {fallback.lines.map((line, i) => <p key={i}>{line}</p>)}
        </div>
      ) : (
        <p className="text-sm text-text-muted">Read the plan for the details.</p>
      )}
    </div>
  );
}

/** A decided card: what was decided, and the plan it was about, to read. */
function DecidedPlan({ block, outcome, applying }: {
  block: ToolCallBlockData;
  outcome: PlanOutcome;
  applying: boolean;
}) {
  // An approval's own text is in its result (with the person's edits, if
  // any); otherwise the call's input is the plan that was decided on.
  const plan = (outcome.kind === 'approved' ? approvedPlanText(block.result) : null) ?? planText(block.input.plan);
  const label = outcomeLabel(outcome);
  return (
    <OutcomeLine
      outcome={outcome}
      applying={applying}
      onReadPlan={plan ? () => openDecidedPlan(plan, block.toolUseId, label) : undefined}
    />
  );
}

function outcomeLabel(outcome: PlanOutcome): string {
  switch (outcome.kind) {
    case 'approved': return outcome.edited ? PLAN_APPROVED_EDITED : PLAN_APPROVED;
    case 'changes': return 'Changes requested';
    case 'declined': return 'Declined';
    case 'closed': return 'Closed';
  }
}

/** What was decided, in one line; an approval whose turn still runs reads "applying". */
function OutcomeLine({ outcome, applying, onReadPlan }: {
  outcome: PlanOutcome;
  applying: boolean;
  onReadPlan?: () => void;
}) {
  const copy = usePlanDecisionCopy();
  let icon = <Check size={14} className="text-success shrink-0" aria-hidden="true" />;
  let text = outcomeLabel(outcome);
  if (outcome.kind === 'approved' && applying) {
    icon = <Loader2 size={14} className="text-accent shrink-0 motion-safe:animate-spin" aria-hidden="true" />;
    text = outcome.edited ? copy.applyingEdited : copy.applying;
  } else if (outcome.kind === 'changes') {
    icon = <MessageSquare size={14} className="text-accent shrink-0" aria-hidden="true" />;
    if (outcome.feedback) text = `Changes requested — ${outcome.feedback}`;
  } else if (outcome.kind === 'declined') {
    icon = <Ban size={14} className="text-text-muted shrink-0" aria-hidden="true" />;
  } else if (outcome.kind === 'closed') {
    icon = <XCircle size={14} className="text-text-muted shrink-0" aria-hidden="true" />;
    if (outcome.reason) text = `Closed — ${outcome.reason}`;
  }
  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <div className="px-3 py-2.5 flex items-center gap-2 min-w-0">
        {icon}
        <span className="text-sm leading-tight font-medium text-text-secondary truncate" title={text}>
          {text}
        </span>
        {onReadPlan && (
          <Button variant="link" size="xs" className="ml-auto shrink-0" onClick={onReadPlan}>
            <BookOpen size={12} aria-hidden="true" />
            {PLAN_READ}
          </Button>
        )}
      </div>
    </div>
  );
}
