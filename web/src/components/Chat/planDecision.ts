import { useChatStore } from '../../stores/chatStore';

/**
 * The plan decision: its words and its effect, in one place.
 *
 * `ExitPlanMode` pauses the agent's turn until the person decides. Two
 * surfaces offer the decision: the card in the chat (`PlanApprovalBlock`)
 * and the plan panel's footer (`SidePanel`'s `PlanActions`). Both take their
 * labels and their actions from here, and both render the same controls
 * (`PlanDecisionControls`), so they never disagree.
 *
 * - **Approve** answers the pause and the agent goes on, in the same turn.
 * - **Edit** first, then **Apply edited plan**: the answer carries the
 *   person's text as `plan`; the CLI saves it to the plan file and the agent
 *   works from it ("Approved Plan (edited by user)").
 * - **Request changes** denies it with the person's text. The text reaches
 *   the agent as the denial message; the agent stays in plan mode, revises
 *   the plan and calls `ExitPlanMode` again, which is a new call and a new
 *   card.
 *
 * The onboarding conversation names the same decision after what it is
 * about: the plan there is the agent's setup proposal.
 */

export interface PlanDecisionCopy {
  /** The card's heading. */
  title: string;
  /** One line under the heading. */
  body: string;
  /** What the plan is, as the name of the box that shows it. */
  planLabel: string;
  approve: string;
  /** Opens the feedback field. */
  requestChanges: string;
  /** The label of the text field that edits the plan. */
  editLabel: string;
  /** After Approve, while the agent still works in that turn. */
  applying: string;
  /** The same, after an approval with edits. */
  applyingEdited: string;
}

const GENERIC_COPY: PlanDecisionCopy = {
  title: 'Plan ready for your decision',
  body: 'Read it below or in the panel. Approve it, or say what should change.',
  planLabel: 'Plan',
  approve: 'Approve',
  requestChanges: 'Request changes…',
  editLabel: 'Edit the plan',
  applying: 'Approved — applying…',
  applyingEdited: 'Approved with your edits — applying…',
};

const ONBOARDING_COPY: PlanDecisionCopy = {
  title: 'Setup proposal',
  body: 'Read it below or in the panel. Apply it, or say what should change.',
  planLabel: 'Setup proposal',
  approve: 'Apply setup',
  requestChanges: 'Change something…',
  editLabel: 'Edit the setup proposal',
  applying: 'Applying the setup…',
  applyingEdited: 'Applying the setup…',
};

/** The feedback field's label. */
export const PLAN_FEEDBACK_LABEL = 'What should change?';
export const PLAN_FEEDBACK_SEND = 'Send';
/** Turns the plan into a text field. */
export const PLAN_EDIT = 'Edit';
export const PLAN_APPLY_EDITED = 'Apply edited plan';
export const PLAN_EDIT_CANCEL = 'Cancel';
export const PLAN_APPROVED = 'Approved';
export const PLAN_APPROVED_EDITED = 'Approved with your edits';

/**
 * What the panel's Approve sends as a chat message when no `ExitPlanMode`
 * waits for an answer (a plan tab of a finished turn, for one).
 */
export const PLAN_APPROVE_MESSAGE = 'Looks good — implement it.';

/** The onboarding conversation's session: `onboarding-<id>` (`nerve/onboarding.py`). */
export function isOnboardingSession(sessionId: string | null | undefined): boolean {
  return !!sessionId && sessionId.startsWith('onboarding-');
}

export function planDecisionCopy(onboarding: boolean): PlanDecisionCopy {
  return onboarding ? ONBOARDING_COPY : GENERIC_COPY;
}

/** The decision's words for the session on screen. */
export function usePlanDecisionCopy(): PlanDecisionCopy {
  return planDecisionCopy(useChatStore(s => isOnboardingSession(s.activeSession)));
}

/** Whether the agent waits for a decision on a plan right now. */
export function usePlanDecisionPending(): boolean {
  return useChatStore(s => s.pendingInteraction?.interactionType === 'plan_exit');
}

/** A plan text worth showing: a string with something in it. */
export function planText(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value : null;
}

/**
 * The text of the plan the agent waits on: the CLI puts the plan file's
 * content into `ExitPlanMode`'s input (`plan`), read again at each call.
 */
export function usePendingPlanText(): string | null {
  return useChatStore(s => (
    s.pendingInteraction?.interactionType === 'plan_exit' ? planText(s.pendingInteraction.toolInput?.plan) : null
  ));
}

/**
 * Approve the plan the agent waits on. False, and nothing sent, when no plan
 * decision is pending (answered meanwhile here or by another client).
 */
export function approvePlan(): boolean {
  const s = useChatStore.getState();
  if (s.pendingInteraction?.interactionType !== 'plan_exit') return false;
  s.answerInteraction(null);
  return true;
}

/**
 * Approve the plan as the person edited it. False, and nothing sent, for an
 * empty text or when no plan decision is pending.
 */
export function approveEditedPlan(text: string): boolean {
  const s = useChatStore.getState();
  if (!text.trim() || s.pendingInteraction?.interactionType !== 'plan_exit') return false;
  s.answerInteraction({ plan: text });
  return true;
}

/**
 * Turn the plan down with the person's text, which the agent receives as the
 * reason. False, and nothing sent, for an empty text or when no plan decision
 * is pending.
 */
export function requestPlanChanges(text: string): boolean {
  const feedback = text.trim();
  const s = useChatStore.getState();
  if (!feedback || s.pendingInteraction?.interactionType !== 'plan_exit') return false;
  s.denyInteraction(feedback);
  return true;
}

/** The first line with text in it, trimmed; `''` for a blank text. */
export function firstLine(text: string): string {
  for (const line of text.split('\n')) {
    const trimmed = line.trim();
    if (trimmed) return trimmed;
  }
  return '';
}

/** How a plan decision ended. */
export type PlanOutcome =
  /** `edited`: approved as the person edited it. */
  | { kind: 'approved'; edited?: boolean }
  | { kind: 'changes'; feedback: string }
  /** Turned down without a reason (the fixed text of an older version). */
  | { kind: 'declined' }
  /** Ended without a decision: timed out, the session stopped, or a channel without plan mode. */
  | { kind: 'closed'; reason: string };

/** The fixed denials of earlier versions, and the server's text for an empty one. */
const DECLINED = new Set(['User declined.', 'User declined the plan.', 'Declined by user.']);

/** The server's own denials (`nerve/agent/interactive.py`, `backends/claude.py`). */
const CLOSED = /timed out\.?$|^Session stopped|is not available in (this channel|non-web sessions)/;

/**
 * The heading the CLI puts above the plan in its result when the person
 * edited it (plain approvals say `## Approved Plan:`). Matched as a whole
 * line, so a plan that merely mentions the words does not count.
 */
const EDITED = /^## Approved Plan \(edited by user\):/m;

/**
 * The outcome of a finished `ExitPlanMode` call, read from its tool result:
 * the CLI returns the denial message as an error result, word for word, so
 * the feedback's first line is the result's first line. Lets a reloaded
 * session, and a client where someone else decided, show what was decided.
 */
export function planOutcomeOf(result: string | undefined, isError: boolean | undefined): PlanOutcome {
  const line = firstLine(result ?? '');
  if (!isError) {
    if (!line) return { kind: 'closed', reason: '' };
    return EDITED.test(result ?? '') ? { kind: 'approved', edited: true } : { kind: 'approved' };
  }
  if (DECLINED.has(line)) return { kind: 'declined' };
  if (CLOSED.test(line)) return { kind: 'closed', reason: line };
  return { kind: 'changes', feedback: line };
}
