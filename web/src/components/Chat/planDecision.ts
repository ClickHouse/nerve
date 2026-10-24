import { useChatStore } from '../../stores/chatStore';
import type { PanelTab } from '../../types/chat';

/**
 * The plan decision: its words and its effect, in one place.
 *
 * `ExitPlanMode` pauses the agent's turn until the person decides. Two
 * surfaces offer the decision: the card in the chat (`PlanApprovalBlock`),
 * compact — a short summary the server writes (`plan_summary`) and the
 * buttons — and the plan panel's footer (`SidePanel`'s `PlanActions`), under
 * the full plan, where it is read and edited. The panel opens on request
 * only (Read the plan, Edit). Both surfaces take their labels and actions
 * from here and render the same controls (`PlanDecisionControls`), so they
 * never disagree.
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
  approve: 'Approve',
  requestChanges: 'Request changes…',
  editLabel: 'Edit the plan',
  applying: 'Approved — applying…',
  applyingEdited: 'Approved with your edits — applying…',
};

const ONBOARDING_COPY: PlanDecisionCopy = {
  title: 'Setup proposal',
  approve: 'Apply setup',
  requestChanges: 'Change something…',
  editLabel: 'Edit the setup proposal',
  applying: 'Applying the setup…',
  applyingEdited: 'Applying the setup…',
};

/** The feedback field's label. */
export const PLAN_FEEDBACK_LABEL = 'What should change?';
export const PLAN_FEEDBACK_SEND = 'Send';
/** Opens the plan in the side panel. */
export const PLAN_READ = 'Read the plan';
/** Turns the plan into a text field (in the panel). */
export const PLAN_EDIT = 'Edit';
export const PLAN_APPLY_EDITED = 'Apply edited plan';
export const PLAN_EDIT_CANCEL = 'Cancel';
export const PLAN_APPROVED = 'Approved';
export const PLAN_APPROVED_EDITED = 'Approved with your edits';
/** The card's summary block. */
export const PLAN_SUMMARY_LABEL = 'Summary';
export const PLAN_SUMMARIZING = 'Summarizing…';

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
 * The live plan tab opened last: the one plan updates go to and the decision
 * is about. A plan opened to read from a decided card (`readOnly`) is not it.
 */
export function latestPlanTab(panels: PanelTab[]): PanelTab | undefined {
  for (let i = panels.length - 1; i >= 0; i--) {
    if (panels[i].type === 'plan' && !panels[i].readOnly) return panels[i];
  }
  return undefined;
}

function planTab(id: string, content: string, extra: Partial<PanelTab> = {}): PanelTab {
  return {
    id, type: 'plan', label: 'Plan', subagentType: 'Plan', description: '', content,
    prompt: '', streaming: false, status: 'complete', startedAt: Date.now(), completedAt: Date.now(), blocks: [],
    ...extra,
  };
}

/**
 * Show the plan the agent waits on in the side panel: focus its tab (filling
 * in the text if the tab has none yet), or open one from the pause's text.
 * The panel opens only on request — this is the request.
 */
export function openLivePlan(plan: string | null, key: string): void {
  const s = useChatStore.getState();
  const tab = latestPlanTab(s.panels);
  if (tab) {
    if (!planText(tab.content) && plan) s.updatePanelTab(tab.id, { content: plan });
    s.focusPanelTab(tab.id);
  } else if (plan) {
    s.openPanelTab(planTab(`plan-${key}`, plan));
  }
}

/**
 * Show the plan of a decided card in the side panel, to read: its own tab
 * (`readOnly`, so no decision footer and no plan updates), focused again on
 * a second click rather than opened twice.
 */
export function openDecidedPlan(plan: string, key: string, description: string): void {
  const s = useChatStore.getState();
  const id = `plan-read-${key}`;
  if (s.panels.some(p => p.id === id)) s.focusPanelTab(id);
  else s.openPanelTab(planTab(id, plan, { description, readOnly: true }));
}

/** The person's Edit on the card: the plan opens in the panel, its footer in edit mode. */
export function editLivePlan(plan: string, key: string): void {
  const s = useChatStore.getState();
  const id = s.pendingInteraction?.interactionType === 'plan_exit' ? s.pendingInteraction.interactionId : null;
  if (!id) return;
  openLivePlan(plan, key);
  s.requestPlanEdit(id);
}

/**
 * Close the tab of a plan that was just approved: the given tab (the
 * footer's own), else the latest plan tab. Whichever surface approved, the
 * plan leaves the panel, so no stale tab offers the decision again.
 */
function closeApprovedPlanTab(tabId?: string): void {
  const s = useChatStore.getState();
  const id = tabId ?? latestPlanTab(s.panels)?.id;
  if (id && s.panels.some(p => p.id === id)) s.closePanelTab(id);
}

/**
 * Approve the plan the agent waits on, and close its tab. False, and nothing
 * sent, when no plan decision is pending (answered meanwhile here or by
 * another client).
 */
export function approvePlan(tabId?: string): boolean {
  const s = useChatStore.getState();
  if (s.pendingInteraction?.interactionType !== 'plan_exit') return false;
  s.answerInteraction(null);
  closeApprovedPlanTab(tabId);
  return true;
}

/**
 * Approve the plan as the person edited it, and close its tab. False, and
 * nothing sent, for an empty text or when no plan decision is pending.
 */
export function approveEditedPlan(text: string, tabId?: string): boolean {
  const s = useChatStore.getState();
  if (!text.trim() || s.pendingInteraction?.interactionType !== 'plan_exit') return false;
  s.answerInteraction({ plan: text });
  closeApprovedPlanTab(tabId);
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

/**
 * How long the card waits for the server's summary before it shows the
 * fallback. The server gives up after 20 s and says so (`unavailable`); this
 * covers a summary that never comes at all.
 */
export const SUMMARY_WAIT_MS = 25_000;

/** A markdown line as plain text: no heading marks, list markers, backticks or asterisks. */
function plainLine(line: string): string {
  return line.trim()
    .replace(/^#{1,6}\s+/, '')
    .replace(/^([-*+]|\d+[.)])\s+/, '')
    .replace(/[`*]/g, '')
    .trim();
}

/**
 * The summary to show when the server has none: the plan's first heading and
 * the first two lines of text after it, as plain text (the first line stands
 * in for a heading when there is none). Empty for an empty plan.
 */
export function planSummaryFallback(plan: string | null): { heading: string; lines: string[] } {
  const all = (plan ?? '').split('\n').map(l => l.trim()).filter(Boolean);
  const at = all.findIndex(l => /^#{1,6}\s/.test(l));
  const start = at === -1 ? 0 : at;
  const heading = all.length ? plainLine(all[start]) : '';
  const lines = all.slice(start + 1).filter(l => !/^#{1,6}\s/.test(l)).slice(0, 2).map(plainLine).filter(Boolean);
  return { heading, lines };
}

/**
 * The plan as approved, from the tool result: the CLI repeats it after an
 * `## Approved Plan:` heading (`## Approved Plan (edited by user):` with the
 * person's edits, which the call's own input never saw). Null without one.
 */
export function approvedPlanText(result: string | undefined): string | null {
  const match = /^## Approved Plan(?: \(edited by user\))?:[^\S\n]*$/m.exec(result ?? '');
  if (!match) return null;
  return planText((result ?? '').slice(match.index + match[0].length).trim());
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
  /**
   * Ended without a person's decision: timed out, the session stopped, a
   * channel without plan mode, or an error before anyone was asked.
   */
  | { kind: 'closed'; reason: string };

/** The fixed denials of earlier versions, and the server's text for an empty one. */
const DECLINED = new Set(['User declined.', 'User declined the plan.', 'Declined by user.']);

/**
 * The server's own denials, word for word (`nerve/agent/interactive.py`,
 * `backends/claude.py`), so feedback that merely ends the same way still
 * reads as feedback.
 */
const SERVER_DENIALS = [
  /^No response received after .+ — timed out\.$/,
  /^Session stopped( by user)?\.$/,
  /^Plan mode is not available in non-web sessions\./,
  /^\S+ is not available in this channel\.$/,
];

/**
 * Errors of the CLI itself, which refuses the call before anyone is asked:
 * wrapped in `<tool_use_error>`, or calling `ExitPlanMode` outside plan mode.
 */
const CLI_ERROR = /^<tool_use_error>|^You are not in plan mode\b/;
const ERROR_TAGS = /<\/?tool_use_error>/g;

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
 *
 * `asked`: whether the person was asked, when that is known. `false` means
 * this client saw the call run and no decision was ever offered for it, so an
 * error is the runtime's, not anyone's feedback. Unknown (history) falls back
 * to the result text alone.
 */
export function planOutcomeOf(
  result: string | undefined, isError: boolean | undefined, asked?: boolean,
): PlanOutcome {
  const line = firstLine(result ?? '');
  if (!isError) {
    if (!line) return { kind: 'closed', reason: '' };
    return EDITED.test(result ?? '') ? { kind: 'approved', edited: true } : { kind: 'approved' };
  }
  if (DECLINED.has(line)) return { kind: 'declined' };
  if (asked === false || CLI_ERROR.test(line) || SERVER_DENIALS.some(re => re.test(line))) {
    return { kind: 'closed', reason: line.replace(ERROR_TAGS, '').trim() };
  }
  return { kind: 'changes', feedback: line };
}
