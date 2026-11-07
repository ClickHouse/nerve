import type { Session } from '../types/chat';

/** Text colour of everything that says a session needs the user. */
export const ATTENTION_TONE = 'text-warning';

/** "Question", "2 questions": a count with its noun. */
function counted(n: number, one: string, many: string): string {
  return n === 1 ? one : `${n} ${many}`;
}

/**
 * Why a session needs the user, most urgent first; empty when it needs
 * nothing. The in-turn question and the review loop come from the live
 * session fields, which WebSocket events keep current between fetches.
 */
export function attentionReasons(session: Session): string[] {
  const a = session.attention;
  const reasons: string[] = [];
  if (session.awaiting_input) reasons.push('Waiting for input');
  if (a?.questions) reasons.push(counted(a.questions, 'Question', 'questions'));
  if (a?.approvals) reasons.push(counted(a.approvals, 'Approval', 'approvals'));
  if (a?.plans) reasons.push(counted(a.plans, 'Plan to review', 'plans to review'));
  if (session.review_loop?.status === 'awaiting_user') reasons.push('Review loop');
  return reasons;
}

/** True when something in the session waits for the user. */
export function needsYou(session: Session): boolean {
  return attentionReasons(session).length > 0;
}

/** "2 need you", "1 needs you". */
export function needsYouLabel(count: number): string {
  return `${count} need${count === 1 ? 's' : ''} you`;
}
