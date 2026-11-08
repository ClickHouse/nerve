import type { Session } from '../types/chat';
import { blockedOnYou } from './sessionAttention';

/**
 * Favicon dot colours. They are fixed because the tab strip of the browser
 * does not follow the panel theme. Both have a contrast of about 3:1 or more
 * to white and to the dark greys of tab strips (#202124 to #35363a).
 */
export const ANSWER_DOT = '#e5484d';
export const UNREAD_DOT = '#437eef';

/**
 * What the tab title and the favicon show: the text before the page title
 * (`prefix`) and the colour of the favicon dot (`dot`, null for no dot).
 *
 * "! " and a red dot while an answer from the person waits: a session that
 * waits (`blockedOnYou`: a question in the turn, a pending question or
 * approval, a review loop on a decision), or one of the `answers` pending
 * questions and approvals that the server counts (also of a cron, hook or
 * archived session, which the session list leaves out). Otherwise "(N) "
 * and a blue dot for N pending notifications, the number that the Notifs
 * badge shows. Otherwise nothing.
 */
export function tabAttention(sessions: Session[], answers: number, unread: number) {
  if (answers > 0 || sessions.some(blockedOnYou)) return { prefix: '! ', dot: ANSWER_DOT };
  if (unread > 0) return { prefix: `(${unread}) `, dot: UNREAD_DOT };
  return { prefix: '', dot: null };
}
