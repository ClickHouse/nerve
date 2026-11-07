import type { Session } from '../types/chat';
import { blockedOnYou } from './sessionAttention';

/**
 * Favicon dot colours. They are fixed because the tab strip of the browser
 * does not follow the panel theme. Both have a contrast of about 3:1 or more
 * to white and to the dark greys of tab strips (#202124 to #35363a).
 */
export const ANSWER_DOT = '#e5484d';
export const UNREAD_DOT = '#437eef';

/** What the tab title and the favicon show. */
export interface TabAttention {
  /** Text before the page title: "! ", "(N) " or nothing. */
  prefix: string;
  /** Colour of the favicon dot, or null for no dot. */
  dot: string | null;
}

/**
 * "! " and a red dot while a session waits for an answer from the person:
 * a question in the turn, a pending question or approval, or a review loop
 * on a decision (`blockedOnYou`). Otherwise "(N) " and a blue dot for N
 * pending notifications, the number that the Notifs badge shows. Otherwise
 * nothing.
 */
export function tabAttention(sessions: Session[], unread: number): TabAttention {
  if (sessions.some(blockedOnYou)) return { prefix: '! ', dot: ANSWER_DOT };
  if (unread > 0) return { prefix: `(${unread}) `, dot: UNREAD_DOT };
  return { prefix: '', dot: null };
}
