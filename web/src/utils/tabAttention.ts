import type { Notification } from '../stores/notificationStore';
import type { Session } from '../types/chat';
import { blockedOnYou } from './sessionAttention';

/**
 * Favicon dot colours. They are fixed because the tab strip of the browser
 * does not follow the panel theme. Both have a contrast of about 3:1 or more
 * to white and to the dark greys of tab strips (#202124 to #35363a).
 */
export const ANSWER_DOT = '#e5484d';
export const UNREAD_DOT = '#437eef';

/** A question or an approval that waits for an answer. */
function waitsForAnswer(notification: Notification): boolean {
  return notification.status === 'pending' && (notification.type === 'question' || notification.type === 'approval');
}

/**
 * What the tab title and the favicon show: the text before the page title
 * (`prefix`) and the colour of the favicon dot (`dot`, null for no dot).
 *
 * "! " and a red dot while an answer from the person waits: a session that
 * waits (`blockedOnYou`: a question in the turn, a pending question or
 * approval, a review loop on a decision), or a pending question or approval
 * in the notification list (also of an archived session, which the session
 * list leaves out). Otherwise "(N) " and a blue dot for N pending
 * notifications, the number that the Notifs badge shows. Otherwise nothing.
 */
export function tabAttention(sessions: Session[], notifications: Notification[], unread: number) {
  if (sessions.some(blockedOnYou) || notifications.some(waitsForAnswer)) return { prefix: '! ', dot: ANSWER_DOT };
  if (unread > 0) return { prefix: `(${unread}) `, dot: UNREAD_DOT };
  return { prefix: '', dot: null };
}
