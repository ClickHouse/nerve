import { describe, expect, it } from 'vitest';
import type { Session, SessionAttention } from '../types/chat';
import { ANSWER_DOT, UNREAD_DOT, tabAttention } from './tabAttention';

const NONE: SessionAttention = { questions: 0, approvals: 0, plans: 0, awaiting_input: false, review: false };
const ANSWER = { prefix: '! ', dot: ANSWER_DOT };

function session(attention: Partial<SessionAttention> = {}, extra: Partial<Session> = {}): Session {
  return {
    id: 's1', title: 'Deploy', source: 'web', updated_at: '2026-11-07T10:00:00Z',
    attention: { ...NONE, ...attention }, ...extra,
  };
}

describe('tabAttention', () => {
  it('shows nothing when nothing waits', () => {
    expect(tabAttention([], 0, 0)).toEqual({ prefix: '', dot: null });
    expect(tabAttention([session()], 0, 0)).toEqual({ prefix: '', dot: null });
  });

  it('shows the unread count with a blue dot', () => {
    expect(tabAttention([session()], 0, 3)).toEqual({ prefix: '(3) ', dot: UNREAD_DOT });
  });

  it('shows "!" with a red dot while a session waits, also with unread items', () => {
    expect(tabAttention([session(), session({ questions: 1 })], 0, 4)).toEqual(ANSWER);
    expect(tabAttention([session({ approvals: 2 })], 0, 0)).toEqual(ANSWER);
    expect(tabAttention([session({}, { awaiting_input: true })], 0, 0)).toEqual(ANSWER);
    expect(tabAttention([session({}, { review_loop: { id: 'rvl-1', status: 'awaiting_user', iteration: 1, max_iterations: 3 } })], 0, 0))
      .toEqual(ANSWER);
  });

  it('shows "!" for a pending question or approval that no listed session has', () => {
    // For example of a cron session or of an archived session.
    expect(tabAttention([], 1, 1)).toEqual(ANSWER);
    expect(tabAttention([session()], 2, 3)).toEqual(ANSWER);
  });

  it('does not show "!" for a plan to review alone', () => {
    expect(tabAttention([session({ plans: 1 })], 0, 1)).toEqual({ prefix: '(1) ', dot: UNREAD_DOT });
  });
});
