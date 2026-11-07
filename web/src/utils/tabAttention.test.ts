import { describe, expect, it } from 'vitest';
import type { Session, SessionAttention } from '../types/chat';
import { ANSWER_DOT, UNREAD_DOT, tabAttention } from './tabAttention';

const NONE: SessionAttention = { questions: 0, approvals: 0, plans: 0, awaiting_input: false, review: false };

function session(attention: Partial<SessionAttention> = {}, extra: Partial<Session> = {}): Session {
  return {
    id: 's1', title: 'Deploy', source: 'web', updated_at: '2026-11-07T10:00:00Z',
    attention: { ...NONE, ...attention }, ...extra,
  };
}

describe('tabAttention', () => {
  it('shows nothing when nothing waits', () => {
    expect(tabAttention([], 0)).toEqual({ prefix: '', dot: null });
    expect(tabAttention([session()], 0)).toEqual({ prefix: '', dot: null });
  });

  it('shows the unread count with a blue dot', () => {
    expect(tabAttention([session()], 3)).toEqual({ prefix: '(3) ', dot: UNREAD_DOT });
  });

  it('shows "!" with a red dot while an answer waits, also with unread items', () => {
    const answer = { prefix: '! ', dot: ANSWER_DOT };
    expect(tabAttention([session(), session({ questions: 1 })], 4)).toEqual(answer);
    expect(tabAttention([session({ approvals: 2 })], 0)).toEqual(answer);
    expect(tabAttention([session({}, { awaiting_input: true })], 0)).toEqual(answer);
    expect(tabAttention([session({}, { review_loop: { id: 'rvl-1', status: 'awaiting_user', iteration: 1, max_iterations: 3 } })], 0))
      .toEqual(answer);
  });

  it('does not show "!" for a plan to review alone', () => {
    expect(tabAttention([session({ plans: 1 })], 1)).toEqual({ prefix: '(1) ', dot: UNREAD_DOT });
  });
});
