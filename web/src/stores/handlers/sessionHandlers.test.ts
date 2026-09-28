// @vitest-environment jsdom
import { describe, expect, it, vi } from 'vitest';

import { handleSessionRunning, handleUserMessage } from './sessionHandlers';
import type { ChatState } from '../chatStore';
import type { Get, Set } from './types';
import type { Session } from '../../types/chat';
import type { WSMessage } from '../../api/websocket';

/** Minimal store double: only the slices handleSessionRunning touches. */
function fakeStore(sessions: Session[], activeSession = '') {
  let state = {
    sessions,
    searchResults: null,
    activeSession,
    isStreaming: false,
    streamingBlocks: [],
    messages: [],
    loadSessions: vi.fn(),
  } as unknown as ChatState;

  const get = (() => state) as Get;
  const set = ((partial) => {
    const patch = typeof partial === 'function' ? partial(state) : partial;
    state = { ...state, ...patch };
  }) as Set;

  return { get, set, current: () => state };
}

const row = (id: string, extra: Partial<Session> = {}): Session => ({
  id, title: id, source: 'web', updated_at: '2026-01-01T00:00:00Z',
  is_running: true, ...extra,
});

const running = (
  extra: Partial<Extract<WSMessage, { type: 'session_running' }>> = {},
): Extract<WSMessage, { type: 'session_running' }> => ({
  type: 'session_running', session_id: 's1', is_running: false, ...extra,
});

describe('handleSessionRunning — parked sessions', () => {
  it('keeps the pending wake-up on the row when the turn ends', () => {
    const store = fakeStore([row('s1'), row('s2')]);
    const fireAt = '2026-01-01T00:10:00Z';

    handleSessionRunning(running({ pending_wakeup_at: fireAt }), store.get, store.set);

    const [s1, s2] = store.current().sessions;
    // The turn is over, but the session is parked — not idle.
    expect(s1.is_running).toBe(false);
    expect(s1.pending_wakeup_at).toBe(fireAt);
    // Other rows are untouched.
    expect(s2.is_running).toBe(true);
    expect(s2.pending_wakeup_at).toBeUndefined();
  });

  it('keeps a live background job on the row', () => {
    const store = fakeStore([row('s1')]);

    handleSessionRunning(running({ has_background_tasks: true }), store.get, store.set);

    expect(store.current().sessions[0].has_background_tasks).toBe(true);
  });

  it('clears stale pending work when the session really is idle', () => {
    const store = fakeStore([
      row('s1', { pending_wakeup_at: '2026-01-01T00:10:00Z', has_background_tasks: true }),
    ]);

    // A transition with no pending-work fields means nothing is scheduled.
    handleSessionRunning(running(), store.get, store.set);

    const s1 = store.current().sessions[0];
    expect(s1.pending_wakeup_at).toBeNull();
    expect(s1.has_background_tasks).toBe(false);
  });

  it('applies the same bits to search results', () => {
    const store = fakeStore([row('s1')]);
    const state = store.current() as unknown as { searchResults: Session[] };
    state.searchResults = [row('s1'), row('other')];

    handleSessionRunning(running({ has_background_tasks: true }), store.get, store.set);

    const results = store.current().searchResults!;
    expect(results[0].has_background_tasks).toBe(true);
    expect(results[1].has_background_tasks).toBeUndefined();
  });

  it('still refetches the feed for a backgrounded session that stopped', () => {
    const store = fakeStore([row('s1')], 'other');

    handleSessionRunning(running(), store.get, store.set);

    expect(store.current().loadSessions).toHaveBeenCalled();
  });
});

/**
 * The live echo: another client of this session sent something, and the event
 * names who. Carrying the id through is what lets a second tab label the bubble
 * as it arrives instead of waiting for the next reload to read the stored row.
 */
describe('handleUserMessage — the sender on a live echo', () => {
  const echo = (
    extra: Partial<Extract<WSMessage, { type: 'user_message' }>> = {},
  ): Extract<WSMessage, { type: 'user_message' }> => ({
    type: 'user_message', session_id: 's1', content: 'hi', ...extra,
  });

  it('carries the actor onto the appended message', () => {
    const store = fakeStore([row('s1')], 's1');

    handleUserMessage(echo({ actor_id: 'actor-bob' }), store.get, store.set);

    const [msg] = store.current().messages;
    expect(msg.role).toBe('user');
    expect(msg.actor_id).toBe('actor-bob');
  });

  it('records no sender when the event names none', () => {
    const store = fakeStore([row('s1')], 's1');

    handleUserMessage(echo(), store.get, store.set);

    // Null, never a placeholder id or a name: an unattributed echo has to
    // render exactly like the unattributed history above it.
    expect(store.current().messages[0].actor_id).toBeNull();
  });

  it('ignores an echo for a session that is not open', () => {
    const store = fakeStore([row('s1')], 'other');

    handleUserMessage(echo({ actor_id: 'actor-bob' }), store.get, store.set);

    expect(store.current().messages).toHaveLength(0);
  });
});
