// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { ChatState } from '../chatStore';
import type { Get, Set } from './types';
import type { PanelTab, Session } from '../../types/chat';
import type { WSMessage } from '../../api/websocket';

// The handlers reach the agent store, whose modules read localStorage at
// import: the in-memory one src/test/setup.ts installs.
vi.mock('../../api/client', () => ({ api: {}, apiErrorCode: () => null }));
vi.mock('../../api/websocket', () => ({ ws: { subscribeAgent: vi.fn(), unsubscribeAgent: vi.fn() } }));

const { handleAnswerInjected, handleSessionRunning, handleSessionStatus, handleUserMessage } = await import('./sessionHandlers');
const { useAgentStore } = await import('../agentStore');
const fx = await import('../../test/agentFixtures');

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

/**
 * An answer to a notification enters the chat as a user message written by
 * the person who answered. The live bubble carries that id, as the stored row
 * does, so it is labelled the same before and after a reload.
 */
describe('handleAnswerInjected: the person who answered', () => {
  const injected = (
    extra: Partial<Extract<WSMessage, { type: 'answer_injected' }>> = {},
  ): Extract<WSMessage, { type: 'answer_injected' }> => ({
    type: 'answer_injected', session_id: 's1', notification_id: 'ask-1',
    title: 'Pick one', answer: 'yes', answered_by: 'web',
    content: '[Answer to: Pick one]\n\nyes', ...extra,
  });

  it('carries the answerer onto the appended message', () => {
    const store = fakeStore([row('s1')], 's1');

    handleAnswerInjected(
      injected({ actor_id: 'actor-alice' }),
      store.get, store.set,
    );

    const [msg] = store.current().messages;
    expect(msg.role).toBe('user');
    expect(msg.actor_id).toBe('actor-alice');
  });

  it('records no sender for a chat-channel answer', () => {
    const store = fakeStore([row('s1')], 's1');

    handleAnswerInjected(injected({ answered_by: 'telegram' }), store.get, store.set);

    expect(store.current().messages[0].actor_id).toBeNull();
  });
});

describe('handleSessionStatus — the agent roster', () => {
  beforeEach(() => {
    useAgentStore.getState().reset(fx.FX_SESSION_ID);
  });

  const status = (extra: Partial<Extract<WSMessage, { type: 'session_status' }>> = {}): Extract<WSMessage, { type: 'session_status' }> => ({
    type: 'session_status', session_id: fx.FX_SESSION_ID, is_running: false, ...extra,
  });

  it('hydrates the agents of an idle session', () => {
    const store = fakeStore([row(fx.FX_SESSION_ID)], fx.FX_SESSION_ID);

    handleSessionStatus(status({ agents: fx.makeSnapshot(fx.FX_WAVE) }), store.get, store.set);

    expect(Object.keys(useAgentStore.getState().agents)).toHaveLength(fx.FX_WAVE.length);
    expect(store.current().isStreaming).toBe(false);
  });

  it('keeps the Agents panel and agent tabs when a running turn replays', () => {
    const tab = (id: string, type: PanelTab['type']): PanelTab => ({
      id, type, label: id, subagentType: id, description: '', content: null, prompt: '',
      streaming: false, status: 'running', startedAt: 1, blocks: [],
    });
    const store = fakeStore([row(fx.FX_SESSION_ID)], fx.FX_SESSION_ID);
    store.set({
      panels: [tab('agents-panel', 'agents'), tab(`agent:${fx.FX_RUNNING.id}`, 'agent'), tab('toolu_old', 'subagent')],
      activePanelId: `agent:${fx.FX_RUNNING.id}`,
      panelVisible: true,
    });

    handleSessionStatus(status({
      is_running: true,
      agents: fx.makeSnapshot([fx.FX_RUNNING]),
      buffered_events: [
        { type: 'tool_use', session_id: fx.FX_SESSION_ID, tool: 'Workflow', input: { name: 'wave' }, tool_use_id: 'toolu_wf' },
        { type: 'tool_use', session_id: fx.FX_SESSION_ID, tool: 'Agent', input: { description: 'x' }, tool_use_id: 'toolu_ag' },
      ],
    }), store.get, store.set);

    const s = store.current();
    expect(s.panels.map(p => p.id)).toEqual(['agents-panel', `agent:${fx.FX_RUNNING.id}`, 'toolu_wf']);
    expect(s.activePanelId).toBe(`agent:${fx.FX_RUNNING.id}`);
    expect(s.panelVisible).toBe(true);
    expect(s.streamingBlocks.map(b => b.type === 'tool_call' && b.tool)).toEqual(['Workflow', 'Agent']);
    expect(useAgentStore.getState().agents[fx.FX_RUNNING.id]).toBeDefined();
  });

  it('sets the row count from the snapshot (a stale count after a restart goes)', () => {
    const store = fakeStore([
      row(fx.FX_SESSION_ID, { background_count: 6, has_background_tasks: true }),
      row('other', { background_count: 2, has_background_tasks: true }),
    ], fx.FX_SESSION_ID);

    handleSessionStatus(status({ agents: fx.makeSnapshot([fx.FX_INTERRUPTED_RESTART]) }), store.get, store.set);

    const [mine, other] = store.current().sessions;
    expect([mine.background_count, mine.has_background_tasks]).toEqual([0, false]);
    expect([other.background_count, other.has_background_tasks]).toEqual([2, true]);
  });

  it('restores which agent asked a pending question', () => {
    const store = fakeStore([row(fx.FX_SESSION_ID)], fx.FX_SESSION_ID);
    store.set({ panels: [], activePanelId: null, panelVisible: false });
    const agent = { agent_id: fx.FX_RUNNING.id, task_id: 'a1b2c3d4e5f6a7b8c', label: 'build-api' };

    handleSessionStatus(status({
      is_running: true,
      buffered_events: [{
        type: 'interaction', session_id: fx.FX_SESSION_ID, interaction_id: 'i-1', interaction_type: 'question',
        tool_name: 'AskUserQuestion', tool_input: {}, agent,
      }],
    }), store.get, store.set);

    expect(store.current().pendingInteraction?.agent).toEqual(agent);
  });
});

describe('handleSessionRunning — the agent count', () => {
  it('copies background_count onto the row', () => {
    const store = fakeStore([row('s1'), row('s2', { background_count: 4 })]);

    handleSessionRunning(running({ has_background_tasks: true, background_count: 5 }), store.get, store.set);

    expect(store.current().sessions.map(s => s.background_count)).toEqual([5, 4]);
  });

  it('clears the count when an older server reports no background work', () => {
    const store = fakeStore([row('s1', { background_count: 3, has_background_tasks: true })]);

    handleSessionRunning(running(), store.get, store.set);
    expect(store.current().sessions[0].background_count).toBe(0);
  });

  it('enters streaming for the active session\'s own turn', () => {
    const store = fakeStore([row('s1')], 's1');
    handleSessionRunning(running({ is_running: true, background_count: 2 }), store.get, store.set);
    expect(store.current().isStreaming).toBe(true);
  });
});

describe('handleUserMessage — queued echoes', () => {
  it('carries the sending tab\'s client_msg_id', () => {
    const store = fakeStore([row('s1')], 's1');

    handleUserMessage(
      { type: 'user_message', session_id: 's1', content: 'also run the e2e suite', client_msg_id: 'c-1' },
      store.get, store.set,
    );

    expect(store.current().messages[0].client_msg_id).toBe('c-1');
  });

  it('shows a resent message once', () => {
    const store = fakeStore([row('s1')], 's1');
    const echo = { type: 'user_message' as const, session_id: 's1', content: 'also run the e2e suite', client_msg_id: 'c-1' };

    handleUserMessage(echo, store.get, store.set);
    handleUserMessage(echo, store.get, store.set);
    // Echoes without an id are never merged.
    handleUserMessage({ ...echo, client_msg_id: null }, store.get, store.set);
    handleUserMessage({ ...echo, client_msg_id: null }, store.get, store.set);

    expect(store.current().messages.map(m => m.client_msg_id)).toEqual(['c-1', null, null]);
  });
});
