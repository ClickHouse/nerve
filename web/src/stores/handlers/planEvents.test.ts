import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { WSMessage } from '../../api/websocket';
import type { PanelTab, Session } from '../../types/chat';

vi.mock('../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));
vi.mock('../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
    answerInteraction: vi.fn(),
  },
}));

const { useChatStore } = await import('../chatStore');

const PLAN = '# Setup proposal — review pull requests in org/repo-a\n\n- Scan every 30 minutes.';

const session = (id: string, backend: string): Session => ({
  id, title: id, source: 'web', updated_at: '2026-01-01T00:00:00Z', backend,
});

function planTab(extra: Partial<PanelTab> = {}): PanelTab {
  return {
    id: 'plan-1', type: 'plan', label: 'Plan', subagentType: 'Plan', description: '', content: 'old',
    prompt: '', streaming: false, status: 'complete', startedAt: 0, completedAt: 1, blocks: [], ...extra,
  };
}

const send = (msg: WSMessage) => useChatStore.getState().handleWSMessage(msg);

const planExit: WSMessage = {
  type: 'interaction', session_id: 's-claude', interaction_id: 'i-plan', interaction_type: 'plan_exit',
  tool_name: 'ExitPlanMode', tool_input: { plan: PLAN },
};

beforeEach(() => {
  useChatStore.setState({
    activeSession: 's-claude',
    sessions: [session('s-claude', 'claude'), session('s-codex', 'codex')],
    pendingInteraction: null, panels: [], activePanelId: null, panelVisible: false,
    planSummaries: {}, planEditRequest: null, isStreaming: false, streamingBlocks: [],
  });
});

describe('an ExitPlanMode pause', () => {
  it('does not open the plan panel, even with a plan tab there', () => {
    useChatStore.setState({ panels: [planTab()], activePanelId: null, panelVisible: false });
    send(planExit);
    const s = useChatStore.getState();
    expect(s.pendingInteraction).toMatchObject({ interactionId: 'i-plan', interactionType: 'plan_exit' });
    expect(s.panelVisible).toBe(false);
    expect(s.activePanelId).toBeNull();
  });
});

describe('plan_summary', () => {
  it('keeps the summary by the pause it belongs to', () => {
    send({
      type: 'plan_summary', session_id: 's-claude', interaction_id: 'i-plan',
      bullets: ['Reviews pull requests every 30 minutes.'], status: 'ok',
    });
    send({ type: 'plan_summary', session_id: 's-claude', interaction_id: 'i-other', bullets: [], status: 'unavailable' });
    expect(useChatStore.getState().planSummaries).toEqual({
      'i-plan': { bullets: ['Reviews pull requests every 30 minutes.'], status: 'ok' },
      'i-other': { bullets: [], status: 'unavailable' },
    });
  });

  it('ignores a summary for another session than the one on screen', () => {
    send({ type: 'plan_summary', session_id: 's-codex', interaction_id: 'i-x', bullets: ['x'], status: 'ok' });
    expect(useChatStore.getState().planSummaries).toEqual({});
  });

  it('is dropped with the session', async () => {
    useChatStore.setState({ planSummaries: { 'i-plan': { bullets: ['x'], status: 'ok' } }, planEditRequest: 'i-plan' });
    await useChatStore.getState().switchSession('s-codex');
    expect(useChatStore.getState()).toMatchObject({ planSummaries: {}, planEditRequest: null });
  });

  it('comes back from the replay of a running turn', () => {
    send({
      type: 'session_status', session_id: 's-claude', is_running: true,
      buffered_events: [
        planExit,
        { type: 'plan_summary', session_id: 's-claude', interaction_id: 'i-plan', bullets: ['A summary.'], status: 'ok' },
      ],
    });
    const s = useChatStore.getState();
    expect(s.pendingInteraction).toMatchObject({ interactionId: 'i-plan' });
    expect(s.planSummaries['i-plan']).toEqual({ bullets: ['A summary.'], status: 'ok' });
  });
});

describe('plan_update', () => {
  it('updates the live plan tab in place, and leaves a plan opened to read alone', () => {
    useChatStore.setState({
      panels: [planTab(), planTab({ id: 'plan-read-tu-old', readOnly: true, content: 'earlier' })],
    });
    send({ type: 'plan_update', session_id: 's-claude', content: PLAN });
    const [live, read] = useChatStore.getState().panels;
    expect(live.content).toBe(PLAN);
    expect(read.content).toBe('earlier');
  });

  it('keeps a new plan in the background in a Claude session: no panel, no focus', () => {
    useChatStore.setState({ panels: [planTab({ id: 'agents-panel', type: 'agents' })], activePanelId: 'agents-panel' });
    send({ type: 'plan_update', session_id: 's-claude', content: PLAN });
    const s = useChatStore.getState();
    expect(s.panels.map(p => p.type)).toEqual(['agents', 'plan']);
    expect(s.panels[1].content).toBe(PLAN);
    expect(s.activePanelId).toBe('agents-panel');
    expect(s.panelVisible).toBe(false);
  });

  it('opens the panel on a new plan in a Codex session, where the panel is the plan\'s only place', () => {
    useChatStore.setState({ activeSession: 's-codex' });
    send({ type: 'plan_update', session_id: 's-codex', content: '1. Read the code' });
    const s = useChatStore.getState();
    expect(s.panels).toHaveLength(1);
    expect(s.panelVisible).toBe(true);
    expect(s.activePanelId).toBe(s.panels[0].id);
  });

  it('restores a Claude session\'s plan on replay without opening the panel', () => {
    send({
      type: 'session_status', session_id: 's-claude', is_running: true,
      buffered_events: [{ type: 'plan_update', session_id: 's-claude', content: PLAN }],
    });
    const s = useChatStore.getState();
    expect(s.panels.find(p => p.type === 'plan')?.content).toBe(PLAN);
    expect(s.panelVisible).toBe(false);
  });
});
