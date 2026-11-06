import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));
vi.mock('../../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
  },
}));

const { useAgentStore } = await import('../../../stores/agentStore');
const { useChatStore, AGENTS_PANEL_ID } = await import('../../../stores/chatStore');
const {
  FX_NOW_MS, FX_RUNNING, FX_SESSION_ID, FX_WAVE, makeAgentStoreSeed, makeSummary,
} = await import('../../../test/agentFixtures');
const { AgentDock } = await import('./AgentDock');

function dockButton() {
  return screen.queryByRole('button', { name: /Agents$/ });
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(FX_NOW_MS);
  useChatStore.setState({ activeSession: FX_SESSION_ID, panels: [], activePanelId: null, panelVisible: false });
  useAgentStore.setState(makeAgentStoreSeed(FX_WAVE));
});
afterEach(() => {
  vi.useRealTimers();
});

describe('AgentDock', () => {
  it('is absent while the session has no agent records', () => {
    useAgentStore.setState(makeAgentStoreSeed([]));
    render(<AgentDock sessionId={FX_SESSION_ID} />);
    expect(dockButton()).toBeNull();
  });

  it('is absent for another session than the one the store holds', () => {
    render(<AgentDock sessionId="s-other" />);
    expect(dockButton()).toBeNull();
  });

  it('counts running, attention and done agents and names the latest activity', () => {
    render(<AgentDock sessionId={FX_SESSION_ID} />);
    const button = dockButton()!;
    expect(button).toHaveAttribute('aria-controls', 'agents-panel');
    expect(button).toHaveTextContent('4 running');
    expect(button).toHaveTextContent('2 need attention');
    expect(button).toHaveTextContent('1 done');
    expect(button).toHaveTextContent('build-ui-tests: Edit: app.test.tsx');
  });

  it('opens the Agents panel, focused, and reports it as expanded', async () => {
    const user = userEvent.setup();
    render(<AgentDock sessionId={FX_SESSION_ID} />);
    expect(dockButton()).toHaveAttribute('aria-expanded', 'false');
    const nonce = useAgentStore.getState().focusNonce;

    await user.click(dockButton()!);

    const chat = useChatStore.getState();
    expect(chat.panelVisible).toBe(true);
    expect(chat.activePanelId).toBe(AGENTS_PANEL_ID);
    expect(useAgentStore.getState().focusNonce).toBeGreaterThan(nonce);
    expect(dockButton()).toHaveAttribute('aria-expanded', 'true');
  });

  it('announces an agent that settles, politely', async () => {
    render(<AgentDock sessionId={FX_SESSION_ID} />);
    const region = screen.getByRole('status');
    expect(region).toHaveTextContent('');

    const finished = { ...FX_RUNNING, status: 'completed' as const, ended_at: '2026-02-27T02:09:59Z', rev: FX_RUNNING.rev + 1 };
    act(() => {
      useAgentStore.getState().applyAgentUpdate({
        type: 'agent_update', session_id: FX_SESSION_ID, agents: [finished],
        summary: makeSummary(FX_WAVE.map(a => (a.id === finished.id ? finished : a))),
      });
    });
    await act(async () => { await new Promise(r => setTimeout(r, 10)); });
    expect(region).toHaveTextContent('build-api finished');
  });
});
