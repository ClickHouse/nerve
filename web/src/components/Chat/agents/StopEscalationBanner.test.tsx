import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { AgentBrief } from '../../../types/agents';

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
const { useChatStore } = await import('../../../stores/chatStore');
const { FX_WAVE, makeAgentStoreSeed } = await import('../../../test/agentFixtures');
const { StopEscalationBanner } = await import('./StopEscalationBanner');

const stopTurn = vi.fn(async () => null);
const brief = (label: string): AgentBrief => ({
  id: `ag_${label}`, label, kind: 'agent', status: 'running', started_at: '2026-02-27T02:01:01Z', tokens: 1000,
});

beforeEach(() => {
  stopTurn.mockClear();
  useAgentStore.setState({ ...makeAgentStoreSeed(FX_WAVE), stopTurn });
  useChatStore.setState({ isStreaming: true });
});

describe('StopEscalationBanner', () => {
  it('is absent without an escalation', () => {
    render(<StopEscalationBanner />);
    expect(screen.queryByRole('alert')).toBeNull();
  });

  it('offers Force stop… when the turn did not stop', async () => {
    useAgentStore.setState({ escalation: { kind: 'not_stopped', wouldEnd: [brief('a'), brief('b')], shellCount: 0 } });
    render(<StopEscalationBanner />);
    expect(screen.getByRole('alert')).toHaveTextContent("The turn didn't stop after 5 s. 2 agents are still running.");
    await userEvent.click(screen.getByRole('button', { name: 'Force stop…' }));
    expect(useAgentStore.getState().stopDialog).toBe('force');
  });

  it('says when a stop request failed, also after the turn ended', async () => {
    useChatStore.setState({ isStreaming: false });
    useAgentStore.setState({ stopError: "Couldn't stop build-api: no answer from the server." });
    render(<StopEscalationBanner />);
    expect(screen.getByRole('alert')).toHaveTextContent("Couldn't stop build-api: no answer from the server.");
    await userEvent.click(screen.getByRole('button', { name: 'Dismiss' }));
    expect(useAgentStore.getState().stopError).toBeNull();
    expect(screen.queryByRole('alert')).toBeNull();
  });

  it('can be dismissed', async () => {
    useAgentStore.setState({ escalation: { kind: 'not_stopped', wouldEnd: [brief('a')], shellCount: 0 } });
    render(<StopEscalationBanner />);
    expect(screen.getByRole('alert')).toHaveTextContent('1 agent is still running');
    await userEvent.click(screen.getByRole('button', { name: 'Dismiss' }));
    expect(useAgentStore.getState().escalation).toBeNull();
    expect(screen.queryByRole('alert')).toBeNull();
  });

  it('asks before stopping agents a runtime cannot spare', async () => {
    useAgentStore.setState({ escalation: { kind: 'needs_confirmation', wouldEnd: [brief('a'), brief('b'), brief('c')], shellCount: 1 } });
    render(<StopEscalationBanner />);
    expect(screen.getByRole('alert')).toHaveTextContent(
      "This runtime can't spare background agents: stopping the turn also stops 3 of them and 1 background shell.",
    );
    await userEvent.click(screen.getByRole('button', { name: 'Stop turn and agents' }));
    expect(stopTurn).toHaveBeenCalledWith({ confirmKillBackground: true });
  });

  it('cancels a stop that needs confirmation', async () => {
    useAgentStore.setState({ escalation: { kind: 'needs_confirmation', wouldEnd: [brief('a')], shellCount: 0 } });
    render(<StopEscalationBanner />);
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(useAgentStore.getState().escalation).toBeNull();
    expect(stopTurn).not.toHaveBeenCalled();
  });

  it('goes away once the turn ends', () => {
    useAgentStore.setState({ escalation: { kind: 'not_stopped', wouldEnd: [brief('a')], shellCount: 0 } });
    render(<StopEscalationBanner />);
    expect(screen.getByRole('alert')).toBeInTheDocument();
    act(() => { useChatStore.setState({ isStreaming: false }); });
    expect(screen.queryByRole('alert')).toBeNull();
    expect(useAgentStore.getState().escalation).toBeNull();
  });
});
