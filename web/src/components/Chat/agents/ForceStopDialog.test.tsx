import { render, screen, within } from '@testing-library/react';
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
const { FX_WAVE, makeAgentStoreSeed } = await import('../../../test/agentFixtures');
const { ForceStopDialog } = await import('./ForceStopDialog');

const forceStop = vi.fn(async () => null);
const wouldEnd: AgentBrief[] = [
  { id: 'ag_000000000101', label: 'build-api', kind: 'agent', status: 'running', started_at: '2026-02-27T02:01:01Z', tokens: 38211 },
  { id: 'ag_000000000102', label: 'build-web', kind: 'agent', status: 'running', started_at: '2026-02-27T02:02:01Z', tokens: 9000 },
];

beforeEach(() => {
  forceStop.mockClear();
  useAgentStore.setState({
    ...makeAgentStoreSeed(FX_WAVE, {
      stopDialog: 'force',
      escalation: { kind: 'not_stopped', wouldEnd, shellCount: 1 },
    }),
    forceStop,
  });
});

describe('ForceStopDialog', () => {
  it('explains what force stop ends', () => {
    render(<ForceStopDialog />);
    const dialog = screen.getByRole('dialog', { name: 'Force stop?' });
    expect(within(dialog).getByText(/restarts this session's runtime and ends everything still running/)).toBeInTheDocument();
    const listed = within(within(dialog).getByRole('list', { name: 'What force stop ends' })).getAllByRole('listitem');
    expect(listed.map(li => li.textContent)).toEqual([expect.stringContaining('build-api'), expect.stringContaining('build-web')]);
    expect(within(dialog).getByText('Also 1 background shell or monitor.')).toBeInTheDocument();
  });

  it('force stops on confirmation', async () => {
    render(<ForceStopDialog />);
    await userEvent.click(screen.getByRole('button', { name: 'Force stop' }));
    expect(forceStop).toHaveBeenCalledTimes(1);
  });

  it('keeps waiting on request', async () => {
    render(<ForceStopDialog />);
    await userEvent.click(screen.getByRole('button', { name: 'Keep waiting' }));
    expect(useAgentStore.getState().stopDialog).toBeNull();
    expect(forceStop).not.toHaveBeenCalled();
  });

  it('is closed unless asked for', () => {
    useAgentStore.setState({ stopDialog: 'all' });
    render(<ForceStopDialog />);
    expect(screen.queryByRole('dialog', { name: 'Force stop?' })).toBeNull();
  });
});
