import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

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
const { FX_RUNNING, FX_WAVE, makeAgentStoreSeed, makeAgentView } = await import('../../../test/agentFixtures');
const { StopAllDialog } = await import('./StopAllDialog');

const stopAll = vi.fn(async () => null);

beforeEach(() => {
  stopAll.mockClear();
  useAgentStore.setState({ ...makeAgentStoreSeed(FX_WAVE, { stopDialog: 'all' }), stopAll });
});

describe('StopAllDialog', () => {
  it('is closed unless asked for', () => {
    useAgentStore.setState({ stopDialog: null });
    render(<StopAllDialog />);
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('lists every live agent, shells included, with the note about resuming', () => {
    render(<StopAllDialog />);
    const dialog = screen.getByRole('dialog', { name: 'Stop all agents?' });
    const listed = within(within(dialog).getByRole('list', { name: 'Agents that will stop' })).getAllByRole('listitem');
    // FX_WAVE runs build-api, build-ui, build-ui-tests, a shell and build-docs (stopping).
    expect(listed).toHaveLength(5);
    expect(within(dialog).getByText('This stops the turn and 4 agents and 1 shell or monitor:')).toBeInTheDocument();
    expect(within(dialog).getByText(/can't be resumed with SendMessage/)).toBeInTheDocument();
  });

  it('stops all, shells included by default', async () => {
    render(<StopAllDialog />);
    const box = screen.getByRole('checkbox', { name: 'Also stop background shells and monitors (1)' });
    expect(box).toBeChecked();
    await userEvent.click(screen.getByRole('button', { name: 'Stop all' }));
    expect(stopAll).toHaveBeenCalledWith({ includeShells: true });
  });

  it('can spare the shells', async () => {
    render(<StopAllDialog />);
    await userEvent.click(screen.getByRole('checkbox', { name: /Also stop background shells/ }));
    expect(screen.getByText('keeps running')).toBeInTheDocument();
    expect(screen.getByText('This stops the turn and 4 agents:')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Stop all' }));
    expect(stopAll).toHaveBeenCalledWith({ includeShells: false });
  });

  it('closes on Cancel without stopping anything', async () => {
    render(<StopAllDialog />);
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(useAgentStore.getState().stopDialog).toBeNull();
    expect(stopAll).not.toHaveBeenCalled();
  });

  it('lists 20 agents, then how many more', () => {
    const many = Array.from({ length: 23 }, (_, i) => makeAgentView({
      ...FX_RUNNING, id: `ag_many${String(i).padStart(7, '0')}`, name: `agent-${i}`, label: `agent-${i}`,
      spawn_tool_use_id: `toolu_many${i}`,
    }));
    useAgentStore.setState(makeAgentStoreSeed(many, { stopDialog: 'all' }));
    render(<StopAllDialog />);
    expect(screen.getByText('+3 more')).toBeInTheDocument();
    expect(screen.queryByRole('checkbox')).toBeNull();
  });
});
