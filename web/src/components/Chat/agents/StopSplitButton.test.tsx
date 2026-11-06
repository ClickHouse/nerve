import { fireEvent, render, screen } from '@testing-library/react';
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
const { FX_CAPABILITIES, FX_COMPLETED, FX_WAVE, makeAgentStoreSeed } = await import('../../../test/agentFixtures');
const { StopSplitButton } = await import('./StopSplitButton');

const onStopTurn = vi.fn();
const onStopAndSend = vi.fn();

function renderButton(hasContent: boolean) {
  return render(<StopSplitButton onStopTurn={onStopTurn} hasContent={hasContent} onStopAndSend={onStopAndSend} />);
}

function menuItems() {
  return screen.getAllByRole('button')
    .map(b => b.textContent ?? '')
    .filter(t => /Stop turn|Stop all/.test(t));
}

beforeEach(() => {
  onStopTurn.mockClear();
  onStopAndSend.mockClear();
  useAgentStore.setState(makeAgentStoreSeed(FX_WAVE));
});

describe('StopSplitButton', () => {
  it('stops the turn with its main half', async () => {
    renderButton(false);
    await userEvent.click(screen.getByRole('button', { name: 'Stop the turn' }));
    expect(onStopTurn).toHaveBeenCalledTimes(1);
  });

  it('offers stop turn, stop turn and send, and stop all while agents run and text waits', async () => {
    renderButton(true);
    const caret = screen.getByRole('button', { name: 'Stop options' });
    expect(caret).toHaveAttribute('aria-expanded', 'false');
    await userEvent.click(caret);
    expect(caret).toHaveAttribute('aria-expanded', 'true');
    expect(menuItems()).toEqual(['Stop turnAgents keep running', 'Stop turn and sendYour message runs next', 'Stop all agents…']);
    expect(screen.queryAllByRole('menu')).toHaveLength(0);
  });

  it('leaves out "Stop turn and send" without text', async () => {
    renderButton(false);
    await userEvent.click(screen.getByRole('button', { name: 'Stop options' }));
    expect(menuItems()).toEqual(['Stop turnAgents keep running', 'Stop all agents…']);
  });

  it('warns when the runtime cannot spare background agents', async () => {
    useAgentStore.setState({ capabilities: { ...FX_CAPABILITIES, interrupt_spares_background: false } });
    renderButton(false);
    await userEvent.click(screen.getByRole('button', { name: 'Stop options' }));
    const live = useAgentStore.getState().summary.live;
    expect(screen.getByText(`Also stops ${live} background agents`)).toBeInTheDocument();
  });

  it('offers no "Stop all agents…" when no agent runs', async () => {
    useAgentStore.setState(makeAgentStoreSeed([FX_COMPLETED]));
    renderButton(true);
    await userEvent.click(screen.getByRole('button', { name: 'Stop options' }));
    expect(menuItems()).toEqual(['Stop turn', 'Stop turn and sendYour message runs next']);
  });

  it('runs the picked item and closes', async () => {
    renderButton(true);
    await userEvent.click(screen.getByRole('button', { name: 'Stop options' }));
    await userEvent.click(screen.getByRole('button', { name: /Stop turn and send/ }));
    expect(onStopAndSend).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole('button', { name: /Stop turn and send/ })).toBeNull();

    await userEvent.click(screen.getByRole('button', { name: 'Stop options' }));
    await userEvent.click(screen.getByRole('button', { name: /Stop all agents/ }));
    expect(useAgentStore.getState().stopDialog).toBe('all');
  });

  it('closes on Escape without the Escape reaching the global stop', async () => {
    const globalKeys = vi.fn();
    document.addEventListener('keydown', globalKeys);
    try {
      renderButton(false);
      await userEvent.click(screen.getByRole('button', { name: 'Stop options' }));
      fireEvent.keyDown(document.activeElement!, { key: 'Escape' });
      expect(screen.getByRole('button', { name: 'Stop options' })).toHaveAttribute('aria-expanded', 'false');
      expect(globalKeys).not.toHaveBeenCalled();
      expect(onStopTurn).not.toHaveBeenCalled();
    } finally {
      document.removeEventListener('keydown', globalKeys);
    }
  });
});
