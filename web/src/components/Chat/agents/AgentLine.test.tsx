import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ToolCallBlockData } from '../../../types/chat';

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
    stopSession: vi.fn(), takePendingMessages: vi.fn(() => []), answerInteraction: vi.fn(),
  },
}));

const { useAgentStore } = await import('../../../stores/agentStore');
const { useChatStore } = await import('../../../stores/chatStore');
const {
  FX_COMPLETED, FX_NOW_MS, FX_RUNNING, FX_SESSION_ID, FX_STOPPED_BY_USER, FX_WAVE, makeAgentStoreSeed,
} = await import('../../../test/agentFixtures');
const { AgentLine } = await import('./AgentLine');

function spawn(toolUseId: string, overrides: Partial<ToolCallBlockData> = {}): ToolCallBlockData {
  return {
    type: 'tool_call', toolUseId, tool: 'Agent', status: 'complete',
    input: { description: 'Build the API', subagent_type: 'general-purpose', prompt: 'Build the API and run its tests.' },
    result: 'Async agent launched successfully.',
    ...overrides,
  };
}

const stopAgent = vi.fn(async () => {});
const openAgentTab = vi.fn();

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(FX_NOW_MS);
  stopAgent.mockClear();
  openAgentTab.mockClear();
  useAgentStore.setState({ ...makeAgentStoreSeed(FX_WAVE), stopAgent });
  useChatStore.setState({ activeSession: FX_SESSION_ID, openAgentTab });
});
afterEach(() => {
  vi.useRealTimers();
});

describe('AgentLine', () => {
  it('shows a running agent with its activity, elapsed time and tokens', () => {
    const { container } = render(<AgentLine block={spawn(FX_RUNNING.spawn_tool_use_id!)} />);
    expect(screen.getByText('build-api')).toBeInTheDocument();
    expect(screen.getByText('general')).toBeInTheDocument();
    expect(screen.getByText('Running the API test suite')).toBeInTheDocument();
    expect(screen.getByText('8m')).toBeInTheDocument();
    expect(screen.getByText('38k')).toBeInTheDocument();
    expect(container.querySelector('[data-agent-spawn="toolu_01AGENT"]')).not.toBeNull();
    expect(screen.getByRole('group', { name: /^build-api, running 8 minutes, 38 thousand tokens/ })).toBeInTheDocument();
  });

  it('finds the record by the persisted agent id as well as by the spawn id', () => {
    render(<AgentLine block={spawn('toolu_unrelated', { agentId: FX_COMPLETED.id })} />);
    expect(screen.getByText('build-chart')).toBeInTheDocument();
    expect(screen.getByText('4 files changed')).toBeInTheDocument();
  });

  it('says how a stopped agent ended, and offers no stop', () => {
    render(<AgentLine block={spawn(FX_STOPPED_BY_USER.spawn_tool_use_id!)} />);
    expect(screen.getByText(/^Stopped by you · \d{2}:\d{2}$/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /^Stop / })).toBeNull();
  });

  it('opens the agent tab', async () => {
    const user = userEvent.setup();
    render(<AgentLine block={spawn(FX_RUNNING.spawn_tool_use_id!)} />);
    await user.click(screen.getByRole('button', { name: 'Open build-api' }));
    expect(openAgentTab).toHaveBeenCalledWith(FX_RUNNING.id);
  });

  it('stops only on the second click', async () => {
    const user = userEvent.setup();
    render(<AgentLine block={spawn(FX_RUNNING.spawn_tool_use_id!)} />);
    await user.click(screen.getByRole('button', { name: 'Stop build-api' }));
    expect(stopAgent).not.toHaveBeenCalled();
    expect(screen.getByText('Click again to stop build-api')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Confirm: stop build-api' }));
    expect(stopAgent).toHaveBeenCalledTimes(1);
    expect(stopAgent).toHaveBeenCalledWith(FX_RUNNING.id);
    expect(screen.getByRole('button', { name: 'Stop build-api' })).toBeInTheDocument();
  });

  it('keeps keyboard focus on the stop button between the two presses', async () => {
    const user = userEvent.setup();
    render(<AgentLine block={spawn(FX_RUNNING.spawn_tool_use_id!)} />);
    const button = screen.getByRole('button', { name: 'Stop build-api' });
    act(() => { button.focus(); });
    await user.keyboard('{Enter}');
    expect(document.activeElement).toBe(button);
    expect(button).toHaveAccessibleName('Confirm: stop build-api');
    await user.keyboard('{Enter}');
    expect(stopAgent).toHaveBeenCalledTimes(1);
  });

  it('lets an armed stop lapse after three seconds', async () => {
    vi.useRealTimers();
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] });
    vi.setSystemTime(FX_NOW_MS);
    render(<AgentLine block={spawn(FX_RUNNING.spawn_tool_use_id!)} />);
    act(() => { screen.getByRole('button', { name: 'Stop build-api' }).click(); });
    expect(screen.getByRole('button', { name: 'Confirm: stop build-api' })).toBeInTheDocument();
    act(() => { vi.advanceTimersByTime(3100); });
    expect(screen.getByRole('button', { name: 'Stop build-api' })).toBeInTheDocument();
    expect(stopAgent).not.toHaveBeenCalled();
  });

  it('reveals the brief and the outcome on expand', async () => {
    const user = userEvent.setup();
    render(<AgentLine block={spawn(FX_COMPLETED.spawn_tool_use_id!)} />);
    await user.click(screen.getByRole('button', { name: 'Show the brief and result' }));
    expect(screen.getByText('Build the API and run its tests.')).toBeInTheDocument();
    // A background spawn's outcome is the record's, not the "launched" note.
    expect(screen.getAllByText('4 files changed').length).toBeGreaterThan(1);
    expect(screen.queryByText('Async agent launched successfully.')).toBeNull();
  });

  it('shows a spawn whose record has not arrived yet as starting', () => {
    render(<AgentLine block={spawn('toolu_new', { status: 'running', result: undefined })} />);
    expect(screen.getByText('Starting…')).toBeInTheDocument();
    expect(screen.getByText('Build the API')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /^Stop/ })).toBeNull();
  });

  it('falls back to the legacy card for old history without a record', () => {
    const { container } = render(
      <AgentLine block={spawn('toolu_old', { input: { description: 'Old work', subagent_type: 'Explore' }, result: 'All good' })} />,
    );
    const card = within(container);
    expect(card.getByText('Explore')).toBeInTheDocument();
    expect(card.getByText('Old work')).toBeInTheDocument();
    expect(card.getByRole('button', { name: /View/ })).toBeInTheDocument();
    expect(card.queryByText('Starting…')).toBeNull();
  });
});

describe('AgentLine for a record the store lacks', () => {
  it("links a fork's inherited card to the session that owns the agent", async () => {
    const { api } = await import('../../../api/client');
    const { ApiError } = await import('../../../api/hosted');
    vi.mocked(api.getAgentDetail).mockRejectedValueOnce(new ApiError(404, JSON.stringify({
      detail: { code: 'agent_in_other_session', message: 'Agent belongs to another session', owner_session_id: FX_SESSION_ID },
    })));
    useAgentStore.setState(makeAgentStoreSeed([], { sessionId: 'fork-1a2b3c4d' }));
    useChatStore.setState({ activeSession: 'fork-1a2b3c4d' });

    render(<AgentLine block={spawn('toolu_inherited', { agentId: 'ag_inherited001' })} />);

    expect(await screen.findByText(/Belongs to another session/)).toBeInTheDocument();
    expect(api.getAgentDetail).toHaveBeenCalledWith('fork-1a2b3c4d', 'ag_inherited001');
  });

  it('adds a record outside the snapshot window to the store, keeping the summary', async () => {
    const { api } = await import('../../../api/client');
    const view = {
      ...FX_COMPLETED, id: 'ag_olderrecord1', spawn_tool_use_id: 'toolu_older', name: 'old-build', label: 'old-build',
    };
    vi.mocked(api.getAgentDetail).mockResolvedValueOnce({ agent: view, prompt: null, result_text: null });
    const seed = makeAgentStoreSeed(FX_WAVE);
    // The session holds more records than the snapshot carried.
    useAgentStore.setState({ ...seed, summary: { ...seed.summary!, total: 450 } });

    render(<AgentLine block={spawn('toolu_older', { agentId: view.id })} />);

    expect(await screen.findByText('old-build')).toBeInTheDocument();
    expect(useAgentStore.getState().agents[view.id]).toEqual(view);
    expect(useAgentStore.getState().summary.total).toBe(450);
  });
});
