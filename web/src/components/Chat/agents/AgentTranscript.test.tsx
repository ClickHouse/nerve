import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { AgentEventItem } from '../../../types/agents';

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
const {
  FX_CHILD, FX_CODEX, FX_FAILED, FX_NOW_MS, FX_RUNNING, FX_SESSION_ID, FX_SHELL, FX_WAVE, FX_WORKFLOW,
  makeAgentStoreSeed, makeTranscriptItems, makeTranscriptState,
} = await import('../../../test/agentFixtures');
const { AgentTranscript } = await import('./AgentTranscript');

const loadEarlier = vi.fn(async () => {});
const refreshOutput = vi.fn(async () => {});
const loadDetail = vi.fn(async () => {});
const stopAgent = vi.fn(async () => {});
const prefillComposer = vi.fn();

function seed(transcripts: Record<string, ReturnType<typeof makeTranscriptState>>, agents = FX_WAVE) {
  useAgentStore.setState({
    ...makeAgentStoreSeed(agents, { transcripts }),
    loadEarlier, refreshOutput, loadDetail, stopAgent, prefillComposer,
  });
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(FX_NOW_MS);
  for (const fn of [loadEarlier, refreshOutput, loadDetail, stopAgent, prefillComposer]) fn.mockClear();
  useChatStore.setState({ activeSession: FX_SESSION_ID, panelVisible: true });
});
afterEach(() => {
  vi.useRealTimers();
});

describe('AgentTranscript', () => {
  it('offers Jump to spawn only for the orchestrator’s own spawns, and its note goes away', () => {
    vi.useRealTimers();
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] });
    vi.setSystemTime(FX_NOW_MS);
    seed({ [FX_RUNNING.id]: makeTranscriptState(makeTranscriptItems(2)) }, [...FX_WAVE, FX_CHILD]);
    const { unmount } = render(<AgentTranscript agentId={FX_CHILD.id} />);
    expect(screen.queryByRole('button', { name: 'Jump to spawn' })).toBeNull();
    unmount();

    render(<AgentTranscript agentId={FX_RUNNING.id} />);
    act(() => { screen.getByRole('button', { name: 'Jump to spawn' }).click(); });
    const note = 'The spawn point is not on the page (it may be in a collapsed group).';
    expect(screen.getByText(note)).toBeInTheDocument();
    act(() => { vi.advanceTimersByTime(4000); });
    expect(screen.queryByText(note)).toBeNull();
  });

  it('renders the transcript as chat blocks with lifecycle dividers', () => {
    const items: AgentEventItem[] = [
      ...makeTranscriptItems(3),
      { seq: 4, at: '2026-02-27T02:09:00Z', kind: 'lifecycle', event: 'stopped', run: 1, reason: 'user', actor_id: null },
      { seq: 5, at: '2026-02-27T02:09:30Z', kind: 'lifecycle', event: 'resumed', run: 2 },
      { seq: 6, at: '2026-02-27T02:09:31Z', kind: 'text', text: 'Picking it up again.' },
    ];
    seed({ [FX_RUNNING.id]: makeTranscriptState(items) });
    render(<AgentTranscript agentId={FX_RUNNING.id} />);

    expect(screen.getByRole('heading', { name: 'build-api' })).toBeInTheDocument();
    expect(screen.getByText('Step 1: checking the build.')).toBeInTheDocument();
    expect(screen.getByText(/pytest -q tests\/test_2\.py/)).toBeInTheDocument();
    expect(screen.getByText('Stopped by you')).toBeInTheDocument();
    expect(screen.getByText('Resumed · run 2')).toBeInTheDocument();
    expect(screen.getByText('Picking it up again.')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Load earlier' })).toBeNull();
  });

  it('loads earlier items on request', async () => {
    const user = userEvent.setup();
    seed({ [FX_RUNNING.id]: makeTranscriptState(makeTranscriptItems(4, 201), { hasMoreBefore: true }) });
    render(<AgentTranscript agentId={FX_RUNNING.id} />);
    await user.click(screen.getByRole('button', { name: 'Load earlier' }));
    expect(loadEarlier).toHaveBeenCalledWith(FX_RUNNING.id);
  });

  it('shows Loading… until the tail arrives, and a retry after a failure', () => {
    seed({ [FX_RUNNING.id]: makeTranscriptState([], { status: 'loading' }) });
    const { unmount } = render(<AgentTranscript agentId={FX_RUNNING.id} />);
    expect(screen.getByText('Loading…')).toBeInTheDocument();
    unmount();

    seed({ [FX_RUNNING.id]: makeTranscriptState([], { status: 'error', error: '500: boom' }) });
    render(<AgentTranscript agentId={FX_RUNNING.id} />);
    expect(screen.getByText("Couldn't load this agent's activity.")).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Retry/ })).toBeInTheDocument();
  });

  it('shows a shell\'s output tail and refreshes it every 5 s while it runs', () => {
    vi.useRealTimers();
    vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] });
    vi.setSystemTime(FX_NOW_MS);
    seed({
      [FX_SHELL.id]: makeTranscriptState([], {
        output: { agent_id: FX_SHELL.id, text: 'e2e: 12 passed', size: 14, truncated: false, available: true },
      }),
    });
    render(<AgentTranscript agentId={FX_SHELL.id} />);
    expect(screen.getByText('$ npm run e2e')).toBeInTheDocument();
    expect(screen.getByText('e2e: 12 passed')).toBeInTheDocument();
    expect(refreshOutput).not.toHaveBeenCalled();
    act(() => { vi.advanceTimersByTime(5000); });
    expect(refreshOutput).toHaveBeenCalledWith(FX_SHELL.id);
  });

  it('says when a shell\'s output file is gone', () => {
    seed({
      [FX_SHELL.id]: makeTranscriptState([], {
        output: { agent_id: FX_SHELL.id, text: '', size: 0, truncated: false, available: false },
      }),
    });
    render(<AgentTranscript agentId={FX_SHELL.id} />);
    expect(screen.getByText('The output file is no longer available.')).toBeInTheDocument();
  });

  it('shows only the brief and result for a backend without transcripts', () => {
    seed({
      [FX_CODEX.id]: makeTranscriptState([], {
        detail: { agent: FX_CODEX, prompt: 'Review the parser for edge cases.', result_text: 'Found two issues.' },
      }),
    }, [...FX_WAVE, FX_CODEX]);
    render(<AgentTranscript agentId={FX_CODEX.id} />);
    expect(screen.getByText("This backend doesn't stream subagent transcripts.")).toBeInTheDocument();
    expect(screen.getByText('Review the parser for edge cases.')).toBeInTheDocument();
    expect(screen.getByText('Found two issues.')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /^Stop/ })).toBeNull();
  });

  it('asks for the detail of such an agent when it is missing', async () => {
    seed({ [FX_CODEX.id]: makeTranscriptState([]) }, [...FX_WAVE, FX_CODEX]);
    render(<AgentTranscript agentId={FX_CODEX.id} />);
    expect(loadDetail).toHaveBeenCalledWith(FX_CODEX.id);
    // The fetch brought nothing: what the record has is shown instead.
    expect(await screen.findByText('Brief')).toBeInTheDocument();
  });

  it('shows a workflow as its phase tree', () => {
    seed({ [FX_WORKFLOW.id]: makeTranscriptState([]) }, [...FX_WAVE, FX_WORKFLOW]);
    render(<AgentTranscript agentId={FX_WORKFLOW.id} />);
    expect(screen.getByText('Build')).toBeInTheDocument();
    expect(screen.getByText('test-a')).toBeInTheDocument();
  });

  it('flags a changed model in amber and explains it', () => {
    const switched = { ...FX_RUNNING, model_changed: { from: 'model-a', to: 'model-b', at: '2026-02-27T02:05:00Z' } };
    seed({ [FX_RUNNING.id]: makeTranscriptState([]) }, [switched]);
    render(<AgentTranscript agentId={FX_RUNNING.id} />);
    expect(screen.getByTitle('requested opus · ran model-a → model-b')).toHaveTextContent('model-b');
  });

  it('stops in two clicks, and follows up through the composer', async () => {
    const user = userEvent.setup();
    seed({ [FX_RUNNING.id]: makeTranscriptState([]) });
    render(<AgentTranscript agentId={FX_RUNNING.id} />);
    await user.click(screen.getByRole('button', { name: 'Stop build-api' }));
    expect(stopAgent).not.toHaveBeenCalled();
    await user.click(screen.getByRole('button', { name: 'Confirm: stop build-api' }));
    expect(stopAgent).toHaveBeenCalledTimes(1);

    await user.click(screen.getByRole('button', { name: 'Follow up with build-api…' }));
    expect(prefillComposer).toHaveBeenCalledWith('Use SendMessage to tell `build-api`: ', 'append');
  });

  it('offers Relaunch… for a failed agent, and no Stop', async () => {
    const user = userEvent.setup();
    seed({ [FX_FAILED.id]: makeTranscriptState([]) });
    render(<AgentTranscript agentId={FX_FAILED.id} />);
    expect(screen.queryByRole('button', { name: /^Stop/ })).toBeNull();
    await user.click(screen.getByRole('button', { name: 'Relaunch review-api…' }));
    expect(prefillComposer).toHaveBeenCalledWith(expect.stringContaining('Relaunch the agent `review-api`'), 'append');
    expect(screen.getByText('Tests failed: 3 errors')).toBeInTheDocument();
  });
});
