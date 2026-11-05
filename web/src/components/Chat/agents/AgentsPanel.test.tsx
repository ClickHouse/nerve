import { act, fireEvent, render, screen, within } from '@testing-library/react';
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
const { useChatStore } = await import('../../../stores/chatStore');
const {
  FX_COMPLETED, FX_FAILED, FX_NOW_MS, FX_RUNNING, FX_SESSION_ID, FX_WAVE, makeAgentStoreSeed, makeAgentView,
} = await import('../../../test/agentFixtures');
const { AgentsPanel } = await import('./AgentsPanel');

const stopAgent = vi.fn(async () => {});
const openAgentTab = vi.fn();

function rowButtons() {
  return screen.getAllByRole('button').filter(b => b.hasAttribute('data-agent-row'));
}

function rowLabels() {
  return rowButtons().map(b => b.getAttribute('aria-label')!.split(',')[0]);
}

function renderPanel() {
  return render(
    <>
      <textarea id="nerve-chat-input" aria-label="Message" />
      <AgentsPanel />
    </>,
  );
}

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

describe('AgentsPanel', () => {
  it('groups agents: needs attention, running as a tree, done, then ended (collapsed)', () => {
    renderPanel();
    const list = screen.getByRole('list', { name: 'Agents' });
    const headings = within(list).getAllByText(/\(\d+\)$/).map(el => el.textContent);
    expect(headings).toEqual(['Needs attention (2)', 'Running (4)', 'Done (1)', 'Ended (1)']);
    expect(rowLabels()).toEqual([
      'review-api', 'build-docs',
      'build-api', 'build-ui', 'build-ui-tests', 'npm run e2e',
      'build-chart',
    ]);
    // Ended is folded away until opened.
    expect(screen.getByRole('button', { name: 'Ended (1)' })).toHaveAttribute('aria-expanded', 'false');
  });

  it('draws the child marker only in the Running tree', () => {
    const nestedFailure = makeAgentView({
      id: 'ag_00000000nf01', name: 'nested-fail', status: 'failed', end_reason: 'error',
      attention: 'failed', parent_agent_id: 'ag_gone', depth: 2,
      ended_at: '2026-02-27T02:08:00Z',
    });
    useAgentStore.setState({ ...makeAgentStoreSeed([...FX_WAVE, nestedFailure]), stopAgent });
    renderPanel();
    const marked = (label: string) => {
      const button = rowButtons().find(b => b.getAttribute('aria-label')!.startsWith(`${label},`))!;
      return button.textContent!.includes('└');
    };
    expect(marked('build-ui-tests')).toBe(true);   // a child under its live parent
    expect(marked('build-ui')).toBe(false);
    expect(marked('nested-fail')).toBe(false);     // a flat group: no tree to point into
  });

  it('opens Ended on request', async () => {
    const user = userEvent.setup();
    renderPanel();
    await user.click(screen.getByRole('button', { name: 'Ended (1)' }));
    expect(rowLabels()).toContain('lint-all');
  });

  it('shows the session summary and offers Stop all while agents run', async () => {
    const user = userEvent.setup();
    renderPanel();
    expect(screen.getByText(/4 running · 2 need attention/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Stop all…' }));
    expect(useAgentStore.getState().stopDialog).toBe('all');
  });

  it('has one tab stop and moves it with the arrow keys, Home and End', () => {
    renderPanel();
    const rows = rowButtons();
    expect(rows.filter(r => r.tabIndex === 0)).toHaveLength(1);
    expect(rows[0].tabIndex).toBe(0);

    act(() => { rows[0].focus(); });
    fireEvent.keyDown(rows[0], { key: 'ArrowDown' });
    expect(document.activeElement).toBe(rows[1]);
    expect(rowButtons()[1].tabIndex).toBe(0);
    expect(rowButtons()[0].tabIndex).toBe(-1);

    fireEvent.keyDown(document.activeElement!, { key: 'End' });
    expect(document.activeElement).toBe(rows[rows.length - 1]);
    fireEvent.keyDown(document.activeElement!, { key: 'ArrowUp' });
    expect(document.activeElement).toBe(rows[rows.length - 2]);
    fireEvent.keyDown(document.activeElement!, { key: 'Home' });
    expect(document.activeElement).toBe(rows[0]);
  });

  it('makes the actions of the focused row tabbable only', () => {
    renderPanel();
    const firstRow = rowButtons()[0].closest('li')!;
    const secondRow = rowButtons()[1].closest('li')!;
    expect(within(firstRow).getByRole('button', { name: 'Open review-api' }).tabIndex).toBe(0);
    expect(within(secondRow).getByRole('button', { name: 'Open build-docs' }).tabIndex).toBe(-1);
  });

  it('stops the focused agent on x pressed twice, once', () => {
    renderPanel();
    const row = rowButtons().find(b => b.getAttribute('aria-label')!.startsWith('build-api,'))!;
    act(() => { row.focus(); });
    fireEvent.keyDown(row, { key: 'x' });
    expect(stopAgent).not.toHaveBeenCalled();
    expect(screen.getByText('Press x again to stop build-api', { selector: 'span' })).toBeInTheDocument();

    fireEvent.keyDown(row, { key: 'x' });
    expect(stopAgent).toHaveBeenCalledTimes(1);
    expect(stopAgent).toHaveBeenCalledWith(FX_RUNNING.id);

    // A third press arms again rather than stopping twice.
    fireEvent.keyDown(row, { key: 'x' });
    expect(stopAgent).toHaveBeenCalledTimes(1);
  });

  it('never stops an agent that already ended', () => {
    renderPanel();
    const row = rowButtons().find(b => b.getAttribute('aria-label')!.startsWith('build-chart,'))!;
    act(() => { row.focus(); });
    fireEvent.keyDown(row, { key: 'x' });
    fireEvent.keyDown(row, { key: 'x' });
    expect(stopAgent).not.toHaveBeenCalled();
  });

  it('sends Escape back to the composer without letting it reach the global handlers', () => {
    const globalKeys = vi.fn();
    document.addEventListener('keydown', globalKeys);
    try {
      renderPanel();
      const row = rowButtons()[2];
      act(() => { row.focus(); });
      fireEvent.keyDown(row, { key: 'Escape' });
      expect(globalKeys).not.toHaveBeenCalled();
      expect(document.activeElement).toBe(document.getElementById('nerve-chat-input'));
    } finally {
      document.removeEventListener('keydown', globalKeys);
    }
  });

  it('keeps focus on an agent whose row moves to another group', () => {
    renderPanel();
    const row = rowButtons().find(b => b.getAttribute('aria-label')!.startsWith('build-api,'))!;
    act(() => { row.focus(); });
    const finished = { ...FX_RUNNING, status: 'completed' as const, ended_at: '2026-02-27T02:09:59Z', rev: FX_RUNNING.rev + 1 };
    act(() => {
      useAgentStore.setState({ agents: { ...useAgentStore.getState().agents, [finished.id]: finished } });
    });
    expect(screen.getByText('Done (2)')).toBeInTheDocument();
    expect((document.activeElement as HTMLElement).getAttribute('aria-label')).toMatch(/^build-api, completed/);
  });

  it('opens an agent with Enter (a row is a button)', async () => {
    const user = userEvent.setup();
    renderPanel();
    act(() => { rowButtons()[0].focus(); });
    await user.keyboard('{Enter}');
    expect(openAgentTab).toHaveBeenCalledWith(FX_FAILED.id);
  });

  it('moves seen failures to Ended with "Mark all seen"', async () => {
    const user = userEvent.setup();
    renderPanel();
    await user.click(screen.getByRole('button', { name: 'Mark all seen' }));
    expect(useAgentStore.getState().seen[FX_FAILED.id]).toBe(true);
    const list = screen.getByRole('list', { name: 'Agents' });
    expect(within(list).getByText('Needs attention (1)')).toBeInTheDocument();
    expect(within(list).getByRole('button', { name: 'Ended (2)' })).toBeInTheDocument();
  });

  it('focuses its first row when focus is requested', () => {
    renderPanel();
    act(() => { useAgentStore.getState().requestFocus(); });
    expect(document.activeElement).toBe(rowButtons()[0]);
  });

  it('shows the first 10 done agents, then all on request', async () => {
    const user = userEvent.setup();
    const done = Array.from({ length: 12 }, (_, i) => makeAgentView({
      ...FX_COMPLETED, id: `ag_done${String(i).padStart(7, '0')}`, name: `done-${i}`, label: `done-${i}`,
      spawn_tool_use_id: `toolu_done${i}`,
    }));
    useAgentStore.setState(makeAgentStoreSeed(done));
    renderPanel();
    expect(rowButtons()).toHaveLength(10);
    await user.click(screen.getByRole('button', { name: 'Show all 12' }));
    expect(rowButtons()).toHaveLength(12);
  });

  it('has an empty state', () => {
    useAgentStore.setState(makeAgentStoreSeed([]));
    renderPanel();
    expect(screen.getByText('No agents in this session yet.')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Stop all…' })).toBeNull();
  });

  it('uses no tree, grid or tablist roles', () => {
    renderPanel();
    for (const role of ['tree', 'treeitem', 'grid', 'treegrid', 'row', 'tablist', 'tab']) {
      expect(screen.queryAllByRole(role)).toHaveLength(0);
    }
  });
});
