import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * The chat page of a session in the middle of an agent wave: the dock above
 * the composer, a live line at each spawn point, and the grouped Agents panel
 * one shortcut away.
 */

const mocks = vi.hoisted(() => ({ calls: {} as Record<string, unknown> }));

vi.mock('../api/client', async (orig) => ({
  ...(await orig<typeof import('../api/client')>()),
  api: new Proxy(mocks.calls, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));
vi.mock('../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(), disconnect: vi.fn(),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
    onMessage: vi.fn(() => () => {}), stopSession: vi.fn(), takePendingMessages: vi.fn(() => []),
  },
}));

const { useChatStore, AGENTS_PANEL_ID } = await import('../stores/chatStore');
const {
  FX_COMPLETED, FX_RUNNING, FX_SESSION_ID, FX_WAVE, makeSnapshot,
} = await import('../test/agentFixtures');
const { ChatPage } = await import('./ChatPage');
const { isMac } = await import('../utils/keyboard');

const session = {
  id: FX_SESSION_ID, title: 'Release wave', source: 'web', updated_at: '2026-02-27T02:00:00Z',
  backend: 'claude', is_running: false, background_count: 5, has_background_tasks: true,
};

function spawnCall(toolUseId: string, description: string) {
  return {
    type: 'tool_call', tool_use_id: toolUseId, tool: 'Agent',
    input: { description, subagent_type: 'general-purpose', prompt: `${description}.`, run_in_background: true },
    result: 'Async agent launched successfully.',
  };
}

beforeEach(() => {
  const fn = (impl: (...args: never[]) => unknown) => vi.fn(impl);
  Object.assign(mocks.calls, {
    listSessions: fn(async () => ({ sessions: [session], archived_count: 0, system_count: 0, has_more: false, next_offset: 1 })),
    getMessages: fn(async () => ({
      messages: [
        { id: 1, role: 'user', content: 'Launch the builders.', created_at: '2026-02-27T02:00:30Z' },
        {
          id: 2, role: 'assistant', created_at: '2026-02-27T02:01:00Z',
          blocks: [
            { type: 'text', content: 'Launching the builders.' },
            spawnCall(FX_RUNNING.spawn_tool_use_id!, 'Build the API'),
            spawnCall(FX_COMPLETED.spawn_tool_use_id!, 'Build the chart'),
          ],
        },
      ],
    })),
    getSessionAgents: fn(async () => makeSnapshot(FX_WAVE)),
    getModifiedFiles: fn(async () => ({ files: [], summary: {} })),
    listActors: fn(async () => ({ actors: [] })),
  });
  useChatStore.setState({
    sessions: [], activeSession: '', messages: [], virtualSession: null, drafts: {},
    panels: [], activePanelId: null, panelVisible: false,
    loadModels: vi.fn(async () => {}),
  });
});

async function renderPage() {
  const view = render(
    <MemoryRouter initialEntries={[`/chat/${FX_SESSION_ID}`]}>
      <Routes><Route path="/chat/:sessionId?" element={<ChatPage />} /></Routes>
    </MemoryRouter>,
  );
  await waitFor(() => expect(screen.getByRole('button', { name: /Agents$/ })).toBeInTheDocument());
  return view;
}

describe('ChatPage with a wave of agents', () => {
  it('shows the dock above the composer and a live line at each spawn point', async () => {
    const { container } = await renderPage();
    const dock = screen.getByRole('button', { name: /Agents$/ });
    expect(dock).toHaveTextContent('4 running');
    expect(dock).toHaveAttribute('aria-expanded', 'false');

    const running = container.querySelector<HTMLElement>(`[data-agent-spawn="${FX_RUNNING.spawn_tool_use_id}"]`)!;
    expect(within(running).getByText('build-api')).toBeInTheDocument();
    expect(within(running).getByRole('button', { name: 'Stop build-api' })).toBeInTheDocument();
    const done = container.querySelector<HTMLElement>(`[data-agent-spawn="${FX_COMPLETED.spawn_tool_use_id}"]`)!;
    expect(within(done).getByText('4 files changed')).toBeInTheDocument();
  });

  it('opens the grouped Agents panel', async () => {
    await renderPage();
    act(() => { useChatStore.getState().openAgentsPanel(); });
    const list = await screen.findByRole('list', { name: 'Agents' });
    expect(within(list).getAllByText(/\(\d+\)$/).map(el => el.textContent))
      .toEqual(['Needs attention (2)', 'Running (4)', 'Done (1)', 'Ended (1)']);
    expect(screen.getByRole('button', { name: /Agents$/ })).toHaveAttribute('aria-expanded', 'true');
  });

  it('toggles the Agents panel with Mod+Shift+E', async () => {
    await renderPage();
    const mod = isMac ? { metaKey: true } : { ctrlKey: true };
    fireEvent.keyDown(document.body, { key: 'e', shiftKey: true, ...mod });
    expect(useChatStore.getState().panelVisible).toBe(true);
    expect(useChatStore.getState().activePanelId).toBe(AGENTS_PANEL_ID);
    expect(await screen.findByRole('list', { name: 'Agents' })).toBeInTheDocument();
    // The shortcut puts focus in the list, on its first row.
    await waitFor(() => expect(document.activeElement?.getAttribute('data-agent-row')).toBeTruthy());

    fireEvent.keyDown(document.activeElement!, { key: 'e', shiftKey: true, ...mod });
    expect(useChatStore.getState().panelVisible).toBe(false);
    // Focus does not stay behind in the hidden panel.
    expect(document.activeElement).toBe(document.getElementById('nerve-chat-input'));
  });
});
