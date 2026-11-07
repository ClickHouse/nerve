import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { SessionTask } from '../api/client';

/**
 * The chat header's tasks button: it shows while the session has tasks,
 * opens the Tasks tab of the side panel, and follows task broadcasts. The
 * header holds one tasks button and one context meter.
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

const { useChatStore, TASKS_PANEL_ID } = await import('../stores/chatStore');
const { ChatPage } = await import('./ChatPage');

const SESSION_ID = 'a1b2c3d4';
const session = {
  id: SESSION_ID, title: 'Triage the inbox', source: 'web', updated_at: '2026-08-05T02:00:00Z', backend: 'claude',
};

function task(id: string, title: string, relation: SessionTask['relation'] = 'created'): SessionTask {
  return {
    id, title, relation, status: 'pending', position: 1024, deadline: null,
    source: 'manual', source_url: null, tags: '',
    created_at: '2026-08-05T00:00:00Z', updated_at: '2026-08-05T00:00:00Z',
  };
}

const TWO = [task('t-1', 'Write the release notes'), task('t-2', 'Rotate the API keys', 'updated')];

let sessionTasks: SessionTask[];

beforeEach(() => {
  sessionTasks = [];
  Object.assign(mocks.calls, {
    listSessions: vi.fn(async () => ({
      sessions: [session], archived_count: 0, system_count: 0, has_more: false, next_offset: 1,
    })),
    getMessages: vi.fn(async () => ({
      messages: [{ id: 1, role: 'user', content: 'Sort the inbox.', created_at: '2026-08-05T02:00:00Z' }],
    })),
    getModifiedFiles: vi.fn(async () => ({ files: [], summary: {} })),
    listActors: vi.fn(async () => ({ actors: [] })),
    listTaskStatuses: vi.fn(async () => ({ statuses: [] })),
    getSessionTasks: vi.fn(async () => ({ tasks: sessionTasks })),
  });
  useChatStore.setState({
    sessions: [], activeSession: '', messages: [], virtualSession: null, drafts: {},
    panels: [], activePanelId: null, panelVisible: false,
    sessionTasks: null, sessionTasksFailed: false,
    loadModels: vi.fn(async () => {}),
  });
});

async function renderPage() {
  render(
    <MemoryRouter initialEntries={[`/chat/${SESSION_ID}`]}>
      <Routes><Route path="/chat/:sessionId?" element={<ChatPage />} /></Routes>
    </MemoryRouter>,
  );
  await waitFor(() => expect(mocks.calls.getSessionTasks).toHaveBeenCalledWith(SESSION_ID));
  await waitFor(() => expect(useChatStore.getState().sessionTasks).not.toBeNull());
}

const tasksButton = (count: number) =>
  screen.queryByRole('button', { name: `Tasks from this session (${count})` });

function broadcast(event: 'created' | 'updated' | 'moved' | 'done' | 'commented', t: SessionTask) {
  act(() => useChatStore.getState().handleWSMessage({ type: 'task_updated', session_id: null, event, task: t }));
}

describe('the tasks button in the chat header', () => {
  it('does not show for a session without tasks', async () => {
    await renderPage();

    expect(screen.queryByRole('button', { name: /Tasks from this session/ })).not.toBeInTheDocument();
  });

  it('shows the count of the session tasks', async () => {
    sessionTasks = TWO;
    await renderPage();

    const button = tasksButton(2);
    expect(button).toBeInTheDocument();
    expect(button).toHaveTextContent('2');
  });

  it('opens the Tasks tab of the side panel and takes the active colour', async () => {
    sessionTasks = TWO;
    await renderPage();
    expect(tasksButton(2)!.querySelector('svg')).not.toHaveClass('text-hue-blue');

    await userEvent.click(tasksButton(2)!);

    const state = useChatStore.getState();
    expect(state.panels.map((p) => p.id)).toEqual([TASKS_PANEL_ID]);
    expect(state.activePanelId).toBe(TASKS_PANEL_ID);
    expect(state.panelVisible).toBe(true);
    expect(await screen.findByRole('link', { name: /Rotate the API keys/ })).toHaveAttribute('href', '/tasks/t-2');
    expect(tasksButton(2)!.querySelector('svg')).toHaveClass('text-hue-blue');
  });

  it('reads the list again on a task change, but not on a comment', async () => {
    await renderPage();
    const getSessionTasks = vi.mocked(mocks.calls.getSessionTasks as () => Promise<unknown>);
    getSessionTasks.mockClear();

    sessionTasks = [task('t-3', 'Renew the certificate')];
    broadcast('created', sessionTasks[0]);
    expect(await screen.findByRole('button', { name: 'Tasks from this session (1)' })).toBeInTheDocument();
    expect(getSessionTasks).toHaveBeenCalledWith(SESSION_ID);

    getSessionTasks.mockClear();
    broadcast('commented', sessionTasks[0]);
    expect(getSessionTasks).not.toHaveBeenCalled();
  });
});

describe('the chat header with the context bar', () => {
  it('shows one context meter and one tasks button, also after a session switch', async () => {
    sessionTasks = TWO;
    const lastUsage = {
      input_tokens: 10, output_tokens: 400, cache_read_input_tokens: 60000,
      cache_creation_input_tokens: 6000, max_context_tokens: 200000, num_turns: 2,
    };
    mocks.calls.getMessages = vi.fn(async () => ({
      messages: [{ id: 1, role: 'user', content: 'Sort the inbox.', created_at: '2026-08-05T02:00:00Z' }],
      last_usage: lastUsage,
    }));
    await renderPage();
    const meters = () => screen.queryAllByRole('button', { name: /^Context window:/ });
    const tasksButtons = () => screen.queryAllByRole('button', { name: /^Tasks from this session/ });
    await waitFor(() => expect(meters()).toHaveLength(1));
    expect(tasksButtons()).toHaveLength(1);

    await act(() => useChatStore.getState().switchSession('e5f6a7b8'));
    await waitFor(() => expect(useChatStore.getState().activeSession).toBe('e5f6a7b8'));
    await waitFor(() => expect(meters()).toHaveLength(1));
    await waitFor(() => expect(tasksButtons()).toHaveLength(1));
  });
});

describe("the store's session tasks", () => {
  it('drops an answer that a newer read replaced', async () => {
    useChatStore.setState({ activeSession: SESSION_ID });
    const answers: ((value: { tasks: SessionTask[] }) => void)[] = [];
    mocks.calls.getSessionTasks = vi.fn(() => new Promise((r) => { answers.push(r); }));
    const { fetchSessionTasks } = useChatStore.getState();

    const first = fetchSessionTasks(SESSION_ID);
    const second = fetchSessionTasks(SESSION_ID);
    answers[1]({ tasks: TWO });
    answers[0]({ tasks: [] });
    await Promise.all([first, second]);

    expect(useChatStore.getState().sessionTasks).toEqual(TWO);
  });

  it('drops the answer for a session that is no longer open', async () => {
    useChatStore.setState({ activeSession: SESSION_ID });
    let answer: (value: { tasks: SessionTask[] }) => void = () => {};
    mocks.calls.getSessionTasks = vi.fn(() => new Promise((r) => { answer = r; }));

    const read = useChatStore.getState().fetchSessionTasks(SESSION_ID);
    useChatStore.setState({ activeSession: 'e5f6a7b8' });
    answer({ tasks: TWO });
    await read;

    expect(useChatStore.getState().sessionTasks).toBeNull();
  });

  it('does not ask the server about a new chat that has no message yet', async () => {
    const virtual = { id: 'virtual-1', title: 'New chat', updated_at: '2026-08-05T02:00:00Z' };
    useChatStore.setState({ activeSession: virtual.id, virtualSession: virtual as never });

    await useChatStore.getState().fetchSessionTasks(virtual.id);

    expect(mocks.calls.getSessionTasks).not.toHaveBeenCalled();
  });
});
