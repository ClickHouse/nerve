import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes, useLocation, type Location } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { SessionTask } from '../../api/client';

/**
 * The Tasks tab of the chat side panel: its states, and a task that opens
 * in the task modal over the chat.
 */

const mocks = vi.hoisted(() => ({ calls: {} as Record<string, unknown> }));

vi.mock('../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../api/client')>()),
  api: new Proxy(mocks.calls, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));
vi.mock('../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
  },
}));

const { useChatStore, TASKS_PANEL_ID } = await import('../../stores/chatStore');
const { useTaskStatusStore } = await import('../../stores/taskStatusStore');
const { SidePanel } = await import('./SidePanel');

const SESSION_ID = 'a1b2c3d4';

function task(id: string, title: string, relation: SessionTask['relation'], status = 'pending'): SessionTask {
  return {
    id, title, relation, status, position: 1024, deadline: null,
    source: 'manual', source_url: null, tags: '',
    created_at: '2026-08-05T00:00:00Z', updated_at: '2026-08-05T00:00:00Z',
  };
}

const TASKS = [
  task('2026-08-05-write-the-release-notes', 'Write the release notes', 'created'),
  task('2026-08-04-rotate-the-api-keys', 'Rotate the API keys', 'updated', 'in_progress'),
];

/** The app's background-location routing (App.tsx), with the panel as the chat page. */
function Shell() {
  const location = useLocation();
  const background = (location.state as { background?: Location } | null)?.background;
  return (
    <>
      <Routes location={background ?? location}>
        <Route path="/chat/:sessionId" element={<SidePanel />} />
        <Route path="/tasks/:taskId" element={<p>Task page</p>} />
      </Routes>
      {background && (
        <Routes>
          <Route path="/tasks/:taskId" element={<p>Task modal</p>} />
        </Routes>
      )}
    </>
  );
}

/** Open the Tasks tab the way the header button does, with the list as `result` gives it. */
async function openTab(result: () => Promise<{ tasks: SessionTask[] }>) {
  mocks.calls.getSessionTasks = vi.fn(result);
  const view = render(
    <MemoryRouter initialEntries={[`/chat/${SESSION_ID}`]}>
      <Shell />
    </MemoryRouter>,
  );
  await act(async () => useChatStore.getState().openTasksPanel());
  return view;
}

beforeEach(() => {
  mocks.calls.listTaskStatuses = vi.fn(async () => ({
    statuses: [
      { name: 'pending', label: 'Pending', color: '#888888', sort_order: 0 },
      { name: 'in_progress', label: 'In progress', color: '#3b82f6', sort_order: 1 },
    ],
  }));
  useTaskStatusStore.setState({ statuses: [], loaded: false, loading: false });
  useChatStore.setState({
    activeSession: SESSION_ID, virtualSession: null,
    panels: [], activePanelId: null, panelVisible: false,
    sessionTasks: null, sessionTasksFailed: false,
  });
});

describe('the Tasks tab', () => {
  it('lists each task with what the session did and its status', async () => {
    await openTab(async () => ({ tasks: TASKS }));

    expect(mocks.calls.getSessionTasks).toHaveBeenCalledWith(SESSION_ID);
    expect(useChatStore.getState().activePanelId).toBe(TASKS_PANEL_ID);
    expect(screen.getByText('2 tasks')).toBeInTheDocument();
    const links = screen.getAllByRole('link');
    expect(links.map((a) => a.getAttribute('href'))).toEqual([
      '/tasks/2026-08-05-write-the-release-notes',
      '/tasks/2026-08-04-rotate-the-api-keys',
    ]);
    expect(within(links[0]).getByText('Created')).toBeInTheDocument();
    expect(within(links[1]).getByText('Updated')).toBeInTheDocument();
    expect(await within(links[1]).findByText('In progress')).toBeInTheDocument();
  });

  it('shows the loading state until the list arrives', async () => {
    let resolve: (value: { tasks: SessionTask[] }) => void = () => {};
    await openTab(() => new Promise((r) => { resolve = r; }));

    expect(screen.getByText('Loading tasks...')).toBeInTheDocument();

    await act(async () => resolve({ tasks: TASKS.slice(0, 1) }));
    expect(screen.queryByText('Loading tasks...')).not.toBeInTheDocument();
    expect(screen.getByText('1 task')).toBeInTheDocument();
  });

  it('shows the empty state', async () => {
    await openTab(async () => ({ tasks: [] }));

    expect(screen.getByText('No tasks from this session')).toBeInTheDocument();
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
  });

  it('shows the error state, and Try again reads the list again', async () => {
    await openTab(async () => { throw new Error('500'); });

    const alert = screen.getByRole('alert');
    expect(alert).toHaveTextContent('The tasks did not load.');

    vi.mocked(mocks.calls.getSessionTasks as () => Promise<{ tasks: SessionTask[] }>)
      .mockResolvedValue({ tasks: TASKS });
    await userEvent.click(within(alert).getByRole('button', { name: 'Try again' }));

    expect(mocks.calls.getSessionTasks).toHaveBeenCalledTimes(2);
    expect(await screen.findByText('Write the release notes')).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('opens a task in the task modal over the chat, with the panel kept', async () => {
    await openTab(async () => ({ tasks: TASKS }));

    await userEvent.click(screen.getByRole('link', { name: /Rotate the API keys/ }));

    expect(screen.getByText('Task modal')).toBeInTheDocument();
    expect(screen.queryByText('Task page')).not.toBeInTheDocument();
    // The chat (here its panel) stays mounted behind the modal.
    expect(screen.getByRole('link', { name: /Write the release notes/ })).toBeInTheDocument();
    expect(useChatStore.getState().panelVisible).toBe(true);
  });
});
