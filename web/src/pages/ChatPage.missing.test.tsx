import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * The address of a session that the server does not have: a link that
 * outlived its session, or a session deleted in another tab. The page says
 * so in place of the messages and shows no composer, because a message for
 * that session has nowhere to go.
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

const { ws } = await import('../api/websocket');
const { useChatStore } = await import('../stores/chatStore');
const { ChatPage } = await import('./ChatPage');

const GONE = 'gone1234';
const EXISTING = 'a1b2c3d4';
const existing = {
  id: EXISTING, title: 'Triage the inbox', source: 'web', updated_at: '2026-08-05T02:00:00Z', backend: 'claude',
};

beforeEach(() => {
  vi.clearAllMocks();
  Object.assign(mocks.calls, {
    listSessions: vi.fn(async () => ({
      sessions: [existing], archived_count: 0, system_count: 0, has_more: false, next_offset: 1,
    })),
    // The server answers the messages of an unknown session with an empty list.
    getMessages: vi.fn(async () => ({ messages: [] })),
    getModifiedFiles: vi.fn(async () => ({ files: [], summary: {} })),
    listActors: vi.fn(async () => ({ actors: [] })),
    listTaskStatuses: vi.fn(async () => ({ statuses: [] })),
    getSessionTasks: vi.fn(async () => ({ tasks: [] })),
  });
  useChatStore.setState({
    sessions: [], activeSession: '', missingSession: null, messages: [], virtualSession: null, drafts: {},
    panels: [], activePanelId: null, panelVisible: false,
    sessionTasks: null, sessionTasksFailed: false,
    loadModels: vi.fn(async () => {}),
  });
});

async function open(path: string) {
  render(
    <MemoryRouter initialEntries={[path]}>
      <Routes><Route path="/chat/:sessionId?" element={<ChatPage />} /></Routes>
    </MemoryRouter>,
  );
  await waitFor(() => expect(mocks.calls.listSessions).toHaveBeenCalled());
  await waitFor(() => expect(useChatStore.getState().loading).toBe(false));
}

async function openSession(sessionId: string) {
  await open(`/chat/${sessionId}`);
  await waitFor(() => expect(ws.switchSession).toHaveBeenCalledWith(sessionId));
}

const notFound = (sessionId: string) =>
  act(() => useChatStore.getState().handleWSMessage({ type: 'session_not_found', session_id: sessionId }));

const notice = () => screen.queryByText('This session does not exist');
/** The block of the notice: its two lines and its button. */
const noticeBlock = () => within(notice()!.closest('[role="status"]') as HTMLElement);
const composer = () => screen.getByPlaceholderText(/Send a message/) as HTMLTextAreaElement;
/** The composer stays mounted and goes out of sight with the notice. */
const composerInSight = () => composer().closest('.hidden') === null;

describe('the address of a session that does not exist', () => {
  it('says that the session does not exist and shows no composer', async () => {
    await openSession(GONE);
    expect(notice()).not.toBeInTheDocument();
    expect(composerInSight()).toBe(true);

    notFound(GONE);

    expect(notice()).toBeInTheDocument();
    expect(noticeBlock().getByText(/It was deleted, or it was a new chat that was never sent\./)).toBeInTheDocument();
    expect(composerInSight()).toBe(false);
    // The header names no session and no backend of one.
    expect(screen.getByText('Session not found')).toBeInTheDocument();
    expect(screen.queryByTitle(/^Agent backend:/)).not.toBeInTheDocument();
  });

  it('starts a new chat from the notice', async () => {
    await openSession(GONE);
    notFound(GONE);

    await userEvent.click(noticeBlock().getByRole('button', { name: 'New chat' }));

    await waitFor(() => expect(notice()).not.toBeInTheDocument());
    expect(useChatStore.getState().activeSession).not.toBe(GONE);
    expect(composerInSight()).toBe(true);
    expect(composer().value).toBe('');
  });

  it('takes what was typed for the session into the new chat', async () => {
    await openSession(GONE);
    await userEvent.type(composer(), 'half a thought');

    notFound(GONE);
    await userEvent.click(noticeBlock().getByRole('button', { name: 'New chat' }));

    await waitFor(() => expect(composer().value).toBe('half a thought'));
    const { activeSession, drafts } = useChatStore.getState();
    expect(drafts[activeSession]).toBe('half a thought');
    expect(drafts[GONE] ?? '').toBe('');
    expect(composerInSight()).toBe(true);
  });

  it('leaves the page of an existing session alone', async () => {
    await openSession(EXISTING);

    notFound(GONE);

    expect(notice()).not.toBeInTheDocument();
    expect(composerInSight()).toBe(true);
  });

  it('is left for another session when the address names none', async () => {
    // Back on /chat from another page, with the missing session still active.
    useChatStore.setState({ activeSession: GONE, missingSession: GONE });

    await open('/chat');

    await waitFor(() => expect(useChatStore.getState().activeSession).toBe(EXISTING));
    expect(notice()).not.toBeInTheDocument();
    expect(composerInSight()).toBe(true);
  });
});
