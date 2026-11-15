import { render, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * What a ```widget block of an answer saves is kept with the chat of its
 * message. A stored message names its chat itself (the server says it with
 * every row); the chat page names the chat on screen for what has no row
 * yet. A block learns it from the page around it, not from the chat's
 * store. (So a page without a chat, which shows text too, need not load
 * that store.)
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

const { useChatStore } = await import('../stores/chatStore');
const { resetMcpAppsConfig } = await import('../stores/mcpAppsStore');
const { resetWidgetChats } = await import('../components/Chat/widgets/storage');
const { ChatPage } = await import('./ChatPage');

const SESSION_ID = 'a1b2c3d4';
const ANSWER = 'Here it is.\n\n```widget\n<button id="more">Count</button>\n```\n';

beforeEach(() => {
  for (const key of Object.keys(mocks.calls)) delete mocks.calls[key];
  Object.assign(mocks.calls, {
    listSessions: vi.fn(async () => ({
      sessions: [{ id: SESSION_ID, title: 'A chat', source: 'web', updated_at: '2026-08-05T02:00:00Z', backend: 'claude' }],
      archived_count: 0, system_count: 0, has_more: false, next_offset: 1,
    })),
    getMessages: vi.fn(async () => ({
      messages: [
        { id: 1, role: 'user', content: 'Count for me.', created_at: '2026-08-05T02:00:00Z' },
        { id: 2, role: 'assistant', content: ANSWER, blocks: [{ type: 'text', content: ANSWER }], created_at: '2026-08-05T02:00:01Z' },
      ],
    })),
    getModifiedFiles: vi.fn(async () => ({ files: [], summary: {} })),
    listActors: vi.fn(async () => ({ actors: [] })),
    listTaskStatuses: vi.fn(async () => ({ statuses: [] })),
    getSessionTasks: vi.fn(async () => ({ tasks: [] })),
    getMcpAppsConfig: vi.fn(async () => ({ enabled: true })),
    getWidgetStates: vi.fn(async () => ({ widgets: [], limits: {} })),
  });
  useChatStore.setState({
    sessions: [], activeSession: '', messages: [], virtualSession: null, drafts: {},
    panels: [], activePanelId: null, panelVisible: false,
    sessionTasks: null, sessionTasksFailed: false,
    loadModels: vi.fn(async () => {}),
  });
  resetMcpAppsConfig();
  resetWidgetChats();
  window.history.pushState({}, '', `/chat/${SESSION_ID}`);
});

afterEach(() => {
  window.history.pushState({}, '', '/');
});

describe('the chat page and the widgets of an answer', () => {
  it('tells a widget block which chat it is in: the block asks for what that chat\'s widgets saved', async () => {
    render(
      <MemoryRouter initialEntries={[`/chat/${SESSION_ID}`]}>
        <Routes><Route path="/chat/:sessionId?" element={<ChatPage />} /></Routes>
      </MemoryRouter>,
    );

    await waitFor(() => expect(document.querySelector('[data-widget]')).not.toBeNull());
    await waitFor(() => expect(mocks.calls.getWidgetStates).toHaveBeenCalled());
    const asked = (mocks.calls.getWidgetStates as ReturnType<typeof vi.fn>).mock.calls.map(call => call[0]);
    expect(new Set(asked)).toEqual(new Set([SESSION_ID]));
  });

  it('keeps the widget of a message with the chat of that message, also when another chat is the one on screen', async () => {
    // Rows of another chat on this chat's page: the widget is the other
    // chat's, not this one's.
    (mocks.calls.getMessages as ReturnType<typeof vi.fn>).mockImplementation(async () => ({
      messages: [
        { id: 7, session_id: 'other-chat', role: 'assistant', content: ANSWER, blocks: [{ type: 'text', content: ANSWER }], created_at: '2026-08-05T02:00:01Z' },
      ],
    }));

    render(
      <MemoryRouter initialEntries={[`/chat/${SESSION_ID}`]}>
        <Routes><Route path="/chat/:sessionId?" element={<ChatPage />} /></Routes>
      </MemoryRouter>,
    );

    await waitFor(() => expect(document.querySelector('[data-widget]')).not.toBeNull());
    await waitFor(() => expect(mocks.calls.getWidgetStates).toHaveBeenCalled());
    expect(useChatStore.getState().activeSession).toBe(SESSION_ID);
    const asked = (mocks.calls.getWidgetStates as ReturnType<typeof vi.fn>).mock.calls.map(call => call[0]);
    expect(new Set(asked)).toEqual(new Set(['other-chat']));
  });
});
