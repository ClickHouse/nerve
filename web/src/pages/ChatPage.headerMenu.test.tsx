import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * The chat header's "..." menu for a session that no loaded sidebar list
 * holds, for example an archived or system session opened from a task link.
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
const { ChatPage } = await import('./ChatPage');

const SESSION_ID = 'cron:daily-digest:20260101-080000-1a2b3c4d';

beforeEach(() => {
  Object.assign(mocks.calls, {
    // The session is in none of the lists that the sidebar loads.
    listSessions: vi.fn(async () => ({
      sessions: [], archived_count: 0, system_count: 1, has_more: false, next_offset: 0,
    })),
    getMessages: vi.fn(async () => ({
      messages: [{ id: 1, role: 'user', content: 'Write the digest.', created_at: '2026-01-01T08:00:00Z' }],
    })),
    getModifiedFiles: vi.fn(async () => ({ files: [], summary: {} })),
    listActors: vi.fn(async () => ({ actors: [] })),
    getSessionTasks: vi.fn(async () => ({ tasks: [] })),
  });
  useChatStore.setState({
    sessions: [], activeSession: '', messages: [], virtualSession: null, drafts: {},
    panels: [], activePanelId: null, panelVisible: false,
    loadModels: vi.fn(async () => {}),
  });
});

describe('ChatPage header menu', () => {
  it('shows for a session that no sidebar list holds and loads its tasks', async () => {
    render(
      <MemoryRouter initialEntries={[`/chat/${encodeURIComponent(SESSION_ID)}`]}>
        <Routes><Route path="/chat/:sessionId?" element={<ChatPage />} /></Routes>
      </MemoryRouter>,
    );
    await waitFor(() => expect(useChatStore.getState().activeSession).toBe(SESSION_ID));

    await userEvent.click(await screen.findByRole('button', { name: 'More actions' }));

    expect(mocks.calls.getSessionTasks).toHaveBeenCalledWith(SESSION_ID);
    expect(
      await screen.findByRole('button', { name: 'No tasks from this session' }),
    ).toBeInTheDocument();
  });
});
