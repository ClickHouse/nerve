import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from 'vitest';
import type { Session } from '../types/chat';

vi.mock('../api/client', async (orig) => ({
  ...(await orig<typeof import('../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= key === 'listActors'
      ? vi.fn(async () => ({ actors: [] }))
      : vi.fn(async () => ({}))),
  }),
}));
vi.mock('../api/websocket', () => ({
  ws: {
    switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(), disconnect: vi.fn(), onMessage: vi.fn(() => () => {}),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
  },
}));

const { MOBILE_QUERY } = await import('../hooks/useMediaQuery');
const { useChatStore } = await import('../stores/chatStore');
const { useSetupStore } = await import('../stores/setupStore');
const { makeState, step } = await import('../test/setupFixtures');
const { InboxPage } = await import('./InboxPage');

function session(id: string, extra: Partial<Session> = {}): Session {
  return {
    id, title: id, source: 'web', updated_at: new Date().toISOString(), is_running: false,
    attention: { questions: 0, approvals: 0, plans: 0, awaiting_input: false, review: false }, ...extra,
  };
}

/** Shows the current path, so a test can see where the page went. */
function LocationProbe() {
  return <output aria-label="Location">{useLocation().pathname}</output>;
}

function renderInbox() {
  return render(
    <MemoryRouter initialEntries={['/inbox']}>
      <Routes>
        <Route path="/inbox" element={<InboxPage />} />
        <Route path="/chat/:sessionId?" element={null} />
      </Routes>
      <LocationProbe />
    </MemoryRouter>,
  );
}

const location = () => screen.getByRole('status', { name: 'Location' });

let createSession: ReturnType<typeof vi.fn>;
let sendMessage: ReturnType<typeof vi.fn>;

beforeEach(() => {
  localStorage.clear();
  useSetupStore.getState().reset();
  // The store's own actions talk to the server; these stand in for them
  // with the same effect on `activeSession`.
  createSession = vi.fn(async () => { useChatStore.setState({ activeSession: 'local-1' }); });
  sendMessage = vi.fn(async () => { useChatStore.setState({ activeSession: 'abc123' }); });
  useChatStore.setState({
    sessions: [
      session('asks', { title: 'Merge the docs PR?', attention: { questions: 1, approvals: 0, plans: 0, awaiting_input: false, review: false } }),
      session('quiet', { title: 'Lunch ideas' }),
    ],
    searchResults: null, archivedSessions: null, systemSessions: null,
    archivedCount: 0, systemCount: 0, drafts: {}, reads: {}, readsBaseline: 0,
    virtualSession: null, activeSession: '',
    loadSessions: vi.fn(async () => {}),
    createSession, sendMessage,
  });
});

describe('the Inbox on a phone', () => {
  let matchMedia: MockInstance<typeof window.matchMedia>;
  beforeEach(() => {
    const stub = window.matchMedia;
    matchMedia = vi.spyOn(window, 'matchMedia').mockImplementation(
      (query: string) => ({ ...stub(query), matches: query === MOBILE_QUERY }),
    );
  });
  // Restore only this spy. vi.restoreAllMocks() also undoes setup.ts's spies.
  afterEach(() => matchMedia.mockRestore());

  it('shows the session list on the page, with the sessions that need you first', () => {
    renderInbox();

    expect(screen.getByRole('heading', { name: 'Inbox' })).toBeInTheDocument();
    // The list is the page, not a drawer.
    expect(screen.queryByRole('dialog', { name: 'Conversations' })).toBeNull();
    const group = screen.getByRole('button', { name: 'Needs you' }).parentElement!;
    expect(within(group).getByRole('link', { name: /Merge the docs PR\?/ })).toHaveAttribute('href', '/chat/asks');
  });

  it('starts a new chat with the message and opens it', async () => {
    renderInbox();
    const send = screen.getByRole('button', { name: 'Send' });
    expect(send).toBeDisabled();

    await userEvent.type(screen.getByRole('textbox', { name: 'Message Nerve' }), '  Plan my week  ');
    await userEvent.click(send);

    expect(createSession).toHaveBeenCalledTimes(1);
    expect(sendMessage).toHaveBeenCalledWith('Plan my week');
    expect(createSession.mock.invocationCallOrder[0]).toBeLessThan(sendMessage.mock.invocationCallOrder[0]);
    expect(location()).toHaveTextContent('/chat/abc123');
  });

  it('names the agent from the setup checklist', () => {
    useSetupStore.setState({
      state: makeState({ steps: [step('profile', {}, { agent_name: 'Ada' })] }),
    });

    renderInbox();

    expect(screen.getByRole('textbox', { name: 'Message Ada' })).toBeInTheDocument();
  });

  it('opens an empty new chat from the New chat button', async () => {
    renderInbox();

    await userEvent.click(screen.getByRole('button', { name: 'New chat' }));

    expect(createSession).toHaveBeenCalledTimes(1);
    expect(sendMessage).not.toHaveBeenCalled();
    expect(location()).toHaveTextContent('/chat/local-1');
  });
});

describe('the Inbox on desktop', () => {
  it('opens the chat page, which has the same list next to the chat', () => {
    renderInbox();

    expect(location()).toHaveTextContent('/chat');
    expect(screen.queryByRole('heading', { name: 'Inbox' })).toBeNull();
  });
});
