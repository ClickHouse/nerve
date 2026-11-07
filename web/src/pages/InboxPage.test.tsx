import { render, screen, waitFor, within } from '@testing-library/react';
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
let startChat: ReturnType<typeof vi.fn>;
let loadSessions: ReturnType<typeof vi.fn>;

beforeEach(() => {
  localStorage.clear();
  useSetupStore.getState().reset();
  // The store's own actions talk to the server; these stand in for them.
  createSession = vi.fn(async () => { useChatStore.setState({ activeSession: 'local-1' }); });
  startChat = vi.fn(async () => 'abc123');
  loadSessions = vi.fn(async () => {});
  useChatStore.setState({
    sessions: [
      session('asks', { title: 'Merge the docs PR?', attention: { questions: 1, approvals: 0, plans: 0, awaiting_input: false, review: false } }),
      session('quiet', { title: 'Lunch ideas' }),
    ],
    searchResults: null, archivedSessions: null, systemSessions: null,
    archivedCount: 0, systemCount: 0, drafts: {}, reads: {}, readsBaseline: 0,
    virtualSession: null, activeSession: '',
    loadSessions, createSession, startChat,
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
    // It renders the store's list; it does not fetch it again.
    expect(loadSessions).not.toHaveBeenCalled();
  });

  it('starts a new chat with the message and opens the chat it started', async () => {
    // Another chat became active meanwhile: the page still opens the new one.
    startChat.mockImplementation(async () => {
      useChatStore.setState({ activeSession: 'other' });
      return 'abc123';
    });
    renderInbox();
    const send = screen.getByRole('button', { name: 'Send' });
    expect(send).toBeDisabled();

    await userEvent.type(screen.getByRole('textbox', { name: 'Message Nerve' }), '  Plan my week  ');
    await userEvent.click(send);

    expect(startChat).toHaveBeenCalledWith('Plan my week');
    // Not the unsent new chat of the chat page, with its settings.
    expect(createSession).not.toHaveBeenCalled();
    await waitFor(() => expect(location()).toHaveTextContent('/chat/abc123'));
  });

  it('keeps the text and stays when the chat cannot be started', async () => {
    startChat.mockResolvedValue(null);
    renderInbox();
    const box = screen.getByRole('textbox', { name: 'Message Nerve' });

    await userEvent.type(box, 'Plan my week');
    await userEvent.click(screen.getByRole('button', { name: 'Send' }));

    await waitFor(() => expect(screen.getByRole('button', { name: 'Send' })).toBeEnabled());
    expect(startChat).toHaveBeenCalledTimes(1);
    expect(box).toHaveValue('Plan my week');
    expect(location()).toHaveTextContent('/inbox');
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
    expect(startChat).not.toHaveBeenCalled();
    await waitFor(() => expect(location()).toHaveTextContent('/chat/local-1'));
  });
});

describe('the Inbox on desktop', () => {
  it('opens the chat page, which has the same list next to the chat', () => {
    renderInbox();

    expect(location()).toHaveTextContent('/chat');
    expect(screen.queryByRole('heading', { name: 'Inbox' })).toBeNull();
  });
});
