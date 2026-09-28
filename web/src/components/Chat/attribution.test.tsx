import { act, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '../../api/client';
import type { ChatMessage } from '../../types/chat';

function installStorage(): void {
  const data = new Map<string, string>();
  const storage = {
    getItem: (k: string) => data.get(k) ?? null,
    setItem: (k: string, v: string) => void data.set(k, String(v)),
    removeItem: (k: string) => void data.delete(k),
    clear: () => data.clear(),
    key: (i: number) => [...data.keys()][i] ?? null,
    get length() { return data.size; },
  };
  for (const target of [globalThis, globalThis.window]) {
    if (target) Object.defineProperty(target, 'localStorage', {
      value: storage, configurable: true, writable: true,
    });
  }
}
installStorage();

const tokenStore = vi.hoisted(() => ({
  value: 'tok' as string | null,
  revision: 0,
}));

vi.mock('../../api/client', () => ({
  api: {
    listActors: vi.fn(),
    getViewer: vi.fn(),
    createSession: vi.fn(),
    runLater: vi.fn(),
    authStatus: vi.fn(async () => ({
      auth_required: true, login: 'password',
    })),
    checkAuth: vi.fn(async () => ({ authenticated: true })),
    login: vi.fn(async () => ({ token: 'tok' })),
  },
  getToken: vi.fn(() => tokenStore.value),
  setToken: vi.fn((token: string) => {
    tokenStore.value = token;
    tokenStore.revision += 1;
    return tokenStore.revision;
  }),
  clearToken: vi.fn((expectedRevision?: number) => {
    if (expectedRevision !== undefined && expectedRevision !== tokenStore.revision) {
      return false;
    }
    tokenStore.value = null;
    tokenStore.revision += 1;
    return true;
  }),
  setUnauthorizedHandler: vi.fn(),
}));
vi.mock('../../stores/helpers/draftStorage', async (orig) => ({
  ...(await orig<Record<string, unknown>>()), clearAllDrafts: vi.fn(),
}));
vi.mock('../../stores/helpers/readStorage', async (orig) => ({
  ...(await orig<Record<string, unknown>>()), clearAllReads: vi.fn(),
}));
vi.mock('../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(),
    send: vi.fn(), connect: vi.fn(),
  },
}));

const client = await import('../../api/client');
const { api } = client;
const setToken = client.setToken as unknown as ReturnType<typeof vi.fn>;
const clearToken = client.clearToken as unknown as ReturnType<typeof vi.fn>;
const { useActorStore } = await import('../../stores/actorStore');
const { useAuthStore, selfActorId } = await import('../../stores/authStore');
const { useChatStore } = await import('../../stores/chatStore');
const { handleUserMessage } = await import('../../stores/handlers/sessionHandlers');
const { MessageList } = await import('./MessageList');

const listActors = api.listActors as unknown as ReturnType<typeof vi.fn>;
const ALICE = 'actor-alice';
const BOB = 'actor-bob';
const SYSTEM = 'actor-system';

function actorRef(id: string, overrides: Partial<ActorRef> = {}): ActorRef {
  return { id, kind: 'human', display_name: null, ...overrides } as ActorRef;
}

const alice = (name: string | null = 'Alice') => actorRef(ALICE, { display_name: name });
const bob = (name: string | null = 'Bob') => actorRef(BOB, { display_name: name });
const system = () => actorRef(SYSTEM, { kind: 'system', display_name: null });

function said(text: string, actor_id: string | null, id = 1): ChatMessage {
  return { id, role: 'user', blocks: [{ type: 'text', content: text }], actor_id };
}

function renderTranscript(messages: ChatMessage[]) {
  return render(<MessageList messages={messages} streamingBlocks={[]} isStreaming={false} />);
}

function labels(): string[] {
  return [...document.querySelectorAll('[data-attribution="message"]')]
    .map((el) => el.textContent ?? '');
}

function signInAs(actorId: string): void {
  useAuthStore.setState({
    viewer: actorRef(actorId),
    account: { id: `account-${actorId}`, username: null },
  });
}

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let release!: (value: T) => void;
  const promise = new Promise<T>((resolve) => { release = resolve; });
  return { promise, resolve: release };
}

function viewerFor(actorId: string, id = 'acc') {
  return {
    actor: actorRef(actorId),
    account: {
      id, actor_id: actorId, username: 'somebody', display_name: null,
      enabled: true, has_password: true, created_at: 't',
    },
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  useActorStore.getState().reset();
  useAuthStore.setState({
    viewer: null, account: null, authenticated: false, ready: true, loading: false,
    sessionExpired: false, error: null,
  });
  tokenStore.value = 'tok';
  tokenStore.revision = 0;
  useChatStore.setState({ messages: [], activeSession: '', virtualSession: null });
  listActors.mockResolvedValue({ actors: [alice(), bob(), system()] });
  (api.getViewer as unknown as ReturnType<typeof vi.fn>)
    .mockRejectedValue(new Error('401: no session'));
});

describe('viewer-relative transcript labels', () => {
  it('shows Alice throughout her history to Bob, while null and assistant rows stay quiet', async () => {
    signInAs(BOB);
    const { container } = renderTranscript([
      said('one', ALICE, 1),
      { id: 2, role: 'assistant', blocks: [{ type: 'text', content: 'answer' }] },
      said('legacy', null, 3),
      said('two', ALICE, 4),
    ]);

    await waitFor(() => expect(labels()).toEqual(['Alice', 'Alice']));
    expect(container.querySelector('[data-role="assistant"] [data-attribution]')).toBeNull();
    expect(screen.getByText('legacy')).toBeInTheDocument();
  });

  it('keeps the viewer\'s own history quiet', async () => {
    signInAs(ALICE);
    renderTranscript([said('one', ALICE, 1), said('two', ALICE, 2)]);

    await waitFor(() => expect(listActors).toHaveBeenCalled());
    expect(labels()).toEqual([]);
  });

  it('always names Nerve and explains its bot glyph', async () => {
    signInAs(ALICE);
    const { container } = renderTranscript([said('scheduled', SYSTEM)]);

    expect(await screen.findByText('Nerve')).toBeInTheDocument();
    expect(screen.getByTitle('Sent by Nerve itself — scheduled or autonomous work'))
      .toBeInTheDocument();
    expect(container.querySelector('[data-attribution] svg')).toHaveAttribute('aria-hidden', 'true');
  });

  it('renders a missing human name neutrally without exposing the id as text', async () => {
    listActors.mockResolvedValue({ actors: [alice(null), bob()] });
    signInAs(BOB);
    renderTranscript([said('still readable', ALICE)]);

    expect(await screen.findByText('Unnamed account')).toBeInTheDocument();
    expect(screen.queryByText(ALICE)).toBeNull();
    expect(screen.getByTitle(`Sent by actor ${ALICE}`)).toBeInTheDocument();
    expect(screen.getByText('still readable')).toBeInTheDocument();
  });

  it('uses visible collision-safe suffixes when Bob and Alice have equal names', async () => {
    const first = '0199aaaa-1111-7000-8000-00000a0000ab';
    const second = '0199aaaa-1111-7000-8000-00000b0000ab';
    listActors.mockResolvedValue({ actors: [
      actorRef(first, { display_name: 'Alex' }),
      actorRef(second, { display_name: 'Alex' }),
    ] });
    signInAs(second);
    renderTranscript([said('Alice-only history', first)]);

    expect(await screen.findByText('Alex (a0000ab)')).toBeInTheDocument();
    expect(screen.getByTitle(`Sent by Alex (a0000ab) — actor ${first}`)).toBeInTheDocument();
  });
});

describe('live and optimistic attribution', () => {
  it('stamps the optimistic row but labels only the other person\'s live echo', async () => {
    signInAs(ALICE);
    useChatStore.setState({ messages: [], activeSession: 's1', virtualSession: null });
    await act(async () => { await useChatStore.getState().sendMessage('ship it?'); });
    act(() => handleUserMessage(
      { type: 'user_message', session_id: 's1', content: 'not yet', actor_id: BOB },
      useChatStore.getState, useChatStore.setState,
    ));

    const messages = useChatStore.getState().messages;
    expect(messages.map((message) => message.actor_id)).toEqual([ALICE, BOB]);
    expect(JSON.stringify(messages[0])).not.toContain('Alice');
    renderTranscript(messages);
    await waitFor(() => expect(labels()).toEqual(['Bob']));
  });

  it('still sends through the null legacy path when no account identity exists', async () => {
    useChatStore.setState({ messages: [], activeSession: 's1', virtualSession: null });
    await act(async () => { await useChatStore.getState().sendMessage('ship it?'); });

    expect(useChatStore.getState().messages[0].actor_id).toBeNull();
    renderTranscript(useChatStore.getState().messages);
    expect(labels()).toEqual([]);
    expect(listActors).not.toHaveBeenCalled();
  });
});

describe('authentication generations', () => {
  it('never installs a login token after logout ends its session', async () => {
    const slowLogin = deferred<{ token: string }>();
    (api.login as unknown as ReturnType<typeof vi.fn>).mockReturnValueOnce(slowLogin.promise);
    let login!: Promise<void>;
    act(() => { login = useAuthStore.getState().login('pw', 'alice'); });
    act(() => useAuthStore.getState().logout());

    await act(async () => {
      slowLogin.resolve({ token: 'late-token' });
      await login;
    });

    expect(setToken).not.toHaveBeenCalledWith('late-token');
    expect(selfActorId()).toBeNull();
    expect(useAuthStore.getState().loading).toBe(false);
  });

  it('does not let a stale login clear a newer byte-identical token', async () => {
    const slowAlice = deferred<ReturnType<typeof viewerFor>>();
    (api.login as unknown as ReturnType<typeof vi.fn>)
      .mockResolvedValueOnce({ token: 'same-token' })
      .mockResolvedValueOnce({ token: 'same-token' });
    (api.getViewer as unknown as ReturnType<typeof vi.fn>)
      .mockReturnValueOnce(slowAlice.promise)
      .mockResolvedValueOnce(viewerFor(BOB, 'acc-bob'));

    let aliceLogin!: Promise<void>;
    act(() => { aliceLogin = useAuthStore.getState().login('pw', 'alice'); });
    await act(async () => { await Promise.resolve(); });
    act(() => useAuthStore.getState().logout());
    await act(async () => { await useAuthStore.getState().login('pw', 'bob'); });
    clearToken.mockClear();

    await act(async () => {
      slowAlice.resolve(viewerFor(ALICE, 'acc-alice'));
      await aliceLogin;
    });

    expect(clearToken).toHaveBeenCalledWith(1);
    expect(tokenStore.value).toBe('same-token');
    expect(selfActorId()).toBe(BOB);
  });
});

describe('deferred and renamed rows', () => {
  it('binds run-later to whoever requested it', async () => {
    signInAs(ALICE);
    (api.createSession as unknown as ReturnType<typeof vi.fn>)
      .mockResolvedValue({ id: 'later-1', title: '', source: 'web', updated_at: 't' });
    (api.runLater as unknown as ReturnType<typeof vi.fn>)
      .mockResolvedValue({ ack: 'Scheduled.' });

    await act(async () => { await useChatStore.getState().runLater('sweep at 9', '1h'); });

    expect(useChatStore.getState().messages[0].actor_id).toBe(ALICE);
  });

  it('abandons a run-later when logout supersedes its viewer', async () => {
    signInAs(ALICE);
    const scheduled = deferred<{ ack: string }>();
    (api.createSession as unknown as ReturnType<typeof vi.fn>)
      .mockResolvedValue({ id: 'later-1', title: '', source: 'web', updated_at: 't' });
    (api.runLater as unknown as ReturnType<typeof vi.fn>).mockReturnValue(scheduled.promise);
    let later!: Promise<void>;
    act(() => { later = useChatStore.getState().runLater('sweep', '1h'); });
    await act(async () => { await Promise.resolve(); });
    act(() => useAuthStore.getState().logout());

    await act(async () => {
      scheduled.resolve({ ack: 'Scheduled.' });
      await later;
    });
    expect(useChatStore.getState().messages).toHaveLength(0);
  });

  it('refreshes a current name without rewriting the message', async () => {
    signInAs(BOB);
    const message = said('one', ALICE);
    const before = structuredClone(message);
    renderTranscript([message]);
    expect(await screen.findByText('Alice')).toBeInTheDocument();

    listActors.mockResolvedValue({ actors: [alice('Alice Doe'), bob(), system()] });
    await act(() => useActorStore.getState().refresh());

    expect(await screen.findByText('Alice Doe')).toBeInTheDocument();
    expect(message).toEqual(before);
  });
});
