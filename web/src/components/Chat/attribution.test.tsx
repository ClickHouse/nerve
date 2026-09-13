import { act, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '../../api/client';
import type { ChatMessage } from '../../types/chat';

/**
 * What attribution looks like on a transcript.
 *
 * The rule these specs pin down is that a label earns its place only when it
 * tells two things apart: the agent's own principal always does, and a person
 * does once a second person has spoken. Everything else renders exactly as it
 * did before attribution existed — which is not a nicety, it is most of the
 * screen. Every assistant row, every tool row and all history from before the
 * columns existed carry no actor, and turning those into "unknown user" chips
 * would be a worse UI than having no attribution at all.
 *
 * The other half is that no name is ever stored. A rename has to change every
 * label here while leaving the message objects byte for byte identical, which
 * is asserted directly rather than inferred.
 */

// The chat store reads localStorage at module init and Node 25 injects an inert
// global that shadows jsdom's (see chatStore.test.ts) — install a real one
// before the dynamic imports below.
function installStorage(): void {
  const data = new Map<string, string>();
  const storage = {
    getItem: (k: string) => (data.has(k) ? data.get(k)! : null),
    setItem: (k: string, v: string) => void data.set(k, String(v)),
    removeItem: (k: string) => void data.delete(k),
    clear: () => data.clear(),
    key: (i: number) => [...data.keys()][i] ?? null,
    get length() { return data.size; },
  };
  for (const target of [globalThis, globalThis.window]) {
    if (target) Object.defineProperty(target, 'localStorage', { value: storage, configurable: true, writable: true });
  }
}
installStorage();

/**
 * Token storage, for real.
 *
 * Whose token is in storage is the thing under test in two of these specs —
 * a stale sign-in must take back its own and leave a newer one alone — and a
 * `getToken` that always answers the same string would make both pass without
 * meaning anything.
 */
const tokenStore = vi.hoisted(() => ({ value: 'tok' as string | null }));

vi.mock('../../api/client', () => ({
  api: {
    listActors: vi.fn(),
    getActor: vi.fn(),
    getOwnAccount: vi.fn(),
    createSession: vi.fn(),
    runLater: vi.fn(),
    authStatus: vi.fn(async () => ({
      auth_required: true, mode: 'local', login: 'password',
      setup_pending: false, multiple_accounts: true,
    })),
    checkAuth: vi.fn(async () => ({ authenticated: true })),
    login: vi.fn(async () => ({ token: 'tok' })),
  },
  getToken: vi.fn(() => tokenStore.value),
  setToken: vi.fn((token: string) => { tokenStore.value = token; }),
  clearToken: vi.fn(() => { tokenStore.value = null; }),
  setUnauthorizedHandler: vi.fn(),
}));
vi.mock('../../stores/helpers/draftStorage', async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  clearAllDrafts: vi.fn(),
}));
vi.mock('../../stores/helpers/readStorage', async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  clearAllReads: vi.fn(),
}));
vi.mock('../../api/websocket', () => ({
  ws: { sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn() },
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

/** A response this test decides when to deliver. */
function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let release!: (value: T) => void;
  const promise = new Promise<T>((r) => { release = r; });
  return { promise, resolve: release };
}

const ALICE = 'actor-alice';
const BOB = 'actor-bob';
const SYSTEM = 'actor-system';

function actorRef(id: string, overrides: Partial<ActorRef> = {}): ActorRef {
  return { id, kind: 'human', display_name: null, profile_version: 1, ...overrides };
}

const alice = (name: string | null = 'Alice') => actorRef(ALICE, { display_name: name });
const bob = (name: string | null = 'Bob') => actorRef(BOB, { display_name: name });
const system = (name: string | null = 'Nerve') =>
  actorRef(SYSTEM, { kind: 'system', display_name: name });

function said(text: string, actor_id: string | null, id = 1): ChatMessage {
  return { id, role: 'user', blocks: [{ type: 'text', content: text }], actor_id };
}

function replied(text: string, id = 2): ChatMessage {
  return { id, role: 'assistant', blocks: [{ type: 'text', content: text }] };
}

function renderTranscript(messages: ChatMessage[]) {
  return render(
    <MessageList messages={messages} streamingBlocks={[]} isStreaming={false} />,
  );
}

/** Every attribution label currently on screen, in DOM order. */
function labels(): string[] {
  return [...document.querySelectorAll('[data-attribution]')]
    .map((el) => el.textContent ?? '');
}

beforeEach(() => {
  vi.clearAllMocks();
  useActorStore.getState().reset();
  useAuthStore.setState({ account: null, loading: false, sessionExpired: false });
  tokenStore.value = 'tok';
  useChatStore.setState({ messages: [], activeSession: '', virtualSession: null });
  listActors.mockResolvedValue({ actors: [alice(), bob(), system()] });
  (api.getOwnAccount as unknown as ReturnType<typeof vi.fn>)
    .mockRejectedValue(new Error('403: no account'));
});

describe('a message with a sender', () => {
  it('is named once a second person has spoken', async () => {
    renderTranscript([said('first', ALICE, 1), said('second', BOB, 2)]);

    expect(await screen.findByText('Alice')).toBeInTheDocument();
    expect(screen.getByText('Bob')).toBeInTheDocument();
  });

  it('names the sender even on the messages that came before the second person', async () => {
    // The earlier messages were not ambiguous when they were sent. They are
    // now, so they get names too — the label describes the transcript's
    // current state, not the state at the time of writing.
    renderTranscript([
      said('one', ALICE, 1), said('two', ALICE, 2), said('three', BOB, 3),
    ]);

    await waitFor(() => expect(labels()).toEqual(['Alice', 'Alice', 'Bob']));
  });

  it('is not named when one person is talking to themselves', async () => {
    renderTranscript([said('one', ALICE, 1), said('two', ALICE, 2)]);

    await waitFor(() => expect(listActors).toHaveBeenCalled());
    expect(labels()).toEqual([]);
    expect(screen.queryByText('Alice')).toBeNull();
  });

  it('shows a display name it does not have as the neutral fallback', async () => {
    listActors.mockResolvedValue({ actors: [alice(null), bob()] });
    renderTranscript([said('one', ALICE, 1), said('two', BOB, 2)]);

    expect(await screen.findByText('Unnamed account')).toBeInTheDocument();
    // Never the raw id in the label itself...
    expect(screen.queryByText(ALICE)).toBeNull();
    // ...but it is in the tooltip, which is what tells "this account has no
    // name" apart from "this id is from somewhere else".
    expect(screen.getByTitle(`Sent by actor ${ALICE}`)).toBeInTheDocument();
  });

  it('renders an id the server has never heard of rather than hiding the message', async () => {
    // Two namespaces in one list is a legitimate state after a later move to
    // external identity: the same person is two actors and one of them is not
    // in this instance's table at all.
    listActors.mockResolvedValue({ actors: [alice()] });
    renderTranscript([said('mine', ALICE, 1), said('theirs', 'actor-elsewhere', 2)]);

    expect(await screen.findByText('Alice')).toBeInTheDocument();
    expect(screen.getByText('Unnamed account')).toBeInTheDocument();
    // The message itself is still there — a name that cannot be resolved must
    // never cost the reader the content.
    expect(screen.getByText('theirs')).toBeInTheDocument();
  });

  it('asks for an unknown id once and does not spin on it', async () => {
    listActors.mockResolvedValue({ actors: [alice()] });
    renderTranscript([said('mine', ALICE, 1), said('theirs', 'actor-elsewhere', 2)]);

    await screen.findByText('Unnamed account');
    await new Promise((r) => setTimeout(r, 20));
    expect(listActors.mock.calls.length).toBeLessThanOrEqual(2);
  });
});

describe('the agent itself', () => {
  it('is named on a message it sent, even with no person to compare it to', async () => {
    renderTranscript([said('run the sweep', SYSTEM, 1)]);

    expect(await screen.findByText('Nerve')).toBeInTheDocument();
  });

  it('says what it is, not just who it is', async () => {
    renderTranscript([said('run the sweep', SYSTEM, 1)]);

    await screen.findByText('Nerve');
    expect(
      screen.getByTitle('Sent by Nerve itself — scheduled or autonomous work'),
    ).toBeInTheDocument();
  });

  it('carries a glyph as well as the wording, marked decorative', async () => {
    const { container } = renderTranscript([said('run the sweep', SYSTEM, 1)]);

    await screen.findByText('Nerve');
    const label = container.querySelector('[data-attribution="message"]')!;
    const glyph = label.querySelector('svg')!;
    // The name is right beside it, so the glyph adds nothing for a screen
    // reader and must not be announced.
    expect(glyph).toHaveAttribute('aria-hidden', 'true');
  });

  it('falls back to its own name when bootstrap gave it none', async () => {
    listActors.mockResolvedValue({ actors: [system(null)] });
    renderTranscript([said('run the sweep', SYSTEM, 1)]);

    expect(await screen.findByText('Nerve')).toBeInTheDocument();
  });
});

describe('a message with no sender', () => {
  it('renders exactly as it always has — no chip, no placeholder', async () => {
    renderTranscript([said('old message', null, 1), said('another', null, 2)]);

    await new Promise((r) => setTimeout(r, 20));
    expect(labels()).toEqual([]);
    expect(screen.queryByText('Unnamed account')).toBeNull();
    expect(screen.getByText('old message')).toBeInTheDocument();
  });

  it('asks the server for nothing at all', async () => {
    renderTranscript([said('old message', null, 1), replied('an answer', 2)]);

    await new Promise((r) => setTimeout(r, 20));
    expect(listActors).not.toHaveBeenCalled();
  });

  it('stays unnamed while the people around it are named', async () => {
    renderTranscript([
      said('history', null, 1), said('one', ALICE, 2), said('two', BOB, 3),
    ]);

    await waitFor(() => expect(labels()).toEqual(['Alice', 'Bob']));
    expect(screen.getByText('history')).toBeInTheDocument();
  });
});

describe('two people with the same display name', () => {
  const ALEX_1 = '0199aaaa-1111-7000-8000-0000000000ab';
  const ALEX_2 = '0199aaaa-1111-7000-8000-0000000000cd';

  const bothAlexes = [
    actorRef(ALEX_1, { display_name: 'Alex' }),
    actorRef(ALEX_2, { display_name: 'Alex' }),
  ];

  it('are told apart in the label, not only in the tooltip', async () => {
    listActors.mockResolvedValue({ actors: bothAlexes });
    renderTranscript([said('one', ALEX_1, 1), said('two', ALEX_2, 2)]);

    // A phone has no hover, so the discriminator has to be visible text.
    await waitFor(() => expect(labels()).toEqual(['Alex (0000ab)', 'Alex (0000cd)']));
  });

  it('keep the full id in the tooltip for whoever needs to be sure', async () => {
    listActors.mockResolvedValue({ actors: bothAlexes });
    renderTranscript([said('one', ALEX_1, 1), said('two', ALEX_2, 2)]);

    await screen.findByText('Alex (0000ab)');
    expect(screen.getByTitle(`Sent by Alex (0000ab) — actor ${ALEX_1}`)).toBeInTheDocument();
  });

  it('leave an unshared name completely alone', async () => {
    listActors.mockResolvedValue({
      actors: [actorRef(ALEX_1, { display_name: 'Alex' }), bob()],
    });
    renderTranscript([said('one', ALEX_1, 1), said('two', BOB, 2)]);

    await waitFor(() => expect(labels()).toEqual(['Alex', 'Bob']));
  });

  it('tell two nameless accounts apart as well', async () => {
    listActors.mockResolvedValue({
      actors: [actorRef(ALEX_1), actorRef(ALEX_2)],
    });
    renderTranscript([said('one', ALEX_1, 1), said('two', ALEX_2, 2)]);

    await waitFor(() => expect(labels()).toEqual([
      'Unnamed account (0000ab)', 'Unnamed account (0000cd)',
    ]));
  });
});

describe('assistant rows', () => {
  it('carry no attribution markup anywhere inside them', async () => {
    const { container } = renderTranscript([
      said('ask', ALICE, 1), replied('answer', 2), said('ask again', BOB, 3),
    ]);

    await waitFor(() => expect(labels()).toEqual(['Alice', 'Bob']));
    // Both people are named, so this is the loudest attribution gets — and the
    // assistant turn between them still has nothing on it.
    const assistant = container.querySelector('[data-role="assistant"]')!;
    expect(assistant.querySelectorAll('[data-attribution]')).toHaveLength(0);
    expect(assistant.textContent).not.toContain('Alice');
    expect(assistant.textContent).not.toContain('Nerve');
  });
});

/**
 * The gate, as a user meets it: two people in one session, right now, with no
 * reload anywhere.
 *
 * This is the case the parts conspire to break. The gateway excludes a sender
 * from its own echo, so each tab holds one message it created locally and one
 * that arrived over the socket. If the local one is unattributed, each
 * transcript contains exactly one actor id, the visibility rule reads that as
 * one person, and *neither* tab shows a label — the two-simultaneous-humans
 * requirement fails while every individual piece looks correct.
 */
describe('two people, live, in one session', () => {
  /** A tab signed in as `me`, with `activeSession` open. */
  function tab(me: string) {
    useAuthStore.setState({ account: { id: 'acc', username: null, actor_id: me } });
    useChatStore.setState({ messages: [], activeSession: 's1', virtualSession: null });
  }

  /** The socket event the *other* tab's message arrives as. */
  function echo(from: string, content: string) {
    handleUserMessage(
      { type: 'user_message', session_id: 's1', content, actor_id: from },
      useChatStore.getState,
      useChatStore.setState,
    );
  }

  it('labels both bubbles in the tab that spoke first', async () => {
    tab(ALICE);
    await act(async () => { await useChatStore.getState().sendMessage('ship it?'); });
    echo(BOB, 'not yet');

    renderTranscript(useChatStore.getState().messages);

    await waitFor(() => expect(labels()).toEqual(['Alice', 'Bob']));
  });

  it('labels both bubbles in the tab that answered', async () => {
    tab(BOB);
    echo(ALICE, 'ship it?');
    await act(async () => { await useChatStore.getState().sendMessage('not yet'); });

    renderTranscript(useChatStore.getState().messages);

    await waitFor(() => expect(labels()).toEqual(['Alice', 'Bob']));
  });

  it('stamps your own message with your actor, not a name', async () => {
    tab(ALICE);
    await act(async () => { await useChatStore.getState().sendMessage('ship it?'); });

    const [mine] = useChatStore.getState().messages;
    expect(mine.actor_id).toBe(ALICE);
    // The id and nothing else — a name here would be the stored-snapshot bug
    // this whole branch exists to avoid.
    expect(JSON.stringify(mine)).not.toContain('Alice');
  });

  it('leaves your own message unattributed when the actor could not be read', async () => {
    // A caller with no account row — the agent's own principal, an MCP token —
    // gets a 403 from /api/accounts, which is the ordinary null path.
    tab(ALICE);
    useAuthStore.setState({ account: null, loading: false, sessionExpired: false });
  tokenStore.value = 'tok';
    await act(async () => { await useChatStore.getState().sendMessage('ship it?'); });

    expect(useChatStore.getState().messages[0].actor_id).toBeNull();
    renderTranscript(useChatStore.getState().messages);
    await new Promise((r) => setTimeout(r, 20));
    expect(labels()).toEqual([]);
  });

  it('does not label a conversation you are having with yourself', async () => {
    tab(ALICE);
    await act(async () => { await useChatStore.getState().sendMessage('one'); });
    await act(async () => { await useChatStore.getState().sendMessage('two'); });

    renderTranscript(useChatStore.getState().messages);

    // Settle inside `act`: this one does fetch the map (there is an id to
    // resolve), it just decides not to label anything with it.
    await act(async () => { await new Promise((r) => setTimeout(r, 20)); });
    expect(labels()).toEqual([]);
  });
});

describe('the signed-in actor', () => {
  const getOwnAccount = () => api.getOwnAccount as unknown as ReturnType<typeof vi.fn>;

  it('comes from the account this session was confirmed as', async () => {
    getOwnAccount().mockResolvedValue({
      id: 'acc-2', actor_id: ALICE, username: 'alice', display_name: 'Alice',
      enabled: true, has_password: true, created_at: 't', updated_at: 't',
      disabled_at: null, is_self: true,
    });

    await act(async () => { await useAuthStore.getState().checkAuth(); });

    // Keyed on `actor_id`, never on the account id — that one is the login and
    // can be renamed away; this one is what a message was stored under.
    expect(selfActorId()).toBe(ALICE);
    expect(useAuthStore.getState().account?.id).toBe('acc-2');
  });

  it('is gone the moment the session is', async () => {
    getOwnAccount().mockResolvedValue({
      id: 'acc-2', actor_id: ALICE, username: 'alice', display_name: 'Alice',
      enabled: true, has_password: true, created_at: 't', updated_at: 't',
      disabled_at: null, is_self: true,
    });
    await act(async () => { await useAuthStore.getState().checkAuth(); });
    expect(selfActorId()).toBe(ALICE);

    act(() => useAuthStore.getState().logout());

    expect(selfActorId()).toBeNull();
  });

  it('stays null when identity cannot be read', async () => {
    getOwnAccount().mockRejectedValue(new Error('403: not an account'));

    await act(async () => { await useAuthStore.getState().checkAuth(); });

    expect(selfActorId()).toBeNull();
  });
});

/**
 * An identity read is a request whose answer can outlive its question.
 *
 * The failure is not a missing label, which is merely disappointing — it is
 * Bob's message carrying Alice's actor and being shown to everyone else as
 * hers, a false statement written into the column this branch exists to make
 * trustworthy.
 *
 * Most of the guarantee comes from the auth store's own shape: identity is
 * read *before* a session is announced as authenticated, and committed in the
 * same update, so there is no window in which the app is usable and who you are
 * is unknown. What these cover is the remaining edge — a decision that is still
 * being made when the session it belongs to ends.
 */
describe('identity across an auth boundary', () => {
  const getOwnAccount = () => api.getOwnAccount as unknown as ReturnType<typeof vi.fn>;
  const login = () => api.login as unknown as ReturnType<typeof vi.fn>;
  const accountFor = (actorId: string, id = 'acc') => ({
    id, actor_id: actorId, username: 'somebody', display_name: null,
    enabled: true, has_password: true, created_at: 't', updated_at: 't',
    disabled_at: null, is_self: true,
  });

  it('does not sign you back in when a sign-in lands after a sign-out', async () => {
    // The overlay's log-out button stays live while an unlock is in flight, so
    // this ordering is a thing a person can actually do.
    const slowIdentity = deferred<ReturnType<typeof accountFor>>();
    getOwnAccount().mockReturnValueOnce(slowIdentity.promise);

    let signIn!: Promise<void>;
    act(() => { signIn = useAuthStore.getState().login('pw', 'alice'); });
    act(() => useAuthStore.getState().logout());

    await act(async () => {
      slowIdentity.resolve(accountFor(ALICE));
      await signIn;
    });

    expect(useAuthStore.getState().authenticated).toBe(false);
    expect(selfActorId()).toBeNull();
  });

  it('does not restore a session that a sign-out ended mid-startup', async () => {
    const slowIdentity = deferred<ReturnType<typeof accountFor>>();
    getOwnAccount().mockReturnValueOnce(slowIdentity.promise);

    let startup!: Promise<void>;
    act(() => { startup = useAuthStore.getState().checkAuth(); });
    act(() => useAuthStore.getState().logout());

    await act(async () => {
      slowIdentity.resolve(accountFor(ALICE));
      await startup;
    });

    expect(selfActorId()).toBeNull();
  });

  it('knows who you are before it lets the app be used', async () => {
    // The window round 2 was worried about — sending before identity is known —
    // cannot open: `login` resolves identity first and commits both together.
    getOwnAccount().mockResolvedValue(accountFor(ALICE));

    await act(async () => { await useAuthStore.getState().login('pw', 'alice'); });

    expect(useAuthStore.getState().authenticated).toBe(true);
    expect(selfActorId()).toBe(ALICE);

    useChatStore.setState({ messages: [], activeSession: 's1', virtualSession: null });
    await act(async () => { await useChatStore.getState().sendMessage('ship it?'); });

    expect(useChatStore.getState().messages[0].actor_id).toBe(ALICE);
  });

  it('still sends when identity can never be read', async () => {
    getOwnAccount().mockRejectedValue(new Error('403: not an account'));
    await act(async () => { await useAuthStore.getState().login('pw', 'alice'); });

    useChatStore.setState({ messages: [], activeSession: 's1', virtualSession: null });
    await act(async () => { await useChatStore.getState().sendMessage('ship it?'); });

    // Not knowing who you are must never become a way to lose a message.
    expect(useChatStore.getState().messages).toHaveLength(1);
    expect(useChatStore.getState().messages[0].actor_id).toBeNull();
  });

  it('never installs a credential for a session that has ended', async () => {
    // The overlay keeps its log-out button live while an unlock is in flight,
    // so this is a thing a person can do. A token arriving afterwards must not
    // quietly sign them back in.
    const slowLogin = deferred<{ token: string }>();
    login().mockReturnValueOnce(slowLogin.promise);
    getOwnAccount().mockResolvedValue(accountFor(ALICE));

    let signIn!: Promise<void>;
    act(() => { signIn = useAuthStore.getState().login('pw', 'alice'); });
    act(() => useAuthStore.getState().logout());

    await act(async () => {
      slowLogin.resolve({ token: 'alices-late-token' });
      await signIn;
    });

    expect(setToken).not.toHaveBeenCalledWith('alices-late-token');
    expect(useAuthStore.getState().authenticated).toBe(false);
    // And the form is usable again: a spinner nobody will ever stop is a login
    // page that cannot be submitted until the tab is reloaded.
    expect(useAuthStore.getState().loading).toBe(false);
  });

  it('does not take back a token that is no longer its own', async () => {
    // Alice's identity read is still open when she signs out and Bob signs in.
    // Her attempt must clean up after itself without touching his credential.
    const alicesIdentity = deferred<ReturnType<typeof accountFor>>();
    login().mockResolvedValueOnce({ token: 'alices-token' });
    getOwnAccount().mockReturnValueOnce(alicesIdentity.promise);

    let alicesSignIn!: Promise<void>;
    act(() => { alicesSignIn = useAuthStore.getState().login('pw', 'alice'); });
    await act(async () => { await Promise.resolve(); });

    act(() => useAuthStore.getState().logout());
    login().mockResolvedValueOnce({ token: 'bobs-token' });
    getOwnAccount().mockResolvedValue(accountFor(BOB, 'acc-2'));
    await act(async () => { await useAuthStore.getState().login('pw', 'bob'); });
    expect(selfActorId()).toBe(BOB);
    clearToken.mockClear();

    await act(async () => {
      alicesIdentity.resolve(accountFor(ALICE));
      await alicesSignIn;
    });

    // Bob stays signed in, with his own token untouched.
    expect(clearToken).not.toHaveBeenCalled();
    expect(selfActorId()).toBe(BOB);
    expect(useAuthStore.getState().authenticated).toBe(true);
  });

  it('labels a deferred run-later with whoever asked for it', async () => {
    // Alice schedules something; Bob signs in before the requests come back.
    // The server records Alice, so the row on screen has to say Alice too —
    // otherwise a reload changes the attribution.
    login().mockResolvedValue({ token: 'alices-token' });
    getOwnAccount().mockResolvedValue(accountFor(ALICE));
    await act(async () => { await useAuthStore.getState().login('pw', 'alice'); });
    expect(selfActorId()).toBe(ALICE);

    const scheduled = deferred<{ ack: string }>();
    (api.createSession as unknown as ReturnType<typeof vi.fn>)
      .mockResolvedValue({ id: 'later-1', title: '', source: 'web', updated_at: 't' });
    (api.runLater as unknown as ReturnType<typeof vi.fn>)
      .mockReturnValueOnce(scheduled.promise);

    useChatStore.setState({ messages: [], activeSession: '', virtualSession: null });
    let later!: Promise<void>;
    act(() => { later = useChatStore.getState().runLater('sweep at 9', '1h'); });

    await act(async () => {
      scheduled.resolve({ ack: 'Scheduled.' });
      await later;
    });

    expect(useChatStore.getState().messages[0].actor_id).toBe(ALICE);
  });

  it('abandons a run-later whose session ended rather than relabelling it', async () => {
    login().mockResolvedValue({ token: 'alices-token' });
    getOwnAccount().mockResolvedValue(accountFor(ALICE));
    await act(async () => { await useAuthStore.getState().login('pw', 'alice'); });

    const scheduled = deferred<{ ack: string }>();
    (api.createSession as unknown as ReturnType<typeof vi.fn>)
      .mockResolvedValue({ id: 'later-1', title: '', source: 'web', updated_at: 't' });
    (api.runLater as unknown as ReturnType<typeof vi.fn>)
      .mockReturnValueOnce(scheduled.promise);

    useChatStore.setState({ messages: [], activeSession: '', virtualSession: null });
    let later!: Promise<void>;
    act(() => { later = useChatStore.getState().runLater('sweep at 9', '1h'); });

    act(() => useAuthStore.getState().logout());
    getOwnAccount().mockResolvedValue(accountFor(BOB, 'acc-2'));
    await act(async () => { await useAuthStore.getState().login('pw', 'bob'); });

    await act(async () => {
      scheduled.resolve({ ack: 'Scheduled.' });
      await later;
    });

    // Bob's screen does not gain Alice's scheduled prompt, and nothing on it
    // is labelled with him either.
    expect(useChatStore.getState().messages).toHaveLength(0);
  });

  it('takes the actor from whoever actually unlocked the app', async () => {
    // A different person answering the expired-session overlay is a sign-out,
    // not a sign-in — so nothing of the previous one, the actor included,
    // survives to stamp their messages.
    useAuthStore.setState({
      account: { id: 'acc-1', username: 'alice', actor_id: ALICE },
      sessionExpired: true, authenticated: false,
    });
    login().mockResolvedValue({ token: 'bobs-token' });
    getOwnAccount().mockResolvedValue(accountFor(BOB, 'acc-2'));

    await act(async () => { await useAuthStore.getState().login('pw', 'bob'); });

    expect(useAuthStore.getState().authenticated).toBe(false);
    expect(selfActorId()).toBeNull();
  });
});

describe('renaming somebody', () => {
  it('changes every label without touching a single message object', async () => {
    const messages = [said('one', ALICE, 1), said('two', BOB, 2)];
    const before = structuredClone(messages);
    renderTranscript(messages);
    expect(await screen.findByText('Alice')).toBeInTheDocument();

    // What the accounts screen does after a rename: a fresh read, with the
    // new name and a bumped profile version.
    listActors.mockResolvedValue({
      actors: [alice('Alice Doe'), { ...bob(), profile_version: 3 }],
    });
    await act(() => useActorStore.getState().refresh());

    expect(await screen.findByText('Alice Doe')).toBeInTheDocument();
    expect(screen.queryByText('Alice')).toBeNull();
    // The rows the label came from are untouched: a name was never stored on
    // one, so there was nothing to rewrite.
    expect(messages).toEqual(before);
    expect(messages[0].actor_id).toBe(ALICE);
  });
});
