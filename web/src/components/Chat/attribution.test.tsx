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

vi.mock('../../api/client', () => ({
  api: {
    listActors: vi.fn(),
    getActor: vi.fn(),
    listAccounts: vi.fn(async () => ({ accounts: [] })),
    authStatus: vi.fn(async () => ({
      auth_required: true, mode: 'local', login: 'password',
      setup_pending: false, multiple_accounts: true,
    })),
    checkAuth: vi.fn(async () => ({ authenticated: true })),
    login: vi.fn(),
  },
  getToken: vi.fn(() => 'tok'),
  setToken: vi.fn(),
  clearToken: vi.fn(),
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

const { api } = await import('../../api/client');
const { useActorStore } = await import('../../stores/actorStore');
const { useAuthStore } = await import('../../stores/authStore');
const { useChatStore } = await import('../../stores/chatStore');
const { handleUserMessage } = await import('../../stores/handlers/sessionHandlers');
const { MessageList } = await import('./MessageList');

const listActors = api.listActors as unknown as ReturnType<typeof vi.fn>;

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
  useAuthStore.setState({ selfActorId: null });
  useChatStore.setState({ messages: [], activeSession: '', virtualSession: null });
  listActors.mockResolvedValue({ actors: [alice(), bob(), system()] });
  (api.listAccounts as unknown as ReturnType<typeof vi.fn>)
    .mockResolvedValue({ accounts: [] });
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
    useAuthStore.setState({ selfActorId: me });
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
    useAuthStore.setState({ selfActorId: null });
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
  it('is read once per session and dropped on logout', async () => {
    const listAccounts = api.listAccounts as unknown as ReturnType<typeof vi.fn>;
    listAccounts.mockResolvedValue({
      accounts: [
        { id: 'acc-1', actor_id: BOB, is_self: false },
        { id: 'acc-2', actor_id: ALICE, is_self: true },
      ],
    });

    await act(async () => { await useAuthStore.getState().checkAuth(); });

    // Keyed on `actor_id` and on `is_self` — never on the account id, which is
    // the login rather than the person.
    await waitFor(() => expect(useAuthStore.getState().selfActorId).toBe(ALICE));

    act(() => useAuthStore.getState().logout());
    expect(useAuthStore.getState().selfActorId).toBeNull();
  });

  it('stays null when the account list is refused', async () => {
    const listAccounts = api.listAccounts as unknown as ReturnType<typeof vi.fn>;
    listAccounts.mockRejectedValue(new Error('403: not an account'));

    await act(async () => { await useAuthStore.getState().checkAuth(); });

    await new Promise((r) => setTimeout(r, 20));
    expect(useAuthStore.getState().selfActorId).toBeNull();
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
