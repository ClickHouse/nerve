// @vitest-environment jsdom
import { act, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '../../api/client';
import type { Session } from '../../types/chat';

/**
 * What attribution looks like on a session.
 *
 * Two surfaces with deliberately different rules, and the difference is the
 * thing most worth pinning down:
 *
 * - **The list** names a creator only when it tells two rows apart — the same
 *   rule the transcript uses. Forty rows repeating one name is noise.
 * - **The header** names the creator whenever there is one. It describes a
 *   single session, so it is an answer rather than a repetition, and it is the
 *   only place the full answer is spelled out.
 *
 * The agent's own principal is always marked on both, because "Nerve did this
 * on a schedule" is a different kind of fact from "a person asked for this" and
 * on a cron-heavy instance it is most of what attribution is for.
 */

// The store reads localStorage at module init and Node 25 injects an inert
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
    listSessions: vi.fn(),
    listArchivedSessions: vi.fn(),
    listSystemSessions: vi.fn(),
    searchSessions: vi.fn(),
    // ChatPage and its composer, so the real header can be mounted.
    getObservabilityStatus: vi.fn(async () => ({ enabled: false })),
    getPromptRewriteStatus: vi.fn(async () => ({ enabled: false })),
    getModels: vi.fn(async () => ({ models: [], default: null })),
    uploadFiles: vi.fn(),
    rewritePrompt: vi.fn(),
    listAccounts: vi.fn(async () => ({ accounts: [] })),
  },
  getToken: vi.fn(() => 'tok'),
  setUnauthorizedHandler: vi.fn(),
}));
vi.mock('../../api/websocket', () => ({
  ws: { switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(), disconnect: vi.fn(), onMessage: vi.fn(() => () => {}) },
}));

const { api } = await import('../../api/client');
const { useActorStore } = await import('../../stores/actorStore');
const { useChatStore } = await import('../../stores/chatStore');
const { SessionSidebar } = await import('./SessionSidebar');
const { SessionCreator } = await import('./ActorLabel');
const { ChatPage } = await import('../../pages/ChatPage');

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

function chat(id: string, created_by_actor_id: string | null): Session {
  return {
    id,
    title: id,
    source: 'web',
    updated_at: new Date().toISOString(),
    created_by_actor_id,
  };
}

function renderSidebar(sessions: Session[]) {
  return render(
    <MemoryRouter>
      <SessionSidebar
        sessions={sessions}
        activeSession=""
        agentStatus={{ state: 'idle' }}
        onCreate={() => {}}
        onDelete={() => {}}
      />
    </MemoryRouter>,
  );
}

/** Every creator marker on screen, by its accessible/hover text. */
function markers(): string[] {
  return [...document.querySelectorAll('[data-attribution="session-row"]')]
    .map((el) => el.getAttribute('title') ?? '');
}

beforeEach(() => {
  vi.clearAllMocks();
  useActorStore.getState().reset();
  useChatStore.setState({
    sessions: [], searchResults: null, archivedSessions: null, systemSessions: null,
    archivedCount: 0, systemCount: 0, drafts: {}, reads: {}, readsBaseline: 0,
    virtualSession: null, activeSession: '',
  });
  listActors.mockResolvedValue({ actors: [alice(), bob(), system()] });
  // ChatPage refetches the feed on mount; the seeded state is what the specs
  // assert on, so the request just has to return a well-formed empty page.
  (api.listSessions as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
    sessions: [], archived_count: 0, system_count: 0, has_more: false, next_offset: 0,
  });
});

describe('the session list', () => {
  it('names both creators once two people have started chats', async () => {
    renderSidebar([chat('s1', ALICE), chat('s2', BOB)]);

    await waitFor(() => expect(markers().sort()).toEqual(['Started by Alice', 'Started by Bob']));
    expect(screen.getByText('Alice')).toBeInTheDocument();
    expect(screen.getByText('Bob')).toBeInTheDocument();
  });

  it('names nobody when every chat is one person\'s', async () => {
    renderSidebar([chat('s1', ALICE), chat('s2', ALICE)]);

    await waitFor(() => expect(listActors).toHaveBeenCalled());
    expect(markers()).toEqual([]);
  });

  it('marks the agent\'s own sessions even with nobody to compare them to', async () => {
    renderSidebar([chat('s1', SYSTEM), chat('s2', SYSTEM)]);

    await waitFor(() => expect(markers()).toHaveLength(2));
    expect(markers()[0]).toBe('Started by Nerve itself — scheduled or autonomous work');
  });

  it('tells the agent apart from a person by glyph as well as wording', async () => {
    const { container } = renderSidebar([chat('s1', SYSTEM), chat('s2', ALICE), chat('s3', BOB)]);

    await waitFor(() => expect(markers()).toHaveLength(3));
    const rows = [...container.querySelectorAll('[data-attribution="session-row"]')];
    const agent = rows.find((el) => el.getAttribute('title')!.includes('Nerve'))!;
    const person = rows.find((el) => el.getAttribute('title') === 'Started by Alice')!;
    // The agent is a glyph with an accessible name; a person is their name.
    expect(agent).toHaveAttribute('role', 'img');
    expect(agent).toHaveAttribute('aria-label', expect.stringContaining('Nerve'));
    expect(agent.querySelector('svg')).not.toBeNull();
    expect(person.querySelector('svg')).toBeNull();
    expect(person).toHaveTextContent('Alice');
  });

  it('leaves a session nobody was recorded for exactly as it was', async () => {
    renderSidebar([chat('s1', null), chat('s2', null)]);

    await new Promise((r) => setTimeout(r, 20));
    expect(markers()).toEqual([]);
    expect(listActors).not.toHaveBeenCalled();
    expect(screen.getByText('s1')).toBeInTheDocument();
  });

  it('keeps history unmarked beside chats that are marked', async () => {
    renderSidebar([chat('s1', null), chat('s2', ALICE), chat('s3', BOB)]);

    await waitFor(() => expect(markers()).toHaveLength(2));
    expect(screen.getByText('s1')).toBeInTheDocument();
  });

  it('falls back neutrally for a creator with no name, and for an unknown id', async () => {
    listActors.mockResolvedValue({ actors: [alice(null)] });
    renderSidebar([chat('s1', ALICE), chat('s2', 'actor-elsewhere')]);

    await waitFor(() => expect(markers()).toHaveLength(2));
    expect(markers()).toEqual([`Started by actor ${ALICE}`, 'Started by actor actor-elsewhere']);
    // Both rows still render their titles: an unresolvable name costs nothing.
    expect(screen.getByText('s1')).toBeInTheDocument();
    expect(screen.getByText('s2')).toBeInTheDocument();
  });

  it('re-labels every row when somebody is renamed, without touching the rows', async () => {
    const sessions = [chat('s1', ALICE), chat('s2', BOB)];
    const before = structuredClone(sessions);
    renderSidebar(sessions);
    await waitFor(() => expect(markers()).toContain('Started by Alice'));

    listActors.mockResolvedValue({ actors: [alice('Alice Doe'), bob()] });
    await act(() => useActorStore.getState().refresh());

    await waitFor(() => expect(markers()).toContain('Started by Alice Doe'));
    expect(sessions).toEqual(before);
  });
});

describe('the chat page', () => {
  /** The real header, so the wiring is asserted and not just the component. */
  async function renderChatPage(session: Session) {
    useChatStore.setState({ sessions: [session], activeSession: session.id });
    // The page refetches the feed on mount and the refetch replaces the list,
    // so the server has to agree with the seed.
    (api.listSessions as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
      sessions: [session], archived_count: 0, system_count: 0, has_more: false, next_offset: 1,
    });
    const result = render(<MemoryRouter><ChatPage /></MemoryRouter>);
    // The page and its composer each start a request on mount. Settle them
    // inside `act` so their state updates belong to the render rather than
    // arriving mid-assertion.
    await act(async () => { await new Promise((r) => setTimeout(r, 0)); });
    return result;
  }

  it('names the creator of the open session in its header', async () => {
    await renderChatPage(chat('s1', BOB));

    expect(await screen.findByText('Started by Bob')).toBeInTheDocument();
  });

  it('says so when the agent started it', async () => {
    await renderChatPage(chat('s1', SYSTEM));

    expect(await screen.findByText('Started by Nerve')).toBeInTheDocument();
  });

  it('puts nothing in the header for a session with no recorded creator', async () => {
    const { container } = await renderChatPage(chat('s1', null));

    expect(container.querySelectorAll('[data-attribution="session-header"]')).toHaveLength(0);
    expect(listActors).not.toHaveBeenCalled();
  });
});

describe('the chat header', () => {
  it('names the creator without waiting for a second person', async () => {
    render(<SessionCreator actorId={ALICE} />);

    expect(await screen.findByText('Started by Alice')).toBeInTheDocument();
  });

  it('says the agent started it, and why that is different', async () => {
    render(<SessionCreator actorId={SYSTEM} />);

    expect(await screen.findByText('Started by Nerve')).toBeInTheDocument();
    expect(
      screen.getByTitle('Started by Nerve itself — scheduled or autonomous work'),
    ).toBeInTheDocument();
  });

  it('renders nothing at all when nobody was recorded', async () => {
    const { container } = render(<SessionCreator actorId={null} />);

    await new Promise((r) => setTimeout(r, 20));
    expect(container).toBeEmptyDOMElement();
    expect(listActors).not.toHaveBeenCalled();
  });

  it('keeps its name reachable when the text is only for screen readers', async () => {
    // Below `md` the chip sheds its text rather than itself, so a phone can
    // still learn who a shared session belongs to — the session list marks a
    // person only once two of them exist, and there it is behind a drawer.
    // The breakpoint itself is a Tailwind class and not assertable here (the
    // stylesheet is not processed and `matchMedia` reports desktop); what is
    // assertable, and what the mobile state depends on, is that the sentence
    // is always in the DOM rather than conditionally rendered, and that the
    // glyph beside it never claims a name of its own.
    const { container } = render(<SessionCreator actorId={ALICE} />);

    expect(await screen.findByText('Started by Alice')).toBeInTheDocument();
    const chip = container.querySelector('[data-attribution="session-header"]')!;
    expect(chip).toHaveAttribute('title', 'Started by Alice');
    expect(chip.querySelector('svg')).toHaveAttribute('aria-hidden', 'true');
    const text = chip.querySelector('span')!;
    expect(text.className).toContain('sr-only');
    expect(text.className).toContain('md:not-sr-only');
  });

  it('falls back to the neutral label rather than the id', async () => {
    listActors.mockResolvedValue({ actors: [alice(null)] });
    render(<SessionCreator actorId={ALICE} />);

    expect(await screen.findByText('Started by Unnamed account')).toBeInTheDocument();
    expect(screen.queryByText(new RegExp(ALICE))).toBeNull();
  });

  it('does not crash on an id the server has never heard of', async () => {
    render(<SessionCreator actorId="actor-elsewhere" />);

    expect(await screen.findByText('Started by Unnamed account')).toBeInTheDocument();
  });
});
