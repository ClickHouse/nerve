// @vitest-environment jsdom
import { act, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '../../api/client';
import type { Session } from '../../types/chat';

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

vi.mock('../../api/client', () => ({
  api: {
    listActors: vi.fn(), listSessions: vi.fn(), listArchivedSessions: vi.fn(),
    listSystemSessions: vi.fn(), searchSessions: vi.fn(),
    getObservabilityStatus: vi.fn(async () => ({ enabled: false })),
    getPromptRewriteStatus: vi.fn(async () => ({ enabled: false })),
    getModels: vi.fn(async () => ({ models: [], default: null })),
    uploadFiles: vi.fn(), rewritePrompt: vi.fn(),
  },
  getToken: vi.fn(() => 'tok'), setUnauthorizedHandler: vi.fn(),
}));
vi.mock('../../api/websocket', () => ({
  ws: {
    switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    disconnect: vi.fn(), onMessage: vi.fn(() => () => {}),
  },
}));

const { api } = await import('../../api/client');
const { useActorStore } = await import('../../stores/actorStore');
const { useAuthStore } = await import('../../stores/authStore');
const { useChatStore } = await import('../../stores/chatStore');
const { SessionSidebar } = await import('./SessionSidebar');
const { SessionCreator } = await import('./ActorLabel');
const { ChatPage } = await import('../../pages/ChatPage');

const listActors = api.listActors as unknown as ReturnType<typeof vi.fn>;
const ALICE = 'actor-alice';
const BOB = 'actor-bob';
const SYSTEM = 'actor-system';

function actorRef(id: string, overrides: Partial<ActorRef> = {}): ActorRef {
  return { id, kind: 'human', display_name: null, username: null, ...overrides } as ActorRef;
}
const alice = (name: string | null = 'Alice') => actorRef(ALICE, { display_name: name });
const bob = (name: string | null = 'Bob') => actorRef(BOB, { display_name: name });
const system = () => actorRef(SYSTEM, { kind: 'system', display_name: null });

function chat(id: string, created_by_actor_id: string | null): Session {
  return {
    id, title: id, source: 'web', updated_at: new Date().toISOString(),
    created_by_actor_id, starred: true,
  };
}

function viewAs(actorId: string): void {
  useAuthStore.setState({
    viewer: actorRef(actorId),
    account: { id: `account-${actorId}`, username: null },
  });
}

function renderSidebar(sessions: Session[]) {
  return render(
    <MemoryRouter>
      <SessionSidebar
        sessions={sessions} activeSession="" agentStatus={{ state: 'idle' }}
        onCreate={() => {}} onDelete={() => {}}
      />
    </MemoryRouter>,
  );
}

function markers(): string[] {
  return [...document.querySelectorAll('[data-attribution="session-row"]')]
    .map((el) => el.getAttribute('title') ?? '');
}

function headerChip(): HTMLElement | null {
  return document.querySelector('[data-attribution="session-header"]');
}

beforeEach(() => {
  vi.clearAllMocks();
  useActorStore.getState().reset();
  viewAs(BOB);
  useChatStore.setState({
    sessions: [], searchResults: null, archivedSessions: null, systemSessions: null,
    archivedCount: 0, systemCount: 0, drafts: {}, reads: {}, readsBaseline: 0,
    virtualSession: null, activeSession: '',
  });
  listActors.mockResolvedValue({ actors: [alice(), bob(), system()] });
  (api.listSessions as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
    sessions: [], archived_count: 0, system_count: 0, has_more: false, next_offset: 0,
  });
});

describe('viewer-relative session rows', () => {
  it('shows Alice throughout an Alice-only sidebar to Bob, but keeps Bob and null rows quiet', async () => {
    renderSidebar([
      chat('alice-1', ALICE), chat('alice-2', ALICE),
      chat('bob', BOB), chat('legacy', null),
    ]);

    await waitFor(() => expect(markers()).toEqual(['Started by Alice', 'Started by Alice']));
    expect(screen.getByText('bob')).toBeInTheDocument();
    expect(screen.getByText('legacy')).toBeInTheDocument();
  });

  it('labels a person without a display name by their login name', async () => {
    listActors.mockResolvedValue({
      actors: [actorRef(ALICE, { username: 'alice' }), bob(), system()],
    });
    renderSidebar([chat('alice-1', ALICE), chat('bob', BOB)]);

    await waitFor(() => expect(markers()).toEqual(['Started by alice']));
    expect(screen.queryByText('Unnamed account')).toBeNull();
  });

  it('always marks Nerve with an accessible bot glyph', async () => {
    const { container } = renderSidebar([chat('system', SYSTEM), chat('bob', BOB)]);

    await waitFor(() => expect(markers()).toEqual([
      'Started by Nerve itself — scheduled or autonomous work',
    ]));
    const marker = container.querySelector('[data-attribution="session-row"]')!;
    expect(marker).toHaveAttribute('role', 'img');
    expect(marker.querySelector('svg')).not.toBeNull();
  });

  it('updates current names without rewriting session ids', async () => {
    const sessions = [chat('alice', ALICE), chat('bob', BOB)];
    const before = structuredClone(sessions);
    renderSidebar(sessions);
    await waitFor(() => expect(markers()).toContain('Started by Alice'));

    listActors.mockResolvedValue({ actors: [alice('Alice Doe'), bob(), system()] });
    await act(() => useActorStore.getState().refresh());

    await waitFor(() => expect(markers()).toContain('Started by Alice Doe'));
    expect(sessions).toEqual(before);
  });
});

describe('the open-session header', () => {
  async function renderChatPage(session: Session) {
    useChatStore.setState({ sessions: [session], activeSession: session.id });
    (api.listSessions as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
      sessions: [session], archived_count: 0, system_count: 0,
      has_more: false, next_offset: 1,
    });
    const result = render(<MemoryRouter><ChatPage /></MemoryRouter>);
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, 0)); });
    return result;
  }

  it('wires Bob\'s view of Alice into the real chat page', async () => {
    await renderChatPage(chat('alice', ALICE));
    await waitFor(() => expect(headerChip()).toHaveTextContent('Started by Alice'));
  });

  it('keeps Bob\'s own header and null legacy creators quiet', async () => {
    const page = await renderChatPage(chat('bob', BOB));
    expect(headerChip()).toBeNull();
    page.unmount();

    listActors.mockClear();
    render(<SessionCreator actorId={null} />);
    expect(headerChip()).toBeNull();
    expect(listActors).not.toHaveBeenCalled();
  });

  it('always shows Nerve in the real chat page', async () => {
    await renderChatPage(chat('system', SYSTEM));
    await waitFor(() => expect(headerChip()).toHaveTextContent('Started by Nerve'));
  });

  it('keeps the name visible on mobile and the verb available to screen readers', async () => {
    render(<SessionCreator actorId={ALICE} />);
    await waitFor(() => expect(headerChip()).toHaveTextContent('Started by Alice'));
    const chip = headerChip()!;
    const verb = chip.querySelector('.sr-only')!;

    expect(verb).toHaveTextContent('Started by');
    expect(verb.textContent).not.toContain('Alice');
    expect(verb.className).toContain('md:not-sr-only');
    expect(chip.querySelector('svg')).toHaveAttribute('aria-hidden', 'true');
  });

  it('shows a collision-safe equal-name suffix on mobile', async () => {
    const first = '0199aaaa-1111-7000-8000-00000a0000ab';
    const second = '0199aaaa-1111-7000-8000-00000b0000ab';
    listActors.mockResolvedValue({ actors: [
      actorRef(first, { display_name: 'Alex' }),
      actorRef(second, { display_name: 'Alex' }),
    ] });
    viewAs(second);
    render(<SessionCreator actorId={first} />);

    await waitFor(() => expect(headerChip()).toHaveTextContent('Started by Alex (a0000ab)'));
    expect(headerChip()!.querySelector('.sr-only')!.textContent).not.toContain('a0000ab');
  });
});
