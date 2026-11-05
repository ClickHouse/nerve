// @vitest-environment jsdom
import { render, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '../../api/client';
import type { WSMessage } from '../../api/websocket';
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
  },
  getToken: vi.fn(() => 'tok'), setToken: vi.fn(), clearToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
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
const { handleSessionRunning } = await import('../../stores/handlers/sessionHandlers');
const { SessionSidebar } = await import('./SessionSidebar');

const listActors = api.listActors as unknown as ReturnType<typeof vi.fn>;
const ALICE = 'actor-alice';
const BOB = 'actor-bob';
const SYSTEM = 'actor-system';

function actorRef(id: string, overrides: Partial<ActorRef> = {}): ActorRef {
  return { id, kind: 'human', display_name: null, username: null, ...overrides } as ActorRef;
}

function parked(
  id: string,
  createdBy: string | null,
  causedBy: string | null,
): Session {
  return {
    id, title: id, source: 'web', updated_at: new Date().toISOString(),
    starred: true, is_running: false,
    pending_wakeup_at: new Date(Date.now() + 30 * 60_000).toISOString(),
    pending_wakeup_created_by_actor_id: createdBy,
    pending_wakeup_caused_by_actor_id: causedBy,
  };
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

/** The parked dots' tooltips, without the local fire time. */
function wakeupTooltips(): string[] {
  return [...document.querySelectorAll('[title^="Scheduled wake-up at"]')]
    .map((el) => (el.getAttribute('title') ?? '').replace(/ at [^ ]+( [AP]M)?/, ' at T'));
}

beforeEach(() => {
  vi.clearAllMocks();
  useActorStore.getState().reset();
  useAuthStore.setState({
    viewer: actorRef(BOB),
    account: { id: 'account-bob', username: null },
  });
  useChatStore.setState({
    sessions: [], searchResults: null, archivedSessions: null, systemSessions: null,
    archivedCount: 0, systemCount: 0, drafts: {}, reads: {}, readsBaseline: 0,
    virtualSession: null, activeSession: '',
  });
  (api.listSessions as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
    sessions: [], archived_count: 0, system_count: 0, has_more: false, next_offset: 0,
  });
  listActors.mockResolvedValue({
    actors: [
      actorRef(ALICE, { display_name: 'Alice' }),
      actorRef(BOB, { display_name: 'Bob' }),
      actorRef(SYSTEM, { kind: 'system' }),
    ],
  });
});

describe('who scheduled a pending wake-up', () => {
  it('names the person, the agent for a person, or the agent alone', async () => {
    renderSidebar([
      parked('deferred', ALICE, ALICE),
      parked('agent-for-bob', SYSTEM, BOB),
      parked('cron-loop', SYSTEM, SYSTEM),
    ]);

    await waitFor(() => expect(wakeupTooltips()).toEqual([
      'Scheduled wake-up at T by Alice',
      'Scheduled wake-up at T by Nerve for Bob',
      'Scheduled wake-up at T by Nerve',
    ]));
  });

  it('names nobody for a wake-up whose scheduler was not recorded', async () => {
    renderSidebar([parked('legacy', null, null)]);

    await waitFor(() => expect(wakeupTooltips()).toEqual(['Scheduled wake-up at T']));
    expect(listActors).not.toHaveBeenCalled();
  });

  it('takes the scheduler from the session_running event', () => {
    useChatStore.setState({ sessions: [parked('s1', ALICE, ALICE)] });
    const event = {
      type: 'session_running', session_id: 's1', is_running: false,
      pending_wakeup_at: '2026-01-01T00:10:00Z',
      pending_wakeup_created_by_actor_id: SYSTEM,
      pending_wakeup_caused_by_actor_id: BOB,
    } as Extract<WSMessage, { type: 'session_running' }>;

    handleSessionRunning(event, useChatStore.getState, useChatStore.setState);

    const [s1] = useChatStore.getState().sessions;
    expect(s1.pending_wakeup_created_by_actor_id).toBe(SYSTEM);
    expect(s1.pending_wakeup_caused_by_actor_id).toBe(BOB);

    handleSessionRunning(
      { type: 'session_running', session_id: 's1', is_running: false },
      useChatStore.getState, useChatStore.setState,
    );
    const [cleared] = useChatStore.getState().sessions;
    expect(cleared.pending_wakeup_at).toBeNull();
    expect(cleared.pending_wakeup_created_by_actor_id).toBeNull();
    expect(cleared.pending_wakeup_caused_by_actor_id).toBeNull();
  });
});
