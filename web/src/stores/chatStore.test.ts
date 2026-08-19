// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest';

// Node 25 injects an inert `localStorage` global that shadows jsdom's Storage
// (see dateGroups.test.ts); the store reads localStorage at module init, so
// install a real in-memory Storage BEFORE the dynamic import below.
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

// Mock the API + websocket modules so importing the store has no live side
// effects; only listSessions behaviour matters for these assertions.
vi.mock('../api/client', () => ({
  api: {
    listSessions: vi.fn(),
    listArchivedSessions: vi.fn(),
    listSystemSessions: vi.fn(),
  },
}));
vi.mock('../api/websocket', () => ({
  ws: { switchSession: vi.fn(), send: vi.fn(), connect: vi.fn() },
}));

const { api } = await import('../api/client');
const { useChatStore } = await import('./chatStore');

const PAGE = 50;
const rows = (from: number, count: number) =>
  Array.from({ length: count }, (_, i) => ({ id: `s${from + i}` }) as never);

beforeEach(() => {
  vi.clearAllMocks();
  // Paginated server: offset 0 → page 1, offset 50 → page 2, then done.
  (api.listSessions as ReturnType<typeof vi.fn>).mockImplementation(async (offset = 0) => {
    if (offset === 0) return { sessions: rows(0, PAGE), archived_count: 0, system_count: 0, has_more: true, next_offset: PAGE };
    if (offset === PAGE) return { sessions: rows(PAGE, PAGE), archived_count: 0, system_count: 0, has_more: false, next_offset: 2 * PAGE };
    return { sessions: [], archived_count: 0, system_count: 0, has_more: false, next_offset: offset };
  });
});

describe('loadSessions depth preservation', () => {
  it('re-pages forward to restore prior depth instead of collapsing to page 1', async () => {
    // User had paged two pages deep before the refresh.
    useChatStore.setState({
      sessions: rows(0, 100), sessionsNextOffset: 100, sessionsHasMore: true,
      archivedSessions: null, systemSessions: null, activeSession: '', virtualSession: null,
    });

    await useChatStore.getState().loadSessions();

    const s = useChatStore.getState();
    expect(s.sessions.length).toBe(100);
    expect(s.sessionsNextOffset).toBe(100);
    // Page 1 + one re-page (offsets 0 and 50); it must NOT stop at page 1.
    expect((api.listSessions as ReturnType<typeof vi.fn>).mock.calls.map(c => c[0] ?? 0)).toEqual([0, 50]);
  });

  it('first-ever load (offset 0) does not re-page', async () => {
    useChatStore.setState({
      sessions: [], sessionsNextOffset: 0, sessionsHasMore: false,
      archivedSessions: null, systemSessions: null, activeSession: '', virtualSession: null,
    });

    await useChatStore.getState().loadSessions();

    const s = useChatStore.getState();
    expect(s.sessions.length).toBe(PAGE);
    expect((api.listSessions as ReturnType<typeof vi.fn>).mock.calls.length).toBe(1);
  });

  it('halts when rows shrank (has_more false) rather than looping forever', async () => {
    // Prior depth 500 but the server now only has two pages.
    useChatStore.setState({
      sessions: rows(0, 500), sessionsNextOffset: 500, sessionsHasMore: true,
      archivedSessions: null, systemSessions: null, activeSession: '', virtualSession: null,
    });

    await useChatStore.getState().loadSessions();

    const s = useChatStore.getState();
    expect(s.sessions.length).toBe(100);
    expect(s.sessionsHasMore).toBe(false);
  });
});
