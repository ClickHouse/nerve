// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

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
// effects. The tests use listSessions and the WebSocket queue.
vi.mock('../api/client', () => ({
  api: {
    listSessions: vi.fn(),
    listNotifications: vi.fn(async () => ({ notifications: [], pending_count: 0 })),
    listArchivedSessions: vi.fn(),
    listSystemSessions: vi.fn(),
    getViewer: vi.fn(async () => { throw new Error("401"); }),
    getMessages: vi.fn(async () => ({ messages: [] })),
    getModifiedFiles: vi.fn(async () => ({ files: [], summary: {} })),
    getSessionAgents: vi.fn(async (sessionId: string) => ({ session_id: sessionId, agents: [], queue: [] })),
    getAgentTranscript: vi.fn(),
    archiveSession: vi.fn(),
    deleteSession: vi.fn(),
    stopSession: vi.fn(),
    createSession: vi.fn(),
    getSessionTasks: vi.fn(async () => ({ tasks: [] })),
  },
  apiErrorCode: () => null,
  // chatStore reads these authStore dependencies when it stamps optimistic rows.
  getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));
vi.mock('../api/websocket', () => ({
  ws: {
    switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    takePendingMessages: vi.fn(() => []),
    sendMessage: vi.fn(() => 'sent'), stopSession: vi.fn(),
    subscribeAgent: vi.fn(), unsubscribeAgent: vi.fn(),
    onOpen: vi.fn(() => () => {}),
  },
}));

const { api } = await import('../api/client');
const { ws } = await import('../api/websocket');
const { useChatStore, registerComposerFlush, AGENTS_PANEL_ID, MAX_AGENT_TABS, EMPTY_REVIEW_LOOP } = await import('./chatStore');
const { reenter, setAuthMode, useHostedStore } = await import('../api/hosted');
const { useAgentStore } = await import('./agentStore');
const fx = await import('../test/agentFixtures');
const { needsYou } = await import('../utils/sessionAttention');
// chatStore registers its socket-open handler once, at module init.
const onSocketOpen = vi.mocked(ws.onOpen).mock.calls[0]?.[0] as (() => void) | undefined;

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

describe('the sessions that need you', () => {
  afterEach(() => vi.useRealTimers());

  it('a later page never repeats a session that page 1 already holds', async () => {
    // Page 1 carries s60 from page 2, because it waits for the user.
    vi.mocked(api.listSessions).mockImplementation(async (offset = 0) => offset === 0
      ? { sessions: [...rows(0, PAGE), rows(60, 1)[0]], archived_count: 0, system_count: 0, has_more: true, next_offset: PAGE }
      : { sessions: rows(PAGE, PAGE), archived_count: 0, system_count: 0, has_more: false, next_offset: 2 * PAGE });
    useChatStore.setState({
      sessions: [], sessionsNextOffset: 0, sessionsHasMore: false,
      archivedSessions: null, systemSessions: null, activeSession: '', virtualSession: null,
    });

    await useChatStore.getState().loadSessions();
    await useChatStore.getState().loadMoreSessions();

    const ids = useChatStore.getState().sessions.map(s => s.id);
    expect(ids.length).toBe(2 * PAGE);
    expect(new Set(ids).size).toBe(2 * PAGE);
    expect(useChatStore.getState().sessionsNextOffset).toBe(2 * PAGE);
  });

  // The server list after the reload, and the store before the event.
  const listed = (sessions: unknown[]) => vi.mocked(api.listSessions).mockResolvedValue(
    { sessions, archived_count: 0, system_count: 0, has_more: false, next_offset: 1 } as never);
  const feed = (sessions: unknown[]) => useChatStore.setState({
    sessions: sessions as never, sessionsNextOffset: 0, sessionsHasMore: false,
    archivedSessions: null, systemSessions: null, activeSession: '', virtualSession: null,
  });
  const chat = { id: 'chat-1', title: 'chat', source: 'web', updated_at: '2026-11-07T04:00:00Z' };
  const cron = { ...chat, id: 'cron-1', source: 'cron' };

  it('a session from outside the feed that starts to wait enters Needs you', async () => {
    vi.useFakeTimers();
    feed([chat]);
    listed([chat, { ...cron, awaiting_input: true }]);

    dispatch({ type: 'session_awaiting_input', session_id: 'cron-1', awaiting: true });
    await vi.advanceTimersByTimeAsync(500);

    const added = useChatStore.getState().sessions.find(s => s.id === 'cron-1');
    expect(added && needsYou(added)).toBe(true);
  });

  it('a session from outside the feed leaves when its review loop stops waiting', async () => {
    vi.useFakeTimers();
    const loop = { id: 'rvl-1', status: 'awaiting_user', iteration: 1, max_iterations: 3 };
    feed([chat, { ...cron, review_loop: loop }]);
    listed([chat]);

    dispatch({ type: 'review_loop_update', session_id: 'cron-1', loop: { ...loop, status: 'implementing' } } as never);
    // The row is patched at once and dropped by the reload.
    expect(needsYou(useChatStore.getState().sessions.find(s => s.id === 'cron-1')!)).toBe(false);
    await vi.advanceTimersByTimeAsync(500);

    expect(useChatStore.getState().sessions.map(s => s.id)).toEqual(['chat-1']);
  });

  it('a burst of notification events reloads the list once', async () => {
    vi.useFakeTimers();
    useChatStore.setState({ sessionsNextOffset: 0, sessionsHasMore: false, archivedSessions: null, systemSessions: null });
    const base = { notification_id: 'n1', session_id: 's1', title: 't' };
    dispatch({ ...base, type: 'notification', notification_type: 'question', body: '', priority: 'normal', options: null });
    dispatch({ ...base, type: 'notification_answered', answer: 'yes', answered_by: 'web' });
    dispatch({ ...base, type: 'notification_expired', notification_type: 'question' });
    dispatch({ ...base, type: 'notification_dismissed', notification_type: 'question' });
    expect(api.listSessions).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(500);
    expect(api.listSessions).toHaveBeenCalledTimes(1);
  });

  it('a dismissal in another place reloads the list and folds the question', async () => {
    vi.useFakeTimers();
    useChatStore.setState({ sessionsNextOffset: 0, sessionsHasMore: false, archivedSessions: null, systemSessions: null });
    const { useNotificationStore } = await import('./notificationStore');
    const asked = {
      id: 'n1', session_id: 's1', session_title: null, type: 'question' as const, title: 't', body: '',
      priority: 'normal', status: 'pending', options: null, answer: null, answered_by: null, answered_at: null,
      created_at: '2026-11-07T10:00:00Z', expires_at: null,
    };
    useNotificationStore.setState({ byId: { n1: asked } });

    dispatch({ type: 'notification_dismissed', notification_id: 'n1', session_id: 's1', notification_type: 'question', title: 't' });
    await vi.advanceTimersByTimeAsync(500);

    expect(api.listSessions).toHaveBeenCalledTimes(1);
    expect(useNotificationStore.getState().byId.n1?.status).toBe('dismissed');
  });

  it('a task event reloads the list: a plan decided in another place clears "Plan to review"', async () => {
    vi.useFakeTimers();
    useChatStore.setState({ sessionsNextOffset: 0, sessionsHasMore: false, archivedSessions: null, systemSessions: null });
    const task = { id: 't1', title: 'Upgrade CI runners', status: 'done' };
    dispatch({ type: 'task_updated', session_id: null, event: 'done', task } as never);
    expect(api.listSessions).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(500);
    expect(api.listSessions).toHaveBeenCalledTimes(1);
  });
});

describe('unsent work on re-entry', () => {
  const takePending = ws.takePendingMessages as unknown as ReturnType<typeof vi.fn>;
  let unregister: () => void = () => {};

  beforeEach(() => {
    localStorage.clear();
    useChatStore.setState({ drafts: {}, activeSession: 's1' });
    takePending.mockReturnValue([]);
  });

  afterEach(() => {
    unregister();
    unregister = () => {};
    vi.unstubAllGlobals();
    useHostedStore.setState({ mode: 'local', problem: null, reentering: false });
  });

  it('writes the composer text at once', () => {
    unregister = registerComposerFlush(() => useChatStore.getState().setDraft('s1', 'half a thought'));

    useChatStore.getState().keepUnsentWork();

    expect(useChatStore.getState().drafts.s1).toBe('half a thought');
    expect(localStorage.getItem('nerve_draft_s1')).toBe('half a thought');
  });

  it('puts queued messages back into the drafts, before the composer text', () => {
    unregister = registerComposerFlush(() => useChatStore.getState().setDraft('s1', 'typed later'));
    takePending.mockReturnValue([
      { session_id: 's1', content: 'queued first' },
      { session_id: 's2', content: 'for another chat' },
      { session_id: 's1', content: 'queued second' },
      { session_id: 's1', content: '   ' },
    ]);

    useChatStore.getState().keepUnsentWork();

    const expected = 'queued first\n\nqueued second\n\ntyped later';
    expect(useChatStore.getState().drafts.s1).toBe(expected);
    expect(localStorage.getItem('nerve_draft_s1')).toBe(expected);
    expect(useChatStore.getState().drafts.s2).toBe('for another chat');
    expect(localStorage.getItem('nerve_draft_s2')).toBe('for another chat');
  });

  it('works without a mounted composer', () => {
    takePending.mockReturnValue([{ session_id: 's3', content: 'queued' }]);

    useChatStore.getState().keepUnsentWork();

    expect(localStorage.getItem('nerve_draft_s3')).toBe('queued');
  });

  it('stops using a composer flush after it is removed', () => {
    const flush = vi.fn();
    registerComposerFlush(flush)();

    useChatStore.getState().keepUnsentWork();

    expect(flush).not.toHaveBeenCalled();
  });

  it('is done before re-entry leaves the page', () => {
    setAuthMode('external');
    let stored: Record<string, string | null> = {};
    const assign = vi.fn(() => {
      stored = {
        s1: localStorage.getItem('nerve_draft_s1'),
        s2: localStorage.getItem('nerve_draft_s2'),
      };
    });
    vi.stubGlobal('location', { ...window.location, pathname: '/chat/s1', search: '', assign });
    unregister = registerComposerFlush(() => useChatStore.getState().setDraft('s1', 'in the composer'));
    takePending.mockReturnValue([{ session_id: 's2', content: 'waiting to send' }]);

    reenter();

    expect(assign).toHaveBeenCalledOnce();
    expect(stored).toEqual({ s1: 'in the composer', s2: 'waiting to send' });
  });
});

// ------------------------------------------------------------------ //
//  Agents                                                              //
// ------------------------------------------------------------------ //

const SID = fx.FX_SESSION_ID;
const row = (id: string, extra: Record<string, unknown> = {}) =>
  ({ id, title: id, source: 'web', updated_at: '2026-02-27T02:00:00Z', ...extra }) as never;
const dispatch = (msg: Parameters<ReturnType<typeof useChatStore.getState>['handleWSMessage']>[0]) =>
  useChatStore.getState().handleWSMessage(msg);

function onSession(extra: Record<string, unknown> = {}) {
  useChatStore.setState({
    activeSession: SID, virtualSession: null, messages: [], isStreaming: false, streamingBlocks: [],
    sessions: [row(SID), row('s-other')], searchResults: null,
    panels: [], activePanelId: null, panelVisible: false, ...extra,
  });
  useAgentStore.getState().reset(SID);
}

describe('agent events in the dispatcher', () => {
  beforeEach(() => onSession());

  it('routes agent_update, agent_event and queue_update to the agent store', () => {
    dispatch({ type: 'agent_update', session_id: SID, agents: [fx.FX_RUNNING], summary: fx.makeSummary([fx.FX_RUNNING]) });
    expect(useAgentStore.getState().agents[fx.FX_RUNNING.id]).toEqual(fx.FX_RUNNING);
    expect(useAgentStore.getState().summary.live).toBe(1);

    useAgentStore.setState({
      transcripts: { [fx.FX_RUNNING.id]: {
        status: 'ready', items: [], firstSeq: null, lastSeq: null, hasMoreBefore: false, pending: [], gapFetching: false,
      } },
    });
    const item = { seq: 1, at: '2026-02-27T02:05:00Z', kind: 'text' as const, text: 'hello' };
    dispatch({ type: 'agent_event', session_id: SID, agent_id: fx.FX_RUNNING.id, item });
    expect(useAgentStore.getState().transcripts[fx.FX_RUNNING.id].items).toEqual([item]);

    const entry = fx.makeQueueEntry();
    dispatch({ type: 'queue_update', session_id: SID, queue: [entry] });
    expect(useAgentStore.getState().queue).toEqual([entry]);
  });

  it('drops the echoed bubble of a queued message that never ran', () => {
    const echo = (content: string, client_msg_id: string) => ({
      role: 'user' as const, blocks: [{ type: 'text' as const, content }], client_msg_id,
    });
    onSession({ messages: [echo('cancelled', 'c-1'), echo('refused', 'c-2'), echo('started', 'c-3')] });

    dispatch({ type: 'queue_update', session_id: SID, queue: [], removed: [
      { queue_id: 'q_1', client_msg_id: 'c-1', reason: 'cancelled' },
      { queue_id: 'q_2', client_msg_id: 'c-2', reason: 'refused' },
      { queue_id: 'q_3', client_msg_id: 'c-3', reason: 'started' },
    ] });

    expect(useChatStore.getState().messages.map(m => m.client_msg_id)).toEqual(['c-3']);
  });

  it('moves the echo of a message that starts after the replies before it', () => {
    const user = (content: string, client_msg_id?: string) => ({
      role: 'user' as const, blocks: [{ type: 'text' as const, content }], created_at: '2026-02-27T02:00:00Z', client_msg_id,
    });
    const reply = (content: string) => ({ role: 'assistant' as const, blocks: [{ type: 'text' as const, content }] });
    const texts = () => useChatStore.getState().messages.map(m => (m.blocks[0] as { content: string }).content);
    // Two sends from another tab wait behind a turn; the second one was moved up.
    onSession({ messages: [user('run the tests'), user('first', 'c-1'), user('second', 'c-2'), reply('tests pass')] });

    dispatch({ type: 'queue_update', session_id: SID, queue: [fx.makeQueueEntry()], removed: [
      { queue_id: 'q_2', client_msg_id: 'c-2', reason: 'started' },
    ] });
    expect(texts()).toEqual(['run the tests', 'first', 'tests pass', 'second']);
    const moved = useChatStore.getState().messages[3];
    expect(moved.client_msg_id).toBe('c-2');
    expect(moved.created_at).not.toBe('2026-02-27T02:00:00Z');

    useChatStore.setState(s => ({ messages: [...s.messages, reply('second done')] }));
    dispatch({ type: 'queue_update', session_id: SID, queue: [], removed: [
      { queue_id: 'q_1', client_msg_id: 'c-1', reason: 'started' },
    ] });
    expect(texts()).toEqual(['run the tests', 'tests pass', 'second', 'second done', 'first']);
  });

  it('drops the agent events of a session that is not on screen', () => {
    dispatch({ type: 'agent_update', session_id: 's-other', agents: [{ ...fx.FX_RUNNING, session_id: 's-other' }], summary: fx.makeSummary([]) });
    dispatch({ type: 'queue_update', session_id: 's-other', queue: [fx.makeQueueEntry()] });

    expect(useAgentStore.getState().agents).toEqual({});
    expect(useAgentStore.getState().queue).toEqual([]);
  });

  it('applies session_agents to any listed session (a global event)', () => {
    useChatStore.setState({ searchResults: [row('s-other')] });

    dispatch({ type: 'session_agents', session_id: 's-other', background_count: 3, has_background_tasks: true });

    const other = useChatStore.getState().sessions.find(x => x.id === 's-other')!;
    expect([other.background_count, other.has_background_tasks]).toEqual([3, true]);
    expect(useChatStore.getState().searchResults![0].background_count).toBe(3);
    expect(useChatStore.getState().sessions.find(x => x.id === SID)!.background_count).toBeUndefined();
  });

  it('ignores the superseded subagent and background-task events', () => {
    dispatch({ type: 'subagent_start', session_id: SID, tool_use_id: 'toolu_a', subagent_type: 'Explore', description: 'x' });
    dispatch({ type: 'subagent_complete', session_id: SID, tool_use_id: 'toolu_a', duration_ms: 1 });
    dispatch({ type: 'background_tasks_update', session_id: SID, tasks: [{ task_id: 't', label: 'x', tool: 'Agent', status: 'running' }] });

    expect(useChatStore.getState().panels).toEqual([]);
    expect(useChatStore.getState().messages).toEqual([]);
  });

  it('keeps the turn streaming when the server refuses an agent subscription', () => {
    const streaming = [{ type: 'text' as const, content: 'Working on it' }];
    onSession({ isStreaming: true, streamingBlocks: streaming });

    dispatch({ type: 'error', session_id: SID, error: 'agent_not_found' });

    expect(useChatStore.getState().isStreaming).toBe(true);
    expect(useChatStore.getState().streamingBlocks).toEqual(streaming);
    expect(useChatStore.getState().messages).toEqual([]);

    // Any other error still ends the turn's stream, as before.
    dispatch({ type: 'error', session_id: SID, error: 'The runtime failed' });
    expect(useChatStore.getState().isStreaming).toBe(false);
    expect(useChatStore.getState().messages).toHaveLength(1);
  });
});

describe('sending while a turn runs', () => {
  beforeEach(() => onSession({ isStreaming: true }));

  it('queues the message as a pending send without a bubble', async () => {
    await useChatStore.getState().sendMessage('also run the e2e suite');

    const pending = Object.values(useAgentStore.getState().pendingSends);
    expect(pending).toHaveLength(1);
    expect(pending[0]).toMatchObject({ content: 'also run the e2e suite', blocks: [{ type: 'text', content: 'also run the e2e suite' }] });
    expect(ws.sendMessage).toHaveBeenCalledWith('also run the e2e suite', SID, undefined, pending[0].clientMsgId);
    expect(useChatStore.getState().messages).toEqual([]);
    expect(useChatStore.getState().isStreaming).toBe(true);
  });

  it('turns the pending send into a bubble when the server starts it', async () => {
    await useChatStore.getState().sendMessage('also run the e2e suite');
    const [pending] = Object.values(useAgentStore.getState().pendingSends);

    dispatch({ type: 'queue_update', session_id: SID, queue: [], removed: [{ queue_id: 'q_1', client_msg_id: pending.clientMsgId, reason: 'started' }] });

    const messages = useChatStore.getState().messages;
    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatchObject({ role: 'user', blocks: pending.blocks, client_msg_id: pending.clientMsgId, actor_id: null });
    expect(useAgentStore.getState().pendingSends).toEqual({});
  });

  it('keeps streaming when the server refuses the queued send', async () => {
    await useChatStore.getState().sendMessage('also run the e2e suite');
    const [pending] = Object.values(useAgentStore.getState().pendingSends);

    dispatch({ type: 'error', session_id: SID, error: 'Nerve is shutting down', client_msg_id: pending.clientMsgId });

    expect(useChatStore.getState().isStreaming).toBe(true);
    expect(useChatStore.getState().messages).toEqual([
      { role: 'assistant', blocks: [{ type: 'text', content: 'Error: Nerve is shutting down' }] },
    ]);
    expect(useAgentStore.getState().pendingSends).toEqual({});
    expect(useAgentStore.getState().composerInsert).toMatchObject({ text: 'also run the e2e suite' });
  });

  it('ends the stream for a refused send this tab did not queue', () => {
    dispatch({ type: 'error', session_id: SID, error: 'Nerve is shutting down', client_msg_id: 'other-tab' });

    expect(useChatStore.getState().isStreaming).toBe(false);
  });

  it('reports a send that could not leave the tab', async () => {
    vi.mocked(ws.sendMessage).mockReturnValueOnce('dropped');

    await useChatStore.getState().sendMessage('lost');

    expect(useAgentStore.getState().pendingSends).toEqual({});
    expect(useChatStore.getState().messages[0].role).toBe('assistant');
    expect(useChatStore.getState().isStreaming).toBe(true);
  });

  it('stamps the client id on the optimistic row when idle', async () => {
    useChatStore.setState({ isStreaming: false });

    await useChatStore.getState().sendMessage('hello');

    const [message] = useChatStore.getState().messages;
    const sentId = vi.mocked(ws.sendMessage).mock.calls[0][3];
    expect(sentId).toBeTruthy();
    expect(message.client_msg_id).toBe(sentId);
    expect(useAgentStore.getState().pendingSends).toEqual({});
  });
});

describe('socket open', () => {
  it('re-binds the session on screen and resubscribes its transcripts', async () => {
    onSession();
    const resubscribeAll = vi.fn();
    const original = useAgentStore.getState().resubscribeAll;
    useAgentStore.setState({ resubscribeAll });
    try {
      onSocketOpen?.();
      expect(ws.switchSession).toHaveBeenCalledWith(SID);
      expect(resubscribeAll).toHaveBeenCalledOnce();
      // The first open loads nothing more; a reconnect fetches the sidebar
      // and the notifications again (they may have changed while the socket
      // was down).
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(api.listSessions).not.toHaveBeenCalled();
      expect(api.listNotifications).not.toHaveBeenCalled();
      onSocketOpen?.();
      expect(api.listSessions).toHaveBeenCalled();
      await vi.waitFor(() => expect(api.listNotifications).toHaveBeenCalledOnce());

      vi.mocked(ws.switchSession).mockClear();
      useChatStore.setState({ activeSession: 'v-1', virtualSession: { id: 'v-1', title: '', source: 'web', updated_at: '' } });
      onSocketOpen?.();
      useChatStore.setState({ activeSession: '', virtualSession: null });
      onSocketOpen?.();
      expect(ws.switchSession).not.toHaveBeenCalled();
    } finally {
      useAgentStore.setState({ resubscribeAll: original });
    }
    expect(onSocketOpen).toBeTypeOf('function');
  });
});

describe('catching up after a reconnect', () => {
  const userRow = (id: number, text: string) => ({ id, role: 'user', content: text });
  const replyRow = (id: number, text: string) => ({ id, role: 'assistant', content: text, blocks: [{ type: 'text', content: text }] });
  const texts = () => useChatStore.getState().messages.map(m => m.blocks.map(b => ('content' in b ? b.content : '')).join(''));
  const shown = (id: number, role: 'user' | 'assistant', text: string) =>
    ({ id, role, blocks: [{ type: 'text' as const, content: text }] });

  it('loads the messages of the open chat on a reconnect', async () => {
    onSession({ messages: [shown(1, 'user', 'hi')] });
    vi.mocked(api.getMessages).mockImplementation(async () => ({ messages: [userRow(1, 'hi'), replyRow(2, 'stored while away')] }));
    try {
      // The first open of the module loads nothing; every later open is a reconnect.
      onSocketOpen?.();
      onSocketOpen?.();
      await vi.waitFor(() => expect(texts()).toEqual(['hi', 'stored while away']));
      expect(api.getMessages).toHaveBeenCalledWith(SID);
    } finally {
      vi.mocked(api.getMessages).mockImplementation(async () => ({ messages: [] }));
    }
  });

  it('reads the tasks of the open chat again on a reconnect', async () => {
    const task = { id: 't-1', title: 'Write the report', status: 'pending', relation: 'created' } as never;
    onSession({ sessionTasks: [] });
    vi.mocked(api.getSessionTasks).mockImplementation(async () => ({ tasks: [task] }));
    try {
      onSocketOpen?.();
      onSocketOpen?.();
      await vi.waitFor(() => expect(useChatStore.getState().sessionTasks).toEqual([task]));
      expect(api.getSessionTasks).toHaveBeenCalledWith(SID);
    } finally {
      vi.mocked(api.getSessionTasks).mockImplementation(async () => ({ tasks: [] }));
    }
  });

  it('keeps the view when nothing changed', async () => {
    const before = [shown(1, 'user', 'hi'), shown(2, 'assistant', 'hello')];
    onSession({ messages: before });
    vi.mocked(api.getMessages).mockResolvedValueOnce({ messages: [userRow(1, 'hi'), replyRow(2, 'hello')] });

    await useChatStore.getState().catchUp();

    expect(useChatStore.getState().messages).toBe(before);
    expect(api.getModifiedFiles).not.toHaveBeenCalled();
  });

  it('keeps a message sent during the reconnect until the server stores it', async () => {
    onSession({ messages: [shown(1, 'user', 'hi')] });
    await useChatStore.getState().sendMessage('run the tests');
    vi.mocked(api.getMessages).mockResolvedValueOnce({ messages: [userRow(1, 'hi'), replyRow(2, 'hello')] });

    await useChatStore.getState().catchUp();

    expect(texts()).toEqual(['hi', 'hello', 'run the tests']);
    expect(useChatStore.getState().messages[2].client_msg_id).toBeTruthy();
  });

  it('reads again when a live event changes the rows during the read', async () => {
    onSession({ messages: [shown(1, 'user', 'hi')] });
    let answer: (value: { messages: unknown[] }) => void = () => {};
    vi.mocked(api.getMessages)
      .mockReturnValueOnce(new Promise((done) => { answer = done; }))
      .mockResolvedValueOnce({ messages: [userRow(1, 'hi'), replyRow(2, 'done reply'), userRow(3, 'next')] });

    const run = useChatStore.getState().catchUp();
    dispatch({ type: 'user_message', session_id: SID, content: 'next', actor_id: null });
    answer({ messages: [userRow(1, 'hi')] });
    await run;

    expect(api.getMessages).toHaveBeenCalledTimes(2);
    expect(texts()).toEqual(['hi', 'done reply', 'next']);
  });

  it('drops the read when another chat opens meanwhile', async () => {
    onSession({ messages: [shown(1, 'user', 'hi')] });
    let answer: (value: { messages: unknown[] }) => void = () => {};
    vi.mocked(api.getMessages).mockReturnValueOnce(new Promise((done) => { answer = done; }));

    const run = useChatStore.getState().catchUp();
    useChatStore.setState({ activeSession: 's-other', messages: [] });
    answer({ messages: [userRow(1, 'hi'), replyRow(2, 'hello')] });
    await run;

    expect(useChatStore.getState().messages).toEqual([]);
  });

  it('shows the stored reply when the turn ended while the socket was down', async () => {
    onSession({
      messages: [shown(1, 'user', 'hi')],
      isStreaming: true,
      streamingBlocks: [{ type: 'text', content: 'half a rep' }],
      agentStatus: { state: 'thinking' },
    });
    vi.mocked(api.getMessages).mockResolvedValueOnce({ messages: [userRow(1, 'hi'), replyRow(2, 'the whole reply')] });

    dispatch({ type: 'session_status', session_id: SID, is_running: false });

    expect(useChatStore.getState().isStreaming).toBe(false);
    expect(useChatStore.getState().streamingBlocks).toEqual([]);
    await vi.waitFor(() => expect(texts()).toEqual(['hi', 'the whole reply']));
  });
});

describe('starting a chat from the Inbox', () => {
  // An unsent new chat with a draft and a review loop set up in the chat page.
  const loop = { ...EMPTY_REVIEW_LOOP, goal: 'Ship the parser', verifier: 'Tests pass', budget: '20' };
  beforeEach(() => {
    useChatStore.setState({
      sessions: [], messages: [], isStreaming: false, streamingBlocks: [],
      activeSession: 'v-1', virtualSession: { id: 'v-1', title: '', source: 'web', updated_at: '' },
      drafts: { 'v-1': 'half a thought' }, newChatReviewLoop: loop,
      newChatBackend: 'codex', newChatModels: { codex: 'gpt-x' },
    });
  });

  it('opens a new default chat and leaves the unsent chat, its draft and its settings alone', async () => {
    vi.mocked(api.createSession).mockResolvedValueOnce({ id: 'n-1', source: 'web' });

    const started = await useChatStore.getState().startChat('Plan my week');

    expect(started).toEqual({ sessionId: 'n-1', sent: true });
    // No backend, model, work directory or review loop: the defaults.
    expect(api.createSession).toHaveBeenCalledWith();
    expect(ws.sendMessage).toHaveBeenCalledWith('Plan my week', 'n-1', undefined, expect.any(String));
    const s = useChatStore.getState();
    expect(s.activeSession).toBe('n-1');
    expect(s.sessions.map(r => r.id)).toEqual(['n-1']);
    expect(s.virtualSession?.id).toBe('v-1');
    expect(s.drafts['v-1']).toBe('half a thought');
    expect(s.newChatReviewLoop).toEqual(loop);
    expect(s.newChatBackend).toBe('codex');
    expect(s.newChatModels).toEqual({ codex: 'gpt-x' });
  });

  it('opens nothing when the chat cannot be created', async () => {
    vi.mocked(api.createSession).mockRejectedValueOnce(new Error('500'));
    vi.spyOn(console, 'error').mockImplementationOnce(() => {});

    expect(await useChatStore.getState().startChat('Plan my week')).toBeNull();

    expect(useChatStore.getState().activeSession).toBe('v-1');
    expect(ws.sendMessage).not.toHaveBeenCalled();
  });

  it('reports a message that could not be sent, and sends a retry to the same chat', async () => {
    vi.mocked(api.createSession).mockResolvedValueOnce({ id: 'n-1', source: 'web' });
    vi.mocked(ws.sendMessage).mockReturnValueOnce('dropped');

    expect(await useChatStore.getState().startChat('Plan my week')).toEqual({ sessionId: 'n-1', sent: false });
    expect(await useChatStore.getState().startChat('Plan my week', 'n-1')).toEqual({ sessionId: 'n-1', sent: true });

    expect(api.createSession).toHaveBeenCalledTimes(1);
    expect(ws.sendMessage).toHaveBeenLastCalledWith('Plan my week', 'n-1', undefined, expect.any(String));
    expect(useChatStore.getState().sessions.map(r => r.id)).toEqual(['n-1']);
  });

  it('starts a new chat when the chat of the failed send is gone', async () => {
    vi.mocked(api.createSession).mockResolvedValueOnce({ id: 'n-2', source: 'web' });

    expect(await useChatStore.getState().startChat('Plan my week', 'n-1')).toEqual({ sessionId: 'n-2', sent: true });
    expect(api.createSession).toHaveBeenCalledTimes(1);
  });
});

describe('switching sessions', () => {
  it('resets the agent store and loads the roster', async () => {
    onSession();
    useAgentStore.getState().hydrate(fx.makeSnapshot([fx.FX_RUNNING]));
    vi.mocked(api.getSessionAgents).mockResolvedValueOnce(fx.makeSnapshot([], { session_id: 's-next' }));

    await useChatStore.getState().switchSession('s-next');

    expect(useAgentStore.getState().sessionId).toBe('s-next');
    expect(useAgentStore.getState().agents).toEqual({});
    expect(api.getSessionAgents).toHaveBeenCalledWith('s-next');
  });

  it('drops the Daybreak switch of an untouched new chat that is left', async () => {
    // The switch belongs to the unsent chat: the next new chat starts off.
    useChatStore.setState({
      sessions: [], messages: [], drafts: {}, newChatReviewLoop: null,
      activeSession: 'v-1', virtualSession: { id: 'v-1', title: '', source: 'web', updated_at: '' },
      newChatDaybreak: true,
    });

    await useChatStore.getState().switchSession('s-next');

    expect(useChatStore.getState().virtualSession).toBeNull();
    expect(useChatStore.getState().newChatDaybreak).toBe(false);
  });
});

describe('a chat that is left while its history is on its way', () => {
  const row = (id: number, session: string) => ({
    id, role: 'assistant', session_id: session, blocks: [{ type: 'text', content: `of ${session}` }],
  });
  /** `chat-a` answers when the test says so; every other chat at once, with one message of its own. */
  function slowChatA() {
    const held: { answer: (value: unknown) => void; fail: (error: Error) => void } = { answer: () => {}, fail: () => {} };
    vi.mocked(api.getMessages).mockImplementation(((id: string) => (id === 'chat-a'
      ? new Promise((resolve, reject) => { held.answer = resolve; held.fail = reject; })
      : Promise.resolve({ messages: [row(2, id)] }))) as never);
    return held;
  }

  beforeEach(() => {
    useChatStore.setState({ activeSession: '', messages: [], virtualSession: null, loading: false });
  });

  afterEach(() => {
    vi.mocked(api.getMessages).mockImplementation(async () => ({ messages: [] }) as never);
  });

  it('does not show that history under the chat the person opened next', async () => {
    const chatA = slowChatA();
    const opening = useChatStore.getState().switchSession('chat-a');
    await useChatStore.getState().switchSession('chat-b');
    expect(useChatStore.getState().messages.map(m => [m.id, m.session_id])).toEqual([[2, 'chat-b']]);
    const asked = vi.mocked(api.getModifiedFiles).mock.calls.length;

    // The history of the chat that was left arrives now.
    chatA.answer({ messages: [row(1, 'chat-a')], last_usage: { input_tokens: 9, output_tokens: 9 } });
    await opening;

    expect(useChatStore.getState().activeSession).toBe('chat-b');
    expect(useChatStore.getState().messages.map(m => [m.id, m.session_id])).toEqual([[2, 'chat-b']]);
    expect(useChatStore.getState().loading).toBe(false);
    // Nor is anything more read for the chat that was left.
    expect(vi.mocked(api.getModifiedFiles).mock.calls.length).toBe(asked);
    expect(vi.mocked(api.getModifiedFiles)).not.toHaveBeenCalledWith('chat-a');
  });

  it('does not end the loading of the next chat when that history fails', async () => {
    const chatA = slowChatA();
    const opening = useChatStore.getState().switchSession('chat-a');
    // The next chat is still loading (its own history has not answered yet).
    useChatStore.setState({ activeSession: 'chat-b', messages: [], loading: true });

    chatA.fail(new Error('timeout'));
    await opening;

    expect(useChatStore.getState()).toMatchObject({ activeSession: 'chat-b', loading: true, messages: [] });
  });

  it('shows the history when the person came back to the chat before it arrived', async () => {
    const chatA = slowChatA();
    const opening = useChatStore.getState().switchSession('chat-a');
    await useChatStore.getState().switchSession('chat-b');
    // Back to the first chat, whose first read is still out.
    useChatStore.setState({ activeSession: 'chat-a', messages: [], loading: true });

    chatA.answer({ messages: [row(1, 'chat-a')] });
    await opening;

    expect(useChatStore.getState().messages.map(m => [m.id, m.session_id])).toEqual([[1, 'chat-a']]);
    expect(useChatStore.getState().loading).toBe(false);
  });
});

describe('archiving a session with running agents', () => {
  const busy = { archived: true, archived_ids: [], skipped: [{ id: SID, reason: 'background agents running' }] };

  beforeEach(() => {
    onSession();
    localStorage.setItem(`nerve_draft_${SID}`, 'unsent');
    useChatStore.setState({ drafts: { [SID]: 'unsent' } });
  });

  it('asks, then archives with force', async () => {
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(true);
    vi.mocked(api.archiveSession)
      .mockResolvedValueOnce(busy)
      .mockResolvedValueOnce({ archived: true, archived_ids: [SID], skipped: [] });

    await useChatStore.getState().archiveSession(SID);

    expect(confirm).toHaveBeenCalledWith('Agents are still running in this session. Archiving stops them. Archive anyway?');
    expect(vi.mocked(api.archiveSession).mock.calls).toEqual([[SID], [SID, { force: true }]]);
    expect(useChatStore.getState().drafts[SID]).toBeUndefined();
    confirm.mockRestore();
  });

  it('keeps the session and its draft when the person declines', async () => {
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false);
    vi.mocked(api.archiveSession).mockResolvedValueOnce(busy);

    await useChatStore.getState().archiveSession(SID);

    expect(api.archiveSession).toHaveBeenCalledTimes(1);
    expect(useChatStore.getState().drafts[SID]).toBe('unsent');
    confirm.mockRestore();
  });
});

describe('deleting a running session', () => {
  beforeEach(() => {
    onSession();
    localStorage.setItem(`nerve_draft_${SID}`, 'unsent');
    useChatStore.setState({ drafts: { [SID]: 'unsent' } });
  });

  it('says why the server refused, and keeps the draft', async () => {
    const alert = vi.spyOn(window, 'alert').mockImplementation(() => {});
    vi.mocked(api.deleteSession).mockRejectedValueOnce(
      new Error('409: {"detail":"The session is running or has queued messages. Stop it, then delete it."}'),
    );

    await useChatStore.getState().deleteSession(SID);

    expect(alert).toHaveBeenCalledWith('Could not delete this chat: it is running or has queued messages. Stop it, then delete it.');
    expect(useChatStore.getState().drafts[SID]).toBe('unsent');
    alert.mockRestore();
  });

  it('shows nothing for another error', async () => {
    const alert = vi.spyOn(window, 'alert').mockImplementation(() => {});
    vi.mocked(api.deleteSession).mockRejectedValueOnce(new Error('500: boom'));

    await useChatStore.getState().deleteSession(SID);

    expect(alert).not.toHaveBeenCalled();
    alert.mockRestore();
  });
});

describe('stopping the turn', () => {
  it('leaves streaming mode when the server has no turn running', async () => {
    onSession({ isStreaming: true, streamingBlocks: [{ type: 'text', content: 'partial' }] });
    vi.mocked(api.stopSession).mockResolvedValueOnce({ scope: 'turn', turn: 'not_running', would_end: [], shell_count: 0 });

    await useChatStore.getState().stopSession();

    expect(api.stopSession).toHaveBeenCalledWith(SID, { scope: 'turn', confirm_kill_background: false });
    expect(useChatStore.getState().isStreaming).toBe(false);
    expect(useChatStore.getState().messages.map(m => m.blocks)).toEqual([[{ type: 'text', content: 'partial' }]]);
    expect(ws.stopSession).not.toHaveBeenCalled();
  });

  it('falls back to the socket frame when the route does not answer', async () => {
    onSession({ isStreaming: true });
    vi.mocked(api.stopSession).mockRejectedValueOnce(new Error('404: {"detail": "Not Found"}'));
    const quiet = vi.spyOn(console, 'error').mockImplementation(() => {});

    await useChatStore.getState().stopSession();

    expect(ws.stopSession).toHaveBeenCalledWith(SID);
    expect(useChatStore.getState().isStreaming).toBe(true);
    quiet.mockRestore();
  });
});

describe('agent tabs', () => {
  beforeEach(() => {
    onSession();
    useAgentStore.getState().hydrate(fx.makeSnapshot(fx.FX_WAVE));
    vi.mocked(api.getAgentTranscript).mockResolvedValue({ agent_id: 'x', items: [], first_seq: null, last_seq: null, has_more_before: false });
  });

  it('opens one transcript tab per agent and streams it', () => {
    useChatStore.getState().openAgentTab(fx.FX_RUNNING.id);
    useChatStore.getState().openAgentTab(fx.FX_RUNNING.id);

    const tabs = useChatStore.getState().panels;
    expect(tabs).toHaveLength(1);
    expect(tabs[0]).toMatchObject({ id: `agent:${fx.FX_RUNNING.id}`, type: 'agent', agentId: fx.FX_RUNNING.id, label: 'build-api', status: 'running' });
    expect(useChatStore.getState().activePanelId).toBe(`agent:${fx.FX_RUNNING.id}`);
    expect(useAgentStore.getState().transcripts[fx.FX_RUNNING.id]).toBeDefined();
    expect(ws.subscribeAgent).toHaveBeenCalledWith(SID, fx.FX_RUNNING.id);
  });

  it('closes the transcript with its tab', () => {
    useChatStore.getState().openAgentTab(fx.FX_RUNNING.id);
    useChatStore.getState().closePanelTab(`agent:${fx.FX_RUNNING.id}`);

    expect(useAgentStore.getState().transcripts[fx.FX_RUNNING.id]).toBeUndefined();
    expect(ws.unsubscribeAgent).toHaveBeenCalledWith(SID, fx.FX_RUNNING.id);
  });

  it(`keeps at most ${MAX_AGENT_TABS} agent tabs, closing the least recently focused`, () => {
    const many = Array.from({ length: MAX_AGENT_TABS + 1 }, () => fx.makeAgentView());
    useAgentStore.getState().hydrate(fx.makeSnapshot(many));
    many.slice(0, MAX_AGENT_TABS).forEach(a => useChatStore.getState().openAgentTab(a.id));
    // The first tab is used again, so the second is now the stalest.
    useChatStore.getState().focusPanelTab(`agent:${many[0].id}`);

    useChatStore.getState().openAgentTab(many[MAX_AGENT_TABS].id);

    const open = useChatStore.getState().panels.map(p => p.agentId);
    expect(open).toHaveLength(MAX_AGENT_TABS);
    expect(open).not.toContain(many[1].id);
    expect(open).toContain(many[0].id);
    expect(useAgentStore.getState().transcripts[many[1].id]).toBeUndefined();
  });

  it('toggles the Agents panel and asks it for focus', () => {
    const nonce = useAgentStore.getState().focusNonce;

    useChatStore.getState().toggleAgentsPanel();
    expect(useChatStore.getState()).toMatchObject({ activePanelId: AGENTS_PANEL_ID, panelVisible: true });
    expect(useChatStore.getState().panels[0]).toMatchObject({ type: 'agents', label: 'Agents', status: 'running' });
    expect(useAgentStore.getState().focusNonce).toBe(nonce + 1);

    useChatStore.getState().toggleAgentsPanel();
    expect(useChatStore.getState().panelVisible).toBe(false);
    expect(useChatStore.getState().panels).toHaveLength(1);
  });

  it('never prunes the Agents panel or agent tabs', () => {
    const done = (id: string) => ({
      id, type: 'subagent' as const, label: id, subagentType: 'Explore', description: '', content: null, prompt: '',
      streaming: false, status: 'complete' as const, startedAt: 1, completedAt: Number(id.slice(1)), blocks: [],
    });
    useChatStore.getState().openAgentsPanel();
    useChatStore.getState().openAgentTab(fx.FX_RUNNING.id);
    for (let i = 1; i <= 7; i++) useChatStore.getState().openPanelTab(done(`t${i}`));

    const ids = useChatStore.getState().panels.map(p => p.id);
    expect(ids).toContain(AGENTS_PANEL_ID);
    expect(ids).toContain(`agent:${fx.FX_RUNNING.id}`);
    expect(ids.filter(id => id.startsWith('t'))).toEqual(['t3', 't4', 't5', 't6', 't7']);
  });
});

describe('a session that the server does not have', () => {
  beforeEach(() => onSession({ activeSession: 'gone1234', missingSession: null, loading: true }));

  it('is marked when the server answers the switch, and loading ends', () => {
    dispatch({ type: 'session_not_found', session_id: 'gone1234' });

    expect(useChatStore.getState().missingSession).toBe('gone1234');
    expect(useChatStore.getState().loading).toBe(false);
  });

  it('is not marked by the answer for a session that is no longer on screen', () => {
    useChatStore.setState({ activeSession: SID });

    dispatch({ type: 'session_not_found', session_id: 'gone1234' });

    expect(useChatStore.getState().missingSession).toBeNull();
    expect(useChatStore.getState().loading).toBe(true);
  });

  it('leaves nothing of the session in the view', () => {
    // A tab that was open on the session learns at a reconnect that it is gone.
    useChatStore.setState({
      messages: [{ role: 'user', blocks: [{ type: 'text', content: 'Sort the inbox.' }] }] as never,
      isStreaming: true, agentStatus: { state: 'thinking' },
      streamingBlocks: [{ type: 'text', content: 'On it' }] as never,
      currentTodos: [{ content: 'Sort', status: 'pending' }] as never,
      contextUsage: { used: 1, max: 2 } as never,
      pendingInteraction: { interactionId: 'i-1' } as never,
      modifiedFiles: [{ path: 'a.txt' }] as never, modifiedFilesCount: 1,
      sessionTasks: [{ id: 't-1' }] as never,
    });
    useChatStore.getState().openAgentsPanel();
    useAgentStore.getState().hydrate(fx.makeSnapshot([fx.FX_RUNNING]));

    dispatch({ type: 'session_not_found', session_id: 'gone1234' });

    const state = useChatStore.getState();
    expect(state.messages).toEqual([]);
    expect(state.isStreaming).toBe(false);
    expect(state.agentStatus).toEqual({ state: 'idle' });
    expect(state.streamingBlocks).toEqual([]);
    expect(state.currentTodos).toEqual([]);
    expect(state.contextUsage).toBeNull();
    expect(state.pendingInteraction).toBeNull();
    expect(state.modifiedFilesCount).toBe(0);
    expect(state.sessionTasks).toBeNull();
    expect(state.panels).toEqual([]);
    expect(state.panelVisible).toBe(false);
    expect(useAgentStore.getState().agents).toEqual({});
  });

  it('writes what the composer holds to the draft before the composer goes out of sight', () => {
    const flush = vi.fn(() => useChatStore.getState().setDraft('gone1234', 'half a thought'));
    const unregister = registerComposerFlush(flush);
    try {
      dispatch({ type: 'session_not_found', session_id: 'gone1234' });
    } finally {
      unregister();
    }

    expect(flush).toHaveBeenCalledOnce();
    expect(useChatStore.getState().drafts.gone1234).toBe('half a thought');
  });

  it('is not marked, and the view is not emptied, by an answer that names no session', () => {
    // The server answers a frame whose session_id is no session ID.
    useChatStore.setState({
      messages: [{ role: 'user', blocks: [{ type: 'text', content: 'Sort the inbox.' }] }] as never,
    });

    for (const id of [null, '', 0, false]) dispatch({ type: 'session_not_found', session_id: id as never });

    expect(useChatStore.getState().missingSession).toBeNull();
    expect(useChatStore.getState().messages).toHaveLength(1);
    expect(useChatStore.getState().loading).toBe(true);
  });

  it('is opened again when it runs, since it exists then', async () => {
    dispatch({ type: 'session_not_found', session_id: 'gone1234' });
    vi.mocked(ws.switchSession).mockClear();
    vi.mocked(api.getMessages).mockResolvedValueOnce({
      messages: [{ id: 1, role: 'user', content: 'Run the nightly report.', created_at: '2026-08-05T02:00:00Z' }],
    } as never);

    dispatch({ type: 'session_running', session_id: 'gone1234', is_running: true } as never);

    expect(useChatStore.getState().missingSession).toBeNull();
    expect(ws.switchSession).toHaveBeenCalledWith('gone1234');
    await vi.waitFor(() => expect(useChatStore.getState().messages).toHaveLength(1));
    // The switch, not the event, decides what the view shows.
    expect(useChatStore.getState().isStreaming).toBe(false);
    // The same event for a session that is not missing opens nothing.
    vi.mocked(ws.switchSession).mockClear();
    dispatch({ type: 'session_running', session_id: 'gone1234', is_running: false } as never);
    expect(ws.switchSession).not.toHaveBeenCalled();
  });

  it('gives its row the running state of the event that opens it again', () => {
    useChatStore.setState({ sessions: [row('gone1234'), row('s-other')] as never });
    dispatch({ type: 'session_not_found', session_id: 'gone1234' });

    dispatch({
      type: 'session_running', session_id: 'gone1234', is_running: true,
      has_background_tasks: true, background_count: 2,
    } as never);

    const listed = useChatStore.getState().sessions.find(sess => sess.id === 'gone1234')!;
    expect(listed.is_running).toBe(true);
    expect(listed.background_count).toBe(2);
  });

  it('is forgotten when another session opens', async () => {
    dispatch({ type: 'session_not_found', session_id: 'gone1234' });

    await useChatStore.getState().switchSession('s-other');

    expect(useChatStore.getState().missingSession).toBeNull();
    expect(ws.switchSession).toHaveBeenCalledWith('s-other');
  });

  it('is named again when the socket opens, since it can exist by now', () => {
    dispatch({ type: 'session_not_found', session_id: 'gone1234' });
    vi.mocked(ws.switchSession).mockClear();

    onSocketOpen?.();

    expect(ws.switchSession).toHaveBeenCalledWith('gone1234');
    // The same answer keeps the mark; the status of the session clears it.
    dispatch({ type: 'session_not_found', session_id: 'gone1234' });
    expect(useChatStore.getState().missingSession).toBe('gone1234');
    dispatch({ type: 'session_status', session_id: 'gone1234', is_running: false });
    expect(useChatStore.getState().missingSession).toBeNull();
    expect(onSocketOpen).toBeTypeOf('function');
  });

  it('is named again when its address is opened again', async () => {
    dispatch({ type: 'session_not_found', session_id: 'gone1234' });
    vi.mocked(ws.switchSession).mockClear();

    await useChatStore.getState().switchSession('gone1234');

    expect(useChatStore.getState().missingSession).toBeNull();
    expect(ws.switchSession).toHaveBeenCalledWith('gone1234');
  });
});

describe('a new chat from a session that does not exist', () => {
  beforeEach(() => {
    onSession({ activeSession: 'gone1234', missingSession: 'gone1234', drafts: { gone1234: 'half a thought' } });
    vi.mocked(api.getMessages).mockResolvedValue({ messages: [] } as never);
  });

  it('takes what was typed for the session', async () => {
    await useChatStore.getState().createSession();

    const { virtualSession, activeSession, drafts } = useChatStore.getState();
    expect(virtualSession).not.toBeNull();
    expect(activeSession).toBe(virtualSession!.id);
    expect(drafts[virtualSession!.id]).toBe('half a thought');
    expect(drafts.gone1234).toBe('');
  });

  it('puts it after the text that an unsent chat holds already', async () => {
    useChatStore.setState({
      virtualSession: { id: 'v-1', title: '', source: 'web', updated_at: '' },
      drafts: { 'v-1': 'older unsent prompt', gone1234: 'important reply' },
    });

    await useChatStore.getState().createSession();

    const { activeSession, drafts } = useChatStore.getState();
    expect(activeSession).toBe('v-1');
    expect(drafts['v-1']).toBe('older unsent prompt\n\nimportant reply');
    expect(drafts.gone1234).toBe('');
  });

  it('leaves the text alone when the unsent chat is the one marked missing', async () => {
    // No flow marks an unsent chat missing; the move must not eat the text if one did.
    useChatStore.setState({
      activeSession: 'v-1', missingSession: 'v-1',
      virtualSession: { id: 'v-1', title: '', source: 'web', updated_at: '' },
      drafts: { 'v-1': 'half a thought' },
    });

    await useChatStore.getState().createSession();

    expect(useChatStore.getState().drafts['v-1']).toBe('half a thought');
  });

  it('moves nothing when nothing was typed', async () => {
    useChatStore.setState({ drafts: { gone1234: '   ' } });

    await useChatStore.getState().createSession();

    const { virtualSession, drafts } = useChatStore.getState();
    expect(drafts[virtualSession!.id] ?? '').toBe('');
  });

  it('leaves the draft of a session that exists where it is', async () => {
    onSession({ drafts: { [SID]: 'kept here' } });

    await useChatStore.getState().createSession();

    const { virtualSession, drafts } = useChatStore.getState();
    expect(drafts[SID]).toBe('kept here');
    expect(drafts[virtualSession!.id] ?? '').toBe('');
  });
});
