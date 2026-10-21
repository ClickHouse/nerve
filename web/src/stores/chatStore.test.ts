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
    listArchivedSessions: vi.fn(),
    listSystemSessions: vi.fn(),
    getViewer: vi.fn(async () => { throw new Error("401"); }),
    getMessages: vi.fn(async () => ({ messages: [] })),
    getModifiedFiles: vi.fn(async () => ({ files: [], summary: {} })),
    getSessionAgents: vi.fn(async (sessionId: string) => ({ session_id: sessionId, agents: [], queue: [] })),
    getAgentTranscript: vi.fn(),
    archiveSession: vi.fn(),
    stopSession: vi.fn(),
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
const { useChatStore, registerComposerFlush, AGENTS_PANEL_ID, MAX_AGENT_TABS } = await import('./chatStore');
const { reenter, setAuthMode, useHostedStore } = await import('../api/hosted');
const { useAgentStore } = await import('./agentStore');
const fx = await import('../test/agentFixtures');
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
  it('re-binds the session on screen and resubscribes its transcripts', () => {
    onSession();
    const resubscribeAll = vi.fn();
    const original = useAgentStore.getState().resubscribeAll;
    useAgentStore.setState({ resubscribeAll });
    try {
      onSocketOpen?.();
      expect(ws.switchSession).toHaveBeenCalledWith(SID);
      expect(resubscribeAll).toHaveBeenCalledOnce();
      // The first open loads nothing more; a reconnect fetches the sidebar
      // again (its counts may have changed while the socket was down).
      expect(api.listSessions).not.toHaveBeenCalled();
      onSocketOpen?.();
      expect(api.listSessions).toHaveBeenCalled();

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
