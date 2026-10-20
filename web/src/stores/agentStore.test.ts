// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { AgentEventItem, AgentTranscriptPage, AgentView } from '../types/agents';

// localStorage is the in-memory one src/test/setup.ts installs.
vi.mock('../api/client', async () => {
  const actual = await vi.importActual<typeof import('../api/client')>('../api/client');
  return {
    apiErrorCode: actual.apiErrorCode,
    api: {
      getSessionAgents: vi.fn(),
      getAgentDetail: vi.fn(),
      getAgentTranscript: vi.fn(),
      getAgentOutput: vi.fn(),
      stopAgent: vi.fn(),
      stopSession: vi.fn(),
      cancelQueued: vi.fn(),
    },
  };
});
vi.mock('../api/websocket', () => ({
  ws: { subscribeAgent: vi.fn(), unsubscribeAgent: vi.fn() },
}));

const { api } = await import('../api/client');
const { ApiError } = await import('../api/hosted');
const { ws } = await import('../api/websocket');
const { useAgentStore, MAX_TRANSCRIPT_ITEMS } = await import('./agentStore');
const fx = await import('../test/agentFixtures');

const SID = fx.FX_SESSION_ID;
const store = () => useAgentStore.getState();

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

function page(items: AgentEventItem[], hasMoreBefore = false, agentId = fx.FX_RUNNING.id): AgentTranscriptPage {
  return {
    agent_id: agentId, items,
    first_seq: items[0]?.seq ?? null, last_seq: items[items.length - 1]?.seq ?? null,
    has_more_before: hasMoreBefore,
  };
}

function event(item: AgentEventItem, agentId = fx.FX_RUNNING.id) {
  return { type: 'agent_event' as const, session_id: SID, agent_id: agentId, item };
}

const text = (seq: number): AgentEventItem => ({ seq, at: '2026-02-27T02:05:00Z', kind: 'text', text: `t${seq}` });
const seqs = (agentId = fx.FX_RUNNING.id) => store().transcripts[agentId]?.items.map(i => i.seq);
const flush = () => new Promise(resolve => setTimeout(resolve, 0));

function apiError(status: number, code: string) {
  return new ApiError(status, JSON.stringify({ detail: { code, message: code } }));
}

/** Open FX_RUNNING's transcript with `items` as the tail page. */
async function openWith(items: AgentEventItem[], hasMoreBefore = false, view: AgentView = fx.FX_RUNNING) {
  vi.mocked(api.getAgentTranscript).mockResolvedValueOnce(page(items, hasMoreBefore, view.id));
  await store().openTranscript(view.id);
}

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  store().reset(SID);
  store().hydrate(fx.makeSnapshot(fx.FX_WAVE));
});

describe('records', () => {
  it('keeps the higher rev per record, whichever arrives first', () => {
    const newer = { ...fx.FX_RUNNING, rev: fx.FX_RUNNING.rev + 10, activity: 'Bash: npm test' };
    const older = { ...fx.FX_RUNNING, rev: fx.FX_RUNNING.rev - 1, activity: 'stale' };
    const summary = fx.makeSummary(fx.FX_WAVE);

    store().applyAgentUpdate({ type: 'agent_update', session_id: SID, agents: [newer], summary });
    store().applyAgentUpdate({ type: 'agent_update', session_id: SID, agents: [older], summary });
    expect(store().agents[fx.FX_RUNNING.id].activity).toBe('Bash: npm test');

    // A snapshot built before the update must not roll it back.
    store().hydrate(fx.makeSnapshot([{ ...fx.FX_RUNNING, rev: fx.FX_RUNNING.rev + 5 }]));
    expect(store().agents[fx.FX_RUNNING.id].rev).toBe(newer.rev);
  });

  it('upserts single records under the rev rule without touching the summary', () => {
    const summary = store().summary;
    const extra = fx.makeAgentView({ spawn_tool_use_id: 'toolu_EXTRA' });
    const stale = { ...fx.FX_RUNNING, rev: fx.FX_RUNNING.rev - 1, activity: 'stale' };

    store().upsertAgents([extra, stale, { ...extra, id: 'ag_elsewhere01', session_id: 's-other' }]);

    expect(store().agents[extra.id]).toEqual(extra);
    expect(store().bySpawn.toolu_EXTRA).toBe(extra.id);
    expect(store().agents[fx.FX_RUNNING.id]).toEqual(fx.FX_RUNNING);
    expect(store().agents.ag_elsewhere01).toBeUndefined();
    expect(store().summary).toBe(summary);
  });

  it('never deletes records on hydrate and rebuilds the spawn index', () => {
    const late = fx.makeAgentView({ spawn_tool_use_id: 'toolu_LATE' });
    store().hydrate(fx.makeSnapshot([late]));

    expect(Object.keys(store().agents)).toHaveLength(fx.FX_WAVE.length + 1);
    expect(store().bySpawn.toolu_LATE).toBe(late.id);
    expect(store().bySpawn[fx.FX_CHILD.spawn_tool_use_id!]).toBe(fx.FX_CHILD.id);
  });

  it('replaces summary, capabilities and queue from a current snapshot', () => {
    const entry = fx.makeQueueEntry();
    const summary = { ...fx.makeSummary(fx.FX_WAVE), tokens: 1 };
    store().hydrate(fx.makeSnapshot(fx.FX_WAVE, {
      summary, queue: [entry], capabilities: { ...fx.FX_CAPABILITIES, stop_task: false },
    }));

    expect(store().queue).toEqual([entry]);
    expect(store().capabilities.stop_task).toBe(false);
    expect(store().summary).toEqual(summary);
  });

  it('keeps the summary when a snapshot is older than an applied update', () => {
    const finished = { ...fx.FX_RUNNING, status: 'completed' as const, rev: fx.FX_RUNNING.rev + 10 };
    const fresh = fx.makeSummary(fx.FX_WAVE.map(a => (a.id === finished.id ? finished : a)));
    store().applyAgentUpdate({ type: 'agent_update', session_id: SID, agents: [finished], summary: fresh });

    // Taken before the agent finished: it still lists it as running.
    store().hydrate(fx.makeSnapshot(fx.FX_WAVE));
    expect(store().summary).toEqual(fresh);
    expect(store().agents[finished.id].status).toBe('completed');

    // A newer snapshot replaces it.
    const later = { ...finished, rev: finished.rev + 1 };
    store().hydrate(fx.makeSnapshot(fx.FX_WAVE.map(a => (a.id === later.id ? later : a))));
    expect(store().summary.completed).toBe(fresh.completed);
  });

  it('treats a snapshot without a live record as older', () => {
    const late = fx.makeAgentView();
    const summary = fx.makeSummary([...fx.FX_WAVE, late]);
    store().applyAgentUpdate({ type: 'agent_update', session_id: SID, agents: [late], summary });

    store().hydrate(fx.makeSnapshot(fx.FX_WAVE));

    expect(store().summary).toEqual(summary);
  });

  it('does not revive a queue entry that already started', () => {
    const entry = fx.makeQueueEntry();
    store().applyQueueUpdate({
      type: 'queue_update', session_id: SID, queue: [],
      removed: [{ queue_id: entry.queue_id, client_msg_id: entry.client_msg_id, reason: 'started' }],
    });

    store().hydrate(fx.makeSnapshot(fx.FX_WAVE, { queue: [entry] }));

    expect(store().queue).toEqual([]);
  });

  it('ignores a snapshot or update of another session', () => {
    const other = fx.makeAgentView({ session_id: 's-other' });
    store().hydrate(fx.makeSnapshot([other], { session_id: 's-other' }));
    store().applyAgentUpdate({ type: 'agent_update', session_id: 's-other', agents: [other], summary: fx.makeSummary([other]) });

    expect(store().agents[other.id]).toBeUndefined();
    expect(store().sessionId).toBe(SID);
  });

  it('adopts the session of a snapshot when it has none', () => {
    store().reset(null);
    store().hydrate(fx.makeSnapshot([fx.FX_RUNNING], { session_id: 's-new' }));

    // FX_RUNNING belongs to the fixture session, so only the session is adopted.
    expect(store().sessionId).toBe('s-new');
    expect(store().agents).toEqual({});
  });

  it('keeps the agents object when an update changes nothing', () => {
    const before = store().agents;
    store().applyAgentUpdate({ type: 'agent_update', session_id: SID, agents: [fx.FX_RUNNING], summary: fx.makeSummary(fx.FX_WAVE) });
    expect(store().agents).toBe(before);
  });

  it('drops a snapshot that arrives after a switch', async () => {
    const answer = deferred<ReturnType<typeof fx.makeSnapshot>>();
    vi.mocked(api.getSessionAgents).mockReturnValueOnce(answer.promise);
    const loading = store().loadSnapshot(SID);
    store().reset(null);
    answer.resolve(fx.makeSnapshot([fx.FX_RUNNING]));
    await loading;

    expect(store().sessionId).toBeNull();
    expect(store().agents).toEqual({});
  });
});

describe('transcripts', () => {
  it('subscribes, loads the tail and merges items that arrived while loading', async () => {
    const tail = deferred<AgentTranscriptPage>();
    vi.mocked(api.getAgentTranscript).mockReturnValueOnce(tail.promise);
    const opening = store().openTranscript(fx.FX_RUNNING.id);

    expect(store().transcripts[fx.FX_RUNNING.id].status).toBe('loading');
    expect(ws.subscribeAgent).toHaveBeenCalledWith(SID, fx.FX_RUNNING.id);
    store().applyAgentEvent(event(text(3)));
    store().applyAgentEvent(event(text(4)));
    expect(store().transcripts[fx.FX_RUNNING.id].pending).toHaveLength(2);

    tail.resolve(page([text(1), text(2), text(3)], true));
    await opening;

    const t = store().transcripts[fx.FX_RUNNING.id];
    expect(api.getAgentTranscript).toHaveBeenCalledWith(SID, fx.FX_RUNNING.id, {});
    expect(t.status).toBe('ready');
    expect(t.items.map(i => i.seq)).toEqual([1, 2, 3, 4]);
    expect([t.firstSeq, t.lastSeq, t.hasMoreBefore]).toEqual([1, 4, true]);
    expect(t.pending).toEqual([]);
    expect(store().seen[fx.FX_RUNNING.id]).toBe(true);
  });

  it('appends the next seq live and drops duplicates', async () => {
    await openWith([text(1), text(2)]);

    store().applyAgentEvent(event(text(3)));
    store().applyAgentEvent(event(text(3)));
    store().applyAgentEvent(event(text(2)));

    expect(seqs()).toEqual([1, 2, 3]);
    expect(store().transcripts[fx.FX_RUNNING.id].lastSeq).toBe(3);
  });

  it('appends past a gap in an entry seeded without openTranscript', () => {
    useAgentStore.setState({ transcripts: { [fx.FX_RUNNING.id]: fx.makeTranscriptState([text(1)]) } });

    store().applyAgentEvent(event(text(3)));

    expect(seqs()).toEqual([1, 3]);
    expect(store().transcripts[fx.FX_RUNNING.id].gapFetching).toBe(false);
    expect(api.getAgentTranscript).not.toHaveBeenCalled();
  });

  it('ignores items of an agent whose transcript is not open', () => {
    store().applyAgentEvent(event(text(1)));
    expect(store().transcripts).toEqual({});
  });

  it('fetches after lastSeq on a gap, then applies what arrived meanwhile', async () => {
    await openWith([text(1), text(2)]);
    const fill = deferred<AgentTranscriptPage>();
    vi.mocked(api.getAgentTranscript).mockReturnValueOnce(fill.promise);

    store().applyAgentEvent(event(text(5)));
    expect(store().transcripts[fx.FX_RUNNING.id].gapFetching).toBe(true);
    expect(api.getAgentTranscript).toHaveBeenLastCalledWith(SID, fx.FX_RUNNING.id, { afterSeq: 2, limit: 500 });
    store().applyAgentEvent(event(text(6)));

    fill.resolve(page([text(3), text(4), text(5)]));
    await flush();

    const t = store().transcripts[fx.FX_RUNNING.id];
    expect(t.items.map(i => i.seq)).toEqual([1, 2, 3, 4, 5, 6]);
    expect(t.gapFetching).toBe(false);
    expect(t.pending).toEqual([]);
  });

  it('jumps to the tail when the gap is larger than a page', async () => {
    await openWith([text(1)]);
    vi.mocked(api.getAgentTranscript)
      .mockResolvedValueOnce(page(fx.makeTranscriptItems(500, 2)))
      .mockResolvedValueOnce(page(fx.makeTranscriptItems(200, 1101), true));

    store().applyAgentEvent(event(text(1300)));
    store().applyAgentEvent(event(text(1301)));
    await vi.waitFor(() => expect(store().transcripts[fx.FX_RUNNING.id].gapFetching).toBe(false));

    const t = store().transcripts[fx.FX_RUNNING.id];
    expect(api.getAgentTranscript).toHaveBeenNthCalledWith(3, SID, fx.FX_RUNNING.id, {});
    expect([t.firstSeq, t.lastSeq, t.hasMoreBefore, t.items.length]).toEqual([1101, 1301, true, 201]);
  });

  it('shows items past a gap that a fetch cannot close', async () => {
    await openWith([text(1)]);
    vi.mocked(api.getAgentTranscript).mockRejectedValueOnce(new TypeError('Failed to fetch'));

    store().applyAgentEvent(event(text(4)));
    await vi.waitFor(() => expect(store().transcripts[fx.FX_RUNNING.id].gapFetching).toBe(false));

    expect(seqs()).toEqual([1, 4]);
  });

  it('keeps at most 1000 items, dropping the oldest', async () => {
    await openWith(fx.makeTranscriptItems(999));

    store().applyAgentEvent(event(text(1000)));
    store().applyAgentEvent(event(text(1001)));
    store().applyAgentEvent(event(text(1002)));

    const t = store().transcripts[fx.FX_RUNNING.id];
    expect(t.items).toHaveLength(MAX_TRANSCRIPT_ITEMS);
    expect(t.firstSeq).toBe(3);
    expect(t.lastSeq).toBe(1002);
    expect(t.hasMoreBefore).toBe(true);
  });

  it('prepends an earlier page', async () => {
    await openWith(fx.makeTranscriptItems(200, 201), true);
    vi.mocked(api.getAgentTranscript).mockResolvedValueOnce(page(fx.makeTranscriptItems(200, 1), false));

    await store().loadEarlier(fx.FX_RUNNING.id);

    const t = store().transcripts[fx.FX_RUNNING.id];
    expect(api.getAgentTranscript).toHaveBeenLastCalledWith(SID, fx.FX_RUNNING.id, { beforeSeq: 201 });
    expect([t.items.length, t.firstSeq, t.lastSeq, t.hasMoreBefore]).toEqual([400, 1, 400, false]);
  });

  it('does not load earlier when nothing is before', async () => {
    await openWith([text(1)]);
    await store().loadEarlier(fx.FX_RUNNING.id);
    expect(api.getAgentTranscript).toHaveBeenCalledTimes(1);
  });

  it('marks a failed load as an error and retries on the next open', async () => {
    vi.mocked(api.getAgentTranscript).mockRejectedValueOnce(new Error('500: boom'));
    await store().openTranscript(fx.FX_RUNNING.id);
    expect(store().transcripts[fx.FX_RUNNING.id].status).toBe('error');
    expect(store().transcripts[fx.FX_RUNNING.id].error).toContain('boom');

    await openWith([text(1)]);
    expect(store().transcripts[fx.FX_RUNNING.id].status).toBe('ready');
  });

  it('opens a shell as its output, without a stream', async () => {
    const output = { agent_id: fx.FX_SHELL.id, text: 'ok\n', size: 3, truncated: false, available: true };
    vi.mocked(api.getAgentOutput).mockResolvedValueOnce(output);

    await store().openTranscript(fx.FX_SHELL.id);

    const t = store().transcripts[fx.FX_SHELL.id];
    expect(t.status).toBe('ready');
    expect(t.output).toEqual(output);
    expect(ws.subscribeAgent).not.toHaveBeenCalled();
    expect(api.getAgentTranscript).not.toHaveBeenCalled();
  });

  it('unsubscribes and forgets a closed transcript, ignoring its late answer', async () => {
    const tail = deferred<AgentTranscriptPage>();
    vi.mocked(api.getAgentTranscript).mockReturnValueOnce(tail.promise);
    const opening = store().openTranscript(fx.FX_RUNNING.id);

    store().closeTranscript(fx.FX_RUNNING.id);
    tail.resolve(page([text(1)]));
    await opening;

    expect(ws.unsubscribeAgent).toHaveBeenCalledWith(SID, fx.FX_RUNNING.id);
    expect(store().transcripts[fx.FX_RUNNING.id]).toBeUndefined();
  });

  it('resubscribes every open transcript and fetches what it missed', async () => {
    await openWith([text(1), text(2)]);
    vi.mocked(ws.subscribeAgent).mockClear();
    vi.mocked(api.getAgentTranscript).mockResolvedValueOnce(page([text(3)]));

    store().resubscribeAll();
    await flush();

    expect(ws.subscribeAgent).toHaveBeenCalledWith(SID, fx.FX_RUNNING.id);
    expect(api.getAgentTranscript).toHaveBeenLastCalledWith(SID, fx.FX_RUNNING.id, { afterSeq: 2, limit: 500 });
    expect(seqs()).toEqual([1, 2, 3]);
  });

  it('unsubscribes open transcripts on reset', async () => {
    await openWith([text(1)]);
    store().reset('s-next');

    expect(ws.unsubscribeAgent).toHaveBeenCalledWith(SID, fx.FX_RUNNING.id);
    expect(store().transcripts).toEqual({});
    expect(store().sessionId).toBe('s-next');
  });

  it('loads the detail into an open transcript', async () => {
    await openWith([text(1)]);
    const agent = { ...fx.FX_RUNNING, rev: fx.FX_RUNNING.rev + 1, summary: 'fresh' };
    vi.mocked(api.getAgentDetail).mockResolvedValueOnce({ agent, prompt: 'Build the API', result_text: null });

    await store().loadDetail(fx.FX_RUNNING.id);

    expect(store().transcripts[fx.FX_RUNNING.id].detail?.prompt).toBe('Build the API');
    expect(store().agents[fx.FX_RUNNING.id].summary).toBe('fresh');
  });
});

describe('queue', () => {
  const pending = (clientMsgId: string, content: string) => ({
    clientMsgId, content, blocks: [{ type: 'text' as const, content }], at: 1,
  });

  it('returns started sends, restores cancelled ones and drops duplicates', () => {
    store().addPendingSend(pending('c-1', 'first'));
    store().addPendingSend(pending('c-2', 'second'));
    store().addPendingSend(pending('c-3', 'third'));
    const left = fx.makeQueueEntry();

    const out = store().applyQueueUpdate({
      type: 'queue_update', session_id: SID, queue: [left],
      removed: [
        { queue_id: 'q_1', client_msg_id: 'c-1', reason: 'started' },
        { queue_id: 'q_2', client_msg_id: 'c-2', reason: 'cancelled' },
        { queue_id: null, client_msg_id: 'c-3', reason: 'duplicate' },
        { queue_id: 'q_4', client_msg_id: 'not-mine', reason: 'started' },
      ],
    });

    expect(out.started.map(p => p.clientMsgId)).toEqual(['c-1']);
    expect(out.cancelled.map(p => p.clientMsgId)).toEqual(['c-2']);
    expect(store().pendingSends).toEqual({});
    expect(store().queue).toEqual([left]);
    expect(store().composerInsert).toMatchObject({ text: 'second', mode: 'append' });
  });

  it('cancels a queued message and restores its text once', async () => {
    const entry = fx.makeQueueEntry({ queue_id: 'q_own', client_msg_id: 'c-own' });
    store().hydrate(fx.makeSnapshot([], { queue: [entry] }));
    store().addPendingSend(pending('c-own', 'also run the e2e suite'));
    vi.mocked(api.cancelQueued).mockResolvedValueOnce({ cancelled: true, content: 'also run the e2e suite' });

    await store().cancelQueued('q_own');
    const insert = store().composerInsert;
    store().applyQueueUpdate({
      type: 'queue_update', session_id: SID, queue: [],
      removed: [{ queue_id: 'q_own', client_msg_id: 'c-own', reason: 'cancelled' }],
    });

    expect(api.cancelQueued).toHaveBeenCalledWith(SID, 'q_own');
    expect(store().queue).toEqual([]);
    expect(store().pendingSends).toEqual({});
    expect(insert).toMatchObject({ text: 'also run the e2e suite', mode: 'append' });
    expect(store().composerInsert).toBe(insert);
  });

  it('restores the text once when the cancel event beats the answer', async () => {
    const entry = fx.makeQueueEntry({ queue_id: 'q_own', client_msg_id: 'c-own' });
    store().hydrate(fx.makeSnapshot([], { queue: [entry] }));
    store().addPendingSend(pending('c-own', 'typed text'));
    const answer = deferred<{ cancelled: true; content: string | null }>();
    vi.mocked(api.cancelQueued).mockReturnValueOnce(answer.promise);

    const cancelling = store().cancelQueued('q_own');
    store().applyQueueUpdate({
      type: 'queue_update', session_id: SID, queue: [],
      removed: [{ queue_id: 'q_own', client_msg_id: 'c-own', reason: 'cancelled' }],
    });
    expect(store().composerInsert).toBeNull();
    answer.resolve({ cancelled: true, content: 'typed text' });
    await cancelling;

    expect(store().composerInsert).toMatchObject({ text: 'typed text' });
  });

  it('keeps an entry that already started', async () => {
    const entry = fx.makeQueueEntry({ queue_id: 'q_late' });
    store().hydrate(fx.makeSnapshot([], { queue: [entry] }));
    vi.mocked(api.cancelQueued).mockRejectedValueOnce(apiError(409, 'already_started'));

    await store().cancelQueued('q_late');

    expect(store().queue).toEqual([entry]);
    expect(store().composerInsert).toBeNull();
  });
});

describe('stop', () => {
  const brief = { id: fx.FX_RUNNING.id, label: 'build-api', kind: 'agent' as const, status: 'running' as const, started_at: null, tokens: 1 };

  it('turns the stop result into the escalation state', async () => {
    vi.mocked(api.stopSession).mockResolvedValueOnce({ scope: 'turn', turn: 'not_stopped', would_end: [brief], shell_count: 1 });
    const first = store().stopTurn();
    expect(store().stopInFlight).toBe(true);
    await first;
    expect(store().stopInFlight).toBe(false);
    expect(store().escalation).toEqual({ kind: 'not_stopped', wouldEnd: [brief], shellCount: 1 });
    expect(api.stopSession).toHaveBeenCalledWith(SID, { scope: 'turn', confirm_kill_background: false });

    vi.mocked(api.stopSession).mockResolvedValueOnce({ scope: 'turn', turn: 'needs_confirmation', would_end: [brief], shell_count: 0 });
    await store().stopTurn();
    expect(store().escalation?.kind).toBe('needs_confirmation');

    vi.mocked(api.stopSession).mockResolvedValueOnce({ scope: 'turn', turn: 'stopped', would_end: [], shell_count: 0 });
    const res = await store().stopTurn({ confirmKillBackground: true });
    expect(api.stopSession).toHaveBeenLastCalledWith(SID, { scope: 'turn', confirm_kill_background: true });
    expect(res?.turn).toBe('stopped');
    expect(store().escalation).toBeNull();
  });

  it('returns null on a network error and keeps the banner', async () => {
    vi.mocked(api.stopSession).mockResolvedValueOnce({ scope: 'turn', turn: 'not_stopped', would_end: [], shell_count: 0 });
    await store().stopTurn();
    vi.mocked(api.stopSession).mockRejectedValueOnce(new TypeError('Failed to fetch'));
    const quiet = vi.spyOn(console, 'error').mockImplementation(() => {});

    expect(await store().stopTurn()).toBeNull();
    expect(store().escalation?.kind).toBe('not_stopped');
    expect(store().stopInFlight).toBe(false);
    quiet.mockRestore();
  });

  it('closes the dialog for stop all and force stop', async () => {
    vi.mocked(api.stopSession).mockResolvedValueOnce({ scope: 'all', turn: 'stopped', stopping: [fx.FX_RUNNING.id], unsupported: [] });
    store().setStopDialog('all');
    const all = await store().stopAll({ includeShells: false });
    expect(api.stopSession).toHaveBeenLastCalledWith(SID, { scope: 'all', include_shells: false });
    expect(all?.stopping).toEqual([fx.FX_RUNNING.id]);
    expect(store().stopDialog).toBeNull();

    vi.mocked(api.stopSession).mockResolvedValueOnce({ scope: 'force', turn: 'killed', ended: [] });
    useAgentStore.setState({ stopDialog: 'force', escalation: { kind: 'not_stopped', wouldEnd: [], shellCount: 0 } });
    const force = await store().forceStop();
    expect(api.stopSession).toHaveBeenLastCalledWith(SID, { scope: 'force' });
    expect(force?.turn).toBe('killed');
    expect([store().stopDialog, store().escalation]).toEqual([null, null]);
  });

  it('stops one agent without changing it, and reloads when the runtime is gone', async () => {
    const stopping = { ...fx.FX_RUNNING, status: 'stopping' as const, rev: fx.FX_RUNNING.rev + 1 };
    vi.mocked(api.stopAgent).mockResolvedValueOnce({ agent: stopping, stopping: [fx.FX_RUNNING.id] });
    await store().stopAgent(fx.FX_RUNNING.id);
    expect(api.stopAgent).toHaveBeenCalledWith(SID, fx.FX_RUNNING.id, true);
    expect(store().agents[fx.FX_RUNNING.id].status).toBe('stopping');

    vi.mocked(api.stopAgent).mockRejectedValueOnce(apiError(409, 'no_runtime'));
    vi.mocked(api.getSessionAgents).mockResolvedValueOnce(fx.makeSnapshot([]));
    await store().stopAgent(fx.FX_PARENT.id);
    expect(api.getSessionAgents).toHaveBeenCalledWith(SID);

    vi.mocked(api.stopAgent).mockRejectedValueOnce(apiError(501, 'unsupported'));
    await store().stopAgent(fx.FX_CODEX.id);
    expect(api.getSessionAgents).toHaveBeenCalledTimes(1);
    expect(store().stopError).toBeNull();
  });

  it('says when a stop request failed, until a later stop works or it is dismissed', async () => {
    const quiet = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(api.stopAgent).mockRejectedValueOnce(new TypeError('Failed to fetch'));
    await store().stopAgent(fx.FX_RUNNING.id);
    expect(store().stopError).toBe(`Couldn't stop ${fx.FX_RUNNING.label}: Failed to fetch.`);

    vi.mocked(api.stopAgent).mockResolvedValueOnce({ agent: fx.FX_RUNNING, stopping: [fx.FX_RUNNING.id] });
    await store().stopAgent(fx.FX_RUNNING.id);
    expect(store().stopError).toBeNull();

    vi.mocked(api.stopSession).mockRejectedValueOnce(apiError(500, 'internal'));
    await store().forceStop();
    expect(store().stopError).toBe('Force stop failed: internal.');
    store().dismissStopError();
    expect(store().stopError).toBeNull();
    quiet.mockRestore();
  });

  it('names the agents Stop all could not stop', async () => {
    vi.mocked(api.stopSession).mockResolvedValueOnce({
      scope: 'all', turn: 'not_running', stopping: [], unsupported: [fx.FX_RUNNING.id, 'ag_unknown'],
    });
    await store().stopAll();
    expect(store().stopError).toBe(`${fx.FX_RUNNING.label}, ag_unknown can't be stopped one by one on this runtime.`);
  });
});

describe('seen and composer', () => {
  it('persists seen agents per session', () => {
    store().markSeen([fx.FX_FAILED.id, fx.FX_LOST.id]);
    store().markSeen([fx.FX_FAILED.id]);

    expect(JSON.parse(localStorage.getItem(`nerve_agents_seen:${SID}`)!)).toEqual([fx.FX_FAILED.id, fx.FX_LOST.id]);
    store().reset('s-other');
    expect(store().seen).toEqual({});
    store().reset(SID);
    expect(store().seen).toEqual({ [fx.FX_FAILED.id]: true, [fx.FX_LOST.id]: true });
  });

  it('keeps the newest 500 seen ids', () => {
    store().markSeen(Array.from({ length: 510 }, (_, i) => `ag_${i}`));

    const ids = JSON.parse(localStorage.getItem(`nerve_agents_seen:${SID}`)!);
    expect(ids).toHaveLength(500);
    expect(ids[0]).toBe('ag_10');
    expect(Object.keys(store().seen)).toHaveLength(500);
  });

  it('prefills the composer with a fresh nonce and bumps the focus nonce', () => {
    store().prefillComposer('Use SendMessage to tell `build-api`: ');
    const first = store().composerInsert!;
    store().prefillComposer('again', 'replace');

    expect(first).toMatchObject({ mode: 'append' });
    expect(store().composerInsert).toMatchObject({ text: 'again', mode: 'replace' });
    expect(store().composerInsert!.nonce).not.toBe(first.nonce);

    const nonce = store().focusNonce;
    store().requestFocus();
    store().reset(SID);
    expect(store().focusNonce).toBe(nonce + 1);
  });
});
