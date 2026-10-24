// The agent records of the active session: the roster (by id, newest rev
// wins), open transcripts, the server-visible turn queue with this tab's
// unconfirmed sends, and the stop / escalation UI state.
//
// This module must not import chatStore: chatStore imports it statically and
// drives `reset` / `loadSnapshot` from switchSession.

import { create } from 'zustand';
import { api, apiErrorCode } from '../api/client';
import { ws } from '../api/websocket';
import type { MessageBlock } from '../types/chat';
import {
  DEFAULT_AGENT_CAPABILITIES, EMPTY_AGENT_SUMMARY, LIVE_STATUSES,
  type AgentBrief, type AgentCapabilities, type AgentDetail, type AgentEventItem, type AgentOutput, type AgentSnapshot, type AgentSummary,
  type AgentTranscriptPage, type AgentView, type AgentWSMessage, type ForceStopResult, type QueueEntryView, type StopAllResult,
  type StopTurnResult,
} from '../types/agents';
import { loadSeen, persistSeen, MAX_SEEN_IDS } from './helpers/agentSeen';

export interface TranscriptState {
  status: 'loading' | 'ready' | 'error';
  items: AgentEventItem[];          // ascending seq, at most 1000 (oldest dropped => hasMoreBefore = true)
  firstSeq: number | null;
  lastSeq: number | null;
  hasMoreBefore: boolean;
  pending: AgentEventItem[];        // live items that arrived while loading
  gapFetching: boolean;
  output?: AgentOutput;             // shells / monitors
  detail?: AgentDetail;             // prompt + result, lazily loaded
  error?: string;
}
export interface PendingSend { clientMsgId: string; content: string; blocks: MessageBlock[]; at: number }
export type EscalationState =
  | { kind: 'not_stopped'; wouldEnd: AgentBrief[]; shellCount: number }
  | { kind: 'needs_confirmation'; wouldEnd: AgentBrief[]; shellCount: number };
export interface ComposerInsert { nonce: number; text: string; mode: 'replace' | 'append' }

export interface AgentStoreState {
  sessionId: string | null;
  agents: Record<string, AgentView>;
  bySpawn: Record<string, string>;            // spawn_tool_use_id -> agent id
  summary: AgentSummary;
  capabilities: AgentCapabilities;
  queue: QueueEntryView[];
  pendingSends: Record<string, PendingSend>;  // by client_msg_id
  transcripts: Record<string, TranscriptState>;
  seen: Record<string, true>;                 // persisted per session (agentSeen.ts)
  escalation: EscalationState | null;
  stopDialog: 'all' | 'force' | null;
  stopInFlight: boolean;
  /** What the last stop request could not do (shown above the composer). */
  stopError: string | null;
  composerInsert: ComposerInsert | null;
  focusNonce: number;                         // bumped by requestFocus(); AgentsPanel focuses its list

  reset(sessionId: string | null): void;
  hydrate(snapshot: AgentSnapshot): void;
  loadSnapshot(sessionId: string): Promise<void>;
  applyAgentUpdate(msg: Extract<AgentWSMessage, { type: 'agent_update' }>): void;
  applyAgentEvent(msg: Extract<AgentWSMessage, { type: 'agent_event' }>): void;
  applyQueueUpdate(msg: Extract<AgentWSMessage, { type: 'queue_update' }>): { started: PendingSend[]; cancelled: PendingSend[] };
  /** Merge records fetched on their own (the detail route) under the rev rule; the summary stays. */
  upsertAgents(views: AgentView[]): void;
  addPendingSend(p: PendingSend): void;
  dropPendingSend(clientMsgId: string): void;
  /** Drop a pending send that the server refused and put its text back in the composer. False if this tab has no such send. */
  refusePendingSend(clientMsgId: string): boolean;
  openTranscript(agentId: string): Promise<void>;
  closeTranscript(agentId: string): void;
  loadEarlier(agentId: string): Promise<void>;
  refreshOutput(agentId: string): Promise<void>;
  loadDetail(agentId: string): Promise<void>;
  resubscribeAll(): void;
  stopAgent(agentId: string): Promise<void>;
  stopTurn(opts?: { confirmKillBackground?: boolean }): Promise<StopTurnResult | null>;
  stopAll(opts?: { includeShells?: boolean }): Promise<StopAllResult | null>;
  forceStop(): Promise<ForceStopResult | null>;
  cancelQueued(queueId: string): Promise<void>;
  markSeen(agentIds: string[]): void;
  setStopDialog(d: 'all' | 'force' | null): void;
  dismissEscalation(): void;
  dismissStopError(): void;
  prefillComposer(text: string, mode?: 'replace' | 'append'): void;
  requestFocus(): void;
}

/** Live transcript items kept in memory per open agent. */
export const MAX_TRANSCRIPT_ITEMS = 1000;
/** Page size of an `after_seq` catch-up fetch (the route's maximum). */
export const GAP_FETCH_LIMIT = 500;
/** Catch-up rounds before items with a remaining gap are shown anyway. */
const MAX_CATCH_UP_ROUNDS = 3;

type AgentData = Pick<AgentStoreState,
  'sessionId' | 'agents' | 'bySpawn' | 'summary' | 'capabilities' | 'queue' | 'pendingSends' | 'transcripts'
  | 'seen' | 'escalation' | 'stopDialog' | 'stopInFlight' | 'stopError' | 'composerInsert'>;

function emptyData(sessionId: string | null): AgentData {
  return {
    sessionId,
    agents: {},
    bySpawn: {},
    summary: EMPTY_AGENT_SUMMARY,
    capabilities: DEFAULT_AGENT_CAPABILITIES,
    queue: [],
    pendingSends: {},
    transcripts: {},
    seen: sessionId ? loadSeen(sessionId) : {},
    escalation: null,
    stopDialog: null,
    stopInFlight: false,
    stopError: null,
    composerInsert: null,
  };
}

/** Why a stop request failed, in a few words. */
function failureReason(e: unknown): string {
  const code = apiErrorCode(e);
  if (code) return code.replace(/_/g, ' ');
  if (e instanceof Error && e.message) return e.message.slice(0, 120);
  return 'no answer from the server';
}

function emptyTranscript(prev?: TranscriptState): TranscriptState {
  return {
    status: 'loading', items: [], firstSeq: null, lastSeq: null, hasMoreBefore: false,
    pending: [], gapFetching: false,
    // A retry after an error keeps what it already had.
    ...(prev?.output ? { output: prev.output } : {}),
    ...(prev?.detail ? { detail: prev.detail } : {}),
  };
}

// ------------------------------------------------------------------ //
//  Bookkeeping outside the reactive state                             //
// ------------------------------------------------------------------ //

/** Bumped by reset(): a request that started in another session is stale. */
let generation = 0;
/** One token per open transcript; an async result applies only while it matches. */
const openTokens = new Map<string, number>();
let nextToken = 0;
/** Agents whose transcript stream this tab subscribed to (shells are not). */
const subscribed = new Set<string>();
/** Transcripts with a catch-up fetch in flight. */
const catchingUp = new Set<string>();
/** Transcripts with a "Load earlier" fetch in flight. */
const loadingEarlier = new Set<string>();
/** client_msg_ids whose cancel this tab requested: the REST answer restores the text. */
const cancelling = new Set<string>();
/** Queue entries already reported gone; an older snapshot must not bring them back. */
const removedQueueIds = new Set<string>();
/** Stop requests in flight in this generation (drives stopInFlight). */
let stopsInFlight = 0;

function isShellLike(view: AgentView | undefined): boolean {
  return view?.kind === 'shell' || view?.kind === 'monitor';
}

function errorText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

// ------------------------------------------------------------------ //
//  Pure helpers                                                        //
// ------------------------------------------------------------------ //

/**
 * Upsert views into `current`, keeping per record the view with the higher
 * `rev` (records are never deleted). Views of another session are ignored.
 * Returns `current` itself when nothing changed, so selectors stay stable.
 */
function mergeViews(
  current: Record<string, AgentView>, incoming: AgentView[] | undefined, sessionId: string,
): Record<string, AgentView> {
  let next = current;
  for (const view of incoming ?? []) {
    if (!view || view.session_id !== sessionId) continue;
    const existing = next[view.id];
    if (existing && existing.rev >= view.rev) continue;
    if (next === current) next = { ...current };
    next[view.id] = view;
  }
  return next;
}

/**
 * Whether a snapshot was taken before changes this store already applied: it
 * holds an older rev of a record, or lacks a live one (a snapshot lists every
 * live record). Its summary is then older than the current one.
 */
function snapshotIsBehind(current: Record<string, AgentView>, snapshot: AgentSnapshot): boolean {
  const listed = new Map((snapshot.agents ?? []).map(v => [v.id, v.rev]));
  for (const view of Object.values(current)) {
    const rev = listed.get(view.id);
    if (rev === undefined ? LIVE_STATUSES.has(view.status) : rev < view.rev) return true;
  }
  return false;
}

/** spawn_tool_use_id -> agent id over every record. */
function spawnIndex(agents: Record<string, AgentView>): Record<string, string> {
  const index: Record<string, string> = {};
  for (const view of Object.values(agents)) {
    if (view.spawn_tool_use_id) index[view.spawn_tool_use_id] = view.id;
  }
  return index;
}

const bySeq = (a: AgentEventItem, b: AgentEventItem) => a.seq - b.seq;

/** Both lists merged in ascending seq, one item per seq (the first list wins). */
function mergeItems(a: AgentEventItem[], b: AgentEventItem[]): AgentEventItem[] {
  if (b.length === 0) return a;
  if (a.length === 0) return [...b].sort(bySeq);
  const seqs = new Map<number, AgentEventItem>();
  for (const item of a) seqs.set(item.seq, item);
  for (const item of b) if (!seqs.has(item.seq)) seqs.set(item.seq, item);
  return [...seqs.values()].sort(bySeq);
}

/** A page's items in ascending seq (a missing page reads as empty). */
function pageItems(page: AgentTranscriptPage | null | undefined): AgentEventItem[] {
  return mergeItems([], page?.items ?? []);
}

/** Set the items (ascending), dropping the oldest above the cap. */
function withItems(t: TranscriptState, items: AgentEventItem[], hasMoreBefore = t.hasMoreBefore): TranscriptState {
  let kept = items;
  let more = hasMoreBefore;
  if (kept.length > MAX_TRANSCRIPT_ITEMS) {
    kept = kept.slice(kept.length - MAX_TRANSCRIPT_ITEMS);
    more = true;
  }
  return {
    ...t,
    items: kept,
    firstSeq: kept.length ? kept[0].seq : null,
    lastSeq: kept.length ? kept[kept.length - 1].seq : null,
    hasMoreBefore: more,
  };
}

/**
 * Apply live items in seq order: an item at or below `lastSeq` is a duplicate
 * and dropped, the next seq is appended, and anything beyond it is a gap.
 * Returns the transcript with what was appended and the items from the first
 * gap on (still in seq order).
 */
function applyLive(t: TranscriptState, incoming: AgentEventItem[]): { transcript: TranscriptState; rest: AgentEventItem[] } {
  const sorted = [...incoming].sort(bySeq);
  let last = t.lastSeq ?? 0;
  const appended: AgentEventItem[] = [];
  let i = 0;
  for (; i < sorted.length; i++) {
    const item = sorted[i];
    if (item.seq <= last) continue;
    if (item.seq !== last + 1) break;
    appended.push(item);
    last = item.seq;
  }
  const transcript = appended.length ? withItems(t, [...t.items, ...appended]) : t;
  return { transcript, rest: sorted.slice(i).filter(item => item.seq > last) };
}

// ------------------------------------------------------------------ //
//  Store                                                               //
// ------------------------------------------------------------------ //

export const useAgentStore = create<AgentStoreState>((set, get) => {
  /** The open transcript of `agentId` if `token` still owns it. */
  const owned = (agentId: string, token: number | undefined): TranscriptState | null => {
    if (token === undefined || openTokens.get(agentId) !== token) return null;
    return get().transcripts[agentId] ?? null;
  };

  const putTranscript = (agentId: string, t: TranscriptState) => {
    set((s) => ({ transcripts: { ...s.transcripts, [agentId]: t } }));
  };

  /** Upsert server views (e.g. a REST answer) under the rev rule. */
  const upsertViews = (views: AgentView[]) => {
    const s = get();
    if (!s.sessionId) return;
    const agents = mergeViews(s.agents, views, s.sessionId);
    if (agents !== s.agents) set({ agents, bySpawn: spawnIndex(agents) });
  };

  const beginStop = () => {
    stopsInFlight += 1;
    if (!get().stopInFlight) set({ stopInFlight: true });
    const gen = generation;
    return () => {
      if (gen !== generation) return;
      stopsInFlight = Math.max(0, stopsInFlight - 1);
      if (stopsInFlight === 0) set({ stopInFlight: false });
    };
  };

  /**
   * Fill the gap after `lastSeq` (`after_seq`, one page of GAP_FETCH_LIMIT),
   * then apply the pending live items. A full page means the gap is larger
   * than a page: the tail replaces the items ("Load earlier" reaches the
   * rest). Items still past a gap after a failed fetch, a round without
   * progress, or a few rounds are shown anyway.
   */
  const catchUp = async (agentId: string, token: number) => {
    if (catchingUp.has(agentId)) return;
    const sessionId = get().sessionId;
    if (!sessionId) return;
    catchingUp.add(agentId);
    try {
      for (let round = 0; round < MAX_CATCH_UP_ROUNDS; round++) {
        const before = owned(agentId, token);
        if (!before) return;
        if (!before.gapFetching) putTranscript(agentId, { ...before, gapFetching: true });
        const from = before.lastSeq ?? 0;
        let fetched: AgentEventItem[];
        let jumped = false;
        try {
          const page = await api.getAgentTranscript(sessionId, agentId, { afterSeq: from, limit: GAP_FETCH_LIMIT });
          fetched = pageItems(page);
          if (fetched.length >= GAP_FETCH_LIMIT) {
            fetched = pageItems(await api.getAgentTranscript(sessionId, agentId, {}));
            jumped = true;
          }
        } catch {
          break;
        }
        let t = owned(agentId, token);
        if (!t) return;
        t = jumped ? withItems(t, fetched, true) : withItems(t, mergeItems(t.items, fetched));
        const { transcript, rest } = applyLive({ ...t, pending: [] }, t.pending);
        if (rest.length === 0) {
          putTranscript(agentId, { ...transcript, pending: [], gapFetching: false });
          return;
        }
        putTranscript(agentId, { ...transcript, pending: rest, gapFetching: true });
        if (!jumped && !fetched.some(item => item.seq > from)) break;
      }
      // The gap did not close (a failed fetch, or items the server no longer
      // has): show what arrived rather than stall the stream.
      const t = owned(agentId, token);
      if (!t) return;
      const ahead = t.pending.filter(item => item.seq > (t.lastSeq ?? 0));
      putTranscript(agentId, { ...withItems(t, mergeItems(t.items, ahead)), pending: [], gapFetching: false });
    } finally {
      catchingUp.delete(agentId);
    }
  };

  /** Fetch the tail of a transcript entry that `token` owns, then go live. */
  const loadTail = async (agentId: string, token: number) => {
    const sessionId = get().sessionId;
    if (!sessionId) return;
    let page;
    try {
      page = await api.getAgentTranscript(sessionId, agentId, {});
    } catch (e) {
      const t = owned(agentId, token);
      if (t) putTranscript(agentId, { ...t, status: 'error', error: errorText(e) });
      return;
    }
    const t = owned(agentId, token);
    if (!t) return;
    const loaded = withItems({ ...t, status: 'ready', error: undefined, pending: [] }, pageItems(page), !!page?.has_more_before);
    const { transcript, rest } = applyLive(loaded, t.pending);
    putTranscript(agentId, { ...transcript, pending: rest, gapFetching: rest.length > 0 });
    get().markSeen([agentId]);
    if (rest.length > 0) void catchUp(agentId, token);
  };

  /** Fetch a shell's output tail into its entry; false when the request failed. */
  const fetchOutput = async (agentId: string, token: number | undefined): Promise<boolean> => {
    const sessionId = get().sessionId;
    if (!sessionId || !owned(agentId, token)) return false;
    try {
      const output = await api.getAgentOutput(sessionId, agentId);
      const t = owned(agentId, token);
      if (t) putTranscript(agentId, { ...t, output, error: undefined });
      return true;
    } catch (e) {
      const t = owned(agentId, token);
      if (t) putTranscript(agentId, { ...t, error: errorText(e) });
      return false;
    }
  };

  return {
    ...emptyData(null),
    focusNonce: 0,

    reset: (sessionId) => {
      const s = get();
      if (s.sessionId) {
        for (const agentId of Object.keys(s.transcripts)) {
          if (subscribed.has(agentId)) ws.unsubscribeAgent(s.sessionId, agentId);
        }
      }
      generation += 1;
      openTokens.clear();
      subscribed.clear();
      catchingUp.clear();
      loadingEarlier.clear();
      cancelling.clear();
      removedQueueIds.clear();
      stopsInFlight = 0;
      // The focus nonce only ever grows, so a consumer that remembers the last
      // value it handled never mistakes a reset for a new request.
      set({ ...emptyData(sessionId) });
    },

    hydrate: (snapshot) => {
      if (!snapshot) return;
      const s = get();
      const adopt = s.sessionId === null;
      const sessionId = adopt ? snapshot.session_id : s.sessionId;
      if (!sessionId || snapshot.session_id !== sessionId) return;
      // The WebSocket and REST snapshots race each other and the live events:
      // records keep their highest rev, and an older snapshot neither rolls
      // the summary back nor revives a queue entry that already left.
      const behind = snapshotIsBehind(s.agents, snapshot);
      const agents = mergeViews(s.agents, snapshot.agents, sessionId);
      set({
        ...(adopt ? { sessionId, seen: loadSeen(sessionId) } : {}),
        agents,
        bySpawn: agents === s.agents && !adopt ? s.bySpawn : spawnIndex(agents),
        summary: behind ? s.summary : snapshot.summary ?? EMPTY_AGENT_SUMMARY,
        capabilities: snapshot.capabilities ?? DEFAULT_AGENT_CAPABILITIES,
        queue: (snapshot.queue ?? []).filter(q => !removedQueueIds.has(q.queue_id)),
      });
    },

    loadSnapshot: async (sessionId) => {
      const gen = generation;
      try {
        const snapshot = await api.getSessionAgents(sessionId);
        // A switch in the meantime makes the answer stale (hydrate would
        // otherwise adopt it after a reset to no session).
        if (gen !== generation) return;
        get().hydrate(snapshot);
      } catch (e) {
        console.warn('Failed to load the agents of the session:', e);
      }
    },

    applyAgentUpdate: (msg) => {
      const s = get();
      if (!s.sessionId || msg.session_id !== s.sessionId) return;
      const agents = mergeViews(s.agents, msg.agents, s.sessionId);
      set({
        ...(agents !== s.agents ? { agents, bySpawn: spawnIndex(agents) } : {}),
        summary: msg.summary ?? s.summary,
      });
    },

    applyAgentEvent: (msg) => {
      const s = get();
      if (!s.sessionId || msg.session_id !== s.sessionId) return;
      const t = s.transcripts[msg.agent_id];
      if (!t || t.status === 'error') return;
      // Not loaded yet, or a catch-up runs: the fetch answer applies these.
      if (t.status === 'loading' || t.gapFetching) {
        putTranscript(msg.agent_id, { ...t, pending: [...t.pending, msg.item] });
        return;
      }
      const { transcript, rest } = applyLive(t, [msg.item]);
      if (rest.length === 0) {
        if (transcript !== t) putTranscript(msg.agent_id, transcript);
        return;
      }
      const token = openTokens.get(msg.agent_id);
      if (token === undefined) {
        // An entry not opened through openTranscript (seeded state) has no
        // fetch to fill the gap: show the item as it is.
        putTranscript(msg.agent_id, withItems(transcript, mergeItems(transcript.items, rest)));
        return;
      }
      putTranscript(msg.agent_id, { ...transcript, pending: rest, gapFetching: true });
      void catchUp(msg.agent_id, token);
    },

    applyQueueUpdate: (msg) => {
      const s = get();
      const result: { started: PendingSend[]; cancelled: PendingSend[] } = { started: [], cancelled: [] };
      if (!s.sessionId || msg.session_id !== s.sessionId) return result;
      let pendingSends = s.pendingSends;
      const restore: string[] = [];
      for (const removal of msg.removed ?? []) {
        if (removal.queue_id) removedQueueIds.add(removal.queue_id);
        const clientMsgId = removal.client_msg_id;
        if (!clientMsgId) continue;
        // This tab's own cancel restores the text from the REST answer.
        const ownCancel = removal.reason === 'cancelled' && cancelling.delete(clientMsgId);
        const pending = pendingSends[clientMsgId];
        if (!pending) continue;
        if (pendingSends === s.pendingSends) pendingSends = { ...s.pendingSends };
        delete pendingSends[clientMsgId];
        if (removal.reason === 'started') {
          result.started.push(pending);
        } else if (removal.reason === 'cancelled') {
          result.cancelled.push(pending);
          if (!ownCancel && pending.content) restore.push(pending.content);
        } else if (removal.reason === 'refused' && pending.content) {
          // Nerve is shutting down: the text goes back to be sent again later.
          restore.push(pending.content);
        }
      }
      set({ queue: msg.queue ?? [], ...(pendingSends !== s.pendingSends ? { pendingSends } : {}) });
      if (restore.length) get().prefillComposer(restore.join('\n\n'), 'append');
      return result;
    },

    upsertAgents: (views) => upsertViews(views),

    addPendingSend: (p) => {
      set((s) => ({ pendingSends: { ...s.pendingSends, [p.clientMsgId]: p } }));
    },

    refusePendingSend: (clientMsgId) => {
      const pending = get().pendingSends[clientMsgId];
      if (!pending) return false;
      get().dropPendingSend(clientMsgId);
      if (pending.content) get().prefillComposer(pending.content, 'append');
      return true;
    },

    dropPendingSend: (clientMsgId) => {
      if (!get().pendingSends[clientMsgId]) return;
      set((s) => {
        const pendingSends = { ...s.pendingSends };
        delete pendingSends[clientMsgId];
        return { pendingSends };
      });
    },

    openTranscript: async (agentId) => {
      const s = get();
      const sessionId = s.sessionId;
      if (!sessionId) return;
      const existing = s.transcripts[agentId];
      // Already open: nothing to do (an errored entry retries).
      if (existing && existing.status !== 'error') return;
      const token = ++nextToken;
      openTokens.set(agentId, token);
      putTranscript(agentId, emptyTranscript(existing));
      if (isShellLike(s.agents[agentId])) {
        // A shell has no streamed items; its tab shows the output tail.
        const ok = await fetchOutput(agentId, token);
        const t = owned(agentId, token);
        if (!t) return;
        putTranscript(agentId, ok ? { ...t, status: 'ready' } : { ...t, status: 'error' });
        if (ok) get().markSeen([agentId]);
        return;
      }
      subscribed.add(agentId);
      ws.subscribeAgent(sessionId, agentId);
      await loadTail(agentId, token);
    },

    closeTranscript: (agentId) => {
      const s = get();
      if (s.sessionId && subscribed.has(agentId)) ws.unsubscribeAgent(s.sessionId, agentId);
      subscribed.delete(agentId);
      openTokens.delete(agentId);
      catchingUp.delete(agentId);
      loadingEarlier.delete(agentId);
      if (!s.transcripts[agentId]) return;
      set((st) => {
        const transcripts = { ...st.transcripts };
        delete transcripts[agentId];
        return { transcripts };
      });
    },

    loadEarlier: async (agentId) => {
      const s = get();
      const sessionId = s.sessionId;
      const t = s.transcripts[agentId];
      const token = openTokens.get(agentId);
      if (!sessionId || !t || t.status !== 'ready' || !t.hasMoreBefore || t.firstSeq === null) return;
      if (loadingEarlier.has(agentId)) return;
      loadingEarlier.add(agentId);
      try {
        const page = await api.getAgentTranscript(sessionId, agentId, { beforeSeq: t.firstSeq });
        const cur = owned(agentId, token);
        if (!cur) return;
        // Prepending is the person's request: it may take the list past the
        // live cap, which applies again when new items arrive.
        const items = mergeItems(cur.items, pageItems(page));
        putTranscript(agentId, {
          ...cur,
          items,
          firstSeq: items.length ? items[0].seq : null,
          lastSeq: items.length ? items[items.length - 1].seq : null,
          hasMoreBefore: !!page?.has_more_before,
          error: undefined,
        });
      } catch (e) {
        const cur = owned(agentId, token);
        if (cur) putTranscript(agentId, { ...cur, error: errorText(e) });
      } finally {
        if (openTokens.get(agentId) === token) loadingEarlier.delete(agentId);
      }
    },

    refreshOutput: async (agentId) => {
      await fetchOutput(agentId, openTokens.get(agentId));
    },

    loadDetail: async (agentId) => {
      const sessionId = get().sessionId;
      const token = openTokens.get(agentId);
      if (!sessionId || !owned(agentId, token)) return;
      try {
        const detail = await api.getAgentDetail(sessionId, agentId);
        const t = owned(agentId, token);
        if (!t) return;
        upsertViews([detail.agent]);
        putTranscript(agentId, { ...t, detail });
      } catch (e) {
        const t = owned(agentId, token);
        if (t) putTranscript(agentId, { ...t, error: errorText(e) });
      }
    },

    resubscribeAll: () => {
      const s = get();
      const sessionId = s.sessionId;
      if (!sessionId) return;
      for (const [agentId, t] of Object.entries(s.transcripts)) {
        const token = openTokens.get(agentId);
        if (token === undefined) continue;
        if (!subscribed.has(agentId)) {
          // A shell: refresh its output instead.
          void fetchOutput(agentId, token);
          continue;
        }
        ws.subscribeAgent(sessionId, agentId);
        if (t.status === 'error') {
          putTranscript(agentId, emptyTranscript(t));
          void loadTail(agentId, token);
        } else if (t.status === 'ready') {
          // Items streamed while the socket was down: fetch after lastSeq.
          void catchUp(agentId, token);
        }
        // 'loading': the tail fetch in flight applies what it finds.
      }
    },

    stopAgent: async (agentId) => {
      const sessionId = get().sessionId;
      if (!sessionId) return;
      const gen = generation;
      try {
        // No optimistic change: the server's answer and its agent_update carry
        // the stopping state.
        const res = await api.stopAgent(sessionId, agentId, true);
        if (gen === generation && res?.agent) upsertViews([res.agent]);
        if (gen === generation && get().stopError) set({ stopError: null });
      } catch (e) {
        const code = apiErrorCode(e);
        if (code === 'no_runtime') {
          // The server reconciled the record to interrupted.
          if (gen === generation) await get().loadSnapshot(sessionId);
        } else if (code !== 'unsupported') {
          console.error('Failed to stop the agent:', e);
          const label = get().agents[agentId]?.label ?? 'the agent';
          if (gen === generation) set({ stopError: `Couldn't stop ${label}: ${failureReason(e)}.` });
        }
      }
    },

    stopTurn: async (opts) => {
      const sessionId = get().sessionId;
      if (!sessionId) return null;
      const gen = generation;
      const end = beginStop();
      try {
        const res = await api.stopSession(sessionId, {
          scope: 'turn', confirm_kill_background: !!opts?.confirmKillBackground,
        }) as StopTurnResult;
        if (gen === generation) {
          if (res.turn === 'needs_confirmation' || res.turn === 'not_stopped') {
            set({ escalation: { kind: res.turn, wouldEnd: res.would_end ?? [], shellCount: res.shell_count ?? 0 }, stopError: null });
          } else {
            set({ escalation: null, stopError: null });
          }
        }
        return res;
      } catch (e) {
        console.error('Failed to stop the turn:', e);
        if (gen === generation) set({ stopError: `Couldn't stop the turn: ${failureReason(e)}.` });
        return null;
      } finally {
        end();
      }
    },

    stopAll: async (opts) => {
      const sessionId = get().sessionId;
      set({ stopDialog: null });
      if (!sessionId) return null;
      const gen = generation;
      const end = beginStop();
      try {
        const res = await api.stopSession(sessionId, {
          scope: 'all', include_shells: opts?.includeShells ?? true,
        }) as StopAllResult;
        // The turn is stopped too, so a banner about it is stale.
        if (gen === generation) {
          const left = (res.unsupported ?? []).map(id => get().agents[id]?.label ?? id);
          set({
            escalation: null,
            stopError: left.length === 0 ? null
              : `${left.slice(0, 3).join(', ')}${left.length > 3 ? ` and ${left.length - 3} more` : ''} `
                + `can't be stopped one by one on this runtime.`,
          });
        }
        return res;
      } catch (e) {
        console.error('Failed to stop all agents:', e);
        if (gen === generation) set({ stopError: `Couldn't stop the agents: ${failureReason(e)}.` });
        return null;
      } finally {
        end();
      }
    },

    forceStop: async () => {
      const sessionId = get().sessionId;
      set({ stopDialog: null, escalation: null });
      if (!sessionId) return null;
      const end = beginStop();
      try {
        return await api.stopSession(sessionId, { scope: 'force' }) as ForceStopResult;
      } catch (e) {
        console.error('Failed to force stop:', e);
        set({ stopError: `Force stop failed: ${failureReason(e)}.` });
        return null;
      } finally {
        end();
      }
    },

    cancelQueued: async (queueId) => {
      const s = get();
      const sessionId = s.sessionId;
      if (!sessionId) return;
      const gen = generation;
      const clientMsgId = s.queue.find(q => q.queue_id === queueId)?.client_msg_id ?? null;
      if (clientMsgId) cancelling.add(clientMsgId);
      const dropEntry = () => {
        removedQueueIds.add(queueId);
        set((st) => ({ queue: st.queue.filter(q => q.queue_id !== queueId) }));
      };
      try {
        const res = await api.cancelQueued(sessionId, queueId);
        if (gen !== generation) return;
        dropEntry();
        // The text comes back from the answer; the queue_update that follows
        // finds no pending send (or skips this tab's own cancel).
        if (clientMsgId) get().dropPendingSend(clientMsgId);
        if (res?.content) get().prefillComposer(res.content, 'append');
      } catch (e) {
        if (clientMsgId) cancelling.delete(clientMsgId);
        const code = apiErrorCode(e);
        if (code === 'queue_entry_not_found') {
          if (gen === generation) dropEntry();
        } else if (code !== 'already_started' && code !== 'not_owner') {
          console.error('Failed to cancel the queued message:', e);
        }
      }
    },

    markSeen: (agentIds) => {
      const s = get();
      const fresh = agentIds.filter((id) => id && !s.seen[id]);
      if (fresh.length === 0) return;
      let ids = [...Object.keys(s.seen), ...fresh];
      if (ids.length > MAX_SEEN_IDS) ids = ids.slice(-MAX_SEEN_IDS);
      const seen: Record<string, true> = {};
      for (const id of ids) seen[id] = true;
      set({ seen });
      if (s.sessionId) persistSeen(s.sessionId, ids);
    },

    setStopDialog: (d) => set({ stopDialog: d }),

    dismissEscalation: () => set({ escalation: null }),

    dismissStopError: () => set({ stopError: null }),

    prefillComposer: (text, mode = 'append') => {
      set({ composerInsert: { nonce: Date.now() + Math.random(), text, mode } });
    },

    requestFocus: () => set((s) => ({ focusNonce: s.focusNonce + 1 })),
  };
});
