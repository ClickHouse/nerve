// Agent records for tests: builders plus named fixtures in every state the UI
// distinguishes. Shapes follow docs/api.md key for key; all names and ids are
// placeholders.
//
// Seed the store with a whole session, either through the reducers:
//
//   useAgentStore.getState().reset(FX_SESSION_ID);
//   useAgentStore.getState().hydrate(makeSnapshot(FX_WAVE));
//
// or directly: useAgentStore.setState(makeAgentStoreSeed(FX_WAVE)).
//
// Times are fixed around FX_NOW / FX_NOW_MS, so pass FX_NOW_MS as `nowMs` to
// the selectors (or fake the clock) for deterministic elapsed / quiet results.

import {
  LIVE_STATUSES,
  type AgentCapabilities, type AgentControls, type AgentEndReason, type AgentEventItem, type AgentSnapshot,
  type AgentSummary, type AgentView, type QueueEntryView, type AgentAttention,
} from '../types/agents';
import type { AgentStoreState, TranscriptState } from '../stores/agentStore';

export const FX_SESSION_ID = 's-1234';
/** "Now" for every fixture: eight minutes after the wave started. */
export const FX_NOW = '2026-02-27T02:10:00Z';
export const FX_NOW_MS = Date.parse(FX_NOW);
/** The actor of the person using the tab in these fixtures. */
export const FX_OWNER_ACTOR = 'act_owner';

/** Capabilities of a live Claude runtime that spares background agents on Stop. */
export const FX_CAPABILITIES: AgentCapabilities = {
  stop_task: true, transcript: true, output: true, interrupt_spares_background: true, quiet_after_minutes: 10,
};

const TERMINAL_END_REASON: Partial<Record<AgentView['status'], AgentEndReason>> = {
  completed: 'completed', failed: 'error', stopped: 'user', interrupted: 'gateway_restart', lost: 'vanished',
};

let fixtureSeq = 0;
const nextFixtureSeq = () => ++fixtureSeq;

/** `ag_` plus 12 hex digits, unique per call. */
function freshAgentId(): string {
  return `ag_${nextFixtureSeq().toString(16).padStart(12, '0')}`;
}

/** The server's label rule: name, description, command, task type, kind. */
function deriveLabel(v: Pick<AgentView, 'name' | 'description' | 'command' | 'task_type' | 'kind'>): string {
  return v.name || v.description || (v.command ? v.command.slice(0, 60) : '') || v.task_type || v.kind;
}

/** The server's attention rule, minus `needs_input` (set it explicitly). */
function deriveAttention(v: Pick<AgentView, 'status'>): AgentAttention | null {
  return v.status === 'failed' || v.status === 'interrupted' || v.status === 'lost' ? v.status : null;
}

function deriveControls(v: AgentView): AgentControls {
  const live = LIVE_STATUSES.has(v.status);
  const stoppedByPerson = v.end_reason === 'user' || v.end_reason === 'stop_all';
  return {
    stop: live && !v.ambient && v.backend === 'claude',
    transcript: v.backend === 'claude' && (v.kind === 'agent' || v.kind === 'teammate' || v.kind === 'remote_agent'),
    output: v.kind === 'shell' || v.kind === 'monitor',
    message: v.backend === 'claude' && v.kind === 'agent' && (live || v.resumable) && !stoppedByPerson,
  };
}

/**
 * One AgentView. The default is a running background general-purpose agent;
 * fields not given in `overrides` that the server derives (label, attention,
 * end_reason, ended_at, controls) follow the overrides the way the server's
 * `to_view()` would.
 */
export function makeAgentView(overrides: Partial<AgentView> = {}): AgentView {
  const n = nextFixtureSeq();
  const base: AgentView = {
    id: freshAgentId(),
    session_id: FX_SESSION_ID,
    backend: 'claude',
    kind: 'agent',
    task_id: `a${n.toString(16).padStart(16, '0')}`,
    task_type: 'local_agent',
    spawn_tool_use_id: `toolu_fx${n.toString().padStart(4, '0')}`,
    parent_agent_id: null,
    depth: 1,
    name: 'build-api',
    label: 'build-api',
    description: 'Build the API',
    agent_type: 'general-purpose',
    command: null,
    background: true,
    ambient: false,
    status: 'running',
    end_reason: null,
    error: null,
    attention: null,
    activity: 'Bash: pytest -q',
    summary: 'Running the API test suite',
    result_preview: null,
    requested_model: 'opus',
    model: 'model-a',
    model_changed: null,
    tokens: 38211,
    tool_uses: 17,
    cost_usd: 1.42,
    runs: 1,
    created_at: '2026-02-27T02:01:00Z',
    started_at: '2026-02-27T02:01:01Z',
    run_started_at: '2026-02-27T02:01:01Z',
    last_activity_at: '2026-02-27T02:05:12Z',
    ended_at: null,
    stop_requested_at: null,
    stopped_by_actor_id: null,
    resumable: false,
    controls: { stop: true, transcript: true, output: false, message: true },
    workflow: null,
    rev: 1772150400000000 + n,
  };
  const v: AgentView = { ...base, ...overrides };
  const terminal = !LIVE_STATUSES.has(v.status);
  if (overrides.label === undefined) v.label = deriveLabel(v);
  if (overrides.attention === undefined) v.attention = deriveAttention(v);
  if (overrides.end_reason === undefined) v.end_reason = terminal ? TERMINAL_END_REASON[v.status] ?? null : null;
  if (overrides.ended_at === undefined) v.ended_at = terminal ? '2026-02-27T02:08:00Z' : null;
  if (overrides.controls === undefined) v.controls = deriveControls(v);
  return v;
}

/** The server's AgentSummary for these records (ambient ones are not counted). */
export function makeSummary(agents: AgentView[]): AgentSummary {
  const counted = agents.filter(a => !a.ambient);
  const live = counted.filter(a => LIVE_STATUSES.has(a.status));
  const count = (status: AgentView['status']) => counted.filter(a => a.status === status).length;
  const liveStarts = live.map(a => a.started_at).filter((t): t is string => !!t).sort();
  return {
    total: counted.length,
    live: live.length,
    starting: count('starting'),
    running: count('running'),
    stopping: count('stopping'),
    completed: count('completed'),
    failed: count('failed'),
    stopped: count('stopped'),
    interrupted: count('interrupted'),
    lost: count('lost'),
    attention: counted.filter(a => a.attention !== null).length,
    shells_live: live.filter(a => a.kind === 'shell' || a.kind === 'monitor').length,
    tokens: counted.reduce((sum, a) => sum + a.tokens, 0),
    cost_usd: Math.round(counted.reduce((sum, a) => sum + a.cost_usd, 0) * 100) / 100,
    oldest_live_started_at: liveStarts[0] ?? null,
  };
}

/** A full AgentSnapshot for these records (session FX_SESSION_ID unless overridden). */
export function makeSnapshot(agents: AgentView[], overrides: Partial<AgentSnapshot> = {}): AgentSnapshot {
  return {
    session_id: FX_SESSION_ID,
    agents,
    summary: makeSummary(agents),
    capabilities: FX_CAPABILITIES,
    queue: [],
    generated_at: FX_NOW,
    ...overrides,
  };
}

/** One queued message of the owner (unique queue and client ids per call). */
export function makeQueueEntry(overrides: Partial<QueueEntryView> = {}): QueueEntryView {
  const n = nextFixtureSeq();
  return {
    queue_id: `q_${n.toString(16).padStart(8, '0')}`,
    client_msg_id: `00000000-0000-4000-8000-${n.toString(16).padStart(12, '0')}`,
    preview: 'also run the e2e suite',
    source: 'web',
    internal: false,
    actor_id: FX_OWNER_ACTOR,
    created_at: '2026-02-27T02:06:00Z',
    ...overrides,
  };
}

/**
 * `n` contiguous transcript items from `startSeq`: a repeating text, tool call,
 * tool result pattern (each result answers the call just before it).
 */
export function makeTranscriptItems(n: number, startSeq = 1): AgentEventItem[] {
  const items: AgentEventItem[] = [];
  for (let i = 0; i < n; i++) {
    const seq = startSeq + i;
    const at = new Date(Date.parse('2026-02-27T02:01:00Z') + seq * 1000).toISOString().replace('.000Z', 'Z');
    if (i % 3 === 0) {
      items.push({ seq, at, kind: 'text', text: `Step ${seq}: checking the build.` });
    } else if (i % 3 === 1) {
      items.push({
        seq, at, kind: 'tool_call', tool_use_id: `toolu_item${seq}`, tool: 'Bash',
        input: { command: `pytest -q tests/test_${seq}.py` }, input_truncated: false, child_agent_id: null,
      });
    } else {
      items.push({
        seq, at, kind: 'tool_result', tool_use_id: `toolu_item${seq - 1}`,
        result: `${seq} passed`, is_error: false, truncated: false, full_size: 9,
      });
    }
  }
  return items;
}

/**
 * A `useAgentStore.setState` patch for one session holding `agents` (the
 * state `reset(FX_SESSION_ID)` + `hydrate(makeSnapshot(agents))` would reach).
 */
export function makeAgentStoreSeed(
  agents: AgentView[], overrides: Partial<AgentStoreState> = {},
): Partial<AgentStoreState> {
  return {
    sessionId: FX_SESSION_ID,
    agents: toAgentMap(agents),
    bySpawn: toSpawnMap(agents),
    summary: makeSummary(agents),
    capabilities: FX_CAPABILITIES,
    queue: [],
    pendingSends: {},
    transcripts: {},
    seen: {},
    escalation: null,
    stopDialog: null,
    stopInFlight: false,
    stopError: null,
    composerInsert: null,
    ...overrides,
  };
}

/** A loaded transcript holding `items` (ascending), for `transcripts[agentId]`. */
export function makeTranscriptState(items: AgentEventItem[], overrides: Partial<TranscriptState> = {}): TranscriptState {
  return {
    status: 'ready',
    items,
    firstSeq: items.length ? items[0].seq : null,
    lastSeq: items.length ? items[items.length - 1].seq : null,
    hasMoreBefore: items.length > 0 && items[0].seq > 1,
    pending: [],
    gapFetching: false,
    ...overrides,
  };
}

/** Records keyed by id, the shape of `useAgentStore.getState().agents`. */
export function toAgentMap(agents: AgentView[]): Record<string, AgentView> {
  return Object.fromEntries(agents.map(a => [a.id, a]));
}

/** spawn_tool_use_id → id, the shape of `useAgentStore.getState().bySpawn`. */
export function toSpawnMap(agents: AgentView[]): Record<string, string> {
  return Object.fromEntries(agents.filter(a => a.spawn_tool_use_id).map(a => [a.spawn_tool_use_id as string, a.id]));
}

// ------------------------------------------------------------------ //
//  Named fixtures                                                      //
// ------------------------------------------------------------------ //

export const FX_RUNNING: AgentView = makeAgentView({
  id: 'ag_3f9a1c2b7d4e', task_id: 'a1b2c3d4e5f6a7b8c', spawn_tool_use_id: 'toolu_01AGENT',
  name: 'build-api', description: 'Build the API', created_at: '2026-02-27T02:01:00Z',
});

export const FX_STARTING: AgentView = makeAgentView({
  id: 'ag_0000000000a2', task_id: null, task_type: null, spawn_tool_use_id: 'toolu_02STARTING',
  name: 'build-web', description: 'Build the web app', status: 'starting',
  activity: null, summary: null, model: null, tokens: 0, tool_uses: 0, cost_usd: 0,
  created_at: '2026-02-27T02:09:50Z', started_at: null, run_started_at: null, last_activity_at: null,
});

export const FX_STOPPING_UNCONFIRMED: AgentView = makeAgentView({
  id: 'ag_0000000000a3', spawn_tool_use_id: 'toolu_03STOPPING',
  name: 'build-docs', description: 'Build the docs', status: 'stopping', attention: 'stop_unconfirmed',
  activity: 'Bash: make html', summary: null, tokens: 12040, tool_uses: 6, cost_usd: 0.31,
  created_at: '2026-02-27T02:02:00Z', started_at: '2026-02-27T02:02:01Z', run_started_at: '2026-02-27T02:02:01Z',
  last_activity_at: '2026-02-27T02:07:30Z', stop_requested_at: '2026-02-27T02:09:40Z',
  stopped_by_actor_id: FX_OWNER_ACTOR,
});

export const FX_COMPLETED: AgentView = makeAgentView({
  id: 'ag_0000000000a4', spawn_tool_use_id: 'toolu_04COMPLETED',
  name: 'build-chart', description: 'Build the chart', status: 'completed', resumable: true,
  activity: 'Edit: chart.tsx', summary: 'Chart builds and renders', result_preview: '4 files changed',
  tokens: 95020, tool_uses: 41, cost_usd: 2.87,
  created_at: '2026-02-27T02:01:30Z', started_at: '2026-02-27T02:01:31Z', run_started_at: '2026-02-27T02:01:31Z',
  last_activity_at: '2026-02-27T02:07:55Z', ended_at: '2026-02-27T02:08:00Z',
});

export const FX_FAILED: AgentView = makeAgentView({
  id: 'ag_0000000000a5', spawn_tool_use_id: 'toolu_05FAILED',
  name: 'review-api', description: 'Review the API', agent_type: 'Explore', status: 'failed',
  error: 'Tests failed: 3 errors\nsee the log', activity: 'Bash: pytest -q', summary: null,
  tokens: 20500, tool_uses: 9, cost_usd: 0.64,
  created_at: '2026-02-27T02:03:00Z', started_at: '2026-02-27T02:03:01Z', run_started_at: '2026-02-27T02:03:01Z',
  last_activity_at: '2026-02-27T02:07:58Z', ended_at: '2026-02-27T02:08:00Z',
});

export const FX_STOPPED_BY_USER: AgentView = makeAgentView({
  id: 'ag_0000000000a6', spawn_tool_use_id: 'toolu_06STOPPED',
  name: 'lint-all', description: 'Lint everything', status: 'stopped', end_reason: 'user',
  activity: 'Bash: npm run lint', summary: null, tokens: 8800, tool_uses: 4, cost_usd: 0.2,
  created_at: '2026-02-27T02:01:45Z', started_at: '2026-02-27T02:01:46Z', run_started_at: '2026-02-27T02:01:46Z',
  last_activity_at: '2026-02-27T02:04:00Z', ended_at: '2026-02-27T02:04:10Z',
  stop_requested_at: '2026-02-27T02:04:05Z', stopped_by_actor_id: FX_OWNER_ACTOR,
});

export const FX_INTERRUPTED_RESTART: AgentView = makeAgentView({
  id: 'ag_0000000000a7', spawn_tool_use_id: 'toolu_07INTERRUPTED',
  name: 'migrate-db', description: 'Migrate the database', status: 'interrupted', end_reason: 'gateway_restart',
  activity: 'Bash: alembic upgrade head', summary: null, tokens: 15000, tool_uses: 7, cost_usd: 0.45,
  created_at: '2026-02-27T01:50:00Z', started_at: '2026-02-27T01:50:01Z', run_started_at: '2026-02-27T01:50:01Z',
  last_activity_at: '2026-02-27T01:58:00Z', ended_at: '2026-02-27T01:59:00Z',
});

export const FX_LOST: AgentView = makeAgentView({
  id: 'ag_0000000000a8', spawn_tool_use_id: 'toolu_08LOST',
  name: 'probe-cache', description: 'Probe the cache', status: 'lost', end_reason: 'vanished',
  activity: 'Read: cache.py', summary: null, tokens: 3000, tool_uses: 2, cost_usd: 0.05,
  created_at: '2026-02-27T01:55:00Z', started_at: '2026-02-27T01:55:01Z', run_started_at: '2026-02-27T01:55:01Z',
  last_activity_at: '2026-02-27T01:56:00Z', ended_at: '2026-02-27T01:57:00Z',
});

export const FX_SHELL: AgentView = makeAgentView({
  id: 'ag_0000000000a9', kind: 'shell', task_id: 'b1c2d3e4f', task_type: 'local_bash',
  spawn_tool_use_id: 'toolu_09SHELL', name: null, description: '', agent_type: null,
  command: 'npm run e2e', requested_model: null, model: null,
  activity: null, summary: null, tokens: 0, tool_uses: 0, cost_usd: 0,
  created_at: '2026-02-27T02:06:00Z', started_at: '2026-02-27T02:06:01Z', run_started_at: '2026-02-27T02:06:01Z',
  last_activity_at: '2026-02-27T02:06:01Z',
});

export const FX_WORKFLOW: AgentView = makeAgentView({
  id: 'ag_0000000000aa', kind: 'workflow', task_type: 'local_workflow', spawn_tool_use_id: 'toolu_10WORKFLOW',
  name: 'release-wave', description: 'Run the release wave', agent_type: null, requested_model: null,
  activity: 'Phase 2 of 3', summary: null, tokens: 210000, tool_uses: 88, cost_usd: 0,
  created_at: '2026-02-27T02:00:30Z', started_at: '2026-02-27T02:00:31Z', run_started_at: '2026-02-27T02:00:31Z',
  last_activity_at: '2026-02-27T02:09:30Z',
  workflow: {
    name: 'release-wave', status: 'running',
    phases: [{ index: 0, title: 'Build' }, { index: 1, title: 'Test' }, { index: 2, title: 'Ship' }],
    agents: [
      { label: 'build-a', phaseIndex: 0, state: 'done', tokens: 52000, toolCalls: 20 },
      { label: 'test-a', phaseIndex: 1, state: 'running', tokens: 31000, toolCalls: 12, lastToolName: 'Bash' },
    ],
    totalTokens: 210000, totalToolCalls: 88, agentCount: 2,
  },
});

export const FX_PARENT: AgentView = makeAgentView({
  id: 'ag_0000000000ab', spawn_tool_use_id: 'toolu_11PARENT',
  name: 'build-ui', description: 'Build the UI', activity: 'Agent: Test the UI', summary: 'Coordinating the UI work',
  tokens: 41000, tool_uses: 12, cost_usd: 1.1,
  created_at: '2026-02-27T02:01:15Z', started_at: '2026-02-27T02:01:16Z', run_started_at: '2026-02-27T02:01:16Z',
  last_activity_at: '2026-02-27T02:06:40Z',
});

export const FX_CHILD: AgentView = makeAgentView({
  id: 'ag_0000000000ac', spawn_tool_use_id: 'toolu_02CHILD', parent_agent_id: FX_PARENT.id, depth: 2,
  name: 'build-ui-tests', description: 'Test the UI', agent_type: 'general-purpose',
  activity: 'Edit: app.test.tsx', summary: null, tokens: 9100, tool_uses: 5, cost_usd: 0.27,
  created_at: '2026-02-27T02:05:00Z', started_at: '2026-02-27T02:05:01Z', run_started_at: '2026-02-27T02:05:01Z',
  last_activity_at: '2026-02-27T02:09:00Z',
});

export const FX_CODEX: AgentView = makeAgentView({
  id: 'ag_0000000000ad', backend: 'codex', task_id: null, task_type: null, spawn_tool_use_id: 'call_codex01',
  name: null, description: 'Review the parser', agent_type: 'worker', requested_model: null, model: null,
  activity: null, summary: null, tokens: 0, tool_uses: 0, cost_usd: 0,
  created_at: '2026-02-27T02:04:00Z', started_at: '2026-02-27T02:04:00Z', run_started_at: '2026-02-27T02:04:00Z',
  last_activity_at: null,
  controls: { stop: false, transcript: false, output: false, message: false },
});

/**
 * A wave of 8 agents covering every Agents-panel group: Needs attention
 * (failed, unconfirmed stop), Running (a plain agent, a parent with a nested
 * child, a shell), Done (completed) and Ended (stopped by the user).
 */
export const FX_WAVE: AgentView[] = [
  FX_RUNNING, FX_PARENT, FX_CHILD, FX_SHELL,
  FX_STOPPING_UNCONFIRMED, FX_COMPLETED, FX_FAILED, FX_STOPPED_BY_USER,
];
