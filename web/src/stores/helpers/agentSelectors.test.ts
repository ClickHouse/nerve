import { describe, expect, it } from 'vitest';
import {
  ATTENTION_ORDER, agentForBlock, clientAttention, dockLine, groupAgents, isLive, toTranscriptSegments, triggersLabel,
} from './agentSelectors';
import {
  FX_CHILD, FX_COMPLETED, FX_FAILED, FX_INTERRUPTED_RESTART, FX_LOST, FX_NOW_MS, FX_PARENT, FX_RUNNING, FX_SHELL,
  FX_STARTING, FX_STOPPED_BY_USER, FX_STOPPING_UNCONFIRMED, FX_WAVE, makeAgentView, toAgentMap, toSpawnMap,
} from '../../test/agentFixtures';
import type { AgentEventItem, AgentView, AutoTurnTrigger } from '../../types/agents';

const QUIET = 10;
const ids = (agents: AgentView[]) => agents.map(a => a.id);
const minutesAgo = (m: number) => new Date(FX_NOW_MS - m * 60_000).toISOString();

describe('groupAgents', () => {
  it('splits a wave into attention, a running tree, done and ended', () => {
    const g = groupAgents(toAgentMap(FX_WAVE), {}, FX_NOW_MS, QUIET);

    // failed ranks before stop_unconfirmed in ATTENTION_ORDER.
    expect(ids(g.attention)).toEqual([FX_FAILED.id, FX_STOPPING_UNCONFIRMED.id]);
    expect(g.running.map(r => [r.agent.id, r.depth, r.childCount])).toEqual([
      [FX_RUNNING.id, 0, 0],
      [FX_PARENT.id, 0, 1],
      [FX_CHILD.id, 1, 0],
      [FX_SHELL.id, 0, 0],
    ]);
    expect(ids(g.done)).toEqual([FX_COMPLETED.id]);
    expect(ids(g.ended)).toEqual([FX_STOPPED_BY_USER.id]);
  });

  it('orders attention by ATTENTION_ORDER, then newest first', () => {
    const asking = makeAgentView({ attention: 'needs_input', last_activity_at: minutesAgo(1) });
    const olderFailure = makeAgentView({ status: 'failed', ended_at: minutesAgo(30) });
    const unconfirmed = makeAgentView({ status: 'stopping', attention: 'stop_unconfirmed' });
    const agents = toAgentMap([FX_LOST, unconfirmed, olderFailure, FX_INTERRUPTED_RESTART, FX_FAILED, asking]);

    const g = groupAgents(agents, {}, FX_NOW_MS, QUIET);

    expect(ids(g.attention)).toEqual([
      asking.id, FX_FAILED.id, olderFailure.id, FX_INTERRUPTED_RESTART.id, FX_LOST.id, unconfirmed.id,
    ]);
    expect(ATTENTION_ORDER).toEqual(['needs_input', 'failed', 'interrupted', 'lost', 'stop_unconfirmed', 'quiet']);
  });

  it('moves seen failures to Ended, newest ended first', () => {
    const seen = { [FX_FAILED.id]: true as const, [FX_LOST.id]: true as const };
    const g = groupAgents(toAgentMap([...FX_WAVE, FX_LOST]), seen, FX_NOW_MS, QUIET);

    expect(ids(g.attention)).toEqual([FX_STOPPING_UNCONFIRMED.id]);
    expect(ids(g.ended)).toEqual([FX_FAILED.id, FX_STOPPED_BY_USER.id, FX_LOST.id]);
  });

  it('roots a child whose parent is not running, keeping its own depth', () => {
    const parentDone = { ...FX_PARENT, status: 'completed' as const, ended_at: minutesAgo(1) };
    const g = groupAgents(toAgentMap([parentDone, FX_CHILD]), {}, FX_NOW_MS, QUIET);

    expect(g.running).toEqual([{ agent: FX_CHILD, depth: 0, childCount: 0 }]);
    expect(g.running[0].agent.depth).toBe(2);
    expect(ids(g.done)).toEqual([parentDone.id]);
  });

  it('nests grandchildren and keeps quiet agents in Running', () => {
    const grandchild = makeAgentView({ parent_agent_id: FX_CHILD.id, depth: 3, created_at: '2026-02-27T02:06:00Z' });
    const quiet = makeAgentView({ last_activity_at: minutesAgo(25), run_started_at: minutesAgo(40) });
    const g = groupAgents(toAgentMap([FX_CHILD, grandchild, FX_PARENT, quiet]), {}, FX_NOW_MS, QUIET);

    expect(g.running.map(r => [r.agent.id, r.depth])).toEqual([
      [quiet.id, 0], [FX_PARENT.id, 0], [FX_CHILD.id, 1], [grandchild.id, 2],
    ]);
    expect(g.attention).toEqual([]);
  });

  it('survives a parent cycle', () => {
    const a = makeAgentView({ id: 'ag_cycle0000001', parent_agent_id: 'ag_cycle0000002' });
    const b = makeAgentView({ id: 'ag_cycle0000002', parent_agent_id: 'ag_cycle0000001' });
    const g = groupAgents(toAgentMap([a, b]), {}, FX_NOW_MS, QUIET);
    expect(g.running).toHaveLength(2);
  });

  it('hides ambient records unless asked', () => {
    const ambient = makeAgentView({ ambient: true, kind: 'other' });
    expect(groupAgents(toAgentMap([ambient]), {}, FX_NOW_MS, QUIET).running).toEqual([]);
    expect(groupAgents(toAgentMap([ambient]), {}, FX_NOW_MS, QUIET, { showAmbient: true }).running).toHaveLength(1);
  });
});

describe('clientAttention', () => {
  it('is quiet after the threshold since the last activity or (re)start', () => {
    const startedLongAgo = { run_started_at: minutesAgo(30) };
    expect(clientAttention(makeAgentView({ ...startedLongAgo, last_activity_at: minutesAgo(9.9) }), FX_NOW_MS, QUIET)).toBeNull();
    expect(clientAttention(makeAgentView({ ...startedLongAgo, last_activity_at: minutesAgo(10) }), FX_NOW_MS, QUIET)).toBe('quiet');
    // Resumed a minute ago after a long silence: not quiet.
    expect(clientAttention(makeAgentView({ last_activity_at: minutesAgo(60), run_started_at: minutesAgo(1) }), FX_NOW_MS, QUIET)).toBeNull();
    expect(clientAttention(makeAgentView({ last_activity_at: null, run_started_at: minutesAgo(11) }), FX_NOW_MS, QUIET)).toBe('quiet');
  });

  it('prefers the server attention and only marks running agents quiet', () => {
    expect(clientAttention(FX_FAILED, FX_NOW_MS, QUIET)).toBe('failed');
    expect(clientAttention(FX_STOPPING_UNCONFIRMED, FX_NOW_MS, QUIET)).toBe('stop_unconfirmed');
    expect(clientAttention({ ...FX_STARTING, run_started_at: minutesAgo(30) }, FX_NOW_MS, QUIET)).toBeNull();
    expect(clientAttention(makeAgentView({ last_activity_at: minutesAgo(99) }), FX_NOW_MS, 0)).toBeNull();
  });

  it('treats starting, running and stopping as live', () => {
    expect([FX_STARTING, FX_RUNNING, FX_STOPPING_UNCONFIRMED, FX_COMPLETED, FX_LOST].map(isLive))
      .toEqual([true, true, true, false, false]);
  });
});

describe('dockLine', () => {
  it('is hidden without non-ambient records', () => {
    expect(dockLine({}, {}, FX_NOW_MS, QUIET).visible).toBe(false);
    const ambient = makeAgentView({ ambient: true });
    expect(dockLine(toAgentMap([ambient]), {}, FX_NOW_MS, QUIET)).toMatchObject({ visible: false, headline: null });
  });

  it('counts the groups and headlines the newest running activity', () => {
    const line = dockLine(toAgentMap(FX_WAVE), {}, FX_NOW_MS, QUIET);

    expect(line).toEqual({
      visible: true, running: 4, attention: 2, done: 1, ended: 1,
      headline: { agentId: FX_CHILD.id, text: 'build-ui-tests: Edit: app.test.tsx' },
    });
  });

  it('prefers the summary and shows a terminal-only session without a headline', () => {
    const fresh = makeAgentView({ summary: 'Writing the migration', last_activity_at: minutesAgo(0.1) });
    expect(dockLine(toAgentMap([FX_RUNNING, fresh]), {}, FX_NOW_MS, QUIET).headline)
      .toEqual({ agentId: fresh.id, text: 'build-api: Writing the migration' });
    expect(dockLine(toAgentMap([FX_COMPLETED]), {}, FX_NOW_MS, QUIET)).toMatchObject({ visible: true, done: 1, headline: null });
  });

  it('shows counts only above 20 live agents', () => {
    const many = Array.from({ length: 21 }, () => makeAgentView());
    const line = dockLine(toAgentMap(many), {}, FX_NOW_MS, QUIET);
    expect(line.running).toBe(21);
    expect(line.headline).toBeNull();
  });
});

describe('agentForBlock', () => {
  const state = { agents: toAgentMap(FX_WAVE), bySpawn: toSpawnMap(FX_WAVE) };

  it('resolves by the persisted agent id first, then by the spawn tool_use id', () => {
    expect(agentForBlock(state, { toolUseId: 'toolu_unrelated', agentId: FX_PARENT.id })).toBe(FX_PARENT);
    expect(agentForBlock(state, { toolUseId: FX_RUNNING.spawn_tool_use_id! })).toBe(FX_RUNNING);
    expect(agentForBlock(state, { toolUseId: FX_RUNNING.spawn_tool_use_id!, agentId: 'ag_unknown00000' })).toBe(FX_RUNNING);
  });

  it('is null for old history without a record', () => {
    expect(agentForBlock(state, { toolUseId: 'toolu_old' })).toBeNull();
  });
});

describe('toTranscriptSegments', () => {
  const at = '2026-02-27T02:05:00Z';
  const items: AgentEventItem[] = [
    { seq: 1, at, kind: 'thinking', text: 'Plan the fix.' },
    { seq: 2, at, kind: 'text', text: 'Running the tests.' },
    { seq: 3, at, kind: 'text', text: 'Two of them.' },
    { seq: 4, at, kind: 'tool_call', tool_use_id: 'toolu_a', tool: 'Bash', input: { command: 'pytest -q' } },
    { seq: 5, at, kind: 'tool_call', tool_use_id: 'toolu_b', tool: 'Agent', input: { description: 'x' }, child_agent_id: FX_CHILD.id },
    { seq: 6, at, kind: 'tool_result', tool_use_id: 'toolu_a', result: '212 passed', is_error: false },
    { seq: 7, at, kind: 'lifecycle', event: 'stopped', run: 1, reason: 'user', actor_id: 'act_owner' },
    { seq: 8, at, kind: 'tool_result', tool_use_id: 'toolu_gone', result: 'tail', is_error: true, truncated: true, full_size: 40000 },
    { seq: 9, at, kind: 'text', text: 'Resumed.' },
  ];

  it('merges text, completes tool calls and splits at lifecycle items', () => {
    const segments = toTranscriptSegments(items);

    expect(segments.map(s => [s.kind, s.key])).toEqual([['blocks', 'b1'], ['lifecycle', 'l7'], ['blocks', 'b8']]);
    const [first, divider, last] = segments;
    expect(first.kind === 'blocks' && first.blocks).toEqual([
      { type: 'thinking', content: 'Plan the fix.' },
      { type: 'text', content: 'Running the tests.\n\nTwo of them.' },
      { type: 'tool_call', toolUseId: 'toolu_a', tool: 'Bash', input: { command: 'pytest -q' }, status: 'complete', result: '212 passed', isError: false },
      { type: 'tool_call', toolUseId: 'toolu_b', tool: 'Agent', input: { description: 'x' }, status: 'running', agentId: FX_CHILD.id },
    ]);
    expect(divider.kind === 'lifecycle' && divider.item.event).toBe('stopped');
    expect(last.kind === 'blocks' && last.blocks).toEqual([
      { type: 'tool_call', toolUseId: 'toolu_gone', tool: 'Tool', input: {}, status: 'complete', isError: true, result: 'tail\n… (truncated; 40000 bytes in full)' },
      { type: 'text', content: 'Resumed.' },
    ]);
  });

  it('keeps segment keys while items are appended', () => {
    const before = toTranscriptSegments(items.slice(0, 3)).map(s => s.key);
    const after = toTranscriptSegments(items).map(s => s.key);
    expect(after[0]).toBe(before[0]);
    expect(toTranscriptSegments([])).toEqual([]);
  });
});

describe('triggersLabel', () => {
  const trigger = (label: string, status: AutoTurnTrigger['status']): AutoTurnTrigger => ({ agent_id: `ag_${label}`, label, kind: 'agent', status });

  it('names one agent and how it settled', () => {
    expect(triggersLabel([trigger('build-chart', 'completed')])).toBe('Background · build-chart finished');
    expect(triggersLabel([trigger('review-api', 'failed')])).toBe('Background · review-api failed');
    expect(triggersLabel([trigger('lint-all', 'stopped')])).toBe('Background · lint-all stopped');
    expect(triggersLabel([trigger('migrate-db', 'interrupted')])).toBe('Background · migrate-db interrupted');
    expect(triggersLabel([trigger('probe-cache', 'lost')])).toBe('Background · probe-cache lost track');
  });

  it('counts several agents and their failures', () => {
    const three = [trigger('a', 'completed'), trigger('b', 'failed'), trigger('c', 'completed')];
    expect(triggersLabel(three)).toBe('Background · 3 agents settled (1 failed)');
    expect(triggersLabel([trigger('a', 'completed')], 4)).toBe('Background · 5 agents settled');
  });

  it('falls back for old history', () => {
    expect(triggersLabel(undefined)).toBe('Background continuation');
    expect(triggersLabel([], 0)).toBe('Background continuation');
  });
});
