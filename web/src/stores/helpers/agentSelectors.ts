// Pure views over the agent records of one session: the Agents
// panel groups, the composer dock line, the record behind an inline spawn
// card, transcript segments, and the auto-turn divider label. No store access,
// no clock: callers pass `nowMs`, so every function is deterministic.

import type { MessageBlock, ToolCallBlockData } from '../../types/chat';
import {
  LIVE_STATUSES,
  type AgentEventItem, type AgentView, type AutoTurnTrigger, type ClientAttention,
} from '../../types/agents';
import type { AgentStoreState } from '../agentStore';

export const ATTENTION_ORDER: ClientAttention[] = ['needs_input', 'failed', 'interrupted', 'lost', 'stop_unconfirmed', 'quiet'];

/** Above this many live agents the dock shows counts only. */
export const DOCK_HEADLINE_MAX_LIVE = 20;

/** Terminal statuses that ask for attention until the agent is seen. */
const FAILURE_STATUSES: ReadonlySet<AgentView['status']> = new Set(['failed', 'interrupted', 'lost']);

function ms(iso: string | null | undefined): number | null {
  if (!iso) return null;
  const t = Date.parse(iso);
  return Number.isFinite(t) ? t : null;
}

/** When the record last did anything: its latest activity or (re)start. */
function activityMs(a: AgentView): number | null {
  const last = ms(a.last_activity_at);
  const run = ms(a.run_started_at);
  if (last === null) return run;
  if (run === null) return last;
  return Math.max(last, run);
}

/** The moment that orders a record among its group, newest first. */
function eventMs(a: AgentView): number {
  return ms(a.ended_at) ?? ms(a.stop_requested_at) ?? activityMs(a) ?? ms(a.created_at) ?? 0;
}

function byCreated(a: AgentView, b: AgentView): number {
  return (ms(a.created_at) ?? 0) - (ms(b.created_at) ?? 0) || a.id.localeCompare(b.id);
}

function newestEndedFirst(a: AgentView, b: AgentView): number {
  return (ms(b.ended_at) ?? ms(b.created_at) ?? 0) - (ms(a.ended_at) ?? ms(a.created_at) ?? 0)
    || a.id.localeCompare(b.id);
}

export function isLive(a: AgentView): boolean {
  return LIVE_STATUSES.has(a.status);
}

/**
 * The server's attention, else `quiet` for a running agent with no activity
 * (latest of `last_activity_at` and `run_started_at`) for `quietAfterMin`
 * minutes. A threshold of 0 or less disables `quiet`.
 */
export function clientAttention(a: AgentView, nowMs: number, quietAfterMin: number): ClientAttention | null {
  if (a.attention) return a.attention;
  if (a.status !== 'running' || !(quietAfterMin > 0)) return null;
  const last = activityMs(a);
  if (last === null) return null;
  return nowMs - last >= quietAfterMin * 60_000 ? 'quiet' : null;
}

export interface AgentTreeRow { agent: AgentView; depth: number; childCount: number }
export interface AgentGroups { attention: AgentView[]; running: AgentTreeRow[]; done: AgentView[]; ended: AgentView[] }

/** Live agents waiting on a person, and failures nobody has looked at yet. */
function inAttention(a: AgentView, seen: Record<string, true>): boolean {
  if (isLive(a)) return a.attention === 'needs_input' || a.attention === 'stop_unconfirmed';
  return FAILURE_STATUSES.has(a.status) && !seen[a.id];
}

function attentionRank(a: AgentView, nowMs: number, quietAfterMin: number): number {
  const att = clientAttention(a, nowMs, quietAfterMin)
    ?? (FAILURE_STATUSES.has(a.status) ? a.status as ClientAttention : null);
  const i = att ? ATTENTION_ORDER.indexOf(att) : -1;
  return i < 0 ? ATTENTION_ORDER.length : i;
}

/** Live records as a forest: children under their live parents, by created_at. */
function runningTree(running: AgentView[]): AgentTreeRow[] {
  const ids = new Set(running.map(a => a.id));
  const children = new Map<string, AgentView[]>();
  const roots: AgentView[] = [];
  for (const a of [...running].sort(byCreated)) {
    const parent = a.parent_agent_id;
    if (parent && parent !== a.id && ids.has(parent)) {
      const list = children.get(parent);
      if (list) list.push(a);
      else children.set(parent, [a]);
    } else {
      roots.push(a);
    }
  }
  const rows: AgentTreeRow[] = [];
  const visited = new Set<string>();
  const walk = (a: AgentView, depth: number) => {
    if (visited.has(a.id)) return;
    visited.add(a.id);
    const kids = children.get(a.id) ?? [];
    rows.push({ agent: a, depth, childCount: kids.length });
    for (const kid of kids) walk(kid, depth + 1);
  };
  for (const root of roots) walk(root, 0);
  // A parent cycle has no root; show what is left at the top level.
  for (const a of [...running].sort(byCreated)) walk(a, 0);
  return rows;
}

/**
 * The Agents panel groups.
 * - attention: live with attention needs_input / stop_unconfirmed, plus failed /
 *   interrupted / lost not in `seen`; by ATTENTION_ORDER, then newest first.
 * - running: the other live records as a tree (a child whose parent is not in
 *   this group is a root; its own `agent.depth` still says how deep it runs).
 *   Quiet agents stay here.
 * - done: completed, newest ended first.
 * - ended: stopped, plus seen failed / interrupted / lost, newest ended first.
 * Ambient records are excluded unless `showAmbient`.
 */
export function groupAgents(
  agents: Record<string, AgentView>, seen: Record<string, true>, nowMs: number,
  quietAfterMin: number, opts?: { showAmbient?: boolean },
): AgentGroups {
  const attention: AgentView[] = [];
  const running: AgentView[] = [];
  const done: AgentView[] = [];
  const ended: AgentView[] = [];
  for (const a of Object.values(agents)) {
    if (a.ambient && !opts?.showAmbient) continue;
    if (inAttention(a, seen)) attention.push(a);
    else if (isLive(a)) running.push(a);
    else if (a.status === 'completed') done.push(a);
    else ended.push(a);
  }
  attention.sort((a, b) =>
    attentionRank(a, nowMs, quietAfterMin) - attentionRank(b, nowMs, quietAfterMin)
    || eventMs(b) - eventMs(a)
    || a.id.localeCompare(b.id));
  done.sort(newestEndedFirst);
  ended.sort(newestEndedFirst);
  return { attention, running: runningTree(running), done, ended };
}

export interface DockLine {
  visible: boolean; running: number; attention: number; done: number; ended: number;
  headline: { agentId: string; text: string } | null;   // newest last_activity_at among running: "label: summary ?? activity"
}

/**
 * The one-line dock above the composer. Visible as soon as one non-ambient
 * record exists. The headline is the running agent with the newest activity
 * (one with something to say wins over a silent one); there is none above
 * DOCK_HEADLINE_MAX_LIVE live agents.
 */
export function dockLine(
  agents: Record<string, AgentView>, seen: Record<string, true>, nowMs: number, quietAfterMin: number,
): DockLine {
  const groups = groupAgents(agents, seen, nowMs, quietAfterMin);
  const visible = Object.values(agents).some(a => !a.ambient);
  const liveCount = Object.values(agents).filter(a => !a.ambient && isLive(a)).length;
  let headline: DockLine['headline'] = null;
  if (liveCount <= DOCK_HEADLINE_MAX_LIVE) {
    const candidates = groups.running
      .map(r => r.agent)
      .sort((a, b) =>
        (activityMs(b) ?? ms(b.created_at) ?? 0) - (activityMs(a) ?? ms(a.created_at) ?? 0)
        || a.id.localeCompare(b.id));
    const pick = candidates.find(a => a.summary || a.activity) ?? candidates[0];
    if (pick) {
      const detail = pick.summary ?? pick.activity;
      headline = { agentId: pick.id, text: detail ? `${pick.label}: ${detail}` : pick.label };
    }
  }
  return {
    visible,
    running: groups.running.length,
    attention: groups.attention.length,
    done: groups.done.length,
    ended: groups.ended.length,
    headline,
  };
}

/** The record behind a spawn card: by its persisted agent id, else by its tool_use id. */
export function agentForBlock(
  state: Pick<AgentStoreState, 'agents' | 'bySpawn'>,
  block: { toolUseId: string; agentId?: string },
): AgentView | null {
  if (block.agentId) {
    const byId = state.agents[block.agentId];
    if (byId) return byId;
  }
  const id = state.bySpawn[block.toolUseId];
  return (id && state.agents[id]) || null;
}

export type TranscriptSegment =
  | { kind: 'blocks'; key: string; blocks: MessageBlock[] }
  | { kind: 'lifecycle'; key: string; item: Extract<AgentEventItem, { kind: 'lifecycle' }> };

function truncatedResult(item: Extract<AgentEventItem, { kind: 'tool_result' }>): string {
  if (!item.truncated) return item.result;
  const size = typeof item.full_size === 'number' ? `; ${item.full_size} bytes in full` : '';
  return `${item.result}\n… (truncated${size})`;
}

/**
 * Transcript items as renderable segments. Text and thinking become blocks
 * (adjacent items of the same kind merge as paragraphs); a tool call becomes a
 * running tool_call block (its `child_agent_id` becomes `agentId`, so a nested
 * spawn resolves to its record); a tool result completes its call; a
 * lifecycle item stands alone between block segments. A result whose call is
 * not among `items` (dropped by the cap) renders as a completed call of an
 * unknown tool. Keys are stable while items are appended.
 */
export function toTranscriptSegments(items: AgentEventItem[]): TranscriptSegment[] {
  const segments: TranscriptSegment[] = [];
  const calls = new Map<string, ToolCallBlockData>();
  let current: { kind: 'blocks'; key: string; blocks: MessageBlock[] } | null = null;
  const blocksFor = (seq: number) => {
    if (!current) {
      current = { kind: 'blocks', key: `b${seq}`, blocks: [] };
      segments.push(current);
    }
    return current.blocks;
  };
  for (const item of items) {
    switch (item.kind) {
      case 'lifecycle':
        current = null;
        segments.push({ kind: 'lifecycle', key: `l${item.seq}`, item });
        break;
      case 'text':
      case 'thinking': {
        const blocks = blocksFor(item.seq);
        const last = blocks[blocks.length - 1];
        if (last && (last.type === 'text' || last.type === 'thinking') && last.type === item.kind) {
          blocks[blocks.length - 1] = { ...last, content: `${last.content}\n\n${item.text}` };
        } else {
          blocks.push({ type: item.kind, content: item.text });
        }
        break;
      }
      case 'tool_call': {
        const block: ToolCallBlockData = {
          type: 'tool_call', toolUseId: item.tool_use_id, tool: item.tool, input: item.input, status: 'running',
          ...(item.child_agent_id ? { agentId: item.child_agent_id } : {}),
        };
        calls.set(item.tool_use_id, block);
        blocksFor(item.seq).push(block);
        break;
      }
      case 'tool_result': {
        const call = calls.get(item.tool_use_id);
        const result = truncatedResult(item);
        if (call) {
          call.result = result;
          call.isError = item.is_error;
          call.status = 'complete';
        } else {
          blocksFor(item.seq).push({
            type: 'tool_call', toolUseId: item.tool_use_id, tool: 'Tool', input: {},
            result, isError: item.is_error, status: 'complete',
          });
        }
        break;
      }
    }
  }
  return segments;
}

const SETTLED_VERB: Partial<Record<AutoTurnTrigger['status'], string>> = {
  completed: 'finished', failed: 'failed', stopped: 'stopped', interrupted: 'interrupted', lost: 'lost track',
};

/**
 * The divider text of an autonomous turn: "Background · build-chart finished"
 * for one trigger, "Background · 3 agents settled (1 failed)" for several
 * (`more` counts triggers beyond the list), "Background continuation" for none
 * (old history).
 */
export function triggersLabel(triggers: AutoTurnTrigger[] | undefined, more?: number): string {
  const list = triggers ?? [];
  const extra = more && more > 0 ? more : 0;
  const total = list.length + extra;
  if (total === 0) return 'Background continuation';
  if (list.length === 1 && extra === 0) {
    const [t] = list;
    return `Background · ${t.label} ${SETTLED_VERB[t.status] ?? 'updated'}`;
  }
  const failed = list.filter(t => t.status === 'failed').length;
  return `Background · ${total} agent${total === 1 ? '' : 's'} settled${failed ? ` (${failed} failed)` : ''}`;
}
