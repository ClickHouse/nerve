// How an agent record looks and reads everywhere it appears: the inline line
// at its spawn point, the dock, the Agents panel rows, the transcript header
// and the side-panel tabs.
//
// Two kinds of colour, never mixed: status ("how it went") uses the outcome
// tokens, identity ("what kind of agent") uses the hue-* scale.

import {
  Bot, CheckCircle2, CircleDashed, CircleHelp, CircleX, Files, Hourglass, Lightbulb, ListTree, Loader2,
  MessageCircleQuestion, Radio, Search, Square, SquareTerminal, Unlink, Users, Workflow, Wrench,
  type Icon,
} from '../../ui/icons';
import {
  LIVE_STATUSES,
  type AgentKind, type AgentStatus, type AgentView, type ClientAttention,
} from '../../../types/agents';
import { clientAttention } from '../../../stores/helpers/agentSelectors';

export interface StatusVisual {
  Icon: Icon;
  /** Colour (and motion) classes for the icon. */
  className: string;
  /** A short lower-case word for labels and screen readers ("running"). */
  label: string;
}

export interface IdentityVisual {
  Icon: Icon;
  /** The identity colour of the icon and of the agent's label. */
  className: string;
}

/** The status icon of a record. `attention` is the client attention (quiet included). */
export function statusVisual(status: AgentStatus, attention: ClientAttention | null): StatusVisual {
  if (attention === 'needs_input' && LIVE_STATUSES.has(status)) {
    return { Icon: MessageCircleQuestion, className: 'text-info', label: 'needs input' };
  }
  switch (status) {
    case 'starting':
      return { Icon: CircleDashed, className: 'text-text-faint', label: 'starting' };
    case 'running':
      return attention === 'quiet'
        ? { Icon: Hourglass, className: 'text-warning', label: 'quiet' }
        : { Icon: Loader2, className: 'motion-safe:animate-spin text-accent', label: 'running' };
    case 'stopping':
      // Muted either way: the row's text and its Retry carry an unconfirmed stop.
      return attention === 'stop_unconfirmed'
        ? { Icon: Loader2, className: 'motion-safe:animate-spin text-text-muted', label: 'stop not confirmed' }
        : { Icon: Loader2, className: 'motion-safe:animate-spin text-text-muted', label: 'stopping' };
    case 'completed':
      return { Icon: CheckCircle2, className: 'text-success', label: 'completed' };
    case 'failed':
      return { Icon: CircleX, className: 'text-error', label: 'failed' };
    case 'stopped':
      return { Icon: Square, className: 'text-text-muted', label: 'stopped' };
    case 'interrupted':
      return { Icon: Unlink, className: 'text-warning', label: 'interrupted' };
    case 'lost':
      return { Icon: CircleHelp, className: 'text-warning', label: 'lost' };
  }
}

/** Identity by agent type or panel-tab type (the colours the side panel already used). */
const IDENTITY: Record<string, IdentityVisual> = {
  'general-purpose': { Icon: Wrench, className: 'text-link' },
  Explore: { Icon: Search, className: 'text-hue-cyan' },
  Plan: { Icon: Lightbulb, className: 'text-hue-amber' },
  Workflow: { Icon: Workflow, className: 'text-hue-violet' },
  workflow: { Icon: Workflow, className: 'text-hue-violet' },
  shell: { Icon: SquareTerminal, className: 'text-hue-emerald' },
  monitor: { Icon: Radio, className: 'text-hue-sky' },
  teammate: { Icon: Users, className: 'text-hue-purple' },
  files: { Icon: Files, className: 'text-hue-teal' },
  agents: { Icon: ListTree, className: 'text-text-muted' },
};

const DEFAULT_IDENTITY: IdentityVisual = { Icon: Bot, className: 'text-text-muted' };

/**
 * Identity of a sub-agent type ("Explore"), an agent kind ("shell") or a side
 * panel tab type ("files", "agents"). Unknown types get the muted bot.
 */
export function identityVisual(type: string | null | undefined): IdentityVisual {
  return (type && IDENTITY[type]) || DEFAULT_IDENTITY;
}

/** Identity of a record: agents by their sub-agent type, everything else by kind. */
export function kindVisual(kind: AgentKind, agentType: string | null): IdentityVisual {
  if (kind === 'agent' || kind === 'remote_agent') return identityVisual(agentType);
  return identityVisual(kind);
}

/** The type badge: the sub-agent type for agents ("general" for general-purpose), else the kind. */
export function typeLabel(view: Pick<AgentView, 'kind' | 'agent_type'>): string {
  if (view.kind === 'agent' || view.kind === 'remote_agent') {
    if (!view.agent_type) return 'agent';
    return view.agent_type === 'general-purpose' ? 'general' : view.agent_type;
  }
  return view.kind === 'mcp_task' ? 'task' : view.kind;
}

// ------------------------------------------------------------------ //
//  Numbers and times                                                   //
// ------------------------------------------------------------------ //

/** Compact elapsed time: "45s", "12m", "1h 5m", "2d 3h". Empty for a missing value. */
export function formatElapsed(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms) || ms < 0) return '';
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ${minutes % 60}m`;
  return `${Math.floor(hours / 24)}d ${hours % 24}h`;
}

const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? '' : 's'}`;

/** Elapsed time for screen readers: "45 seconds", "4 minutes", "1 hour 5 minutes". */
export function formatElapsedLong(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms) || ms < 0) return '';
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return plural(seconds, 'second');
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return plural(minutes, 'minute');
  const hours = Math.floor(minutes / 60);
  return minutes % 60 ? `${plural(hours, 'hour')} ${plural(minutes % 60, 'minute')}` : plural(hours, 'hour');
}

const trimZero = (s: string) => s.replace(/\.0$/, '');

/** Token count: "512", "1.5k", "38k", "1.2M". */
export function formatTokens(n: number): string {
  if (!Number.isFinite(n) || n <= 0) return '0';
  if (n < 1000) return String(Math.round(n));
  if (n < 10_000) return `${trimZero((n / 1000).toFixed(1))}k`;
  const k = Math.round(n / 1000);
  if (k < 1000) return `${k}k`;
  return `${trimZero((n / 1_000_000).toFixed(1))}M`;
}

/** Token count for screen readers: "38 thousand tokens", "1.2 million tokens". */
export function formatTokensLong(n: number): string {
  if (!Number.isFinite(n) || n <= 0) return '0 tokens';
  if (n < 1000) return plural(Math.round(n), 'token');
  const k = Math.round(n / 1000);
  if (k < 1000) return `${k} thousand tokens`;
  return `${trimZero((n / 1_000_000).toFixed(1))} million tokens`;
}

/** The live cost estimate: "~$1.42" (empty when there is none). */
export function formatCost(usd: number): string {
  if (!Number.isFinite(usd) || usd <= 0) return '';
  if (usd < 0.01) return '<$0.01';
  return `~$${usd.toFixed(2)}`;
}

/** A wall-clock time in 24-hour form: "14:02". */
export function formatClock(iso: string | null | undefined): string {
  if (!iso) return '';
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return '';
  return new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hourCycle: 'h23' });
}

function ms(iso: string | null | undefined): number | null {
  if (!iso) return null;
  const t = Date.parse(iso);
  return Number.isFinite(t) ? t : null;
}

/**
 * How long a record has run: `now − run_started_at` while live,
 * `ended_at − run_started_at` once it ended. Null before it started.
 */
export function agentElapsedMs(view: AgentView, nowMs: number): number | null {
  const start = ms(view.run_started_at) ?? ms(view.started_at);
  if (start === null) return null;
  const end = LIVE_STATUSES.has(view.status) ? nowMs : (ms(view.ended_at) ?? nowMs);
  return Math.max(0, end - start);
}

/** Minutes since the record last did anything (for "Quiet 12m"). */
function quietMinutes(view: AgentView, nowMs: number): number {
  const last = Math.max(ms(view.last_activity_at) ?? 0, ms(view.run_started_at) ?? 0);
  return last ? Math.max(0, Math.floor((nowMs - last) / 60_000)) : 0;
}

/** The first non-empty line of a text. */
function firstLine(text: string | null | undefined): string {
  return (text ?? '').split('\n').map(l => l.trim()).find(Boolean) ?? '';
}

// ------------------------------------------------------------------ //
//  Copy                                                                //
// ------------------------------------------------------------------ //

/**
 * Why and when a record ended: "Stopped by you · 14:02",
 * "Interrupted: gateway restart · 14:10", "Lost track: …". For a completed
 * record its result preview. Never says "done" for a record that did not
 * complete. `selfActorId` tells "by you" from a stop someone else made.
 */
export function endReasonText(view: AgentView, selfActorId?: string | null): string {
  const at = formatClock(view.ended_at ?? view.stop_requested_at);
  const stamp = (text: string) => (at ? `${text} · ${at}` : text);
  switch (view.status) {
    case 'completed':
      return firstLine(view.result_preview) || 'Completed';
    case 'failed':
      return firstLine(view.error) || (view.end_reason === 'spawn_failed' ? 'Failed to start' : 'Failed');
    case 'stopped':
      switch (view.end_reason) {
        case 'user': {
          const byOther = !!view.stopped_by_actor_id && !!selfActorId && view.stopped_by_actor_id !== selfActorId;
          return stamp(byOther ? 'Stopped by someone else' : 'Stopped by you');
        }
        case 'stop_all': return stamp('Stopped (stop all)');
        case 'turn_stop': return 'Stopped with the turn';
        case 'orchestrator': return 'Stopped by the orchestrator';
        default: return 'Stopped';
      }
    case 'interrupted':
      switch (view.end_reason) {
        case 'gateway_restart': return stamp('Interrupted: gateway restart');
        case 'process_ended': return 'Interrupted: session process ended';
        case 'force_stop': return 'Interrupted: force stop';
        case 'model_switch': return 'Interrupted: model switch';
        case 'archived': return 'Interrupted: session archived';
        default: return 'Interrupted';
      }
    case 'lost':
      return 'Lost track: no final status from the runtime';
    default:
      return '';
  }
}

/**
 * The secondary text of a record wherever it is listed:
 * "Starting…", the summary or activity while running, "Quiet 12m · …",
 * "Stopping…" / "Stop not confirmed", "Waiting for your answer", or the end
 * reason.
 */
export function agentDetailText(
  view: AgentView, nowMs: number, quietAfterMin: number, selfActorId?: string | null,
): string {
  const attention = clientAttention(view, nowMs, quietAfterMin);
  if (attention === 'needs_input' && LIVE_STATUSES.has(view.status)) return 'Waiting for your answer';
  switch (view.status) {
    case 'starting':
      return 'Starting…';
    case 'running': {
      const doing = view.summary || view.activity || '';
      if (attention === 'quiet') {
        const quiet = `Quiet ${quietMinutes(view, nowMs)}m`;
        return view.activity ? `${quiet} · ${view.activity}` : quiet;
      }
      return doing || 'Running';
    }
    case 'stopping':
      return attention === 'stop_unconfirmed' ? 'Stop not confirmed' : 'Stopping…';
    default:
      return endReasonText(view, selfActorId);
  }
}

/**
 * The accessible name of a record's row: "build-api, running 4 minutes,
 * 38 thousand tokens, Bash: pytest -q".
 */
export function agentAriaLabel(
  view: AgentView, nowMs: number, quietAfterMin: number, selfActorId?: string | null,
): string {
  const visual = statusVisual(view.status, clientAttention(view, nowMs, quietAfterMin));
  const elapsed = formatElapsedLong(agentElapsedMs(view, nowMs));
  const live = LIVE_STATUSES.has(view.status);
  const state = elapsed ? `${visual.label} ${live ? '' : 'after '}${elapsed}` : visual.label;
  const parts = [view.label, state];
  if (view.tokens > 0) parts.push(formatTokensLong(view.tokens));
  const detail = agentDetailText(view, nowMs, quietAfterMin, selfActorId);
  if (detail && detail.toLowerCase() !== visual.label) parts.push(detail);
  return parts.join(', ');
}

/** The verb of a settled record in announcements: "build-chart finished". */
export function settledVerb(status: AgentStatus): string {
  switch (status) {
    case 'completed': return 'finished';
    case 'failed': return 'failed';
    case 'stopped': return 'stopped';
    case 'interrupted': return 'was interrupted';
    case 'lost': return 'lost track';
    default: return status;
  }
}

/** The composer text of "Follow up…". */
export function followUpText(view: Pick<AgentView, 'label'>): string {
  return `Use SendMessage to tell \`${view.label}\`: `;
}

/** The composer text of "Relaunch…". */
export function relaunchText(view: Pick<AgentView, 'label' | 'id'>): string {
  return `Relaunch the agent \`${view.label}\` (id ${view.id}) with its original brief. Partial work may remain in its working directory.`;
}

/** Statuses whose records offer "Relaunch…". */
export function canRelaunch(view: Pick<AgentView, 'status'>): boolean {
  return view.status === 'failed' || view.status === 'stopped' || view.status === 'interrupted' || view.status === 'lost';
}
