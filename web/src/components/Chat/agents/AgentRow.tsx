import { memo } from 'react';
import type { AgentView } from '../../../types/agents';
import { useAgentStore } from '../../../stores/agentStore';
import { useChatStore } from '../../../stores/chatStore';
import { clientAttention, isLive } from '../../../stores/helpers/agentSelectors';
import { Badge, IconButton } from '../../ui';
import { ArrowUpRight, RotateCcw, RotateCw } from '../../ui/icons';
import {
  agentAriaLabel, agentDetailText, agentElapsedMs, canRelaunch, formatCost, formatElapsed, formatTokens,
  kindVisual, relaunchText, statusVisual, typeLabel,
} from './agentVisuals';
import { prefillComposer } from './agentActions';
import { TwoStepStopButton } from './TwoStepStopButton';
import { useTwoStepConfirm } from './useTwoStepConfirm';

interface AgentRowProps {
  agent: AgentView;
  /** Tree indent (0 for a root). */
  depth: number;
  /** A row of the Running tree: it draws the └ child marker. The other
   * groups are flat lists, where the marker would read as a child of the
   * row above. */
  tree?: boolean;
  /** The list's roving tab stop: this row (and its actions) is tabbable. */
  focused: boolean;
  /** "Now" for elapsed and quiet; only read for live agents. */
  nowMs: number;
  quietAfterMin: number;
  selfActorId: string | null;
  /** The keyboard stop is armed on this row ("Press x again…"). */
  keyArmed: boolean;
  /** The row or one of its actions received focus. */
  onFocusRow: (agentId: string) => void;
}

/**
 * One agent in the Agents panel: status, identity, label, type, what it is
 * doing (or how it ended), elapsed, tokens, cost, and its actions — Open,
 * a two-step Stop, Retry for an unconfirmed stop, Relaunch… once it ended
 * without completing. Every action is visible without hovering.
 */
function AgentRowImpl({ agent, depth, tree = false, focused, nowMs, quietAfterMin, selfActorId, keyArmed, onFocusRow }: AgentRowProps) {
  const stop = useTwoStepConfirm(3000);
  const live = isLive(agent);
  const attention = clientAttention(agent, nowMs, quietAfterMin);
  const status = statusVisual(agent.status, attention);
  const identity = kindVisual(agent.kind, agent.agent_type);
  const detail = agentDetailText(agent, nowMs, quietAfterMin, selfActorId);
  const elapsed = formatElapsed(agentElapsedMs(agent, nowMs));
  const cost = formatCost(agent.cost_usd);
  const canStop = live && agent.controls.stop && agent.status !== 'stopping';
  const canRetry = agent.status === 'stopping' && agent.attention === 'stop_unconfirmed' && agent.controls.stop;
  const tab = focused ? 0 : -1;
  const armed = keyArmed || stop.armed;

  const open = () => useChatStore.getState().openAgentTab(agent.id);
  const stopAgent = () => { void useAgentStore.getState().stopAgent(agent.id); };

  return (
    <li
      data-agent-id={agent.id}
      className="flex items-center gap-1 pr-2 text-xs tabular-nums transition-colors hover:bg-surface-hover focus-within:bg-accent/10 [content-visibility:auto] [contain-intrinsic-size:auto_30px]"
    >
      <button
        type="button"
        data-agent-row={agent.id}
        tabIndex={tab}
        aria-label={agentAriaLabel(agent, nowMs, quietAfterMin, selfActorId)}
        onClick={open}
        onFocus={() => onFocusRow(agent.id)}
        className="flex-1 min-w-0 flex items-center gap-2 py-1.5 text-left cursor-pointer outline-none rounded-sm focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-accent/60"
        style={{ paddingLeft: 12 + 16 * Math.min(depth, 3) }}
      >
        {tree && (depth > 0 || agent.depth > 1) && (
          <span className="text-text-faint shrink-0" aria-hidden="true">└</span>
        )}
        <status.Icon size={13} className={`shrink-0 ${status.className}`} aria-hidden="true" />
        <identity.Icon size={12} className={`shrink-0 ${identity.className}`} aria-hidden="true" />
        <span className={`font-medium truncate max-w-[45%] ${identity.className}`}>{agent.label}</span>
        <Badge tone="neutral" size="xs" className="shrink-0">{typeLabel(agent)}</Badge>
        <span className={`truncate flex-1 min-w-0 ${armed ? 'text-error' : 'text-text-muted'}`}>
          {keyArmed ? `Press x again to stop ${agent.label}` : stop.armed ? `Click again to stop ${agent.label}` : detail}
        </span>
        {elapsed && <span className="shrink-0 text-text-faint">{elapsed}</span>}
        {agent.tokens > 0 && <span className="shrink-0 text-text-faint hidden @xs:inline">{formatTokens(agent.tokens)}</span>}
        {cost && <span className="shrink-0 text-text-faint hidden @md:inline">{cost}</span>}
      </button>
      <div className="flex items-center gap-0.5 shrink-0">
        <IconButton size="xs" label={`Open ${agent.label}`} tabIndex={tab} onClick={open} onFocus={() => onFocusRow(agent.id)}>
          <ArrowUpRight size={13} />
        </IconButton>
        {canStop && (
          <TwoStepStopButton
            label={agent.label}
            armed={stop.armed}
            onPress={() => stop.press(stopAgent)}
            onDisarm={stop.disarm}
            tabIndex={tab}
            onFocus={() => onFocusRow(agent.id)}
          />
        )}
        {canRetry && (
          <IconButton size="xs" label={`Retry stopping ${agent.label}`} tabIndex={tab} onClick={stopAgent} onFocus={() => onFocusRow(agent.id)}>
            <RotateCw size={12} />
          </IconButton>
        )}
        {canRelaunch(agent) && (
          <IconButton
            size="xs"
            label={`Relaunch ${agent.label}…`}
            tabIndex={tab}
            onClick={() => prefillComposer(relaunchText(agent))}
            onFocus={() => onFocusRow(agent.id)}
          >
            <RotateCcw size={12} />
          </IconButton>
        )}
      </div>
    </li>
  );
}

/**
 * Rows re-render when their record changes (a new `rev`), their place in the
 * list does, or — for live agents only — the clock ticks. A finished row stays
 * put through every tick of a 50-agent wave.
 */
export const AgentRow = memo(AgentRowImpl, (prev, next) =>
  prev.agent === next.agent
  && prev.agent.rev === next.agent.rev
  && prev.depth === next.depth
  && prev.tree === next.tree
  && prev.focused === next.focused
  && prev.keyArmed === next.keyArmed
  && prev.quietAfterMin === next.quietAfterMin
  && prev.selfActorId === next.selfActorId
  && prev.onFocusRow === next.onFocusRow
  && (prev.nowMs === next.nowMs || !isLive(next.agent)));
