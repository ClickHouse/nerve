import { useEffect, useMemo, useState } from 'react';
import type { AgentStatus, AgentView } from '../../../types/agents';
import { LIVE_STATUSES } from '../../../types/agents';
import { useAgentStore } from '../../../stores/agentStore';
import { AGENTS_PANEL_ID, useChatStore } from '../../../stores/chatStore';
import { dockLine, isLive } from '../../../stores/helpers/agentSelectors';
import { useNow } from '../../../hooks/useNow';
import { AlertTriangle, CheckCircle2, ListTree, Loader2 } from '../../ui/icons';
import { settledVerb } from './agentVisuals';

/** At most one announcement per this long. */
const ANNOUNCE_EVERY_MS = 5000;
/** Agents named in one announcement before "and N more". */
const ANNOUNCE_NAMES = 3;

/**
 * One line above the composer while the session has agent records: how many
 * run, how many need attention, how many are done, and what the most recently
 * active one is doing. The whole line opens the Agents panel and puts focus
 * in its list. Absent when the session has no records.
 *
 * A polite live region announces agents that settle ("build-chart finished"),
 * throttled to one message per 5 s.
 */
export function AgentDock({ sessionId }: { sessionId: string }) {
  const storeSession = useAgentStore(s => s.sessionId);
  const agents = useAgentStore(s => s.agents);
  const seen = useAgentStore(s => s.seen);
  const quietAfter = useAgentStore(s => s.capabilities.quiet_after_minutes);
  const panelOpen = useChatStore(s => s.panelVisible && s.activePanelId === AGENTS_PANEL_ID);
  const anyLive = useMemo(() => Object.values(agents).some(a => !a.ambient && isLive(a)), [agents]);
  // Only quiet detection needs the clock here; a slow tick is plenty.
  const now = useNow(15_000, anyLive);
  const line = useMemo(() => dockLine(agents, seen, now, quietAfter), [agents, seen, now, quietAfter]);
  const announcement = useSettledAnnouncements();

  const visible = storeSession === sessionId && line.visible;
  return (
    <>
      {visible && (
        <div className="mx-4 mt-2 shrink-0">
          <button
            type="button"
            aria-expanded={panelOpen}
            aria-controls="agents-panel"
            onClick={() => useChatStore.getState().openAgentsPanel(true)}
            className="w-full flex items-center gap-3 h-8 px-3 rounded-lg border border-border-subtle bg-surface text-xs text-text-muted tabular-nums text-left cursor-pointer transition-colors hover:bg-surface-hover hover:text-text-secondary"
          >
            {line.running > 0 && (
              <span className="flex items-center gap-1 shrink-0">
                <Loader2 size={12} className="motion-safe:animate-spin text-accent" aria-hidden="true" />
                {line.running} running
              </span>
            )}
            {line.attention > 0 && (
              <span className="flex items-center gap-1 shrink-0 text-warning">
                <AlertTriangle size={12} aria-hidden="true" />
                {line.attention}
                <span className="sr-only"> need{line.attention === 1 ? 's' : ''} attention</span>
              </span>
            )}
            {line.done > 0 && (
              <span className="flex items-center gap-1 shrink-0">
                <CheckCircle2 size={12} className="text-success" aria-hidden="true" />
                {line.done}
                <span className="sr-only"> done</span>
              </span>
            )}
            {line.running === 0 && line.attention === 0 && line.done === 0 && line.ended > 0 && (
              <span className="shrink-0">{line.ended} ended</span>
            )}
            {line.headline && (
              <span className="truncate min-w-0 text-text-secondary">{line.headline.text}</span>
            )}
            <span className="ml-auto flex items-center gap-1 shrink-0">
              <ListTree size={14} aria-hidden="true" />
              Agents
            </span>
          </button>
        </div>
      )}
      <div aria-live="polite" role="status" className="sr-only">{announcement}</div>
    </>
  );
}

function liveStatuses(agents: Record<string, AgentView>): Map<string, AgentStatus> {
  const live = new Map<string, AgentStatus>();
  for (const a of Object.values(agents)) if (!a.ambient && LIVE_STATUSES.has(a.status)) live.set(a.id, a.status);
  return live;
}

/**
 * Announcements of agents that went from live to settled, at most one per
 * ANNOUNCE_EVERY_MS (later ones are folded into the next message). Records
 * that first appear settled — a reload, a switch — announce nothing.
 */
function useSettledAnnouncements(): string {
  const [message, setMessage] = useState('');
  useEffect(() => {
    let previous = liveStatuses(useAgentStore.getState().agents);
    let previousSession = useAgentStore.getState().sessionId;
    let pending: string[] = [];
    let lastAt = 0;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const flush = () => {
      timer = null;
      if (pending.length === 0) return;
      const names = pending.slice(0, ANNOUNCE_NAMES);
      const more = pending.length - names.length;
      setMessage(more > 0 ? `${names.join(', ')}, and ${more} more agents settled` : names.join(', '));
      pending = [];
      lastAt = Date.now();
    };

    const unsubscribe = useAgentStore.subscribe((state) => {
      if (state.sessionId !== previousSession) {
        previousSession = state.sessionId;
        previous = liveStatuses(state.agents);
        pending = [];
        return;
      }
      for (const [id] of previous) {
        const now = state.agents[id];
        if (now && !LIVE_STATUSES.has(now.status)) pending.push(`${now.label} ${settledVerb(now.status)}`);
      }
      previous = liveStatuses(state.agents);
      if (pending.length > 0 && timer === null) {
        timer = setTimeout(flush, Math.max(0, lastAt + ANNOUNCE_EVERY_MS - Date.now()));
      }
    });
    return () => {
      unsubscribe();
      if (timer !== null) clearTimeout(timer);
    };
  }, []);
  return message;
}
