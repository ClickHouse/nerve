import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent, type ReactNode } from 'react';
import type { AgentView } from '../../../types/agents';
import { useAgentStore } from '../../../stores/agentStore';
import { useAuthStore } from '../../../stores/authStore';
import { useChatStore } from '../../../stores/chatStore';
import { groupAgents, isLive, type AgentTreeRow } from '../../../stores/helpers/agentSelectors';
import { useNow } from '../../../hooks/useNow';
import { Button, IconButton } from '../../ui';
import { CheckCheck, ChevronDown, ChevronRight, ListTree, X } from '../../ui/icons';
import { formatCost } from './agentVisuals';
import { focusComposer } from './agentActions';
import { AgentRow } from './AgentRow';

/** Done rows shown before "Show all N". */
const DONE_PREVIEW = 10;
/** The keyboard stop (`x`, then `x` again) stays armed this long. */
const KEY_ARM_MS = 2000;

/** The last focus request (agentStore.focusNonce) a panel acted on; it outlives remounts. */
let handledFocusNonce = 0;

/**
 * The Agents roster: every agent record of the session, grouped Needs
 * attention → Running (as a tree) → Done → Ended.
 *
 * Keyboard: one tab stop for the whole list (a roving
 * tabindex over the rows), ↑ ↓ Home End move, Enter opens, `x` arms and a
 * second `x` stops, Escape goes back to the composer — and never reaches the
 * global Escape that stops the turn. A plain list of buttons: no tree or grid
 * roles.
 */
export function AgentsPanel() {
  const agents = useAgentStore(s => s.agents);
  const seen = useAgentStore(s => s.seen);
  const summary = useAgentStore(s => s.summary);
  const quietAfter = useAgentStore(s => s.capabilities.quiet_after_minutes);
  const focusNonce = useAgentStore(s => s.focusNonce);
  const self = useAuthStore(s => s.viewer?.id ?? null);
  const anyLive = useMemo(() => Object.values(agents).some(a => !a.ambient && isLive(a)), [agents]);
  const now = useNow(1000, anyLive);
  const groups = useMemo(() => groupAgents(agents, seen, now, quietAfter), [agents, seen, now, quietAfter]);

  const [focusedId, setFocusedId] = useState<string | null>(null);
  const [showAllDone, setShowAllDone] = useState(false);
  const [endedOpen, setEndedOpen] = useState(false);
  const [keyArmedId, setKeyArmedId] = useState<string | null>(null);
  const [announcement, setAnnouncement] = useState('');
  const rootRef = useRef<HTMLElement>(null);
  const listRef = useRef<HTMLUListElement>(null);
  const armTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  /** Focus is on a row (or its actions) and has not been taken elsewhere. */
  const rowFocused = useRef(false);

  const doneShown = showAllDone ? groups.done : groups.done.slice(0, DONE_PREVIEW);
  const endedShown = endedOpen ? groups.ended : [];
  const visibleIds = [
    ...groups.attention.map(a => a.id),
    ...groups.running.map(r => r.agent.id),
    ...doneShown.map(a => a.id),
    ...endedShown.map(a => a.id),
  ];
  // The roving tab stop: the row last focused while it is still listed, else the first.
  const activeId = focusedId && visibleIds.includes(focusedId) ? focusedId : visibleIds[0] ?? null;
  const empty = groups.attention.length + groups.running.length + groups.done.length + groups.ended.length === 0;

  const disarmKey = useCallback(() => {
    if (armTimer.current !== null) clearTimeout(armTimer.current);
    armTimer.current = null;
    setKeyArmedId(null);
  }, []);

  useEffect(() => () => {
    if (armTimer.current !== null) clearTimeout(armTimer.current);
  }, []);

  const onFocusRow = useCallback((agentId: string) => {
    setFocusedId(agentId);
    setKeyArmedId(prev => (prev && prev !== agentId ? null : prev));
  }, []);

  const focusRow = (agentId: string | undefined) => {
    if (!agentId) return;
    const button = [...(listRef.current?.querySelectorAll<HTMLElement>('[data-agent-row]') ?? [])]
      .find(el => el.dataset.agentRow === agentId);
    button?.focus();
  };

  // A row that changes group (running → done) is a new element, and the
  // browser drops the focus it had: give it back to the same agent's row.
  useEffect(() => {
    if (!rowFocused.current || !activeId) return;
    const current = document.activeElement;
    if (current && current !== document.body) return;
    focusRow(activeId);
  });

  // Focus leaves the list on purpose by a press elsewhere, or by Tab / Escape.
  useEffect(() => {
    const onDown = (e: PointerEvent) => {
      if (!listRef.current?.contains(e.target as Node)) rowFocused.current = false;
    };
    document.addEventListener('pointerdown', onDown, true);
    return () => document.removeEventListener('pointerdown', onDown, true);
  }, []);

  // ⌘⇧E, ↓ in an empty composer and the dock ask for the list's focus.
  useEffect(() => {
    if (focusNonce === handledFocusNonce) return;
    handledFocusNonce = focusNonce;
    const target = listRef.current?.querySelector<HTMLElement>('[data-agent-row][tabindex="0"]');
    (target ?? rootRef.current)?.focus();
  }, [focusNonce]);

  const armOrStop = (agent: AgentView) => {
    if (!isLive(agent) || !agent.controls.stop || agent.status === 'stopping') {
      setAnnouncement(isLive(agent) ? `${agent.label} is already stopping` : `${agent.label} is not running`);
      return;
    }
    if (keyArmedId === agent.id) {
      disarmKey();
      setAnnouncement(`Stopping ${agent.label}`);
      void useAgentStore.getState().stopAgent(agent.id);
      return;
    }
    if (armTimer.current !== null) clearTimeout(armTimer.current);
    setKeyArmedId(agent.id);
    setAnnouncement(`Press x again to stop ${agent.label}`);
    armTimer.current = setTimeout(() => {
      armTimer.current = null;
      setKeyArmedId(null);
    }, KEY_ARM_MS);
  };

  const onKeyDown = (e: KeyboardEvent<HTMLElement>) => {
    const rowId = (e.target as HTMLElement).closest<HTMLElement>('[data-agent-id]')?.dataset.agentId ?? null;
    const at = rowId ? visibleIds.indexOf(rowId) : -1;
    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        focusRow(visibleIds[at < 0 ? 0 : Math.min(at + 1, visibleIds.length - 1)]);
        return;
      case 'ArrowUp':
        e.preventDefault();
        focusRow(visibleIds[at < 0 ? 0 : Math.max(at - 1, 0)]);
        return;
      case 'Home':
        e.preventDefault();
        focusRow(visibleIds[0]);
        return;
      case 'End':
        e.preventDefault();
        focusRow(visibleIds[visibleIds.length - 1]);
        return;
      case 'Escape':
        // Back to the composer. Stopping here keeps the global Escape (stop
        // the turn) from ever firing out of the list.
        e.preventDefault();
        e.stopPropagation();
        disarmKey();
        focusComposer();
        return;
      case 'x':
      case 'X': {
        if (e.metaKey || e.ctrlKey || e.altKey || !rowId) return;
        const agent = agents[rowId];
        if (!agent) return;
        e.preventDefault();
        armOrStop(agent);
        return;
      }
    }
  };

  const markAllSeen = () => {
    const ids = groups.attention.filter(a => !isLive(a)).map(a => a.id);
    useAgentStore.getState().markSeen(ids);
    setAnnouncement(ids.length === 1 ? 'Moved 1 agent to Ended' : `Moved ${ids.length} agents to Ended`);
  };

  const row = (agent: AgentView, depth = 0, tree = false) => (
    <AgentRow
      key={agent.id}
      agent={agent}
      depth={depth}
      tree={tree}
      focused={agent.id === activeId}
      nowMs={now}
      quietAfterMin={quietAfter}
      selfActorId={self}
      keyArmed={keyArmedId === agent.id}
      onFocusRow={onFocusRow}
    />
  );

  const headline = [
    groups.running.length > 0 ? `${groups.running.length} running` : null,
    groups.attention.length > 0
      ? `${groups.attention.length} need${groups.attention.length === 1 ? 's' : ''} attention`
      : null,
    formatCost(summary.cost_usd) || null,
  ].filter(Boolean).join(' · ');
  const failuresToClear = groups.attention.some(a => !isLive(a));

  return (
    <section
      id="agents-panel"
      ref={rootRef}
      tabIndex={-1}
      aria-label="Agents panel"
      onKeyDown={onKeyDown}
      className="flex-1 flex flex-col min-h-0 bg-bg-sunken outline-none"
    >
      <header className="flex items-center gap-2 px-4 py-2.5 border-b border-border-subtle bg-bg shrink-0 min-w-0">
        <ListTree size={14} className="text-text-muted shrink-0" aria-hidden="true" />
        <h2 className="text-2xs uppercase tracking-wider text-text-faint font-medium shrink-0">Agents</h2>
        {headline && <span className="text-xs text-text-muted tabular-nums truncate">{headline}</span>}
        <div className="ml-auto flex items-center gap-1 shrink-0">
          {summary.live > 0 && (
            <Button variant="dangerGhost" size="xs" onClick={() => useAgentStore.getState().setStopDialog('all')}>
              Stop all…
            </Button>
          )}
          <IconButton label="Close the panel" size="xs" onClick={() => useChatStore.getState().togglePanel()}>
            <X size={14} />
          </IconButton>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto @container">
        {empty ? (
          <div className="h-full flex flex-col items-center justify-center gap-1.5 px-6 text-center">
            <ListTree size={20} className="text-text-faint" aria-hidden="true" />
            <p className="text-sm text-text-muted">No agents in this session yet.</p>
            <p className="text-xs text-text-faint">Agents the orchestrator launches show up here, with their progress.</p>
          </div>
        ) : (
          <ul
            ref={listRef}
            aria-label="Agents"
            className="pb-3"
            onFocus={() => { rowFocused.current = true; }}
            onBlur={(e) => {
              const next = e.relatedTarget;
              if (next instanceof Node && !e.currentTarget.contains(next)) rowFocused.current = false;
            }}
          >
            {groups.attention.length > 0 && (
              <Group
                title="Needs attention"
                count={groups.attention.length}
                action={failuresToClear && (
                  <Button variant="ghost" size="xs" onClick={markAllSeen}>
                    <CheckCheck size={12} /> Mark all seen
                  </Button>
                )}
              >
                {groups.attention.map(a => row(a))}
              </Group>
            )}
            {groups.running.length > 0 && (
              <Group title="Running" count={groups.running.length}>
                {groups.running.map((r: AgentTreeRow) => row(r.agent, r.depth, true))}
              </Group>
            )}
            {groups.done.length > 0 && (
              <Group
                title="Done"
                count={groups.done.length}
                action={groups.done.length > DONE_PREVIEW && (
                  <Button
                    variant="ghost"
                    size="xs"
                    aria-expanded={showAllDone}
                    onClick={() => setShowAllDone(v => !v)}
                  >
                    {showAllDone ? 'Show fewer' : `Show all ${groups.done.length}`}
                  </Button>
                )}
              >
                {doneShown.map(a => row(a))}
              </Group>
            )}
            {groups.ended.length > 0 && (
              <Group
                title="Ended"
                count={groups.ended.length}
                collapsed={!endedOpen}
                onToggle={() => setEndedOpen(v => !v)}
              >
                {endedShown.map(a => row(a))}
              </Group>
            )}
          </ul>
        )}
      </div>
      <div aria-live="polite" className="sr-only">{announcement}</div>
    </section>
  );
}

const GROUP_LABEL = 'text-2xs uppercase tracking-wider text-text-faint';

/** One group of the roster: its label (a disclosure when collapsible) and its rows. */
function Group({ title, count, action, collapsed, onToggle, children }: {
  title: string;
  count: number;
  action?: ReactNode;
  /** Set (with onToggle) for a group that folds away. */
  collapsed?: boolean;
  onToggle?: () => void;
  children: ReactNode;
}) {
  const label = `${title} (${count})`;
  return (
    <li className="list-none">
      <div className="flex items-center gap-2 px-3 pt-3 pb-1 min-h-7">
        {onToggle ? (
          <button
            type="button"
            aria-expanded={!collapsed}
            onClick={onToggle}
            className={`${GROUP_LABEL} inline-flex items-center gap-1 cursor-pointer hover:text-text-muted`}
          >
            {collapsed ? <ChevronRight size={11} aria-hidden="true" /> : <ChevronDown size={11} aria-hidden="true" />}
            {label}
          </button>
        ) : (
          <h3 className={GROUP_LABEL}>{label}</h3>
        )}
        {action && <div className="ml-auto">{action}</div>}
      </div>
      {!collapsed && <ul aria-label={title}>{children}</ul>}
    </li>
  );
}
