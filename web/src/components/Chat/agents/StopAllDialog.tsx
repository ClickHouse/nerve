import { useMemo, useState } from 'react';
import type { AgentView } from '../../../types/agents';
import { useAgentStore } from '../../../stores/agentStore';
import { isLive } from '../../../stores/helpers/agentSelectors';
import { useNow } from '../../../hooks/useNow';
import { Button, Checkbox, Modal } from '../../ui';
import { agentElapsedMs, formatElapsed, formatTokens, kindVisual, typeLabel } from './agentVisuals';

/** Rows listed before "+N more". */
const LISTED = 20;

const isShellLike = (a: AgentView) => a.kind === 'shell' || a.kind === 'monitor';

/**
 * "Stop all agents?": lists every live agent of the session, with a switch
 * for background shells and monitors, and stops them all (and the turn).
 */
export function StopAllDialog() {
  const open = useAgentStore(s => s.stopDialog === 'all');
  // Mounted per opening, so the shells switch starts on each time.
  return open ? <StopAllDialogOpen /> : null;
}

function StopAllDialogOpen() {
  const agents = useAgentStore(s => s.agents);
  const [includeShells, setIncludeShells] = useState(true);
  const now = useNow(1000);
  const live = useMemo(
    () => Object.values(agents)
      .filter(a => !a.ambient && isLive(a))
      .sort((a, b) => a.created_at.localeCompare(b.created_at)),
    [agents],
  );
  const shells = live.filter(isShellLike).length;
  const agentCount = live.length - shells;
  // What stops, counted apart: shells and monitors are not agents.
  const ending = [
    agentCount > 0 ? (agentCount === 1 ? '1 agent' : `${agentCount} agents`) : null,
    includeShells && shells > 0 ? (shells === 1 ? '1 shell or monitor' : `${shells} shells and monitors`) : null,
  ].filter(Boolean).join(' and ');
  const close = () => useAgentStore.getState().setStopDialog(null);
  const stopAll = () => { void useAgentStore.getState().stopAll({ includeShells }); };

  return (
    <Modal
      open
      onClose={close}
      title="Stop all agents?"
      size="md"
      footer={(
        <>
          <Button variant="ghost" size="sm" onClick={close}>Cancel</Button>
          <Button variant="dangerSolid" size="sm" onClick={stopAll}>Stop all</Button>
        </>
      )}
    >
      <div className="px-5 py-4 space-y-3">
        <p className="text-sm text-text-secondary">
          {live.length === 0
            ? 'No agents are running. The turn stops too, if one is running.'
            : ending
              ? `This stops the turn and ${ending}:`
              : 'This stops the turn; the shells and monitors keep running:'}
        </p>
        {live.length > 0 && (
          <ul className="max-h-64 overflow-y-auto rounded-lg border border-border-subtle divide-y divide-border-subtle" aria-label="Agents that will stop">
            {live.slice(0, LISTED).map(agent => {
              const identity = kindVisual(agent.kind, agent.agent_type);
              const spared = isShellLike(agent) && !includeShells;
              return (
                <li key={agent.id} className={`flex items-center gap-2 px-3 py-1.5 text-xs tabular-nums ${spared ? 'opacity-60' : ''}`}>
                  <identity.Icon size={12} className={`shrink-0 ${identity.className}`} aria-hidden="true" />
                  <span className={`font-medium truncate ${identity.className}`}>{agent.label}</span>
                  <span className="text-text-faint shrink-0">{typeLabel(agent)}</span>
                  {spared && <span className="text-text-faint shrink-0">keeps running</span>}
                  <span className="ml-auto shrink-0 text-text-faint">{formatElapsed(agentElapsedMs(agent, now))}</span>
                  {agent.tokens > 0 && <span className="shrink-0 text-text-faint">{formatTokens(agent.tokens)}</span>}
                </li>
              );
            })}
            {live.length > LISTED && (
              <li className="px-3 py-1.5 text-xs text-text-faint">+{live.length - LISTED} more</li>
            )}
          </ul>
        )}
        {shells > 0 && (
          <Checkbox
            checked={includeShells}
            onChange={e => setIncludeShells(e.target.checked)}
            label={`Also stop background shells and monitors (${shells})`}
          />
        )}
        <p className="text-xs text-text-muted">Stopped agents can't be resumed with SendMessage; relaunch them if needed.</p>
      </div>
    </Modal>
  );
}
