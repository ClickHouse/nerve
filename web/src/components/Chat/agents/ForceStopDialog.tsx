import { useMemo } from 'react';
import type { AgentBrief } from '../../../types/agents';
import { useAgentStore } from '../../../stores/agentStore';
import { isLive } from '../../../stores/helpers/agentSelectors';
import { Button, Modal } from '../../ui';
import { formatTokens, kindVisual } from './agentVisuals';

/** Rows listed before "+N more". */
const LISTED = 20;

/**
 * "Force stop?": the turn did not stop, and forcing it restarts the session's
 * runtime, which ends everything still running. Lists what ends.
 */
export function ForceStopDialog() {
  const open = useAgentStore(s => s.stopDialog === 'force');
  const escalation = useAgentStore(s => s.escalation);
  const agents = useAgentStore(s => s.agents);

  // What the server said would end; without a banner's answer, what is live now.
  const ending = useMemo<AgentBrief[]>(() => escalation?.wouldEnd ?? Object.values(agents)
    .filter(a => !a.ambient && isLive(a) && a.kind !== 'shell' && a.kind !== 'monitor')
    .map(a => ({ id: a.id, label: a.label, kind: a.kind, status: a.status, started_at: a.started_at, tokens: a.tokens })),
  [escalation, agents]);
  const shellCount = escalation?.shellCount
    ?? Object.values(agents).filter(a => !a.ambient && isLive(a) && (a.kind === 'shell' || a.kind === 'monitor')).length;

  const close = () => useAgentStore.getState().setStopDialog(null);
  const force = () => { void useAgentStore.getState().forceStop(); };

  return (
    <Modal
      open={open}
      onClose={close}
      title="Force stop?"
      size="md"
      footer={(
        <>
          <Button variant="ghost" size="sm" onClick={close}>Keep waiting</Button>
          <Button variant="dangerSolid" size="sm" onClick={force}>Force stop</Button>
        </>
      )}
    >
      <div className="px-5 py-4 space-y-3">
        <p className="text-sm text-text-secondary">
          The turn didn't stop. Force stop restarts this session's runtime and ends everything still running:
        </p>
        {ending.length > 0 ? (
          <ul className="max-h-64 overflow-y-auto rounded-lg border border-border-subtle divide-y divide-border-subtle" aria-label="What force stop ends">
            {ending.slice(0, LISTED).map(brief => {
              const identity = kindVisual(brief.kind, null);
              return (
                <li key={brief.id} className="flex items-center gap-2 px-3 py-1.5 text-xs tabular-nums">
                  <identity.Icon size={12} className={`shrink-0 ${identity.className}`} aria-hidden="true" />
                  <span className="font-medium truncate text-text-secondary">{brief.label}</span>
                  <span className="text-text-faint shrink-0">{brief.kind}</span>
                  {brief.tokens > 0 && <span className="ml-auto shrink-0 text-text-faint">{formatTokens(brief.tokens)}</span>}
                </li>
              );
            })}
            {ending.length > LISTED && <li className="px-3 py-1.5 text-xs text-text-faint">+{ending.length - LISTED} more</li>}
          </ul>
        ) : (
          <p className="text-xs text-text-muted">No agents are running; only the turn ends.</p>
        )}
        {shellCount > 0 && (
          <p className="text-xs text-text-muted">
            Also {shellCount} background shell{shellCount === 1 ? '' : 's'} or monitor{shellCount === 1 ? '' : 's'}.
          </p>
        )}
      </div>
    </Modal>
  );
}
