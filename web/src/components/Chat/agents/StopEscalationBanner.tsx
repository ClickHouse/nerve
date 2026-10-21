import { useEffect, useRef } from 'react';
import { useAgentStore } from '../../../stores/agentStore';
import { useChatStore } from '../../../stores/chatStore';
import { Button } from '../../ui';
import { AlertTriangle } from '../../ui/icons';

/**
 * What a Stop could not do on its own, above the composer:
 * - the turn did not stop within 5 s while agents run: offer Force stop…;
 * - the runtime cannot spare background agents: confirm stopping them too.
 * Both are about a running turn: once it ends, the banner goes.
 * Otherwise, a stop request that failed says so here until dismissed.
 */
export function StopEscalationBanner() {
  const escalation = useAgentStore(s => s.escalation);
  const stopError = useAgentStore(s => s.stopError);
  const stopping = useAgentStore(s => s.stopInFlight);
  const streaming = useChatStore(s => s.isStreaming);
  const wasStreaming = useRef(streaming);

  useEffect(() => {
    if (wasStreaming.current && !streaming && useAgentStore.getState().escalation) {
      useAgentStore.getState().dismissEscalation();
    }
    wasStreaming.current = streaming;
  }, [streaming]);

  if (!escalation || !streaming) {
    if (!stopError) return null;
    return (
      <div
        role="alert"
        className="mx-4 mt-2 flex flex-wrap items-center gap-2 px-3 py-2 rounded-lg border border-warning-border bg-warning-bg text-warning text-xs shrink-0"
      >
        <AlertTriangle size={14} className="shrink-0" aria-hidden="true" />
        <span className="flex-1 min-w-[12rem]">{stopError}</span>
        <Button variant="ghost" size="xs" onClick={() => useAgentStore.getState().dismissStopError()}>Dismiss</Button>
      </div>
    );
  }

  const n = escalation.wouldEnd.length;
  const shells = escalation.shellCount > 0
    ? ` and ${escalation.shellCount} background shell${escalation.shellCount === 1 ? '' : 's'}`
    : '';
  const store = useAgentStore.getState;

  return (
    <div
      role="alert"
      className="mx-4 mt-2 flex flex-wrap items-center gap-2 px-3 py-2 rounded-lg border border-warning-border bg-warning-bg text-warning text-xs shrink-0"
    >
      <AlertTriangle size={14} className="shrink-0" aria-hidden="true" />
      {escalation.kind === 'not_stopped' ? (
        <>
          <span className="flex-1 min-w-[12rem]">
            The turn didn't stop after 5 s. {n === 1 ? '1 agent is' : `${n} agents are`} still running{shells}.
          </span>
          <Button variant="warning" size="xs" onClick={() => store().setStopDialog('force')}>Force stop…</Button>
          <Button variant="ghost" size="xs" onClick={() => store().dismissEscalation()}>Dismiss</Button>
        </>
      ) : (
        <>
          <span className="flex-1 min-w-[12rem]">
            This runtime can't spare background agents: stopping the turn also stops {n} of them{shells}.
          </span>
          <Button
            variant="dangerSolid"
            size="xs"
            disabled={stopping}
            onClick={() => { void store().stopTurn({ confirmKillBackground: true }); }}
          >
            Stop turn and agents
          </Button>
          <Button variant="ghost" size="xs" onClick={() => store().dismissEscalation()}>Cancel</Button>
        </>
      )}
    </div>
  );
}
