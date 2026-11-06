import type { AutoTurnBlockData } from '../../../types/chat';
import { useAgentStore } from '../../../stores/agentStore';
import { useChatStore } from '../../../stores/chatStore';
import { triggersLabel } from '../../../stores/helpers/agentSelectors';
import { RefreshCw } from '../../ui/icons';
import { settledVerb } from './agentVisuals';

const PILL = 'inline-flex items-center gap-1 px-2 py-0.5 rounded-full border border-border bg-surface';

/**
 * The divider that opens an autonomous turn: which agents settling woke the
 * orchestrator ("Background · build-chart finished"), or "Background
 * continuation" for old history. With one trigger whose record is loaded the
 * pill opens that agent.
 */
export function AutoTurnDivider({ block }: { block: AutoTurnBlockData }) {
  const label = triggersLabel(block.triggers, block.more);
  const only = block.triggers?.length === 1 && !block.more ? block.triggers[0] : null;
  const known = useAgentStore(s => (only ? !!s.agents[only.agent_id] : false));
  const many = (block.triggers?.length ?? 0) > 1;
  const title = many
    ? block.triggers!.map(t => `${t.label} ${settledVerb(t.status)}`).join(' · ')
      + (block.more ? ` · ${block.more} more` : '')
    : undefined;

  return (
    <div className="flex items-center gap-1.5 my-1.5 text-xs text-text-muted">
      <span className="h-px flex-1 bg-border" />
      {only && known ? (
        <button
          type="button"
          onClick={() => useChatStore.getState().openAgentTab(only.agent_id)}
          aria-label={`${label}. Open ${only.label}`}
          className={`${PILL} cursor-pointer transition-colors hover:text-text-secondary hover:bg-surface-hover`}
        >
          <RefreshCw size={11} aria-hidden="true" />
          {label}
        </button>
      ) : (
        <span className={PILL} title={title}>
          <RefreshCw size={11} aria-hidden="true" />
          {label}
        </span>
      )}
      <span className="h-px flex-1 bg-border" />
    </div>
  );
}
