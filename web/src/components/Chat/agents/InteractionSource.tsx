import type { InteractionAgentRef } from '../../../types/agents';
import { useChatStore } from '../../../stores/chatStore';
import { Button } from '../../ui';
import { ArrowUpRight } from '../../ui/icons';

/**
 * "from build-api [Open]" in the header of a question or approval card that a
 * sub-agent raised (the orchestrator's own cards carry no agent). Open shows
 * that agent's tab, where its transcript holds the request.
 */
export function InteractionSource({ agent }: { agent: InteractionAgentRef }) {
  const agentId = agent.agent_id;
  return (
    <span className="flex items-center gap-1 min-w-0 text-xs text-text-muted">
      <span className="truncate">
        from <span className="font-medium text-text-secondary">{agent.label}</span>
      </span>
      {agentId && (
        <Button
          variant="link"
          size="xs"
          aria-label={`Open ${agent.label}`}
          onClick={() => useChatStore.getState().openAgentTab(agentId)}
          className="shrink-0"
        >
          Open <ArrowUpRight size={11} aria-hidden="true" />
        </Button>
      )}
    </span>
  );
}
