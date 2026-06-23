import { Workflow as WorkflowIcon, Loader2, Check, X, Ban, ArrowRight } from 'lucide-react';
import type { ToolCallBlockData, WorkflowSnapshot } from '../../../types/chat';
import { useChatStore } from '../../../stores/chatStore';

/** Current phase index (1-based) = the highest phase that has agents, biased
 *  toward any phase with a running agent. Falls back to phases length. */
function deriveCurrentPhase(wf: WorkflowSnapshot): number {
  const running = wf.agents.filter(a => a.state === 'running');
  const pool = running.length > 0 ? running : wf.agents;
  let max = 0;
  for (const a of pool) {
    if (typeof a.phaseIndex === 'number' && a.phaseIndex > max) max = a.phaseIndex;
  }
  return max || (wf.phases.length ? 1 : 0);
}

export function WorkflowToolBlock({ block }: { block: ToolCallBlockData }) {
  const panels = useChatStore(s => s.panels);
  const wf = block.workflow;

  const name = wf?.name || String(block.input?.name || 'Workflow');
  const status = wf?.status || (block.status === 'running' ? 'running' : 'completed');
  const isRunning = status === 'running';
  const isFailed = status === 'failed';

  const agents = wf?.agents || [];
  const total = wf?.agentCount ?? agents.length;
  const done = agents.filter(a => a.state === 'done').length;
  const phaseCount = wf?.phases.length || 0;
  const curPhase = wf ? deriveCurrentPhase(wf) : 0;
  const curPhaseTitle = wf?.phases.find(p => p.index === curPhase)?.title;
  const tokens = wf?.totalTokens || 0;

  const hasTab = panels.some(p => p.id === block.toolUseId);

  const handleViewInPanel = () => {
    const store = useChatStore.getState();
    if (hasTab) {
      store.focusPanelTab(block.toolUseId);
    } else {
      store.openPanelTab({
        id: block.toolUseId,
        type: 'workflow',
        label: name,
        subagentType: 'Workflow',
        description: '',
        content: wf?.summary || null,
        prompt: '',
        streaming: false,
        status: isFailed ? 'error' : 'complete',
        isError: isFailed,
        startedAt: Date.now(),
        completedAt: Date.now(),
        blocks: [],
        workflow: wf,
      });
    }
  };

  const StatusIcon = isRunning ? (
    <Loader2 size={14} className="text-hue-violet animate-spin shrink-0" />
  ) : isFailed ? (
    <X size={14} className="text-hue-red shrink-0" />
  ) : status === 'stopped' ? (
    <Ban size={14} className="text-text-muted shrink-0" />
  ) : (
    <Check size={14} className="text-hue-emerald shrink-0" />
  );

  return (
    <div className="my-1.5 border border-violet-400/20 rounded-lg bg-surface overflow-hidden">
      <div className="flex items-center gap-2 px-3 py-2">
        {StatusIcon}
        <WorkflowIcon size={14} className="text-hue-violet shrink-0" />
        <span className="text-[13px] font-medium text-text-secondary truncate">{name}</span>

        {phaseCount > 0 && (
          <span className="text-[11px] text-text-faint shrink-0">
            phase {curPhase}/{phaseCount}{curPhaseTitle ? ` · ${curPhaseTitle}` : ''}
          </span>
        )}

        <div className="ml-auto shrink-0 flex items-center gap-2">
          {total > 0 && (
            <span className="text-[11px] text-text-faint font-mono" title="agents done / total">
              {done}/{total} agents
            </span>
          )}
          {tokens > 0 && (
            <span className="text-[11px] text-text-faint font-mono" title="total tokens">
              {tokens >= 1000 ? `${Math.round(tokens / 1000)}k` : tokens} tok
            </span>
          )}
          {(isRunning || wf) && (
            <button
              onClick={handleViewInPanel}
              className="flex items-center gap-1 px-2 py-0.5 text-[11px] text-text-dim hover:text-text-secondary cursor-pointer transition-colors rounded hover:bg-surface-raised"
              title="View workflow in side panel"
            >
              View <ArrowRight size={10} />
            </button>
          )}
        </div>
      </div>

      {/* Summary line when settled */}
      {!isRunning && wf?.summary && (
        <div className="px-3 pb-2 text-[11px] text-text-faint truncate">{wf.summary}</div>
      )}
    </div>
  );
}
