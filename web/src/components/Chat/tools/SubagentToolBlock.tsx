import { useState } from 'react';
import { ChevronRight, ChevronDown, Bot, Search, Lightbulb, Wrench, Loader2, ArrowRight } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';
import { MarkdownContent } from '../MarkdownContent';
import { useChatStore } from '../../../stores/chatStore';
import { extractResultText } from '../../../utils/extractResultText';

const AGENT_ICONS: Record<string, typeof Bot> = {
  Explore: Search,
  Plan: Lightbulb,
  'general-purpose': Wrench,
};

const AGENT_COLORS: Record<string, string> = {
  Explore: 'text-cyan-400',
  Plan: 'text-amber-400',
  'general-purpose': 'text-indigo-400',
};

export function SubagentToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const [showPrompt, setShowPrompt] = useState(false);
  const panels = useChatStore(s => s.panels);
  const isRunning = block.status === 'running';

  const description = String(block.input.description || '');
  const subagentType = String(block.input.subagent_type || block.input.model || 'agent');
  const prompt = String(block.input.prompt || '');
  const model = block.input.model ? String(block.input.model) : null;

  const Icon = AGENT_ICONS[subagentType] || Bot;
  const color = AGENT_COLORS[subagentType] || 'text-text-muted';

  const resultText = block.result ? extractResultText(block.result) : '';
  const displayText = resultText.length > 3000 ? resultText.slice(0, 3000) + '\n\n...(truncated)' : resultText;

  // Check if this sub-agent has a panel tab
  const hasTab = panels.some(p => p.id === block.toolUseId);

  const handleViewInPanel = (e: React.MouseEvent) => {
    e.stopPropagation();
    const store = useChatStore.getState();
    if (hasTab) {
      store.focusPanelTab(block.toolUseId);
    } else {
      // Re-open as a tab (for completed sub-agents from history)
      store.openPanelTab({
        id: block.toolUseId,
        type: subagentType === 'Plan' ? 'plan' : 'subagent',
        label: subagentType,
        subagentType,
        description,
        model: model || undefined,
        content: resultText || null,
        prompt,
        streaming: false,
        status: block.isError ? 'error' : 'complete',
        startedAt: Date.now(),
        completedAt: Date.now(),
        isError: block.isError,
        blocks: [],
      });
    }
  };

  // Brief summary for completed sub-agents (first non-empty line of result)
  const summaryLine = resultText
    ? resultText.split('\n').find(l => l.trim())?.slice(0, 120) || ''
    : '';

  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      {/* Compact card header */}
      <div className="flex items-center gap-2 px-3 py-2">
        {isRunning
          ? <Loader2 size={14} className="text-indigo-400 animate-spin shrink-0" />
          : <Icon size={14} className={`shrink-0 ${block.isError ? 'text-red-400' : color}`} />
        }
        <span className={`text-[13px] font-medium ${color}`}>{subagentType}</span>
        {description && <span className="text-[12px] text-text-muted truncate flex-1">{description}</span>}
        {model && <span className="text-[10px] text-text-faint shrink-0">{model}</span>}

        <div className="ml-auto shrink-0 flex items-center gap-1.5">
          {/* View in panel button */}
          {(isRunning || resultText) && (
            <button
              onClick={handleViewInPanel}
              className="flex items-center gap-1 px-2 py-0.5 text-[11px] text-text-dim hover:text-text-secondary cursor-pointer transition-colors rounded hover:bg-surface-raised"
              title="View in side panel"
            >
              View <ArrowRight size={10} />
            </button>
          )}
          {/* Expand toggle (inline fallback) */}
          <button
            onClick={() => setExpanded(!expanded)}
            className="p-1 text-text-faint hover:text-text-muted cursor-pointer transition-colors"
          >
            {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          </button>
        </div>
      </div>

      {/* Summary line when complete and collapsed */}
      {!expanded && !isRunning && summaryLine && (
        <div className="px-3 pb-2 text-[11px] text-text-faint truncate">
          {summaryLine}{summaryLine.length >= 120 ? '...' : ''}
        </div>
      )}

      {/* Expanded inline view (fallback) */}
      {expanded && (
        <div className="border-t border-border">
          {/* Prompt (collapsible) */}
          {prompt && (
            <div className="border-b border-border-subtle">
              <button
                onClick={() => setShowPrompt(!showPrompt)}
                className="flex items-center gap-1.5 px-3 py-1.5 w-full text-left text-[10px] uppercase tracking-wider text-text-faint hover:text-text-muted cursor-pointer"
              >
                {showPrompt ? <ChevronDown size={10} /> : <ChevronRight size={10} />}
                Prompt
              </button>
              {showPrompt && (
                <pre className="px-3 pb-2 text-[12px] text-text-muted whitespace-pre-wrap max-h-40 overflow-y-auto">
                  {prompt}
                </pre>
              )}
            </div>
          )}

          {/* Result rendered as markdown */}
          {displayText && (
            <div className="px-3 py-2 max-h-96 overflow-y-auto text-[13px]">
              <MarkdownContent content={displayText} />
            </div>
          )}

          {block.isError && resultText && (
            <pre className="px-3 py-2 text-[12px] text-red-400 whitespace-pre-wrap">
              {resultText}
            </pre>
          )}

          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2">
              <Loader2 size={12} className="animate-spin" /> Agent working...
            </div>
          )}
        </div>
      )}
    </div>
  );
}
