import { useState } from 'react';
import { ChevronRight, ChevronDown, Bot, Search, Lightbulb, Wrench, Loader2, ArrowRight } from '../../ui/icons';
import type { ToolCallBlockData } from '../../../types/chat';
import { MarkdownContent } from '../MarkdownContent';
import { useChatStore } from '../../../stores/chatStore';
import { extractResultText } from '../../../utils/extractResultText';
import { Button, IconButton } from '../../ui';

const AGENT_ICONS: Record<string, typeof Bot> = {
  Explore: Search,
  Plan: Lightbulb,
  'general-purpose': Wrench,
};

/** Agent identity, not status — which sub-agent ran, at a glance. */
const AGENT_COLORS: Record<string, string> = {
  Explore: 'text-hue-cyan',
  Plan: 'text-hue-amber',
  'general-purpose': 'text-link',
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
          ? <Loader2 size={14} className="text-accent animate-spin shrink-0" />
          : <Icon size={14} className={`shrink-0 ${block.isError ? 'text-error' : color}`} />
        }
        <span className={`text-sm leading-tight font-medium ${color}`}>{subagentType}</span>
        {description && <span className="text-xs text-text-muted truncate flex-1">{description}</span>}
        {model && <span className="text-2xs text-text-faint shrink-0">{model}</span>}

        <div className="ml-auto shrink-0 flex items-center gap-1.5">
          {/* View in panel button */}
          {(isRunning || resultText) && (
            <Button
              variant="subtle"
              size="xs"
              onClick={handleViewInPanel}
              title="View in side panel"
            >
              View <ArrowRight size={10} />
            </Button>
          )}
          {/* Expand toggle (inline fallback) */}
          <IconButton
            size="xs"
            label={expanded ? 'Collapse result' : 'Expand result'}
            onClick={() => setExpanded(!expanded)}
          >
            {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          </IconButton>
        </div>
      </div>

      {/* Summary line when complete and collapsed */}
      {!expanded && !isRunning && summaryLine && (
        <div className="px-3 pb-2 text-xs text-text-faint truncate">
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
                className="flex items-center gap-1.5 px-3 py-1.5 w-full text-left text-2xs uppercase tracking-wider text-text-faint hover:text-text-muted cursor-pointer"
              >
                {showPrompt ? <ChevronDown size={10} /> : <ChevronRight size={10} />}
                Prompt
              </button>
              {showPrompt && (
                <pre className="px-3 pb-2 text-xs text-text-muted whitespace-pre-wrap max-h-40 overflow-y-auto">
                  {prompt}
                </pre>
              )}
            </div>
          )}

          {/* Result rendered as markdown */}
          {displayText && (
            <div className="px-3 py-2 max-h-96 overflow-y-auto text-sm">
              <MarkdownContent content={displayText} />
            </div>
          )}

          {block.isError && resultText && (
            <pre className="px-3 py-2 text-xs text-error whitespace-pre-wrap">
              {resultText}
            </pre>
          )}

          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-xs text-text-dim flex items-center gap-2">
              <Loader2 size={12} className="animate-spin" /> Agent working...
            </div>
          )}
        </div>
      )}
    </div>
  );
}
