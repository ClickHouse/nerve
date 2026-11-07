import { useState } from 'react';
import { Bell, Loader2, ChevronRight, ChevronDown } from '../../ui/icons';
import type { ToolCallBlockData } from '../../../types/chat';
import { Badge, type BadgeTone } from '../../ui';

/** Extract readable text from MCP content blocks. */
function extractText(result: string): string {
  try {
    const parsed = JSON.parse(result);
    if (Array.isArray(parsed)) {
      return parsed
        .filter((b: any) => b.type === 'text')
        .map((b: any) => b.text)
        .join('\n');
    }
  } catch { /* not JSON */ }
  return result;
}

/** Notification severity → chip tone. */
const PRIORITY_TONES: Record<string, BadgeTone> = {
  urgent: 'danger',
  high: 'warning',
};

/** Nerve's `notify` call in the chat. `ask_user` has its own card, `AskUserBlock`. */
export function NotificationToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const isRunning = block.status === 'running';

  const title = String(block.input.title || '');
  const priority = String(block.input.priority || 'normal');
  const body = String(block.input.body || '');
  const iconColor = block.isError ? 'text-error' : 'text-hue-amber';

  const resultText = block.result ? extractText(block.result) : '';
  const isSent = resultText.includes('sent') || resultText.includes('Sent');

  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-surface-raised transition-colors"
      >
        {isRunning
          ? <Loader2 size={14} className="text-accent animate-spin shrink-0" />
          : <Bell size={14} className={`shrink-0 ${iconColor}`} />
        }
        <span className="text-sm leading-tight font-medium text-text-secondary">Notify</span>
        {title && <span className="text-xs text-text-muted truncate">{title}</span>}
        {priority !== 'normal' && (
          <Badge tone={PRIORITY_TONES[priority] || 'neutral'} className="shrink-0">
            {priority}
          </Badge>
        )}
        {isSent && !isRunning && (
          <span className="text-2xs text-success shrink-0">sent</span>
        )}
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-border">
          <div className="px-3 py-2">
            {title && <p className="text-sm text-text font-medium">{title}</p>}
            {body && <p className="text-xs text-text-muted mt-0.5">{body}</p>}
          </div>

          {/* Result */}
          {resultText && (
            <div className="px-3 py-2 border-t border-border-subtle">
              <pre className={`text-xs font-mono whitespace-pre-wrap ${block.isError ? 'text-error' : 'text-text-muted'}`}>
                {resultText}
              </pre>
            </div>
          )}

          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-xs text-text-dim flex items-center gap-2 border-t border-border-subtle">
              <Loader2 size={12} className="animate-spin" /> Sending...
            </div>
          )}
        </div>
      )}
    </div>
  );
}
