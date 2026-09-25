import { useState } from 'react';
import { Bell, HelpCircle, Loader2, ChevronRight, ChevronDown } from '../../ui/icons';
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

export function NotificationToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const isRunning = block.status === 'running';
  const isNotify = block.tool === 'notify';
  const isAsk = block.tool === 'ask_user';

  const title = String(block.input.title || '');
  const priority = String(block.input.priority || 'normal');
  const optionsRaw = String(block.input.options || '');
  const options = optionsRaw ? optionsRaw.split(',').map(o => o.trim()).filter(Boolean) : [];
  const wait = String(block.input.wait || 'false').toLowerCase() === 'true';
  const body = String(block.input.body || '');

  const Icon = isAsk ? HelpCircle : Bell;
  const iconColor = block.isError ? 'text-error' : isAsk ? 'text-hue-blue' : 'text-hue-amber';
  const label = isNotify ? 'Notify' : 'Ask User';

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
          : <Icon size={14} className={`shrink-0 ${iconColor}`} />
        }
        <span className="text-sm leading-tight font-medium text-text-secondary">{label}</span>
        {title && <span className="text-xs text-text-muted truncate">{title}</span>}
        {priority !== 'normal' && (
          <Badge tone={PRIORITY_TONES[priority] || 'neutral'} className="shrink-0">
            {priority}
          </Badge>
        )}
        {wait && isAsk && (
          <Badge tone="info" className="shrink-0">blocking</Badge>
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

            {/* Options for questions */}
            {isAsk && options.length > 0 && (
              <div className="flex flex-wrap gap-1.5 mt-2">
                {options.map(opt => (
                  <Badge key={opt} tone="info" size="sm" outline>{opt}</Badge>
                ))}
              </div>
            )}
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
