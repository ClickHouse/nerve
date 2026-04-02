import { useState } from 'react';
import { Bell, HelpCircle, Loader2, ChevronRight, ChevronDown } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';

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
  const iconColor = block.isError ? 'text-red-400' : isAsk ? 'text-blue-400' : 'text-amber-400';
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
          ? <Loader2 size={14} className="text-[#6366f1] animate-spin shrink-0" />
          : <Icon size={14} className={`shrink-0 ${iconColor}`} />
        }
        <span className="text-[13px] font-medium text-text-secondary">{label}</span>
        {title && <span className="text-[12px] text-text-muted truncate">{title}</span>}
        {priority !== 'normal' && (
          <span className={`text-[10px] px-1.5 py-0.5 rounded shrink-0 ${
            priority === 'urgent' ? 'bg-red-500/15 text-red-400' :
            priority === 'high' ? 'bg-orange-400/15 text-orange-400' :
            'bg-border-subtle text-text-muted'
          }`}>
            {priority}
          </span>
        )}
        {wait && isAsk && (
          <span className="text-[10px] px-1.5 py-0.5 rounded bg-blue-500/15 text-blue-400 shrink-0">blocking</span>
        )}
        {isSent && !isRunning && (
          <span className="text-[10px] text-emerald-400/70 shrink-0">sent</span>
        )}
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-border">
          <div className="px-3 py-2">
            {title && <p className="text-[13px] text-text font-medium">{title}</p>}
            {body && <p className="text-[12px] text-text-muted mt-0.5">{body}</p>}

            {/* Options for questions */}
            {isAsk && options.length > 0 && (
              <div className="flex flex-wrap gap-1.5 mt-2">
                {options.map(opt => (
                  <span key={opt} className="px-2 py-0.5 text-[11px] bg-[#1a1a2e] text-blue-300/80 rounded border border-blue-500/20">
                    {opt}
                  </span>
                ))}
              </div>
            )}
          </div>

          {/* Result */}
          {resultText && (
            <div className="px-3 py-2 border-t border-border-subtle">
              <pre className={`text-[12px] font-mono whitespace-pre-wrap ${block.isError ? 'text-red-400' : 'text-text-muted'}`}>
                {resultText}
              </pre>
            </div>
          )}

          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2 border-t border-border-subtle">
              <Loader2 size={12} className="animate-spin" /> Sending...
            </div>
          )}
        </div>
      )}
    </div>
  );
}
