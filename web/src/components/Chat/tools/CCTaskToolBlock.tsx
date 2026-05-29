import { ChevronRight, ChevronDown, Plus, Pencil, ListTodo, FileText, OctagonX, Radio, Loader2 } from 'lucide-react';
import { useState } from 'react';
import type { ToolCallBlockData } from '../../../types/chat';
import { extractResultText } from '../../../utils/extractResultText';

/**
 * Compact card for Claude Code 2.1+ task tools (TaskCreate / TaskUpdate /
 * TaskList / TaskGet / TaskStop / TaskOutput). One-line by default; click
 * to expand input + result.
 *
 * The real "what tasks are open" view lives in the TaskPanel below the
 * message stream — these cards just acknowledge the call in the chat.
 */
export function CCTaskToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const isRunning = block.status === 'running';
  const input = block.input || {};
  const resultText = block.result !== undefined ? extractResultText(block.result) : '';

  let Icon = ListTodo;
  let label = block.tool;
  let summary = '';
  let iconColor = 'text-text-muted';

  switch (block.tool) {
    case 'TaskCreate': {
      Icon = Plus;
      label = 'Create task';
      summary = String(input.subject || '');
      iconColor = 'text-hue-blue';
      break;
    }
    case 'TaskUpdate': {
      Icon = Pencil;
      label = 'Update task';
      const id = String(input.taskId || '');
      const status = String(input.status || '');
      summary = id
        ? `#${id}${status ? ' → ' + status : ''}`
        : status;
      iconColor = status === 'completed' ? 'text-hue-green' : 'text-hue-amber';
      break;
    }
    case 'TaskList': {
      Icon = ListTodo;
      label = 'List tasks';
      // Count the rendered "#N [status]" lines; fall back to "" so the
      // summary stays empty rather than showing a misleading "0 tasks"
      // while the call is still running.
      if (resultText) {
        if (/^\s*No tasks found\s*$/i.test(resultText)) {
          summary = 'no tasks';
        } else {
          const count = resultText.split('\n').filter(l => /^\s*#\d+\s+\[/.test(l)).length;
          summary = count > 0 ? `${count} task${count === 1 ? '' : 's'}` : '';
        }
      }
      iconColor = 'text-text-muted';
      break;
    }
    case 'TaskGet': {
      Icon = FileText;
      label = 'Read task';
      const id = String(input.taskId || '');
      summary = id ? `#${id}` : '';
      iconColor = 'text-text-muted';
      break;
    }
    case 'TaskStop': {
      Icon = OctagonX;
      label = 'Stop task';
      summary = String(input.taskId || input.task_id || '');
      iconColor = 'text-hue-red';
      break;
    }
    case 'TaskOutput': {
      Icon = Radio;
      label = 'Task output';
      summary = String(input.taskId || input.task_id || '');
      iconColor = 'text-text-muted';
      break;
    }
  }

  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-surface-raised transition-colors"
      >
        {isRunning
          ? <Loader2 size={14} className="text-accent animate-spin shrink-0" />
          : <Icon size={14} className={`shrink-0 ${block.isError ? 'text-hue-red' : iconColor}`} />
        }
        <span className="text-[13px] font-medium text-text-secondary shrink-0 whitespace-nowrap">{label}</span>
        {summary && <span className="text-[12px] text-text-muted truncate">{summary}</span>}
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-border">
          {/* Input */}
          <div className="px-3 py-2">
            <div className="text-[10px] uppercase tracking-wider text-text-faint mb-1">Input</div>
            <pre className="text-[12px] text-text-muted font-mono whitespace-pre-wrap overflow-x-auto max-h-40 overflow-y-auto bg-bg rounded p-2 border border-border-subtle">
              {JSON.stringify(input, null, 2)}
            </pre>
          </div>

          {/* Result */}
          {resultText && (
            <div className="px-3 py-2 border-t border-border-subtle">
              <div className="text-[10px] uppercase tracking-wider text-text-faint mb-1">
                {block.isError ? 'Error' : 'Result'}
              </div>
              <pre className={`text-[12px] whitespace-pre-wrap overflow-x-auto max-h-60 overflow-y-auto bg-bg rounded p-2 border border-border-subtle ${block.isError ? 'text-hue-red' : 'text-text-muted'}`}>
                {resultText}
              </pre>
            </div>
          )}

          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2 border-t border-border-subtle">
              <Loader2 size={12} className="animate-spin" /> Running...
            </div>
          )}
        </div>
      )}
    </div>
  );
}
