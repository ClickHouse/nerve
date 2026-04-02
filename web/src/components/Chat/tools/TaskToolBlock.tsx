import { useState } from 'react';
import { ChevronRight, ChevronDown, CheckSquare, ListTodo, Plus, CheckCircle, Pencil, FileText, Loader2 } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';

const STATUS_COLORS: Record<string, string> = {
  pending: 'bg-yellow-500/15 text-yellow-400',
  'in-progress': 'bg-blue-500/15 text-blue-400',
  'in_progress': 'bg-blue-500/15 text-blue-400',
  done: 'bg-green-500/15 text-green-400',
  completed: 'bg-green-500/15 text-green-400',
  deferred: 'bg-border-subtle text-text-muted',
};

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

interface ParsedTask {
  title: string;
  status: string;
  id?: string;
  deadline?: string;
}

/** Parse task list from result text. */
function parseTaskList(text: string): ParsedTask[] {
  // Try JSON first
  try {
    const parsed = JSON.parse(text);
    const tasks = Array.isArray(parsed) ? parsed : parsed?.tasks;
    if (Array.isArray(tasks)) {
      return tasks.map((t: any) => ({
        title: t.title || t.id || '?',
        status: t.status || 'pending',
        id: t.id,
        deadline: t.deadline,
      }));
    }
  } catch { /* not JSON */ }

  // Try parsing text format: "- [status] title" or "**title** (status)"
  const items: ParsedTask[] = [];
  for (const line of text.split('\n')) {
    const match = line.match(/^[-•]\s*(?:\[(\w+)\]\s*)?(.+)/);
    if (match) {
      items.push({ title: match[2].trim(), status: match[1] || 'pending' });
    }
  }
  return items;
}

export function TaskToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const isRunning = block.status === 'running';

  const toolName = block.tool.split('__').pop() || block.tool;
  const isCreate = toolName.includes('create');
  const isList = toolName.includes('list');
  const isDone = toolName.includes('done');
  const isUpdate = toolName.includes('update');
  const isRead = toolName === 'task_read';

  let label: string;
  let Icon = CheckSquare;
  if (isCreate) { label = 'Create Task'; Icon = Plus; }
  else if (isList) { label = 'List Tasks'; Icon = ListTodo; }
  else if (isDone) { label = 'Complete Task'; Icon = CheckCircle; }
  else if (isUpdate) { label = 'Update Task'; Icon = Pencil; }
  else if (isRead) { label = 'Read Task'; Icon = FileText; }
  else { label = 'Task'; }

  const title = String(block.input.title || block.input.task_id || '');
  const status = String(block.input.status || (isDone ? 'done' : ''));

  const resultText = block.result ? extractText(block.result) : '';
  const taskList = isList ? parseTaskList(resultText) : [];

  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-surface-raised transition-colors"
      >
        {isRunning
          ? <Loader2 size={14} className="text-[#6366f1] animate-spin shrink-0" />
          : <Icon size={14} className={`shrink-0 ${block.isError ? 'text-red-400' : isDone ? 'text-green-400' : isCreate ? 'text-blue-400' : 'text-text-muted'}`} />
        }
        <span className="text-[13px] font-medium text-text-secondary shrink-0 whitespace-nowrap">{label}</span>
        {title && <span className="text-[12px] text-text-muted truncate">{title}</span>}
        {status && (
          <span className={`text-[10px] px-1.5 py-0.5 rounded ${STATUS_COLORS[status] || 'bg-border-subtle text-text-muted'}`}>
            {status}
          </span>
        )}
        {isList && taskList.length > 0 && (
          <span className="text-[10px] text-text-faint shrink-0">{taskList.length} tasks</span>
        )}
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-border">
          {/* Create: show what was created */}
          {isCreate && title && (
            <div className="px-3 py-2">
              <div className="text-[12px] text-text-secondary flex items-center gap-2">
                <Plus size={11} className="text-blue-400" />
                <span className="font-medium">{title}</span>
              </div>
              {block.input.content ? (
                <p className="text-[12px] text-text-muted mt-1 pl-5">{String(block.input.content).slice(0, 200)}</p>
              ) : null}
              {block.input.deadline ? (
                <p className="text-[10px] text-text-dim mt-1 pl-5">Deadline: {String(block.input.deadline)}</p>
              ) : null}
            </div>
          )}

          {/* Done/Update: show status change */}
          {(isDone || isUpdate) && (
            <div className="px-3 py-2 text-[12px]">
              <span className="text-text-muted">{block.input.task_id ? String(block.input.task_id) : title}</span>
              {block.input.note ? <p className="text-text-muted mt-1">{String(block.input.note)}</p> : null}
            </div>
          )}

          {/* Task list */}
          {taskList.length > 0 ? (
            <div className="px-3 py-2 space-y-1 max-h-60 overflow-y-auto">
              {taskList.map((t, i) => (
                <div key={i} className="flex items-center gap-2 text-[12px]">
                  <span className={`px-1.5 py-0.5 rounded text-[10px] shrink-0 ${STATUS_COLORS[t.status] || 'bg-border-subtle text-text-muted'}`}>
                    {t.status}
                  </span>
                  <span className="text-text-secondary truncate">{t.title}</span>
                  {t.deadline && <span className="text-[10px] text-text-faint shrink-0">{t.deadline}</span>}
                </div>
              ))}
            </div>
          ) : resultText && !isCreate && !isDone && !isUpdate ? (
            <pre className={`px-3 py-2 text-[12px] whitespace-pre-wrap max-h-60 overflow-y-auto ${block.isError ? 'text-red-400' : 'text-text-muted'}`}>
              {resultText}
            </pre>
          ) : null}

          {/* Success message */}
          {(isCreate || isDone) && resultText && !block.isError && !taskList.length && (
            <div className="px-3 py-1.5 text-[11px] text-green-400/70 border-t border-border-subtle">
              {isDone ? 'Task completed' : 'Task created'}
            </div>
          )}

          {block.isError && resultText && (
            <pre className="px-3 py-2 text-[12px] text-red-400 whitespace-pre-wrap border-t border-border-subtle">
              {resultText}
            </pre>
          )}

          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2">
              <Loader2 size={12} className="animate-spin" /> Working...
            </div>
          )}
        </div>
      )}
    </div>
  );
}
