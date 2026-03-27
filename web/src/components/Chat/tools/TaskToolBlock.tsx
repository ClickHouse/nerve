import { CheckSquare, ListTodo, Plus, CheckCircle, Pencil, FileText, Loader2 } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';
import { extractText } from '../../../utils/extractResultText';
import { TASK_STATUS_COLORS as STATUS_COLORS } from '../../../constants/statusStyles';
import { CollapsibleToolBlock } from './CollapsibleToolBlock';

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
    <CollapsibleToolBlock
      isRunning={isRunning}
      isError={block.isError}
      icon={Icon}
      iconClassName={isDone ? 'text-green-400' : isCreate ? 'text-blue-400' : 'text-[#888]'}
      label={label}
      headerExtra={<>
        {title && <span className="text-[12px] text-[#777] truncate">{title}</span>}
        {status && (
          <span className={`text-[10px] px-1.5 py-0.5 rounded ${STATUS_COLORS[status] || 'bg-[#333] text-[#888]'}`}>
            {status}
          </span>
        )}
        {isList && taskList.length > 0 && (
          <span className="text-[10px] text-[#555] shrink-0">{taskList.length} tasks</span>
        )}
      </>}
    >
      {/* Create: show what was created */}
      {isCreate && title && (
        <div className="px-3 py-2">
          <div className="text-[12px] text-[#bbb] flex items-center gap-2">
            <Plus size={11} className="text-blue-400" />
            <span className="font-medium">{title}</span>
          </div>
          {block.input.content ? (
            <p className="text-[12px] text-[#888] mt-1 pl-5">{String(block.input.content).slice(0, 200)}</p>
          ) : null}
          {block.input.deadline ? (
            <p className="text-[10px] text-[#666] mt-1 pl-5">Deadline: {String(block.input.deadline)}</p>
          ) : null}
        </div>
      )}

      {/* Done/Update: show status change */}
      {(isDone || isUpdate) && (
        <div className="px-3 py-2 text-[12px]">
          <span className="text-[#888]">{block.input.task_id ? String(block.input.task_id) : title}</span>
          {block.input.note ? <p className="text-[#777] mt-1">{String(block.input.note)}</p> : null}
        </div>
      )}

      {/* Task list */}
      {taskList.length > 0 ? (
        <div className="px-3 py-2 space-y-1 max-h-60 overflow-y-auto">
          {taskList.map((t, i) => (
            <div key={i} className="flex items-center gap-2 text-[12px]">
              <span className={`px-1.5 py-0.5 rounded text-[10px] shrink-0 ${STATUS_COLORS[t.status] || 'bg-[#333] text-[#888]'}`}>
                {t.status}
              </span>
              <span className="text-[#bbb] truncate">{t.title}</span>
              {t.deadline && <span className="text-[10px] text-[#555] shrink-0">{t.deadline}</span>}
            </div>
          ))}
        </div>
      ) : resultText && !isCreate && !isDone && !isUpdate ? (
        <pre className={`px-3 py-2 text-[12px] whitespace-pre-wrap max-h-60 overflow-y-auto ${block.isError ? 'text-red-400' : 'text-[#999]'}`}>
          {resultText}
        </pre>
      ) : null}

      {/* Success message */}
      {(isCreate || isDone) && resultText && !block.isError && !taskList.length && (
        <div className="px-3 py-1.5 text-[11px] text-green-400/70 border-t border-[#222]">
          {isDone ? 'Task completed' : 'Task created'}
        </div>
      )}

      {block.isError && resultText && (
        <pre className="px-3 py-2 text-[12px] text-red-400 whitespace-pre-wrap border-t border-[#222]">
          {resultText}
        </pre>
      )}

      {isRunning && block.result === undefined && (
        <div className="px-3 py-3 text-[12px] text-[#666] flex items-center gap-2">
          <Loader2 size={12} className="animate-spin" /> Working...
        </div>
      )}
    </CollapsibleToolBlock>
  );
}
