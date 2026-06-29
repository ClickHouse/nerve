import { useEffect, useRef, useState } from 'react';
import { CheckCircle2, ChevronDown, ChevronRight, Circle, Loader2 } from 'lucide-react';
import type { TodoItem, CCTask } from '../../stores/chatStore';

/**
 * Unified row model rendered by the panel. Both legacy TodoWrite items
 * and Claude Code 2.1+ tasks collapse into this shape so the panel only
 * needs one renderer.
 */
interface PanelRow {
  key: string;
  label: string;
  activeLabel: string;
  status: 'pending' | 'in_progress' | 'completed';
  /** Numeric task id ("1", "2", ...) — only set for CC tasks; shown as a badge. */
  id?: string;
}

function ccTaskToRow(task: CCTask, index: number): PanelRow {
  // Skip the placeholder prefix in the badge — show "—" while we wait.
  const badge = task.id.startsWith('pending:') ? undefined : task.id;
  return {
    key: `cc:${task.id || index}`,
    label: task.subject,
    activeLabel: task.activeForm || task.subject,
    status: task.status,
    id: badge,
  };
}

function todoToRow(todo: TodoItem, index: number): PanelRow {
  return {
    key: `todo:${index}:${todo.content}`,
    label: todo.content,
    activeLabel: todo.activeForm || todo.content,
    status: todo.status,
  };
}

export function TodoPanel({
  todos,
  ccTasks,
}: {
  todos: TodoItem[];
  ccTasks?: CCTask[];
}) {
  const [visible, setVisible] = useState(true);
  const [expandCompleted, setExpandCompleted] = useState(false);
  const hideTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Track previous row count so we can re-show the panel when new work appears.
  const prevCountRef = useRef<number>(0);

  // CC tasks supersede legacy TodoWrite in Claude Code 2.1+, but a session
  // mid-migration could have both. Show whichever has rows; if both, show
  // CC tasks (the modern source of truth).
  const rows: PanelRow[] = ccTasks && ccTasks.length > 0
    ? ccTasks.map(ccTaskToRow)
    : todos.map(todoToRow);

  const allDone = rows.length > 0 && rows.every(r => r.status === 'completed');

  // Auto-hide 5s after all items complete
  useEffect(() => {
    if (hideTimer.current) {
      clearTimeout(hideTimer.current);
      hideTimer.current = null;
    }

    if (allDone) {
      hideTimer.current = setTimeout(() => setVisible(false), 5000);
    } else {
      setVisible(true);
    }

    return () => {
      if (hideTimer.current) clearTimeout(hideTimer.current);
    };
  }, [allDone]);

  // Reset visibility when row count grows from empty
  useEffect(() => {
    if (prevCountRef.current === 0 && rows.length > 0) {
      setVisible(true);
    }
    prevCountRef.current = rows.length;
  }, [rows.length]);

  if (rows.length === 0 || !visible) return null;

  const completedRows = rows.filter(r => r.status === 'completed');
  const activeRows = rows.filter(r => r.status !== 'completed');
  const completedCount = completedRows.length;

  // Collapse the completed history into a single summary row when there's
  // still active work to show. When everything is done, keep the list
  // expanded — the panel will auto-hide in 5s anyway, and a lone "N
  // completed" summary with no detail would be useless.
  const collapseCompleted = !allDone && completedCount > 0 && !expandCompleted;

  return (
    <div className={`border-t border-border-subtle bg-bg-sunken shrink-0 transition-all duration-300 ${allDone ? 'opacity-60' : ''}`}>
      <div className="max-w-[var(--chat-width)] mx-auto px-5 py-2.5">
        <div className="flex items-center gap-2 mb-1.5">
          <span className="text-[11px] font-medium text-text-faint uppercase tracking-wider">Tasks</span>
          <span className="text-[10px] text-text-faint">
            {completedCount}/{rows.length}
          </span>
        </div>
        <div className="space-y-0.5">
          {collapseCompleted ? (
            <>
              <CompletedSummaryRow
                count={completedCount}
                expanded={false}
                onClick={() => setExpandCompleted(true)}
              />
              {activeRows.map(row => (
                <TaskRow key={row.key} row={row} />
              ))}
            </>
          ) : (
            <>
              {!allDone && completedCount > 0 && (
                <CompletedSummaryRow
                  count={completedCount}
                  expanded={true}
                  onClick={() => setExpandCompleted(false)}
                />
              )}
              {rows.map(row => (
                <TaskRow key={row.key} row={row} />
              ))}
            </>
          )}
        </div>
      </div>
    </div>
  );
}

function CompletedSummaryRow({
  count,
  expanded,
  onClick,
}: {
  count: number;
  expanded: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="flex items-center gap-2 py-0.5 text-[13px] w-full text-left rounded text-text-faint hover:text-text-muted transition-colors"
    >
      <CheckCircle2 size={14} className="text-hue-green shrink-0 opacity-60" />
      {expanded ? (
        <ChevronDown size={12} className="shrink-0" />
      ) : (
        <ChevronRight size={12} className="shrink-0" />
      )}
      <span>
        {count} completed {count === 1 ? 'task' : 'tasks'}
      </span>
    </button>
  );
}

function TaskRow({ row }: { row: PanelRow }) {
  const isCompleted = row.status === 'completed';
  const isActive = row.status === 'in_progress';

  return (
    <div className={`todo-row flex items-center gap-2 py-0.5 text-[13px] transition-opacity duration-300 ${isCompleted ? 'opacity-50' : ''}`}>
      {isCompleted ? (
        <CheckCircle2 size={14} className="text-hue-green shrink-0 todo-icon-enter" />
      ) : isActive ? (
        <Loader2 size={14} className="text-accent shrink-0 animate-spin" />
      ) : (
        <Circle size={14} className="text-text-faint shrink-0" />
      )}
      {row.id && (
        <span className="text-[10px] tabular-nums text-text-faint shrink-0">#{row.id}</span>
      )}
      <span className={`${isCompleted ? 'line-through text-text-faint' : isActive ? 'text-text' : 'text-text-muted'} transition-colors duration-300`}>
        {isActive ? row.activeLabel : row.label}
      </span>
    </div>
  );
}
