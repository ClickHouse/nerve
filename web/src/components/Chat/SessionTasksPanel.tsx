import { useEffect } from 'react';
import { Link, useLocation } from 'react-router-dom';
import type { TaskSessionRelation } from '../../api/client';
import { useChatStore } from '../../stores/chatStore';
import { useTaskStatusStore } from '../../stores/taskStatusStore';
import { StatusBadge } from '../Tasks/StatusControls';
import { Button } from '../ui';
import { Loader2 } from '../ui/icons';

/** What the session did to a task, as the list shows it. */
const RELATION_TEXT: Record<TaskSessionRelation, string> = {
  created: 'Created',
  implemented: 'Implemented',
  updated: 'Updated',
};

/**
 * The Tasks tab of the chat side panel: the tasks that the open session
 * created, implemented or updated (the chat store's `sessionTasks`).
 *
 * A task opens in the task modal over the chat: the link gives the chat as
 * the background location, as the task board does. Closing the modal goes
 * back to this chat with the panel as it was.
 */
export function SessionTasksPanel() {
  const tasks = useChatStore(s => s.sessionTasks);
  const failed = useChatStore(s => s.sessionTasksFailed);
  const activeSession = useChatStore(s => s.activeSession);
  const fetchSessionTasks = useChatStore(s => s.fetchSessionTasks);
  const loadStatuses = useTaskStatusStore(s => s.load);
  const location = useLocation();

  // The status badges take their labels and colours from this store.
  useEffect(() => {
    void loadStatuses();
  }, [loadStatuses]);

  let body;
  if (failed) {
    body = (
      <div role="alert" className="px-4 py-8 flex flex-col items-center gap-2 text-sm text-text-faint">
        The tasks did not load.
        <Button variant="link" size="sm" onClick={() => void fetchSessionTasks(activeSession)}>
          Try again
        </Button>
      </div>
    );
  } else if (tasks === null) {
    body = (
      <div className="flex items-center gap-2 justify-center py-8 text-sm text-text-faint">
        <Loader2 size={14} className="animate-spin" /> Loading tasks...
      </div>
    );
  } else if (tasks.length === 0) {
    body = (
      <div className="px-4 py-8 text-center text-sm text-text-faint">
        No tasks from this session
      </div>
    );
  } else {
    body = (
      <ul>
        {tasks.map(task => (
          <li key={task.id} className="border-b border-surface-raised last:border-b-0">
            <Link
              to={`/tasks/${task.id}`}
              state={{ background: location }}
              className="flex items-center gap-3 px-4 py-2.5 hover:bg-surface outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-accent/60"
            >
              <span className="min-w-0 flex-1">
                <span className="block truncate text-sm leading-tight font-medium text-text-secondary">
                  {task.title}
                </span>
                <span className="block mt-1 text-xs leading-tight text-text-faint">
                  {RELATION_TEXT[task.relation]}
                </span>
              </span>
              <StatusBadge status={task.status} className="shrink-0" />
            </Link>
          </li>
        ))}
      </ul>
    );
  }

  return (
    <div className="flex flex-col flex-1 min-h-0">
      {!failed && !!tasks?.length && (
        <div className="px-4 py-2 border-b border-border-subtle bg-bg-sunken shrink-0 text-xs text-text-muted">
          {tasks.length} task{tasks.length !== 1 ? 's' : ''}
        </div>
      )}
      <div className="flex-1 overflow-y-auto">{body}</div>
    </div>
  );
}
