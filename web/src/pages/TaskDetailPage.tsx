import { useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft } from '../components/ui/icons';
import { Button, IconButton } from '../components/ui';
import { useTaskStore } from '../stores/taskStore';
import { useTaskStatusStore } from '../stores/taskStatusStore';
import { TaskDetailBody } from '../components/Tasks/TaskDetailBody';

/**
 * The full-page task route. Everything below the heading is `TaskDetailBody`,
 * the same component the board's modal renders — this page owns only the
 * chrome the modal doesn't have: a back button and a title.
 */
export function TaskDetailPage() {
  const { taskId } = useParams<{ taskId: string }>();
  const navigate = useNavigate();
  const selectedTask = useTaskStore((s) => s.selectedTask);
  const detailLoading = useTaskStore((s) => s.detailLoading);
  const loadTask = useTaskStore((s) => s.loadTask);
  const clearSelectedTask = useTaskStore((s) => s.clearSelectedTask);
  const loadStatuses = useTaskStatusStore((s) => s.load);

  useEffect(() => {
    if (taskId) loadTask(taskId);
    loadStatuses();
    return () => clearSelectedTask();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [taskId]);

  if (detailLoading) {
    return (
      <div className="h-full flex items-center justify-center text-text-faint">
        Loading...
      </div>
    );
  }

  if (!selectedTask) {
    return (
      <div className="h-full flex flex-col items-center justify-center gap-3 text-text-faint">
        <span>Task not found</span>
        <Button variant="link" size="md" onClick={() => navigate('/tasks')}>
          Back to tasks
        </Button>
      </div>
    );
  }

  return (
    // min-h-0 so the body's flex-1 can size against this column rather than
    // overflowing it.
    <div className="h-full flex flex-col min-h-0">
      <div className="border-b border-border-subtle px-6 py-3 bg-bg shrink-0 flex items-center gap-3 min-w-0">
        <IconButton label="Back to tasks" onClick={() => navigate('/tasks')}>
          <ArrowLeft size={18} />
        </IconButton>
        <h1 className="text-lg font-semibold text-text truncate">{selectedTask.title}</h1>
      </div>

      <TaskDetailBody task={selectedTask} />
    </div>
  );
}
