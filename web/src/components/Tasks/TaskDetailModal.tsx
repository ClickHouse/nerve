import { useEffect } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useTaskStore } from '../../stores/taskStore';
import { useTaskStatusStore } from '../../stores/taskStatusStore';
import { IconButton, Modal } from '../ui';
import { Maximize2 } from '../ui/icons';
import { TaskDetailBody } from './TaskDetailBody';

/**
 * `/tasks/:taskId` rendered over the board.
 *
 * Mounted only when the route was reached with a `background` location in
 * history state (see `TasksPage.openTask`). A cold load or a refresh of
 * the same URL has no such state and falls through to the full
 * `TaskDetailPage` — so the URL stays shareable and the back button still
 * means "back", while clicking a card doesn't tear down the board.
 */
export function TaskDetailModal() {
  const { taskId } = useParams<{ taskId: string }>();
  const navigate = useNavigate();

  const selectedTask = useTaskStore((s) => s.selectedTask);
  const detailLoading = useTaskStore((s) => s.detailLoading);
  const loadTask = useTaskStore((s) => s.loadTask);
  const clearSelectedTask = useTaskStore((s) => s.clearSelectedTask);
  const loadStatuses = useTaskStatusStore((s) => s.load);

  useEffect(() => {
    if (taskId) void loadTask(taskId);
    void loadStatuses();
    return () => clearSelectedTask();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [taskId]);

  // -1 rather than navigate('/tasks'): the board is still mounted behind
  // this, so going back restores it with its scroll position intact.
  const close = () => navigate(-1);

  const title = detailLoading
    ? 'Loading...'
    : selectedTask?.title ?? 'Task not found';

  return (
    <Modal
      open
      onClose={close}
      title={
        <span className="flex items-center gap-2 min-w-0">
          <span className="truncate">{title}</span>
          {taskId && (
            <IconButton
              label="Open as full page"
              size="xs"
              onClick={() => navigate(`/tasks/${taskId}`, { replace: true })}
            >
              <Maximize2 size={13} />
            </IconButton>
          )}
        </span>
      }
      // The task body is a document, not a form — it needs room to read.
      size="wide"
      // Markdown editing behind a backdrop click is too much to lose.
      closeOnBackdrop={false}
      className="h-[85vh] max-h-[85vh]"
    >
      {detailLoading && (
        <div className="p-8 text-center text-text-faint text-sm">Loading...</div>
      )}
      {!detailLoading && !selectedTask && (
        <div className="p-8 text-center text-text-faint text-sm">Task not found.</div>
      )}
      {!detailLoading && selectedTask && <TaskDetailBody task={selectedTask} />}
    </Modal>
  );
}
