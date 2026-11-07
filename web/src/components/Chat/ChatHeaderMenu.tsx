import { useCallback, useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, type SessionTask, type TaskSessionRelation } from '../../api/client';
import { useTaskStatusStore } from '../../stores/taskStatusStore';
import { StatusBadge } from '../Tasks/StatusControls';
import { Button, IconButton, Modal } from '../ui';
import { CheckSquare, MoreHorizontal } from '../ui/icons';

/** What the session did to a task, as the dialog shows it. */
const RELATION_TEXT: Record<TaskSessionRelation, string> = {
  created: 'Created',
  implemented: 'Implemented',
  updated: 'Updated',
};

/**
 * The chat header's "..." menu, for a saved session.
 *
 * Its item "Tasks from this session (N)" opens a dialog with the tasks
 * that the session created, implemented or updated. The tasks load each
 * time the menu opens, so the count is current and a closed menu costs no
 * request. The parent gives the component a `key` per session, so a
 * session switch starts with an empty state.
 */
export function ChatHeaderMenu({ sessionId }: { sessionId: string }) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [tasks, setTasks] = useState<SessionTask[] | null>(null);
  const [failed, setFailed] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const loadStatuses = useTaskStatusStore((s) => s.load);

  const load = useCallback(() => {
    setFailed(false);
    api.getSessionTasks(sessionId)
      .then(({ tasks }) => setTasks(tasks))
      .catch(() => setFailed(true));
  }, [sessionId]);

  // Close on an outside press, and on Escape, which must not also reach the
  // global Escape that stops the turn (the same as the model menu).
  useEffect(() => {
    if (!menuOpen) return;
    const onDown = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) setMenuOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return;
      e.preventDefault();
      e.stopPropagation();
      setMenuOpen(false);
      triggerRef.current?.focus();
    };
    document.addEventListener('mousedown', onDown);
    document.addEventListener('keydown', onKey, true);
    return () => {
      document.removeEventListener('mousedown', onDown);
      document.removeEventListener('keydown', onKey, true);
    };
  }, [menuOpen]);

  const toggleMenu = () => {
    if (!menuOpen) load();
    setMenuOpen((open) => !open);
  };

  const openDialog = () => {
    setMenuOpen(false);
    setDialogOpen(true);
    void loadStatuses();
  };

  return (
    <div className="relative shrink-0" ref={menuRef}>
      <IconButton
        ref={triggerRef}
        label="More actions"
        size="xs"
        active={menuOpen}
        // A disclosure of ordinary buttons, not the ARIA menu pattern; the
        // composer's menu has the same note.
        aria-expanded={menuOpen}
        onClick={toggleMenu}
      >
        <MoreHorizontal size={16} />
      </IconButton>

      {menuOpen && (
        <div className="absolute right-0 top-full mt-1 z-50 w-max min-w-[200px] bg-surface-raised border border-border-subtle rounded-lg shadow-xl py-1">
          <Button
            variant="subtle"
            size="sm"
            fullWidth
            onClick={openDialog}
            className="justify-start gap-2.5 px-3 py-1.5 rounded-none text-left whitespace-nowrap"
          >
            <CheckSquare size={14} />
            Tasks from this session{tasks && ` (${tasks.length})`}
          </Button>
        </div>
      )}

      <Modal
        open={dialogOpen}
        onClose={() => setDialogOpen(false)}
        title="Tasks from this session"
      >
        <SessionTaskList tasks={tasks} failed={failed} onRetry={load} onOpen={() => setDialogOpen(false)} />
      </Modal>
    </div>
  );
}

function SessionTaskList({ tasks, failed, onRetry, onOpen }: {
  tasks: SessionTask[] | null;
  failed: boolean;
  onRetry: () => void;
  onOpen: () => void;
}) {
  if (failed) {
    return (
      <div role="alert" className="px-5 py-6 text-sm text-text-muted flex items-center gap-3">
        The tasks did not load.
        <Button variant="link" size="sm" onClick={onRetry}>Try again</Button>
      </div>
    );
  }
  if (tasks === null) {
    return <p className="px-5 py-6 text-sm text-text-faint">Loading tasks...</p>;
  }
  if (tasks.length === 0) {
    return (
      <p className="px-5 py-6 text-sm text-text-faint">
        This session did not create or change a task.
      </p>
    );
  }
  return (
    <ul className="py-2">
      {tasks.map((task) => (
        <li key={task.id}>
          <Link
            to={`/tasks/${task.id}`}
            onClick={onOpen}
            className="flex items-center gap-3 px-5 py-2.5 hover:bg-surface-hover focus-visible:bg-surface-hover outline-none"
          >
            <span className="min-w-0 flex-1">
              <span className="block truncate text-sm text-text">{task.title}</span>
              <span className="block text-xs text-text-faint">{RELATION_TEXT[task.relation]}</span>
            </span>
            <StatusBadge status={task.status} className="shrink-0" />
          </Link>
        </li>
      ))}
    </ul>
  );
}
