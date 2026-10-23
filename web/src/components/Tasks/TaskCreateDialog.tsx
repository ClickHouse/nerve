import { useEffect, useState } from 'react';
import { useTaskStatusStore, type TaskStatusDef } from '../../stores/taskStatusStore';
import { Button, Modal, Select, TextArea, TextField } from '../ui';

const FORM_ID = 'task-create-form';

// A new task from the UI waits in the backlog unless the user picks another
// status. The agent's `task_create` keeps `pending` as its default.
const DEFAULT_STATUS = 'backlog';
const FALLBACK_STATUS = 'pending';

function labelOf(statuses: TaskStatusDef[], name: string): string {
  return statuses.find((s) => s.name === name)?.label ?? name;
}

export function TaskCreateDialog({ onClose, onCreate, initialStatus }: {
  onClose: () => void;
  onCreate: (title: string, content: string, deadline: string, status: string) => void;
  /** The status of the board column that opened the dialog, if any. */
  initialStatus?: string | null;
}) {
  const statuses = useTaskStatusStore((s) => s.statuses);
  const loadStatuses = useTaskStatusStore((s) => s.load);
  const [title, setTitle] = useState('');
  const [content, setContent] = useState('');
  const [deadline, setDeadline] = useState('');
  // Null until the user picks one, so the default follows the statuses
  // that arrive after the dialog opens.
  const [picked, setPicked] = useState<string | null>(initialStatus ?? null);

  useEffect(() => { void loadStatuses(); }, [loadStatuses]);

  const hasBacklog = statuses.some((s) => s.name === DEFAULT_STATUS);
  const status = picked ?? (hasBacklog ? DEFAULT_STATUS : FALLBACK_STATUS);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!title.trim()) return;
    onCreate(title.trim(), content.trim(), deadline, status);
  };

  return (
    <Modal
      open
      onClose={onClose}
      title="New Task"
      // A stray backdrop click shouldn't bin a half-typed task.
      closeOnBackdrop={false}
      footer={
        <>
          <Button variant="ghost" size="md" onClick={onClose}>
            Cancel
          </Button>
          {/* Outside the <form>, associated by id — keeps the button in the
              modal's footer slot while Enter-to-submit still works. */}
          <Button
            variant="primary"
            size="md"
            type="submit"
            form={FORM_ID}
            disabled={!title.trim()}
          >
            Create
          </Button>
        </>
      }
    >
      <form id={FORM_ID} onSubmit={handleSubmit} className="p-5 space-y-4">
        <div>
          <label className="block text-xs text-text-muted mb-1">Title</label>
          <TextField
            value={title}
            onChange={e => setTitle(e.target.value)}
            autoFocus
          />
        </div>
        <div>
          <label className="block text-xs text-text-muted mb-1">Details</label>
          <TextArea
            value={content}
            onChange={e => setContent(e.target.value)}
            rows={4}
          />
        </div>
        <div>
          <label htmlFor="task-create-status" className="block text-xs text-text-muted mb-1">
            Status
          </label>
          <Select
            id="task-create-status"
            value={status}
            onChange={(e) => setPicked(e.target.value)}
            options={statuses.map((s) => ({ value: s.name, label: s.label }))}
          />
          <p className="mt-1 text-xs text-text-faint">
            The agent can pick up tasks in {labelOf(statuses, FALLBACK_STATUS)}.
            {hasBacklog && ` Tasks in ${labelOf(statuses, DEFAULT_STATUS)} wait until you move them.`}
          </p>
        </div>
        <div>
          <label className="block text-xs text-text-muted mb-1">Deadline</label>
          <TextField
            type="date"
            value={deadline}
            onChange={e => setDeadline(e.target.value)}
            fullWidth={false}
          />
        </div>
      </form>
    </Modal>
  );
}
