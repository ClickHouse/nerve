import { useState } from 'react';
import { Button, Modal, TextArea, TextField } from '../ui';

const FORM_ID = 'task-create-form';

export function TaskCreateDialog({ onClose, onCreate }: {
  onClose: () => void;
  onCreate: (title: string, content: string, deadline: string) => void;
}) {
  const [title, setTitle] = useState('');
  const [content, setContent] = useState('');
  const [deadline, setDeadline] = useState('');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!title.trim()) return;
    onCreate(title.trim(), content.trim(), deadline);
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
