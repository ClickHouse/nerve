import { useState } from 'react';
import { Modal } from '../ui/Modal';

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
          <button
            type="button"
            onClick={onClose}
            className="px-4 py-2 text-[13px] text-text-muted hover:text-text-secondary cursor-pointer"
          >
            Cancel
          </button>
          {/* Outside the <form>, associated by id — keeps the button in the
              modal's footer slot while Enter-to-submit still works. */}
          <button
            type="submit"
            form={FORM_ID}
            className="px-4 py-2 text-[13px] bg-accent hover:bg-accent-hover text-white rounded-lg cursor-pointer disabled:opacity-50"
            disabled={!title.trim()}
          >
            Create
          </button>
        </>
      }
    >
      <form id={FORM_ID} onSubmit={handleSubmit} className="p-5 space-y-4">
        <div>
          <label className="block text-[12px] text-text-muted mb-1">Title</label>
          <input
            value={title}
            onChange={e => setTitle(e.target.value)}
            autoFocus
            className="w-full px-3 py-2 bg-surface-raised border border-border-subtle rounded-lg text-[14px] text-text outline-none focus:border-accent/50"
          />
        </div>
        <div>
          <label className="block text-[12px] text-text-muted mb-1">Details</label>
          <textarea
            value={content}
            onChange={e => setContent(e.target.value)}
            rows={4}
            className="w-full px-3 py-2 bg-surface-raised border border-border-subtle rounded-lg text-[14px] text-text outline-none focus:border-accent/50 resize-none"
          />
        </div>
        <div>
          <label className="block text-[12px] text-text-muted mb-1">Deadline</label>
          <input
            type="date"
            value={deadline}
            onChange={e => setDeadline(e.target.value)}
            className="px-3 py-2 bg-surface-raised border border-border-subtle rounded-lg text-[14px] text-text outline-none focus:border-accent/50"
          />
        </div>
      </form>
    </Modal>
  );
}
