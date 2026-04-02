import { useState } from 'react';
import { X } from 'lucide-react';

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
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50" onClick={onClose}>
      <div className="bg-surface-raised border border-border-subtle rounded-xl w-[480px] max-w-[90vw]" onClick={e => e.stopPropagation()}>
        <div className="flex items-center justify-between px-5 py-3 border-b border-border">
          <h2 className="text-[15px] font-semibold">New Task</h2>
          <button onClick={onClose} className="text-text-faint hover:text-text-muted cursor-pointer p-1">
            <X size={18} />
          </button>
        </div>
        <form onSubmit={handleSubmit} className="p-5 space-y-4">
          <div>
            <label className="block text-[12px] text-text-muted mb-1">Title</label>
            <input
              value={title}
              onChange={e => setTitle(e.target.value)}
              autoFocus
              className="w-full px-3 py-2 bg-surface-raised border border-border-subtle rounded-lg text-[14px] text-text outline-none focus:border-[#6366f1]/50"
            />
          </div>
          <div>
            <label className="block text-[12px] text-text-muted mb-1">Details</label>
            <textarea
              value={content}
              onChange={e => setContent(e.target.value)}
              rows={4}
              className="w-full px-3 py-2 bg-surface-raised border border-border-subtle rounded-lg text-[14px] text-text outline-none focus:border-[#6366f1]/50 resize-none"
            />
          </div>
          <div>
            <label className="block text-[12px] text-text-muted mb-1">Deadline</label>
            <input
              type="date"
              value={deadline}
              onChange={e => setDeadline(e.target.value)}
              className="px-3 py-2 bg-surface-raised border border-border-subtle rounded-lg text-[14px] text-text outline-none focus:border-[#6366f1]/50"
            />
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <button type="button" onClick={onClose}
              className="px-4 py-2 text-[13px] text-text-muted hover:text-text-muted cursor-pointer">
              Cancel
            </button>
            <button type="submit"
              className="px-4 py-2 text-[13px] bg-[#6366f1] hover:bg-[#818cf8] text-white rounded-lg cursor-pointer disabled:opacity-50"
              disabled={!title.trim()}>
              Create
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
