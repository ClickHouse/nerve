import { useCallback, useEffect, useState } from 'react';
import { Calendar, Edit3, ExternalLink, Eye, Save } from 'lucide-react';
import type { Task } from '../../api/client';
import { useTaskStore } from '../../stores/taskStore';
import { StatusBadge, StatusSelect } from './StatusControls';
import { MarkdownContent } from '../Chat/MarkdownContent';

/**
 * The task editor itself — metadata row, edit/preview toggle, markdown
 * body, save.
 *
 * Shared by the full page and the board's modal so the two can't drift.
 * Only the surrounding chrome differs: the page supplies a back button and
 * heading, the modal supplies the dialog frame.
 */
export function TaskDetailBody({ task }: { task: Task }) {
  const saving = useTaskStore((s) => s.saving);
  const saveTaskContent = useTaskStore((s) => s.saveTaskContent);
  const updateStatus = useTaskStore((s) => s.updateStatus);

  const [mode, setMode] = useState<'edit' | 'preview'>('preview');
  const [localContent, setLocalContent] = useState('');
  const [dirty, setDirty] = useState(false);

  // Adopt the loaded markdown, but never clobber unsaved edits — a
  // task_updated broadcast can land mid-typing.
  useEffect(() => {
    if (task.content != null && !dirty) {
      setLocalContent(task.content);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [task.content]);

  const handleSave = useCallback(async () => {
    if (!dirty) return;
    await saveTaskContent(task.id, localContent);
    setDirty(false);
  }, [task.id, dirty, localContent, saveTaskContent]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 's') {
      e.preventDefault();
      void handleSave();
    }
  }, [handleSave]);

  return (
    <div className="flex flex-col h-full min-h-0">
      <div className="flex items-center gap-3 px-5 py-2.5 text-[12px] border-b border-border-subtle shrink-0 flex-wrap">
        <StatusBadge status={task.status} />
        <StatusSelect
          value={task.status}
          onChange={(status) => updateStatus(task.id, status)}
          className="text-[12px] px-2 py-1 bg-surface-raised border border-border rounded text-text-muted outline-none cursor-pointer"
        />
        {task.deadline && (
          <span className="flex items-center gap-1 text-text-dim">
            <Calendar size={11} /> {task.deadline}
          </span>
        )}
        {task.source && <span className="text-text-faint">from {task.source}</span>}
        {task.source_url && (
          <a
            href={task.source_url}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1 text-accent hover:underline"
          >
            <ExternalLink size={11} /> source
          </a>
        )}

        <div className="ml-auto flex items-center gap-2">
          {dirty && (
            <button
              onClick={handleSave}
              disabled={saving}
              className="flex items-center gap-1.5 px-3 py-1 text-[12px] bg-accent hover:bg-accent-hover text-white rounded-md cursor-pointer disabled:opacity-50"
            >
              <Save size={12} /> {saving ? 'Saving...' : 'Save'}
            </button>
          )}
          <div className="flex bg-surface-raised rounded-md border border-border">
            <button
              onClick={() => setMode('edit')}
              aria-pressed={mode === 'edit'}
              title="Edit"
              className={`px-2.5 py-1 rounded-l-md cursor-pointer transition-colors
                ${mode === 'edit' ? 'text-text' : 'text-text-dim hover:text-text-muted'}`}
            >
              <Edit3 size={13} />
            </button>
            <button
              onClick={() => setMode('preview')}
              aria-pressed={mode === 'preview'}
              title="Preview"
              className={`px-2.5 py-1 rounded-r-md cursor-pointer transition-colors
                ${mode === 'preview' ? 'text-text' : 'text-text-dim hover:text-text-muted'}`}
            >
              <Eye size={13} />
            </button>
          </div>
        </div>
      </div>

      {mode === 'edit' ? (
        <textarea
          value={localContent}
          onChange={(e) => { setLocalContent(e.target.value); setDirty(true); }}
          onKeyDown={handleKeyDown}
          className="flex-1 min-h-0 p-5 bg-bg-sunken text-[13px] text-text font-mono leading-relaxed outline-none resize-none"
          spellCheck={false}
          placeholder="Task content..."
        />
      ) : (
        <div className="flex-1 min-h-0 overflow-y-auto p-5">
          {localContent
            ? <MarkdownContent content={localContent} />
            : <span className="text-text-faint italic">No content</span>}
        </div>
      )}
    </div>
  );
}
