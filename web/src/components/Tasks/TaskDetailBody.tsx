import { useCallback, useEffect, useState } from 'react';
import type { Task } from '../../api/client';
import { useTaskStore } from '../../stores/taskStore';
import { Button, IconButton } from '../ui';
import { Calendar, Edit3, ExternalLink, Eye, History, Save } from '../ui/icons';
import { StatusBadge, StatusSelect } from './StatusControls';
import { MarkdownContent } from '../Chat/MarkdownContent';
import { TaskTimeline } from './TaskTimeline';

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
  const [showHistory, setShowHistory] = useState(false);
  const [localContent, setLocalContent] = useState('');
  const [dirty, setDirty] = useState(false);
  const [saveError, setSaveError] = useState(false);

  // Adopt the loaded markdown, but never clobber unsaved edits.
  //
  // Defensive rather than a fix for something reachable today: the only
  // writers of `content` are the initial fetch and the user's own save,
  // because a task_updated broadcast carries the `tasks` row and that table
  // has no content column. The guard is here so that stops being a thing
  // anyone has to know before adding one.
  useEffect(() => {
    if (task.content != null && !dirty) {
      setLocalContent(task.content);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [task.content]);

  const handleSave = useCallback(async () => {
    if (!dirty) return;
    // Keep `dirty` set when the save fails. It is what keeps the Save button
    // on screen, so the retry stays available and the editor cannot look
    // settled while the content is still only in the browser.
    const saved = await saveTaskContent(task.id, localContent);
    setSaveError(!saved);
    if (saved) setDirty(false);
  }, [task.id, dirty, localContent, saveTaskContent]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 's') {
      e.preventDefault();
      void handleSave();
    }
  }, [handleSave]);

  return (
    <div className="flex flex-col h-full min-h-0">
      <div className="flex items-center gap-3 px-5 py-2.5 text-xs border-b border-border-subtle shrink-0 flex-wrap">
        <StatusBadge status={task.status} />
        <StatusSelect
          value={task.status}
          onChange={(status) => updateStatus(task.id, status)}
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
          <IconButton
            label="Status history"
            size="xs"
            active={showHistory}
            aria-pressed={showHistory}
            onClick={() => setShowHistory((v) => !v)}
          >
            <History size={13} />
          </IconButton>
          {dirty && (
            <>
              {saveError && (
                <span role="alert" className="text-xs text-error">
                  Save failed — not saved yet.
                </span>
              )}
              <Button variant="primary" size="xs" onClick={handleSave} disabled={saving}>
                <Save size={12} /> {saving ? 'Saving...' : 'Save'}
              </Button>
            </>
          )}
          {/* `overflow-hidden` so the segments' own radii are clipped by the
              group rather than fought with a per-corner override. */}
          <div className="flex bg-surface-raised rounded-md border border-border overflow-hidden">
            <IconButton
              label="Edit"
              size="xs"
              active={mode === 'edit'}
              aria-pressed={mode === 'edit'}
              onClick={() => setMode('edit')}
            >
              <Edit3 size={13} />
            </IconButton>
            <IconButton
              label="Preview"
              size="xs"
              active={mode === 'preview'}
              aria-pressed={mode === 'preview'}
              onClick={() => setMode('preview')}
            >
              <Eye size={13} />
            </IconButton>
          </div>
        </div>
      </div>

      {showHistory && (
        <div className="px-5 py-3 border-b border-border-subtle bg-surface-raised/30 max-h-52 overflow-y-auto shrink-0">
          <TaskTimeline taskId={task.id} currentStatus={task.status} />
        </div>
      )}

      {mode === 'edit' ? (
        // Deliberately *not* the house `TextArea`. That primitive is a form
        // field — `bg-surface-raised`, a border, a radius and `px-3 py-2` — and
        // this is a full-bleed document pane that fills the panel and sits on
        // the sunken surface. Its chrome cannot be undone from here either:
        // Tailwind emits same-property utilities alphabetically, so
        // `bg-bg-sunken` on the call site loses to the primitive's
        // `bg-surface-raised` at equal specificity. The type scale below is the
        // design system's; the box is this pane's own.
        <textarea
          value={localContent}
          onChange={(e) => { setLocalContent(e.target.value); setDirty(true); setSaveError(false); }}
          onKeyDown={handleKeyDown}
          className="flex-1 min-h-0 p-5 bg-bg-sunken text-sm text-text font-mono leading-relaxed outline-none resize-none"
          spellCheck={false}
          placeholder="Task content..."
        />
      ) : (
        <div className="flex-1 min-h-0 overflow-y-auto px-6 py-5">
          {localContent
            ? <MarkdownContent content={localContent} />
            : <span className="text-text-faint italic">No content</span>}
        </div>
      )}
    </div>
  );
}
