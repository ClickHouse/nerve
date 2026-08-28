import { useState, useCallback } from 'react';
import { MarkdownContent } from '../Chat/MarkdownContent';
import { Button, IconButton } from '../ui';
import { Eye, Edit3, Lock, Save } from '../ui/icons';

interface FileEditorProps {
  path: string;
  content: string;
  modified: boolean;
  // Set for a file the server will refuse to write — a reviewed file on a
  // locked instance. The save is offered nowhere it would come back a 403.
  readOnly?: boolean;
  saving: boolean;
  onContentChange: (content: string) => void;
  onSave: () => void;
}

export function FileEditor({ path, content, modified, readOnly, saving, onContentChange, onSave }: FileEditorProps) {
  const [mode, setMode] = useState<'edit' | 'preview'>('edit');

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 's') {
      e.preventDefault();
      if (!readOnly) onSave();
    }
  }, [onSave, readOnly]);

  return (
    <div className="flex-1 flex flex-col min-h-0">
      {/* Toolbar */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-border-subtle bg-bg shrink-0">
        <span className="text-sm text-text-dim font-mono">{path}</span>
        <div className="flex items-center gap-2">
          {/* A segmented pair. IconButton's required label gives each half an
              accessible name, and `active` is the accent tint the rest of the
              app uses for a current selection. `px-2.5` keeps the segments
              wide — the `p-1` of size xs would halve them. */}
          <div className="flex bg-surface-raised rounded-md border border-border">
            <IconButton
              label="Edit source" size="xs" className="px-2.5"
              active={mode === 'edit'} onClick={() => setMode('edit')}
            >
              <Edit3 size={13} />
            </IconButton>
            <IconButton
              label="Preview rendered" size="xs" className="px-2.5"
              active={mode === 'preview'} onClick={() => setMode('preview')}
            >
              <Eye size={13} />
            </IconButton>
          </div>
          {readOnly && (
            <span className="flex items-center gap-1.5 text-xs text-text-dim">
              <Lock size={12} />
              Read-only (lockdown)
            </span>
          )}
          {modified && !readOnly && (
            <Button variant="primary" onClick={onSave} disabled={saving}>
              <Save size={12} />
              {saving ? 'Saving...' : 'Save'}
            </Button>
          )}
        </div>
      </div>

      {/* Content */}
      {mode === 'edit' ? (
        <textarea
          value={content}
          onChange={(e) => onContentChange(e.target.value)}
          onKeyDown={handleKeyDown}
          readOnly={readOnly}
          // Not the `TextArea` primitive: `.editor-textarea` carries the mono
          // stack and `tab-size: 2` that make this a code editor rather than a
          // form field, and the primitive's field chrome (border, radius,
          // raised surface, px-3 py-2) is wrong for a full-pane editor.
          className="flex-1 p-4 bg-bg-sunken text-sm text-text outline-none resize-none editor-textarea"
          spellCheck={false}
        />
      ) : (
        <div className="flex-1 p-6 overflow-y-auto">
          <div className="max-w-3xl">
            <MarkdownContent content={content} />
          </div>
        </div>
      )}
    </div>
  );
}
