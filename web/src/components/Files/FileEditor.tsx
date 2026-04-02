import { useState, useCallback } from 'react';
import { Eye, Edit3, Save } from 'lucide-react';
import { MarkdownContent } from '../Chat/MarkdownContent';

interface FileEditorProps {
  path: string;
  content: string;
  modified: boolean;
  saving: boolean;
  onContentChange: (content: string) => void;
  onSave: () => void;
}

export function FileEditor({ path, content, modified, saving, onContentChange, onSave }: FileEditorProps) {
  const [mode, setMode] = useState<'edit' | 'preview'>('edit');

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 's') {
      e.preventDefault();
      onSave();
    }
  }, [onSave]);

  return (
    <div className="flex-1 flex flex-col min-h-0">
      {/* Toolbar */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-border-subtle bg-bg shrink-0">
        <span className="text-[13px] text-text-dim font-mono">{path}</span>
        <div className="flex items-center gap-2">
          <div className="flex bg-surface-raised rounded-md border border-border">
            <button
              onClick={() => setMode('edit')}
              className={`px-2.5 py-1 text-[12px] rounded-l-md cursor-pointer
                ${mode === 'edit' ? 'bg-surface-raised text-text' : 'text-text-dim hover:text-text-muted'}`}
            >
              <Edit3 size={13} />
            </button>
            <button
              onClick={() => setMode('preview')}
              className={`px-2.5 py-1 text-[12px] rounded-r-md cursor-pointer
                ${mode === 'preview' ? 'bg-surface-raised text-text' : 'text-text-dim hover:text-text-muted'}`}
            >
              <Eye size={13} />
            </button>
          </div>
          {modified && (
            <button
              onClick={onSave}
              disabled={saving}
              className="flex items-center gap-1.5 px-3 py-1 text-[12px] bg-[#6366f1] hover:bg-[#818cf8] text-white rounded-md cursor-pointer disabled:opacity-50"
            >
              <Save size={12} />
              {saving ? 'Saving...' : 'Save'}
            </button>
          )}
        </div>
      </div>

      {/* Content */}
      {mode === 'edit' ? (
        <textarea
          value={content}
          onChange={(e) => onContentChange(e.target.value)}
          onKeyDown={handleKeyDown}
          className="flex-1 p-4 bg-bg-sunken text-[14px] text-text outline-none resize-none editor-textarea"
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
