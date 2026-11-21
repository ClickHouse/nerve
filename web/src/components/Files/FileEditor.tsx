import { useState, useCallback } from 'react';
import type { ReadOnlyReason } from '../../api/client';
import { authUrl } from '../../api/hosted';
import { MarkdownContent } from '../Chat/MarkdownContent';
import { Button, IconButton } from '../ui';
import { Download, Eye, Edit3, Lock, Save } from '../ui/icons';
import { imageLinkProps, isRasterImagePath } from '../../utils/imageLinks';

interface FileEditorProps {
  path: string;
  content: string;
  modified: boolean;
  // Set for a file the server will refuse to write — a reviewed file on a
  // locked instance. The save is offered nowhere it would come back a 403.
  readOnly?: boolean;
  /** `lockdown`: a reviewed file on a locked instance; `not_memory`: only
   *  memory markdown has a write route. */
  readOnlyReason?: ReadOnlyReason;
  /** Shown as a notice, not as text. */
  unreadable?: 'binary' | 'too_large';
  size?: number;
  saving: boolean;
  onContentChange: (content: string) => void;
  onSave: () => void;
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export function FileEditor({
  path, content, modified, readOnly, readOnlyReason, unreadable, size, saving, onContentChange, onSave,
}: FileEditorProps) {
  const [mode, setMode] = useState<'edit' | 'preview'>('edit');
  // Rendering is for markdown; any other file shows its source only.
  const isMarkdown = /\.(md|markdown)$/i.test(path);
  const showPreview = isMarkdown && mode === 'preview' && !unreadable;

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
          {isMarkdown && !unreadable && (
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
          )}
          {readOnly && (
            <span className="flex items-center gap-1.5 text-xs text-text-dim">
              <Lock size={12} />
              {readOnlyReason === 'lockdown' ? 'Read-only (lockdown)' : 'Read-only'}
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
      {unreadable && isRasterImagePath(path) ? (
        <ImagePreview path={path} size={size} />
      ) : unreadable ? (
        <div className="flex-1 flex flex-col items-center justify-center gap-3 p-6 text-sm text-text-faint text-center">
          <span>
            {unreadable === 'binary'
              ? 'Binary file: not shown.'
              : `Too large to show${size !== undefined ? ` (${formatSize(size)})` : ''}. The limit is files.max_file_bytes.`}
          </span>
          {/* A link, not a fetch: the browser streams the file to disk, so a
              large one never sits in memory. The token goes in the query
              because a link sends no header. */}
          <a
            href={authUrl(`/api/workspace/download?path=${encodeURIComponent(path)}`)}
            download={path.split('/').pop() || 'file'}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md border border-border bg-surface-raised text-text-secondary hover:text-text hover:bg-surface-hover"
          >
            <Download size={13} aria-hidden="true" />
            Download{size !== undefined ? ` (${formatSize(size)})` : ''}
          </a>
        </div>
      ) : !showPreview ? (
        <textarea
          value={content}
          onChange={(e) => onContentChange(e.target.value)}
          onKeyDown={handleKeyDown}
          readOnly={readOnly}
          // Not the `TextArea` primitive: `.editor-textarea` carries the mono
          // stack and `tab-size: 2` that make this a code editor rather than a
          // form field, and the primitive's field chrome (border, radius,
          // background, px-3 py-2) is wrong for a full-pane editor.
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

/** A raster image in place of the binary notice; a click opens it in the zoomable viewer. */
function ImagePreview({ path, size }: { path: string; size?: number }) {
  const name = path.split('/').pop() || 'image';
  const download = `/api/workspace/download?path=${encodeURIComponent(path)}`;
  const [dims, setDims] = useState<{ width: number; height: number } | null>(null);
  return (
    <div className="flex-1 min-h-0 flex flex-col items-center gap-3 p-6 overflow-auto" data-lightbox-group="">
      <a
        {...imageLinkProps(`${download}&inline=1`, name, download)}
        className="block rounded-lg overflow-hidden border border-border hover:border-accent/50 transition-colors cursor-zoom-in"
      >
        <img
          src={authUrl(`${download}&inline=1`)}
          alt={name}
          className="block max-w-full max-h-[70vh] object-contain"
          onLoad={e => setDims({ width: e.currentTarget.naturalWidth, height: e.currentTarget.naturalHeight })}
        />
      </a>
      <div className="flex items-center gap-3 text-xs text-text-faint">
        {dims && <span className="tabular-nums">{dims.width} × {dims.height}</span>}
        <a
          href={authUrl(download)}
          download={name}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md border border-border bg-surface-raised text-text-secondary hover:text-text hover:bg-surface-hover"
        >
          <Download size={13} aria-hidden="true" />
          Download{size !== undefined ? ` (${formatSize(size)})` : ''}
        </a>
      </div>
    </div>
  );
}
