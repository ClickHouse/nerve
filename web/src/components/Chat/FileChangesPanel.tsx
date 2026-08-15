import { useState, useEffect, useRef, lazy, Suspense } from 'react';
import { ArrowLeft, Eye, FilePlus, FileEdit, FileX, Loader2, RefreshCw, WrapText } from '../ui/icons';
import { Button, IconButton } from '../ui';
import { useChatStore } from '../../stores/chatStore';
import { api } from '../../api/client';
import { SelectionToolbar } from './SelectionToolbar';
import { MarkdownContent } from './MarkdownContent';
import { MAX_DIFF_LINES } from '../../types/chat';
import type { FileDiff, ModifiedFileSummary } from '../../types/chat';

// The diff renderer pulls in @pierre/diffs + Shiki — only loaded when a file
// diff is actually opened, keeping it off the initial bundle.
const DiffView = lazy(() => import('./DiffView').then((m) => ({ default: m.DiffView })));

// ------------------------------------------------------------------ //
//  File list view                                                      //
// ------------------------------------------------------------------ //

const STATUS_ICON: Record<string, typeof FileEdit> = {
  created: FilePlus,
  modified: FileEdit,
  deleted: FileX,
};

const STATUS_COLOR: Record<string, string> = {
  created: 'text-diff-add',
  modified: 'text-warning',
  deleted: 'text-diff-del',
};

const STATUS_BADGE: Record<string, string> = {
  created: '+',
  modified: 'M',
  deleted: 'D',
};

function splitPath(shortPath: string): { fileName: string; dirPath: string } {
  const parts = shortPath.split('/');
  const fileName = parts.pop() || shortPath;
  const dirPath = parts.join('/');
  return { fileName, dirPath };
}

function FileCard({ file, onClick }: { file: ModifiedFileSummary; onClick: () => void }) {
  const { fileName, dirPath } = splitPath(file.short_path);
  const Icon = STATUS_ICON[file.status] || FileEdit;
  const color = STATUS_COLOR[file.status] || 'text-text-muted';
  const badge = STATUS_BADGE[file.status] || '?';

  return (
    <Button
      variant="subtle"
      size="md"
      fullWidth
      onClick={onClick}
      className="px-4 py-2.5 rounded-none text-left hover:bg-surface border-b border-surface-raised last:border-b-0 group"
    >
      <div className="flex w-full items-center gap-2.5">
        <span className={`text-xs leading-tight font-bold font-mono w-4 text-center shrink-0 ${color}`}>
          {badge}
        </span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <Icon size={13} className={`shrink-0 ${color}`} />
            <span className="text-sm leading-tight font-medium text-text-secondary truncate">{fileName}</span>
          </div>
          {dirPath && (
            <div className="text-xs leading-tight text-text-faint truncate ml-[21px]">{dirPath}</div>
          )}
        </div>
        <div className="flex items-center gap-1.5 shrink-0 text-xs leading-tight font-mono tabular-nums">
          {file.stats.additions > 0 && (
            <span className="text-diff-add">+{file.stats.additions}</span>
          )}
          {file.stats.deletions > 0 && (
            <span className="text-diff-del">&minus;{file.stats.deletions}</span>
          )}
        </div>
      </div>
    </Button>
  );
}

// ------------------------------------------------------------------ //
//  Detail view (loads diff on demand)                                  //
// ------------------------------------------------------------------ //

// Persisted line-wrap preference for diff inspection (shared across sessions).
const WRAP_STORAGE_KEY = 'nerve_diff_wrap';

function FileDetailView({ file, onBack }: { file: ModifiedFileSummary; onBack: () => void }) {
  const activeSession = useChatStore(s => s.activeSession);
  const [diff, setDiff] = useState<FileDiff | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [wrap, setWrap] = useState(() => localStorage.getItem(WRAP_STORAGE_KEY) === 'true');
  // Markdown files only: toggle between the raw diff and a rendered preview.
  const [preview, setPreview] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const toggleWrap = () => {
    const next = !wrap;
    setWrap(next);
    localStorage.setItem(WRAP_STORAGE_KEY, String(next));
  };

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    setPreview(false);
    api.getFileDiff(activeSession, file.path)
      .then(data => { if (!cancelled) setDiff(data); })
      .catch(e => { if (!cancelled) setError(String(e)); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [activeSession, file.path]);

  // Empty string is a valid (empty) markdown doc — check against null/undefined.
  const canPreview = diff?.markdown_content != null;

  const { fileName } = splitPath(file.short_path);
  const color = STATUS_COLOR[file.status] || 'text-text-muted';

  return (
    <div className="flex flex-col h-full">
      {/* Detail header */}
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-border-subtle bg-bg-sunken shrink-0">
        <IconButton label="Back to the file list" size="xs" onClick={onBack}>
          <ArrowLeft size={14} />
        </IconButton>
        <span className={`text-sm font-medium ${color}`}>{fileName}</span>
        <div className="flex items-center gap-1.5 text-xs font-mono tabular-nums">
          {diff?.stats && diff.stats.additions > 0 && (
            <span className="text-diff-add">+{diff.stats.additions}</span>
          )}
          {diff?.stats && diff.stats.deletions > 0 && (
            <span className="text-diff-del">&minus;{diff.stats.deletions}</span>
          )}
        </div>
        <div className="ml-auto flex items-center gap-1">
          {canPreview && (
            <IconButton
              label={preview ? 'Show raw diff' : 'Show rendered markdown'}
              size="xs"
              active={preview}
              aria-pressed={preview}
              onClick={() => setPreview(p => !p)}
            >
              <Eye size={13} />
            </IconButton>
          )}
          <IconButton
            label={wrap ? 'Disable line wrapping' : 'Enable line wrapping'}
            size="xs"
            active={wrap}
            aria-pressed={wrap}
            onClick={toggleWrap}
          >
            <WrapText size={13} />
          </IconButton>
        </div>
      </div>
      <div className="text-xs text-text-faint px-4 py-1 bg-bg-sunken border-b border-surface-raised">
        {file.short_path}
      </div>

      {/* Diff content */}
      <div ref={containerRef} className="flex-1 overflow-y-auto relative" data-role="plan">
        <SelectionToolbar containerRef={containerRef} />
        {loading && (
          <div className="flex items-center gap-2 justify-center py-8 text-sm text-text-faint">
            <Loader2 size={14} className="animate-spin" /> Loading diff...
          </div>
        )}
        {error && (
          <div className="px-4 py-4 text-sm text-hue-red">Failed to load diff: {error}</div>
        )}
        {diff && !loading && (
          preview && diff.markdown_content != null ? (
            <div className="px-4 py-3 text-sm">
              <MarkdownContent content={diff.markdown_content} />
              {diff.markdown_truncated && (
                <div className="text-center py-3 mt-3 text-xs text-text-faint border-t border-border-subtle">
                  Preview truncated at {MAX_DIFF_LINES} lines
                </div>
              )}
            </div>
          ) : (
            <Suspense
              fallback={
                <div className="flex items-center gap-2 justify-center py-8 text-sm text-text-faint">
                  <Loader2 size={14} className="animate-spin" /> Loading diff…
                </div>
              }
            >
              <DiffView diff={diff} wrap={wrap} />
            </Suspense>
          )
        )}
      </div>
    </div>
  );
}

// ------------------------------------------------------------------ //
//  Main panel component                                                //
// ------------------------------------------------------------------ //

export function FileChangesPanel() {
  const modifiedFiles = useChatStore(s => s.modifiedFiles);
  const activeSession = useChatStore(s => s.activeSession);
  const fetchModifiedFiles = useChatStore(s => s.fetchModifiedFiles);
  const [selectedFile, setSelectedFile] = useState<ModifiedFileSummary | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  // Reset selection when session changes
  useEffect(() => {
    setSelectedFile(null);
  }, [activeSession]);

  const handleRefresh = async () => {
    setRefreshing(true);
    await fetchModifiedFiles(activeSession);
    setRefreshing(false);
  };

  if (selectedFile) {
    return (
      <FileDetailView
        file={selectedFile}
        onBack={() => setSelectedFile(null)}
      />
    );
  }

  const totalAdd = modifiedFiles.reduce((sum, f) => sum + f.stats.additions, 0);
  const totalDel = modifiedFiles.reduce((sum, f) => sum + f.stats.deletions, 0);

  return (
    <div className="flex flex-col h-full">
      {/* List header */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-border-subtle bg-bg-sunken shrink-0">
        <div className="flex items-center gap-2 text-xs text-text-muted">
          <span>{modifiedFiles.length} file{modifiedFiles.length !== 1 ? 's' : ''}</span>
          {totalAdd > 0 && <span className="text-hue-green font-mono">+{totalAdd}</span>}
          {totalDel > 0 && <span className="text-hue-red font-mono">&minus;{totalDel}</span>}
        </div>
        <IconButton label="Refresh the file list" size="xs" onClick={handleRefresh}>
          <RefreshCw size={12} className={refreshing ? 'animate-spin' : ''} />
        </IconButton>
      </div>

      {/* File list */}
      <div className="flex-1 overflow-y-auto">
        {modifiedFiles.length === 0 ? (
          <div className="px-4 py-8 text-center text-sm text-text-faint">
            No files modified in this session
          </div>
        ) : (
          modifiedFiles.map(file => (
            <FileCard
              key={file.path}
              file={file}
              onClick={() => setSelectedFile(file)}
            />
          ))
        )}
      </div>
    </div>
  );
}
