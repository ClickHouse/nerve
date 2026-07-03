import { useState, useEffect, useRef, lazy, Suspense } from 'react';
import { ArrowLeft, FilePlus, FileEdit, FileX, Loader2, RefreshCw, WrapText } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';
import { api } from '../../api/client';
import { SelectionToolbar } from './SelectionToolbar';
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
    <button
      onClick={onClick}
      className="w-full text-left px-4 py-2.5 hover:bg-surface transition-colors cursor-pointer border-b border-surface-raised last:border-b-0 group"
    >
      <div className="flex items-center gap-2.5">
        <span className={`text-[11px] font-bold font-mono w-4 text-center shrink-0 ${color}`}>
          {badge}
        </span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <Icon size={13} className={`shrink-0 ${color}`} />
            <span className="text-[13px] font-medium text-text-secondary truncate">{fileName}</span>
          </div>
          {dirPath && (
            <div className="text-[11px] text-text-faint truncate ml-[21px]">{dirPath}</div>
          )}
        </div>
        <div className="flex items-center gap-1.5 shrink-0 text-[11px] font-mono tabular-nums">
          {file.stats.additions > 0 && (
            <span className="text-diff-add">+{file.stats.additions}</span>
          )}
          {file.stats.deletions > 0 && (
            <span className="text-diff-del">&minus;{file.stats.deletions}</span>
          )}
        </div>
      </div>
    </button>
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
    api.getFileDiff(activeSession, file.path)
      .then(data => { if (!cancelled) setDiff(data); })
      .catch(e => { if (!cancelled) setError(String(e)); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [activeSession, file.path]);

  const { fileName } = splitPath(file.short_path);
  const color = STATUS_COLOR[file.status] || 'text-text-muted';

  return (
    <div className="flex flex-col h-full">
      {/* Detail header */}
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-border-subtle bg-bg-sunken shrink-0">
        <button
          onClick={onBack}
          className="w-5 h-5 flex items-center justify-center text-text-faint hover:text-text-muted cursor-pointer transition-colors"
        >
          <ArrowLeft size={14} />
        </button>
        <span className={`text-[13px] font-medium ${color}`}>{fileName}</span>
        <div className="flex items-center gap-1.5 text-[11px] font-mono tabular-nums">
          {diff?.stats && diff.stats.additions > 0 && (
            <span className="text-diff-add">+{diff.stats.additions}</span>
          )}
          {diff?.stats && diff.stats.deletions > 0 && (
            <span className="text-diff-del">&minus;{diff.stats.deletions}</span>
          )}
        </div>
        <button
          onClick={toggleWrap}
          aria-pressed={wrap}
          className={`ml-auto w-5 h-5 flex items-center justify-center cursor-pointer transition-colors ${
            wrap ? 'text-accent' : 'text-text-faint hover:text-text-muted'
          }`}
          title={wrap ? 'Disable line wrapping' : 'Enable line wrapping'}
        >
          <WrapText size={13} />
        </button>
      </div>
      <div className="text-[11px] text-text-faint px-4 py-1 bg-bg-sunken border-b border-surface-raised">
        {file.short_path}
      </div>

      {/* Diff content */}
      <div ref={containerRef} className="flex-1 overflow-y-auto relative" data-role="plan">
        <SelectionToolbar containerRef={containerRef} />
        {loading && (
          <div className="flex items-center gap-2 justify-center py-8 text-[13px] text-text-faint">
            <Loader2 size={14} className="animate-spin" /> Loading diff...
          </div>
        )}
        {error && (
          <div className="px-4 py-4 text-[13px] text-hue-red">Failed to load diff: {error}</div>
        )}
        {diff && !loading && (
          <Suspense
            fallback={
              <div className="flex items-center gap-2 justify-center py-8 text-[13px] text-text-faint">
                <Loader2 size={14} className="animate-spin" /> Loading diff…
              </div>
            }
          >
            <DiffView diff={diff} wrap={wrap} />
          </Suspense>
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
        <div className="flex items-center gap-2 text-[12px] text-text-muted">
          <span>{modifiedFiles.length} file{modifiedFiles.length !== 1 ? 's' : ''}</span>
          {totalAdd > 0 && <span className="text-hue-green font-mono">+{totalAdd}</span>}
          {totalDel > 0 && <span className="text-hue-red font-mono">&minus;{totalDel}</span>}
        </div>
        <button
          onClick={handleRefresh}
          className="w-5 h-5 flex items-center justify-center text-text-faint hover:text-text-muted cursor-pointer transition-colors"
          title="Refresh file list"
        >
          <RefreshCw size={12} className={refreshing ? 'animate-spin' : ''} />
        </button>
      </div>

      {/* File list */}
      <div className="flex-1 overflow-y-auto">
        {modifiedFiles.length === 0 ? (
          <div className="px-4 py-8 text-center text-[13px] text-text-faint">
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
