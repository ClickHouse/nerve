import { useState, useEffect, useRef, lazy, Suspense } from 'react';
import {
  ArrowLeft, Eye, FilePlus, FileEdit, FileX, Loader2, RefreshCw, WrapText,
  Plus, GitBranch, Trash2, X, MessageSquare, FileText,
} from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';
import { useReviewStore } from '../../stores/reviewStore';
import { api } from '../../api/client';
import { SelectionToolbar } from './SelectionToolbar';
import { MarkdownContent } from './MarkdownContent';
import { MAX_DIFF_LINES } from '../../types/chat';
import type { FileDiff, ModifiedFileSummary } from '../../types/chat';
import type { ReviewChangedFile } from '../../types/review';
import { ReviewDiff } from '../Review/ReviewDiff';

// The diff renderer pulls in @pierre/diffs + Shiki — only loaded when a file
// diff is actually opened, keeping it off the initial bundle.
const DiffView = lazy(() => import('./DiffView').then((m) => ({ default: m.DiffView })));

// ------------------------------------------------------------------ //
//  Shared bits                                                         //
// ------------------------------------------------------------------ //

const STATUS_ICON: Record<string, typeof FileEdit> = {
  created: FilePlus,
  modified: FileEdit,
  deleted: FileX,
  renamed: FileEdit,
};

const STATUS_COLOR: Record<string, string> = {
  created: 'text-diff-add',
  modified: 'text-warning',
  deleted: 'text-diff-del',
  renamed: 'text-hue-blue',
};

const STATUS_BADGE: Record<string, string> = {
  created: '+',
  modified: 'M',
  deleted: 'D',
  renamed: 'R',
};

function splitPath(shortPath: string): { fileName: string; dirPath: string } {
  const parts = shortPath.split('/');
  const fileName = parts.pop() || shortPath;
  const dirPath = parts.join('/');
  return { fileName, dirPath };
}

function basename(p: string): string {
  const parts = p.replace(/\/+$/, '').split('/');
  return parts[parts.length - 1] || p;
}

// ------------------------------------------------------------------ //
//  Snapshot "session edits" list item + detail (existing behavior)     //
// ------------------------------------------------------------------ //

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
        <span className={`text-[11px] font-bold font-mono w-4 text-center shrink-0 ${color}`}>{badge}</span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <Icon size={13} className={`shrink-0 ${color}`} />
            <span className="text-[13px] font-medium text-text-secondary truncate">{fileName}</span>
          </div>
          {dirPath && <div className="text-[11px] text-text-faint truncate ml-[21px]">{dirPath}</div>}
        </div>
        <div className="flex items-center gap-1.5 shrink-0 text-[11px] font-mono tabular-nums">
          {file.stats.additions > 0 && <span className="text-diff-add">+{file.stats.additions}</span>}
          {file.stats.deletions > 0 && <span className="text-diff-del">&minus;{file.stats.deletions}</span>}
        </div>
      </div>
    </button>
  );
}

const WRAP_STORAGE_KEY = 'nerve_diff_wrap';

function FileDetailView({ file, onBack }: { file: ModifiedFileSummary; onBack: () => void }) {
  const activeSession = useChatStore(s => s.activeSession);
  const [diff, setDiff] = useState<FileDiff | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [wrap, setWrap] = useState(() => localStorage.getItem(WRAP_STORAGE_KEY) === 'true');
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

  const { fileName } = splitPath(file.short_path);
  const color = STATUS_COLOR[file.status] || 'text-text-muted';

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-border-subtle bg-bg-sunken shrink-0">
        <button onClick={onBack} className="w-5 h-5 flex items-center justify-center text-text-faint hover:text-text-muted cursor-pointer">
          <ArrowLeft size={14} />
        </button>
        <span className={`text-[13px] font-medium ${color}`}>{fileName}</span>
        <div className="flex items-center gap-1.5 text-[11px] font-mono tabular-nums">
          {diff?.stats && diff.stats.additions > 0 && <span className="text-diff-add">+{diff.stats.additions}</span>}
          {diff?.stats && diff.stats.deletions > 0 && <span className="text-diff-del">&minus;{diff.stats.deletions}</span>}
        </div>
        <div className="ml-auto flex items-center gap-1">
          {diff?.markdown_content != null && (
            <button onClick={() => setPreview(p => !p)} aria-pressed={preview}
              className={`w-5 h-5 flex items-center justify-center cursor-pointer ${preview ? 'text-accent' : 'text-text-faint hover:text-text-muted'}`}
              title={preview ? 'Show raw diff' : 'Show rendered markdown'}>
              <Eye size={13} />
            </button>
          )}
          <button onClick={toggleWrap} aria-pressed={wrap}
            className={`w-5 h-5 flex items-center justify-center cursor-pointer ${wrap ? 'text-accent' : 'text-text-faint hover:text-text-muted'}`}
            title={wrap ? 'Disable line wrapping' : 'Enable line wrapping'}>
            <WrapText size={13} />
          </button>
        </div>
      </div>
      <div className="text-[11px] text-text-faint px-4 py-1 bg-bg-sunken border-b border-surface-raised">{file.short_path}</div>
      <div ref={containerRef} className="flex-1 overflow-y-auto relative" data-role="plan">
        <SelectionToolbar containerRef={containerRef} />
        {loading && <div className="flex items-center gap-2 justify-center py-8 text-[13px] text-text-faint"><Loader2 size={14} className="animate-spin" /> Loading diff...</div>}
        {error && <div className="px-4 py-4 text-[13px] text-hue-red">Failed to load diff: {error}</div>}
        {diff && !loading && (
          preview && diff.markdown_content != null ? (
            <div className="px-4 py-3 text-[13px]">
              <MarkdownContent content={diff.markdown_content} />
              {diff.markdown_truncated && <div className="text-center py-3 mt-3 text-[11px] text-text-faint border-t border-border-subtle">Preview truncated at {MAX_DIFF_LINES} lines</div>}
            </div>
          ) : (
            <Suspense fallback={<div className="flex items-center gap-2 justify-center py-8 text-[13px] text-text-faint"><Loader2 size={14} className="animate-spin" /> Loading diff…</div>}>
              <DiffView diff={diff} wrap={wrap} />
            </Suspense>
          )
        )}
      </div>
    </div>
  );
}

// ------------------------------------------------------------------ //
//  Attach-worktree picker                                              //
// ------------------------------------------------------------------ //

function AttachPicker({ onClose }: { onClose: () => void }) {
  const repos = useReviewStore(s => s.repos);
  const loadRepos = useReviewStore(s => s.loadRepos);
  const attach = useReviewStore(s => s.attach);
  const busy = useReviewStore(s => s.busy);
  const [sel, setSel] = useState('');       // "worktreePath branch"
  const [base, setBase] = useState('HEAD');

  useEffect(() => { loadRepos(); }, [loadRepos]);

  const options: { path: string; branch: string | null; repo: string }[] = [];
  for (const r of repos) for (const wt of r.worktrees) options.push({ path: wt.path, branch: wt.branch ?? null, repo: r.root });

  const doAttach = () => {
    if (!sel) return;
    const [path, branch] = sel.split(' ');
    attach(path, branch || null, base.trim() || 'HEAD').then(onClose);
  };

  return (
    <div className="px-4 py-2.5 border-b border-surface-raised bg-bg-sunken space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-[11px] uppercase tracking-wide text-text-faint">Attach a worktree</span>
        <button onClick={onClose} className="text-text-faint hover:text-text-muted"><X size={13} /></button>
      </div>
      <select value={sel} onChange={e => setSel(e.target.value)}
        className="w-full bg-bg border border-border rounded px-2 py-1 text-[12px] text-text">
        <option value="">{repos.length ? 'Select a worktree…' : 'Loading repos…'}</option>
        {options.map(o => (
          <option key={o.path} value={`${o.path} ${o.branch ?? ''}`}>
            {o.branch || basename(o.path)} — {o.repo}
          </option>
        ))}
      </select>
      <div className="flex items-center gap-2">
        <label className="text-[11px] text-text-faint">vs</label>
        <input value={base} onChange={e => setBase(e.target.value)}
          className="w-24 bg-bg border border-border rounded px-1.5 py-0.5 text-[12px] text-text" />
        <button disabled={!sel || busy} onClick={doAttach}
          className="ml-auto px-2.5 py-1 text-[11px] rounded-md bg-accent/15 text-accent hover:bg-accent/25 disabled:opacity-40">
          Attach
        </button>
      </div>
    </div>
  );
}

// ------------------------------------------------------------------ //
//  Attached-worktree views (git diff + line comments)                  //
// ------------------------------------------------------------------ //

function ChangedFileRow({ file, onClick }: { file: ReviewChangedFile; onClick: () => void }) {
  const { fileName, dirPath } = splitPath(file.path);
  const color = STATUS_COLOR[file.status] || 'text-text-muted';
  const badge = STATUS_BADGE[file.status] || '?';
  return (
    <button onClick={onClick}
      className="w-full text-left px-4 py-2 hover:bg-surface transition-colors cursor-pointer border-b border-surface-raised last:border-b-0">
      <div className="flex items-center gap-2.5">
        <span className={`text-[11px] font-bold font-mono w-4 text-center shrink-0 ${color}`}>{badge}</span>
        <div className="flex-1 min-w-0">
          <div className="text-[13px] font-medium text-text-secondary truncate">{fileName}</div>
          {dirPath && <div className="text-[11px] text-text-faint truncate">{dirPath}</div>}
        </div>
        <div className="flex items-center gap-1.5 shrink-0 text-[11px] font-mono tabular-nums">
          {file.additions > 0 && <span className="text-diff-add">+{file.additions}</span>}
          {file.deletions > 0 && <span className="text-diff-del">&minus;{file.deletions}</span>}
        </div>
      </div>
    </button>
  );
}

// The active worktree's changed-files list (+ header with base + refresh).
// "Submit review (N)" — batches all staged (pending) comments + an optional
// overall summary and delivers them to the session as ONE turn. Shown only
// while there are pending comments on the active review.
function SubmitReviewBar() {
  const activeReview = useReviewStore(s => s.activeReview);
  const submitReview = useReviewStore(s => s.submitReview);
  const busy = useReviewStore(s => s.busy);
  const [open, setOpen] = useState(false);
  const [summary, setSummary] = useState('');
  const pending = (activeReview?.threads || []).reduce(
    (n, t) => n + (t.comments || []).filter(c => c.pending).length, 0);
  if (!activeReview || pending === 0) return null;
  const submit = () => { void submitReview(summary).then(() => { setSummary(''); setOpen(false); }); };
  return (
    <div className="px-4 py-2 border-b border-border-subtle bg-yellow-400/5 shrink-0">
      {!open ? (
        <button onClick={() => setOpen(true)}
          className="w-full text-[12px] px-2 py-1 rounded-md bg-accent/15 text-accent hover:bg-accent/25 cursor-pointer">
          Submit review ({pending} pending comment{pending !== 1 ? 's' : ''})
        </button>
      ) : (
        <div className="space-y-1.5">
          <textarea value={summary} onChange={e => setSummary(e.target.value)} rows={2}
            placeholder="Overall summary (optional)…"
            className="w-full bg-bg border border-border rounded-md px-2 py-1 text-[12px] text-text resize-y focus:outline-none focus:border-accent" />
          <div className="flex items-center justify-between gap-2">
            <span className="text-[10px] text-text-faint">Sends all {pending} pending + summary as one turn.</span>
            <div className="flex gap-2 shrink-0">
              <button onClick={() => setOpen(false)} className="px-2 py-1 text-[11px] rounded-md text-text-muted hover:bg-surface-hover cursor-pointer">Cancel</button>
              <button disabled={busy} onClick={submit}
                className="px-2.5 py-1 text-[11px] rounded-md bg-accent/15 text-accent hover:bg-accent/25 disabled:opacity-40 cursor-pointer">Send</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function ReviewWorktreeView() {
  const r = useReviewStore(s => s.activeReview)!;
  const changed = useReviewStore(s => s.changed);
  const loading = useReviewStore(s => s.loadingChanged);
  const closeReview = useReviewStore(s => s.closeReview);
  const selectFile = useReviewStore(s => s.selectFile);
  const selectReview = useReviewStore(s => s.selectReview);

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-border-subtle bg-bg-sunken shrink-0">
        <button onClick={closeReview} className="w-5 h-5 flex items-center justify-center text-text-faint hover:text-text-muted cursor-pointer">
          <ArrowLeft size={14} />
        </button>
        <GitBranch size={13} className="text-text-faint shrink-0" />
        <span className="text-[13px] font-medium text-text truncate">{r.branch || basename(r.worktree)}</span>
        <span className="text-[11px] text-text-faint">vs {r.base_ref}</span>
        <button onClick={() => selectReview(r.id)} title="Refresh" className="ml-auto text-text-faint hover:text-text-muted">
          <RefreshCw size={12} />
        </button>
      </div>
      <div className="text-[11px] text-text-faint px-4 py-1 bg-bg-sunken border-b border-surface-raised truncate">{r.worktree}</div>
      <SubmitReviewBar />
      <div className="flex-1 overflow-y-auto">
        {loading && <div className="flex items-center gap-2 justify-center py-8 text-[13px] text-text-faint"><Loader2 size={14} className="animate-spin" /> Loading changes…</div>}
        {!loading && changed.length === 0 && <div className="px-4 py-8 text-center text-[13px] text-text-faint">No changes vs {r.base_ref}.</div>}
        {changed.map(f => <ChangedFileRow key={f.path} file={f} onClick={() => selectFile(f.path)} />)}
      </div>
    </div>
  );
}

// A single file's git diff with inline, line-anchored comment threads.
function ReviewFileDetail() {
  const r = useReviewStore(s => s.activeReview)!;
  const path = useReviewStore(s => s.path)!;
  const mode = useReviewStore(s => s.mode);
  const diff = useReviewStore(s => s.diff);
  const fileContent = useReviewStore(s => s.fileContent);
  const loading = useReviewStore(s => s.loadingDiff);
  const busy = useReviewStore(s => s.busy);
  const backToFiles = useReviewStore(s => s.backToFiles);
  const openFullFile = useReviewStore(s => s.openFullFile);
  const selectFile = useReviewStore(s => s.selectFile);
  const addComment = useReviewStore(s => s.addComment);
  const reply = useReviewStore(s => s.reply);
  const resolve = useReviewStore(s => s.resolve);

  const threads = (r.threads || []).filter(t => t.file_path === path);

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-border-subtle bg-bg-sunken shrink-0">
        <button onClick={backToFiles} className="w-5 h-5 flex items-center justify-center text-text-faint hover:text-text-muted cursor-pointer">
          <ArrowLeft size={14} />
        </button>
        <span className="text-[13px] font-medium text-text-secondary truncate">{basename(path)}</span>
        <div className="ml-auto flex items-center gap-1">
          <button onClick={() => (mode === 'file' ? selectFile(path) : openFullFile(path))}
            className={`px-1.5 h-5 flex items-center gap-1 rounded text-[11px] cursor-pointer ${mode === 'file' ? 'text-accent' : 'text-text-faint hover:text-text-muted'}`}
            title={mode === 'file' ? 'Show diff' : 'Show whole file'}>
            <FileText size={12} />{mode === 'file' ? 'diff' : 'file'}
          </button>
        </div>
      </div>
      <div className="text-[11px] text-text-faint px-4 py-1 bg-bg-sunken border-b border-surface-raised truncate">{path}</div>
      <SubmitReviewBar />
      <div className="flex-1 overflow-y-auto">
        {loading && <div className="flex items-center gap-2 justify-center py-8 text-[13px] text-text-faint"><Loader2 size={14} className="animate-spin" /> Loading…</div>}
        {!loading && mode === 'file' && fileContent?.binary && <div className="px-4 py-8 text-center text-[13px] text-text-faint">Binary file — not shown.</div>}
        {!loading && mode === 'file' && fileContent?.too_large && <div className="px-4 py-8 text-center text-[13px] text-text-faint">File too large to display.</div>}
        {!loading && !(mode === 'file' && (fileContent?.binary || fileContent?.too_large)) && (
          <ReviewDiff
            filePath={path}
            diff={diff}
            fileContent={mode === 'file' ? (fileContent?.content ?? null) : null}
            mode={mode}
            threads={threads}
            busy={busy}
            onAddComment={addComment}
            onReply={reply}
            onResolve={resolve}
          />
        )}
      </div>
    </div>
  );
}

// ------------------------------------------------------------------ //
//  Main panel                                                          //
// ------------------------------------------------------------------ //

export function FileChangesPanel() {
  const activeSession = useChatStore(s => s.activeSession);
  const modifiedFiles = useChatStore(s => s.modifiedFiles);
  const fetchModifiedFiles = useChatStore(s => s.fetchModifiedFiles);

  const setSession = useReviewStore(s => s.setSession);
  const attached = useReviewStore(s => s.attached);
  const activeReview = useReviewStore(s => s.activeReview);
  const reviewPath = useReviewStore(s => s.path);
  const selectReview = useReviewStore(s => s.selectReview);
  const detach = useReviewStore(s => s.detach);
  const error = useReviewStore(s => s.error);
  const clearError = useReviewStore(s => s.clearError);

  const [selectedSnapshot, setSelectedSnapshot] = useState<ModifiedFileSummary | null>(null);
  const [showAttach, setShowAttach] = useState(false);
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => { if (activeSession) setSession(activeSession); }, [activeSession, setSession]);
  useEffect(() => { setSelectedSnapshot(null); }, [activeSession]);

  // --- Detail views take over the whole panel ---
  if (selectedSnapshot) {
    return <FileDetailView file={selectedSnapshot} onBack={() => setSelectedSnapshot(null)} />;
  }
  if (activeReview && reviewPath) return <ReviewFileDetail />;
  if (activeReview) return <ReviewWorktreeView />;

  // --- List view: session edits + attached worktrees ---
  const totalAdd = modifiedFiles.reduce((s, f) => s + f.stats.additions, 0);
  const totalDel = modifiedFiles.reduce((s, f) => s + f.stats.deletions, 0);

  const handleRefresh = async () => {
    setRefreshing(true);
    await fetchModifiedFiles(activeSession);
    setRefreshing(false);
  };

  return (
    <div className="flex flex-col h-full">
      {error && (
        <div className="px-4 py-1.5 bg-red-500/10 text-hue-red text-[12px] flex items-center justify-between shrink-0">
          <span className="truncate">{error}</span>
          <button onClick={clearError}><X size={13} /></button>
        </div>
      )}

      <div className="flex-1 overflow-y-auto">
        {/* Session edits (snapshot-based) */}
        <div className="flex items-center justify-between px-4 py-2 border-b border-border-subtle bg-bg-sunken">
          <div className="flex items-center gap-2 text-[12px] text-text-muted">
            <span>{modifiedFiles.length} edit{modifiedFiles.length !== 1 ? 's' : ''} this session</span>
            {totalAdd > 0 && <span className="text-hue-green font-mono">+{totalAdd}</span>}
            {totalDel > 0 && <span className="text-hue-red font-mono">&minus;{totalDel}</span>}
          </div>
          <button onClick={handleRefresh} className="w-5 h-5 flex items-center justify-center text-text-faint hover:text-text-muted cursor-pointer" title="Refresh">
            <RefreshCw size={12} className={refreshing ? 'animate-spin' : ''} />
          </button>
        </div>
        {modifiedFiles.map(file => (
          <FileCard key={file.path} file={file} onClick={() => setSelectedSnapshot(file)} />
        ))}

        {/* Attached worktrees (git diffs + comments) */}
        <div className="flex items-center justify-between px-4 py-2 border-y border-border-subtle bg-bg-sunken mt-1">
          <span className="text-[12px] text-text-muted">Worktrees</span>
          <button onClick={() => setShowAttach(v => !v)}
            className="flex items-center gap-1 text-[11px] text-accent hover:text-accent cursor-pointer" title="Attach a git worktree to review">
            <Plus size={13} /> Attach
          </button>
        </div>
        {showAttach && <AttachPicker onClose={() => setShowAttach(false)} />}
        {attached.length === 0 && !showAttach && (
          <div className="px-4 py-4 text-[12px] text-text-faint">
            No worktrees attached. Use <span className="text-accent">Attach</span> to review a repo's changes here and leave line comments.
          </div>
        )}
        {attached.map(rev => (
          <div key={rev.id} className="flex items-center gap-2 px-4 py-2 hover:bg-surface border-b border-surface-raised group">
            <button onClick={() => selectReview(rev.id)} className="flex items-center gap-2 flex-1 min-w-0 text-left cursor-pointer">
              <GitBranch size={13} className="text-text-faint shrink-0" />
              <span className="text-[13px] text-text-secondary truncate">
                {rev.created_by === 'agent' ? '🤖 ' : ''}{rev.title || rev.branch || basename(rev.worktree)}
              </span>
              {!!rev.open_thread_count && (
                <span className="flex items-center gap-0.5 text-[10px] px-1 rounded-full bg-yellow-400/15 text-hue-yellow shrink-0">
                  <MessageSquare size={9} />{rev.open_thread_count}
                </span>
              )}
            </button>
            <button onClick={() => detach(rev.id)} title="Detach"
              className="opacity-0 group-hover:opacity-100 text-text-faint hover:text-hue-red cursor-pointer">
              <Trash2 size={12} />
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
