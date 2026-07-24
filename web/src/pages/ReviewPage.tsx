import { useEffect, useMemo, useState } from 'react';
import { GitBranch, FileText, RefreshCw, X, FolderGit2, MessagesSquare } from 'lucide-react';
import { useReviewStore } from '../stores/reviewStore';
import { ReviewDiff } from '../components/Review/ReviewDiff';

function basename(p: string): string {
  const parts = p.replace(/\/+$/, '').split('/');
  return parts[parts.length - 1] || p;
}

const STATUS_COLOR: Record<string, string> = {
  created: 'text-hue-emerald',
  modified: 'text-hue-yellow',
  deleted: 'text-hue-red',
  renamed: 'text-hue-blue',
};

export function ReviewPage() {
  const s = useReviewStore();
  const [openPath, setOpenPath] = useState('');

  useEffect(() => {
    s.loadRepos();
    s.loadSessions();
    s.loadReviews();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const currentThreads = useMemo(
    () => (s.review?.threads || []).filter(t => t.file_path === s.path),
    [s.review, s.path],
  );

  const sessionOptions = useMemo(() => {
    const opts = [{ id: '', label: 'New review session (auto)' }];
    for (const sess of s.sessions) opts.push({ id: sess.id, label: `${sess.title} (${sess.id})` });
    if (s.target && !s.sessions.find(o => o.id === s.target)) {
      opts.push({ id: s.target, label: `${s.target} (not running)` });
    }
    return opts;
  }, [s.sessions, s.target]);

  return (
    <div className="flex h-full min-h-0">
      {/* Left sidebar */}
      <div className="w-72 shrink-0 border-r border-border flex flex-col min-h-0 bg-surface/40">
        <div className="px-3 py-2 border-b border-border flex items-center gap-2 text-[13px] font-medium text-text">
          <FolderGit2 size={15} /> Code Review
        </div>

        <div className="flex-1 overflow-y-auto">
          {/* Repos / worktrees */}
          <div className="px-3 py-2">
            <div className="text-[11px] uppercase tracking-wide text-text-faint mb-1">Worktrees</div>
            {s.loadingRepos && <div className="text-[12px] text-text-faint">Loading…</div>}
            {!s.loadingRepos && s.repos.length === 0 && (
              <div className="text-[12px] text-text-faint">
                No repositories configured. Set <code>code_review.repos</code> in config.local.yaml.
              </div>
            )}
            {s.repos.map(repo => (
              <div key={repo.resolved} className="mb-2">
                <div className="text-[11px] text-text-muted truncate" title={repo.root}>{repo.root}</div>
                {repo.worktrees.map(wt => (
                  <button
                    key={wt.path}
                    onClick={() => s.selectWorktree(wt.path, wt.branch ?? null)}
                    title={wt.path}
                    className={`w-full text-left px-2 py-1 rounded-md text-[12px] flex items-center gap-1.5 ${
                      s.worktree === wt.path ? 'bg-accent/15 text-accent' : 'text-text-secondary hover:bg-surface-hover'
                    }`}
                  >
                    <GitBranch size={12} className="shrink-0" />
                    <span className="truncate">{wt.branch || basename(wt.path)}</span>
                  </button>
                ))}
              </div>
            ))}
          </div>

          {/* Changed files */}
          {s.worktree && (
            <div className="px-3 py-2 border-t border-border-subtle">
              <div className="flex items-center justify-between mb-1">
                <div className="text-[11px] uppercase tracking-wide text-text-faint">
                  Changes vs
                  <input
                    value={s.base}
                    onChange={e => useReviewStore.setState({ base: e.target.value })}
                    onBlur={e => s.setBase(e.target.value.trim() || 'HEAD')}
                    onKeyDown={e => { if (e.key === 'Enter') s.setBase((e.target as HTMLInputElement).value.trim() || 'HEAD'); }}
                    className="ml-1 w-16 bg-bg border border-border rounded px-1 text-[11px] text-text"
                  />
                </div>
                <button onClick={() => s.selectWorktree(s.worktree!, s.branch)} title="Refresh" className="text-text-faint hover:text-text-muted">
                  <RefreshCw size={12} />
                </button>
              </div>
              {s.loadingChanged && <div className="text-[12px] text-text-faint">Loading…</div>}
              {!s.loadingChanged && s.changed.length === 0 && (
                <div className="text-[12px] text-text-faint">No uncommitted changes.</div>
              )}
              {s.changed.map(f => (
                <button
                  key={f.path}
                  onClick={() => s.selectFile(f.path)}
                  title={f.path}
                  className={`w-full text-left px-2 py-1 rounded-md text-[12px] flex items-center gap-1.5 ${
                    s.path === f.path && s.mode === 'diff' ? 'bg-accent/15 text-accent' : 'text-text-secondary hover:bg-surface-hover'
                  }`}
                >
                  <FileText size={12} className={`shrink-0 ${STATUS_COLOR[f.status] || ''}`} />
                  <span className="truncate flex-1">{f.path}</span>
                  <span className="text-[10px] text-hue-emerald">+{f.additions}</span>
                  <span className="text-[10px] text-hue-red">-{f.deletions}</span>
                </button>
              ))}
              {/* Open an arbitrary file (full-file view) */}
              <div className="mt-2 flex items-center gap-1">
                <input
                  value={openPath}
                  onChange={e => setOpenPath(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Enter' && openPath.trim()) s.openFullFile(openPath.trim()); }}
                  placeholder="open file by path…"
                  className="flex-1 bg-bg border border-border rounded px-1.5 py-0.5 text-[11px] text-text"
                />
                <button
                  onClick={() => openPath.trim() && s.openFullFile(openPath.trim())}
                  className="px-1.5 py-0.5 text-[11px] rounded bg-surface-hover text-text-muted hover:text-text"
                >open</button>
              </div>
            </div>
          )}

          {/* Reviews */}
          <div className="px-3 py-2 border-t border-border-subtle">
            <div className="text-[11px] uppercase tracking-wide text-text-faint mb-1">Reviews</div>
            {s.reviews.length === 0 && <div className="text-[12px] text-text-faint">None yet.</div>}
            {s.reviews.map(r => (
              <button
                key={r.id}
                onClick={() => s.selectReview(r)}
                className={`w-full text-left px-2 py-1 rounded-md text-[12px] ${
                  s.review?.id === r.id ? 'bg-accent/15 text-accent' : 'text-text-secondary hover:bg-surface-hover'
                }`}
              >
                <div className="flex items-center gap-1.5">
                  <MessagesSquare size={12} className="shrink-0" />
                  <span className="truncate flex-1">{r.title || basename(r.worktree)}</span>
                  {!!r.open_thread_count && (
                    <span className="text-[10px] px-1 rounded-full bg-yellow-400/15 text-hue-yellow">{r.open_thread_count}</span>
                  )}
                </div>
                <div className="text-[10px] text-text-faint truncate">
                  {r.created_by === 'agent' ? '🤖 ' : ''}{r.branch || basename(r.worktree)} · {r.status}
                </div>
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Main area */}
      <div className="flex-1 flex flex-col min-h-0">
        {/* Header */}
        <div className="px-4 py-2 border-b border-border flex items-center gap-3 text-[12px] shrink-0">
          <div className="min-w-0 flex-1">
            {s.worktree ? (
              <div className="flex items-center gap-2">
                <GitBranch size={13} className="text-text-faint" />
                <span className="text-text font-medium">{s.branch || basename(s.worktree)}</span>
                {s.path && <span className="text-text-faint truncate">· {s.path}{s.mode === 'file' ? ' (full file)' : ''}</span>}
              </div>
            ) : (
              <span className="text-text-faint">Select a worktree to review its uncommitted changes.</span>
            )}
          </div>
          {s.worktree && (
            <div className="flex items-center gap-1.5 shrink-0">
              <span className="text-text-faint">Route comments to:</span>
              <select
                value={s.target}
                onChange={e => s.setTarget(e.target.value)}
                className="bg-bg border border-border rounded px-1.5 py-0.5 text-[11px] text-text max-w-[220px]"
              >
                {sessionOptions.map(o => <option key={o.id} value={o.id}>{o.label}</option>)}
              </select>
            </div>
          )}
        </div>

        {s.error && (
          <div className="px-4 py-1.5 bg-red-500/10 text-hue-red text-[12px] flex items-center justify-between">
            <span>{s.error}</span>
            <button onClick={s.clearError}><X size={13} /></button>
          </div>
        )}

        {/* Body */}
        <div className="flex-1 overflow-auto min-h-0">
          {!s.path && (
            <div className="h-full flex items-center justify-center text-[13px] text-text-faint">
              {s.worktree ? 'Pick a changed file, or open a file by path.' : 'No file selected.'}
            </div>
          )}
          {s.path && s.loadingDiff && (
            <div className="px-4 py-8 text-center text-[13px] text-text-faint">Loading…</div>
          )}
          {s.path && !s.loadingDiff && (
            s.mode === 'file' && s.fileContent?.binary ? (
              <div className="px-4 py-8 text-center text-[13px] text-text-faint">Binary file — not shown.</div>
            ) : s.mode === 'file' && s.fileContent?.too_large ? (
              <div className="px-4 py-8 text-center text-[13px] text-text-faint">File too large to display.</div>
            ) : (
              <ReviewDiff
                filePath={s.path}
                diff={s.diff}
                fileContent={s.mode === 'file' ? (s.fileContent?.content ?? null) : null}
                mode={s.mode}
                threads={currentThreads}
                busy={s.busy}
                onAddComment={s.addComment}
                onReply={s.reply}
                onResolve={s.resolve}
              />
            )
          )}
        </div>
      </div>
    </div>
  );
}
