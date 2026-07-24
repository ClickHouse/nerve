import { useMemo, useState } from 'react';
import { MessageSquarePlus, Check, CornerDownRight, Bot, User } from 'lucide-react';
import type { FileDiff } from '../../types/chat';
import type { ReviewThread } from '../../types/review';
import type { CommentAnchor } from '../../stores/reviewStore';

type RowType = 'addition' | 'deletion' | 'context' | 'info';

interface Row {
  key: string;
  kind: 'hunk' | 'line';
  type?: RowType;
  oldLine?: number | null;
  newLine?: number | null;
  content: string;
  header?: string;
}

function buildRowsFromDiff(diff: FileDiff): Row[] {
  const rows: Row[] = [];
  diff.hunks.forEach((h, hi) => {
    rows.push({ key: `h${hi}`, kind: 'hunk', content: '', header: h.header || `@@ -${h.old_start} +${h.new_start} @@` });
    h.lines.forEach((ln: any, li) => {
      rows.push({
        key: `h${hi}l${li}`,
        kind: 'line',
        type: ln.type,
        oldLine: ln.old_line ?? null,
        newLine: ln.new_line ?? null,
        content: ln.content ?? '',
      });
    });
  });
  return rows;
}

function buildRowsFromFile(content: string): Row[] {
  return content.split('\n').map((line, i) => ({
    key: `f${i}`,
    kind: 'line' as const,
    type: 'context' as RowType,
    newLine: i + 1,
    content: line,
  }));
}

function anchorFor(row: Row, filePath: string): CommentAnchor {
  const side: 'new' | 'old' = row.type === 'deletion' ? 'old' : 'new';
  const line = side === 'old' ? row.oldLine ?? null : row.newLine ?? null;
  return { file_path: filePath, side, line_start: line, line_end: line, anchor_snippet: row.content };
}

const AUTHOR_STYLE: Record<string, string> = {
  human: 'text-hue-blue',
  agent: 'text-hue-emerald',
};

function ThreadBlock({
  thread, busy, onReply, onResolve,
}: {
  thread: ReviewThread;
  busy: boolean;
  onReply: (threadId: string, body: string) => void;
  onResolve: (threadId: string) => void;
}) {
  const [reply, setReply] = useState('');
  const resolved = thread.status === 'resolved';
  return (
    <div className="ml-16 my-1.5 border-l-2 border-accent/40 bg-surface rounded-r-md">
      <div className="px-3 py-1.5 flex items-center gap-2 text-[11px] text-text-faint border-b border-border-subtle">
        <span>thread on {thread.side === 'old' ? 'old' : 'new'} line {thread.line_start ?? '?'}</span>
        <span className={`px-1.5 py-0.5 rounded-full border text-[10px] ${
          resolved ? 'border-border-subtle text-text-muted'
          : thread.status === 'answered' ? 'border-emerald-400/30 text-hue-emerald'
          : 'border-yellow-400/30 text-hue-yellow'}`}>
          {thread.status}
        </span>
      </div>
      <div className="px-3 py-2 space-y-2">
        {(thread.comments || []).map(c => (
          <div key={c.id} className="text-[12px]">
            <div className="flex items-center gap-1.5 mb-0.5">
              {c.author === 'agent' ? <Bot size={12} className="text-hue-emerald" /> : <User size={12} className="text-hue-blue" />}
              <span className={`font-medium ${AUTHOR_STYLE[c.author] || ''}`}>{c.author}</span>
              <span className="text-text-faint text-[10px]">{c.created_at?.slice(0, 16).replace('T', ' ')}</span>
            </div>
            <div className="whitespace-pre-wrap text-text-secondary pl-4">{c.body}</div>
          </div>
        ))}
      </div>
      {!resolved && (
        <div className="px-3 pb-2 flex items-end gap-2">
          <textarea
            value={reply}
            onChange={e => setReply(e.target.value)}
            placeholder="Reply…"
            rows={1}
            className="flex-1 bg-bg border border-border rounded-md px-2 py-1 text-[12px] text-text resize-y focus:outline-none focus:border-accent"
          />
          <button
            disabled={busy || !reply.trim()}
            onClick={() => { onReply(thread.id, reply.trim()); setReply(''); }}
            className="px-2 py-1 text-[11px] rounded-md bg-accent/15 text-accent hover:bg-accent/25 disabled:opacity-40 flex items-center gap-1"
          >
            <CornerDownRight size={12} /> Reply
          </button>
          <button
            disabled={busy}
            onClick={() => onResolve(thread.id)}
            title="Resolve thread"
            className="px-2 py-1 text-[11px] rounded-md text-text-muted hover:text-hue-emerald hover:bg-surface-hover disabled:opacity-40 flex items-center gap-1"
          >
            <Check size={12} /> Resolve
          </button>
        </div>
      )}
    </div>
  );
}

export function ReviewDiff({
  filePath, diff, fileContent, mode, threads, busy, onAddComment, onReply, onResolve,
}: {
  filePath: string;
  diff: FileDiff | null;
  fileContent: string | null;
  mode: 'diff' | 'file';
  threads: ReviewThread[];
  busy: boolean;
  onAddComment: (anchor: CommentAnchor, body: string) => void;
  onReply: (threadId: string, body: string) => void;
  onResolve: (threadId: string) => void;
}) {
  const [composingKey, setComposingKey] = useState<string | null>(null);
  const [draft, setDraft] = useState('');

  const rows = useMemo(() => {
    if (mode === 'file') return fileContent != null ? buildRowsFromFile(fileContent) : [];
    return diff ? buildRowsFromDiff(diff) : [];
  }, [mode, diff, fileContent]);

  // Group threads by "side:line" so each anchored line can render its threads.
  const { byKey, unanchored } = useMemo(() => {
    const byKey = new Map<string, ReviewThread[]>();
    for (const t of threads) {
      if (t.line_start == null) continue;
      const k = `${t.side}:${t.line_start}`;
      (byKey.get(k) ?? byKey.set(k, []).get(k)!).push(t);
    }
    return { byKey, unanchored: byKey };
  }, [threads]);

  const rendered = new Set<string>();

  const startCompose = (key: string) => { setComposingKey(key); setDraft(''); };
  const submit = (row: Row) => {
    const body = draft.trim();
    if (!body) return;
    onAddComment(anchorFor(row, filePath), body);
    setComposingKey(null);
    setDraft('');
  };

  if (!rows.length) {
    return <div className="px-4 py-8 text-center text-[13px] text-text-faint">No changes to show.</div>;
  }

  return (
    <div className="font-mono text-[12.5px] leading-[1.5]">
      {rows.map(row => {
        if (row.kind === 'hunk') {
          return (
            <div key={row.key} className="px-3 py-0.5 bg-surface text-text-faint text-[11px] border-y border-border-subtle select-none">
              {row.header}
            </div>
          );
        }
        const isAdd = row.type === 'addition';
        const isDel = row.type === 'deletion';
        const isInfo = row.type === 'info';
        const side: 'new' | 'old' = isDel ? 'old' : 'new';
        const lineNo = side === 'old' ? row.oldLine : row.newLine;
        const anchorKey = lineNo != null ? `${side}:${lineNo}` : null;
        const rowThreads = anchorKey ? byKey.get(anchorKey) : undefined;
        if (anchorKey && rowThreads) rendered.add(anchorKey);
        const canComment = !isInfo && lineNo != null;
        const marker = isAdd ? '+' : isDel ? '-' : ' ';
        return (
          <div key={row.key}>
            <div
              className={`group flex items-start ${
                isAdd ? 'bg-emerald-500/10' : isDel ? 'bg-red-500/10' : ''
              } hover:bg-surface-hover/60`}
            >
              <span className="w-10 shrink-0 px-1 text-right text-text-faint select-none">{row.oldLine ?? ''}</span>
              <span className="w-10 shrink-0 px-1 text-right text-text-faint select-none">{row.newLine ?? ''}</span>
              <span className="w-5 shrink-0 text-center select-none opacity-60">
                {canComment && (
                  <button
                    onClick={() => startCompose(row.key)}
                    title="Comment on this line"
                    className="opacity-0 group-hover:opacity-100 text-accent hover:text-accent transition-opacity cursor-pointer"
                  >
                    <MessageSquarePlus size={12} />
                  </button>
                )}
              </span>
              <span className={`w-4 shrink-0 select-none ${isAdd ? 'text-hue-emerald' : isDel ? 'text-hue-red' : 'text-text-faint'}`}>{marker}</span>
              <span className="flex-1 whitespace-pre-wrap break-all pr-3 text-text-secondary">{row.content || ' '}</span>
            </div>

            {composingKey === row.key && (
              <div className="ml-16 my-1.5 border-l-2 border-accent/40 bg-surface rounded-r-md p-2 font-sans">
                <textarea
                  autoFocus
                  value={draft}
                  onChange={e => setDraft(e.target.value)}
                  placeholder={`Comment on ${side} line ${lineNo}… (routes to the session)`}
                  rows={2}
                  className="w-full bg-bg border border-border rounded-md px-2 py-1 text-[12px] text-text resize-y focus:outline-none focus:border-accent"
                />
                <div className="flex justify-end gap-2 mt-1.5">
                  <button onClick={() => setComposingKey(null)} className="px-2 py-1 text-[11px] rounded-md text-text-muted hover:bg-surface-hover">Cancel</button>
                  <button
                    disabled={busy || !draft.trim()}
                    onClick={() => submit(row)}
                    className="px-2.5 py-1 text-[11px] rounded-md bg-accent/15 text-accent hover:bg-accent/25 disabled:opacity-40"
                  >
                    Comment
                  </button>
                </div>
              </div>
            )}

            {rowThreads?.map(t => (
              <ThreadBlock key={t.id} thread={t} busy={busy} onReply={onReply} onResolve={onResolve} />
            ))}
          </div>
        );
      })}

      {/* Threads whose anchor line isn't visible in the current view (e.g. diff
          context shifted, or a whole-file comment on a line outside the diff). */}
      {(() => {
        const orphans: ReviewThread[] = [];
        for (const [k, ts] of unanchored.entries()) {
          if (!rendered.has(k)) orphans.push(...ts);
        }
        if (!orphans.length) return null;
        return (
          <div className="mt-3 pt-2 border-t border-border-subtle font-sans">
            <div className="px-3 text-[11px] text-text-faint mb-1">Other threads on this file</div>
            {orphans.map(t => (
              <ThreadBlock key={t.id} thread={t} busy={busy} onReply={onReply} onResolve={onResolve} />
            ))}
          </div>
        );
      })()}
    </div>
  );
}
