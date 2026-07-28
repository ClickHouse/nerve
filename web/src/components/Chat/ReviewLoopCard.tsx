import { useCallback, useEffect, useRef, useState } from 'react';
import {
  Repeat, ChevronDown, ChevronRight, Hammer, SearchCheck, Eye,
  CircleCheck, CircleX, CircleHelp, Circle, Loader2, Square, ExternalLink,
  FileText,
} from 'lucide-react';
import { Link } from 'react-router-dom';
import { api } from '../../api/client';
import type { ReviewLoop, ReviewLoopAttempt } from '../../api/client';
import { useChatStore } from '../../stores/chatStore';
import { useWorkflowRunStore, isActiveRun } from '../../stores/workflowRunStore';

const LIVE = new Set(['pending', 'implementing', 'verifying']);

function statusLabel(loop: ReviewLoop): string {
  switch (loop.status) {
    case 'implementing': return `iteration ${loop.iteration}/${loop.max_iterations} — implementing`;
    case 'verifying': return `iteration ${loop.iteration}/${loop.max_iterations} — verifying`;
    case 'awaiting_user': return `needs your decision${loop.failure_reason ? ` · ${loop.failure_reason.replace(/_/g, ' ')}` : ''}`;
    case 'passed': return `passed on iteration ${loop.iteration}`;
    case 'failed': return `failed${loop.failure_reason ? ` · ${loop.failure_reason.replace(/_/g, ' ')}` : ''}`;
    case 'killed': return 'killed';
    default: return loop.status;
  }
}

function statusTone(status: string): string {
  switch (status) {
    case 'passed': return 'text-emerald-400';
    case 'awaiting_user': return 'text-orange-400';
    case 'failed': case 'killed': return 'text-red-400';
    default: return 'text-emerald-400';
  }
}

function CriterionIcon({ status }: { status: string }) {
  switch (status) {
    case 'met': return <CircleCheck size={13} className="text-emerald-400 shrink-0" />;
    case 'unmet': return <CircleX size={13} className="text-red-400 shrink-0" />;
    case 'unverifiable': return <CircleHelp size={13} className="text-amber-400 shrink-0" />;
    default: return <Circle size={13} className="text-text-faint shrink-0" />;
  }
}

function fmtUsd(v: number | null | undefined): string {
  return `$${(v ?? 0).toFixed(2)}`;
}

/**
 * Sticky loop dashboard pinned above the transcript of an observer session.
 * Collapsed: one status strip (pulse · iteration · criteria met · spend bar).
 * Expanded: live criteria checklist, attempt timeline with "watch the leg
 * working" jumps (legs are real sessions streaming in real time), inline
 * decision buttons when the loop parks, kill while it runs.
 */
export function ReviewLoopCard({ loopId }: { loopId: string }) {
  const [loop, setLoop] = useState<ReviewLoop | null>(null);
  const [attempts, setAttempts] = useState<ReviewLoopAttempt[]>([]);
  const [expanded, setExpanded] = useState(false);
  const [acting, setActing] = useState<string | null>(null);
  const [budgetAdd, setBudgetAdd] = useState('');
  const [error, setError] = useState<string | null>(null);
  const fetching = useRef(false);
  const autoExpanded = useRef(false);

  const switchSession = useChatStore(s => s.switchSession);
  const openPanelTab = useChatStore(s => s.openPanelTab);
  const updatePanelTab = useChatStore(s => s.updatePanelTab);
  // Chip state on the session row is WS-fed — use it as the refetch signal.
  const chip = useChatStore(
    s => s.sessions.find(sess => sess.review_loop?.id === loopId)?.review_loop,
  );
  const runs = useWorkflowRunStore(s => s.runs);
  const loadRuns = useWorkflowRunStore(s => s.loadRuns);

  const refresh = useCallback(async () => {
    if (fetching.current) return;
    fetching.current = true;
    try {
      const data = await api.getReviewLoop(loopId);
      setLoop(data.loop);
      setAttempts(data.attempts ?? []);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      fetching.current = false;
    }
  }, [loopId]);

  // Fetch on mount + whenever the loop advances (WS chip changes).
  useEffect(() => { void refresh(); }, [refresh, chip?.status, chip?.iteration, chip?.updated_at]);

  // 15s poll fallback while live (dropped-socket safety; guard prevents stacking).
  useEffect(() => {
    if (!loop || !LIVE.has(loop.status)) return;
    const t = setInterval(() => void refresh(), 15_000);
    return () => clearInterval(t);
  }, [loop?.status, refresh]);  // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-expand once when a decision is needed.
  useEffect(() => {
    if (loop?.status === 'awaiting_user' && !autoExpanded.current) {
      autoExpanded.current = true;
      setExpanded(true);
    }
  }, [loop?.status]);

  // Live leg: current run's spend/status streams via workflow_run_update.
  const currentRun = loop?.current_run_id
    ? runs.find(r => r.id === loop.current_run_id)
    : undefined;
  useEffect(() => {
    if (loop && LIVE.has(loop.status) && loop.current_run_id && !currentRun) {
      void loadRuns();
    }
  }, [loop?.current_run_id, loop?.status]);  // eslint-disable-line react-hooks/exhaustive-deps

  if (!loop) return null;

  const live = LIVE.has(loop.status);
  const met = loop.criteria.filter(c => c.last_status === 'met').length;
  const liveSpend = currentRun && isActiveRun(currentRun) ? currentRun.spent_usd : 0;
  const spent = Math.max(loop.spent_usd, loop.spent_usd + liveSpend);
  const frac = loop.budget_usd > 0 ? Math.min(1, spent / loop.budget_usd) : 0;
  const tone = statusTone(loop.status);

  const decide = async (decision: string) => {
    setActing(decision);
    setError(null);
    try {
      await api.decideReviewLoop(loop.id, decision);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setActing(null);
    }
  };

  const openState = async () => {
    if (!loop) return;
    try {
      const st = await api.getReviewLoopState(loop.id);
      const content = st.exists
        ? (st.truncated ? `_…truncated to the last 256KB…_\n\n${st.content}` : st.content)
        : `_No state file yet._\n\nThe implementer maintains it at \`${st.path}\` — it appears once the first implementation leg runs.`;
      const tabId = `rvl-state-${loop.id}`;
      openPanelTab({
        id: tabId,
        type: 'subagent',
        label: 'STATE.md',
        subagentType: 'review-loop',
        description: loop.title || loop.id,
        content,
        prompt: st.path,
        streaming: false,
        status: 'complete',
        startedAt: Date.now(),
        blocks: [{ type: 'text', content }],
      });
      // Refresh content when the tab already existed (openPanelTab focuses).
      updatePanelTab(tabId, { content, blocks: [{ type: 'text', content }] });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const kill = async () => {
    if (!window.confirm(`Kill review loop ${loop.id}? The current leg will be stopped.`)) return;
    setActing('kill');
    try {
      await api.killReviewLoop(loop.id, 'killed from loop card');
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setActing(null);
    }
  };

  return (
    <div className="border-b border-border-subtle bg-surface/60 shrink-0">
      <div className="max-w-[var(--chat-width)] mx-auto px-5">
        {/* Collapsed strip */}
        <button
          onClick={() => setExpanded(v => !v)}
          className="w-full flex items-center gap-2.5 py-2 cursor-pointer text-left"
        >
          {expanded ? <ChevronDown size={13} className="text-text-faint shrink-0" /> : <ChevronRight size={13} className="text-text-faint shrink-0" />}
          <span className={`flex items-center gap-1.5 ${tone} shrink-0`}>
            {live && <span className="w-1.5 h-1.5 rounded-full bg-current animate-pulse" />}
            <Repeat size={14} />
          </span>
          <span className="text-[13px] text-text-secondary font-medium truncate">
            {statusLabel(loop)}
          </span>
          <span className="text-[12px] text-text-faint shrink-0 tabular-nums">
            {met}/{loop.criteria.length} criteria
          </span>
          <span className="flex-1" />
          {/* Spend bar */}
          <span className="flex items-center gap-2 shrink-0">
            <span className="w-24 h-1.5 rounded-full bg-surface-raised overflow-hidden">
              <span
                className={`block h-full rounded-full ${frac >= 0.8 ? 'bg-orange-400' : 'bg-emerald-500/70'}`}
                style={{ width: `${Math.max(2, frac * 100)}%` }}
              />
            </span>
            <span className="text-[11px] text-text-faint tabular-nums">
              {fmtUsd(spent)} / {fmtUsd(loop.budget_usd)}
            </span>
          </span>
        </button>

        {expanded && (
          <div className="pb-3 space-y-3">
            {/* Criteria checklist */}
            <div className="space-y-1">
              {loop.criteria.map(c => (
                <div key={c.id} className="flex items-start gap-2 text-[12.5px]">
                  <span className="mt-0.5"><CriterionIcon status={c.last_status} /></span>
                  <span className="text-text-faint shrink-0 font-medium">{c.id}</span>
                  <span className="text-text-secondary min-w-0">{c.statement}
                    {c.source === 'verifier' && (
                      <span className="ml-1.5 text-[10px] text-emerald-400/70 border border-emerald-400/25 rounded px-1 py-px align-middle">
                        added by verifier · iter {c.added_iteration}
                      </span>
                    )}
                  </span>
                </div>
              ))}
            </div>

            {/* Attempts timeline */}
            {attempts.length > 0 && (
              <div className="space-y-0.5">
                {attempts.map(a => {
                  const run = runs.find(r => r.id === a.run_id);
                  const isLiveLeg = a.run_id === loop.current_run_id && live;
                  const sessionId = run?.session_id ?? `workflow:${a.run_id}`;
                  const verdict = a.verdict?.verdict;
                  return (
                    <div key={a.id} className="flex items-center gap-2 text-[12px] py-0.5">
                      {a.role === 'implementer'
                        ? <Hammer size={12} className="text-hue-violet/70 shrink-0" />
                        : <SearchCheck size={12} className="text-hue-teal shrink-0" />}
                      <span className="text-text-secondary shrink-0">
                        {a.role === 'implementer' ? 'implement' : 'verify'} #{a.iteration}
                        {a.attempt_no > 1 ? `.${a.attempt_no}` : ''}
                      </span>
                      {isLiveLeg ? (
                        <span className="flex items-center gap-1 text-emerald-400 shrink-0">
                          <Loader2 size={11} className="animate-spin" /> running
                        </span>
                      ) : (
                        <span className={`shrink-0 ${
                          a.status === 'done' ? 'text-text-faint'
                          : a.status === 'dispatching' || a.status === 'running' ? 'text-emerald-400'
                          : 'text-red-400/80'
                        }`}>{a.status}</span>
                      )}
                      {verdict && (
                        <span
                          title={a.verdict?.summary || ''}
                          className={`shrink-0 text-[10px] uppercase tracking-wide px-1.5 py-px rounded border ${
                            verdict === 'pass'
                              ? 'text-emerald-400 border-emerald-400/25 bg-emerald-400/10'
                              : 'text-amber-400 border-amber-400/25 bg-amber-400/10'
                          }`}
                        >{verdict.replace(/_/g, ' ')}</span>
                      )}
                      <span className="text-text-faint tabular-nums shrink-0">
                        {fmtUsd(isLiveLeg && currentRun ? currentRun.spent_usd : a.spend_usd)}
                      </span>
                      <span className="flex-1" />
                      {/* A dispatching leg's session may not exist yet —
                          don't offer a jump into a 404. */}
                      {(a.status !== 'dispatching' || !!run?.session_id) && (
                        <button
                          onClick={() => void switchSession(sessionId)}
                          className="flex items-center gap-1 text-[11px] text-text-muted hover:text-text-secondary cursor-pointer transition-colors shrink-0"
                          title={isLiveLeg
                            ? 'Watch this leg working in real time (opens its session)'
                            : "Open this leg's session transcript"}
                        >
                          <Eye size={12} /> {isLiveLeg ? 'watch live' : 'view'}
                        </button>
                      )}
                    </div>
                  );
                })}
              </div>
            )}

            {/* Decision bar (parked) */}
            {loop.status === 'awaiting_user' && (
              <div className="flex items-center gap-2 flex-wrap pt-1">
                <button
                  onClick={() => void decide('accept')}
                  disabled={!!acting}
                  className="h-7 px-3 rounded-lg bg-emerald-500/80 hover:bg-emerald-500 text-white text-[12px] font-medium cursor-pointer disabled:opacity-40 transition-colors"
                >Accept as-is</button>
                {loop.failure_reason === 'budget' && (() => {
                  const suggested = Math.max(5, Math.round(loop.budget_usd * 0.25));
                  const amount = budgetAdd.trim() || String(suggested);
                  return (
                    <span className="flex items-center gap-1">
                      <button
                        onClick={() => void decide(`budget:${amount}`)}
                        disabled={!!acting || !(parseFloat(amount) > 0)}
                        className="h-7 px-3 rounded-lg bg-surface-raised border border-emerald-400/40 hover:border-emerald-400 text-emerald-400 text-[12px] cursor-pointer disabled:opacity-40 transition-colors"
                        title="Raise the loop budget and resume where it stopped"
                      >Add ${amount} budget</button>
                      <input
                        type="number"
                        min="1"
                        step="1"
                        value={budgetAdd}
                        onChange={(e) => setBudgetAdd(e.target.value)}
                        placeholder={String(suggested)}
                        aria-label="Budget to add in dollars"
                        className="h-7 w-16 px-2 bg-surface-raised border border-border rounded-lg text-[12px] text-text outline-none focus:border-emerald-400/50"
                      />
                    </span>
                  );
                })()}
                <button
                  onClick={() => void decide('iterate:2')}
                  disabled={!!acting}
                  className="h-7 px-3 rounded-lg bg-surface-raised border border-border hover:border-accent/50 text-text-secondary text-[12px] cursor-pointer disabled:opacity-40 transition-colors"
                >Grant +2 iterations</button>
                {loop.failure_reason === 'scope' && (
                  <button
                    onClick={() => void decide('adopt_and_continue')}
                    disabled={!!acting}
                    className="h-7 px-3 rounded-lg bg-surface-raised border border-emerald-400/40 hover:border-emerald-400 text-emerald-400 text-[12px] cursor-pointer disabled:opacity-40 transition-colors"
                  >Adopt & continue</button>
                )}
                <button
                  onClick={() => void decide('abandon')}
                  disabled={!!acting}
                  className="h-7 px-3 rounded-lg bg-surface-raised border border-border hover:border-red-400/60 text-red-400/90 text-[12px] cursor-pointer disabled:opacity-40 transition-colors"
                >Abandon</button>
                {acting && <Loader2 size={13} className="animate-spin text-text-faint" />}
              </div>
            )}

            {/* Footer: config + actions */}
            <div className="flex items-center gap-3 text-[11px] text-text-faint flex-wrap">
              <span>impl: <code className="text-text-muted">{loop.implementer.engine}{loop.implementer.model ? ` · ${loop.implementer.model}` : ''}</code></span>
              <span>verify: <code className="text-text-muted">{loop.verifier.engine}{loop.verifier.model ? ` · ${loop.verifier.model}` : ''}</code></span>
              <span>adoption: {loop.criteria_adoption}</span>
              {loop.cwd && <span className="truncate max-w-[260px]" title={loop.cwd}>cwd: {loop.cwd}</span>}
              <span className="flex-1" />
              <button
                onClick={() => void openState()}
                className="flex items-center gap-1 text-text-faint hover:text-text-secondary cursor-pointer transition-colors"
                title="Open the implementer's handoff file (STATE.md) in the side panel"
              >
                <FileText size={11} /> STATE.md
              </button>
              <Link
                to="/workflow-runs"
                className="flex items-center gap-1 text-text-faint hover:text-text-secondary no-underline transition-colors"
                title="All workflow runs (leg journals live there)"
              >
                <ExternalLink size={11} /> runs
              </Link>
              {live && (
                <button
                  onClick={() => void kill()}
                  disabled={!!acting}
                  className="flex items-center gap-1 text-red-400/80 hover:text-red-400 cursor-pointer disabled:opacity-40 transition-colors"
                  title="Kill the loop (stops the current leg)"
                >
                  <Square size={11} /> kill
                </button>
              )}
            </div>

            {error && <div className="text-[12px] text-error">{error}</div>}
          </div>
        )}
      </div>
    </div>
  );
}
