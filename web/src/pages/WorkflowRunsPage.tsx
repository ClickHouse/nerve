import { useEffect, useState, type ReactNode } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  AlertCircle,
  Ban,
  CheckCircle2,
  ChevronRight,
  CircleDashed,
  CircleDollarSign,
  Loader2,
  MessageSquare,
  OctagonX,
  RefreshCw,
  Rocket,
  XCircle,
} from 'lucide-react';
import {
  api,
  type WorkflowRun,
  type WorkflowRunJournal,
  type WorkflowRunJournalEvent,
  type WorkflowRunStatus,
} from '../api/client';
import { useWorkflowRunStore, isActiveRun } from '../stores/workflowRunStore';

const POLL_INTERVAL_MS = 15_000;
const EVENT_TAIL = 20;

function statusLabel(status: WorkflowRunStatus): string {
  return status.replace(/_/g, ' ');
}

function statusBadgeClasses(status: WorkflowRunStatus): string {
  switch (status) {
    case 'running':
      return 'border-blue-400/25 bg-blue-400/10 text-hue-blue';
    case 'done':
      return 'border-emerald-400/25 bg-emerald-400/10 text-hue-emerald';
    case 'failed':
      return 'border-red-400/25 bg-red-400/10 text-hue-red';
    case 'budget_exhausted':
      return 'border-amber-400/25 bg-amber-400/10 text-hue-amber';
    default: // pending, killed
      return 'border-border bg-surface-raised text-text-muted';
  }
}

function StatusIcon({ status, size = 15 }: { status: WorkflowRunStatus; size?: number }) {
  switch (status) {
    case 'running':
      return <Loader2 size={size} className="text-hue-blue animate-spin shrink-0" />;
    case 'done':
      return <CheckCircle2 size={size} className="text-hue-emerald shrink-0" />;
    case 'failed':
      return <XCircle size={size} className="text-hue-red shrink-0" />;
    case 'killed':
      return <Ban size={size} className="text-text-muted shrink-0" />;
    case 'budget_exhausted':
      return <CircleDollarSign size={size} className="text-hue-amber shrink-0" />;
    default: // pending
      return <CircleDashed size={size} className="text-text-faint shrink-0" />;
  }
}

function fmtUsd(value: number): string {
  if (value > 0 && value < 0.01) return `$${value.toFixed(4)}`;
  return `$${value.toFixed(2)}`;
}

function relativeTime(value?: string | null): string {
  if (!value) return 'unknown time';
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) return value;
  const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
  if (seconds < 60) return 'just now';
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 7) return `${days}d ago`;
  return new Date(timestamp).toLocaleDateString();
}

function formatTimestamp(value?: string | null): string {
  if (!value) return '—';
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? new Date(timestamp).toLocaleString() : value;
}

function runTitle(run: WorkflowRun): string {
  if (run.title) return run.title;
  const prompt = (run.spec?.prompt || '').trim().replace(/\s+/g, ' ');
  if (prompt) return prompt.length > 96 ? `${prompt.slice(0, 96)}…` : prompt;
  return run.id;
}

/** Compact JSON of a journal event's payload beyond the ts/run_id/event envelope. */
function eventExtras(event: WorkflowRunJournalEvent): string {
  const extras: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(event)) {
    if (key === 'ts' || key === 'run_id' || key === 'event') continue;
    extras[key] = value;
  }
  if (Object.keys(extras).length === 0) return '';
  try {
    const text = JSON.stringify(extras);
    return text.length > 200 ? `${text.slice(0, 200)}…` : text;
  } catch {
    return '';
  }
}

/**
 * Spend-vs-budget: green, amber from the warn threshold (80%), red at 100%.
 * Unbudgeted runs (allow_unbudgeted) show plain spend instead of a bar.
 */
function BudgetBar({ run }: { run: WorkflowRun }) {
  if (run.budget_usd === null || run.budget_usd <= 0) {
    return (
      <span className="text-[11px] text-text-dim tabular-nums whitespace-nowrap">
        {fmtUsd(run.spent_usd)} spent
      </span>
    );
  }
  const pct = (run.spent_usd / run.budget_usd) * 100;
  let color = '#10b981'; // emerald-500
  if (pct >= 100) color = '#ef4444'; // red-500
  else if (pct >= 80) color = '#f59e0b'; // amber-500
  return (
    <div
      className="flex items-center gap-2 whitespace-nowrap"
      title={run.warned_at ? `Budget warning issued ${relativeTime(run.warned_at)}` : `${Math.round(pct)}% of budget`}
    >
      <span className="text-[11px] text-text-dim tabular-nums">
        {fmtUsd(run.spent_usd)} / {fmtUsd(run.budget_usd)}
      </span>
      <div className="w-24 h-1.5 bg-border-subtle rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-300"
          style={{ width: `${Math.min(pct, 100)}%`, backgroundColor: color }}
        />
      </div>
    </div>
  );
}

function Meta({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="min-w-0">
      <div className="text-[9px] uppercase tracking-wider text-text-faint">{label}</div>
      <div className={`mt-0.5 text-[11px] text-text-secondary truncate ${mono ? 'font-mono' : ''}`} title={value}>
        {value}
      </div>
    </div>
  );
}

function DetailBlock({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-text-faint mb-1.5">{label}</div>
      {children}
    </div>
  );
}

function JournalEventRow({ event }: { event: WorkflowRunJournalEvent }) {
  const extras = eventExtras(event);
  return (
    <div className="flex items-start gap-2.5 text-[11px] leading-5 min-w-0">
      <time className="text-text-faint tabular-nums shrink-0" title={formatTimestamp(event.ts)}>
        {event.ts ? new Date(event.ts).toLocaleTimeString() : '—'}
      </time>
      <span className="font-mono text-text-secondary shrink-0">{event.event || 'event'}</span>
      {extras && <span className="text-text-dim truncate" title={extras}>{extras}</span>}
    </div>
  );
}

function RunCard({ run }: { run: WorkflowRun }) {
  const navigate = useNavigate();
  const killRun = useWorkflowRunStore(s => s.killRun);
  const [expanded, setExpanded] = useState(false);
  const [journal, setJournal] = useState<WorkflowRunJournal | null>(null);
  const [journalLoading, setJournalLoading] = useState(false);
  const [journalError, setJournalError] = useState<string | null>(null);
  const [killing, setKilling] = useState(false);

  // Fetch the journal on expand; refetch while expanded whenever the run
  // changes (WS spend/status updates bump updated_at) so the tail stays live.
  useEffect(() => {
    if (!expanded) return;
    let cancelled = false;
    setJournalLoading(true);
    api.getWorkflowRunJournal(run.id)
      .then(j => {
        if (!cancelled) {
          setJournal(j);
          setJournalError(null);
        }
      })
      .catch(reason => {
        if (!cancelled) setJournalError(reason instanceof Error ? reason.message : String(reason));
      })
      .finally(() => {
        if (!cancelled) setJournalLoading(false);
      });
    return () => { cancelled = true; };
  }, [expanded, run.id, run.updated_at]);

  const killable = run.status === 'pending' || run.status === 'running';
  const resultText = (journal?.has_result && journal.result) ? journal.result : (run.result || '');
  const events = (journal?.events || []).slice(-EVENT_TAIL);

  const onKill = async () => {
    if (!window.confirm(`Kill workflow run ${run.id}? The agent will be stopped and the run marked as killed.`)) return;
    setKilling(true);
    try {
      await killRun(run.id, 'Killed from the web UI');
    } finally {
      setKilling(false);
    }
  };

  return (
    <div className="rounded-xl border border-border-subtle bg-surface overflow-hidden">
      <div
        onClick={() => setExpanded(v => !v)}
        className="px-4 py-3 hover:bg-surface-hover transition-colors cursor-pointer"
      >
        <div className="flex items-center gap-3 min-w-0">
          <StatusIcon status={run.status} />
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 min-w-0">
              <span className="text-[13px] font-medium text-text-secondary truncate">{runTitle(run)}</span>
              <span className="shrink-0 text-[9px] uppercase tracking-wider px-1.5 py-0.5 rounded bg-surface-raised text-text-faint border border-border-subtle font-mono">
                {run.engine}
              </span>
            </div>
            <div className="mt-1.5 flex items-center gap-2.5 text-[10px] text-text-faint min-w-0">
              <span className={`inline-flex items-center gap-1.5 px-1.5 py-0.5 rounded-full border capitalize shrink-0 ${statusBadgeClasses(run.status)}`}>
                {run.status === 'running' && (
                  <span className="relative flex h-1.5 w-1.5">
                    <span className="absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-60 animate-ping" />
                    <span className="relative inline-flex h-1.5 w-1.5 rounded-full bg-blue-400" />
                  </span>
                )}
                {statusLabel(run.status)}
              </span>
              <span className="font-mono shrink-0">{run.id}</span>
              <span className="shrink-0" title={formatTimestamp(run.created_at)}>
                created {relativeTime(run.created_at)}
              </span>
            </div>
          </div>
          <BudgetBar run={run} />
          {run.session_id && (
            <button
              type="button"
              onClick={e => { e.stopPropagation(); navigate(`/chat/${run.session_id}`); }}
              className="shrink-0 inline-flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg border border-border bg-surface text-[11px] text-text-muted hover:bg-surface-hover hover:text-text-secondary cursor-pointer"
              title="Open the run's session chat"
            >
              <MessageSquare size={12} /> Chat
            </button>
          )}
          <ChevronRight
            size={15}
            className={`text-text-faint transition-transform shrink-0 ${expanded ? 'rotate-90' : ''}`}
          />
        </div>
      </div>

      {expanded && (
        <div className="border-t border-border-subtle bg-bg-sunken px-4 py-3.5 space-y-3.5">
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-x-6 gap-y-2.5">
            <Meta label="Run id" value={run.id} mono />
            <Meta label="Created by" value={run.created_by || '—'} />
            <Meta label="Model" value={run.spec?.model || 'default'} mono />
            <Meta label="Effort" value={run.spec?.effort || '—'} />
            <Meta label="Working dir" value={run.spec?.cwd || '—'} mono />
            <Meta label="Journal" value={run.journal_dir || '—'} mono />
            <Meta label="Started" value={formatTimestamp(run.started_at)} />
            <Meta label="Finished" value={formatTimestamp(run.finished_at)} />
          </div>

          <DetailBlock label="Prompt">
            <pre className="text-[11px] leading-5 whitespace-pre-wrap break-words max-h-64 overflow-auto text-text-muted">
              {run.spec?.prompt || '—'}
            </pre>
          </DetailBlock>

          {run.error && (
            <DetailBlock label="Error">
              <pre className="text-[11px] leading-5 whitespace-pre-wrap break-words max-h-48 overflow-auto text-hue-red">
                {run.error}
              </pre>
            </DetailBlock>
          )}

          {resultText && (
            <DetailBlock label="Result">
              <pre className="text-[11px] leading-5 whitespace-pre-wrap break-words max-h-96 overflow-auto text-text-muted">
                {resultText}
              </pre>
            </DetailBlock>
          )}

          <DetailBlock label={journal && events.length > 0 ? `Journal events (last ${events.length})` : 'Journal events'}>
            {journalLoading && !journal ? (
              <div className="py-1 flex items-center gap-2 text-[11px] text-text-faint">
                <Loader2 size={12} className="animate-spin" /> Loading journal…
              </div>
            ) : journalError ? (
              <div className="text-[11px] text-hue-red">{journalError}</div>
            ) : events.length > 0 ? (
              <div className="space-y-1 max-h-64 overflow-auto">
                {events.map((event, index) => (
                  <JournalEventRow key={`${event.ts || 'event'}-${index}`} event={event} />
                ))}
              </div>
            ) : (
              <div className="text-[11px] text-text-faint">No journal events recorded yet.</div>
            )}
          </DetailBlock>

          {killable && (
            <div className="flex justify-end pt-0.5">
              <button
                type="button"
                onClick={() => { void onKill(); }}
                disabled={killing}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-red-400/25 bg-red-400/10 text-hue-red text-[12px] hover:bg-red-400/20 disabled:opacity-50 cursor-pointer"
              >
                {killing ? <Loader2 size={12} className="animate-spin" /> : <OctagonX size={12} />}
                Kill run
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function EmptyState() {
  return (
    <div className="h-full flex items-center justify-center p-8">
      <div className="max-w-md text-center">
        <div className="w-14 h-14 mx-auto rounded-2xl border border-border bg-surface flex items-center justify-center">
          <Rocket size={26} className="text-text-faint" />
        </div>
        <h2 className="mt-4 text-base font-medium text-text-secondary">No workflow runs yet</h2>
        <p className="mt-1.5 text-[13px] leading-5 text-text-dim">
          Workflow runs are long-lived autonomous agent runs with a hard spend budget.
          Each run executes in its own session, its cost is tracked while it works, and
          it is stopped automatically when the budget is exhausted.
        </p>
        <p className="mt-2 text-[13px] leading-5 text-text-dim">
          Start one by asking the agent to use the{' '}
          <code className="bg-surface-raised px-1.5 py-0.5 rounded text-[12px]">workflow_run_start</code>{' '}
          tool, or via{' '}
          <code className="bg-surface-raised px-1.5 py-0.5 rounded text-[12px]">POST /api/workflow-runs</code>.
        </p>
      </div>
    </div>
  );
}

export function WorkflowRunsPage() {
  const runs = useWorkflowRunStore(s => s.runs);
  const total = useWorkflowRunStore(s => s.total);
  const loading = useWorkflowRunStore(s => s.loading);
  const error = useWorkflowRunStore(s => s.error);
  const loadRuns = useWorkflowRunStore(s => s.loadRuns);
  const [refreshing, setRefreshing] = useState(false);

  const activeCount = runs.filter(isActiveRun).length;
  const hasActive = activeCount > 0;

  useEffect(() => {
    void loadRuns();
  }, [loadRuns]);

  // Poll fallback while any run is pending/running — the workflow_run_update
  // WS event is the primary update path; this covers dropped sockets.
  useEffect(() => {
    if (!hasActive) return;
    const timer = window.setInterval(() => { void loadRuns(); }, POLL_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [hasActive, loadRuns]);

  const onRefresh = async () => {
    setRefreshing(true);
    try {
      await loadRuns();
    } finally {
      setRefreshing(false);
    }
  };

  return (
    <div className="h-full flex flex-col overflow-hidden bg-bg">
      <header className="h-[58px] shrink-0 border-b border-border-subtle px-5 flex items-center justify-between bg-bg">
        <div className="flex items-center gap-3 min-w-0">
          <div className="w-8 h-8 rounded-lg bg-amber-400/10 border border-amber-400/20 flex items-center justify-center shrink-0">
            <Rocket size={17} className="text-hue-amber" />
          </div>
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              <h1 className="text-[15px] font-semibold text-text">Workflow Runs</h1>
              {hasActive && (
                <span className="flex items-center gap-1 text-[10px] text-hue-blue">
                  <span className="relative flex h-1.5 w-1.5">
                    <span className="absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-60 animate-ping" />
                    <span className="relative inline-flex h-1.5 w-1.5 rounded-full bg-blue-400" />
                  </span>
                  {activeCount} active
                </span>
              )}
            </div>
            <p className="text-[11px] text-text-dim">Budgeted autonomous agent runs</p>
          </div>
        </div>
        <button
          type="button"
          onClick={() => { void onRefresh(); }}
          disabled={refreshing}
          className="p-2 rounded-lg text-text-dim hover:text-text-secondary hover:bg-surface-raised disabled:opacity-50 cursor-pointer"
          title="Refresh runs"
        >
          <RefreshCw size={15} className={refreshing ? 'animate-spin' : ''} />
        </button>
      </header>

      {error && (
        <div className="shrink-0 px-4 py-2 border-b border-red-400/20 bg-red-400/5 flex items-center gap-2 text-[11px] text-hue-red">
          <AlertCircle size={13} />
          <span className="truncate">{error}</span>
        </div>
      )}

      <div className="flex-1 min-h-0 overflow-y-auto">
        {loading ? (
          <div className="h-full flex items-center justify-center text-text-faint">
            <Loader2 size={20} className="animate-spin" />
          </div>
        ) : runs.length === 0 ? (
          <EmptyState />
        ) : (
          <div className="max-w-4xl mx-auto px-6 py-5">
            <div className="space-y-2.5">
              {runs.map(run => (
                <RunCard key={run.id} run={run} />
              ))}
            </div>
            {total > runs.length && (
              <div className="mt-4 text-center text-[10px] text-text-faint">
                Showing the latest {runs.length} of {total} runs.
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
