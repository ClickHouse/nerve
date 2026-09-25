import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { ChevronDown, ChevronRight, Loader2, OctagonX, Rocket } from '../../ui/icons';
import type { ToolCallBlockData } from '../../../types/chat';
import { api, type WorkflowRun, type WorkflowRunStatus } from '../../../api/client';
import { useWorkflowRunStore, isActiveRun } from '../../../stores/workflowRunStore';
import { Button } from '../../ui';

/** Extract readable text from MCP content blocks. */
function extractText(result: string): string {
  try {
    const parsed: unknown = JSON.parse(result);
    if (Array.isArray(parsed)) {
      return parsed
        .filter(b => b && b.type === 'text')
        .map(b => String(b.text))
        .join('\n');
    }
  } catch { /* not JSON */ }
  return result;
}

const RESULT_SNIPPET_CHARS = 300;

function fmtUsd(value: number): string {
  if (value > 0 && value < 0.01) return `$${value.toFixed(4)}`;
  return `$${value.toFixed(2)}`;
}

function statusLabel(status: WorkflowRunStatus): string {
  return status.replace(/_/g, ' ');
}

/**
 * How the run ended — outcome, not identity, so this is the one colour map in
 * the file that takes the status tokens rather than `hue-*`. (The Rocket glyph
 * beside it stays `text-hue-amber`: that says "workflow run", not "went well".)
 */
function statusBadgeClasses(status: WorkflowRunStatus): string {
  switch (status) {
    case 'running':
      return 'border-info-border bg-info-bg text-info';
    case 'done':
      return 'border-success-border bg-success-bg text-success';
    case 'failed':
      return 'border-error-border bg-error-bg text-error';
    case 'budget_exhausted':
      return 'border-warning-border bg-warning-bg text-warning';
    default: // pending, killed
      return 'border-border bg-surface-raised text-text-muted';
  }
}

function StatusBadge({ status }: { status: WorkflowRunStatus }) {
  return (
    <span className={`inline-flex items-center gap-1.5 px-1.5 py-0.5 rounded-full border text-2xs capitalize shrink-0 ${statusBadgeClasses(status)}`}>
      {status === 'running' && (
        <span className="relative flex h-1.5 w-1.5">
          <span className="absolute inline-flex h-full w-full rounded-full bg-info opacity-60 animate-ping" />
          <span className="relative inline-flex h-1.5 w-1.5 rounded-full bg-info" />
        </span>
      )}
      {statusLabel(status)}
    </span>
  );
}

/**
 * Spend vs budget: emerald, amber from 80%, red at 100%. Unbudgeted runs show
 * plain spend.
 *
 * Only the *width* is data-driven, so it is the only thing in `style`; the
 * colour is a three-step ramp, so it becomes a class. It uses `hue-*` rather
 * than `bg-success`/`bg-warning`/`bg-error` because those tokens are Click UI's
 * feedback *foregrounds* — pale mint / pale pink, meant to sit on the matching
 * `-bg` tint. This bar sits on a neutral `bg-border-subtle` track, where the
 * saturated theme-adaptive hues are what stay readable in both themes.
 */
function SpendLine({ run }: { run: WorkflowRun }) {
  if (run.budget_usd === null || run.budget_usd <= 0) {
    return (
      <span className="text-xs text-text-dim tabular-nums whitespace-nowrap">
        {fmtUsd(run.spent_usd)} spent
      </span>
    );
  }
  const pct = (run.spent_usd / run.budget_usd) * 100;
  const barColor = pct >= 100 ? 'bg-hue-red' : pct >= 80 ? 'bg-hue-amber' : 'bg-hue-emerald';
  return (
    <div className="flex items-center gap-2 min-w-0" title={`${Math.round(pct)}% of budget`}>
      <span className="text-xs text-text-dim tabular-nums shrink-0">
        {fmtUsd(run.spent_usd)} of {fmtUsd(run.budget_usd)}
      </span>
      <div className="w-32 h-1.5 bg-border-subtle rounded-full overflow-hidden shrink-0">
        <div
          className={`h-full rounded-full transition-all duration-300 ${barColor}`}
          style={{ width: `${Math.min(pct, 100)}%` }}
        />
      </div>
    </div>
  );
}

/** Raw-text fallback when the run no longer exists server-side (pruned). */
function RawResultFallback({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const text = block.result ? extractText(block.result) : '';
  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-surface-raised transition-colors"
      >
        <Rocket size={14} className="text-text-muted shrink-0" />
        <span className="text-sm leading-tight font-mono font-medium text-text-secondary truncate">{block.tool}</span>
        <span className="text-xs text-text-faint shrink-0">run no longer available</span>
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </button>
      {expanded && (
        <div className="border-t border-border px-3 py-2">
          <pre className="text-xs text-text-muted font-mono whitespace-pre-wrap overflow-x-auto max-h-60 overflow-y-auto bg-bg rounded p-2 border border-border-subtle">
            {text || 'No result recorded.'}
          </pre>
        </div>
      )}
    </div>
  );
}

/**
 * Live in-chat card for workflow_run_start / workflow_run_status tool calls.
 * Reads the run from the runs store (kept fresh by the global
 * `workflow_run_update` WS event); on mount after a page reload it seeds the
 * store via GET /workflow-runs/:id.
 */
export function WorkflowRunToolBlock({ block, runId }: { block: ToolCallBlockData; runId: string }) {
  const run = useWorkflowRunStore(s => s.runs.find(r => r.id === runId));
  const [missing, setMissing] = useState(false);
  const [killing, setKilling] = useState(false);
  const [killError, setKillError] = useState<string | null>(null);
  const [showFullResult, setShowFullResult] = useState(false);

  // Seed the store when the run isn't in it yet (page reload: the WS upsert
  // only covers updates that happened while this tab was connected).
  useEffect(() => {
    if (run || missing) return;
    let cancelled = false;
    api.getWorkflowRun(runId)
      .then(fetched => {
        if (!cancelled) useWorkflowRunStore.getState().handleRunUpdate(fetched);
      })
      .catch(() => {
        // 404 = pruned run; anything else also degrades to the raw text.
        if (!cancelled) setMissing(true);
      });
    return () => { cancelled = true; };
  }, [runId, run, missing]);

  if (missing && !run) {
    return <RawResultFallback block={block} />;
  }

  if (!run) {
    return (
      <div className="my-1.5 border border-border rounded-lg bg-surface px-3 py-2 flex items-center gap-2">
        <Loader2 size={14} className="animate-spin text-text-muted shrink-0" />
        <span className="text-sm leading-tight text-text-secondary">Workflow run</span>
        <span className="text-xs font-mono text-text-faint">{runId}</span>
      </div>
    );
  }

  const active = isActiveRun(run);
  const title = run.title || run.engine;
  const showError = (run.status === 'failed' || run.status === 'budget_exhausted');
  const errorText = run.error || (run.status === 'budget_exhausted' ? 'Budget exhausted — the run was stopped.' : '');
  const resultText = (!active && !showError && run.result) ? run.result.trim() : '';
  const resultTruncated = resultText.length > RESULT_SNIPPET_CHARS;
  const resultShown = showFullResult || !resultTruncated
    ? resultText
    : `${resultText.slice(0, RESULT_SNIPPET_CHARS)}…`;

  const onKill = async () => {
    if (!window.confirm(`Kill workflow run ${run.id}? The agent will be stopped and the run marked as killed.`)) return;
    setKilling(true);
    setKillError(null);
    try {
      const updated = await api.killWorkflowRun(run.id, 'Killed from chat');
      useWorkflowRunStore.getState().handleRunUpdate(updated);
    } catch (e) {
      setKillError(e instanceof Error ? e.message : String(e));
    } finally {
      setKilling(false);
    }
  };

  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <div className="flex items-center gap-2 px-3 py-2 min-w-0">
        <Rocket size={14} className="text-hue-amber shrink-0" />
        <StatusBadge status={run.status} />
        <span className="text-sm leading-tight font-medium text-text-secondary truncate" title={title}>{title}</span>
        <span className="text-xs font-mono text-text-faint shrink-0">{run.id}</span>
        {active && (
          <Button
            variant="danger"
            size="xs"
            className="ml-auto"
            onClick={() => { void onKill(); }}
            disabled={killing}
            title="Stop this run and mark it as killed"
          >
            {killing ? <Loader2 size={11} className="animate-spin" /> : <OctagonX size={11} />}
            Kill
          </Button>
        )}
      </div>

      <div className="px-3 pb-2">
        <SpendLine run={run} />
      </div>

      {killError && (
        <div className="px-3 pb-2 text-xs text-error break-words">{killError}</div>
      )}

      {showError && errorText && (
        <div className="px-3 pb-2">
          <pre className={`text-xs leading-5 whitespace-pre-wrap break-words max-h-40 overflow-y-auto ${run.status === 'budget_exhausted' ? 'text-warning' : 'text-error'}`}>
            {errorText}
          </pre>
        </div>
      )}

      {resultText && (
        <div className="px-3 pb-2">
          <pre className="text-xs leading-5 whitespace-pre-wrap break-words max-h-64 overflow-y-auto text-text-muted">
            {resultShown}
          </pre>
          {resultTruncated && (
            <Button
              variant="ghost"
              size="xs"
              className="mt-1 -ml-2"
              onClick={() => setShowFullResult(v => !v)}
            >
              {showFullResult ? 'Show less' : 'Show full result'}
            </Button>
          )}
        </div>
      )}

      <div className="px-3 py-1.5 border-t border-border-subtle flex items-center gap-3 text-xs">
        {run.session_id && (
          <Link
            to={`/chat/${run.session_id}`}
            className="text-text-dim hover:text-text-secondary transition-colors"
          >
            Open session
          </Link>
        )}
        <Link
          to="/workflow-runs"
          className="text-text-dim hover:text-text-secondary transition-colors"
        >
          All runs
        </Link>
      </div>
    </div>
  );
}
