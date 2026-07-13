import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import {
  Activity,
  AlertCircle,
  Ban,
  CheckCircle2,
  CircleDashed,
  Clock3,
  Cpu,
  GitBranch,
  Loader2,
  RefreshCw,
  Timer,
  Workflow,
  XCircle,
} from 'lucide-react';
import {
  api,
  type UltracodeRun,
  type UltracodeRunEvent,
  type UltracodeRunStep,
  type UltracodeUsage,
} from '../api/client';

type DetailTab = 'steps' | 'events' | 'result';

const ACTIVE_STATUSES = new Set(['queued', 'pending', 'starting', 'running', 'in_progress']);
const TERMINAL_FAILURES = new Set(['failed', 'error', 'cancelled', 'canceled', 'abandoned', 'refuted']);

function normalizedStatus(status?: string): string {
  return (status || 'unknown').toLowerCase().replace(/\s+/g, '_');
}

function isActiveStatus(status?: string): boolean {
  return ACTIVE_STATUSES.has(normalizedStatus(status));
}

function isFailureStatus(status?: string): boolean {
  return TERMINAL_FAILURES.has(normalizedStatus(status));
}

function statusClasses(status?: string): string {
  const value = normalizedStatus(status);
  if (value === 'completed' || value === 'done' || value === 'success') {
    return 'border-emerald-400/25 bg-emerald-400/10 text-hue-emerald';
  }
  if (isFailureStatus(value)) {
    return 'border-red-400/25 bg-red-400/10 text-hue-red';
  }
  if (isActiveStatus(value)) {
    return 'border-blue-400/25 bg-blue-400/10 text-hue-blue';
  }
  return 'border-border bg-surface-raised text-text-muted';
}

function StatusIcon({ status, size = 14 }: { status?: string; size?: number }) {
  const value = normalizedStatus(status);
  if (value === 'completed' || value === 'done' || value === 'success') {
    return <CheckCircle2 size={size} className="text-hue-emerald shrink-0" />;
  }
  if (value === 'cancelled' || value === 'canceled' || value === 'refuted') {
    return <Ban size={size} className="text-hue-red shrink-0" />;
  }
  if (isFailureStatus(value)) {
    return <XCircle size={size} className="text-hue-red shrink-0" />;
  }
  if (isActiveStatus(value)) {
    return <Loader2 size={size} className="text-hue-blue animate-spin shrink-0" />;
  }
  return <CircleDashed size={size} className="text-text-faint shrink-0" />;
}

function runTitle(run: UltracodeRun): string {
  return run.name || run.display_name || run.task || run.slug || run.id;
}

function formatCount(value: number | null | undefined): string {
  const number = Number(value || 0);
  if (number >= 1_000_000_000) return `${(number / 1_000_000_000).toFixed(1)}B`;
  if (number >= 1_000_000) return `${(number / 1_000_000).toFixed(1)}M`;
  if (number >= 1_000) return `${(number / 1_000).toFixed(1)}K`;
  return number.toLocaleString();
}

function formatDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return '—';
  const seconds = Math.max(0, Math.floor(ms / 1000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ${minutes % 60}m`;
  const days = Math.floor(hours / 24);
  return `${days}d ${hours % 24}h`;
}

function runDuration(run: UltracodeRun): number | null {
  if (typeof run.duration_ms === 'number') return run.duration_ms;
  if (!run.started_at) return null;
  const started = Date.parse(run.started_at);
  const ended = run.completed_at ? Date.parse(run.completed_at) : Date.now();
  return Number.isFinite(started) && Number.isFinite(ended) ? Math.max(0, ended - started) : null;
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

function usageTotal(usage?: UltracodeUsage): number {
  if (!usage) return 0;
  if (typeof usage.total_tokens === 'number') return usage.total_tokens;
  return (usage.input_tokens || 0) + (usage.output_tokens || 0);
}

function cacheRate(usage?: UltracodeUsage): string {
  const input = usage?.input_tokens || 0;
  const cached = usage?.cached_input_tokens || 0;
  if (!input) return '—';
  return `${Math.min(100, Math.round((cached / input) * 100))}%`;
}

function stepsFor(run: UltracodeRun | null): UltracodeRunStep[] {
  if (!run) return [];
  if (Array.isArray(run.steps) && run.steps.length > 0) return run.steps;
  return Array.isArray(run.workers) ? run.workers : [];
}

function workerCount(run: UltracodeRun): number {
  if (typeof run.workers === 'number') return run.workers;
  if (Array.isArray(run.workers)) return run.workers.length;
  return run.steps?.length || 0;
}

function formatValue(value: unknown, limit = 16_000): { text: string; truncated: boolean } {
  if (value === undefined || value === null) return { text: '', truncated: false };
  let text: string;
  if (typeof value === 'string') {
    text = value;
  } else {
    try {
      text = JSON.stringify(value, null, 2);
    } catch {
      text = String(value);
    }
  }
  if (text.length <= limit) return { text, truncated: false };
  return { text: `${text.slice(0, limit)}\n…`, truncated: true };
}

function StatCard({ icon, label, value, detail }: {
  icon: ReactNode;
  label: string;
  value: string;
  detail?: string;
}) {
  return (
    <div className="rounded-xl border border-border-subtle bg-surface px-3.5 py-3 min-w-0">
      <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-[0.13em] text-text-faint">
        {icon}
        {label}
      </div>
      <div className="mt-1.5 text-lg font-semibold tabular-nums text-text truncate">{value}</div>
      {detail && <div className="mt-0.5 text-[11px] text-text-dim truncate">{detail}</div>}
    </div>
  );
}

function RunListItem({ run, selected, onSelect }: {
  run: UltracodeRun;
  selected: boolean;
  onSelect: () => void;
}) {
  const usage = run.aggregate_usage;
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`w-full text-left px-3.5 py-3 border-b border-border-subtle border-l-2 transition-colors cursor-pointer ${
        selected
          ? 'bg-accent/10 border-l-accent'
          : 'border-l-transparent hover:bg-surface-hover'
      }`}
    >
      <div className="flex items-start gap-2.5 min-w-0">
        <div className="pt-0.5"><StatusIcon status={run.status} size={15} /></div>
        <div className="min-w-0 flex-1">
          <div className="flex items-center justify-between gap-2">
            <span className="text-[13px] font-medium text-text-secondary truncate">{runTitle(run)}</span>
            <span className="text-[10px] text-text-faint shrink-0">
              {relativeTime(run.updated_at || run.completed_at || run.started_at)}
            </span>
          </div>
          {run.task && run.task !== runTitle(run) && (
            <div className="mt-0.5 text-[11px] text-text-dim truncate">{run.task}</div>
          )}
          <div className="mt-2 flex items-center gap-2.5 text-[10px] text-text-faint tabular-nums">
            <span className={`px-1.5 py-0.5 rounded border capitalize ${statusClasses(run.status)}`}>
              {normalizedStatus(run.status).replace(/_/g, ' ')}
            </span>
            <span>{workerCount(run)} agents</span>
            <span>{formatDuration(runDuration(run))}</span>
            {usage && <span>{formatCount(usageTotal(usage))} tok</span>}
          </div>
        </div>
      </div>
    </button>
  );
}

function StepCard({ step, index }: { step: UltracodeRunStep; index: number }) {
  const output = formatValue(step.error || (step.result ?? step.value));
  const title = step.title || step.label || step.step_id || step.id || `Agent ${index + 1}`;
  const dependencies = step.depends_on || (Array.isArray(step.spec?.depends_on) ? step.spec.depends_on as string[] : []);
  return (
    <details
      className="group rounded-xl border border-border-subtle bg-surface overflow-hidden"
      open={isActiveStatus(step.status) || undefined}
    >
      <summary className="list-none cursor-pointer px-4 py-3.5 hover:bg-surface-hover transition-colors [&::-webkit-details-marker]:hidden">
        <div className="flex items-center gap-3 min-w-0">
          <StatusIcon status={step.status} size={16} />
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 min-w-0">
              <span className="text-[13px] font-medium text-text-secondary truncate">{title}</span>
              {step.kind && (
                <span className="text-[9px] uppercase tracking-wider px-1.5 py-0.5 rounded bg-surface-raised text-text-faint border border-border-subtle">
                  {step.kind}
                </span>
              )}
            </div>
            <div className="mt-1 flex items-center gap-3 text-[10px] text-text-dim min-w-0">
              {step.model && <span className="truncate">{step.model}</span>}
              {step.reasoning_effort && <span>{step.reasoning_effort}</span>}
              {dependencies.length > 0 && (
                <span className="flex items-center gap-1 truncate" title={dependencies.join(', ')}>
                  <GitBranch size={10} /> {dependencies.length} dep{dependencies.length === 1 ? '' : 's'}
                </span>
              )}
            </div>
          </div>
          <div className="text-right shrink-0">
            <div className="text-[11px] text-text-muted tabular-nums">{formatDuration(step.duration_ms)}</div>
            {step.usage && <div className="text-[10px] text-text-faint tabular-nums">{formatCount(usageTotal(step.usage))} tok</div>}
          </div>
          <span className="text-text-faint group-open:rotate-90 transition-transform">›</span>
        </div>
      </summary>
      <div className="border-t border-border-subtle bg-bg-sunken px-4 py-3">
        {output.text ? (
          <>
            <pre className={`text-[11px] leading-5 whitespace-pre-wrap break-words max-h-96 overflow-auto ${step.error ? 'text-hue-red' : 'text-text-muted'}`}>
              {output.text}
            </pre>
            {output.truncated && (
              <div className="mt-2 text-[10px] text-hue-amber">Output truncated in the dashboard.</div>
            )}
          </>
        ) : (
          <div className="text-[11px] text-text-faint">No output recorded yet.</div>
        )}
      </div>
    </details>
  );
}

function EventRow({ event }: { event: UltracodeRunEvent }) {
  const hasFailure = isFailureStatus(event.status) || event.type?.includes('failed');
  const active = event.type?.includes('started') || event.type?.includes('running');
  const eventData = formatValue(event.data, 2_000);
  return (
    <div className="relative pl-7 pb-4 last:pb-0">
      <div className="absolute left-[6px] top-4 bottom-0 w-px bg-border-subtle last:hidden" />
      <div className={`absolute left-0 top-1.5 w-[13px] h-[13px] rounded-full border-2 border-bg ${
        hasFailure ? 'bg-red-400' : active ? 'bg-blue-400' : 'bg-emerald-400'
      }`} />
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="text-[12px] text-text-secondary">
            <span className="font-mono text-[11px]">{event.type || 'event'}</span>
            {event.label && <span className="text-text-muted"> · {event.label}</span>}
          </div>
          {event.message && <div className="mt-1 text-[11px] text-text-muted whitespace-pre-wrap">{event.message}</div>}
          {eventData.text && (
            <pre className="mt-1.5 text-[10px] leading-4 text-text-dim whitespace-pre-wrap break-words max-h-32 overflow-auto">
              {eventData.text}
            </pre>
          )}
        </div>
        <time className="text-[10px] text-text-faint tabular-nums shrink-0" title={formatTimestamp(event.at)}>
          {event.at ? new Date(event.at).toLocaleTimeString() : '—'}
        </time>
      </div>
    </div>
  );
}

function EmptyDashboard({ message, onRefresh }: { message: string; onRefresh: () => void }) {
  return (
    <div className="h-full flex items-center justify-center p-8">
      <div className="max-w-md text-center">
        <div className="w-14 h-14 mx-auto rounded-2xl border border-border bg-surface flex items-center justify-center">
          <Workflow size={26} className="text-text-faint" />
        </div>
        <h2 className="mt-4 text-base font-medium text-text-secondary">No Ultracode runs to show</h2>
        <p className="mt-1.5 text-[13px] leading-5 text-text-dim">{message}</p>
        <button
          type="button"
          onClick={onRefresh}
          className="mt-4 inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-border bg-surface text-[12px] text-text-muted hover:bg-surface-hover hover:text-text-secondary cursor-pointer"
        >
          <RefreshCw size={12} /> Refresh
        </button>
      </div>
    </div>
  );
}

export function UltracodePage() {
  const [runs, setRuns] = useState<UltracodeRun[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selectedRun, setSelectedRun] = useState<UltracodeRun | null>(null);
  const [tab, setTab] = useState<DetailTab>('steps');
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const selectedIdRef = useRef<string | null>(null);

  useEffect(() => {
    selectedIdRef.current = selectedId;
  }, [selectedId]);

  const steps = useMemo(() => stepsFor(selectedRun), [selectedRun]);
  const events = useMemo(() => [...(selectedRun?.events || [])].reverse().slice(0, 200), [selectedRun]);
  const active = Boolean(selectedRun && selectedRun.id === selectedId && isActiveStatus(selectedRun.status));

  const refreshAll = useCallback(async (showSpinner = true) => {
    if (showSpinner) setRefreshing(true);
    try {
      const list = await api.listUltracodeRuns(50);
      const nextRuns = list.runs || [];
      setRuns(nextRuns);
      const currentId = selectedIdRef.current;
      const nextId = currentId && nextRuns.some(run => run.id === currentId)
        ? currentId
        : nextRuns[0]?.id || null;
      if (nextId) {
        const detail = await api.getUltracodeRun(nextId);
        setSelectedRun(detail.run);
      } else {
        setSelectedRun(null);
      }
      setSelectedId(nextId);
      setError(null);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setLoading(false);
      if (showSpinner) setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    void refreshAll(false);
  }, [refreshAll]);

  useEffect(() => {
    if (!selectedId || selectedRun?.id === selectedId) return;
    let cancelled = false;
    setDetailLoading(true);
    api.getUltracodeRun(selectedId)
      .then(({ run }) => {
        if (!cancelled) {
          setSelectedRun(run);
          setError(null);
        }
      })
      .catch(reason => {
        if (!cancelled) setError(reason instanceof Error ? reason.message : String(reason));
      })
      .finally(() => {
        if (!cancelled) setDetailLoading(false);
      });
    return () => { cancelled = true; };
  }, [selectedId, selectedRun?.id]);

  useEffect(() => {
    if (!selectedId || !active) return;
    let cancelled = false;
    let inFlight = false;
    const poll = async () => {
      if (cancelled || inFlight) return;
      inFlight = true;
      try {
        const [detail, list] = await Promise.all([
          api.getUltracodeRun(selectedId),
          api.listUltracodeRuns(50),
        ]);
        if (!cancelled) {
          setSelectedRun(detail.run);
          setRuns(list.runs || []);
          setError(null);
        }
      } catch (reason) {
        if (!cancelled) setError(reason instanceof Error ? reason.message : String(reason));
      } finally {
        inFlight = false;
      }
    };
    const timer = window.setInterval(() => { void poll(); }, 2_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [active, selectedId]);

  const usage = selectedRun?.aggregate_usage;
  const topResult = formatValue(selectedRun?.error || selectedRun?.result);
  const failedSteps = steps.filter(step => isFailureStatus(step.status)).length;
  const completedSteps = steps.filter(step => normalizedStatus(step.status) === 'completed').length;

  return (
    <div className="h-full flex flex-col overflow-hidden bg-bg">
      <header className="h-[58px] shrink-0 border-b border-border-subtle px-5 flex items-center justify-between bg-bg">
        <div className="flex items-center gap-3 min-w-0">
          <div className="w-8 h-8 rounded-lg bg-violet-400/10 border border-violet-400/20 flex items-center justify-center shrink-0">
            <Workflow size={17} className="text-hue-violet" />
          </div>
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              <h1 className="text-[15px] font-semibold text-text">Ultracode</h1>
              <span className="text-[9px] uppercase tracking-[0.14em] px-1.5 py-0.5 rounded border border-border bg-surface-raised text-text-faint">
                read only
              </span>
              {active && (
                <span className="flex items-center gap-1 text-[10px] text-hue-blue">
                  <span className="relative flex h-1.5 w-1.5">
                    <span className="absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-60 animate-ping" />
                    <span className="relative inline-flex h-1.5 w-1.5 rounded-full bg-blue-400" />
                  </span>
                  live
                </span>
              )}
            </div>
            <p className="text-[11px] text-text-dim">Parallel worker runs and execution journals</p>
          </div>
        </div>
        <button
          type="button"
          onClick={() => { void refreshAll(); }}
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

      <div className="flex-1 min-h-0 flex">
        <aside className="w-[330px] shrink-0 border-r border-border-subtle bg-bg-sunken flex flex-col min-h-0">
          <div className="h-10 px-3.5 border-b border-border-subtle flex items-center justify-between shrink-0">
            <span className="text-[10px] uppercase tracking-[0.13em] text-text-faint">Runs</span>
            <span className="text-[10px] tabular-nums text-text-faint">{runs.length}</span>
          </div>
          <div className="flex-1 overflow-y-auto">
            {loading ? (
              <div className="h-28 flex items-center justify-center text-text-faint"><Loader2 size={18} className="animate-spin" /></div>
            ) : runs.length > 0 ? (
              runs.map(run => (
                <RunListItem
                  key={run.id}
                  run={run}
                  selected={run.id === selectedId}
                  onSelect={() => {
                    setSelectedId(run.id);
                    setTab('steps');
                  }}
                />
              ))
            ) : (
              <div className="px-5 py-10 text-center text-[12px] text-text-faint">No run journals found.</div>
            )}
          </div>
        </aside>

        <main className="flex-1 min-w-0 min-h-0 overflow-hidden">
          {!selectedRun && !detailLoading ? (
            <EmptyDashboard
              message={error ? 'The dashboard backend is not available yet.' : 'Start an Ultracode workflow and its journal will appear here.'}
              onRefresh={() => { void refreshAll(); }}
            />
          ) : detailLoading && !selectedRun ? (
            <div className="h-full flex items-center justify-center text-text-faint"><Loader2 size={20} className="animate-spin" /></div>
          ) : selectedRun ? (
            <div className="h-full overflow-y-auto">
              <div className="max-w-6xl mx-auto px-6 py-5 space-y-5">
                <section>
                  <div className="flex items-start justify-between gap-5">
                    <div className="min-w-0">
                      <div className="flex items-center gap-2.5">
                        <StatusIcon status={selectedRun.status} size={19} />
                        <h2 className="text-xl font-semibold text-text truncate">{runTitle(selectedRun)}</h2>
                        <span className={`text-[10px] capitalize px-2 py-0.5 rounded-full border shrink-0 ${statusClasses(selectedRun.status)}`}>
                          {normalizedStatus(selectedRun.status).replace(/_/g, ' ')}
                        </span>
                      </div>
                      {selectedRun.task && selectedRun.task !== runTitle(selectedRun) && (
                        <p className="mt-2 text-[13px] leading-5 text-text-muted max-w-3xl">{selectedRun.task}</p>
                      )}
                      <div className="mt-2 flex items-center gap-3 text-[10px] text-text-dim min-w-0">
                        <span className="font-mono truncate" title={selectedRun.cwd || undefined}>{selectedRun.cwd || 'workspace'}</span>
                        <span className="shrink-0" title={formatTimestamp(selectedRun.started_at)}>
                          started {relativeTime(selectedRun.started_at)}
                        </span>
                        <span className="font-mono text-text-faint truncate" title={selectedRun.id}>{selectedRun.id}</span>
                      </div>
                    </div>
                  </div>
                </section>

                <section className="grid grid-cols-2 lg:grid-cols-4 gap-3">
                  <StatCard
                    icon={<Timer size={11} />}
                    label="Elapsed"
                    value={formatDuration(runDuration(selectedRun))}
                    detail={selectedRun.completed_at ? `ended ${relativeTime(selectedRun.completed_at)}` : 'in progress'}
                  />
                  <StatCard
                    icon={<Cpu size={11} />}
                    label="Agents"
                    value={String(steps.length || workerCount(selectedRun))}
                    detail={`${completedSteps} done${failedSteps ? ` · ${failedSteps} failed` : ''}`}
                  />
                  <StatCard
                    icon={<Activity size={11} />}
                    label="Tokens"
                    value={formatCount(usageTotal(usage))}
                    detail={`${formatCount(usage?.output_tokens)} output`}
                  />
                  <StatCard
                    icon={<Clock3 size={11} />}
                    label="Cache hit"
                    value={cacheRate(usage)}
                    detail={`${formatCount(usage?.cached_input_tokens)} cached input`}
                  />
                </section>

                {usage && (
                  <section className="rounded-xl border border-border-subtle bg-surface px-4 py-3">
                    <div className="flex items-center justify-between gap-4">
                      <div>
                        <div className="text-[10px] uppercase tracking-[0.13em] text-text-faint">Usage</div>
                        <div className="mt-0.5 text-[11px] text-text-dim">Aggregate across every worker in this run</div>
                      </div>
                      <div className="flex items-center gap-5 text-right tabular-nums">
                        {[
                          ['Input', usage.input_tokens],
                          ['Cached', usage.cached_input_tokens],
                          ['Output', usage.output_tokens],
                          ['Reasoning', usage.reasoning_output_tokens],
                        ].map(([label, value]) => (
                          <div key={String(label)}>
                            <div className="text-[10px] text-text-faint">{label}</div>
                            <div className="mt-0.5 text-[12px] text-text-secondary">{formatCount(value as number | undefined)}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </section>
                )}

                <section>
                  <div className="flex items-center gap-1 border-b border-border-subtle mb-4">
                    {([
                      ['steps', `Agents ${steps.length}`],
                      ['events', `Events ${selectedRun.events?.length || 0}`],
                      ['result', 'Run detail'],
                    ] as Array<[DetailTab, string]>).map(([id, label]) => (
                      <button
                        key={id}
                        type="button"
                        onClick={() => setTab(id)}
                        className={`px-3 py-2 text-[12px] border-b-2 -mb-px transition-colors cursor-pointer ${
                          tab === id
                            ? 'border-accent text-text-secondary'
                            : 'border-transparent text-text-dim hover:text-text-muted'
                        }`}
                      >
                        {label}
                      </button>
                    ))}
                  </div>

                  {tab === 'steps' && (
                    <div className="space-y-2.5">
                      {steps.length > 0 ? steps.map((step, index) => (
                        <StepCard key={step.step_id || step.id || `${index}`} step={step} index={index} />
                      )) : (
                        <div className="rounded-xl border border-dashed border-border p-8 text-center text-[12px] text-text-faint">
                          No worker records in this journal.
                        </div>
                      )}
                    </div>
                  )}

                  {tab === 'events' && (
                    <div className="rounded-xl border border-border-subtle bg-surface px-4 py-4">
                      {events.length > 0 ? events.map((event, index) => (
                        <EventRow key={`${event.at || 'event'}-${event.type || ''}-${index}`} event={event} />
                      )) : (
                        <div className="py-6 text-center text-[12px] text-text-faint">No journal events recorded.</div>
                      )}
                      {(selectedRun.events?.length || 0) > events.length && (
                        <div className="pt-3 border-t border-border-subtle text-center text-[10px] text-text-faint">
                          Showing the latest {events.length} events.
                        </div>
                      )}
                    </div>
                  )}

                  {tab === 'result' && (
                    <div className="space-y-3">
                      <div className="rounded-xl border border-border-subtle bg-surface overflow-hidden">
                        <div className="px-4 py-2.5 border-b border-border-subtle text-[10px] uppercase tracking-[0.13em] text-text-faint">
                          Final result
                        </div>
                        <div className="bg-bg-sunken px-4 py-3">
                          {topResult.text ? (
                            <>
                              <pre className={`text-[11px] leading-5 whitespace-pre-wrap break-words max-h-[32rem] overflow-auto ${selectedRun.error ? 'text-hue-red' : 'text-text-muted'}`}>
                                {topResult.text}
                              </pre>
                              {topResult.truncated && <div className="mt-2 text-[10px] text-hue-amber">Result truncated in the dashboard.</div>}
                            </>
                          ) : (
                            <div className="text-[11px] text-text-faint">No top-level result recorded.</div>
                          )}
                        </div>
                      </div>
                      <div className="rounded-xl border border-border-subtle bg-surface overflow-hidden">
                        <div className="px-4 py-2.5 border-b border-border-subtle text-[10px] uppercase tracking-[0.13em] text-text-faint">
                          Run options
                        </div>
                        <pre className="bg-bg-sunken px-4 py-3 text-[11px] leading-5 text-text-muted whitespace-pre-wrap break-words overflow-auto">
                          {formatValue(selectedRun.options || {}).text}
                        </pre>
                      </div>
                    </div>
                  )}
                </section>
              </div>
            </div>
          ) : null}
        </main>
      </div>
    </div>
  );
}
