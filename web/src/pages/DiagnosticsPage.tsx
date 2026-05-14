import { useEffect, useState } from 'react';
// import { useNavigate } from 'react-router-dom';
import { api } from '../api/client';
import { Server, HardDrive, RefreshCw, Clock, CheckCircle2, XCircle, Database, Activity, Brain, Play, Loader2, DollarSign, Zap, BarChart3 } from 'lucide-react';

function formatUptime(isoDate: string): string {
  const diff = Date.now() - new Date(isoDate).getTime();
  const hours = Math.floor(diff / 3600000);
  const minutes = Math.floor((diff % 3600000) / 60000);
  if (hours >= 24) {
    const days = Math.floor(hours / 24);
    return `${days}d ${hours % 24}h`;
  }
  return `${hours}h ${minutes}m`;
}

function formatTokens(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

/** Shorten model identifiers for display (e.g. "claude-opus-4-6-20250901" → "Opus 4.6") */
function formatModelName(model: string): string {
  const m = model.replace(/^claude-/, '').replace(/-\d{8}$/, '');
  const match = m.match(/^(\w+)-(\d+)-(\d+)/);
  if (match) return `${match[1].charAt(0).toUpperCase() + match[1].slice(1)} ${match[2]}.${match[3]}`;
  const match2 = m.match(/^(\w+)-(\d+)/);
  if (match2) return `${match2[1].charAt(0).toUpperCase() + match2[1].slice(1)} ${match2[2]}`;
  return model;
}

function RunButton({ onClick, label, title }: { onClick: () => Promise<void>; label: string; title: string }) {
  const [running, setRunning] = useState(false);

  const handleClick = async () => {
    if (running) return;
    setRunning(true);
    try {
      await onClick();
    } finally {
      setRunning(false);
    }
  };

  return (
    <button
      onClick={handleClick}
      disabled={running}
      className={`flex items-center gap-1.5 px-3 py-2 text-[12px] bg-surface border border-border-subtle rounded-lg cursor-pointer transition-colors shrink-0 ${
        running
          ? 'text-text-faint cursor-not-allowed'
          : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised'
      }`}
      title={title}
    >
      {running
        ? <><Loader2 size={12} className="animate-spin" /> Running...</>
        : <><Play size={12} /> {label}</>
      }
    </button>
  );
}

export function DiagnosticsPage() {
  // const navigate = useNavigate();
  const [data, setData] = useState<any>(null);
  const [cronLogs, setCronLogs] = useState<any[]>([]);
  const [memuHealth, setMemuHealth] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  const load = async () => {
    try {
      const [diag, logs, health] = await Promise.all([
        api.getDiagnostics(),
        api.getCronLogs(undefined, 30),
        api.getMemuHealth().catch(() => null),
      ]);
      setData(diag);
      setCronLogs(logs.logs);
      setMemuHealth(health);
    } catch (e) {
      console.error('Failed to load diagnostics:', e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  if (loading) return <div className="flex-1 flex items-center justify-center text-text-faint">Loading...</div>;
  if (!data) return <div className="flex-1 flex items-center justify-center text-hue-red">Failed to load</div>;

  const usage = data.usage;

  return (
    <div className="h-full overflow-y-auto">
      <div className="border-b border-border-subtle px-6 py-3 flex items-center justify-between bg-bg shrink-0">
        <h1 className="text-lg font-semibold">Diagnostics</h1>
        <button onClick={load} className="text-text-dim hover:text-text-muted cursor-pointer p-1.5 hover:bg-surface-raised rounded">
          <RefreshCw size={16} />
        </button>
      </div>

      <div className="p-6 max-w-5xl mx-auto space-y-6">
        {/* System Info */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <InfoCard icon={Server} label="Hostname" value={data.system?.hostname} />
          <InfoCard icon={Server} label="Platform" value={data.system?.platform?.split('-')[0]} />
          <InfoCard icon={HardDrive} label="Memory" value={`${data.system?.memory_mb} MB`} />
          <InfoCard icon={HardDrive} label="Disk Free" value={`${data.system?.disk_free_gb} / ${data.system?.disk_total_gb} GB`} />
        </div>

        {/* Observability — Langfuse status */}
        {data.langfuse && (
          <div className="text-[12px] text-text-dim flex items-center gap-3 flex-wrap">
            <span className="flex items-center gap-1.5">
              {data.langfuse.enabled ? (
                <CheckCircle2 size={12} className="text-hue-green" />
              ) : (
                <XCircle size={12} className="text-text-faint" />
              )}
              <span>
                Langfuse:{' '}
                <span className={data.langfuse.enabled ? 'text-text-secondary' : 'text-text-faint'}>
                  {data.langfuse.enabled ? 'enabled' : 'disabled'}
                </span>
              </span>
            </span>
            {data.langfuse.host && (
              <a
                href={data.langfuse.host}
                target="_blank"
                rel="noopener noreferrer"
                className="text-text-faint hover:text-text-secondary underline-offset-2 hover:underline"
              >
                {data.langfuse.host.replace(/^https?:\/\//, '')}
              </a>
            )}
            {data.langfuse.last_flush_at && (
              <span className="text-text-faint">
                last flush {new Date(data.langfuse.last_flush_at).toLocaleString()}
              </span>
            )}
          </div>
        )}

        {/* Usage & Cost */}
        {usage?.last_7d && (
          <section>
            <h2 className="text-[14px] font-medium text-text-muted mb-3 flex items-center gap-2">
              <DollarSign size={14} /> Usage & Cost (7 days)
            </h2>

            {/* Summary cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-3">
              <InfoCard icon={BarChart3} label="Tokens (in/out)"
                value={`${formatTokens(usage.last_7d.total_input)} / ${formatTokens(usage.last_7d.total_output)}`} />
              <InfoCard icon={DollarSign} label="Cost"
                value={`$${usage.last_7d.total_cost_usd?.toFixed(2) ?? '0.00'}`} />
              <InfoCard icon={Zap} label="Cache Hit Rate"
                value={`${((usage.cache_hit_rate?.rate ?? 0) * 100).toFixed(1)}%`} />
              <InfoCard icon={Activity} label="Turns / Sessions"
                value={`${usage.last_7d.turns} / ${usage.last_7d.sessions}`} />
            </div>

            {/* Daily usage chart — simple CSS bar chart */}
            {usage.daily?.length > 0 && (
              <div className="mb-3">
                <div className="text-[11px] text-text-dim mb-2">Daily cost</div>
                <div className="flex items-end gap-1 h-20">
                  {[...usage.daily].reverse().map((day: any) => {
                    const cost = day.cost_usd || 0;
                    const maxCost = Math.max(...usage.daily.map((d: any) => d.cost_usd || 0));
                    const heightPct = maxCost > 0 ? Math.max(2, (cost / maxCost) * 100) : 2;
                    const dateLabel = day.date?.slice(5) || ''; // MM-DD
                    return (
                      <div key={day.date} className="flex-1 flex flex-col items-center gap-0.5 min-w-0">
                        <div
                          className="w-full bg-accent/30 rounded-t-sm hover:bg-accent/50 transition-colors relative group"
                          style={{ height: `${heightPct}%` }}
                        >
                          <div className="absolute -top-6 left-1/2 -translate-x-1/2 hidden group-hover:block z-10
                            bg-surface-raised border border-border-subtle rounded px-1.5 py-0.5 text-[10px] text-text-secondary whitespace-nowrap shadow-lg">
                            {formatTokens((day.input_tokens || 0) + (day.output_tokens || 0))} &middot; ${cost.toFixed(2)}
                          </div>
                        </div>
                        <span className="text-[9px] text-text-faint tabular-nums">{dateLabel}</span>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {/* By model breakdown */}
            {usage.by_model?.length > 0 && (
              <div className="border border-border-subtle rounded-lg overflow-hidden mb-3">
                <table className="w-full text-[13px]">
                  <thead>
                    <tr className="bg-surface text-text-muted">
                      <th className="text-left px-4 py-2 font-medium">Model</th>
                      <th className="text-right px-4 py-2 font-medium">Turns</th>
                      <th className="text-right px-4 py-2 font-medium">Input</th>
                      <th className="text-right px-4 py-2 font-medium">Output</th>
                      <th className="text-right px-4 py-2 font-medium">Cost</th>
                    </tr>
                  </thead>
                  <tbody>
                    {usage.by_model.map((m: any) => (
                      <tr key={m.model} className="border-t border-border-subtle hover:bg-surface">
                        <td className="px-4 py-2 font-mono text-text-secondary text-[12px]">{formatModelName(m.model)}</td>
                        <td className="px-4 py-2 text-right text-text-dim tabular-nums">{m.turns}</td>
                        <td className="px-4 py-2 text-right text-text-secondary tabular-nums">{formatTokens(m.input_tokens || 0)}</td>
                        <td className="px-4 py-2 text-right text-text-secondary tabular-nums">{formatTokens(m.output_tokens || 0)}</td>
                        <td className="px-4 py-2 text-right text-text-secondary tabular-nums">${(m.cost_usd || 0).toFixed(2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* By source breakdown */}
            {usage.by_source?.length > 0 && (
              <div className="border border-border-subtle rounded-lg overflow-hidden">
                <table className="w-full text-[13px]">
                  <thead>
                    <tr className="bg-surface text-text-muted">
                      <th className="text-left px-4 py-2 font-medium">Source</th>
                      <th className="text-right px-4 py-2 font-medium">Sessions</th>
                      <th className="text-right px-4 py-2 font-medium">Turns</th>
                      <th className="text-right px-4 py-2 font-medium">Input</th>
                      <th className="text-right px-4 py-2 font-medium">Output</th>
                      <th className="text-right px-4 py-2 font-medium">Cost</th>
                    </tr>
                  </thead>
                  <tbody>
                    {usage.by_source.map((src: any) => (
                      <tr key={src.source} className="border-t border-border-subtle hover:bg-surface">
                        <td className="px-4 py-2 font-mono text-text-secondary">{src.source}</td>
                        <td className="px-4 py-2 text-right text-text-dim tabular-nums">{src.sessions}</td>
                        <td className="px-4 py-2 text-right text-text-dim tabular-nums">{src.turns}</td>
                        <td className="px-4 py-2 text-right text-text-secondary tabular-nums">{formatTokens(src.input_tokens || 0)}</td>
                        <td className="px-4 py-2 text-right text-text-secondary tabular-nums">{formatTokens(src.output_tokens || 0)}</td>
                        <td className="px-4 py-2 text-right text-text-secondary tabular-nums">${(src.cost_usd || 0).toFixed(2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        )}

        {/* memU Health */}
        {memuHealth && (
          <section>
            <h2 className="text-[14px] font-medium text-text-muted mb-3 flex items-center gap-2">
              <Database size={14} /> memU Memory Service
            </h2>

            {/* Summary cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-3">
              <InfoCard icon={Activity} label="Status"
                value={memuHealth.service_available ? 'Available' : 'Unavailable'} />
              <InfoCard icon={Clock} label="Uptime"
                value={memuHealth.initialized_at ? formatUptime(memuHealth.initialized_at) : 'N/A'} />
              <InfoCard icon={Database} label="Total Items"
                value={String(memuHealth.database?.total_items ?? 0)} />
              <InfoCard icon={HardDrive} label="DB Size"
                value={`${memuHealth.database?.db_size_mb ?? 0} MB`} />
            </div>

            {/* Type distribution badges */}
            {memuHealth.database?.type_distribution && (
              <div className="flex gap-2 mb-3 flex-wrap">
                {Object.entries(memuHealth.database.type_distribution).map(([type, count]) => (
                  <span key={type} className="text-[12px] px-2 py-1 bg-surface border border-border-subtle rounded">
                    <span className="text-text-muted">{type}:</span>{' '}
                    <span className="text-text-secondary">{String(count)}</span>
                  </span>
                ))}
                {memuHealth.database?.total_categories > 0 && (
                  <span className="text-[12px] px-2 py-1 bg-surface border border-border-subtle rounded">
                    <span className="text-text-muted">categories:</span>{' '}
                    <span className="text-text-secondary">{memuHealth.database.total_categories}</span>
                  </span>
                )}
                {memuHealth.database?.total_resources > 0 && (
                  <span className="text-[12px] px-2 py-1 bg-surface border border-border-subtle rounded">
                    <span className="text-text-muted">resources:</span>{' '}
                    <span className="text-text-secondary">{memuHealth.database.total_resources}</span>
                  </span>
                )}
                {memuHealth.database?.events_missing_happened_at > 0 && (
                  <span className="text-[12px] px-2 py-1 bg-amber-500/10 border border-amber-500/20 rounded text-amber-600">
                    {memuHealth.database.events_missing_happened_at} events missing happened_at
                  </span>
                )}
              </div>
            )}

            {/* In-flight operations */}
            {memuHealth.in_flight?.length > 0 && (
              <div className="mb-3 p-3 bg-blue-500/10 border border-blue-500/20 rounded-lg">
                <div className="text-[12px] text-blue-600 mb-1">In-flight operations:</div>
                {memuHealth.in_flight.map((op: any, i: number) => (
                  <div key={i} className="text-[12px] text-text-secondary">
                    {op.operation} — <span className="text-text-muted">{op.description}</span> ({op.elapsed_s}s)
                  </div>
                ))}
              </div>
            )}

            {/* Operation stats table */}
            <div className="border border-border-subtle rounded-lg overflow-hidden">
              <table className="w-full text-[13px]">
                <thead>
                  <tr className="bg-surface text-text-muted">
                    <th className="text-left px-4 py-2 font-medium">Operation</th>
                    <th className="text-left px-4 py-2 font-medium">Calls</th>
                    <th className="text-left px-4 py-2 font-medium">Avg Duration</th>
                    <th className="text-left px-4 py-2 font-medium">Errors</th>
                    <th className="text-left px-4 py-2 font-medium">Last Error</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(memuHealth.operations || {}).map(([name, stats]: [string, any]) => (
                    <tr key={name} className="border-t border-border-subtle hover:bg-surface">
                      <td className="px-4 py-2 font-mono text-text-secondary">{name}</td>
                      <td className="px-4 py-2 text-text-secondary">{stats.call_count}</td>
                      <td className="px-4 py-2 text-text-dim">
                        {stats.call_count > 0 ? `${stats.avg_duration_s}s` : '-'}
                      </td>
                      <td className="px-4 py-2">
                        {stats.error_count > 0
                          ? <span className="text-hue-red">{stats.error_count}</span>
                          : <span className="text-text-dim">0</span>
                        }
                      </td>
                      <td className="px-4 py-2 text-hue-red text-[12px] truncate max-w-xs">{stats.last_error || ''}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {/* Memorization Sweep */}
        {data.memorization && (
          <section>
            <h2 className="text-[14px] font-medium text-text-muted mb-3 flex items-center gap-2">
              <Brain size={14} /> Memorization Sweep
            </h2>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-3">
              <InfoCard icon={Clock} label="Interval" value={`${data.memorization.interval_minutes}m`} />
              <InfoCard icon={Activity} label="Total Runs" value={String(data.memorization.total_runs)} />
              <InfoCard icon={Database} label="Pending" value={String(data.memorization.sessions_pending)} />
              <InfoCard icon={Activity} label="Errors" value={String(data.memorization.total_errors)} />
            </div>

            {/* Last run details */}
            <div className="flex items-center gap-3 mb-3">
              <div className="flex-1 p-3 bg-surface border border-border-subtle rounded-lg">
                <div className="text-[11px] text-text-dim mb-1">Last run</div>
                <div className="text-[13px] text-text-secondary">
                  {data.memorization.last_run_at
                    ? new Date(data.memorization.last_run_at).toLocaleString()
                    : 'Not yet'}
                </div>
                {data.memorization.last_result && !data.memorization.last_result.error && (
                  <div className="text-[12px] text-text-dim mt-1">
                    {data.memorization.last_result.sessions_indexed > 0
                      ? `${data.memorization.last_result.sessions_indexed} sessions, ${data.memorization.last_result.messages_indexed} messages indexed`
                      : 'Nothing to index'}
                  </div>
                )}
                {data.memorization.last_result?.error && (
                  <div className="text-[12px] text-hue-red mt-1 truncate">
                    Error: {data.memorization.last_result.error}
                  </div>
                )}
              </div>
              <RunButton
                onClick={async () => { await api.triggerMemorizationSweep(); await load(); }}
                label="Run now"
                title="Run memorization sweep now"
              />
            </div>
          </section>
        )}

        {/* Cron Logs */}
        <section>
          <h2 className="text-[14px] font-medium text-text-muted mb-3 flex items-center gap-2">
            <Clock size={14} /> Cron Logs
          </h2>
          {cronLogs.length === 0 ? (
            <div className="text-text-faint text-sm">No cron logs</div>
          ) : (
            <div className="border border-border-subtle rounded-lg overflow-hidden">
              <table className="w-full text-[13px]">
                <thead>
                  <tr className="bg-surface text-text-muted">
                    <th className="text-left px-4 py-2 font-medium">Job</th>
                    <th className="text-left px-4 py-2 font-medium">Status</th>
                    <th className="text-left px-4 py-2 font-medium">Started</th>
                    <th className="text-left px-4 py-2 font-medium">Error</th>
                  </tr>
                </thead>
                <tbody>
                  {cronLogs.map((log) => (
                    <tr key={log.id} className="border-t border-border-subtle hover:bg-surface">
                      <td className="px-4 py-2 font-mono text-text-secondary">{log.job_id}</td>
                      <td className="px-4 py-2">
                        {log.status === 'success'
                          ? <span className="flex items-center gap-1 text-hue-emerald"><CheckCircle2 size={12} /> ok</span>
                          : <span className="flex items-center gap-1 text-hue-red"><XCircle size={12} /> error</span>
                        }
                      </td>
                      <td className="px-4 py-2 text-text-dim">{log.started_at}</td>
                      <td className="px-4 py-2 text-hue-red text-[12px] truncate max-w-xs">{log.error || ''}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>

        <div className="text-[12px] text-text-faint pt-2">
          Workspace: {data.workspace} | Sessions: {data.sessions_count}
        </div>
      </div>
    </div>
  );
}

function InfoCard({ icon: Icon, label, value }: { icon: typeof Server; label: string; value: string }) {
  return (
    <div className="p-3 bg-surface border border-border-subtle rounded-lg">
      <div className="flex items-center gap-1.5 text-[11px] text-text-dim mb-1">
        <Icon size={12} /> {label}
      </div>
      <div className="text-[14px] text-text-secondary truncate">{value}</div>
    </div>
  );
}
