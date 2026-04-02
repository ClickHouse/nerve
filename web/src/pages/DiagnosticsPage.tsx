import { useEffect, useState } from 'react';
// import { useNavigate } from 'react-router-dom';
import { api } from '../api/client';
import { Server, HardDrive, RefreshCw, Clock, CheckCircle2, XCircle, Database, Activity, Brain, Play, Loader2 } from 'lucide-react';

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
  if (!data) return <div className="flex-1 flex items-center justify-center text-red-400">Failed to load</div>;

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
                  <span className="text-[12px] px-2 py-1 bg-surface border border-amber-900/30 rounded text-amber-400">
                    {memuHealth.database.events_missing_happened_at} events missing happened_at
                  </span>
                )}
              </div>
            )}

            {/* In-flight operations */}
            {memuHealth.in_flight?.length > 0 && (
              <div className="mb-3 p-3 bg-surface border border-blue-900/30 rounded-lg">
                <div className="text-[12px] text-blue-400 mb-1">In-flight operations:</div>
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
                          ? <span className="text-red-400">{stats.error_count}</span>
                          : <span className="text-text-dim">0</span>
                        }
                      </td>
                      <td className="px-4 py-2 text-red-400 text-[12px] truncate max-w-xs">{stats.last_error || ''}</td>
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
                  <div className="text-[12px] text-red-400 mt-1 truncate">
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
                          ? <span className="flex items-center gap-1 text-emerald-400"><CheckCircle2 size={12} /> ok</span>
                          : <span className="flex items-center gap-1 text-red-400"><XCircle size={12} /> error</span>
                        }
                      </td>
                      <td className="px-4 py-2 text-text-dim">{log.started_at}</td>
                      <td className="px-4 py-2 text-red-400 text-[12px] truncate max-w-xs">{log.error || ''}</td>
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
