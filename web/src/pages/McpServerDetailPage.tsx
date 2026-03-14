import { useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Zap, CheckCircle, XCircle, Clock, Plug } from 'lucide-react';
import { useMcpStore } from '../stores/mcpStore';

const TYPE_COLORS: Record<string, string> = {
  sdk: 'text-[#6366f1] bg-[#6366f1]/10',
  stdio: 'text-emerald-400 bg-emerald-400/10',
  sse: 'text-amber-400 bg-amber-400/10',
  http: 'text-sky-400 bg-sky-400/10',
};

function UsageBar({ total, success }: { total: number; success: number }) {
  if (total === 0) return null;
  const pct = Math.round((success / total) * 100);
  return (
    <div className="w-full bg-[#252525] rounded-full h-1.5">
      <div
        className={`h-1.5 rounded-full ${pct >= 90 ? 'bg-emerald-500' : pct >= 70 ? 'bg-amber-500' : 'bg-red-500'}`}
        style={{ width: `${pct}%` }}
      />
    </div>
  );
}

export function McpServerDetailPage() {
  const { serverName } = useParams<{ serverName: string }>();
  const navigate = useNavigate();
  const { selectedServer, detailLoading, loadServer, clearSelectedServer } = useMcpStore();

  useEffect(() => {
    if (serverName) loadServer(decodeURIComponent(serverName));
    return () => clearSelectedServer();
  }, [serverName]);

  if (detailLoading || !selectedServer) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <span className="text-[#666] text-sm">
          {detailLoading ? 'Loading...' : 'Server not found'}
        </span>
      </div>
    );
  }

  const s = selectedServer;
  const successRate = s.total_invocations > 0
    ? Math.round((s.success_count / s.total_invocations) * 100)
    : null;
  const typeClass = TYPE_COLORS[s.type] || 'text-[#888] bg-[#252525]';

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden">
      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-3 border-b border-[#2a2a2a] shrink-0">
        <button
          onClick={() => navigate('/mcp')}
          className="text-[#666] hover:text-[#ccc] cursor-pointer"
        >
          <ArrowLeft size={16} />
        </button>
        <div className="flex items-center gap-2 flex-1">
          <h1 className="text-sm font-medium text-[#e0e0e0]">{s.name}</h1>
          <span className={`text-[10px] px-1.5 py-0.5 rounded font-mono ${typeClass}`}>
            {s.type}
          </span>
          {!s.enabled && (
            <span className="text-[10px] text-amber-500/70 bg-amber-500/10 px-1.5 py-0.5 rounded">
              disabled
            </span>
          )}
        </div>
      </div>

      {/* Body */}
      <div className="flex-1 overflow-y-auto">
        <div className="flex flex-col lg:flex-row gap-4 p-4">
          {/* Left: Tool breakdown */}
          <div className="flex-1 min-w-0">
            <h2 className="text-xs font-medium text-[#888] uppercase tracking-wider mb-3">
              Tools ({s.tools.length})
            </h2>
            {s.tools.length === 0 ? (
              <p className="text-xs text-[#555]">No tool usage recorded yet.</p>
            ) : (
              <div className="space-y-1">
                {s.tools.map(t => {
                  const tRate = t.invocations > 0
                    ? Math.round((t.success_count / t.invocations) * 100)
                    : null;
                  return (
                    <div
                      key={t.tool_name}
                      className="flex items-center gap-3 px-3 py-2 bg-[#1a1a1a] border border-[#2a2a2a] rounded text-xs"
                    >
                      <span className="text-[#e0e0e0] font-mono flex-1 truncate">
                        {t.tool_name}
                      </span>
                      <span className="text-[#666] tabular-nums">{t.invocations} calls</span>
                      {tRate !== null && (
                        <span className={`tabular-nums ${tRate >= 90 ? 'text-emerald-500' : 'text-amber-500'}`}>
                          {tRate}%
                        </span>
                      )}
                      {t.last_used && (
                        <span className="text-[#555]">
                          {new Date(t.last_used).toLocaleDateString()}
                        </span>
                      )}
                    </div>
                  );
                })}
              </div>
            )}

            {/* Recent usage */}
            <h2 className="text-xs font-medium text-[#888] uppercase tracking-wider mt-6 mb-3">
              Recent Usage
            </h2>
            {s.recent_usage.length === 0 ? (
              <p className="text-xs text-[#555]">No recent usage.</p>
            ) : (
              <div className="space-y-1">
                {s.recent_usage.map(u => (
                  <div
                    key={u.id}
                    className="flex items-center gap-3 px-3 py-1.5 text-[11px] bg-[#1a1a1a] border border-[#2a2a2a] rounded"
                  >
                    {u.success
                      ? <CheckCircle size={10} className="text-emerald-500 shrink-0" />
                      : <XCircle size={10} className="text-red-500 shrink-0" />}
                    <span className="text-[#e0e0e0] font-mono truncate flex-1">
                      {u.tool_name}
                    </span>
                    {u.duration_ms != null && (
                      <span className="text-[#555] tabular-nums">{u.duration_ms}ms</span>
                    )}
                    <span className="text-[#555]">
                      {new Date(u.created_at).toLocaleString()}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Right: Stats panel */}
          <div className="w-full lg:w-64 shrink-0 space-y-4">
            <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
              <h3 className="text-xs font-medium text-[#888] uppercase tracking-wider mb-3">
                Usage Statistics
              </h3>
              <div className="space-y-3 text-xs">
                <div className="flex justify-between">
                  <span className="text-[#888] flex items-center gap-1"><Zap size={10} /> Invocations</span>
                  <span className="text-[#e0e0e0] tabular-nums">{s.total_invocations}</span>
                </div>
                {successRate !== null && (
                  <>
                    <div className="flex justify-between">
                      <span className="text-[#888]">Success Rate</span>
                      <span className={successRate >= 90 ? 'text-emerald-400' : 'text-amber-400'}>
                        {successRate}%
                      </span>
                    </div>
                    <UsageBar total={s.total_invocations} success={s.success_count} />
                  </>
                )}
                {s.avg_duration_ms != null && (
                  <div className="flex justify-between">
                    <span className="text-[#888]">Avg Duration</span>
                    <span className="text-[#e0e0e0] tabular-nums">{s.avg_duration_ms}ms</span>
                  </div>
                )}
                {s.last_used && (
                  <div className="flex justify-between">
                    <span className="text-[#888] flex items-center gap-1"><Clock size={10} /> Last Used</span>
                    <span className="text-[#e0e0e0]">{new Date(s.last_used).toLocaleDateString()}</span>
                  </div>
                )}
              </div>
            </div>

            <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
              <h3 className="text-xs font-medium text-[#888] uppercase tracking-wider mb-3">
                Server Info
              </h3>
              <div className="space-y-2 text-xs">
                <div className="flex justify-between">
                  <span className="text-[#888]">Type</span>
                  <span className={`font-mono ${typeClass} px-1 rounded`}>{s.type}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-[#888] flex items-center gap-1"><Plug size={10} /> Tools</span>
                  <span className="text-[#e0e0e0]">{s.tool_count || s.tools.length}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-[#888]">First Seen</span>
                  <span className="text-[#e0e0e0]">{new Date(s.first_seen_at).toLocaleDateString()}</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
