import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { RefreshCw, Zap, CheckCircle, XCircle, Clock, Plug } from 'lucide-react';
import { useMcpStore, type McpServer } from '../stores/mcpStore';
import { formatMcpName } from '../utils/formatMcpName';

const TYPE_COLORS: Record<string, string> = {
  sdk: 'text-[#6366f1] bg-[#6366f1]/10',
  stdio: 'text-emerald-400 bg-emerald-400/10',
  sse: 'text-amber-400 bg-amber-400/10',
  http: 'text-sky-400 bg-sky-400/10',
  plugin: 'text-violet-400 bg-violet-400/10',
};

function ServerCard({ server }: { server: McpServer }) {
  const navigate = useNavigate();
  const successRate = server.total_invocations > 0
    ? Math.round((server.success_count / server.total_invocations) * 100)
    : null;
  const typeClass = TYPE_COLORS[server.type] || 'text-text-muted bg-surface-raised';

  return (
    <div
      className="bg-surface-raised border border-border rounded-lg p-4 hover:border-[#3a3a3a] cursor-pointer transition-colors"
      onClick={() => navigate(`/mcp/${encodeURIComponent(server.name)}`)}
    >
      <div className="flex items-start justify-between mb-2">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="text-sm font-medium text-text truncate">{formatMcpName(server.name)}</h3>
            <span className={`text-[10px] px-1.5 py-0.5 rounded font-mono ${typeClass}`}>
              {server.type}
            </span>
          </div>
        </div>
        {!server.enabled && (
          <span className="text-[10px] text-amber-500/70 bg-amber-500/10 px-1.5 py-0.5 rounded ml-2">
            disabled
          </span>
        )}
      </div>

      <div className="flex items-center gap-4 mt-3 text-[10px] text-text-dim">
        {server.tool_count > 0 && (
          <div className="flex items-center gap-1">
            <Plug size={10} />
            <span>{server.tool_count} tools</span>
          </div>
        )}
        <div className="flex items-center gap-1">
          <Zap size={10} />
          <span>{server.total_invocations} uses</span>
        </div>
        {successRate !== null && (
          <div className="flex items-center gap-1">
            {successRate >= 90
              ? <CheckCircle size={10} className="text-emerald-500" />
              : <XCircle size={10} className="text-amber-500" />}
            <span>{successRate}%</span>
          </div>
        )}
        {server.last_used && (
          <div className="flex items-center gap-1">
            <Clock size={10} />
            <span>{new Date(server.last_used).toLocaleDateString()}</span>
          </div>
        )}
      </div>
    </div>
  );
}

export function McpServersPage() {
  const { servers, loading, reloading, loadServers, reloadServers } = useMcpStore();

  useEffect(() => { loadServers(); }, []);

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-border shrink-0">
        <div className="flex items-center gap-2">
          <h1 className="text-sm font-medium text-text">MCP Servers</h1>
          <span className="text-[10px] text-text-dim bg-surface-raised px-1.5 py-0.5 rounded">
            {servers.length}
          </span>
        </div>
        <button
          onClick={reloadServers}
          disabled={reloading}
          className="flex items-center gap-1 px-2 py-1 text-xs text-text-muted hover:text-text-secondary hover:bg-surface-hover rounded cursor-pointer disabled:opacity-50"
          title="Reload MCP config from YAML files"
        >
          <RefreshCw size={12} className={reloading ? 'animate-spin' : ''} />
          Reload
        </button>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4">
        {loading ? (
          <div className="text-center text-text-dim text-sm py-12">Loading MCP servers...</div>
        ) : servers.length === 0 ? (
          <div className="text-center py-12">
            <Plug size={32} className="mx-auto text-text-faint mb-3" />
            <p className="text-sm text-text-dim mb-1">No MCP servers registered</p>
            <p className="text-xs text-text-faint">
              Add external MCP servers in{' '}
              <code className="text-[#6366f1]">config.yaml</code>
              {' '}under the{' '}
              <code className="text-[#6366f1]">mcp_servers</code> key.
            </p>
          </div>
        ) : (
          <div className="grid gap-3 grid-cols-1 md:grid-cols-2 xl:grid-cols-3">
            {servers.map(server => (
              <ServerCard key={server.name} server={server} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
