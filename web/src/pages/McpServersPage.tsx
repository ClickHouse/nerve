import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { RefreshCw, Zap, CheckCircle, XCircle, Clock, Plug } from '../components/ui/icons';
import { Badge, Button } from '../components/ui';
import { useMcpStore, type McpServer } from '../stores/mcpStore';
import { formatMcpName } from '../utils/formatMcpName';

/** Transport identity, not status — so `hue-*` rather than a feedback token. */
const TYPE_COLORS: Record<string, string> = {
  sdk: 'text-accent bg-accent/10',
  stdio: 'text-hue-emerald bg-hue-emerald/10',
  sse: 'text-hue-amber bg-hue-amber/10',
  http: 'text-hue-sky bg-hue-sky/10',
  plugin: 'text-hue-violet bg-hue-violet/10',
};

function ServerCard({ server }: { server: McpServer }) {
  const navigate = useNavigate();
  const successRate = server.total_invocations > 0
    ? Math.round((server.success_count / server.total_invocations) * 100)
    : null;
  const typeClass = TYPE_COLORS[server.type] || 'text-text-muted bg-surface-raised';

  return (
    <div
      className="bg-surface-raised border border-border rounded-lg p-4 hover:border-border cursor-pointer transition-colors"
      onClick={() => navigate(`/mcp/${encodeURIComponent(server.name)}`)}
    >
      <div className="flex items-start justify-between mb-2">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="text-sm font-medium text-text truncate">{formatMcpName(server.name)}</h3>
            <span className={`text-2xs px-1.5 py-0.5 rounded font-mono ${typeClass}`}>
              {server.type}
            </span>
          </div>
        </div>
        {!server.enabled && (
          <Badge tone="warning" className="ml-2">disabled</Badge>
        )}
      </div>

      <div className="flex items-center gap-4 mt-3 text-2xs text-text-dim">
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
              ? <CheckCircle size={10} className="text-hue-emerald" />
              : <XCircle size={10} className="text-hue-amber" />}
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
          <Badge>{servers.length}</Badge>
        </div>
        <Button
          variant="subtle"
          size="xs"
          onClick={reloadServers}
          disabled={reloading}
          title="Reload MCP config from YAML files"
        >
          <RefreshCw size={12} className={reloading ? 'animate-spin' : ''} />
          Reload
        </Button>
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
              <code className="text-accent">config.yaml</code>
              {' '}under the{' '}
              <code className="text-accent">mcp_servers</code> key.
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
