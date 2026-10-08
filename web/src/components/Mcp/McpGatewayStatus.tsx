import { CheckCircle2, XCircle } from '../ui/icons';
import { notAppliedLines } from '../../stores/mcpStore';

/** The `mcp_gateway` block of `GET /api/diagnostics` (external mode only). */
export interface McpGatewayDiagnostics {
  url: string | null;
  generation: number | null;
  digest: string | null;
  servers: string[];
  applied_at: string | null;
  checked_at: string | null;
  error: string | null;
  retrying: boolean;
  /** Catalog server ID -> backend -> reason, for servers a backend leaves out. */
  not_applied?: Record<string, Record<string, string>>;
}

/** The catalog that new sessions use, and the result of the last read. */
export function McpGatewayStatus({ status }: { status: McpGatewayDiagnostics }) {
  const applied = status.generation !== null;
  const healthy = applied && !status.error && Object.keys(status.not_applied ?? {}).length === 0;
  return (
    <div className="text-xs text-text-dim flex items-center gap-3 flex-wrap" role="group" aria-label="MCP gateway">
      <span className="flex items-center gap-1.5">
        {healthy ? (
          <CheckCircle2 size={12} className="text-hue-green" />
        ) : (
          <XCircle size={12} className="text-hue-amber" />
        )}
        <span>
          MCP gateway:{' '}
          <span className="text-text-secondary">
            {applied
              ? `catalog generation ${status.generation}, ${status.servers.length} server${status.servers.length === 1 ? '' : 's'}`
              : 'no catalog applied'}
          </span>
        </span>
      </span>
      {status.servers.length > 0 && (
        <span className="text-text-faint font-mono">{status.servers.join(', ')}</span>
      )}
      {status.error && <span className="text-hue-amber">{status.error}</span>}
      {Object.entries(status.not_applied ?? {}).sort(([a], [b]) => a.localeCompare(b)).map(
        ([serverId, backends]) => (
          <span key={serverId} className="text-hue-amber" role="alert">
            {serverId}: {notAppliedLines({ not_applied: backends }).join('; ')}
          </span>
        ),
      )}
      {status.retrying && <span className="text-text-faint">retrying</span>}
      {status.checked_at && (
        <span className="text-text-faint">
          last read {new Date(status.checked_at).toLocaleString()}
        </span>
      )}
    </div>
  );
}
