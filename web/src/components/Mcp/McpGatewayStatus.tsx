import { CheckCircle2, XCircle } from '../ui/icons';
import { notAppliedLines } from '../../stores/mcpStore';

/** One running client: the catalog generation it follows. */
export interface McpGatewaySession {
  session_id: string;
  backend: string;
  /** The generation that the client was built with. */
  built_generation: number | null;
  /** The last generation that the client follows in full, apart from servers it cannot add. */
  generation: number | null;
  /** Servers that the client does not follow (added, refused, waiting, or not changeable). */
  pending: string[];
}

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
  /** Seconds between two catalog reads; 0 when the periodic read is off. */
  refresh_seconds?: number | null;
  /** Catalog server ID -> backend -> reason, for servers a backend leaves out. */
  not_applied?: Record<string, Record<string, string>>;
  sessions?: McpGatewaySession[];
}

/** Whether a running session lacks a part of the applied catalog. */
function isBehind(session: McpGatewaySession, generation: number | null): boolean {
  return session.generation !== generation || session.pending.length > 0;
}

/** The catalog that new sessions use, and the result of the last read. */
export function McpGatewayStatus({ status }: { status: McpGatewayDiagnostics }) {
  const applied = status.generation !== null;
  const sessions = status.sessions ?? [];
  const behind = sessions.filter((session) => isBehind(session, status.generation));
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
      {applied && sessions.length > 0 && (
        <span className="text-text-faint">
          {sessions.length - behind.length} of {sessions.length} running session
          {sessions.length === 1 ? '' : 's'} on this generation
        </span>
      )}
      {behind.map((session) => (
        <span key={session.session_id} className="text-hue-amber" role="status">
          Session {session.session_id} ({session.backend}): generation{' '}
          {session.generation ?? 'none'}
          {session.pending.length > 0 && `; not followed: ${session.pending.join(', ')}`}
        </span>
      ))}
      {status.retrying && <span className="text-text-faint">retrying</span>}
      {status.checked_at && (
        <span className="text-text-faint">
          last read {new Date(status.checked_at).toLocaleString()}
        </span>
      )}
    </div>
  );
}
