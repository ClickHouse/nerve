import { CheckCircle2, XCircle } from '../ui/icons';
import { TOOL_MODE_ALL, allToolsText, notAppliedLines } from '../../stores/mcpStore';

/** One server of the catalog: its desired state and what the gateway observed. */
export interface McpGatewayServer {
  id: string;
  desired_revision: string;
  /** `selected` names the tools; `all` gives every tool of the server and names none. */
  tool_mode: string;
  /** `accept`, or `review` when the gateway serves the approved tool definitions. */
  tool_changes: string;
  /** The last tool list that the gateway served to the agent; null when unknown. */
  inventory: {
    revision: string;
    tool_count: number;
    observed_at: string;
    desired_revision: string;
  } | null;
  health: {
    inventory_state: 'unknown' | 'current' | 'stale';
    checked_at: string | null;
    failure: string | null;
  };
}

/** The `mcp_gateway` block of `GET /api/diagnostics` (external mode only). */
export interface McpGatewayDiagnostics {
  url: string | null;
  generation: number | null;
  digest: string | null;
  /** The agent's desired revision in the last catalog read. */
  desired_revision: string | null;
  servers: McpGatewayServer[];
  /** When Nerve last read a catalog; null before the first one. */
  fetched_at: string | null;
  checked_at: string | null;
  error: string | null;
  retrying: boolean;
  /** Catalog server ID -> backend -> reason, for servers a backend leaves out. */
  not_applied?: Record<string, Record<string, string>>;
}

/** Text for the failure reasons of the catalog contract. */
const FAILURE_TEXT: Record<string, string> = {
  upstream_unavailable: 'upstream unavailable',
  upstream_timeout: 'upstream timeout',
  upstream_error: 'upstream error',
  credential_missing: 'no credential',
  credential_unavailable: 'credential unavailable',
  credential_rejected: 'credential rejected',
  resources_exhausted: 'gateway out of memory',
};

/** A revision without its prefix, cut to 12 digits. */
function shortRevision(revision: string): string {
  return revision.replace(/^sha256:/, '').slice(0, 12);
}

/**
 * What the gateway observed of one server, for example "2 tools observed
 * (stale)". With the mode `all` the count is the size of the last list, not
 * a limit: "all tools, 2 observed".
 */
export function observedText(server: McpGatewayServer): string {
  const { inventory, health } = server;
  let text: string;
  if (server.tool_mode === TOOL_MODE_ALL) {
    text = allToolsText(inventory === null ? null : inventory.tool_count);
  } else {
    text = inventory === null
      ? 'tools not observed yet'
      : `${inventory.tool_count} tool${inventory.tool_count === 1 ? '' : 's'} observed`;
  }
  if (health.inventory_state === 'stale') text += ' (stale)';
  if (health.failure !== null) {
    const reason = FAILURE_TEXT[health.failure];
    text += reason ? `; last check failed: ${reason}` : '; last check failed';
  }
  return text;
}

/**
 * The catalog that Nerve read last, which new sessions get, and the result of
 * the last read. It does not tell what a running session loaded.
 */
export function McpGatewayStatus({ status }: { status: McpGatewayDiagnostics }) {
  const fetched = status.generation !== null;
  const healthy = fetched && !status.error && Object.keys(status.not_applied ?? {}).length === 0;
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
            {fetched
              ? `catalog generation ${status.generation}, ${status.servers.length} server${status.servers.length === 1 ? '' : 's'} for new sessions`
              : 'no catalog read yet'}
          </span>
        </span>
      </span>
      {fetched && status.desired_revision && (
        <span className="text-text-faint font-mono" title={status.desired_revision}>
          desired revision {shortRevision(status.desired_revision)}
        </span>
      )}
      {status.servers.map((server) => (
        <span
          key={server.id}
          className={server.health.failure !== null ? 'text-hue-amber' : 'text-text-faint'}
          title={`desired revision ${server.desired_revision}`}
        >
          <span className="font-mono">{server.id}</span>: {observedText(server)}
        </span>
      ))}
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
