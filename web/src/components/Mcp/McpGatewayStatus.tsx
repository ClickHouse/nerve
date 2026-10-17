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

/** The last update of a running client to a newer catalog. */
export interface McpGatewaySessionUpdate {
  method: 'in_place' | 'replacement';
  result: 'applied' | 'replaced' | 'partial' | 'failed';
  target_revision: string | null;
  at: string;
  error: string | null;
}

/** Why a running client does not have the last catalog read. */
export interface McpGatewaySessionPending {
  reason: 'waiting_for_turn' | 'waiting_for_safe_boundary' | 'apply_error' | string;
  /** The desired revision of the last catalog read. */
  target_revision: string | null;
  /** The servers that the change touches. */
  servers: string[];
  /** The live work that the change waits for. */
  blocked_by: string[];
  error: string | null;
}

/** One running client and the catalog that it has. */
export interface McpGatewaySession {
  session_id: string;
  backend: string;
  /** The desired revision of the catalog that the client was built with. */
  built_revision: string | null;
  /** The desired revision of the last catalog that the client took in full. */
  applied_revision: string | null;
  /** Server ID -> the inventory revision that the client knows. */
  inventory: Record<string, string>;
  last_update: McpGatewaySessionUpdate | null;
  /** Null when the client has the last catalog read. */
  pending: McpGatewaySessionPending | null;
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
  /** Seconds between two catalog reads; 0 when the periodic read is off. */
  refresh_seconds?: number | null;
  /** Catalog server ID -> backend -> reason, for servers a backend leaves out. */
  not_applied?: Record<string, Record<string, string>>;
  sessions?: McpGatewaySession[];
}

/** Text for the live work that a catalog change waits for. */
const BLOCKER_TEXT: Record<string, string> = {
  autonomous_turn: 'an autonomous turn',
  buffered_output: 'unread output',
  background_task: 'a background task',
  active_turn: 'an active turn',
  command: 'a running command',
  child_agent: 'a child agent',
  state_unknown: 'work of unknown state',
};

/** Why a running session does not have the last catalog read. */
export function pendingText(pending: McpGatewaySessionPending): string {
  let text: string;
  if (pending.reason === 'waiting_for_turn') {
    text = 'changes before its next turn';
  } else if (pending.reason === 'waiting_for_safe_boundary') {
    const work = pending.blocked_by.map((blocker) => BLOCKER_TEXT[blocker] ?? blocker);
    const named = work.length > 1
      ? `${work.slice(0, -1).join(', ')} and ${work[work.length - 1]}`
      : work[0] ?? 'running work';
    text = `waits for ${named} to end`;
  } else if (pending.reason === 'apply_error') {
    text = `could not take the catalog${pending.error ? ` (${pending.error})` : ''}; the next turn tries again`;
  } else {
    text = pending.reason;
  }
  if (pending.servers.length > 0) text += `; servers: ${pending.servers.join(', ')}`;
  return text;
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
  const sessions = status.sessions ?? [];
  const behind = sessions.filter((session) => session.pending !== null);
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
      {fetched && sessions.length > 0 && (
        <span className="text-text-faint">
          {sessions.length - behind.length} of {sessions.length} running session
          {sessions.length === 1 ? '' : 's'} with this catalog
        </span>
      )}
      {fetched && behind.map((session) => session.pending && (
        <span
          key={session.session_id}
          className={session.pending.reason === 'apply_error' ? 'text-hue-red' : 'text-hue-amber'}
          role="status"
          title={session.applied_revision ? `applied desired revision ${session.applied_revision}` : undefined}
        >
          Session {session.session_id} ({session.backend}): {pendingText(session.pending)}
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
