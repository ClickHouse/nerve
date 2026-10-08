import { create } from 'zustand';
import { api } from '../api/client';
import { formatMcpName } from '../utils/formatMcpName';

/** The value of `managed_by` for a server from the MCP gateway's catalog. */
export const MANAGED_BY_ORGANIZATION = 'organization';

export interface McpServer {
  name: string;
  type: string;
  enabled: boolean;
  tool_count: number;
  total_invocations: number;
  success_count: number;
  avg_duration_ms: number | null;
  last_used: string | null;
  first_seen_at: string;
  last_seen_at: string;
  /**
   * External mode only. `organization` for a server from the MCP gateway's
   * catalog, `null` for Nerve's own server. Absent in local mode.
   */
  managed_by?: string | null;
  /** Text from the organization's catalog, for a managed server. */
  display_name?: string;
  description?: string;
  /**
   * For a managed server: the agent backends (`codex`, `claude`) whose
   * sessions do not get it, each with the reason. Empty when all do.
   */
  not_applied?: Record<string, string>;
}

/** Whether a server, or a server list, is managed by the organization. */
export function isManaged(item: { managed_by?: string | null } | null | undefined): boolean {
  return item?.managed_by === MANAGED_BY_ORGANIZATION;
}

const BACKEND_LABELS: Record<string, string> = { claude: 'Claude', codex: 'Codex' };

/**
 * One line per backend that leaves the server out, for example
 * "Not applied to Codex: name used by the system configuration (...)".
 */
export function notAppliedLines(server: { not_applied?: Record<string, string> }): string[] {
  return Object.entries(server.not_applied ?? {})
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([backend, reason]) => `Not applied to ${BACKEND_LABELS[backend] ?? backend}: ${reason}`);
}

/** The name to show: the catalog's display name for a managed server. */
export function mcpServerTitle(server: McpServer): string {
  return (isManaged(server) && server.display_name) || formatMcpName(server.name);
}

export interface McpToolBreakdown {
  tool_name: string;
  invocations: number;
  success_count: number;
  avg_duration_ms: number | null;
  last_used: string | null;
}

export interface McpServerDetail extends McpServer {
  tools: McpToolBreakdown[];
  recent_usage: Array<{
    id: number;
    server_name: string;
    tool_name: string;
    session_id: string | null;
    duration_ms: number | null;
    success: boolean;
    error: string | null;
    created_at: string;
  }>;
}

interface McpState {
  servers: McpServer[];
  /** True in external mode: the organization manages the servers. */
  managedByOrganization: boolean;
  selectedServer: McpServerDetail | null;
  loading: boolean;
  detailLoading: boolean;
  reloading: boolean;

  loadServers: () => Promise<void>;
  loadServer: (name: string) => Promise<void>;
  reloadServers: () => Promise<void>;
  clearSelectedServer: () => void;
}

export const useMcpStore = create<McpState>((set, get) => ({
  servers: [],
  managedByOrganization: false,
  selectedServer: null,
  loading: true,
  detailLoading: false,
  reloading: false,

  loadServers: async () => {
    try {
      const list = await api.listMcpServers();
      set({
        servers: list.servers,
        managedByOrganization: isManaged(list),
        loading: false,
      });
    } catch (e) {
      console.error('Failed to load MCP servers:', e);
      set({ loading: false });
    }
  },

  loadServer: async (name: string) => {
    set({ detailLoading: true, selectedServer: null });
    try {
      const server = await api.getMcpServer(name);
      set({ selectedServer: server, detailLoading: false });
    } catch (e) {
      console.error('Failed to load MCP server:', e);
      set({ detailLoading: false });
    }
  },

  reloadServers: async () => {
    set({ reloading: true });
    try {
      await api.reloadMcpServers();
      await get().loadServers();
    } catch (e) {
      console.error('Failed to reload MCP servers:', e);
    } finally {
      set({ reloading: false });
    }
  },

  clearSelectedServer: () => set({ selectedServer: null }),
}));
