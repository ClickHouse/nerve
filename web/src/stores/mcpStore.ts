import { create } from 'zustand';
import { api } from '../api/client';

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
  selectedServer: null,
  loading: true,
  detailLoading: false,
  reloading: false,

  loadServers: async () => {
    try {
      const { servers } = await api.listMcpServers();
      set({ servers, loading: false });
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
