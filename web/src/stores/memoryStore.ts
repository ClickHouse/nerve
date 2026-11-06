import { create } from 'zustand';
import { api } from '../api/client';

export interface Category {
  id: string;
  name: string;
  description: string;
  summary: string | null;
}

export interface MemoryItem {
  id: string;
  memory_type: string;
  summary: string;
  resource_id: string | null;
  created_at: string;
  happened_at: string | null;
}

export interface Resource {
  id: string;
  url: string;
  modality: string;
  caption: string | null;
  created_at: string;
}

export interface AuditLogEntry {
  id: number;
  timestamp: string;
  action: string;
  target_type: string;
  target_id: string | null;
  source: string | null;
  details: Record<string, any> | null;
}

export type TabView = 'facts' | 'timeline' | 'sources' | 'log';

interface MemoryState {
  // Data
  available: boolean;
  /** The memory backend when the server names it: `loom` keeps memory in a
   * Loom service, whose own dashboard (`dashboardUrl`) browses it. */
  backend: string | null;
  dashboardUrl: string | null;
  categories: Category[];
  items: MemoryItem[];
  resources: Resource[];
  categoryItems: Record<string, string[]>;
  loading: boolean;

  // Audit
  auditLogs: AuditLogEntry[];
  auditLoading: boolean;
  auditFilter: { action: string; target_type: string };

  // UI state
  activeTab: TabView;
  selectedCategory: string | null;
  searchQuery: string;
  editingItemId: string | null;
  deletingItemId: string | null;
  editingCategoryId: string | null;

  // Actions
  load: () => Promise<void>;
  setActiveTab: (tab: TabView) => void;
  setSelectedCategory: (id: string | null) => void;
  setSearchQuery: (q: string) => void;
  setEditingItemId: (id: string | null) => void;
  setDeletingItemId: (id: string | null) => void;
  setEditingCategoryId: (id: string | null) => void;
  updateItem: (id: string, data: { content?: string; memory_type?: string; categories?: string[] }) => Promise<void>;
  deleteItem: (id: string) => Promise<void>;
  createCategory: (name: string, description: string) => Promise<void>;
  updateCategory: (id: string, data: { summary?: string; description?: string }) => Promise<void>;
  loadAuditLogs: () => Promise<void>;
  setAuditFilter: (filter: { action?: string; target_type?: string }) => void;
}

export const useMemoryStore = create<MemoryState>((set, get) => ({
  available: false,
  backend: null,
  dashboardUrl: null,
  categories: [],
  items: [],
  resources: [],
  categoryItems: {},
  loading: true,

  auditLogs: [],
  auditLoading: false,
  auditFilter: { action: '', target_type: '' },

  activeTab: 'facts',
  selectedCategory: null,
  searchQuery: '',
  editingItemId: null,
  deletingItemId: null,
  editingCategoryId: null,

  load: async () => {
    try {
      const data = await api.getMemuData();
      set({
        available: data.available,
        backend: data.backend ?? null,
        dashboardUrl: data.dashboard_url ?? null,
        categories: data.categories || [],
        items: data.items || [],
        resources: data.resources || [],
        categoryItems: data.category_items || {},
        loading: false,
      });
    } catch {
      set({ loading: false });
    }
  },

  setActiveTab: (tab) => set({ activeTab: tab }),
  setSelectedCategory: (id) => set({ selectedCategory: id }),
  setSearchQuery: (q) => set({ searchQuery: q }),
  setEditingItemId: (id) => set({ editingItemId: id }),
  setDeletingItemId: (id) => set({ deletingItemId: id }),
  setEditingCategoryId: (id) => set({ editingCategoryId: id }),

  updateItem: async (id, data) => {
    await api.updateMemuItem(id, data);
    set({ editingItemId: null });
    await get().load();
  },

  deleteItem: async (id) => {
    await api.deleteMemuItem(id);
    set({ deletingItemId: null });
    await get().load();
  },

  createCategory: async (name, description) => {
    await api.createMemuCategory(name, description);
    await get().load();
  },

  updateCategory: async (id, data) => {
    await api.updateMemuCategory(id, data);
    set({ editingCategoryId: null });
    await get().load();
  },

  loadAuditLogs: async () => {
    set({ auditLoading: true });
    try {
      const { action, target_type } = get().auditFilter;
      const { logs } = await api.getMemuAuditLog({ action, target_type, limit: 200 });
      set({ auditLogs: logs, auditLoading: false });
    } catch {
      set({ auditLoading: false });
    }
  },

  setAuditFilter: (filter) => {
    set(s => ({ auditFilter: { ...s.auditFilter, ...filter } }));
    get().loadAuditLogs();
  },
}));
