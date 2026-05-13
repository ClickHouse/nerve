import { create } from 'zustand';
import { api } from '../api/client';

export interface Task {
  id: string;
  title: string;
  status: string;
  deadline: string | null;
  source: string;
  source_url: string | null;
  created_at: string;
  updated_at: string;
  content?: string;
}

export type TaskSort = 'deadline' | 'updated_at' | 'created_at';

export const TASKS_PAGE_SIZE = 50;

interface TaskState {
  tasks: Task[];
  filter: string;
  searchQuery: string;
  sort: TaskSort;
  page: number;
  total: number;
  loading: boolean;
  showCreateDialog: boolean;

  // Detail view
  selectedTask: Task | null;
  detailLoading: boolean;
  saving: boolean;

  loadTasks: () => Promise<void>;
  setFilter: (f: string) => void;
  setSearch: (q: string) => void;
  setSort: (s: TaskSort) => void;
  setPage: (p: number) => void;
  updateStatus: (id: string, status: string) => Promise<void>;
  createTask: (title: string, content: string, deadline: string) => Promise<void>;
  setShowCreateDialog: (show: boolean) => void;

  loadTask: (id: string) => Promise<void>;
  saveTaskContent: (id: string, content: string) => Promise<void>;
  clearSelectedTask: () => void;
}

export const useTaskStore = create<TaskState>((set, get) => ({
  tasks: [],
  filter: '',
  searchQuery: '',
  sort: 'deadline',
  page: 1,
  total: 0,
  loading: true,
  showCreateDialog: false,

  selectedTask: null,
  detailLoading: false,
  saving: false,

  loadTasks: async () => {
    set({ loading: true });
    try {
      const { filter, searchQuery, sort, page } = get();
      const result = searchQuery
        ? await api.searchTasks(searchQuery, filter || undefined)
        : await api.listTasks({
            status: filter || undefined,
            sort,
            limit: TASKS_PAGE_SIZE,
            offset: (page - 1) * TASKS_PAGE_SIZE,
          });
      set({ tasks: result.tasks, total: result.total ?? result.tasks.length, loading: false });
    } catch (e) {
      console.error('Failed to load tasks:', e);
      set({ loading: false });
    }
  },

  setFilter: (f: string) => {
    set({ filter: f, page: 1 });
    get().loadTasks();
  },

  setSearch: (q: string) => {
    set({ searchQuery: q, page: 1 });
    get().loadTasks();
  },

  setSort: (s: TaskSort) => {
    set({ sort: s, page: 1 });
    get().loadTasks();
  },

  setPage: (p: number) => {
    set({ page: Math.max(1, p) });
    get().loadTasks();
  },

  updateStatus: async (id: string, status: string) => {
    await api.updateTask(id, { status });
    // Update selected task inline if viewing it
    const sel = get().selectedTask;
    if (sel && sel.id === id) {
      set({ selectedTask: { ...sel, status } });
    }
    get().loadTasks();
  },

  createTask: async (title: string, content: string, deadline: string) => {
    await api.createTask({ title, content, deadline });
    set({ showCreateDialog: false, page: 1 });
    get().loadTasks();
  },

  setShowCreateDialog: (show: boolean) => set({ showCreateDialog: show }),

  loadTask: async (id: string) => {
    set({ detailLoading: true, selectedTask: null });
    try {
      const task = await api.getTask(id);
      set({ selectedTask: task, detailLoading: false });
    } catch (e) {
      console.error('Failed to load task:', e);
      set({ detailLoading: false });
    }
  },

  saveTaskContent: async (id: string, content: string) => {
    set({ saving: true });
    try {
      await api.updateTask(id, { content });
      // Update local state
      const sel = get().selectedTask;
      if (sel && sel.id === id) {
        set({ selectedTask: { ...sel, content } });
      }
    } catch (e) {
      console.error('Failed to save task:', e);
    } finally {
      set({ saving: false });
    }
  },

  clearSelectedTask: () => set({ selectedTask: null }),
}));
