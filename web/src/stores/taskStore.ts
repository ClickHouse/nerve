import { create } from 'zustand';
import { api, type Task } from '../api/client';

export type { Task };

export type TaskSort = 'deadline' | 'updated_at' | 'created_at';
export type TaskViewMode = 'board' | 'list';

export const TASKS_PAGE_SIZE = 50;

/** One board column: a page of its tasks plus the lane's true total. */
export interface Lane {
  status: string;
  total: number;
  tasks: Task[];
}

/**
 * Where a dragged card should land, expressed as its new neighbours.
 *
 * The server resolves this into a rank. Sending intent rather than a
 * computed position means a board that's a few seconds stale still
 * produces a sane result instead of overwriting someone else's ordering
 * with numbers derived from a lane it no longer matches.
 */
export interface MoveIntent {
  status: string;
  beforeId: string | null;
  afterId: string | null;
}

const VIEW_KEY = 'nerve_tasks_view';

/** localStorage is unavailable in private modes; never let it break a render. */
function readStoredView(): TaskViewMode | null {
  try {
    const raw = localStorage.getItem(VIEW_KEY);
    return raw === 'board' || raw === 'list' ? raw : null;
  } catch {
    return null;
  }
}

function writeStoredView(mode: TaskViewMode): void {
  try {
    localStorage.setItem(VIEW_KEY, mode);
  } catch {
    /* not fatal — the choice just won't survive a reload */
  }
}

/** Board below this width is unusable; the list is the better default. */
const BOARD_MIN_WIDTH = 1280;

function defaultViewMode(): TaskViewMode {
  const stored = readStoredView();
  if (stored) return stored;
  if (typeof window === 'undefined') return 'list';
  return window.innerWidth >= BOARD_MIN_WIDTH ? 'board' : 'list';
}

interface TaskState {
  // List view
  tasks: Task[];
  filter: string;
  searchQuery: string;
  sort: TaskSort;
  page: number;
  total: number;
  loading: boolean;
  showCreateDialog: boolean;

  // Board view
  viewMode: TaskViewMode;
  lanes: Lane[];
  boardLoading: boolean;
  boardError: string | null;
  tagFilter: string;
  availableTags: { name: string; count: number }[];
  /** Lane to pre-select when the create dialog is opened from a column. */
  createInStatus: string | null;

  // Detail view
  selectedTask: Task | null;
  detailLoading: boolean;
  saving: boolean;

  loadTasks: () => Promise<void>;
  loadBoard: (opts?: { quiet?: boolean }) => Promise<void>;
  loadTags: () => Promise<void>;
  setViewMode: (mode: TaskViewMode) => void;
  setTagFilter: (tag: string) => void;
  moveTask: (taskId: string, intent: MoveIntent) => Promise<void>;
  applyLocalMove: (taskId: string, intent: MoveIntent) => void;
  handleTaskEvent: (task: Task) => void;
  setFilter: (f: string) => void;
  setSearch: (q: string) => void;
  setSort: (s: TaskSort) => void;
  setPage: (p: number) => void;
  updateStatus: (id: string, status: string) => Promise<void>;
  createTask: (title: string, content: string, deadline: string) => Promise<void>;
  setShowCreateDialog: (show: boolean, status?: string | null) => void;

  loadTask: (id: string) => Promise<void>;
  saveTaskContent: (id: string, content: string) => Promise<void>;
  clearSelectedTask: () => void;
}

/** Remove a task from every lane, returning the pruned lanes. */
function withoutTask(lanes: Lane[], taskId: string): Lane[] {
  return lanes.map((lane) => {
    const kept = lane.tasks.filter((t) => t.id !== taskId);
    return kept.length === lane.tasks.length
      ? lane
      : { ...lane, tasks: kept, total: Math.max(0, lane.total - 1) };
  });
}

/** Insert a task into `status` at the slot described by the intent. */
function withTaskAt(lanes: Lane[], task: Task, intent: MoveIntent): Lane[] {
  return lanes.map((lane) => {
    if (lane.status !== intent.status) return lane;
    const tasks = [...lane.tasks];
    let index = tasks.length;
    if (intent.afterId) {
      const at = tasks.findIndex((t) => t.id === intent.afterId);
      if (at !== -1) index = at;
    } else if (intent.beforeId) {
      const at = tasks.findIndex((t) => t.id === intent.beforeId);
      if (at !== -1) index = at + 1;
    } else {
      // No anchors: an empty lane, or a drop on the column background.
      index = tasks.length;
    }
    tasks.splice(index, 0, { ...task, status: intent.status });
    return { ...lane, tasks, total: lane.total + 1 };
  });
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

  viewMode: defaultViewMode(),
  lanes: [],
  boardLoading: true,
  boardError: null,
  tagFilter: '',
  availableTags: [],
  createInStatus: null,

  selectedTask: null,
  detailLoading: false,
  saving: false,

  loadTasks: async () => {
    set({ loading: true });
    try {
      const { filter, searchQuery, sort, page, tagFilter } = get();
      const result = searchQuery
        ? await api.searchTasks(searchQuery, filter || undefined)
        : await api.listTasks({
            status: filter || undefined,
            tag: tagFilter || undefined,
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

  /**
   * `quiet` refetches without flipping the spinner — used to resync after a
   * failed drag, where the board is already on screen and blanking it would
   * be a worse experience than a brief inconsistency.
   */
  loadBoard: async ({ quiet = false } = {}) => {
    // Only an explicit load clears the error. A quiet resync is usually
    // triggered *by* a failure, so clearing here would wipe the message
    // explaining what just went wrong before it could be read.
    if (!quiet) set({ boardLoading: true, boardError: null });
    try {
      const { tagFilter } = get();
      const { lanes } = await api.getTaskBoard({ tag: tagFilter || undefined });
      set({ lanes, boardLoading: false });
    } catch (e) {
      console.error('Failed to load board:', e);
      set({ boardLoading: false, boardError: 'Could not load the board.' });
    }
  },

  loadTags: async () => {
    try {
      const { tags } = await api.listTaskTags();
      set({ availableTags: tags });
    } catch (e) {
      console.error('Failed to load tags:', e);
    }
  },

  setViewMode: (mode) => {
    writeStoredView(mode);
    set({ viewMode: mode });
    if (mode === 'board') void get().loadBoard();
    else void get().loadTasks();
  },

  setTagFilter: (tag) => {
    set({ tagFilter: tag, page: 1 });
    if (get().viewMode === 'board') void get().loadBoard();
    else void get().loadTasks();
  },

  /** Reorder lanes in place, without touching the server. */
  applyLocalMove: (taskId, intent) => {
    const { lanes } = get();
    const task = lanes.flatMap((l) => l.tasks).find((t) => t.id === taskId);
    if (!task) return;
    set({ lanes: withTaskAt(withoutTask(lanes, taskId), task, intent) });
  },

  moveTask: async (taskId, intent) => {
    // Snapshot before the optimistic write so a failure can restore the
    // exact prior order rather than approximating it.
    const snapshot = get().lanes;
    get().applyLocalMove(taskId, intent);

    try {
      const { task } = await api.moveTask(taskId, {
        status: intent.status,
        before_id: intent.beforeId,
        after_id: intent.afterId,
      });
      // Reconcile against the server's authoritative row — mainly to pick
      // up the real `position` and `updated_at`, and any file move that
      // came with a status change.
      set({
        lanes: get().lanes.map((lane) => ({
          ...lane,
          tasks: lane.tasks.map((t) => (t.id === task.id ? { ...t, ...task } : t)),
        })),
        // A move that lands clears any stale failure banner.
        boardError: null,
      });
    } catch (e) {
      console.error('Move failed:', e);
      set({ lanes: snapshot, boardError: 'Move failed — the board has been restored.' });
      // Something rejected the move (a status that vanished, a task deleted
      // from under us); resync rather than trusting the snapshot for long.
      void get().loadBoard({ quiet: true });
    }
  },

  /**
   * Apply a `task_updated` broadcast.
   *
   * Fires for changes this client didn't make — the agent working in
   * another session, a second tab, the HTTP API. It's also echoed back for
   * changes this client *did* make, so it has to be idempotent: replacing
   * in place when the lane is unchanged, and only re-slotting on an actual
   * status change.
   */
  handleTaskEvent: (task) => {
    const { lanes, selectedTask } = get();

    if (selectedTask?.id === task.id) {
      // Keep any locally-loaded markdown; the broadcast carries the row only.
      set({ selectedTask: { ...selectedTask, ...task } });
    }

    if (lanes.length === 0) return;

    const currentLane = lanes.find((l) => l.tasks.some((t) => t.id === task.id));

    if (currentLane?.status === task.status) {
      set({
        lanes: lanes.map((lane) =>
          lane.status !== task.status
            ? lane
            : { ...lane, tasks: lane.tasks.map((t) => (t.id === task.id ? { ...t, ...task } : t)) },
        ),
      });
      return;
    }

    const pruned = currentLane ? withoutTask(lanes, task.id) : lanes;
    const target = pruned.find((l) => l.status === task.status);
    if (!target) {
      // A status this board doesn't know about yet (someone just created
      // one). A full reload is the only way to get its lane.
      void get().loadBoard({ quiet: true });
      return;
    }

    // Insert by rank so a card from elsewhere lands where the server would
    // have put it, rather than at whichever end is convenient.
    set({
      lanes: pruned.map((lane) => {
        if (lane.status !== task.status) return lane;
        const tasks = [...lane.tasks];
        const at = tasks.findIndex((t) => t.position > task.position);
        tasks.splice(at === -1 ? tasks.length : at, 0, task);
        // withoutTask already decremented the source lane, so this is a
        // plain +1 whether the card came from another lane or from nowhere
        // (a task created directly into this status).
        return { ...lane, tasks, total: lane.total + 1 };
      }),
    });
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
    const sel = get().selectedTask;
    if (sel && sel.id === id) {
      set({ selectedTask: { ...sel, status } });
    }
    // The WS broadcast updates the board; only the list needs a refetch.
    if (get().viewMode === 'list') get().loadTasks();
  },

  createTask: async (title: string, content: string, deadline: string) => {
    const { createInStatus } = get();
    await api.createTask({
      title,
      content,
      deadline,
      ...(createInStatus ? { status: createInStatus } : {}),
    });
    set({ showCreateDialog: false, createInStatus: null, page: 1 });
    if (get().viewMode === 'board') void get().loadBoard({ quiet: true });
    else get().loadTasks();
  },

  setShowCreateDialog: (show: boolean, status: string | null = null) =>
    set({ showCreateDialog: show, createInStatus: show ? status : null }),

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
