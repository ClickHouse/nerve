import { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { ChevronLeft, ChevronRight, Columns3, List, Plus, Search, SlidersHorizontal, X } from 'lucide-react';
import { useTaskStore, TASKS_PAGE_SIZE, type TaskSort, type TaskViewMode } from '../stores/taskStore';
import { useTaskStatusStore } from '../stores/taskStatusStore';
import { TaskFilters } from '../components/Tasks/TaskFilters';
import { TaskCard } from '../components/Tasks/TaskCard';
import { TaskCreateDialog } from '../components/Tasks/TaskCreateDialog';
import { TaskStatusManager } from '../components/Tasks/TaskStatusManager';
import { TaskBoard } from '../components/Tasks/Board/TaskBoard';
import { BoardFilterBar } from '../components/Tasks/Board/BoardFilterBar';
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts';
import type { ShortcutDef } from '../utils/keyboard';

const SORT_OPTIONS: { value: TaskSort; label: string }[] = [
  { value: 'deadline', label: 'Deadline' },
  { value: 'updated_at', label: 'Last update' },
  { value: 'created_at', label: 'Created' },
];

const VIEW_OPTIONS: { value: TaskViewMode; label: string; Icon: typeof List }[] = [
  { value: 'board', label: 'Board', Icon: Columns3 },
  { value: 'list', label: 'List', Icon: List },
];

export function TasksPage() {
  const {
    tasks, filter, searchQuery, sort, page, total, loading, showCreateDialog,
    viewMode, loadTasks, loadBoard, loadTags, setViewMode,
    setFilter, setSearch, setSort, setPage,
    updateStatus, createTask, setShowCreateDialog,
  } = useTaskStore();

  const loadStatuses = useTaskStatusStore((s) => s.load);
  const navigate = useNavigate();
  const location = useLocation();

  const [localQuery, setLocalQuery] = useState(searchQuery);
  const [showStatusManager, setShowStatusManager] = useState(false);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const searchRef = useRef<HTMLInputElement>(null);

  const isBoard = viewMode === 'board';

  useEffect(() => {
    loadStatuses();
    loadTags();
    if (isBoard) loadBoard();
    else loadTasks();
    // Mount-only: view switches load through setViewMode.
  }, []);

  const isSearching = searchQuery.trim().length > 0;
  const pageStart = total === 0 ? 0 : (page - 1) * TASKS_PAGE_SIZE + 1;
  const pageEnd = Math.min(page * TASKS_PAGE_SIZE, total);
  const totalPages = Math.max(1, Math.ceil(total / TASKS_PAGE_SIZE));
  const hasPrev = page > 1;
  const hasNext = page < totalPages;

  const handleSearchChange = useCallback((value: string) => {
    setLocalQuery(value);
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => setSearch(value), 250);
  }, [setSearch]);

  const clearSearch = useCallback(() => {
    setLocalQuery('');
    clearTimeout(debounceRef.current);
    setSearch('');
  }, [setSearch]);

  // Cleanup debounce on unmount
  useEffect(() => () => clearTimeout(debounceRef.current), []);

  // Open a task over the board: /tasks/:id renders as a modal when it's
  // reached from here, and as the full page on a cold load or refresh.
  const openTask = useCallback((task: { id: string }) => {
    navigate(`/tasks/${task.id}`, { state: { background: location } });
  }, [navigate, location]);

  // Page-scoped, like ChatPage's — they only bind while /tasks is mounted.
  // Card-level navigation is dnd-kit's (Tab to a card, Space to pick up,
  // arrows to move), so these cover only what the page itself owns.
  const shortcuts = useMemo<ShortcutDef[]>(() => [
    {
      id: 'tasks-board-view',
      combo: { key: 'b' },
      description: 'Board view',
      section: 'tasks',
      action: () => setViewMode('board'),
    },
    {
      id: 'tasks-list-view',
      combo: { key: 'l' },
      description: 'List view',
      section: 'tasks',
      action: () => setViewMode('list'),
    },
    {
      id: 'tasks-new',
      combo: { key: 'n' },
      description: 'New task',
      section: 'tasks',
      action: () => setShowCreateDialog(true),
    },
    {
      id: 'tasks-focus-search',
      combo: { key: '/' },
      description: 'Focus task search',
      section: 'tasks',
      action: () => searchRef.current?.focus(),
    },
  ], [setViewMode, setShowCreateDialog]);

  useKeyboardShortcuts(shortcuts);

  return (
    <div className="h-full flex flex-col min-w-0">
      <div className="border-b border-border-subtle px-4 py-3 flex items-center justify-between gap-4 bg-bg shrink-0">
        <div className="flex items-center gap-3 min-w-0">
          <h1 className="text-lg font-semibold shrink-0">Tasks</h1>

          <div className="flex items-center bg-surface-raised border border-border-subtle rounded-lg p-0.5 shrink-0">
            {VIEW_OPTIONS.map(({ value, label, Icon }) => (
              <button
                key={value}
                onClick={() => setViewMode(value)}
                aria-pressed={viewMode === value}
                title={`${label} view`}
                className={`flex items-center gap-1.5 px-2.5 py-1 text-[12px] rounded-md cursor-pointer transition-colors
                  ${viewMode === value
                    ? 'bg-surface text-text shadow-sm'
                    : 'text-text-faint hover:text-text-secondary'}`}
              >
                <Icon size={13} /> {label}
              </button>
            ))}
          </div>

          {/* Status pills are the list's filter; on the board every status
              is already a lane, so they'd only hide columns. */}
          {!isBoard && <TaskFilters active={filter} onChange={setFilter} />}

          <div className="relative ml-2 shrink-0">
            <Search size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-text-faint" />
            <input
              ref={searchRef}
              type="text"
              value={localQuery}
              onChange={e => handleSearchChange(e.target.value)}
              placeholder="Search..."
              className="pl-8 pr-7 py-1.5 w-48 text-[13px] bg-surface-raised border border-border-subtle rounded-lg
                text-text-secondary placeholder:text-placeholder focus:outline-none focus:border-accent/50
                transition-colors"
            />
            {localQuery && (
              <button
                onClick={clearSearch}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-text-faint hover:text-text-muted cursor-pointer"
              >
                <X size={13} />
              </button>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          {!isSearching && !isBoard && (
            <label className="flex items-center gap-1.5 text-[12px] text-text-faint">
              Sort by
              <select
                value={sort}
                onChange={e => setSort(e.target.value as TaskSort)}
                className="px-2 py-1.5 text-[13px] bg-surface-raised border border-border-subtle rounded-lg text-text-secondary focus:outline-none focus:border-accent/50 cursor-pointer"
              >
                {SORT_OPTIONS.map(opt => (
                  <option key={opt.value} value={opt.value}>{opt.label}</option>
                ))}
              </select>
            </label>
          )}
          <button
            onClick={() => setShowStatusManager(true)}
            title="Manage statuses"
            className="flex items-center gap-1.5 px-3 py-1.5 text-[13px] text-text-secondary bg-surface-raised border border-border-subtle hover:border-border rounded-lg cursor-pointer"
          >
            <SlidersHorizontal size={14} /> Statuses
          </button>
          <button
            onClick={() => setShowCreateDialog(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 text-[13px] bg-accent hover:bg-accent-hover text-white rounded-lg cursor-pointer"
          >
            <Plus size={14} /> New Task
          </button>
        </div>
      </div>

      {isBoard && <BoardFilterBar />}

      {isBoard ? (
        // The board owns its own scrolling: columns scroll vertically,
        // the rail of columns scrolls horizontally. No page-level scroll,
        // and no max-width — filling the viewport is the whole point.
        <TaskBoard onOpenTask={openTask} />
      ) : (
      <div className="flex-1 overflow-y-auto p-6">
        {loading ? (
          <div className="text-text-faint text-center py-10">Loading...</div>
        ) : tasks.length === 0 ? (
          <div className="text-text-faint text-center py-10">
            {searchQuery ? `No tasks matching "${searchQuery}"` : 'No tasks'}
          </div>
        ) : (
          <div className="max-w-3xl mx-auto space-y-2">
            {tasks.map(task => (
              <TaskCard key={task.id} task={task} onStatusChange={updateStatus} />
            ))}

            {!isSearching && total > 0 && (
              <div className="flex items-center justify-between pt-4 text-[12px] text-text-faint">
                <span>
                  Showing {pageStart}–{pageEnd} of {total}
                </span>
                {totalPages > 1 && (
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => setPage(page - 1)}
                      disabled={!hasPrev}
                      className="p-1.5 rounded-md text-text-dim hover:bg-surface-raised hover:text-text-muted disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer"
                      aria-label="Previous page"
                    >
                      <ChevronLeft size={14} />
                    </button>
                    <span className="px-2 text-text-dim">
                      Page {page} of {totalPages}
                    </span>
                    <button
                      onClick={() => setPage(page + 1)}
                      disabled={!hasNext}
                      className="p-1.5 rounded-md text-text-dim hover:bg-surface-raised hover:text-text-muted disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer"
                      aria-label="Next page"
                    >
                      <ChevronRight size={14} />
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
      )}

      {showCreateDialog && (
        <TaskCreateDialog
          onClose={() => setShowCreateDialog(false)}
          onCreate={createTask}
        />
      )}

      {showStatusManager && (
        <TaskStatusManager onClose={() => setShowStatusManager(false)} />
      )}
    </div>
  );
}
