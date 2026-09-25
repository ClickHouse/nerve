import { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { ChevronLeft, ChevronRight, Columns3, List, Plus, Search, SlidersHorizontal, X } from '../components/ui/icons';
import { Button, IconButton, Select, TextField } from '../components/ui';
import { useTaskStore, TASKS_PAGE_SIZE, type TaskSort, type TaskViewMode } from '../stores/taskStore';
import { useTaskStatusStore } from '../stores/taskStatusStore';
import { TaskFilters } from '../components/Tasks/TaskFilters';
import { TaskCard } from '../components/Tasks/TaskCard';
import { TaskCreateDialog } from '../components/Tasks/TaskCreateDialog';
import { TaskStatusManager } from '../components/Tasks/TaskStatusManager';
import { TaskBoard } from '../components/Tasks/Board/TaskBoard';
import { BoardFilterBar } from '../components/Tasks/Board/BoardFilterBar';
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts';
import { isModalOpen } from '../components/ui/modalStack';
import type { ShortcutDef } from '../utils/keyboard';
import { PageHeader } from '../components/ui/PageHeader';

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

  // A dialog on top owns the keyboard. This page stays mounted behind one —
  // always behind the task modal, which is the whole point of the background
  // location, and behind every other dialog too. Ungated, `n` opens a second
  // dialog over the first and `/` moves focus out of an aria-modal dialog
  // into a search box hidden behind the backdrop.
  const noDialogOpen = useCallback(() => !isModalOpen(), []);

  // Page-scoped, like ChatPage's — they only bind while /tasks is mounted.
  // Card-level navigation is dnd-kit's (Tab to a card, Space to pick up,
  // arrows to move), so these cover only what the page itself owns.
  const shortcuts = useMemo<ShortcutDef[]>(() => [
    {
      id: 'tasks-board-view',
      combo: { key: 'b' },
      description: 'Board view',
      section: 'tasks',
      when: noDialogOpen,
      action: () => setViewMode('board'),
    },
    {
      id: 'tasks-list-view',
      combo: { key: 'l' },
      description: 'List view',
      section: 'tasks',
      when: noDialogOpen,
      action: () => setViewMode('list'),
    },
    {
      id: 'tasks-new',
      combo: { key: 'n' },
      description: 'New task',
      section: 'tasks',
      when: noDialogOpen,
      action: () => setShowCreateDialog(true),
    },
    {
      id: 'tasks-focus-search',
      combo: { key: '/' },
      description: 'Focus task search',
      section: 'tasks',
      when: noDialogOpen,
      action: () => searchRef.current?.focus(),
    },
  ], [setViewMode, setShowCreateDialog, noDialogOpen]);

  useKeyboardShortcuts(shortcuts);

  return (
    <div className="h-full flex flex-col min-w-0">
      <PageHeader
        title="Tasks"
        filters={
          <>
            {/* A segmented control: `subtle` + `active` is the app's selected
                treatment, so the raised-surface segment becomes an accent tint. */}
            <div className="flex items-center bg-surface-raised border border-border-subtle rounded-lg p-0.5 shrink-0 mr-2">
              {VIEW_OPTIONS.map(({ value, label, Icon }) => (
                <Button
                  key={value}
                  variant="subtle"
                  size="xs"
                  active={viewMode === value}
                  onClick={() => setViewMode(value)}
                  aria-pressed={viewMode === value}
                  title={`${label} view`}
                >
                  <Icon size={13} /> {label}
                </Button>
              ))}
            </div>

            {/* Status pills are the list's filter; on the board every status
                is already a lane, so they'd only hide columns. */}
            {!isBoard && <TaskFilters active={filter} onChange={setFilter} />}
          </>
        }
        search={
          <div className="relative">
            <Search size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-text-faint" />
            {/* `pl-8 pr-7` survives the field's own `px-3`: Tailwind emits the
                per-side padding utilities after the axis ones. */}
            <TextField
              ref={searchRef}
              value={localQuery}
              onChange={e => handleSearchChange(e.target.value)}
              placeholder="Search..."
              fullWidth={false}
              className="pl-8 pr-7 w-48"
            />
            {localQuery && (
              <IconButton
                label="Clear search"
                size="xs"
                onClick={clearSearch}
                className="absolute right-2 top-1/2 -translate-y-1/2"
              >
                <X size={13} />
              </IconButton>
            )}
          </div>
        }
        actions={
          <>
            {!isSearching && !isBoard && (
              // The "Sort by" label costs more than it explains once space is
              // tight; the select still names itself via title/aria-label.
              <label className="flex items-center gap-1.5 text-xs text-text-faint">
                <span className="hidden lg:inline">Sort by</span>
                {/* The house `Select` is a native `<select>`; a portalling one
                    could not live inside this page's dialogs. */}
                <Select
                  value={sort}
                  onChange={e => setSort(e.target.value as TaskSort)}
                  title="Sort tasks"
                  aria-label="Sort tasks"
                  options={SORT_OPTIONS}
                />
              </label>
            )}
            <Button
              variant="secondary"
              size="md"
              onClick={() => setShowStatusManager(true)}
              title="Manage statuses"
              aria-label="Manage statuses"
              className="whitespace-nowrap"
            >
              <SlidersHorizontal size={14} /> <span className="hidden sm:inline">Statuses</span>
            </Button>
            <Button
              variant="primary"
              size="md"
              onClick={() => setShowCreateDialog(true)}
              // The label collapses to an icon on narrow screens, so the
              // button carries its name explicitly rather than relying on
              // text that is not always rendered.
              title="New task"
              aria-label="New task"
              className="whitespace-nowrap"
            >
              <Plus size={14} /> <span className="hidden sm:inline">New Task</span>
            </Button>
          </>
        }
      />

      {isBoard && <BoardFilterBar />}

      {isBoard ? (
        // The board owns its own scrolling: columns scroll vertically,
        // the rail of columns scrolls horizontally. No page-level scroll,
        // and no max-width — filling the viewport is the whole point.
        <TaskBoard onOpenTask={openTask} />
      ) : (
      <div className="flex-1 overflow-y-auto p-4 md:p-6">
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
              <div className="flex items-center justify-between pt-4 text-xs text-text-faint">
                <span>
                  Showing {pageStart}–{pageEnd} of {total}
                </span>
                {totalPages > 1 && (
                  <div className="flex items-center gap-1">
                    <IconButton
                      label="Previous page"
                      size="xs"
                      onClick={() => setPage(page - 1)}
                      disabled={!hasPrev}
                    >
                      <ChevronLeft size={14} />
                    </IconButton>
                    <span className="px-2 text-text-dim">
                      Page {page} of {totalPages}
                    </span>
                    <IconButton
                      label="Next page"
                      size="xs"
                      onClick={() => setPage(page + 1)}
                      disabled={!hasNext}
                    >
                      <ChevronRight size={14} />
                    </IconButton>
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
