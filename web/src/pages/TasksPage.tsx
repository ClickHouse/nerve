import { useEffect, useRef, useState, useCallback } from 'react';
import { ChevronLeft, ChevronRight, Plus, Search, X } from 'lucide-react';
import { useTaskStore, TASKS_PAGE_SIZE, type TaskSort } from '../stores/taskStore';
import { TaskFilters } from '../components/Tasks/TaskFilters';
import { TaskCard } from '../components/Tasks/TaskCard';
import { TaskCreateDialog } from '../components/Tasks/TaskCreateDialog';

const SORT_OPTIONS: { value: TaskSort; label: string }[] = [
  { value: 'deadline', label: 'Deadline' },
  { value: 'updated_at', label: 'Last update' },
  { value: 'created_at', label: 'Created' },
];

export function TasksPage() {
  const {
    tasks, filter, searchQuery, sort, page, total, loading, showCreateDialog,
    loadTasks, setFilter, setSearch, setSort, setPage,
    updateStatus, createTask, setShowCreateDialog,
  } = useTaskStore();

  const [localQuery, setLocalQuery] = useState(searchQuery);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  useEffect(() => { loadTasks(); }, []);

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

  return (
    <div className="h-full flex flex-col">
      <div className="border-b border-border-subtle px-6 py-3 flex items-center justify-between bg-bg shrink-0">
        <div className="flex items-center gap-4">
          <h1 className="text-lg font-semibold">Tasks</h1>
          <TaskFilters active={filter} onChange={setFilter} />

          <div className="relative ml-2">
            <Search size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-text-faint" />
            <input
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
        <div className="flex items-center gap-2">
          {!isSearching && (
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
            onClick={() => setShowCreateDialog(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 text-[13px] bg-accent hover:bg-accent-hover text-white rounded-lg cursor-pointer"
          >
            <Plus size={14} /> New Task
          </button>
        </div>
      </div>

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

      {showCreateDialog && (
        <TaskCreateDialog
          onClose={() => setShowCreateDialog(false)}
          onCreate={createTask}
        />
      )}
    </div>
  );
}
