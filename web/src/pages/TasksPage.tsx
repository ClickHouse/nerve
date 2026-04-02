import { useEffect, useRef, useState, useCallback } from 'react';
import { Plus, Search, X } from 'lucide-react';
import { useTaskStore } from '../stores/taskStore';
import { TaskFilters } from '../components/Tasks/TaskFilters';
import { TaskCard } from '../components/Tasks/TaskCard';
import { TaskCreateDialog } from '../components/Tasks/TaskCreateDialog';

export function TasksPage() {
  const {
    tasks, filter, searchQuery, loading, showCreateDialog,
    loadTasks, setFilter, setSearch, updateStatus, createTask, setShowCreateDialog,
  } = useTaskStore();

  const [localQuery, setLocalQuery] = useState(searchQuery);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  useEffect(() => { loadTasks(); }, []);

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
                text-text-secondary placeholder-[#555] focus:outline-none focus:border-[#6366f1]/50
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
        <button
          onClick={() => setShowCreateDialog(true)}
          className="flex items-center gap-1.5 px-3 py-1.5 text-[13px] bg-[#6366f1] hover:bg-[#818cf8] text-white rounded-lg cursor-pointer"
        >
          <Plus size={14} /> New Task
        </button>
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
