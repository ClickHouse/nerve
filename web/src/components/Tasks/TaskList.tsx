import { useEffect, useState } from 'react';
import { api } from '../../api/client';

interface Task {
  id: string;
  title: string;
  status: string;
  deadline: string | null;
  source: string;
  created_at: string;
}

const STATUS_COLORS: Record<string, string> = {
  pending: 'text-yellow-400',
  in_progress: 'text-blue-400',
  done: 'text-green-400',
  deferred: 'text-text-muted',
};

export function TaskList() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [filter, setFilter] = useState('');
  const [loading, setLoading] = useState(true);

  const loadTasks = async () => {
    try {
      const { tasks } = await api.listTasks(filter || undefined);
      setTasks(tasks);
    } catch (e) {
      console.error('Failed to load tasks:', e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { loadTasks(); }, [filter]);

  const handleStatusChange = async (id: string, newStatus: string) => {
    await api.updateTask(id, { status: newStatus });
    loadTasks();
  };

  return (
    <div className="p-4">
      <div className="flex items-center gap-2 mb-4">
        <h2 className="text-lg font-semibold">Tasks</h2>
        <select
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="text-sm px-2 py-1 bg-surface-raised border border-border-subtle rounded text-text outline-none"
        >
          <option value="">Active</option>
          <option value="pending">Pending</option>
          <option value="in_progress">In Progress</option>
          <option value="done">Done</option>
          <option value="deferred">Deferred</option>
        </select>
      </div>

      {loading ? (
        <div className="text-text-faint">Loading...</div>
      ) : tasks.length === 0 ? (
        <div className="text-text-faint">No tasks</div>
      ) : (
        <div className="space-y-2">
          {tasks.map((task) => (
            <div
              key={task.id}
              className="p-3 bg-surface-raised border border-border-subtle rounded"
            >
              <div className="flex items-start justify-between">
                <div>
                  <div className="font-medium">{task.title}</div>
                  <div className="text-xs text-text-dim mt-1">
                    <span className={STATUS_COLORS[task.status] || ''}>{task.status}</span>
                    {task.deadline && <span className="ml-2">Due: {task.deadline}</span>}
                    {task.source && <span className="ml-2">from {task.source}</span>}
                  </div>
                </div>
                <select
                  value={task.status}
                  onChange={(e) => handleStatusChange(task.id, e.target.value)}
                  className="text-xs px-1.5 py-0.5 bg-surface-raised border border-border-subtle rounded text-text-muted outline-none"
                >
                  <option value="pending">Pending</option>
                  <option value="in_progress">In Progress</option>
                  <option value="done">Done</option>
                  <option value="deferred">Deferred</option>
                </select>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
