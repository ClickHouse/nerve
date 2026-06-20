import { useEffect, useState } from 'react';
import { api } from '../../api/client';
import { useTaskStatusStore } from '../../stores/taskStatusStore';
import { StatusBadge, StatusSelect } from './StatusControls';

interface Task {
  id: string;
  title: string;
  status: string;
  deadline: string | null;
  source: string;
  created_at: string;
}

export function TaskList() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [filter, setFilter] = useState('');
  const [loading, setLoading] = useState(true);
  const statuses = useTaskStatusStore((s) => s.statuses);
  const loadStatuses = useTaskStatusStore((s) => s.load);

  const loadTasks = async () => {
    try {
      const { tasks } = await api.listTasks({ status: filter || undefined });
      setTasks(tasks);
    } catch (e) {
      console.error('Failed to load tasks:', e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { loadStatuses(); }, []);
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
          {statuses.map((s) => (
            <option key={s.name} value={s.name}>{s.label}</option>
          ))}
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
                  <div className="text-xs text-text-dim mt-1 flex items-center gap-2">
                    <StatusBadge status={task.status} />
                    {task.deadline && <span>Due: {task.deadline}</span>}
                    {task.source && <span>from {task.source}</span>}
                  </div>
                </div>
                <StatusSelect
                  value={task.status}
                  onChange={(status) => handleStatusChange(task.id, status)}
                  className="text-xs px-1.5 py-0.5 bg-surface-raised border border-border-subtle rounded text-text-muted outline-none"
                />
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
