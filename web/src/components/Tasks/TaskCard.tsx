import { useNavigate } from 'react-router-dom';
import { Calendar, ExternalLink } from 'lucide-react';
import type { Task } from '../../stores/taskStore';

const STATUS_STYLES: Record<string, string> = {
  pending: 'bg-yellow-400/10 text-yellow-400 border-yellow-400/20',
  in_progress: 'bg-blue-400/10 text-blue-400 border-blue-400/20',
  done: 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20',
  deferred: 'bg-border-subtle/50 text-text-muted border-border-subtle',
};

export function TaskCard({ task, onStatusChange }: {
  task: Task;
  onStatusChange: (id: string, status: string) => void;
}) {
  const navigate = useNavigate();

  return (
    <div
      onClick={() => navigate(`/tasks/${task.id}`)}
      className="p-4 bg-surface border border-border-subtle rounded-lg hover:border-border transition-colors cursor-pointer"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <h3 className="font-medium text-[15px] text-text mb-1">{task.title}</h3>
          <div className="flex items-center gap-3 text-[12px]">
            <span className={`px-2 py-0.5 rounded-full border ${STATUS_STYLES[task.status] || STATUS_STYLES.deferred}`}>
              {task.status}
            </span>
            {task.deadline && (
              <span className="flex items-center gap-1 text-text-dim">
                <Calendar size={11} /> {task.deadline}
              </span>
            )}
            {task.source && (
              <span className="text-text-faint">from {task.source}</span>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0" onClick={e => e.stopPropagation()}>
          {task.source_url && (
            <a
              href={task.source_url}
              target="_blank"
              rel="noopener noreferrer"
              className="p-1.5 text-text-faint hover:text-text-muted hover:bg-surface-hover rounded cursor-pointer"
            >
              <ExternalLink size={14} />
            </a>
          )}
          <select
            value={task.status}
            onChange={(e) => onStatusChange(task.id, e.target.value)}
            className="text-[12px] px-2 py-1 bg-surface-raised border border-border rounded text-text-muted outline-none cursor-pointer"
          >
            <option value="pending">Pending</option>
            <option value="in_progress">In Progress</option>
            <option value="done">Done</option>
            <option value="deferred">Deferred</option>
          </select>
        </div>
      </div>
    </div>
  );
}
