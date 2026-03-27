import { useNavigate } from 'react-router-dom';
import { Calendar, ExternalLink } from 'lucide-react';
import type { Task } from '../../stores/taskStore';
import { TASK_STATUS_STYLES as STATUS_STYLES } from '../../constants/statusStyles';

export function TaskCard({ task, onStatusChange }: {
  task: Task;
  onStatusChange: (id: string, status: string) => void;
}) {
  const navigate = useNavigate();

  return (
    <div
      onClick={() => navigate(`/tasks/${task.id}`)}
      className="p-4 bg-[#141414] border border-[#222] rounded-lg hover:border-[#444] transition-colors cursor-pointer"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <h3 className="font-medium text-[15px] text-[#e0e0e0] mb-1">{task.title}</h3>
          <div className="flex items-center gap-3 text-[12px]">
            <span className={`px-2 py-0.5 rounded-full border ${STATUS_STYLES[task.status] || STATUS_STYLES.deferred}`}>
              {task.status}
            </span>
            {task.deadline && (
              <span className="flex items-center gap-1 text-[#666]">
                <Calendar size={11} /> {task.deadline}
              </span>
            )}
            {task.source && (
              <span className="text-[#555]">from {task.source}</span>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0" onClick={e => e.stopPropagation()}>
          {task.source_url && (
            <a
              href={task.source_url}
              target="_blank"
              rel="noopener noreferrer"
              className="p-1.5 text-[#555] hover:text-[#aaa] hover:bg-[#1f1f1f] rounded cursor-pointer"
            >
              <ExternalLink size={14} />
            </a>
          )}
          <select
            value={task.status}
            onChange={(e) => onStatusChange(task.id, e.target.value)}
            className="text-[12px] px-2 py-1 bg-[#1a1a1a] border border-[#2a2a2a] rounded text-[#aaa] outline-none cursor-pointer"
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
