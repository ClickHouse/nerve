import { useNavigate } from 'react-router-dom';
import { Calendar, Clock, ExternalLink } from 'lucide-react';
import type { Task } from '../../stores/taskStore';
import { formatTimeAgo } from '../../utils/dateGroups';
import { StatusBadge, StatusSelect } from './StatusControls';

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
      {/* On a phone the status select held ~110px against a title that then
          had to wrap inside ~200px. It drops to the meta line instead, where
          there is room to spare, and the title gets the full card width.
          Ordering does it without duplicating anything:

            phone   title / meta + controls
            ≥ sm    title + controls / meta                                */}
      <div className="flex flex-wrap items-start gap-x-3 gap-y-2">
        <h3 className="font-medium text-[15px] text-text min-w-0 basis-full sm:basis-0 sm:flex-1 order-0">
          {task.title}
        </h3>

        <div className="flex items-center gap-3 text-[12px] min-w-0 order-2 sm:order-3 sm:basis-full">
          {/* Hidden on a phone: the select sitting beside it already names
              the status, and one value is not worth showing twice. */}
          <span className="hidden sm:flex">
            <StatusBadge status={task.status} />
          </span>
          {task.deadline && (
            <span className="flex items-center gap-1 text-text-dim whitespace-nowrap">
              <Calendar size={11} /> {task.deadline}
            </span>
          )}
          {task.updated_at && (
            <span
              className="flex items-center gap-1 text-text-faint whitespace-nowrap"
              title={`Updated ${task.updated_at}`}
            >
              <Clock size={11} /> {formatTimeAgo(task.updated_at)}
            </span>
          )}
          {task.source && (
            <span className="text-text-faint truncate">from {task.source}</span>
          )}
        </div>

        <div
          className="flex items-center gap-2 shrink-0 ml-auto order-3 sm:order-2"
          onClick={e => e.stopPropagation()}
        >
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
          <StatusSelect
            value={task.status}
            onChange={(status) => onStatusChange(task.id, status)}
            className="text-[12px] px-2 py-1 bg-surface-raised border border-border rounded text-text-muted outline-none cursor-pointer"
          />
        </div>
      </div>
    </div>
  );
}
