import { memo } from 'react';
import { useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { Calendar, Clock, ExternalLink } from 'lucide-react';
import type { Task } from '../../../api/client';
import { formatTimeAgo } from '../../../utils/dateGroups';

/**
 * Deadline urgency, as a token class rather than a raw colour so it tracks
 * the theme. Overdue and due-today are the two states worth interrupting
 * someone for; anything further out is informational.
 */
function deadlineTone(deadline: string): string {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const due = new Date(`${deadline}T00:00:00`);
  if (Number.isNaN(due.getTime())) return 'text-text-dim';
  const days = Math.round((due.getTime() - today.getTime()) / 86_400_000);
  if (days < 0) return 'text-hue-red';
  if (days === 0) return 'text-hue-orange';
  if (days <= 3) return 'text-hue-yellow';
  return 'text-text-dim';
}

function parseTags(tags: string | null | undefined): string[] {
  return (tags || '').split(',').map((t) => t.trim()).filter(Boolean);
}

export interface BoardCardProps {
  task: Task;
  onOpen: (task: Task) => void;
}

/**
 * A single draggable card.
 *
 * Deliberately lighter than the list view's `TaskCard`: at ~280px there is
 * no room for the inline status `<select>`, and a card whose whole surface
 * is a drag handle shouldn't also contain a control that swallows pointer
 * events. Changing status here is the drag itself.
 */
function BoardCardInner({ task, onOpen }: BoardCardProps) {
  const {
    attributes, listeners, setNodeRef, transform, transition, isDragging,
  } = useSortable({ id: task.id, data: { type: 'task', task } });

  const tags = parseTags(task.tags);

  return (
    <div
      ref={setNodeRef}
      style={{ transform: CSS.Translate.toString(transform), transition }}
      {...attributes}
      {...listeners}
      onClick={() => onOpen(task)}
      // The drag sensor has a 4px activation distance, so a plain click
      // still reaches this handler and opens the task.
      className={`group text-left w-full p-3 bg-surface border border-border-subtle rounded-lg
        hover:border-border cursor-pointer transition-colors
        ${isDragging ? 'opacity-40' : ''}`}
    >
      <h3 className="text-[13px] font-medium text-text leading-snug line-clamp-2 mb-2">
        {task.title}
      </h3>

      {tags.length > 0 && (
        <div className="flex flex-wrap gap-1 mb-2">
          {tags.slice(0, 3).map((tag) => (
            <span
              key={tag}
              className="px-1.5 py-0.5 text-[10px] rounded bg-surface-raised text-text-dim border border-border-subtle"
            >
              {tag}
            </span>
          ))}
          {tags.length > 3 && (
            <span className="px-1 py-0.5 text-[10px] text-text-faint">
              +{tags.length - 3}
            </span>
          )}
        </div>
      )}

      <div className="flex items-center gap-2.5 text-[11px] flex-wrap">
        {task.deadline && (
          <span className={`flex items-center gap-1 ${deadlineTone(task.deadline)}`}>
            <Calendar size={10} /> {task.deadline}
          </span>
        )}
        {task.updated_at && (
          <span
            className="flex items-center gap-1 text-text-faint"
            title={`Updated ${task.updated_at}`}
          >
            <Clock size={10} /> {formatTimeAgo(task.updated_at)}
          </span>
        )}
        {task.source && task.source !== 'manual' && (
          <span className="text-text-faint">{task.source}</span>
        )}
        {task.source_url && (
          <a
            href={task.source_url}
            target="_blank"
            rel="noopener noreferrer"
            onClick={(e) => e.stopPropagation()}
            // Not a drag target: stop the sensor claiming the pointer so
            // the link is clickable rather than the start of a drag.
            onPointerDown={(e) => e.stopPropagation()}
            className="ml-auto text-text-faint hover:text-text-muted opacity-0 group-hover:opacity-100 transition-opacity"
            aria-label="Open source link"
          >
            <ExternalLink size={11} />
          </a>
        )}
      </div>
    </div>
  );
}

// Lanes re-render on every drag frame; without memo each card in every
// lane re-renders with them.
export const BoardCard = memo(BoardCardInner);

/** The card rendered under the cursor mid-drag (no sortable wiring). */
export function BoardCardOverlay({ task }: { task: Task }) {
  const tags = parseTags(task.tags);
  return (
    <div className="w-[272px] p-3 bg-surface border border-accent/50 rounded-lg shadow-xl rotate-2">
      <h3 className="text-[13px] font-medium text-text leading-snug line-clamp-2">
        {task.title}
      </h3>
      {tags.length > 0 && (
        <div className="flex flex-wrap gap-1 mt-2">
          {tags.slice(0, 3).map((tag) => (
            <span
              key={tag}
              className="px-1.5 py-0.5 text-[10px] rounded bg-surface-raised text-text-dim border border-border-subtle"
            >
              {tag}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
