import { useDroppable } from '@dnd-kit/core';
import { SortableContext, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { ChevronLeft, ChevronRight, Plus } from 'lucide-react';
import type { Task, TaskStatusDef } from '../../../api/client';
import type { Lane } from '../../../stores/taskStore';
import { BoardCard } from './BoardCard';

export interface BoardColumnProps {
  lane: Lane;
  status: TaskStatusDef | undefined;
  collapsed: boolean;
  onToggleCollapse: (status: string) => void;
  onCreate: (status: string) => void;
  onOpenTask: (task: Task) => void;
}

export function BoardColumn({
  lane, status, collapsed, onToggleCollapse, onCreate, onOpenTask,
}: BoardColumnProps) {
  // A lane must accept a drop even with no cards in it, and an empty
  // SortableContext registers no droppable of its own — hence the explicit
  // one on the column body.
  const { setNodeRef, isOver } = useDroppable({
    id: `lane:${lane.status}`,
    data: { type: 'lane', status: lane.status },
  });

  const label = status?.label ?? lane.status;
  const color = status?.color ?? '#6b7280';
  const hidden = lane.total - lane.tasks.length;

  if (collapsed) {
    return (
      <div className="w-11 shrink-0 flex flex-col items-center gap-3 py-3 bg-surface-raised/40 border border-border-subtle rounded-xl">
        <button
          onClick={() => onToggleCollapse(lane.status)}
          className="text-text-faint hover:text-text-muted cursor-pointer"
          aria-label={`Expand ${label} column`}
          title={`Expand ${label}`}
        >
          <ChevronRight size={15} />
        </button>
        <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: color }} />
        {/* Vertical rail: the label reads bottom-to-top so long status
            names stay legible instead of being truncated to a glyph. */}
        <span
          className="text-[12px] font-medium text-text-secondary whitespace-nowrap"
          style={{ writingMode: 'vertical-rl', transform: 'rotate(180deg)' }}
        >
          {label}
        </span>
        <span className="text-[11px] text-text-faint tabular-nums">{lane.total}</span>
      </div>
    );
  }

  return (
    <div className="w-[300px] shrink-0 flex flex-col bg-surface-raised/40 border border-border-subtle rounded-xl max-h-full">
      <div className="shrink-0 px-3 py-2.5 border-b border-border-subtle">
        <div className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: color }} />
          <h2 className="text-[13px] font-semibold text-text-secondary truncate">{label}</h2>
          <span className="text-[11px] text-text-faint tabular-nums">{lane.total}</span>
          <div className="ml-auto flex items-center gap-0.5">
            <button
              onClick={() => onCreate(lane.status)}
              className="p-1 text-text-faint hover:text-text-muted cursor-pointer rounded"
              aria-label={`New task in ${label}`}
              title={`New task in ${label}`}
            >
              <Plus size={14} />
            </button>
            <button
              onClick={() => onToggleCollapse(lane.status)}
              className="p-1 text-text-faint hover:text-text-muted cursor-pointer rounded"
              aria-label={`Collapse ${label} column`}
              title="Collapse"
            >
              <ChevronLeft size={14} />
            </button>
          </div>
        </div>
        {status?.description && (
          <p className="mt-1 text-[11px] text-text-faint line-clamp-1" title={status.description}>
            {status.description}
          </p>
        )}
      </div>

      <div
        ref={setNodeRef}
        className={`flex-1 overflow-y-auto min-h-0 p-2 space-y-2 rounded-b-xl transition-colors
          ${isOver ? 'bg-accent/5' : ''}`}
      >
        <SortableContext
          items={lane.tasks.map((t) => t.id)}
          strategy={verticalListSortingStrategy}
        >
          {lane.tasks.map((task) => (
            <BoardCard key={task.id} task={task} onOpen={onOpenTask} />
          ))}
        </SortableContext>

        {lane.tasks.length === 0 && (
          <div
            className={`h-20 flex items-center justify-center text-[12px] rounded-lg border border-dashed
              ${isOver
                ? 'border-accent/40 text-accent'
                : 'border-border-subtle text-text-faint'}`}
          >
            {isOver ? 'Drop here' : 'No tasks'}
          </div>
        )}

        {hidden > 0 && (
          // The lane is paginated server-side; say so rather than silently
          // showing a partial column whose count doesn't match its cards.
          <p className="pt-1 pb-2 text-center text-[11px] text-text-faint">
            +{hidden} more — narrow the filters or use List view
          </p>
        )}
      </div>
    </div>
  );
}
