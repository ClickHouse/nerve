import { useDroppable } from '@dnd-kit/core';
import { SortableContext, useSortable, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { columnDragId } from './dropIntent';
import type { Task, TaskStatusDef } from '../../../api/client';
import type { Lane } from '../../../stores/taskStore';
import { IconButton } from '../../ui';
import { ChevronLeft, ChevronRight, GripVertical, Plus } from '../../ui/icons';
import { BoardCard } from './BoardCard';

export interface BoardColumnProps {
  lane: Lane;
  status: TaskStatusDef | undefined;
  collapsed: boolean;
  statusSince: Record<string, string>;
  onToggleCollapse: (status: string) => void;
  onCreate: (status: string) => void;
  onOpenTask: (task: Task) => void;
}

export function BoardColumn({
  lane, status, collapsed, statusSince, onToggleCollapse, onCreate, onOpenTask,
}: BoardColumnProps) {
  // A lane must accept a drop even with no cards in it, and an empty
  // SortableContext registers no droppable of its own — hence the explicit
  // one on the column body.
  const { setNodeRef, isOver } = useDroppable({
    id: `lane:${lane.status}`,
    data: { type: 'lane', status: lane.status },
  });

  // The column is itself sortable, but only by its header: the body has to
  // stay a drop target for cards, and putting drag listeners on the whole
  // column would make every card drag also pick up its column.
  const {
    setNodeRef: setColumnRef,
    attributes: columnAttributes,
    listeners: columnListeners,
    transform: columnTransform,
    transition: columnTransition,
    isDragging: isColumnDragging,
  } = useSortable({
    id: columnDragId(lane.status),
    data: { type: 'column', status: lane.status },
  });

  const columnStyle = {
    transform: CSS.Translate.toString(columnTransform),
    transition: columnTransition,
  };
  const dragHandle = (
    <span
      {...columnAttributes}
      {...columnListeners}
      title="Drag to reorder column"
      aria-label={`Reorder ${status?.label ?? lane.status} column`}
      className="text-text-faint hover:text-text-muted cursor-grab active:cursor-grabbing shrink-0"
    >
      <GripVertical size={13} />
    </span>
  );

  const label = status?.label ?? lane.status;
  const color = status?.color ?? '#6b7280';
  const hidden = lane.total - lane.tasks.length;

  if (collapsed) {
    return (
      <div
        ref={setColumnRef}
        style={columnStyle}
        className={`w-11 shrink-0 flex flex-col items-center gap-3 py-3 bg-surface-raised/40 border border-border-subtle rounded-xl
          ${isColumnDragging ? 'opacity-40' : ''}`}
      >
        {dragHandle}
        <IconButton
          label={`Expand ${label} column`}
          size="xs"
          onClick={() => onToggleCollapse(lane.status)}
        >
          <ChevronRight size={15} />
        </IconButton>
        <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: color }} />
        {/* Vertical rail: the label reads bottom-to-top so long status
            names stay legible instead of being truncated to a glyph. */}
        <span
          className="text-xs font-medium text-text-secondary whitespace-nowrap"
          style={{ writingMode: 'vertical-rl', transform: 'rotate(180deg)' }}
        >
          {label}
        </span>
        <span className="text-xs text-text-faint tabular-nums">{lane.total}</span>
      </div>
    );
  }

  return (
    <div
      ref={setColumnRef}
      style={columnStyle}
      className={`w-[300px] shrink-0 flex flex-col bg-surface-raised/40 border border-border-subtle rounded-xl max-h-full
        ${isColumnDragging ? 'opacity-40' : ''}`}
    >
      <div className="shrink-0 px-3 py-2.5 border-b border-border-subtle">
        <div className="flex items-center gap-2">
          {dragHandle}
          <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: color }} />
          {/* The named type steps carry a line-height, so the header pins its
              own — a lane header is chrome and must not grow at the cards'
              expense. */}
          <h2 className="text-sm leading-tight font-semibold text-text-secondary truncate">{label}</h2>
          <span className="text-xs leading-tight text-text-faint tabular-nums">{lane.total}</span>
          <div className="ml-auto flex items-center gap-0.5">
            <IconButton
              label={`New task in ${label}`}
              size="xs"
              onClick={() => onCreate(lane.status)}
            >
              <Plus size={14} />
            </IconButton>
            <IconButton
              label={`Collapse ${label} column`}
              size="xs"
              onClick={() => onToggleCollapse(lane.status)}
            >
              <ChevronLeft size={14} />
            </IconButton>
          </div>
        </div>
        {status?.description && (
          <p className="mt-1 text-xs leading-tight text-text-faint line-clamp-1" title={status.description}>
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
            <BoardCard
              key={task.id}
              task={task}
              statusSince={statusSince[task.id]}
              onOpen={onOpenTask}
            />
          ))}
        </SortableContext>

        {lane.tasks.length === 0 && (
          <div
            className={`h-20 flex items-center justify-center text-xs rounded-lg border border-dashed
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
          <p className="pt-1 pb-2 text-center text-xs leading-tight text-text-faint">
            +{hidden} more — narrow the filters or use List view
          </p>
        )}
      </div>
    </div>
  );
}
