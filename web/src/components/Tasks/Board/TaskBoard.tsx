import { useCallback, useMemo, useState } from 'react';
import {
  DndContext,
  DragOverlay,
  KeyboardSensor,
  PointerSensor,
  closestCorners,
  useSensor,
  useSensors,
  type DragEndEvent,
  type DragStartEvent,
} from '@dnd-kit/core';
import {
  SortableContext,
  horizontalListSortingStrategy,
  sortableKeyboardCoordinates,
} from '@dnd-kit/sortable';
import type { Task } from '../../../api/client';
import { useTaskStatusStore } from '../../../stores/taskStatusStore';
import { useTaskStore } from '../../../stores/taskStore';
import { boardAnnouncements } from './announcements';
import { BoardCardOverlay } from './BoardCard';
import {
  columnDragId,
  isNoOpMove,
  reorderStatuses,
  resolveDropIntent,
  statusFromDropTarget,
} from './dropIntent';
import { BoardColumn } from './BoardColumn';

const COLLAPSED_KEY = 'nerve_board_collapsed';

function readCollapsed(): string[] {
  try {
    const raw = localStorage.getItem(COLLAPSED_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed.filter((x) => typeof x === 'string') : [];
  } catch {
    return [];
  }
}

function writeCollapsed(next: string[]): void {
  try {
    localStorage.setItem(COLLAPSED_KEY, JSON.stringify(next));
  } catch {
    /* not fatal */
  }
}

export function TaskBoard({ onOpenTask }: { onOpenTask: (task: Task) => void }) {
  const lanes = useTaskStore((s) => s.lanes);
  const boardLoading = useTaskStore((s) => s.boardLoading);
  const boardError = useTaskStore((s) => s.boardError);
  const statusSince = useTaskStore((s) => s.statusSince);
  const moveTask = useTaskStore((s) => s.moveTask);
  const setShowCreateDialog = useTaskStore((s) => s.setShowCreateDialog);
  const searchQuery = useTaskStore((s) => s.searchQuery);
  const statuses = useTaskStatusStore((s) => s.statuses);
  const reorderColumns = useTaskStatusStore((s) => s.reorder);

  const [activeTask, setActiveTask] = useState<Task | null>(null);
  const [activeColumn, setActiveColumn] = useState<string | null>(null);
  const [collapsed, setCollapsed] = useState<string[]>(readCollapsed);

  const sensors = useSensors(
    // 4px of travel before a drag begins, so a plain click still reaches
    // the card's onClick and opens the task instead of starting a drag.
    useSensor(PointerSensor, { activationConstraint: { distance: 4 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  );

  const statusByName = useMemo(
    () => new Map(statuses.map((s) => [s.name, s])),
    [statuses],
  );

  const announcements = useMemo(
    () => boardAnnouncements((name) => statusByName.get(name)?.label ?? name),
    [statusByName],
  );

  const handleToggleCollapse = useCallback((status: string) => {
    setCollapsed((prev) => {
      const next = prev.includes(status)
        ? prev.filter((s) => s !== status)
        : [...prev, status];
      writeCollapsed(next);
      return next;
    });
  }, []);

  const handleDragStart = useCallback((event: DragStartEvent) => {
    const data = event.active.data.current;
    if (data?.type === 'column') {
      setActiveColumn(String(data.status));
      return;
    }
    setActiveTask((data?.task as Task | undefined) ?? null);
  }, []);

  const handleDragEnd = useCallback((event: DragEndEvent) => {
    setActiveTask(null);
    setActiveColumn(null);
    const { active, over } = event;
    if (!over) return;

    const activeId = String(active.id);
    const overId = String(over.id);
    if (activeId === overId) return;

    // Columns and cards share one DndContext, so branch on what was picked
    // up rather than on what it landed on.
    if (active.data.current?.type === 'column') {
      const moved = String(active.data.current.status);
      const target = statusFromDropTarget(lanes, overId);
      if (!target) return;
      const next = reorderStatuses(lanes.map((l) => l.status), moved, target);
      if (next) void reorderColumns(next);
      return;
    }

    const intent = resolveDropIntent(lanes, activeId, overId);
    if (!intent) return;
    // Skip the round trip when the card was dropped back where it started.
    if (isNoOpMove(lanes, activeId, intent)) return;

    void moveTask(activeId, intent);
  }, [lanes, moveTask, reorderColumns]);

  if (boardLoading) {
    return <div className="text-text-faint text-center py-10">Loading board...</div>;
  }

  if (lanes.length === 0) {
    return <div className="text-text-faint text-center py-10">No statuses configured.</div>;
  }

  // Every lane empty under an active search means no matches — say that
  // once, rather than repeating "No tasks" in each column as if the board
  // itself were empty.
  if (searchQuery.trim() && lanes.every((lane) => lane.tasks.length === 0)) {
    return (
      <div className="text-text-faint text-center py-10 text-sm">
        No tasks matching &ldquo;{searchQuery.trim()}&rdquo;
      </div>
    );
  }

  return (
    <>
      {boardError && (
        <div className="mx-4 mb-2 px-3 py-2 text-xs text-error bg-error-bg border border-error-border rounded-lg">
          {boardError}
        </div>
      )}
      <DndContext
        sensors={sensors}
        collisionDetection={closestCorners}
        onDragStart={handleDragStart}
        onDragEnd={handleDragEnd}
        onDragCancel={() => { setActiveTask(null); setActiveColumn(null); }}
        accessibility={{ announcements }}
      >
        <div className="flex-1 min-h-0 overflow-x-auto overflow-y-hidden px-4 pb-4">
          <div className="flex gap-3 h-full items-start min-w-min">
            <SortableContext
              items={lanes.map((l) => columnDragId(l.status))}
              strategy={horizontalListSortingStrategy}
            >
            {lanes.map((lane) => (
              <BoardColumn
                key={lane.status}
                lane={lane}
                status={statusByName.get(lane.status)}
                collapsed={collapsed.includes(lane.status)}
                statusSince={statusSince}
                onToggleCollapse={handleToggleCollapse}
                onCreate={(status) => setShowCreateDialog(true, status)}
                onOpenTask={onOpenTask}
              />
            ))}
            </SortableContext>
          </div>
        </div>

        <DragOverlay dropAnimation={null}>
          {activeTask && <BoardCardOverlay task={activeTask} />}
          {activeColumn && (
            <div className="w-[300px] px-3 py-2.5 bg-surface-raised border border-accent/50 rounded-xl shadow-xl text-sm leading-tight font-semibold text-text-secondary">
              {statusByName.get(activeColumn)?.label ?? activeColumn}
            </div>
          )}
        </DragOverlay>
      </DndContext>
    </>
  );
}
