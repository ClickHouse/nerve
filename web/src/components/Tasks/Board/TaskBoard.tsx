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
import { sortableKeyboardCoordinates } from '@dnd-kit/sortable';
import type { Task } from '../../../api/client';
import { useTaskStatusStore } from '../../../stores/taskStatusStore';
import { useTaskStore } from '../../../stores/taskStore';
import { boardAnnouncements } from './announcements';
import { BoardCardOverlay } from './BoardCard';
import { isNoOpMove, resolveDropIntent } from './dropIntent';
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
  const moveTask = useTaskStore((s) => s.moveTask);
  const setShowCreateDialog = useTaskStore((s) => s.setShowCreateDialog);
  const searchQuery = useTaskStore((s) => s.searchQuery);
  const statuses = useTaskStatusStore((s) => s.statuses);

  const [activeTask, setActiveTask] = useState<Task | null>(null);
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
    const task = event.active.data.current?.task as Task | undefined;
    setActiveTask(task ?? null);
  }, []);

  const handleDragEnd = useCallback((event: DragEndEvent) => {
    setActiveTask(null);
    const { active, over } = event;
    if (!over) return;

    const activeId = String(active.id);
    const overId = String(over.id);
    if (activeId === overId) return;

    const intent = resolveDropIntent(lanes, activeId, overId);
    if (!intent) return;
    // Skip the round trip when the card was dropped back where it started.
    if (isNoOpMove(lanes, activeId, intent)) return;

    void moveTask(activeId, intent);
  }, [lanes, moveTask]);

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
      <div className="text-text-faint text-center py-10 text-[13px]">
        No tasks matching &ldquo;{searchQuery.trim()}&rdquo;
      </div>
    );
  }

  return (
    <>
      {boardError && (
        <div className="mx-4 mb-2 px-3 py-2 text-[12px] text-hue-red bg-red-400/10 border border-red-400/20 rounded-lg">
          {boardError}
        </div>
      )}
      <DndContext
        sensors={sensors}
        collisionDetection={closestCorners}
        onDragStart={handleDragStart}
        onDragEnd={handleDragEnd}
        onDragCancel={() => setActiveTask(null)}
        accessibility={{ announcements }}
      >
        <div className="flex-1 min-h-0 overflow-x-auto overflow-y-hidden px-4 pb-4">
          <div className="flex gap-3 h-full items-start min-w-min">
            {lanes.map((lane) => (
              <BoardColumn
                key={lane.status}
                lane={lane}
                status={statusByName.get(lane.status)}
                collapsed={collapsed.includes(lane.status)}
                onToggleCollapse={handleToggleCollapse}
                onCreate={(status) => setShowCreateDialog(true, status)}
                onOpenTask={onOpenTask}
              />
            ))}
          </div>
        </div>

        <DragOverlay dropAnimation={null}>
          {activeTask && <BoardCardOverlay task={activeTask} />}
        </DragOverlay>
      </DndContext>
    </>
  );
}
