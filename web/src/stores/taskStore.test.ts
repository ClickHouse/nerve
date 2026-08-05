import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Task } from '../api/client';

vi.mock('../api/client', () => ({
  api: {
    moveTask: vi.fn(),
    getTaskBoard: vi.fn(),
    listTaskTags: vi.fn(),
    listTasks: vi.fn(),
    searchTasks: vi.fn(),
  },
}));

const { api } = await import('../api/client');
const { useTaskStore } = await import('./taskStore');

function task(id: string, status = 'pending', position = 0): Task {
  return {
    id, title: id, status, position,
    deadline: null, source: 'manual', source_url: null, tags: '',
    created_at: '2026-08-05T00:00:00Z', updated_at: '2026-08-05T00:00:00Z',
  };
}

const laneOrder = (status: string) =>
  useTaskStore.getState().lanes.find((l) => l.status === status)!.tasks.map((t) => t.id);

const laneTotal = (status: string) =>
  useTaskStore.getState().lanes.find((l) => l.status === status)!.total;

beforeEach(() => {
  vi.clearAllMocks();
  useTaskStore.setState({
    lanes: [
      {
        status: 'pending',
        total: 3,
        tasks: [task('a', 'pending', 1024), task('b', 'pending', 2048), task('c', 'pending', 3072)],
      },
      { status: 'in_progress', total: 1, tasks: [task('x', 'in_progress', 1024)] },
      { status: 'done', total: 0, tasks: [] },
    ],
    selectedTask: null,
    boardError: null,
  });
});

describe('applyLocalMove', () => {
  it('reorders within a lane', () => {
    useTaskStore.getState().applyLocalMove('c', {
      status: 'pending', beforeId: null, afterId: 'a',
    });
    expect(laneOrder('pending')).toEqual(['c', 'a', 'b']);
  });

  it('appends when there are no anchors', () => {
    useTaskStore.getState().applyLocalMove('a', {
      status: 'pending', beforeId: null, afterId: null,
    });
    expect(laneOrder('pending')).toEqual(['b', 'c', 'a']);
  });

  it('moves across lanes and fixes both totals', () => {
    useTaskStore.getState().applyLocalMove('a', {
      status: 'in_progress', beforeId: null, afterId: 'x',
    });
    expect(laneOrder('pending')).toEqual(['b', 'c']);
    expect(laneOrder('in_progress')).toEqual(['a', 'x']);
    // Counts drive the lane headers and the "+N more" affordance; a move
    // that shifts a card without shifting the totals shows a lane claiming
    // more cards than it can ever display.
    expect(laneTotal('pending')).toBe(2);
    expect(laneTotal('in_progress')).toBe(2);
  });
});

describe('moveTask', () => {
  it('applies optimistically before the request resolves', async () => {
    let resolveRequest: (v: { task: Task }) => void = () => {};
    vi.mocked(api.moveTask).mockReturnValue(
      new Promise((res) => { resolveRequest = res; }) as ReturnType<typeof api.moveTask>,
    );

    const pending = useTaskStore.getState().moveTask('c', {
      status: 'pending', beforeId: null, afterId: 'a',
    });

    // The card has already moved — that's the point of optimistic UI.
    expect(laneOrder('pending')).toEqual(['c', 'a', 'b']);
    resolveRequest({ task: task('c', 'pending', 512) });
    await pending;
    expect(laneOrder('pending')).toEqual(['c', 'a', 'b']);
  });

  it('reconciles the server row into the moved card', async () => {
    vi.mocked(api.moveTask).mockResolvedValue({
      task: { ...task('c', 'pending', 512), updated_at: '2026-08-05T09:00:00Z' },
    });

    await useTaskStore.getState().moveTask('c', {
      status: 'pending', beforeId: null, afterId: 'a',
    });

    const moved = useTaskStore.getState().lanes[0].tasks[0];
    // The server owns the rank; without adopting it the next drag computes
    // anchors from a position that never existed.
    expect(moved.position).toBe(512);
    expect(moved.updated_at).toBe('2026-08-05T09:00:00Z');
  });

  it('rolls back to the exact prior order when the request fails', async () => {
    vi.mocked(api.moveTask).mockRejectedValue(new Error('409: conflict'));
    // Resync in flight but unresolved — this asserts the *immediate*
    // rollback, which is what the user sees while the refetch travels.
    vi.mocked(api.getTaskBoard).mockReturnValue(
      new Promise(() => {}) as ReturnType<typeof api.getTaskBoard>,
    );

    await useTaskStore.getState().moveTask('a', {
      status: 'in_progress', beforeId: null, afterId: 'x',
    });

    // Restored from the snapshot, not recomputed — totals included.
    expect(laneOrder('pending')).toEqual(['a', 'b', 'c']);
    expect(laneTotal('pending')).toBe(3);
    expect(laneOrder('in_progress')).toEqual(['x']);
    expect(useTaskStore.getState().boardError).toBeTruthy();
  });

  it('resyncs from the server after a failed move', async () => {
    vi.mocked(api.moveTask).mockRejectedValue(new Error('409: conflict'));
    // A rejected move means the board was already out of date, so the
    // snapshot is a stopgap: the server's state wins once it arrives.
    vi.mocked(api.getTaskBoard).mockResolvedValue({
      statuses: [],
      lanes: [
        { status: 'pending', total: 1, tasks: [task('b', 'pending', 2048)] },
        { status: 'in_progress', total: 1, tasks: [task('a', 'in_progress', 1024)] },
      ],
    });

    await useTaskStore.getState().moveTask('a', {
      status: 'in_progress', beforeId: null, afterId: 'x',
    });
    // Let the un-awaited resync settle.
    await vi.waitFor(() => expect(api.getTaskBoard).toHaveBeenCalled());
    await Promise.resolve();

    expect(laneOrder('pending')).toEqual(['b']);
    expect(laneOrder('in_progress')).toEqual(['a']);
  });
});

describe('handleTaskEvent', () => {
  it('updates a card in place when the lane is unchanged', () => {
    useTaskStore.getState().handleTaskEvent(
      { ...task('b', 'pending', 2048), title: 'renamed' },
    );
    expect(laneOrder('pending')).toEqual(['a', 'b', 'c']);
    expect(useTaskStore.getState().lanes[0].tasks[1].title).toBe('renamed');
    // An in-place edit must not inflate the count.
    expect(laneTotal('pending')).toBe(3);
  });

  it('re-slots a card that changed status elsewhere', () => {
    useTaskStore.getState().handleTaskEvent(task('a', 'in_progress', 2048));

    expect(laneOrder('pending')).toEqual(['b', 'c']);
    // Inserted by rank, not appended: position 2048 sorts after x's 1024.
    expect(laneOrder('in_progress')).toEqual(['x', 'a']);
    expect(laneTotal('pending')).toBe(2);
    expect(laneTotal('in_progress')).toBe(2);
  });

  it('inserts a task the board has never seen', () => {
    useTaskStore.getState().handleTaskEvent(task('new', 'pending', 0));

    // Rank 0 sorts above everything — where a newly created task belongs.
    expect(laneOrder('pending')).toEqual(['new', 'a', 'b', 'c']);
    expect(laneTotal('pending')).toBe(4);
  });

  it('reloads when the status has no lane on this board', () => {
    vi.mocked(api.getTaskBoard).mockResolvedValue({ statuses: [], lanes: [] });

    useTaskStore.getState().handleTaskEvent(task('a', 'in_review', 1024));

    // Someone added a status; only a refetch can produce its column.
    expect(api.getTaskBoard).toHaveBeenCalled();
  });

  it('is a no-op on an empty board', () => {
    useTaskStore.setState({ lanes: [] });
    expect(() =>
      useTaskStore.getState().handleTaskEvent(task('a')),
    ).not.toThrow();
  });

  it('refreshes the open detail task without dropping its loaded content', () => {
    useTaskStore.setState({
      selectedTask: { ...task('a'), content: '# loaded markdown' },
    });

    useTaskStore.getState().handleTaskEvent(
      { ...task('a', 'in_progress', 1024), title: 'renamed' },
    );

    const sel = useTaskStore.getState().selectedTask!;
    expect(sel.title).toBe('renamed');
    expect(sel.status).toBe('in_progress');
    // The broadcast carries the row, not the file — merging must not blank
    // the markdown the detail view already fetched.
    expect(sel.content).toBe('# loaded markdown');
  });
});
