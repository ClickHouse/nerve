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
    viewMode: 'board',
    searchQuery: '',
    tagFilter: '',
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

  it('reloads instead of inserting into a lane it has not fully loaded', () => {
    // The lane holds 40 tasks and shows 3. An update for one of the other
    // 37 looks identical to a brand-new task from here, and inserting it
    // would count it twice — once in the page, once in the total that
    // drives "+N more".
    useTaskStore.setState({
      lanes: [
        { status: 'pending', total: 40, tasks: [task('a', 'pending', 1024)] },
        { status: 'in_progress', total: 1, tasks: [task('x', 'in_progress', 1024)] },
      ],
    });
    vi.mocked(api.getTaskBoard).mockResolvedValue({ statuses: [], lanes: [] });

    useTaskStore.getState().handleTaskEvent(task('deep', 'pending', 9999));

    expect(api.getTaskBoard).toHaveBeenCalled();
    expect(laneOrder('pending')).toEqual(['a']);
    expect(laneTotal('pending')).toBe(40);
  });

  it('reloads instead of showing a card the tag filter excludes', () => {
    // The lanes on screen are the filtered set, so a task that is missing
    // from them may simply not match. Inserting it puts a card on the board
    // that contradicts the filter the user set.
    useTaskStore.setState({ tagFilter: 'urgent' });
    vi.mocked(api.getTaskBoard).mockResolvedValue({ statuses: [], lanes: [] });

    useTaskStore.getState().handleTaskEvent(task('chore', 'pending', 0));

    expect(api.getTaskBoard).toHaveBeenCalled();
    expect(laneOrder('pending')).toEqual(['a', 'b', 'c']);
  });

  it('reloads instead of showing a card the search excludes', () => {
    // Same reasoning as the tag filter, and it needs its own check: under a
    // search the server sets total to the match count, so the lane never
    // looks truncated.
    useTaskStore.setState({ searchQuery: 'encoder' });
    vi.mocked(api.getTaskBoard).mockResolvedValue({ statuses: [], lanes: [] });

    useTaskStore.getState().handleTaskEvent(task('unrelated', 'pending', 0));

    expect(api.getTaskBoard).toHaveBeenCalled();
    expect(laneOrder('pending')).toEqual(['a', 'b', 'c']);
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

describe('setSearch', () => {
  it('refreshes the board when the board is on screen', async () => {
    // The bug this pins: setSearch called loadTasks(), which only fills
    // tasks[] — the list's state. The board renders from lanes[], so
    // typing in the search box filtered an array nothing was reading.
    useTaskStore.setState({ viewMode: 'board' });
    vi.mocked(api.getTaskBoard).mockResolvedValue({
      statuses: [], lanes: [],
    });

    useTaskStore.getState().setSearch('encoder');

    await vi.waitFor(() => expect(api.getTaskBoard).toHaveBeenCalled());
    expect(api.listTasks).not.toHaveBeenCalled();
  });

  it('sends the query to the board endpoint', async () => {
    useTaskStore.setState({ viewMode: 'board' });
    vi.mocked(api.getTaskBoard).mockResolvedValue({
      statuses: [], lanes: [],
    });

    useTaskStore.getState().setSearch('  encoder  ');

    await vi.waitFor(() =>
      expect(api.getTaskBoard).toHaveBeenCalledWith(
        expect.objectContaining({ q: 'encoder' }),
      ),
    );
  });

  it('omits the query entirely once search is cleared', async () => {
    useTaskStore.setState({ viewMode: 'board', searchQuery: 'encoder' });
    vi.mocked(api.getTaskBoard).mockResolvedValue({
      statuses: [], lanes: [],
    });

    useTaskStore.getState().setSearch('');

    // undefined, not '' — an empty q would make the server run a search
    // that matches nothing instead of listing the lane.
    await vi.waitFor(() =>
      expect(api.getTaskBoard).toHaveBeenCalledWith(
        expect.objectContaining({ q: undefined }),
      ),
    );
  });

  it('still refreshes the list in list view', async () => {
    useTaskStore.setState({ viewMode: 'list' });
    // A non-empty query routes through searchTasks, not listTasks.
    vi.mocked(api.searchTasks).mockResolvedValue({
      tasks: [], total: 0, limit: 0, offset: 0,
    });

    useTaskStore.getState().setSearch('encoder');

    await vi.waitFor(() => expect(api.searchTasks).toHaveBeenCalled());
    expect(api.getTaskBoard).not.toHaveBeenCalled();
  });
});

describe('loadTasks', () => {
  it('does not apply the board tag filter to the list', async () => {
    // tagFilter belongs to the board: only the facet bar sets it and only
    // the board renders it. Leaking it into the list silently hides rows in
    // a view with nothing to show for the filter and no way to clear it —
    // and only while the search box is empty, since /tasks/search takes no
    // tag, so the result set would change on typing for no visible reason.
    useTaskStore.setState({ viewMode: 'list', tagFilter: 'urgent' });
    vi.mocked(api.listTasks).mockResolvedValue({
      tasks: [], total: 0, limit: 0, offset: 0,
    });

    await useTaskStore.getState().loadTasks();

    expect(api.listTasks).toHaveBeenCalledWith(
      expect.not.objectContaining({ tag: expect.anything() }),
    );
  });
});
