import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef, TaskComment } from '../../api/client';

vi.mock('../../api/client', () => ({
  api: {
    listTaskComments: vi.fn(),
    addTaskComment: vi.fn(),
    deleteTaskComment: vi.fn(),
    listActors: vi.fn(),
  },
  // The viewer comes from the auth store, which wires these on import.
  setToken: vi.fn(), clearToken: vi.fn(), getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));
vi.mock('../../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const { api } = await import('../../api/client');
const { useActorStore } = await import('../../stores/actorStore');
const { useAuthStore } = await import('../../stores/authStore');
const { TaskComments } = await import('./TaskComments');

const listTaskComments = vi.mocked(api.listTaskComments);
const addTaskComment = vi.mocked(api.addTaskComment);
const deleteTaskComment = vi.mocked(api.deleteTaskComment);
const listActors = vi.mocked(api.listActors);

const ALICE = 'actor-alice';
const BOB = 'actor-bob';
const SYSTEM = 'actor-system';

function actorRef(id: string, overrides: Partial<ActorRef> = {}): ActorRef {
  return { id, kind: 'human', display_name: null, username: null, ...overrides };
}

let nextId = 1;
function comment(overrides: Partial<TaskComment>): TaskComment {
  return {
    id: nextId++,
    task_id: 't1',
    body: 'A comment',
    actor_id: ALICE,
    caused_by_actor_id: ALICE,
    created_at: '2026-01-01T00:00:00+00:00',
    ...overrides,
  };
}

const box = () => screen.getByRole('textbox', { name: 'New comment' });
const commentButton = () => screen.getByRole('button', { name: 'Comment' });

beforeEach(() => {
  vi.clearAllMocks();
  useActorStore.getState().reset();
  listActors.mockResolvedValue({
    actors: [
      actorRef(ALICE, { display_name: 'Alice' }),
      actorRef(BOB, { display_name: 'Bob' }),
      actorRef(SYSTEM, { kind: 'system' }),
    ],
  });
  useAuthStore.setState({
    viewer: actorRef(ALICE),
    account: { id: 'account-alice', username: 'alice' },
  });
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('TaskComments', () => {
  it('shows each comment with its author, and the agent for the person', async () => {
    listTaskComments.mockResolvedValue({
      comments: [
        comment({ body: 'Can you check **Safari**?', actor_id: BOB, caused_by_actor_id: BOB }),
        comment({ body: 'Checking now.', actor_id: SYSTEM, caused_by_actor_id: ALICE }),
      ],
    });
    render(<TaskComments taskId="t1" />);

    expect(await screen.findByText('Safari')).toBeInTheDocument();
    expect(screen.getByText('Checking now.')).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Comments (2)' })).toBeInTheDocument();
    await waitFor(() => {
      const labels = [...document.querySelectorAll('[data-attribution="actor"]')]
        .map((el) => el.textContent);
      expect(labels).toEqual(['Bob', 'Nerve for Alice']);
    });
  });

  it('says so when there are no comments', async () => {
    listTaskComments.mockResolvedValue({ comments: [] });
    render(<TaskComments taskId="t1" />);
    expect(await screen.findByText('No comments yet.')).toBeInTheDocument();
    expect(commentButton()).toBeDisabled();
  });

  it('adds a comment and clears the box', async () => {
    listTaskComments.mockResolvedValue({ comments: [] });
    addTaskComment.mockResolvedValue({ comment: comment({ body: 'Looks good' }) });
    render(<TaskComments taskId="t1" />);
    await screen.findByText('No comments yet.');

    await userEvent.type(box(), '  Looks good  ');
    await userEvent.click(commentButton());

    expect(addTaskComment).toHaveBeenCalledWith('t1', 'Looks good');
    expect(await screen.findByText('Looks good')).toBeInTheDocument();
    expect(box()).toHaveValue('');
  });

  it('sends with Ctrl+Enter', async () => {
    listTaskComments.mockResolvedValue({ comments: [] });
    addTaskComment.mockResolvedValue({ comment: comment({ body: 'Shipped' }) });
    render(<TaskComments taskId="t1" />);
    await screen.findByText('No comments yet.');

    await userEvent.type(box(), 'Shipped{Control>}{Enter}{/Control}');

    expect(addTaskComment).toHaveBeenCalledWith('t1', 'Shipped');
  });

  it('keeps the text and says so when the comment is not added', async () => {
    listTaskComments.mockResolvedValue({ comments: [] });
    addTaskComment.mockRejectedValue(new Error('500'));
    render(<TaskComments taskId="t1" />);
    await screen.findByText('No comments yet.');

    await userEvent.type(box(), 'Lost?');
    await userEvent.click(commentButton());

    expect(await screen.findByRole('alert')).toHaveTextContent('not added');
    expect(box()).toHaveValue('Lost?');
  });

  it('offers delete only on the viewer\'s own comments', async () => {
    const mine = comment({ body: 'Mine', actor_id: ALICE });
    listTaskComments.mockResolvedValue({
      comments: [mine, comment({ body: 'Bob wrote this', actor_id: BOB, caused_by_actor_id: BOB })],
    });
    deleteTaskComment.mockResolvedValue({ id: mine.id, deleted: true });
    vi.spyOn(window, 'confirm').mockReturnValue(true);
    render(<TaskComments taskId="t1" />);
    await screen.findByText('Mine');

    const deletes = screen.getAllByRole('button', { name: 'Delete comment' });
    expect(deletes).toHaveLength(1);
    await userEvent.click(deletes[0]);

    expect(deleteTaskComment).toHaveBeenCalledWith('t1', mine.id);
    await waitFor(() => expect(screen.queryByText('Mine')).not.toBeInTheDocument());
    expect(screen.getByText('Bob wrote this')).toBeInTheDocument();
  });

  it('reads the comments again when the refresh key changes', async () => {
    listTaskComments.mockResolvedValueOnce({ comments: [] });
    const { rerender } = render(<TaskComments taskId="t1" refreshKey="a" />);
    await screen.findByText('No comments yet.');

    listTaskComments.mockResolvedValueOnce({
      comments: [comment({ body: 'From the agent', actor_id: SYSTEM, caused_by_actor_id: ALICE })],
    });
    rerender(<TaskComments taskId="t1" refreshKey="b" />);

    expect(await screen.findByText('From the agent')).toBeInTheDocument();
    expect(listTaskComments).toHaveBeenCalledTimes(2);
  });
});
