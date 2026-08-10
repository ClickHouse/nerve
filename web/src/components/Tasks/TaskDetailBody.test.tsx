import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { MockInstance } from 'vitest';

import { api, type Task } from '../../api/client';
import { TaskDetailBody } from './TaskDetailBody';

vi.mock('../../api/client', () => ({
  api: { updateTask: vi.fn() },
}));

/**
 * The dirty guard is the one piece of behaviour here that a reader cannot
 * verify by looking: it only shows itself when a re-render arrives while the
 * textarea holds unsaved text. Now that the full page and the board's modal
 * both render this component, a regression would silently discard typing in
 * two places at once.
 */

function task(content: string): Task & { content: string } {
  return {
    id: 't1', title: 'Fix the encoder', status: 'pending', position: 1024,
    deadline: null, source: 'manual', source_url: null, tags: '',
    created_at: '2026-08-05T00:00:00Z', updated_at: '2026-08-05T00:00:00Z',
    content,
  };
}

const editor = () => screen.getByPlaceholderText('Task content...');
const saveButton = () => screen.queryByRole('button', { name: /save/i });

async function startEditing() {
  await userEvent.click(screen.getByTitle('Edit'));
}

describe('TaskDetailBody content sync', () => {
  it('adopts the task content when nothing has been typed', async () => {
    const { rerender } = render(<TaskDetailBody task={task('# original')} />);
    await startEditing();
    expect(editor()).toHaveValue('# original');

    rerender(<TaskDetailBody task={task('# rewritten elsewhere')} />);

    // Clean editor: the newer content is strictly better than what is shown.
    expect(editor()).toHaveValue('# rewritten elsewhere');
  });

  it('keeps unsaved edits when the task content changes underneath', async () => {
    const { rerender } = render(<TaskDetailBody task={task('# original')} />);
    await startEditing();
    await userEvent.type(editor(), ' plus my notes');
    expect(editor()).toHaveValue('# original plus my notes');

    rerender(<TaskDetailBody task={task('# rewritten elsewhere')} />);

    // The whole point: an incoming update must not throw away typing.
    expect(editor()).toHaveValue('# original plus my notes');
  });

  it('offers Save only once something has been typed', async () => {
    render(<TaskDetailBody task={task('# original')} />);
    await startEditing();
    expect(screen.queryByRole('button', { name: /save/i })).not.toBeInTheDocument();

    await userEvent.type(editor(), '!');

    expect(screen.getByRole('button', { name: /save/i })).toBeInTheDocument();
  });
});

/**
 * A save that fails has to look different from a save that worked. The old
 * code cleared `dirty` either way, which hid the Save button and left the
 * editor looking settled while the only copy of the text was still in the
 * browser — the user found out on the next reload.
 */
describe('TaskDetailBody failed saves', () => {
  const updateTask = vi.mocked(api.updateTask);
  let consoleError: MockInstance;

  const savedResponse = (content: string) => ({
    task: task(content), task_id: 't1', updated: true,
  });

  beforeEach(() => {
    updateTask.mockReset();
    // The store logs the rejection deliberately. Swallow it here so a genuine
    // React warning still stands out in the run output.
    consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleError.mockRestore();
  });

  async function typeAndFailToSave(addition = ' plus my notes') {
    render(<TaskDetailBody task={task('# original')} />);
    await startEditing();
    await userEvent.type(editor(), addition);
    await userEvent.click(screen.getByRole('button', { name: /save/i }));
    return screen.findByRole('alert');
  }

  it('says the save failed and keeps offering the retry', async () => {
    updateTask.mockRejectedValue(new Error('network down'));

    expect(await typeAndFailToSave()).toHaveTextContent(/save failed/i);

    // The edit is still in the box, and still offered for saving. Before the
    // fix the button vanished, which read as "saved".
    expect(editor()).toHaveValue('# original plus my notes');
    expect(saveButton()).toBeInTheDocument();
  });

  it('keeps the failed edit safe from an update arriving underneath', async () => {
    updateTask.mockRejectedValue(new Error('network down'));
    const { rerender } = render(<TaskDetailBody task={task('# original')} />);
    await startEditing();
    await userEvent.type(editor(), ' plus my notes');
    await userEvent.click(screen.getByRole('button', { name: /save/i }));
    await screen.findByRole('alert');

    rerender(<TaskDetailBody task={task('# rewritten elsewhere')} />);

    // `dirty` staying set is what keeps the content-sync guard armed. Clearing
    // it on a failure would hand the only copy of the text to the next
    // re-render, which is how the edit actually got lost.
    expect(editor()).toHaveValue('# original plus my notes');
  });

  it('clears the failure once a retry succeeds', async () => {
    updateTask
      .mockRejectedValueOnce(new Error('network down'))
      .mockResolvedValueOnce(savedResponse('# original!'));

    await typeAndFailToSave('!');
    await userEvent.click(screen.getByRole('button', { name: /save/i }));

    await waitFor(() => expect(screen.queryByRole('alert')).not.toBeInTheDocument());
    expect(saveButton()).not.toBeInTheDocument();
  });

  it('clears the failure as soon as the user edits again', async () => {
    updateTask.mockRejectedValue(new Error('network down'));

    await typeAndFailToSave('!');
    await userEvent.type(editor(), '?');

    // Stale complaint about text the user has already moved past, but the
    // edit is still unsaved so Save has to stay.
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(saveButton()).toBeInTheDocument();
  });
});
