import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import type { Task } from '../../api/client';
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
