import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { api, type SessionTask } from '../../api/client';
import { ChatHeaderMenu } from './ChatHeaderMenu';

vi.mock('../../api/client', () => ({
  api: {
    getSessionTasks: vi.fn(),
    listTaskStatuses: vi.fn(async () => ({ statuses: [] })),
  },
}));

const getSessionTasks = vi.mocked(api.getSessionTasks);

function task(id: string, title: string, relation: SessionTask['relation']): SessionTask {
  return {
    id, title, relation, status: 'pending', position: 1024, deadline: null,
    source: 'manual', source_url: null, tags: '',
    created_at: '2026-08-05T00:00:00Z', updated_at: '2026-08-05T00:00:00Z',
  };
}

function renderMenu() {
  render(<ChatHeaderMenu sessionId="s-1" />, { wrapper: MemoryRouter });
  return userEvent.setup();
}

const menuButton = () => screen.getByRole('button', { name: 'More actions' });

describe('ChatHeaderMenu', () => {
  beforeEach(() => getSessionTasks.mockReset());

  it('counts the tasks of the session and lists them in a dialog', async () => {
    getSessionTasks.mockResolvedValue({
      tasks: [
        task('t-1', 'Write the release notes', 'created'),
        task('t-2', 'Rotate the API keys', 'updated'),
      ],
    });
    const user = renderMenu();
    expect(getSessionTasks).not.toHaveBeenCalled();

    await user.click(menuButton());
    expect(getSessionTasks).toHaveBeenCalledWith('s-1');
    const item = await screen.findByRole('button', { name: 'Tasks from this session (2)' });

    await user.click(item);

    const dialog = screen.getByRole('dialog', { name: 'Tasks from this session' });
    const links = dialog.querySelectorAll('a');
    expect([...links].map((a) => [a.getAttribute('href'), a.textContent])).toEqual([
      ['/tasks/t-1', 'Write the release notesCreatedpending'],
      ['/tasks/t-2', 'Rotate the API keysUpdatedpending'],
    ]);
  });

  it('closes on Escape without letting it reach the page', async () => {
    // The global Escape that stops the running turn listens on the document.
    const pageEscape = vi.fn();
    document.addEventListener('keydown', pageEscape);
    getSessionTasks.mockResolvedValue({ tasks: [] });
    const user = renderMenu();
    await user.click(menuButton());
    expect(menuButton()).toHaveAttribute('aria-expanded', 'true');

    await user.keyboard('{Escape}');

    expect(menuButton()).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByRole('button', { name: /from this session/ })).toBeNull();
    expect(document.activeElement).toBe(menuButton());
    expect(pageEscape).not.toHaveBeenCalled();
    document.removeEventListener('keydown', pageEscape);
  });

  it('shows a disabled item and opens no dialog when the session has no tasks', async () => {
    getSessionTasks.mockResolvedValue({ tasks: [] });
    const user = renderMenu();
    await user.click(menuButton());
    const item = await screen.findByRole('button', { name: 'No tasks from this session' });
    expect(item).toBeDisabled();

    await user.click(item);

    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('offers a retry when the tasks do not load', async () => {
    getSessionTasks.mockRejectedValueOnce(new Error('offline'));
    const user = renderMenu();
    await user.click(menuButton());
    await user.click(screen.getByRole('button', { name: 'Tasks from this session' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('The tasks did not load.');

    getSessionTasks.mockResolvedValueOnce({ tasks: [task('t-1', 'Write the release notes', 'created')] });
    await user.click(screen.getByRole('button', { name: 'Try again' }));

    await waitFor(() => expect(screen.queryByRole('alert')).toBeNull());
    expect(screen.getByRole('link', { name: /Write the release notes/ })).toHaveAttribute('href', '/tasks/t-1');
  });
});
