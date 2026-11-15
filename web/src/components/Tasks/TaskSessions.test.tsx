import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactElement } from 'react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import type { Task } from '../../api/client';
import { TaskCard } from './TaskCard';
import { TaskSessions } from './TaskSessions';

function task(fields: Partial<Task> = {}): Task {
  return {
    id: 't1', title: 'Write the release notes', status: 'pending', position: 1024,
    deadline: null, source: 'manual', source_url: null, tags: '',
    created_at: '2026-08-05T00:00:00Z', updated_at: '2026-08-05T00:00:00Z',
    ...fields,
  };
}

/** The card on `/`, with stand-ins for the pages its links open. */
function renderOnBoard(ui: ReactElement) {
  return render(
    <MemoryRouter>
      <Routes>
        <Route path="/" element={ui} />
        <Route path="/chat/:id" element={<p>Chat page</p>} />
        <Route path="/tasks/:id" element={<p>Task page</p>} />
      </Routes>
    </MemoryRouter>,
  );
}

describe('TaskCard origin', () => {
  it('links to the session that created the task, without opening the task', async () => {
    renderOnBoard(
      <TaskCard
        task={task({ created_session_id: 's-1', created_session_title: 'Plan the release' })}
        onStatusChange={vi.fn()}
      />,
    );
    expect(screen.queryByText('from manual')).toBeNull();
    const link = screen.getByRole('link', { name: 'Plan the release' });
    expect(link).toHaveAttribute('href', '/chat/s-1');

    await userEvent.click(link);

    expect(screen.getByText('Chat page')).toBeInTheDocument();
    expect(screen.queryByText('Task page')).toBeNull();
  });

  it('shows the source when the creating session does not exist', () => {
    renderOnBoard(
      <TaskCard
        task={task({ created_session_id: 's-gone', created_session_title: null })}
        onStatusChange={vi.fn()}
      />,
    );
    expect(screen.getByText('from manual')).toBeInTheDocument();
    expect(screen.queryByRole('link')).toBeNull();
  });
});

describe('TaskSessions', () => {
  it('lists each session with its relation and a link to its chat', () => {
    render(
      <TaskSessions
        sessions={[
          { id: 's-1', title: 'Plan the release', relation: 'created' },
          { id: 'impl-1', title: 'Implement: Write the release notes', relation: 'implemented' },
          { id: 's-2', title: 'Weekly triage', relation: 'updated' },
        ]}
      />,
      { wrapper: MemoryRouter },
    );
    const section = screen.getByRole('region', { name: 'Sessions' });
    const rows = section.querySelectorAll('li');
    expect([...rows].map((row) => row.textContent)).toEqual([
      'Created inPlan the release',
      'Implemented inImplement: Write the release notes',
      'Updated inWeekly triage',
    ]);
    expect(screen.getByRole('link', { name: 'Weekly triage' })).toHaveAttribute('href', '/chat/s-2');
  });

  it('renders nothing without sessions', () => {
    const { container } = render(<TaskSessions sessions={[]} />, { wrapper: MemoryRouter });
    expect(container).toBeEmptyDOMElement();
  });
});
