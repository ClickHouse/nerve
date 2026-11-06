import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { TaskStatusDef } from '../../api/client';

vi.mock('../../api/client', () => ({
  api: { listTaskStatuses: vi.fn() },
}));

const { api } = await import('../../api/client');
const { useTaskStatusStore } = await import('../../stores/taskStatusStore');
const { TaskCreateDialog } = await import('./TaskCreateDialog');

const listTaskStatuses = vi.mocked(api.listTaskStatuses);

function status(name: string, label: string, sort_order: number): TaskStatusDef {
  return { name, label, color: '#888888', description: '', is_system: 0, sort_order };
}

const BUILT_IN = [
  status('backlog', 'Backlog', 0),
  status('pending', 'Pending', 1),
  status('plan_review', 'Plan in review', 2),
  status('in_progress', 'In Progress', 3),
  status('done', 'Done', 4),
];

async function createWith(props: { initialStatus?: string | null } = {}) {
  const onCreate = vi.fn();
  render(<TaskCreateDialog onClose={vi.fn()} onCreate={onCreate} {...props} />);
  const picker = await screen.findByLabelText('Status');
  await waitFor(() => expect(picker.querySelectorAll('option').length).toBeGreaterThan(0));
  return { onCreate, picker };
}

async function submit(title = 'Write the release notes') {
  await userEvent.type(screen.getAllByRole('textbox')[0], title);
  await userEvent.click(screen.getByRole('button', { name: 'Create' }));
}

beforeEach(() => {
  vi.clearAllMocks();
  useTaskStatusStore.setState({ statuses: [], loaded: false, loading: false });
});

describe('TaskCreateDialog status', () => {
  it('starts in the backlog and says which status the agent picks up', async () => {
    listTaskStatuses.mockResolvedValue({ statuses: BUILT_IN });
    const { onCreate, picker } = await createWith();

    expect(picker).toHaveValue('backlog');
    expect(screen.getByText(/The agent can pick up tasks in Pending\./)).toBeInTheDocument();
    await submit();
    expect(onCreate).toHaveBeenCalledWith('Write the release notes', '', '', 'backlog');
  });

  it('creates the task in the status the user picks', async () => {
    listTaskStatuses.mockResolvedValue({ statuses: BUILT_IN });
    const { onCreate, picker } = await createWith();

    await userEvent.selectOptions(picker, 'pending');
    await submit();
    expect(onCreate).toHaveBeenCalledWith('Write the release notes', '', '', 'pending');
  });

  it('starts in the status of the column that opened it', async () => {
    listTaskStatuses.mockResolvedValue({ statuses: BUILT_IN });
    const { picker } = await createWith({ initialStatus: 'in_progress' });
    expect(picker).toHaveValue('in_progress');
  });

  it('falls back to pending when there is no backlog status', async () => {
    listTaskStatuses.mockResolvedValue({
      statuses: BUILT_IN.filter((s) => s.name !== 'backlog'),
    });
    const { onCreate, picker } = await createWith();

    expect(picker).toHaveValue('pending');
    expect(screen.queryByText(/wait until you move them/)).not.toBeInTheDocument();
    await submit();
    expect(onCreate).toHaveBeenCalledWith('Write the release notes', '', '', 'pending');
  });
});
