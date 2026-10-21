import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes, useParams } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../api/client', () => ({
  api: { getTaskPlans: vi.fn() },
  setToken: vi.fn(), clearToken: vi.fn(), getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

const { api } = await import('../../api/client');
const { TaskPlan } = await import('./TaskPlan');

const getTaskPlans = vi.mocked(api.getTaskPlans);

function plan(overrides: Record<string, unknown>) {
  return {
    id: 'plan-1', task_id: 't1', status: 'pending', version: 1, feedback: null,
    content: '# Plan', created_at: '2026-08-05T00:00:00Z', ...overrides,
  };
}

function PlanPageProbe() {
  const { planId } = useParams();
  return <div>plan page {planId}</div>;
}

function renderAt() {
  return render(
    <MemoryRouter initialEntries={['/tasks/t1']}>
      <Routes>
        <Route path="/tasks/:taskId" element={<TaskPlan taskId="t1" />} />
        <Route path="/plans/:planId" element={<PlanPageProbe />} />
      </Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  getTaskPlans.mockReset();
});

describe('TaskPlan', () => {
  it('shows nothing for a task without a plan', async () => {
    getTaskPlans.mockResolvedValue({ plans: [] });
    renderAt();
    await waitFor(() => expect(getTaskPlans).toHaveBeenCalledWith('t1'));
    expect(screen.queryByRole('region', { name: 'Plan' })).not.toBeInTheDocument();
  });

  it('asks for a review of a pending plan and opens it', async () => {
    getTaskPlans.mockResolvedValue({ plans: [plan({ id: 'plan-2', version: 2 })] });
    renderAt();

    const region = await screen.findByRole('region', { name: 'Plan' });
    expect(region).toHaveTextContent('A plan waits for your review');
    expect(region).toHaveTextContent('v2');

    await userEvent.click(screen.getByRole('button', { name: 'Open plan' }));
    expect(await screen.findByText('plan page plan-2')).toBeInTheDocument();
  });

  it('shows the latest version with its status when nothing waits for review', async () => {
    // Newest first, as the server sends them.
    getTaskPlans.mockResolvedValue({
      plans: [
        plan({ id: 'plan-3', version: 3, status: 'implementing' }),
        plan({ id: 'plan-2', version: 2, status: 'superseded' }),
      ],
    });
    renderAt();

    const region = await screen.findByRole('region', { name: 'Plan' });
    expect(region).toHaveTextContent('Plan v3');
    expect(region).toHaveTextContent('implementing');
    expect(region).not.toHaveTextContent('superseded');
    expect(region).not.toHaveTextContent('waits for your review');

    await userEvent.click(screen.getByRole('button', { name: 'Open plan' }));
    expect(await screen.findByText('plan page plan-3')).toBeInTheDocument();
  });

  it('does not ask for a review while a revision is requested', async () => {
    getTaskPlans.mockResolvedValue({ plans: [plan({ feedback: 'Split the migration.' })] });
    renderAt();

    const region = await screen.findByRole('region', { name: 'Plan' });
    expect(region).toHaveTextContent('Revision requested');
    expect(region).not.toHaveTextContent('waits for your review');
  });

  it('shows nothing when the plans cannot be read', async () => {
    getTaskPlans.mockRejectedValue(new Error('network down'));
    renderAt();
    await waitFor(() => expect(getTaskPlans).toHaveBeenCalled());
    expect(screen.queryByRole('region', { name: 'Plan' })).not.toBeInTheDocument();
  });
});
