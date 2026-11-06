import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../api/client', () => ({
  api: {
    listPlans: vi.fn(),
  },
  setToken: vi.fn(), clearToken: vi.fn(), getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

const { api } = await import('../api/client');
const { PlansPage } = await import('./PlansPage');

const listPlans = vi.mocked(api.listPlans);

beforeEach(() => {
  vi.clearAllMocks();
  listPlans.mockResolvedValue({ plans: [] });
});

describe('PlansPage filters', () => {
  it('filters by the plan statuses and has no revising filter', async () => {
    render(<MemoryRouter><PlansPage /></MemoryRouter>);
    await screen.findByText(/^No (pending )?plans/);
    // The page header renders its filters once per layout.
    const labels = new Set(
      screen.getAllByRole('button').map((button) => button.textContent?.trim()),
    );
    for (const label of ['All', 'Pending', 'Approved', 'Implementing', 'Declined']) {
      expect(labels).toContain(label);
    }
    expect(screen.queryByRole('button', { name: /Revising/ })).not.toBeInTheDocument();
  });
});
