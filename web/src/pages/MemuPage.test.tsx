import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../api/client', () => ({
  api: {
    getMemuData: vi.fn(),
    getMemuAuditLog: vi.fn(),
  },
  setToken: vi.fn(), clearToken: vi.fn(), getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

const { api } = await import('../api/client');
const { useMemoryStore } = await import('../stores/memoryStore');
const { MemuPage } = await import('./MemuPage');

const getMemuData = vi.mocked(api.getMemuData);

describe('MemuPage', () => {
  beforeEach(() => {
    useMemoryStore.setState({ loading: true, available: false, backend: null, dashboardUrl: null });
  });

  it('points at the Loom dashboard when memory is kept in Loom', async () => {
    getMemuData.mockResolvedValue({
      available: false,
      backend: 'loom',
      reason: 'not available for this backend',
      dashboard_url: 'http://loom.example.test',
      categories: [],
      items: [],
      resources: [],
      category_items: {},
    });

    render(<MemuPage />);

    expect(await screen.findByText('Memory is kept in Loom')).toBeInTheDocument();
    const link = screen.getByRole('link', { name: /Open the Loom dashboard/ });
    expect(link).toHaveAttribute('href', 'http://loom.example.test');
    expect(screen.queryByText('memU not available')).not.toBeInTheDocument();
  });

  it('still says memU is unavailable when memU is the backend', async () => {
    getMemuData.mockResolvedValue({ available: false, categories: [], items: [], resources: [] });

    render(<MemuPage />);

    expect(await screen.findByText('memU not available')).toBeInTheDocument();
    expect(screen.queryByRole('link', { name: /Loom/ })).not.toBeInTheDocument();
  });
});
