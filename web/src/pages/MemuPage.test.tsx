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

function loomSnapshot(dashboardUrl: string) {
  return {
    available: false, backend: 'loom', dashboard_url: dashboardUrl,
    categories: [], items: [], resources: [], category_items: {},
  };
}

describe('MemuPage', () => {
  beforeEach(() => {
    useMemoryStore.setState({ loading: true, available: false, backend: null, dashboardUrl: null });
  });

  it('points at the Loom dashboard when memory is kept in Loom', async () => {
    getMemuData.mockResolvedValue(loomSnapshot('https://loom.example.test/memories?namespace=agent-home'));

    render(<MemuPage />);

    expect(await screen.findByText('Memory is kept in Loom')).toBeInTheDocument();
    const link = screen.getByRole('link', { name: /Open the Loom dashboard/ });
    expect(link).toHaveAttribute('href', 'https://loom.example.test/memories?namespace=agent-home');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    expect(screen.queryByText('memU not available')).not.toBeInTheDocument();
  });

  it('shows no link without a dashboard address, or with one that is not http(s)', async () => {
    for (const url of ['', 'javascript:alert(1)']) {
      useMemoryStore.setState({ loading: true, available: false, backend: null, dashboardUrl: null });
      getMemuData.mockResolvedValue(loomSnapshot(url));
      const view = render(<MemuPage />);
      expect(await screen.findByText('Memory is kept in Loom')).toBeInTheDocument();
      expect(screen.queryByRole('link')).not.toBeInTheDocument();
      view.unmount();
    }
  });

  it('still says memU is unavailable when memU is the backend', async () => {
    getMemuData.mockResolvedValue({ available: false, categories: [], items: [], resources: [] });

    render(<MemuPage />);

    expect(await screen.findByText('memU not available')).toBeInTheDocument();
    expect(screen.queryByText('Memory is kept in Loom')).not.toBeInTheDocument();
  });
});
