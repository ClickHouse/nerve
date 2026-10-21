import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../api/client', () => ({
  api: {
    getDiagnostics: vi.fn(),
    getCronLogs: vi.fn(),
    getMemuHealth: vi.fn(),
    listExternalAgents: vi.fn(),
  },
  setToken: vi.fn(), clearToken: vi.fn(), getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

const { api } = await import('../api/client');
const { DiagnosticsPage } = await import('./DiagnosticsPage');

function loomHealth(extraction: { queued: number; failed: number }, dashboardUrl = '') {
  return {
    backend: 'loom',
    service_available: true,
    initialized_at: new Date().toISOString(),
    dashboard_url: dashboardUrl,
    database: {
      total_items: 3, total_categories: 0, total_resources: 0,
      type_distribution: {}, events_missing_happened_at: 0, db_size_mb: 0,
    },
    loom: { namespace: 'agent-home', error: '' },
    extraction: { ...extraction, last_check: {} },
    operations: {},
    in_flight: [],
  };
}

describe('DiagnosticsPage memory card with the Loom backend', () => {
  beforeEach(() => {
    vi.mocked(api.getDiagnostics).mockResolvedValue({ system: {} });
    vi.mocked(api.getCronLogs).mockResolvedValue({ logs: [], total: 0, limit: 30, offset: 0 });
    vi.mocked(api.listExternalAgents).mockResolvedValue({
      enabled: false, sync_interval_minutes: 60, conflict_policy: 'skip', available: [], configured: [],
    });
  });

  it('counts the conversation windows Loom gave up on', async () => {
    vi.mocked(api.getMemuHealth).mockResolvedValue(loomHealth({ queued: 2, failed: 1 }));

    render(<DiagnosticsPage />);

    expect(await screen.findByText(/Conversation extraction: 2 queued in Loom, 1 failed/)).toBeInTheDocument();
    expect(screen.getByText(/gave up on these windows after three sends/)).toBeInTheDocument();
    // No dashboard address for people: no link at all.
    expect(screen.queryByRole('link', { name: /Open dashboard/ })).not.toBeInTheDocument();
  });

  it('links the dashboard when there is one', async () => {
    const url = 'https://loom.example.test/memories?namespace=agent-home';
    vi.mocked(api.getMemuHealth).mockResolvedValue(loomHealth({ queued: 0, failed: 0 }, url));

    render(<DiagnosticsPage />);

    const link = await screen.findByRole('link', { name: /Open dashboard/ });
    expect(link).toHaveAttribute('href', url);
    expect(screen.getByText(/Conversation extraction: 0 queued in Loom, 0 failed/)).toBeInTheDocument();
    expect(screen.queryByText(/gave up on these windows/)).not.toBeInTheDocument();
  });
});
