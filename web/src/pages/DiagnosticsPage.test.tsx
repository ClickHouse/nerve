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

const DATABASE = {
  total_items: 3, total_categories: 0, total_resources: 0,
  type_distribution: {}, events_missing_happened_at: 0, db_size_mb: 0,
};

function loomHealth(loom: Record<string, unknown>, dashboardUrl = '') {
  return {
    backend: 'loom',
    service_available: !loom.error,
    initialized_at: new Date().toISOString(),
    dashboard_url: dashboardUrl,
    database: DATABASE,
    loom: {
      gateway_url: 'http://192.0.2.1:3130', server: 'loom', org: 'acme', namespace: 'agent-home',
      identity_source: 'loom', error: '', checked_at: new Date().toISOString(), ...loom,
    },
    operations: {},
    in_flight: [],
  };
}

describe('DiagnosticsPage memory card', () => {
  beforeEach(() => {
    vi.mocked(api.getDiagnostics).mockResolvedValue({ system: {} });
    vi.mocked(api.getCronLogs).mockResolvedValue({ logs: [], total: 0, limit: 30, offset: 0 });
    vi.mocked(api.listExternalAgents).mockResolvedValue({
      enabled: false, sync_interval_minutes: 60, conflict_policy: 'skip', available: [], configured: [],
    });
  });

  it('names Loom, the namespace and the gateway it is reached through', async () => {
    vi.mocked(api.getMemuHealth).mockResolvedValue(loomHealth({}));

    render(<DiagnosticsPage />);

    expect(await screen.findByText('Loom Memory Service')).toBeInTheDocument();
    expect(screen.getByText('agent-home')).toBeInTheDocument();
    expect(screen.getByText(/Through the MCP gateway at http:\/\/192\.0\.2\.1:3130/)).toHaveTextContent('org acme');
    expect(screen.getByText(/namespace from Loom/)).toBeInTheDocument();
    expect(screen.queryByText('DB Size')).not.toBeInTheDocument();
    // No dashboard address for people: no link at all.
    expect(screen.queryByRole('link', { name: /Open dashboard/ })).not.toBeInTheDocument();
  });

  it('says why Loom cannot be reached, and links the dashboard when there is one', async () => {
    const url = 'https://loom.example.test/memories?namespace=agent-home';
    vi.mocked(api.getMemuHealth).mockResolvedValue(loomHealth(
      { error: "the MCP gateway's catalog lists no 'loom' server for this agent", namespace: '' }, url,
    ));

    render(<DiagnosticsPage />);

    expect(await screen.findByText(/catalog lists no 'loom' server/)).toBeInTheDocument();
    expect(screen.getByText('not resolved')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /Open dashboard/ })).toHaveAttribute('href', url);
  });

  it('keeps the memU card as it was', async () => {
    vi.mocked(api.getMemuHealth).mockResolvedValue({
      backend: 'memu', service_available: true, initialized_at: new Date().toISOString(),
      database: { ...DATABASE, db_size_mb: 2 }, operations: {}, in_flight: [],
    });

    render(<DiagnosticsPage />);

    expect(await screen.findByText('memU Memory Service')).toBeInTheDocument();
    expect(screen.getByText('DB Size')).toBeInTheDocument();
    expect(screen.queryByText(/MCP gateway/)).not.toBeInTheDocument();
  });
});
