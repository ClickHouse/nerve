import { render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../api/client', () => ({
  api: {
    listMcpServers: vi.fn(),
    getMcpServer: vi.fn(),
    reloadMcpServers: vi.fn(),
  },
}));

const client = await import('../api/client');
const { McpServersPage } = await import('./McpServersPage');
const { useMcpStore } = await import('../stores/mcpStore');

const api = client.api as unknown as Record<string, ReturnType<typeof vi.fn>>;

function server(name: string, extra: Record<string, unknown> = {}) {
  return {
    name,
    type: name === 'nerve' ? 'sdk' : 'http',
    enabled: true,
    tool_count: 2,
    total_invocations: 0,
    success_count: 0,
    avg_duration_ms: null,
    last_used: null,
    first_seen_at: '2026-10-01T00:00:00Z',
    last_seen_at: '2026-10-01T00:00:00Z',
    ...extra,
  };
}

async function renderPage() {
  render(<MemoryRouter><McpServersPage /></MemoryRouter>);
  await screen.findByRole('heading', { name: 'MCP Servers' });
  await vi.waitFor(() => expect(useMcpStore.getState().loading).toBe(false));
}

beforeEach(() => {
  vi.clearAllMocks();
  useMcpStore.setState({ servers: [], managedByOrganization: false, loading: true });
});

describe('McpServersPage in local mode', () => {
  it('offers the reload and marks no server as managed', async () => {
    api.listMcpServers.mockResolvedValue({
      servers: [server('nerve'), server('grafana', { type: 'stdio' })],
    });
    await renderPage();

    expect(screen.getByRole('button', { name: /reload/i })).toBeInTheDocument();
    expect(screen.queryByText('Managed by organization')).not.toBeInTheDocument();
    expect(screen.queryByRole('note')).not.toBeInTheDocument();
    expect(screen.getByRole('article', { name: 'grafana' })).toBeInTheDocument();
  });

  it('says where to add servers when there is none', async () => {
    api.listMcpServers.mockResolvedValue({ servers: [] });
    await renderPage();
    expect(screen.getByText(/Add external MCP servers in/)).toBeInTheDocument();
  });
});

describe('McpServersPage in external mode', () => {
  const managedList = {
    managed_by: 'organization',
    servers: [
      server('docs', {
        managed_by: 'organization',
        display_name: 'Docs',
        description: 'Search the product documentation.',
      }),
      server('nerve', { managed_by: null }),
    ],
  };

  it('shows the catalog servers read-only', async () => {
    api.listMcpServers.mockResolvedValue(managedList);
    await renderPage();

    expect(screen.queryByRole('button', { name: /reload/i })).not.toBeInTheDocument();
    expect(screen.getByRole('note')).toHaveTextContent(
      'Your organization manages the MCP servers of this agent through the MCP gateway.',
    );

    const docs = screen.getByRole('article', { name: 'Docs' });
    expect(within(docs).getByText('Managed by organization')).toBeInTheDocument();
    expect(within(docs).getByText('Search the product documentation.')).toBeInTheDocument();

    const nerve = screen.getByRole('article', { name: 'nerve' });
    expect(within(nerve).queryByText('Managed by organization')).not.toBeInTheDocument();
  });

  it('shows a server that Codex sessions do not get', async () => {
    api.listMcpServers.mockResolvedValue({
      managed_by: 'organization',
      servers: [
        server('docs', {
          managed_by: 'organization',
          display_name: 'Docs',
          not_applied: { codex: 'name used by the system configuration (/etc/codex/config.toml)' },
        }),
        server('github', { managed_by: 'organization', display_name: 'GitHub', not_applied: {} }),
      ],
    });
    await renderPage();

    const docs = screen.getByRole('article', { name: 'Docs' });
    expect(within(docs).getByRole('alert')).toHaveTextContent(
      'Not applied to Codex: name used by the system configuration (/etc/codex/config.toml)',
    );
    const github = screen.getByRole('article', { name: 'GitHub' });
    expect(within(github).queryByRole('alert')).not.toBeInTheDocument();
  });

  it('does not point at the configuration files', async () => {
    api.listMcpServers.mockResolvedValue({ managed_by: 'organization', servers: [] });
    await renderPage();
    expect(screen.queryByText(/Add external MCP servers in/)).not.toBeInTheDocument();
    expect(screen.getByRole('note')).toBeInTheDocument();
  });
});
