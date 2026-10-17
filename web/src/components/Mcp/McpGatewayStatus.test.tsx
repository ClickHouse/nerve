import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import {
  McpGatewayStatus, type McpGatewayDiagnostics, type McpGatewayServer,
} from './McpGatewayStatus';

const DESIRED = `sha256:0c6e3487${'1'.repeat(56)}`;

function server(id: string, overrides: Partial<McpGatewayServer> = {}): McpGatewayServer {
  return {
    id,
    desired_revision: `sha256:${'d'.repeat(64)}`,
    tool_mode: 'selected',
    tool_changes: 'accept',
    inventory: {
      revision: `sha256:${'a'.repeat(64)}`,
      tool_count: 2,
      observed_at: '2026-10-17T12:00:00Z',
      desired_revision: `sha256:${'d'.repeat(64)}`,
    },
    health: { inventory_state: 'current', checked_at: '2026-10-17T12:00:00Z', failure: null },
    ...overrides,
  };
}

function status(overrides: Partial<McpGatewayDiagnostics> = {}): McpGatewayDiagnostics {
  return {
    url: 'http://192.0.2.1:8080',
    generation: 7,
    digest: `sha256:${'0'.repeat(64)}`,
    desired_revision: DESIRED,
    servers: [server('docs'), server('github')],
    fetched_at: '2026-10-08T10:05:00Z',
    checked_at: '2026-10-08T10:05:00Z',
    error: null,
    retrying: false,
    ...overrides,
  };
}

describe('McpGatewayStatus', () => {
  it('reports the catalog read and its servers, not a running session', () => {
    render(<McpGatewayStatus status={status()} />);
    const group = screen.getByRole('group', { name: 'MCP gateway' });
    expect(group).toHaveTextContent('catalog generation 7, 2 servers for new sessions');
    expect(group).toHaveTextContent('desired revision 0c6e34871111');
    expect(group).toHaveTextContent('docs: 2 tools observed');
    expect(group).toHaveTextContent('github: 2 tools observed');
    expect(group).not.toHaveTextContent('applied');
    expect(group).not.toHaveTextContent('retrying');
  });

  it('reports a gateway that gave no catalog yet', () => {
    render(<McpGatewayStatus status={status({
      generation: null,
      digest: null,
      desired_revision: null,
      servers: [],
      fetched_at: null,
      error: 'cannot reach the MCP gateway (ConnectError)',
      retrying: true,
    })} />);
    const group = screen.getByRole('group', { name: 'MCP gateway' });
    expect(group).toHaveTextContent('no catalog read yet');
    expect(group).not.toHaveTextContent('desired revision');
    expect(group).toHaveTextContent('cannot reach the MCP gateway (ConnectError)');
    expect(group).toHaveTextContent('retrying');
  });

  it('keeps the last catalog during an outage', () => {
    render(<McpGatewayStatus status={status({
      servers: [server('docs')], error: 'MCP gateway answered HTTP 503', retrying: true,
    })} />);
    const group = screen.getByRole('group', { name: 'MCP gateway' });
    expect(group).toHaveTextContent('catalog generation 7, 1 server for new sessions');
    expect(group).toHaveTextContent('MCP gateway answered HTTP 503');
  });

  it('shows what the gateway observed of each server apart from the desired state', () => {
    render(<McpGatewayStatus status={status({
      servers: [
        server('docs', { inventory: null, health: { inventory_state: 'unknown', checked_at: null, failure: null } }),
        server('github', {
          health: { inventory_state: 'stale', checked_at: '2026-10-17T12:05:00Z', failure: 'upstream_timeout' },
        }),
        server('jira', {
          inventory: null,
          health: { inventory_state: 'unknown', checked_at: '2026-10-17T12:05:00Z', failure: 'quota_spent' },
        }),
        server('wiki', {
          inventory: {
            revision: `sha256:${'b'.repeat(64)}`, tool_count: 0,
            observed_at: '2026-10-17T12:00:00Z', desired_revision: `sha256:${'d'.repeat(64)}`,
          },
        }),
        server('zoo', {
          health: { inventory_state: 'stale', checked_at: '2026-10-17T12:05:00Z', failure: 'resources_exhausted' },
        }),
      ],
    })} />);
    const group = screen.getByRole('group', { name: 'MCP gateway' });
    expect(group).toHaveTextContent('catalog generation 7, 5 servers for new sessions');
    expect(group).toHaveTextContent('docs: tools not observed yet');
    expect(group).toHaveTextContent('github: 2 tools observed (stale); last check failed: upstream timeout');
    // A reason that Nerve does not know is a failure without detail.
    expect(group).toHaveTextContent('jira: tools not observed yet; last check failed');
    expect(group).not.toHaveTextContent('quota_spent');
    expect(group).toHaveTextContent('wiki: 0 tools observed');
    expect(group).toHaveTextContent('zoo: 2 tools observed (stale); last check failed: gateway out of memory');
  });

  it('never shows the size of the last list as the tools of a server that gives every tool', () => {
    render(<McpGatewayStatus status={status({
      servers: [
        server('docs', { tool_mode: 'all' }),
        server('wiki', {
          tool_mode: 'all',
          inventory: null,
          health: { inventory_state: 'unknown', checked_at: null, failure: null },
        }),
        server('zoo', {
          tool_mode: 'all',
          health: { inventory_state: 'stale', checked_at: '2026-10-17T12:05:00Z', failure: 'upstream_timeout' },
        }),
      ],
    })} />);
    const group = screen.getByRole('group', { name: 'MCP gateway' });
    expect(group).toHaveTextContent('docs: all tools, 2 observed');
    expect(group).toHaveTextContent('wiki: all tools, not observed yet');
    expect(group).toHaveTextContent('zoo: all tools, 2 observed (stale); last check failed: upstream timeout');
    expect(group).not.toHaveTextContent('2 tools');
  });
});

describe('McpGatewayStatus with a server that a backend leaves out', () => {
  it('names the server, the backend and the reason', () => {
    render(<McpGatewayStatus status={status({
      not_applied: {
        docs: { codex: 'name used by the system configuration (/etc/codex/config.toml)' },
      },
    })} />);
    const group = screen.getByRole('group', { name: 'MCP gateway' });
    expect(screen.getByRole('alert')).toHaveTextContent(
      'docs: Not applied to Codex: name used by the system configuration (/etc/codex/config.toml)',
    );
    expect(group).toHaveTextContent('catalog generation 7, 2 servers');
  });
});
