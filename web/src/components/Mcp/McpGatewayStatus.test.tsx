import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { McpGatewayStatus, type McpGatewayDiagnostics } from './McpGatewayStatus';

function status(overrides: Partial<McpGatewayDiagnostics> = {}): McpGatewayDiagnostics {
  return {
    url: 'http://192.0.2.1:8080',
    generation: 7,
    digest: `sha256:${'0'.repeat(64)}`,
    servers: ['docs', 'github'],
    applied_at: '2026-10-08T10:00:00Z',
    checked_at: '2026-10-08T10:05:00Z',
    error: null,
    retrying: false,
    ...overrides,
  };
}

describe('McpGatewayStatus', () => {
  it('reports the applied catalog generation and its servers', () => {
    render(<McpGatewayStatus status={status()} />);
    const group = screen.getByRole('group', { name: 'MCP gateway' });
    expect(group).toHaveTextContent('catalog generation 7, 2 servers');
    expect(group).toHaveTextContent('docs, github');
    expect(group).not.toHaveTextContent('retrying');
  });

  it('reports a gateway that gave no catalog yet', () => {
    render(<McpGatewayStatus status={status({
      generation: null,
      digest: null,
      servers: [],
      applied_at: null,
      error: 'cannot reach the MCP gateway (ConnectError)',
      retrying: true,
    })} />);
    const group = screen.getByRole('group', { name: 'MCP gateway' });
    expect(group).toHaveTextContent('no catalog applied');
    expect(group).toHaveTextContent('cannot reach the MCP gateway (ConnectError)');
    expect(group).toHaveTextContent('retrying');
  });

  it('keeps the applied generation during an outage', () => {
    render(<McpGatewayStatus status={status({
      servers: ['docs'], error: 'MCP gateway answered HTTP 503', retrying: true,
    })} />);
    const group = screen.getByRole('group', { name: 'MCP gateway' });
    expect(group).toHaveTextContent('catalog generation 7, 1 server');
    expect(group).toHaveTextContent('MCP gateway answered HTTP 503');
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

describe('McpGatewayStatus with running sessions', () => {
  it('counts the sessions on the applied generation', () => {
    render(<McpGatewayStatus status={status({
      sessions: [
        { session_id: 's-1', backend: 'claude', built_generation: 6, generation: 7, pending: [] },
        { session_id: 's-2', backend: 'codex', built_generation: 7, generation: 7, pending: [] },
      ],
    })} />);
    const group = screen.getByRole('group', { name: 'MCP gateway' });
    expect(group).toHaveTextContent('2 of 2 running sessions on this generation');
    expect(screen.queryByRole('status')).toBeNull();
  });

  it('names a session that lacks a part of the catalog', () => {
    render(<McpGatewayStatus status={status({
      generation: 8,
      sessions: [
        { session_id: 's-1', backend: 'claude', built_generation: 7, generation: 8, pending: ['jira'] },
        { session_id: 's-2', backend: 'codex', built_generation: 7, generation: 7, pending: ['docs', 'github'] },
        { session_id: 's-3', backend: 'claude', built_generation: 8, generation: 8, pending: [] },
      ],
    })} />);
    const group = screen.getByRole('group', { name: 'MCP gateway' });
    expect(group).toHaveTextContent('1 of 3 running sessions on this generation');
    const lines = screen.getAllByRole('status').map((line) => line.textContent);
    expect(lines).toEqual([
      'Session s-1 (claude): generation 8; not followed: jira',
      'Session s-2 (codex): generation 7; not followed: docs, github',
    ]);
  });

  it('says nothing about sessions without a catalog', () => {
    render(<McpGatewayStatus status={status({
      generation: null, digest: null, servers: [], applied_at: null,
      sessions: [
        { session_id: 's-1', backend: 'claude', built_generation: null, generation: null, pending: [] },
      ],
    })} />);
    const group = screen.getByRole('group', { name: 'MCP gateway' });
    expect(group).not.toHaveTextContent('running session');
  });
});
