import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { ToolCallBlockData } from '../../types/chat';

vi.mock('../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));
vi.mock('../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
  },
}));

const { ToolCallGroupBlock } = await import('./ToolCallGroupBlock');

function calls(tool: string, n: number): ToolCallBlockData[] {
  return Array.from({ length: n }, (_, i) => ({
    type: 'tool_call' as const, toolUseId: `toolu_${tool}_${i}`, tool, status: 'complete' as const,
    input: tool === 'Agent'
      ? { description: `Agent number ${i}`, subagent_type: 'general-purpose' }
      : { command: `echo ${i}` },
    result: 'ok',
  }));
}

describe('ToolCallGroupBlock', () => {
  it('keeps the last 8 agent spawns in view and offers the rest', async () => {
    render(<ToolCallGroupBlock group={{ type: 'tool_call_group', tool: 'Agent', blocks: calls('Agent', 12) }} />);
    expect(screen.getByRole('button', { name: /Show 4 more agents/ })).toHaveAttribute('aria-expanded', 'false');
    expect(screen.getByText('12 agents')).toBeInTheDocument();
    expect(screen.queryByText('Agent number 3')).toBeNull();
    expect(screen.getByText('Agent number 4')).toBeInTheDocument();
    expect(screen.getByText('Agent number 11')).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: /Show 4 more agents/ }));
    expect(screen.getByText('Agent number 0')).toBeInTheDocument();
  });

  it('shows a group of up to 8 agents whole', () => {
    render(<ToolCallGroupBlock group={{ type: 'tool_call_group', tool: 'Agent', blocks: calls('Agent', 8) }} />);
    expect(screen.queryByRole('button', { name: /more agents/ })).toBeNull();
    expect(screen.getByText('Agent number 0')).toBeInTheDocument();
  });

  it('still collapses other tools to the last 3', () => {
    render(<ToolCallGroupBlock group={{ type: 'tool_call_group', tool: 'Bash', blocks: calls('Bash', 5) }} />);
    expect(screen.getByRole('button', { name: /Show 2 more/ })).toBeInTheDocument();
    expect(screen.getByText('5 Bash calls')).toBeInTheDocument();
  });
});
