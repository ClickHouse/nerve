import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { WorkflowRun } from '../../api/client';
import { useWorkflowRunStore } from '../../stores/workflowRunStore';
import type { MessageBlock, ToolCallBlockData } from '../../types/chat';
import { BlockRenderer } from './BlockRenderer';

vi.mock('../../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../api/client')>()),
  api: {
    // Every run under test is seeded into the store, so a fetch means the
    // card did not find it there.
    getWorkflowRun: vi.fn(async () => { throw new Error('unexpected fetch'); }),
  },
  getToken: vi.fn(() => 'token'),
}));

function run(id: string, title: string): WorkflowRun {
  return {
    id,
    engine: 'claude-workflow',
    title,
    spec: { prompt: 'say hello' } as WorkflowRun['spec'],
    status: 'done',
    budget_usd: 1,
    spent_usd: 0.25,
    warned_at: null,
    session_id: `workflow:${id}`,
    journal_dir: null,
    created_by: 'session:test',
    error: null,
    result: 'hello',
    created_at: '2026-01-01T00:00:00Z',
    started_at: '2026-01-01T00:00:00Z',
    finished_at: '2026-01-01T00:00:05Z',
    updated_at: '2026-01-01T00:00:05Z',
  };
}

function call(toolUseId: string, tool: string, result: string): ToolCallBlockData {
  return { type: 'tool_call', toolUseId, tool, input: {}, result, status: 'complete' };
}

const START = 'mcp__nerve__workflow_run_start';
const STATUS = 'mcp__nerve__workflow_run_status';

function renderBlocks(blocks: MessageBlock[]) {
  return render(<MemoryRouter><BlockRenderer blocks={blocks} /></MemoryRouter>);
}

describe('BlockRenderer workflow run cards', () => {
  beforeEach(() => {
    useWorkflowRunStore.setState({
      runs: [run('wfr-0000000a', 'First run'), run('wfr-0000000b', 'Second run')],
    });
  });

  it('shows one card per run when the agent polls it', async () => {
    renderBlocks([
      call('t1', START, 'Workflow run wfr-0000000a created (running).'),
      { type: 'thinking', content: 'check again' },
      call('t2', STATUS, JSON.stringify([{ type: 'text', text: 'wfr-0000000a [running] First run — spent $0.00' }])),
      { type: 'thinking', content: 'check again' },
      call('t3', STATUS, 'wfr-0000000a [done] First run — spent $0.25'),
      call('t4', START, 'Workflow run wfr-0000000b created (running).'),
    ]);

    expect(screen.getAllByText('First run')).toHaveLength(1);
    expect(screen.getAllByText('Second run')).toHaveLength(1);
    expect(screen.getAllByRole('button', { name: /Status check/ })).toHaveLength(2);
  });

  it('shows one card per run inside a group of consecutive status calls', () => {
    renderBlocks([
      call('t1', START, 'Workflow run wfr-0000000a created (running).'),
      call('t2', STATUS, 'wfr-0000000a [running] First run — spent $0.00'),
      call('t3', STATUS, 'wfr-0000000a [done] First run — spent $0.25'),
    ]);

    expect(screen.getAllByText('First run')).toHaveLength(1);
    expect(screen.getAllByRole('button', { name: /Status check/ })).toHaveLength(2);
  });

  it('keeps the cards visible when a collapsed group hides the calls that hold them', () => {
    // Parallel polls of two runs, three times, with nothing in between: one
    // group of six status calls. t1 and t2 hold the cards and fall before the
    // visible tail of three (t4–t6).
    renderBlocks([
      call('t1', STATUS, 'wfr-0000000a [running] First run — spent $0.00'),
      call('t2', STATUS, 'wfr-0000000b [running] Second run — spent $0.00'),
      call('t3', STATUS, 'wfr-0000000a [running] First run — spent $0.10'),
      call('t4', STATUS, 'wfr-0000000b [running] Second run — spent $0.10'),
      call('t5', STATUS, 'wfr-0000000a [done] First run — spent $0.25'),
      call('t6', STATUS, 'wfr-0000000b [done] Second run — spent $0.25'),
    ]);

    expect(screen.getAllByText('First run')).toHaveLength(1);
    expect(screen.getAllByText('Second run')).toHaveLength(1);
    // t3 is the only call the collapsed group hides.
    expect(screen.getByRole('button', { name: /Show 1 more/ })).toHaveAttribute('aria-expanded', 'false');
    expect(screen.getAllByRole('button', { name: /Status check/ })).toHaveLength(3);
  });

  it('expands a repeat row to the result the agent got at that time', async () => {
    renderBlocks([
      call('t1', START, 'Workflow run wfr-0000000a created (running).'),
      call('t2', STATUS, 'wfr-0000000a [running] First run — spent $0.00'),
    ]);

    const row = screen.getByRole('button', { name: /Status check/ });
    expect(row).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByText(/\[running\]/)).toBeNull();

    await userEvent.click(row);

    expect(row).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByText('wfr-0000000a [running] First run — spent $0.00')).toBeInTheDocument();
  });
});
