import { describe, expect, it } from 'vitest';
import type { MessageBlock, ToolCallBlockData } from '../types/chat';
import {
  extractWorkflowRunId,
  findRepeatWorkflowRunCalls,
  isWorkflowRunTool,
  toolResultText,
  workflowRunCardId,
} from './workflowRunCards';

const START = 'mcp__nerve__workflow_run_start';
const STATUS = 'mcp__nerve__workflow_run_status';

function call(toolUseId: string, tool: string, result?: string, isError = false): ToolCallBlockData {
  return {
    type: 'tool_call',
    toolUseId,
    tool,
    input: {},
    result,
    isError,
    status: result === undefined ? 'running' : 'complete',
  };
}

const started = (runId: string) => `Workflow run ${runId} created (running). Engine: claude-workflow.`;
const status = (runId: string, state = 'running') => `${runId} [${state}] Demo run — spent $0.00 / budget $1.00`;

describe('extractWorkflowRunId', () => {
  it('reads the id from a plain-text result', () => {
    expect(extractWorkflowRunId(started('wfr-0000000a'))).toBe('wfr-0000000a');
  });

  it('reads the id from an MCP content-block array', () => {
    const result = JSON.stringify([{ type: 'text', text: status('wfr-0000000b') }]);
    expect(extractWorkflowRunId(result)).toBe('wfr-0000000b');
  });

  it('returns null when there is no result or no id', () => {
    expect(extractWorkflowRunId(undefined)).toBeNull();
    expect(extractWorkflowRunId('No such workflow run.')).toBeNull();
  });
});

describe('isWorkflowRunTool', () => {
  it('matches the start and status tools only', () => {
    expect(isWorkflowRunTool(START)).toBe(true);
    expect(isWorkflowRunTool(STATUS)).toBe(true);
    expect(isWorkflowRunTool('mcp__nerve__workflow_run_kill')).toBe(false);
    expect(isWorkflowRunTool('Bash')).toBe(false);
  });
});

describe('toolResultText', () => {
  it('joins the text blocks of an MCP content-block array', () => {
    const result = JSON.stringify([
      { type: 'text', text: 'line one' },
      { type: 'image', data: '' },
      { type: 'text', text: 'line two' },
    ]);
    expect(toolResultText(result)).toBe('line one\nline two');
  });

  it('returns plain text and non-array JSON unchanged', () => {
    expect(toolResultText('plain text')).toBe('plain text');
    expect(toolResultText('{"id": 1}')).toBe('{"id": 1}');
  });
});

describe('workflowRunCardId', () => {
  it('returns the run id for a completed start or status call', () => {
    expect(workflowRunCardId(call('t1', START, started('wfr-0000000a')))).toBe('wfr-0000000a');
    expect(workflowRunCardId(call('t2', STATUS, status('wfr-0000000a')))).toBe('wfr-0000000a');
  });

  it('returns null for calls that render no card', () => {
    expect(workflowRunCardId(call('t1', STATUS))).toBeNull();
    expect(workflowRunCardId(call('t2', STATUS, status('wfr-0000000a'), true))).toBeNull();
    expect(workflowRunCardId(call('t3', 'Bash', 'wfr-0000000a'))).toBeNull();
  });
});

describe('findRepeatWorkflowRunCalls', () => {
  it('keeps the card on the first call and marks later calls for the same run', () => {
    const blocks: MessageBlock[] = [
      call('t1', START, started('wfr-0000000a')),
      { type: 'thinking', content: 'still running, check again' },
      call('t2', STATUS, status('wfr-0000000a')),
      call('t3', STATUS, status('wfr-0000000a', 'done')),
    ];
    expect(findRepeatWorkflowRunCalls(blocks)).toEqual(new Set(['t2', 't3']));
  });

  it('gives each run its own card', () => {
    const blocks: MessageBlock[] = [
      call('t1', START, started('wfr-0000000a')),
      call('t2', START, started('wfr-0000000b')),
      call('t3', STATUS, status('wfr-0000000b')),
    ];
    expect(findRepeatWorkflowRunCalls(blocks)).toEqual(new Set(['t3']));
  });

  it('does not let a call that renders no card claim the run', () => {
    const blocks: MessageBlock[] = [
      // Still in flight: no result, so no id and no card yet.
      call('t1', STATUS),
      // Failed: rendered as the generic tool block, not as a card.
      call('t2', STATUS, status('wfr-0000000a'), true),
      call('t3', STATUS, status('wfr-0000000a')),
      call('t4', STATUS, status('wfr-0000000a', 'done')),
    ];
    expect(findRepeatWorkflowRunCalls(blocks)).toEqual(new Set(['t4']));
  });

  it('ignores run ids in the results of other tools', () => {
    const blocks: MessageBlock[] = [
      call('t1', 'Bash', 'wfr-0000000a'),
      call('t2', STATUS, status('wfr-0000000a')),
    ];
    expect(findRepeatWorkflowRunCalls(blocks)).toEqual(new Set());
  });
});
