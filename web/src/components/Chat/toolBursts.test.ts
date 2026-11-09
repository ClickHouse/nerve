import { describe, expect, it } from 'vitest';
import type { MessageBlock, ToolCallBlockData } from '../../types/chat';
import type { RenderItem, ToolBurst } from '../../types/renderBlocks';
import { burstSummary, groupToolBursts, isPinnedCall } from './toolBursts';

let nextId = 0;
function call(tool: string, input: Record<string, unknown> = {}, extra: Partial<ToolCallBlockData> = {}): ToolCallBlockData {
  nextId += 1;
  return { type: 'tool_call', toolUseId: `tu-${nextId}`, tool, input, result: 'ok', status: 'complete', ...extra };
}
const text = (content: string): MessageBlock => ({ type: 'text', content });
const thinking = (content: string): MessageBlock => ({ type: 'thinking', content });
const read = (path: string) => call('Read', { file_path: path });

const NO_REPEATS: ReadonlySet<string> = new Set();

/** The item types, with each burst's rows in brackets. */
function shape(items: RenderItem[]): string[] {
  return items.map(item => (item.type === 'tool_burst'
    ? `burst[${item.items.map(row => (row.type === 'tool_call' ? row.tool : row.type)).join(',')}]`
    : item.type === 'tool_call' ? item.tool : item.type));
}

describe('groupToolBursts', () => {
  it('keeps thinking and different tools in one burst between two text blocks', () => {
    const items = groupToolBursts([
      text('I look first.'),
      thinking('where is it'), read('a.ts'), call('Bash'), thinking('found it'), call('Grep'), call('Edit'),
      text('Done.'),
    ], NO_REPEATS);
    expect(shape(items)).toEqual(['text', 'burst[thinking,Read,Bash,thinking,Grep,Edit]', 'text']);
    expect((items[1] as ToolBurst).calls.map(c => c.tool)).toEqual(['Read', 'Bash', 'Grep', 'Edit']);
  });

  it('groups consecutive calls of one tool inside the burst', () => {
    const items = groupToolBursts([read('a.ts'), read('b.ts'), read('c.ts'), call('Bash')], NO_REPEATS);
    expect(shape(items)).toEqual(['burst[tool_call_group,Bash]']);
  });

  it('ends a burst at any block that is not thinking or a tool call', () => {
    const items = groupToolBursts([
      call('Bash'), { type: 'image', url: '/x.png', filename: 'x.png', media_type: 'image/png' }, call('Bash'),
    ], NO_REPEATS);
    expect(shape(items)).toEqual(['burst[Bash]', 'image', 'burst[Bash]']);
  });

  it.each([
    'mcp__nerve__ask_user', 'AskUserQuestion', 'mcp__nerve__ask_form', 'ExitPlanMode', 'mcp__nerve__send_file',
    'mcp__nerve__plan_propose', 'mcp__nerve__plan_update',
  ])('never puts %s in a burst', (tool) => {
    const items = groupToolBursts([call('Bash'), call('Read'), call(tool), call('Bash')], NO_REPEATS);
    expect(shape(items)).toEqual(['burst[Bash,Read]', tool, 'burst[Bash]']);
  });

  it('keeps the first workflow-run card out of a burst, and its repeat rows in', () => {
    const start = call('mcp__nerve__workflow_run_start', {}, { result: 'Workflow run wfr-0000000a created (running).' });
    const status = call('mcp__nerve__workflow_run_status', {}, { result: 'wfr-0000000a [done] Run' });
    const repeats = new Set([status.toolUseId]);
    expect(isPinnedCall(start, repeats)).toBe(true);
    expect(isPinnedCall(status, repeats)).toBe(false);
    const items = groupToolBursts([call('Bash'), start, call('Read'), status], repeats);
    expect(shape(items)).toEqual(['burst[Bash]', 'mcp__nerve__workflow_run_start', 'burst[Read,mcp__nerve__workflow_run_status]']);
  });

  it('puts a failed workflow_run call in the burst: it has no card', () => {
    const failed = call('mcp__nerve__workflow_run_status', {}, { result: 'no such workflow run', isError: true });
    expect(isPinnedCall(failed, NO_REPEATS)).toBe(false);
  });
});

describe('burstSummary', () => {
  it.each<[string, ToolCallBlockData[], string]>([
    ['Read', [read('a.ts'), read('b.ts')], 'Read 2 files'],
    ['Read of one file twice', [read('a.ts'), read('a.ts')], 'Read 1 file'],
    ['Bash', [call('Bash'), call('Bash'), call('Bash')], 'Ran 3 commands'],
    ['Edit and Write', [call('Edit', { file_path: 'a.ts' }), call('Write', { file_path: 'b.ts' }), call('Edit', { file_path: 'a.ts' })], 'Edited 2 files'],
    ['Grep and Glob', [call('Grep'), call('Glob')], 'Searched 2 times'],
    ['one search', [call('Grep')], 'Searched once'],
    ['WebFetch', [call('WebFetch'), call('WebFetch')], 'Fetched 2 pages'],
    ['WebSearch', [call('WebSearch')], 'Searched the web once'],
    ['Agent and Task', [call('Agent'), call('Task')], 'Started 2 sub-agents'],
    ['MCP tools', [call('mcp__nerve__memory_recall'), call('mcp__nerve__task_list')], 'Used 2 nerve tools'],
    ['plugin MCP tool', [call('mcp__plugin_Notion_notion__search')], 'Used 1 Notion tool'],
    ['other tools alone', [call('TodoWrite'), call('Skill')], 'Used 2 tools'],
    ['other tool with known work', [call('Bash'), call('TodoWrite')], 'Ran 1 command, used 1 other tool'],
  ])('names %s', (_name, calls, expected) => {
    expect(burstSummary(calls)).toBe(expected);
  });

  it('orders the phrases by the first call of each kind', () => {
    expect(burstSummary([
      read('a.ts'), call('Bash'), read('b.ts'), call('Grep'), call('Edit', { file_path: 'a.ts' }), read('c.ts'),
      call('Bash'), call('Glob'), call('Edit', { file_path: 'b.ts' }), call('Bash'), call('Bash'),
    ])).toBe('Read 3 files, ran 4 commands, searched 2 times, edited 2 files');
  });

  it('counts calls without a file path one by one', () => {
    expect(burstSummary([call('Read'), call('Read')])).toBe('Read 2 files');
  });
});
