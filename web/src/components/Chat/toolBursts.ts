import type { MessageBlock, ThinkingBlockData, ToolCallBlockData } from '../../types/chat';
import type { RenderItem, ToolBurst } from '../../types/renderBlocks';
import { formatMcpName } from '../../utils/formatMcpName';
import { groupToolCalls } from '../../utils/groupToolCalls';
import { toolBaseName, toolServer } from '../../utils/toolNames';
import { workflowRunCardId } from '../../utils/workflowRunCards';
import { isAskFormTool } from './tools/formQuestions';

/**
 * Tool bursts in the chat transcript.
 *
 * A burst is the longest run of thinking and tool_call blocks between two
 * other blocks of one message (usually text). A finished burst shows as one
 * line that names the work ("Read 3 files, ran 4 commands") and opens to the
 * rows. Cards that the person acts on stay outside every burst.
 */

/**
 * True for a call whose card the person acts on or reads: a question
 * (`ask_user`, `AskUserQuestion`, `ask_form`), the plan decision
 * (`ExitPlanMode`), a file that the agent sends (`send_file`), or the live
 * card of a workflow run. Only the first call for a run in a message shows
 * that card; later calls for the run show a compact row, which a burst can
 * hold.
 */
export function isPinnedCall(block: ToolCallBlockData, repeatRunCalls: ReadonlySet<string>): boolean {
  if (block.tool === 'AskUserQuestion' || block.tool === 'ExitPlanMode') return true;
  const baseName = toolBaseName(block.tool);
  if (isAskFormTool(block.tool) || baseName === 'ask_user' || baseName === 'send_file') return true;
  return workflowRunCardId(block) !== null && !repeatRunCalls.has(block.toolUseId);
}

type BurstBlock = ThinkingBlockData | ToolCallBlockData;

/**
 * The render items of one message with tool bursts: each run of thinking and
 * tool_call blocks becomes one `tool_burst` (consecutive same-tool calls are
 * grouped inside it). Every other block, and every pinned call, stays a
 * separate item and ends the run.
 */
export function groupToolBursts(blocks: MessageBlock[], repeatRunCalls: ReadonlySet<string>): RenderItem[] {
  const result: RenderItem[] = [];
  let run: BurstBlock[] = [];

  function flushRun() {
    if (run.length === 0) return;
    result.push({
      type: 'tool_burst',
      items: groupToolCalls(run),
      calls: run.filter(block => block.type === 'tool_call'),
    } satisfies ToolBurst);
    run = [];
  }

  for (const block of blocks) {
    if (block.type === 'thinking' || (block.type === 'tool_call' && !isPinnedCall(block, repeatRunCalls))) {
      run.push(block);
    } else {
      flushRun();
      result.push(block);
    }
  }
  flushRun();
  return result;
}

type Work = 'read' | 'edit' | 'command' | 'search' | 'fetch' | 'webSearch' | 'agent';

const WORK_OF_TOOL: Record<string, Work> = {
  Read: 'read',
  Edit: 'edit',
  MultiEdit: 'edit',
  Write: 'edit',
  NotebookEdit: 'edit',
  Bash: 'command',
  Grep: 'search',
  Glob: 'search',
  WebFetch: 'fetch',
  WebSearch: 'webSearch',
  Agent: 'agent',
  Task: 'agent',
};

/** Work counted in distinct files, not in calls. */
const FILE_WORK: ReadonlySet<string> = new Set<Work>(['read', 'edit']);

const OTHER = 'other';
const MCP_KEY_PREFIX = 'mcp:';

const count = (n: number, noun: string) => `${n} ${noun}${n === 1 ? '' : 's'}`;
const times = (n: number) => (n === 1 ? 'once' : `${n} times`);

const PHRASES: Record<Work, (n: number) => string> = {
  read: n => `read ${count(n, 'file')}`,
  edit: n => `edited ${count(n, 'file')}`,
  command: n => `ran ${count(n, 'command')}`,
  search: n => `searched ${times(n)}`,
  fetch: n => `fetched ${count(n, 'page')}`,
  webSearch: n => `searched the web ${times(n)}`,
  agent: n => `started ${count(n, 'sub-agent')}`,
};

/** The kind of work a call does: a `Work`, `mcp:<server>`, or `other`. */
function workKey(tool: string): string {
  const server = toolServer(tool);
  if (server) return MCP_KEY_PREFIX + server;
  return WORK_OF_TOOL[tool] ?? OTHER;
}

function filePath(call: ToolCallBlockData): string {
  const path = call.input.file_path ?? call.input.notebook_path;
  return typeof path === 'string' ? path : '';
}

function phrase(key: string, n: number, onlyKind: boolean): string {
  if (key.startsWith(MCP_KEY_PREFIX)) {
    return `used ${count(n, `${formatMcpName(key.slice(MCP_KEY_PREFIX.length))} tool`)}`;
  }
  if (key === OTHER) return `used ${count(n, onlyKind ? 'tool' : 'other tool')}`;
  return PHRASES[key as Work](n);
}

/**
 * One sentence that names the work of the calls, one phrase per kind of work
 * in the order it first appears: "Read 3 files, ran 4 commands, searched
 * 2 times". Reads and edits count distinct files.
 */
export function burstSummary(calls: ToolCallBlockData[]): string {
  const units = new Map<string, Set<string>>();
  calls.forEach((call, index) => {
    const key = workKey(call.tool);
    const unit = (FILE_WORK.has(key) && filePath(call)) || `#${index}`;
    const seen = units.get(key) ?? new Set<string>();
    seen.add(unit);
    units.set(key, seen);
  });
  const sentence = [...units]
    .map(([key, seen]) => phrase(key, seen.size, units.size === 1))
    .join(', ');
  return sentence.charAt(0).toUpperCase() + sentence.slice(1);
}
