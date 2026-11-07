import type { MessageBlock, ToolCallBlockData } from '../types/chat';
import type { ToolCallGroup } from '../types/renderBlocks';

const isToolCall = (block: MessageBlock): block is ToolCallBlockData => block.type === 'tool_call';

/**
 * Groups consecutive tool_call blocks with the same `tool` name.
 *
 * - Single tool calls pass through ungrouped.
 * - 2+ consecutive same-tool calls become a ToolCallGroup.
 * - Any non-tool block (text, thinking) breaks the current group.
 * - Different tool names break the current group.
 */
export function groupToolCalls<B extends MessageBlock>(blocks: B[]): (B | ToolCallGroup)[] {
  const result: (B | ToolCallGroup)[] = [];
  let currentRun: (B & ToolCallBlockData)[] = [];
  let currentTool: string | null = null;

  function flushRun() {
    if (currentRun.length === 0) return;
    if (currentRun.length === 1) {
      result.push(currentRun[0]);
    } else {
      result.push({
        type: 'tool_call_group',
        tool: currentTool!,
        blocks: currentRun,
      } satisfies ToolCallGroup);
    }
    currentRun = [];
    currentTool = null;
  }

  for (const block of blocks) {
    if (isToolCall(block)) {
      if (currentTool === block.tool) {
        currentRun.push(block);
      } else {
        flushRun();
        currentTool = block.tool;
        currentRun = [block];
      }
    } else {
      flushRun();
      result.push(block);
    }
  }

  flushRun();
  return result;
}
