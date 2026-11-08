import type { MessageBlock, ThinkingBlockData, ToolCallBlockData } from './chat';

/**
 * A group of consecutive tool_call blocks sharing the same `tool` name.
 * Only created when the run of consecutive same-tool calls has length >= 2.
 */
export interface ToolCallGroup {
  type: 'tool_call_group';
  tool: string;
  blocks: ToolCallBlockData[];
}

/** A row in a tool burst: a thinking block, one call, or a same-tool group. */
export type BurstItem = ThinkingBlockData | ToolCallBlockData | ToolCallGroup;

/**
 * The longest run of thinking and tool_call blocks between two other blocks
 * of one message (usually text). Interactive cards are never in a burst.
 */
export interface ToolBurst {
  type: 'tool_burst';
  /** The rows, with consecutive same-tool calls grouped. */
  items: BurstItem[];
  /** Every tool call of the burst, in order. */
  calls: ToolCallBlockData[];
}

/**
 * Union type used by rendering components after grouping.
 */
export type RenderItem = MessageBlock | ToolCallGroup | ToolBurst;
