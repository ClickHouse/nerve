import type { ChatMessage } from '../types/chat';

/**
 * Start index of a bounded tail of already-loaded messages. Bound both
 * messages and blocks: one agent turn may contain thousands of tool calls.
 * Always include at least one message, even if it exceeds the block budget.
 */
export function recentMessageStart(
  messages: readonly Pick<ChatMessage, 'blocks'>[],
  endExclusive = messages.length,
  maxMessages = 30,
  maxBlocks = 400,
): number {
  let start = Math.max(0, Math.min(endExclusive, messages.length));
  let count = 0;
  let blocks = 0;
  while (start > 0) {
    const nextBlocks = Math.max(1, messages[start - 1].blocks.length);
    if (count > 0 && (count >= maxMessages || blocks + nextBlocks > maxBlocks)) break;
    count += 1;
    blocks += nextBlocks;
    start -= 1;
  }
  return start;
}
