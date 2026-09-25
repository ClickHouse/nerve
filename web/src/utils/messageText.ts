import type { ChatMessage, TextBlockData } from '../types/chat';

/** Joined text content of a message (copy action, previews). */
export function messageText(msg: ChatMessage): string {
  return msg.blocks
    .filter((b): b is TextBlockData => b.type === 'text')
    .map(b => b.content)
    .join('\n');
}
