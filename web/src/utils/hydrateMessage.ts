import type { ChatMessage, MessageBlock } from '../types/chat';

export function hydrateMessage(raw: any): ChatMessage {
  if (raw.role === 'user') {
    const userBlocks: MessageBlock[] = [{ type: 'text', content: raw.content || '' }];
    // Restore image/file blocks from DB (uploaded files)
    if (raw.blocks && Array.isArray(raw.blocks)) {
      for (const b of raw.blocks) {
        if (b.type === 'image') {
          userBlocks.push({ type: 'image', url: b.url || '', filename: b.filename || '', media_type: b.media_type || '' });
        } else if (b.type === 'file') {
          userBlocks.push({ type: 'file', url: b.url || '', filename: b.filename || '', size: b.size });
        }
      }
    }
    return {
      id: raw.id,
      role: 'user',
      blocks: userBlocks,
      channel: raw.channel,
      created_at: raw.created_at,
    };
  }

  // Assistant messages always carry an ordered `blocks` array (V26
  // migration backfilled any legacy rows that only had the dropped
  // `tool_calls` column).
  const rawBlocks = Array.isArray(raw.blocks) ? raw.blocks : [];
  const blocks: MessageBlock[] = rawBlocks.map((b: any) => {
    if (b.type === 'thinking') {
      return { type: 'thinking' as const, content: b.content || '' };
    }
    if (b.type === 'tool_call') {
      return {
        type: 'tool_call' as const,
        toolUseId: b.tool_use_id || '',
        tool: b.tool,
        input: b.input || {},
        result: b.result,
        isError: b.is_error,
        status: 'complete' as const,
      };
    }
    if (b.type === 'image') {
      return { type: 'image' as const, url: b.url || '', filename: b.filename || '', media_type: b.media_type || '' };
    }
    if (b.type === 'file') {
      return { type: 'file' as const, url: b.url || '', filename: b.filename || '', size: b.size };
    }
    // Default: text
    return { type: 'text' as const, content: b.content || '' };
  });

  // If a row somehow has neither blocks nor any reconstructed content
  // (shouldn't happen post-V26), fall back to whatever raw.content is so
  // we don't render a completely empty message.
  if (blocks.length === 0 && raw.content) {
    blocks.push({ type: 'text', content: raw.content });
  }

  return {
    id: raw.id,
    role: 'assistant',
    blocks,
    channel: raw.channel,
    created_at: raw.created_at,
  };
}
