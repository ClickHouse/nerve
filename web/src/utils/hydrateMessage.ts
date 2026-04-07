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

  // If ordered blocks are stored, use them directly (preserves interleaving)
  if (raw.blocks && Array.isArray(raw.blocks)) {
    const blocks: MessageBlock[] = raw.blocks.map((b: any) => {
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

    return {
      id: raw.id,
      role: 'assistant',
      blocks,
      channel: raw.channel,
      created_at: raw.created_at,
    };
  }

  // Fallback for pre-migration messages without blocks column:
  // thinking → tool_calls → text (loses interleaving)
  const blocks: MessageBlock[] = [];

  if (raw.thinking) {
    blocks.push({ type: 'thinking', content: raw.thinking });
  }

  if (raw.tool_calls) {
    const toolCalls = typeof raw.tool_calls === 'string' ? JSON.parse(raw.tool_calls) : raw.tool_calls;
    for (const tc of toolCalls) {
      blocks.push({
        type: 'tool_call',
        toolUseId: tc.tool_use_id || '',
        tool: tc.tool,
        input: tc.input || {},
        result: tc.result,
        isError: tc.is_error,
        status: 'complete',
      });
    }
  }

  if (raw.content) {
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
