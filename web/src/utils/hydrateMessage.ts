import type { ChatMessage, MessageBlock } from '../types/chat';
import { mcpAppFields } from './mcpAppFields';

export function hydrateMessage(raw: any): ChatMessage {
  // The chat the row belongs to: what a widget or a view in it is kept under.
  const session = typeof raw.session_id === 'string' && raw.session_id ? { session_id: raw.session_id } : {};
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
      ...session,
      role: 'user',
      blocks: userBlocks,
      channel: raw.channel,
      created_at: raw.created_at,
      native_turn_id: raw.native_turn_id,
      // Resolve the stable id at render time so renamed actors need no message
      // rewrite. Null rows remain unlabeled.
      actor_id: raw.actor_id ?? null,
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
        // Dynamic-workflow snapshot, folded into the block by the backend
        // (merge_workflow_into_call) so the panel reconstructs after reload.
        workflow: b.workflow,
        // The agent record a spawn call created (Agent / Task / Workflow /
        // Monitor / background Bash), stamped when the turn finalized. Old
        // history has none and renders the legacy card.
        ...(typeof b.agent_id === 'string' && b.agent_id ? { agentId: b.agent_id } : {}),
        // The view of an MCP Apps tool and the result it reads, so that a
        // chat opened again shows the view. Other calls carry neither.
        ...mcpAppFields(b),
      };
    }
    if (b.type === 'image') {
      return { type: 'image' as const, url: b.url || '', filename: b.filename || '', media_type: b.media_type || '' };
    }
    if (b.type === 'file') {
      return { type: 'file' as const, url: b.url || '', filename: b.filename || '', size: b.size };
    }
    if (b.type === 'wakeup') {
      return { type: 'wakeup' as const };
    }
    if (b.type === 'auto') {
      // What woke the orchestrator; old markers carry neither field.
      return {
        type: 'auto' as const,
        ...(Array.isArray(b.triggers) ? { triggers: b.triggers } : {}),
        ...(typeof b.more === 'number' ? { more: b.more } : {}),
      };
    }
    if (b.type === 'model_change') {
      return { type: 'model_change' as const, from: b.from, to: b.to || '', downgrade: b.downgrade };
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

  // Assistant authorship is represented by role, not actor_id. Omitting the
  // field prevents backend payloads from attaching a principal accidentally.
  return {
    id: raw.id,
    ...session,
    role: 'assistant',
    blocks,
    channel: raw.channel,
    created_at: raw.created_at,
    native_turn_id: raw.native_turn_id,
  };
}
