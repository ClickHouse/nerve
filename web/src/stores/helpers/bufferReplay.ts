import type { WSMessage } from '../../api/websocket';
import type { ChatMessage, MessageBlock, PanelTab, AgentStatus } from '../../types/chat';
import { extractResultText } from '../../utils/extractResultText';
import type { TodoItem } from '../chatStore';

/**
 * Apply a single stream event to a blocks array (pure function for replay).
 * Skips events with parent_tool_use_id — those belong to panels, not main chat.
 */
export function applyStreamEvent(blocks: MessageBlock[], event: WSMessage): MessageBlock[] {
  // Sub-agent child events go to panels, not main chat
  if ('parent_tool_use_id' in event && event.parent_tool_use_id) return blocks;

  const result = [...blocks];
  switch (event.type) {
    case 'thinking': {
      const last = result[result.length - 1];
      if (last?.type === 'thinking') {
        result[result.length - 1] = { ...last, content: last.content + event.content };
      } else {
        result.push({ type: 'thinking', content: event.content });
      }
      break;
    }
    case 'token': {
      const last = result[result.length - 1];
      if (last?.type === 'text') {
        result[result.length - 1] = { ...last, content: last.content + event.content };
      } else {
        result.push({ type: 'text', content: event.content });
      }
      break;
    }
    case 'tool_use': {
      result.push({
        type: 'tool_call',
        toolUseId: event.tool_use_id || '',
        tool: event.tool,
        input: event.input,
        status: 'running',
      });
      break;
    }
    case 'tool_result': {
      for (let i = 0; i < result.length; i++) {
        const b = result[i];
        if (b.type === 'tool_call' && b.toolUseId === event.tool_use_id) {
          result[i] = { ...b, result: event.result, isError: event.is_error, status: 'complete' as const };
          break;
        }
      }
      break;
    }
    case 'hoa_progress': {
      for (let i = result.length - 1; i >= 0; i--) {
        const b = result[i];
        if (b.type === 'tool_call' && b.tool.includes('hoa_execute')) {
          const prev = b.hoaEvents || [];
          result[i] = { ...b, hoaEvents: [...prev, event.event] };
          break;
        }
      }
      break;
    }
  }
  return result;
}

/** Rebuild panel tabs from buffered WS events (for reconnect replay). */
export function rebuildPanelTabsFromBuffer(
  events: WSMessage[],
  blocks: MessageBlock[],
): { panels: PanelTab[]; activePanelId: string | null } {
  const panels: PanelTab[] = [];
  const panelMap = new Map<string, PanelTab>();

  // First pass: create panel tabs for Task tool_use events
  for (const event of events) {
    if (event.type === 'tool_use' && event.tool === 'Task') {
      const subagentType = String(event.input?.subagent_type || event.input?.model || 'agent');
      const toolUseId = event.tool_use_id || '';
      const block = blocks.find(
        b => b.type === 'tool_call' && b.toolUseId === toolUseId,
      );
      const isComplete = block?.type === 'tool_call' && block.status === 'complete';
      const tab: PanelTab = {
        id: toolUseId,
        type: subagentType === 'Plan' ? 'plan' : 'subagent',
        label: subagentType,
        subagentType,
        description: String(event.input?.description || ''),
        model: event.input?.model ? String(event.input.model) : undefined,
        content: isComplete && block?.type === 'tool_call'
          ? extractResultText(block.result || '')
          : null,
        prompt: String(event.input?.prompt || ''),
        streaming: !isComplete,
        status: isComplete
          ? (block?.type === 'tool_call' && block.isError ? 'error' : 'complete')
          : 'running',
        startedAt: Date.now(),
        completedAt: isComplete ? Date.now() : undefined,
        isError: block?.type === 'tool_call' ? block.isError : false,
        blocks: [],
      };
      panels.push(tab);
      panelMap.set(toolUseId, tab);
    }
  }

  // Second pass: collect child events into their parent panel's blocks
  for (const event of events) {
    if (!('parent_tool_use_id' in event) || !event.parent_tool_use_id) continue;
    const panel = panelMap.get(event.parent_tool_use_id);
    if (!panel) continue;

    if (event.type === 'thinking') {
      const last = panel.blocks[panel.blocks.length - 1];
      if (last?.type === 'thinking') {
        last.content += event.content;
      } else {
        panel.blocks.push({ type: 'thinking', content: event.content });
      }
    } else if (event.type === 'token') {
      const last = panel.blocks[panel.blocks.length - 1];
      if (last?.type === 'text') {
        last.content += event.content;
      } else {
        panel.blocks.push({ type: 'text', content: event.content });
      }
    } else if (event.type === 'tool_use') {
      panel.blocks.push({
        type: 'tool_call',
        toolUseId: event.tool_use_id || '',
        tool: event.tool,
        input: event.input,
        status: 'running',
      });
    } else if (event.type === 'tool_result') {
      for (const b of panel.blocks) {
        if (b.type === 'tool_call' && b.toolUseId === event.tool_use_id) {
          b.result = event.result;
          b.isError = event.is_error;
          b.status = 'complete';
          break;
        }
      }
    }
  }

  // Focus last running tab, or last tab overall
  const lastRunning = [...panels].reverse().find(p => p.status === 'running');
  return {
    panels,
    activePanelId: lastRunning?.id || panels[panels.length - 1]?.id || null,
  };
}

/** Derive agent status from current blocks state. */
export function deriveStatus(blocks: MessageBlock[]): AgentStatus {
  if (blocks.length === 0) return { state: 'thinking' };
  const last = blocks[blocks.length - 1];
  if (last.type === 'thinking') return { state: 'thinking' };
  if (last.type === 'text') return { state: 'writing' };
  if (last.type === 'tool_call' && last.status === 'running') return { state: 'tool', toolName: last.tool };
  return { state: 'thinking' };
}

/** Extract the latest TodoWrite todos from loaded message history. Skip if all done. */
export function extractTodosFromMessages(messages: ChatMessage[]): TodoItem[] {
  // Walk backwards to find the most recent TodoWrite tool call
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (msg.role !== 'assistant') continue;
    for (let j = msg.blocks.length - 1; j >= 0; j--) {
      const block = msg.blocks[j];
      if (block.type === 'tool_call' && block.tool === 'TodoWrite' && Array.isArray(block.input?.todos)) {
        const todos = block.input.todos as TodoItem[];
        // Don't restore a fully-completed list — nothing useful to show
        if (todos.every(t => t.status === 'completed')) return [];
        return todos;
      }
    }
  }
  return [];
}
