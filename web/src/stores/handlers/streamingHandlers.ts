import type { WSMessage } from '../../api/websocket';
import { extractResultText } from '../../utils/extractResultText';
import { appendBlockToPanel, updateToolResultInPanel, scheduleAutoClose } from '../helpers/blockHelpers';
import type { TodoItem } from '../chatStore';
import type { Get, Set } from './types';

// ------------------------------------------------------------------ //
//  Streaming handlers: thinking, token, tool_use, tool_result         //
// ------------------------------------------------------------------ //

export function handleThinking(
  msg: Extract<WSMessage, { type: 'thinking' }>,
  get: Get,
  set: Set,
): void {
  const state = get();
  const parentId = msg.parent_tool_use_id;
  if (parentId && state.panels.some(p => p.id === parentId && p.status === 'running')) {
    set(s => ({
      panels: appendBlockToPanel(s.panels, parentId, { type: 'thinking', content: msg.content }),
    }));
  } else {
    const blocks = [...state.streamingBlocks];
    const last = blocks[blocks.length - 1];
    if (last?.type === 'thinking') {
      blocks[blocks.length - 1] = { ...last, content: last.content + msg.content };
    } else {
      blocks.push({ type: 'thinking', content: msg.content });
    }
    set({ streamingBlocks: blocks, agentStatus: { state: 'thinking' } });
  }
}

export function handleToken(
  msg: Extract<WSMessage, { type: 'token' }>,
  get: Get,
  set: Set,
): void {
  const state = get();
  const parentId = msg.parent_tool_use_id;
  if (parentId && state.panels.some(p => p.id === parentId && p.status === 'running')) {
    set(s => ({
      panels: appendBlockToPanel(s.panels, parentId, { type: 'text', content: msg.content }),
    }));
  } else {
    const blocks = [...state.streamingBlocks];
    const last = blocks[blocks.length - 1];
    if (last?.type === 'text') {
      blocks[blocks.length - 1] = { ...last, content: last.content + msg.content };
    } else {
      blocks.push({ type: 'text', content: msg.content });
    }
    set({ streamingBlocks: blocks, agentStatus: { state: 'writing' } });
  }
}

export function handleToolUse(
  msg: Extract<WSMessage, { type: 'tool_use' }>,
  get: Get,
  set: Set,
): void {
  const state = get();

  // Is this a Task (sub-agent) call?
  if (msg.tool === 'Task') {
    const toolUseId = msg.tool_use_id || '';
    // Add compact card to main chat
    const blocks = [...state.streamingBlocks];
    blocks.push({
      type: 'tool_call',
      toolUseId,
      tool: msg.tool,
      input: msg.input,
      status: 'running',
    });
    set({ streamingBlocks: blocks, agentStatus: { state: 'tool', toolName: msg.tool } });

    // Open panel tab
    const subagentType = String(msg.input?.subagent_type || msg.input?.model || 'agent');
    const isPlan = subagentType === 'Plan';
    get().openPanelTab({
      id: toolUseId,
      type: isPlan ? 'plan' : 'subagent',
      label: subagentType,
      subagentType,
      description: String(msg.input?.description || ''),
      model: msg.input?.model ? String(msg.input.model) : undefined,
      content: null,
      prompt: String(msg.input?.prompt || ''),
      streaming: true,
      status: 'running',
      startedAt: Date.now(),
      blocks: [],
    });
    return;
  }

  // Is this a child tool call inside a running sub-agent?
  const parentId = msg.parent_tool_use_id;
  if (parentId && state.panels.some(p => p.id === parentId && p.status === 'running')) {
    set(s => ({
      panels: appendBlockToPanel(s.panels, parentId, {
        type: 'tool_call',
        toolUseId: msg.tool_use_id || '',
        tool: msg.tool,
        input: msg.input,
        status: 'running',
      }),
    }));
  } else {
    // Normal: add to main chat
    const blocks = [...state.streamingBlocks];
    blocks.push({
      type: 'tool_call',
      toolUseId: msg.tool_use_id || '',
      tool: msg.tool,
      input: msg.input,
      status: 'running',
    });
    const extraUpdate: Record<string, unknown> = {};
    if (msg.tool === 'TodoWrite' && Array.isArray(msg.input?.todos)) {
      extraUpdate.currentTodos = msg.input.todos as TodoItem[];
    }
    set({ streamingBlocks: blocks, agentStatus: { state: 'tool', toolName: msg.tool }, ...extraUpdate });
  }
}

export function handleToolResult(
  msg: Extract<WSMessage, { type: 'tool_result' }>,
  get: Get,
  set: Set,
): void {
  const state = get();

  // Is this a sub-agent (Task) completing?
  // Check if this tool_use_id matches a panel tab (= it's a Task result)
  const completingTab = state.panels.find(p => p.id === msg.tool_use_id && p.status === 'running');
  if (completingTab) {
    // Update compact card in main chat
    const blocks = state.streamingBlocks.map(b => {
      if (b.type === 'tool_call' && b.toolUseId === msg.tool_use_id) {
        return { ...b, result: msg.result, isError: msg.is_error, status: 'complete' as const };
      }
      return b;
    });
    set({ streamingBlocks: blocks, agentStatus: { state: 'thinking' } });

    // Update panel tab with final content
    get().updatePanelTab(msg.tool_use_id!, {
      content: extractResultText(msg.result),
      streaming: false,
      status: msg.is_error ? 'error' : 'complete',
      isError: msg.is_error || false,
      completedAt: Date.now(),
    });
    // Auto-close non-plan tabs after delay
    if (completingTab.type !== 'plan') {
      scheduleAutoClose(msg.tool_use_id!, get);
    }
    return;
  }

  // Is this a child tool result inside a sub-agent?
  const parentId = msg.parent_tool_use_id;
  if (parentId && state.panels.some(p => p.id === parentId && p.status === 'running')) {
    set(s => ({
      panels: updateToolResultInPanel(s.panels, parentId, msg.tool_use_id || '', msg.result, msg.is_error),
    }));
  } else {
    // Normal: update main chat
    const blocks = state.streamingBlocks.map(b => {
      if (b.type === 'tool_call' && b.toolUseId === msg.tool_use_id) {
        return { ...b, result: msg.result, isError: msg.is_error, status: 'complete' as const };
      }
      return b;
    });
    set({ streamingBlocks: blocks, agentStatus: { state: 'thinking' } });

    // Update matching panel tab (for non-sub-agent panels like plan_update)
    const matchingTab = state.panels.find(p => p.id === msg.tool_use_id);
    if (matchingTab) {
      get().updatePanelTab(msg.tool_use_id!, {
        content: extractResultText(msg.result),
        streaming: false,
        status: msg.is_error ? 'error' : 'complete',
        isError: msg.is_error || false,
        completedAt: Date.now(),
      });
    }
  }
}

// ------------------------------------------------------------------ //
//  Turn lifecycle: done, stopped, error                               //
// ------------------------------------------------------------------ //

/** Mark any still-running panel tabs as complete & schedule auto-close. */
function finalizeRunningPanels(get: Get): void {
  for (const panel of get().panels) {
    if (panel.status === 'running') {
      get().updatePanelTab(panel.id, {
        status: 'complete',
        streaming: false,
        completedAt: Date.now(),
      });
      if (panel.type !== 'plan') {
        scheduleAutoClose(panel.id, get);
      }
    }
  }
}

export function handleDone(
  msg: Extract<WSMessage, { type: 'done' }>,
  get: Get,
  set: Set,
): void {
  const state = get();
  const doneUpdate: Record<string, unknown> = {
    agentStatus: { state: 'idle' },
  };
  if (msg.usage) {
    doneUpdate.contextUsage = {
      input_tokens: msg.usage.input_tokens || 0,
      output_tokens: msg.usage.output_tokens || 0,
      cache_creation_input_tokens: msg.usage.cache_creation_input_tokens || 0,
      cache_read_input_tokens: msg.usage.cache_read_input_tokens || 0,
      max_context_tokens: msg.max_context_tokens || 200_000,
    };
  }
  if (state.streamingBlocks.length > 0) {
    // Mark any running tool calls as complete
    const finalBlocks = state.streamingBlocks.map(b =>
      b.type === 'tool_call' && b.status === 'running'
        ? { ...b, status: 'complete' as const }
        : b
    );
    set((s) => ({
      messages: [...s.messages, { role: 'assistant' as const, blocks: finalBlocks }],
      streamingBlocks: [],
      isStreaming: false,
      ...doneUpdate,
    }));
  } else {
    set({ isStreaming: false, ...doneUpdate });
  }
  finalizeRunningPanels(get);
  // Reload sessions to pick up updated_at changes
  get().loadSessions();
}

export function handleStopped(
  _msg: Extract<WSMessage, { type: 'stopped' }>,
  get: Get,
  set: Set,
): void {
  const state = get();
  const finalBlocks = state.streamingBlocks.map(b =>
    b.type === 'tool_call' && b.status === 'running'
      ? { ...b, status: 'complete' as const }
      : b
  );
  if (finalBlocks.length > 0) {
    finalBlocks.push({ type: 'text', content: '\n\n*[Stopped by user]*' });
  }
  set((s) => ({
    messages: [...s.messages, {
      role: 'assistant' as const,
      blocks: finalBlocks.length > 0
        ? finalBlocks
        : [{ type: 'text', content: '*[Stopped by user]*' }],
    }],
    streamingBlocks: [],
    isStreaming: false,
    agentStatus: { state: 'idle' },
  }));
  finalizeRunningPanels(get);
  get().loadSessions();
}

export function handleError(
  msg: Extract<WSMessage, { type: 'error' }>,
  _get: Get,
  set: Set,
): void {
  set((s) => ({
    messages: [...s.messages, { role: 'assistant' as const, blocks: [{ type: 'text', content: `Error: ${msg.error}` }] }],
    streamingBlocks: [],
    isStreaming: false,
    agentStatus: { state: 'idle' },
  }));
}
