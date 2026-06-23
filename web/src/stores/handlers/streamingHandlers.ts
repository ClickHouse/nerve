import type { WSMessage } from '../../api/websocket';
import { extractResultText } from '../../utils/extractResultText';
import { appendBlockToPanel, updateToolResultInPanel, scheduleAutoClose } from '../helpers/blockHelpers';
import type { TodoItem, CCTask } from '../chatStore';
import {
  applyCCTaskCreateInput,
  applyCCTaskUpdateInput,
  parseCCTaskListResult,
  parseCCTaskGetResult,
  parseCCTaskCreateResult,
} from '../helpers/ccTasks';
import type { Get, Set } from './types';

/** Tool names that drive the Claude Code task panel (TaskCreate / TaskUpdate
 *  / TaskList / TaskGet). TaskStop / TaskOutput target background subagent
 *  jobs, not the task list, and are intentionally excluded. */
const CC_TASK_TOOLS = new Set([
  'TaskCreate',
  'TaskUpdate',
  'TaskList',
  'TaskGet',
]);

/** Subagent-spawning tool name. Claude Code 2.1.x renamed this from "Task"
 *  → "Agent"; we match both so old chat history still opens panels. */
function isSubagentTool(name: string | undefined): boolean {
  return name === 'Agent' || name === 'Task';
}

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

export function handleWakeup(
  _msg: Extract<WSMessage, { type: 'wakeup' }>,
  get: Get,
  set: Set,
): void {
  // A self-scheduled wakeup just fired — prepend a marker so this turn
  // renders with a "scheduled wakeup" chip, live and after reload.
  const blocks = [...get().streamingBlocks];
  if (!blocks.some((b) => b.type === 'wakeup')) {
    blocks.unshift({ type: 'wakeup' });
  }
  set({ streamingBlocks: blocks, isStreaming: true });
}

export function handleAutoTurn(
  _msg: Extract<WSMessage, { type: 'auto_turn' }>,
  get: Get,
  set: Set,
): void {
  // The CLI started an autonomous turn (e.g. a background task settled
  // and the agent is reporting the result) — prepend a marker so the
  // turn renders with a "background continuation" chip.
  const blocks = [...get().streamingBlocks];
  if (!blocks.some((b) => b.type === 'auto')) {
    blocks.unshift({ type: 'auto' });
  }
  set({ streamingBlocks: blocks, isStreaming: true, agentStatus: { state: 'thinking' } });
}

export function handleToolUse(
  msg: Extract<WSMessage, { type: 'tool_use' }>,
  get: Get,
  set: Set,
): void {
  const state = get();

  // Is this a sub-agent spawn call? (Claude Code 2.1.x renamed Task → Agent)
  if (isSubagentTool(msg.tool)) {
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

  // Is this a dynamic-workflow launch? The Workflow tool spawns a background
  // runtime; the chat shows a compact card and the live phase/agent tree
  // lives in a dedicated side-panel tab keyed by this tool_use_id.
  if (msg.tool === 'Workflow') {
    const toolUseId = msg.tool_use_id || '';
    const name = String(msg.input?.name || 'Workflow');
    const blocks = [...state.streamingBlocks];
    blocks.push({
      type: 'tool_call',
      toolUseId,
      tool: msg.tool,
      input: msg.input,
      status: 'running',
    });
    set({ streamingBlocks: blocks, agentStatus: { state: 'tool', toolName: msg.tool } });
    get().openPanelTab({
      id: toolUseId,
      type: 'workflow',
      label: name,
      subagentType: 'Workflow',
      description: '',
      content: null,
      prompt: '',
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
    // Optimistically reflect Claude Code task tool calls in the panel before
    // the result arrives. TaskCreate adds a placeholder row (real ID lands on
    // tool_result); TaskUpdate mutates by taskId so the row reacts instantly.
    if (msg.tool === 'TaskCreate') {
      const input = (msg.input ?? {}) as Record<string, unknown>;
      extraUpdate.currentCCTasks = applyCCTaskCreateInput(state.currentCCTasks, input, msg.tool_use_id || '');
    } else if (msg.tool === 'TaskUpdate') {
      const input = (msg.input ?? {}) as Record<string, unknown>;
      extraUpdate.currentCCTasks = applyCCTaskUpdateInput(state.currentCCTasks, input);
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

  // A Workflow tool returns immediately with its run id while the workflow
  // keeps running in the background. Record the result on the chat card but
  // DO NOT close the panel — it settles later via a terminal workflow_progress.
  const workflowTab = state.panels.find(p => p.id === msg.tool_use_id && p.type === 'workflow');
  if (workflowTab) {
    const blocks = state.streamingBlocks.map(b =>
      b.type === 'tool_call' && b.toolUseId === msg.tool_use_id
        ? { ...b, result: msg.result, isError: msg.is_error, status: 'complete' as const }
        : b
    );
    set({ streamingBlocks: blocks, agentStatus: { state: 'thinking' } });
    return;
  }

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
    // Find the originating tool_use to know which CC task tool this result
    // belongs to. The tool name doesn't ride on tool_result, so we have to
    // look it up in the block we just updated.
    const ccTaskUpdate: { currentCCTasks?: CCTask[] } = {};
    if (!msg.is_error && msg.tool_use_id) {
      const sourceBlock = blocks.find(
        b => b.type === 'tool_call' && b.toolUseId === msg.tool_use_id,
      );
      const sourceTool = sourceBlock?.type === 'tool_call' ? sourceBlock.tool : undefined;
      if (sourceTool && CC_TASK_TOOLS.has(sourceTool)) {
        const resultText = extractResultText(msg.result);
        let next: CCTask[] | null = null;
        if (sourceTool === 'TaskList') {
          next = parseCCTaskListResult(resultText, state.currentCCTasks);
        } else if (sourceTool === 'TaskCreate') {
          next = parseCCTaskCreateResult(resultText, state.currentCCTasks, msg.tool_use_id);
        } else if (sourceTool === 'TaskGet') {
          next = parseCCTaskGetResult(resultText, state.currentCCTasks);
        }
        // TaskUpdate result is opaque — we already applied input optimistically.
        if (next) ccTaskUpdate.currentCCTasks = next;
      }
    }
    set({ streamingBlocks: blocks, agentStatus: { state: 'thinking' }, ...ccTaskUpdate });

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
    // Workflows run in the background past the launching turn — they settle
    // on their own terminal workflow_progress, not when this turn ends.
    if (panel.type === 'workflow') continue;
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
    const cc = (msg.usage as { cache_creation?: { ephemeral_5m_input_tokens?: number; ephemeral_1h_input_tokens?: number } }).cache_creation;
    doneUpdate.contextUsage = {
      input_tokens: msg.usage.input_tokens || 0,
      output_tokens: msg.usage.output_tokens || 0,
      cache_creation_input_tokens: msg.usage.cache_creation_input_tokens || 0,
      cache_read_input_tokens: msg.usage.cache_read_input_tokens || 0,
      cache_creation_5m_input_tokens: cc?.ephemeral_5m_input_tokens ?? 0,
      cache_creation_1h_input_tokens: cc?.ephemeral_1h_input_tokens ?? 0,
      max_context_tokens: msg.max_context_tokens || 200_000,
      num_turns: msg.num_turns || 1,
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
