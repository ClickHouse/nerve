import type { WSMessage } from '../../api/websocket';
import type { MessageBlock } from '../../types/chat';
import { extractResultText } from '../../utils/extractResultText';
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

// ------------------------------------------------------------------ //
//  Delta batching: thinking and token                                 //
// ------------------------------------------------------------------ //

/** A thinking or token delta that is not yet in the store. */
interface PendingDelta {
  type: 'thinking' | 'text';
  /** Active session when the delta arrived. */
  sessionId: string;
  content: string;
}

// Deltas are applied to the store once per animation frame, not once per
// WS message, so that the chat renders at most once per frame.
let pendingDeltas: PendingDelta[] = [];
let flushFrame: number | null = null;

/** Discard deltas from the view being left before another session can reuse it. */
export function discardStreamDeltas(): void {
  if (flushFrame !== null) {
    cancelAnimationFrame(flushFrame);
    flushFrame = null;
  }
  pendingDeltas = [];
}

function queueDelta(delta: PendingDelta, get: Get, set: Set): void {
  const last = pendingDeltas[pendingDeltas.length - 1];
  if (last && last.type === delta.type && last.sessionId === delta.sessionId) {
    last.content += delta.content;
  } else {
    pendingDeltas.push(delta);
  }
  if (flushFrame === null) {
    flushFrame = requestAnimationFrame(() => flushStreamDeltas(get, set));
  }
}

/** Apply all queued deltas to the store. The WS dispatcher calls this before
 *  it handles any other event, so blocks stay in the order of the events. */
export function flushStreamDeltas(get: Get, set: Set): void {
  if (flushFrame !== null) {
    cancelAnimationFrame(flushFrame);
    flushFrame = null;
  }
  if (pendingDeltas.length === 0) return;
  const deltas = pendingDeltas;
  pendingDeltas = [];
  for (const delta of deltas) {
    // Drop deltas of a session that is no longer on screen.
    if (delta.sessionId !== get().activeSession) continue;
    applyDelta(delta, get, set);
  }
}

function applyDelta(delta: PendingDelta, get: Get, set: Set): void {
  const state = get();
  const blocks = [...state.streamingBlocks];
  const last = blocks[blocks.length - 1];
  if ((last?.type === 'thinking' || last?.type === 'text') && last.type === delta.type) {
    blocks[blocks.length - 1] = { ...last, content: last.content + delta.content };
  } else {
    blocks.push({ type: delta.type, content: delta.content });
  }
  // Keep the same agentStatus object when the state does not change, so that
  // components which select agentStatus do not render again.
  const agentState = delta.type === 'thinking' ? 'thinking' as const : 'writing' as const;
  const agentStatus = state.agentStatus.state === agentState ? state.agentStatus : { state: agentState };
  set({ streamingBlocks: blocks, agentStatus });
}

// ------------------------------------------------------------------ //
//  Streaming handlers: thinking, token, tool_use, tool_result         //
// ------------------------------------------------------------------ //

// Events tagged with parent_tool_use_id are a subagent's own work. The gateway
// sends them only to that agent's transcript channel (agent_event); one that
// still reaches the session stream (an older server mid-deploy) is dropped
// here, never shown as the orchestrator's (mirrors applyStreamEvent).

export function handleThinking(
  msg: Extract<WSMessage, { type: 'thinking' }>,
  get: Get,
  set: Set,
): void {
  if (msg.parent_tool_use_id) return;
  queueDelta({
    type: 'thinking',
    sessionId: get().activeSession,
    content: msg.content,
  }, get, set);
}

export function handleToken(
  msg: Extract<WSMessage, { type: 'token' }>,
  get: Get,
  set: Set,
): void {
  if (msg.parent_tool_use_id) return;
  queueDelta({
    type: 'text',
    sessionId: get().activeSession,
    content: msg.content,
  }, get, set);
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

export function handleModelChanged(
  msg: Extract<WSMessage, { type: 'model_changed' }>,
  get: Get,
  set: Set,
): void {
  // The API switched the model serving this session (e.g. a capacity
  // downgrade away from the configured model, or the recovery back).
  // Append a marker chip at the current stream position — the backend
  // persists the matching block, so it survives reload.
  const blocks = [...get().streamingBlocks];
  blocks.push({
    type: 'model_change',
    from: msg.from_model,
    to: msg.to_model,
    downgrade: msg.downgrade,
  });
  set({ streamingBlocks: blocks });
}

export function handleAutoTurn(
  msg: Extract<WSMessage, { type: 'auto_turn' }>,
  get: Get,
  set: Set,
): void {
  // The CLI started an autonomous turn (e.g. a background task settled
  // and the agent is reporting the result) — prepend a marker naming what
  // woke it ("Background · build-chart finished").
  const blocks = [...get().streamingBlocks];
  if (!blocks.some((b) => b.type === 'auto')) {
    blocks.unshift(autoMarker(msg));
  }
  set({ streamingBlocks: blocks, isStreaming: true, agentStatus: { state: 'thinking' } });
}

/** The leading marker of an autonomous turn, with what triggered it. */
function autoMarker(msg: Extract<WSMessage, { type: 'auto_turn' }>): MessageBlock {
  return {
    type: 'auto',
    ...(msg.triggers ? { triggers: msg.triggers } : {}),
    ...(typeof msg.more === 'number' ? { more: msg.more } : {}),
  };
}

export function handleToolUse(
  msg: Extract<WSMessage, { type: 'tool_use' }>,
  get: Get,
  set: Set,
): void {
  // A subagent's own tool call: it belongs to the agent's transcript.
  if (msg.parent_tool_use_id) return;
  const state = get();

  // An Agent / Task spawn falls through to the main chat below as a plain
  // tool_call block: its agent record (agent_update) drives the inline line
  // and the Agents panel, so no panel opens here.

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

export function handleToolResult(
  msg: Extract<WSMessage, { type: 'tool_result' }>,
  get: Get,
  set: Set,
): void {
  // A subagent's own tool result: it belongs to the agent's transcript.
  if (msg.parent_tool_use_id) return;
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

  // Update the main chat (an Agent / Task result completes its spawn card).
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

export function handleToolOutput(
  msg: Extract<WSMessage, { type: 'tool_output' }>,
  _get: Get,
  set: Set,
): void {
  const parentId = msg.parent_tool_use_id;
  if (parentId) {
    // Child-command live output remains visible in the parent panel once the
    // final tool_result arrives; avoid manufacturing a completed result here.
    return;
  }
  set(state => ({
    streamingBlocks: state.streamingBlocks.map(block => {
      if (block.type !== 'tool_call' || block.toolUseId !== msg.tool_use_id) return block;
      return {
        ...block,
        result: `${block.result ?? ''}${msg.content}`,
        status: 'running' as const,
      };
    }),
  }));
}

// ------------------------------------------------------------------ //
//  Turn lifecycle: done, stopped, error                               //
// ------------------------------------------------------------------ //

/** Settle a plan tab still streaming when the turn ends. Workflows settle on
 *  their own terminal workflow_progress, and the Agents / agent tabs follow
 *  the agent records, so neither ends with the turn. */
function finalizeRunningPanels(get: Get): void {
  for (const panel of get().panels) {
    if (panel.type !== 'plan' || panel.status !== 'running') continue;
    get().updatePanelTab(panel.id, {
      status: 'complete',
      streaming: false,
      completedAt: Date.now(),
    });
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
      context_tokens: msg.context_tokens,
    };
  }
  // A turn that produced nothing but its "background" marker (e.g. the
  // runtime only reported usage) leaves no message behind.
  const loneAutoMarker = state.streamingBlocks.length === 1 && state.streamingBlocks[0].type === 'auto';
  if (state.streamingBlocks.length > 0 && !loneAutoMarker) {
    // Mark any running tool calls as complete
    const finalBlocks = state.streamingBlocks.map(b =>
      b.type === 'tool_call' && b.status === 'running'
        ? { ...b, status: 'complete' as const }
        : b
    );
    set((s) => ({
      messages: [...s.messages, { role: 'assistant' as const, blocks: finalBlocks, created_at: new Date().toISOString() }],
      streamingBlocks: [],
      isStreaming: false,
      ...doneUpdate,
    }));
  } else {
    set({ isStreaming: false, streamingBlocks: [], ...doneUpdate });
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
      created_at: new Date().toISOString(),
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
